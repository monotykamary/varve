use super::native::NativeRuntime;
use super::native_tests::{
    native, native_budget, native_test_guard, options, oracle_tables, paths, resident, stored,
};
use super::*;
use crate::model::RollupRow;
use crate::raw_memory::{RawMemoryBudget, SharedRawRows};
use serde_json::json;
use std::sync::mpsc;
use tempfile::TempDir;

fn engine_config(library: std::path::PathBuf) -> crate::Config {
    crate::Config {
        duckdb_library: Some(library),
        query_native_reuse: true,
        query_executable: "/no-cli-fallback-permitted".into(),
        decoded_cache_bytes: 0,
        ..crate::Config::default()
    }
}

#[test]
fn native_reuse_engine_hot_queries_observe_every_durable_write_without_retaining_credit() {
    let _guard = native_test_guard();
    let (library, _) = paths();
    let directory = TempDir::new().unwrap();
    let config = engine_config(library);
    let encoded = serde_json::to_value(&config).unwrap();
    assert_eq!(encoded["query_native_reuse"], true);
    let default: crate::Config = serde_json::from_value(json!({})).unwrap();
    assert!(!default.query_native_reuse);
    let db = crate::Database::open(directory.path(), config.clone()).unwrap();
    db.create_table("metrics", crate::TableConfig::default())
        .unwrap();
    let sql = "SELECT count(*) AS n, sum(value) AS total FROM metrics";
    for i in 1..=12 {
        let receipt = db
            .write(
                "metrics",
                &format!("row-{i}"),
                vec![stored(i, i as f64, 1, 0).row],
                i,
            )
            .unwrap();
        assert_eq!(receipt.durability, "local_fsync");
        let before = db.status().unwrap().raw_memory.reserved_bytes;
        for _ in 0..2 {
            assert_eq!(
                db.query(sql).unwrap(),
                json!([{"n":i,"total":(i * (i + 1) / 2) as f64}])
            );
            let status = db.status().unwrap();
            assert_eq!(status.raw_memory.reserved_bytes, before);
            assert_eq!((status.active_queries, status.active_snapshots), (0, 0));
            assert_eq!(status.derived_working_bytes, 0);
        }
    }
    let stats = db.native_query_worker_stats().unwrap();
    assert_eq!((stats.spawned, stats.reused, stats.idle), (1, 23, 1));
    assert_eq!(
        db.query_worker_stats().spawned,
        0,
        "legacy CLI diagnostics remain process-only"
    );
    let status = db.status().unwrap().native_query.unwrap();
    assert_eq!(status["reuse_enabled"], true);
    assert_eq!(status["workers"]["reused"], 23);
    drop(db);
    let reopened = crate::Database::open(directory.path(), config).unwrap();
    assert_eq!(reopened.native_query_worker_stats().unwrap().idle, 0);
    assert_eq!(reopened.query(sql).unwrap(), json!([{"n":12,"total":78.0}]));
    assert_eq!(reopened.native_query_worker_stats().unwrap().spawned, 1);
}

#[test]
fn native_reuse_engine_cold_snapshot_pins_end_before_idle_and_gc_replaces_authority() {
    let _guard = native_test_guard();
    let (library, _) = paths();
    let directory = TempDir::new().unwrap();
    let db = crate::Database::open(directory.path(), engine_config(library)).unwrap();
    db.create_table(
        "metrics",
        crate::TableConfig {
            shards: 1,
            window_us: 100,
            retention_us: Some(10),
            rollup_widths_us: vec![100],
            ..crate::TableConfig::default()
        },
    )
    .unwrap();
    db.write("metrics", "old", vec![stored(1, 1.0, 1, 0).row], 1)
        .unwrap();
    db.checkpoint().unwrap();
    let old_path = {
        let state = db.inner.state.lock().unwrap();
        db.inner
            .root
            .join(state.catalog.tables["metrics"].segments[0].key())
    };
    let sql = "SELECT value FROM metrics";
    for _ in 0..2 {
        assert_eq!(db.query(sql).unwrap(), json!([{"value":1.0}]));
    }
    assert_eq!(db.native_query_worker_stats().unwrap().reused, 1);
    let runtime = db.inner.native_runtime.as_ref().unwrap();
    thread::scope(|scope| {
        let (entered, release) = pause(runtime);
        let reader = db.clone();
        let worker = scope.spawn(move || reader.query(sql));
        entered.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(db.status().unwrap().active_snapshots, 1);
        db.write("metrics", "new", vec![stored(21, 21.0, 2, 0).row], 21)
            .unwrap();
        db.checkpoint().unwrap();
        db.maintain(22).unwrap();
        assert!(
            old_path.exists(),
            "active native snapshot must pin selected original file"
        );
        drop(release);
        assert_eq!(worker.join().unwrap().unwrap(), json!([{"value":1.0}]));
    });
    assert_eq!(db.status().unwrap().active_snapshots, 0);
    assert!(
        db.inner.segment_pins.lock().unwrap().is_empty(),
        "idle runtime cannot pin original files"
    );
    assert_eq!(db.native_query_worker_stats().unwrap().idle, 1);
    db.maintain(23).unwrap();
    assert!(!old_path.exists(), "idle session prevented retired file GC");
    let spawned = db.native_query_worker_stats().unwrap().spawned;
    assert_eq!(db.query(sql).unwrap(), json!([{"value":21.0}]));
    assert_eq!(db.native_query_worker_stats().unwrap().spawned, spawned + 1);
    assert_eq!(
        db.query("SELECT sum FROM metrics__rollup").unwrap(),
        json!([{"sum":22.0}])
    );
    assert_eq!(
        db.query(sql).unwrap(),
        json!([{"value":21.0}]),
        "historical rollups must never answer current raw SQL"
    );
}

fn sample(value: f64) -> (Vec<QueryTable>, ResidentSnapshot) {
    resident(
        SharedRawRows::test_rows(vec![stored(1, value, 1, 0)]),
        vec![],
        vec![],
    )
}

#[test]
fn native_reuse_refreshes_current_rows_catalogs_rollups_and_retires_without_pins() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap().with_reuse(true);
    let budget = native_budget();
    for i in 0..130 {
        let rows = SharedRawRows::test_rows(vec![stored(i, i as f64, i as u64 + 1, 0)]);
        let rollup = RollupRow::from_row(1, &rows[0]).unwrap();
        let (tables, mut snapshot) = resident(rows.clone(), vec![], vec![rollup]);
        snapshot.sequence = i as u64 + 1;
        let catalog = QueryCatalog {
            relations: vec![CatalogRelation {
                name: "current_meta".into(),
                columns: vec![("revision".into(), "BIGINT".into())],
                rows: vec![json!([i])],
            }],
            aggregates: vec![],
        };
        let sql = "SELECT value, sequence, (SELECT revision FROM current_meta()) AS revision, (SELECT sum FROM metrics__rollup) AS rollup FROM metrics";
        let result = runtime
            .execute(
                &tables,
                Some(&snapshot),
                sql,
                &options,
                &catalog,
                &budget,
                &AtomicBool::new(false),
            )
            .unwrap();
        assert_eq!(
            result,
            json!([{"value":i as f64,"sequence":(i + 1).to_string(),"revision":i,"rollup":i as f64}])
        );
        assert_eq!(
            rows.strong_count(),
            2,
            "idle worker retained raw owner at refresh {i}"
        );
        assert_eq!(runtime.live_callback_owners(), 0);
        assert_eq!(budget.status().reserved_bytes, 0);
        assert_eq!(budget.status().working_bytes, 0);
        let usage = runtime.last_scratch_usage().unwrap();
        assert_eq!(usage.owner_live_after_close, 0);
        assert_eq!(usage.owner_bytes_after_close, 0);
    }
    let stats = runtime.stats();
    assert_eq!(
        stats.spawned, 3,
        "must reuse actual sessions, not fall back after every rollback"
    );
    assert_eq!(stats.reused, 127);
    assert_eq!(stats.resets, 130);
    assert_eq!((stats.active, stats.idle, stats.discarded), (0, 1, 2));
    drop(runtime);
    assert_eq!(budget.status().reserved_bytes, 0);
}

#[test]
fn native_reuse_replaces_namespace_and_current_table_sets_and_matches_cli_exactly() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap().with_reuse(true);
    let (tables, snapshot) = sample(-0.0);
    let catalog = QueryCatalog::default();
    let oracle = oracle_tables(&tables, &snapshot);
    for sql in [
        "SELECT timestamp_us, value, sequence, ordinal, tags FROM metrics",
        "SELECT NULL AS missing, 18446744073709551615::UBIGINT AS unsigned, 12.50::DECIMAL(6,2) AS decimal",
        "SELECT {'decimal':12.50::DECIMAL(6,2),'items':[1,NULL,3]} AS nested",
        "SELECT * FROM metrics WHERE false",
    ] {
        for _ in 0..2 {
            assert_eq!(
                native(&runtime, &tables, &snapshot, sql, &options, &catalog),
                execute_with_catalog(&oracle, sql, &options, &catalog).unwrap(),
                "{sql}"
            );
        }
    }
    // Physical plans describe different scanners; data results above still
    // require exact CLI equality. Reuse must preserve the fresh native plan.
    fn plan_text(value: &serde_json::Value) -> &str {
        let rows = value.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].as_object().unwrap().len(), 1);
        let text = rows[0]["plan"].as_str().unwrap();
        assert!(!text.trim().is_empty());
        text
    }
    let explain = "EXPLAIN SELECT value FROM metrics";
    let fresh = NativeRuntime::new(&library, 1).unwrap();
    let budget = native_budget();
    let expected_plan = fresh
        .execute(
            &tables,
            Some(&snapshot),
            explain,
            &options,
            &catalog,
            &budget,
            &AtomicBool::new(false),
        )
        .unwrap();
    let text = plan_text(&expected_plan);
    assert!(text.contains("__VARVE_RAW_SCAN_0"), "{text}");
    assert!(text.contains("Projections: value"), "{text}");
    assert_eq!(
        (
            fresh.stats().spawned,
            fresh.stats().reused,
            fresh.stats().idle
        ),
        (1, 0, 0)
    );
    assert_eq!(fresh.live_callback_owners(), 0);
    assert_eq!(budget.status().reserved_bytes, 0);
    assert_eq!(budget.status().working_bytes, 0);
    let before_plans = runtime.stats();
    for _ in 0..2 {
        let actual = runtime
            .execute(
                &tables,
                Some(&snapshot),
                explain,
                &options,
                &catalog,
                &budget,
                &AtomicBool::new(false),
            )
            .unwrap();
        plan_text(&actual);
        assert_eq!(actual, expected_plan);
        assert_eq!((runtime.stats().active, runtime.stats().idle), (0, 1));
        assert_eq!(runtime.live_callback_owners(), 0);
        assert_eq!(budget.status().reserved_bytes, 0);
        assert_eq!(budget.status().working_bytes, 0);
        let usage = runtime.last_scratch_usage().unwrap();
        assert_eq!(usage.owner_live_after_close, 0);
        assert_eq!(usage.owner_bytes_after_close, 0);
    }
    assert_eq!(runtime.stats().spawned, before_plans.spawned);
    assert_eq!(runtime.stats().reused, before_plans.reused + 2);
    assert_eq!(runtime.stats().resets, before_plans.resets + 2);
    let cli_plan = execute_with_catalog(&oracle, explain, &options, &catalog).unwrap();
    let text = plan_text(&cli_plan);
    assert!(text.contains("Seq Scan"), "{text}");
    assert!(text.contains("__varve_input"), "{text}");

    // Seed an idle session with the same bounded-output key, then overflow an
    // EXPLAIN on that reused session. Failure must close, not reset or retain it.
    let small = QueryOptions {
        max_output_bytes: serde_json::to_vec(&expected_plan).unwrap().len() - 1,
        ..options.clone()
    };
    assert_eq!(
        runtime
            .execute(
                &tables,
                Some(&snapshot),
                "SELECT value FROM metrics",
                &small,
                &catalog,
                &budget,
                &AtomicBool::new(false),
            )
            .unwrap(),
        json!([{"value":-0.0}])
    );
    let before_overflow = runtime.stats();
    let error = runtime
        .execute(
            &tables,
            Some(&snapshot),
            explain,
            &small,
            &catalog,
            &budget,
            &AtomicBool::new(false),
        )
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("output exceeded"),
        "{error:#}"
    );
    assert_eq!(runtime.stats().reused, before_overflow.reused + 1);
    assert_eq!(runtime.stats().resets, before_overflow.resets);
    assert_eq!(runtime.stats().discarded, before_overflow.discarded + 1);
    assert_eq!((runtime.stats().active, runtime.stats().idle), (0, 0));
    assert_eq!(runtime.live_callback_owners(), 0);
    assert_eq!(budget.status().reserved_bytes, 0);
    assert_eq!(budget.status().working_bytes, 0);
    let usage = runtime.last_scratch_usage().unwrap();
    assert_eq!(usage.owner_live_after_close, 0);
    assert_eq!(usage.owner_bytes_after_close, 0);
    let recovered = runtime
        .execute(
            &tables,
            Some(&snapshot),
            explain,
            &options,
            &catalog,
            &budget,
            &AtomicBool::new(false),
        )
        .unwrap();
    assert_eq!(recovered, expected_plan);
    assert_eq!(runtime.stats().spawned, before_overflow.spawned + 1);
    assert_eq!(runtime.stats().resets, before_overflow.resets + 1);
    assert_eq!((runtime.stats().active, runtime.stats().idle), (0, 1));
    assert_eq!(runtime.live_callback_owners(), 0);
    assert_eq!(budget.status().reserved_bytes, 0);
    assert_eq!(budget.status().working_bytes, 0);
    assert!(runtime.stats().reused > 0);
    let before = runtime.stats().spawned;
    let (mut next_tables, mut next_snapshot) = sample(99.0);
    next_snapshot.namespace = "other-authority".into();
    next_tables[0].name = "other".into();
    next_snapshot.tables[0].name = "other".into();
    next_snapshot.lineage[0].name = "other".into();
    assert_eq!(
        native(
            &runtime,
            &next_tables,
            &next_snapshot,
            "SELECT value FROM other",
            &options,
            &catalog
        ),
        json!([{"value":99.0}])
    );
    assert_eq!(runtime.stats().spawned, before + 1);
    // Same namespace, completely different current relation set: no name cache.
    let empty = ResidentSnapshot {
        namespace: next_snapshot.namespace.clone(),
        sequence: 0,
        tables: vec![],
        lineage: vec![],
    };
    assert_eq!(
        native(
            &runtime,
            &[],
            &empty,
            "SELECT count(*) AS n FROM duckdb_tables() WHERE table_name IN ('metrics','other')",
            &options,
            &catalog
        ),
        json!([{"n":0}])
    );
    let error = runtime
        .execute(
            &[],
            Some(&empty),
            "SELECT * FROM other",
            &options,
            &catalog,
            &native_budget(),
            &AtomicBool::new(false),
        )
        .unwrap_err();
    assert!(format!("{error:#}").contains("other"));
    assert_eq!(runtime.stats().idle, 0);
    assert_eq!(runtime.live_callback_owners(), 0);
}

#[test]
fn native_reuse_cold_allowlist_is_exact_not_a_union_and_settings_replace_sessions() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let mut options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap().with_reuse(true);
    let directory = TempDir::new().unwrap();
    let a = directory.path().join("a.parquet");
    let b = directory.path().join("b.parquet");
    crate::segment::write(&a, &[stored(1, 10.0, 1, 0)]).unwrap();
    crate::segment::write(&b, &[stored(1, 20.0, 1, 0)]).unwrap();
    let catalog = QueryCatalog::default();
    for (index, path) in [&a, &b].into_iter().enumerate() {
        let file = ResidentFile {
            id: format!("cold-{index}"),
            path: path.canonicalize().unwrap(),
            rows: 1,
            charged_bytes: 4096,
            min_timestamp_us: 1,
            max_timestamp_us: 1,
        };
        let (tables, snapshot) = resident(SharedRawRows::test_rows(vec![]), vec![file], vec![]);
        for _ in 0..2 {
            assert_eq!(
                native(
                    &runtime,
                    &tables,
                    &snapshot,
                    "SELECT value FROM metrics",
                    &options,
                    &catalog
                ),
                json!([{"value":10.0 * (index + 1) as f64}])
            );
        }
        let settings = native(
            &runtime,
            &tables,
            &snapshot,
            "SELECT current_setting('allowed_paths') AS paths",
            &options,
            &catalog,
        );
        let encoded = settings.to_string();
        assert!(encoded.contains(path.file_name().unwrap().to_str().unwrap()));
        let other = if index == 0 { &b } else { &a };
        assert!(!encoded.contains(other.file_name().unwrap().to_str().unwrap()));
        let error = runtime
            .execute(
                &tables,
                Some(&snapshot),
                &format!("SELECT * FROM read_parquet({})", quote_path(other).unwrap()),
                &options,
                &catalog,
                &native_budget(),
                &AtomicBool::new(false),
            )
            .unwrap_err();
        assert!(
            format!("{error:#}").to_lowercase().contains("permission"),
            "{error:#}"
        );
        assert_eq!(runtime.live_callback_owners(), 0);
    }
    let (tables, snapshot) = sample(3.0);
    for change in 0..5 {
        match change {
            1 => options.threads = 1,
            2 => options.memory_mb += 1,
            3 => options.timeout_ms += 1,
            4 => options.max_output_bytes += 1,
            _ => {}
        }
        let before = runtime.stats().spawned;
        native(
            &runtime,
            &tables,
            &snapshot,
            "SELECT value FROM metrics",
            &options,
            &catalog,
        );
        assert_eq!(runtime.stats().spawned, before + 1);
        native(
            &runtime,
            &tables,
            &snapshot,
            "SELECT value FROM metrics",
            &options,
            &catalog,
        );
        assert_eq!(runtime.stats().spawned, before + 1);
    }
}

#[test]
fn native_reuse_cancel_at_each_phase_discards_then_accepts_fresh_inputs() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap().with_reuse(true);
    let (tables, snapshot) = sample(7.0);
    let catalog = QueryCatalog::default();
    for phase in 1..=4 {
        native(
            &runtime,
            &tables,
            &snapshot,
            "SELECT value FROM metrics",
            &options,
            &catalog,
        );
        let cancelled = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&cancelled);
        if phase == 4 {
            runtime.set_before_execute_hook(move || signal.store(true, Ordering::Release));
        } else {
            runtime.set_phase_hook(phase, move || signal.store(true, Ordering::Release));
        }
        let budget = native_budget();
        let error = runtime
            .execute(
                &tables,
                Some(&snapshot),
                "SELECT value FROM metrics",
                &options,
                &catalog,
                &budget,
                &cancelled,
            )
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("cancelled"),
            "phase {phase}: {error:#}"
        );
        assert_eq!(runtime.stats().idle, 0);
        assert_eq!(runtime.live_callback_owners(), 0);
        assert_eq!(budget.status().reserved_bytes, 0);
        let (new_tables, new_snapshot) = sample(8.0);
        assert_eq!(
            native(
                &runtime,
                &new_tables,
                &new_snapshot,
                "SELECT value FROM metrics",
                &options,
                &catalog
            ),
            json!([{"value":8.0}])
        );
    }
}

#[test]
fn native_reuse_failed_bind_overflow_unwind_and_fresh_only_do_not_poison_idle_workers() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 2).unwrap().with_reuse(true);
    let (tables, snapshot) = sample(7.0);
    let catalog = QueryCatalog::default();
    let budget = native_budget();
    native(
        &runtime,
        &tables,
        &snapshot,
        "SELECT value FROM metrics",
        &options,
        &catalog,
    );
    let reused = runtime.stats().reused;
    native(
        &runtime,
        &tables,
        &snapshot,
        "SELECT random() AS random",
        &options,
        &catalog,
    );
    assert_eq!(
        runtime.stats().reused,
        reused,
        "stateful SQL must never acquire an idle session"
    );
    assert_eq!(runtime.stats().idle, 1);
    for stage in 1..=6 {
        {
            let _fault = runtime.inject_callback_failure(stage);
            assert!(
                runtime
                    .execute(
                        &tables,
                        Some(&snapshot),
                        "SELECT value FROM metrics",
                        &options,
                        &catalog,
                        &budget,
                        &AtomicBool::new(false)
                    )
                    .is_err()
            );
        }
        assert_eq!(runtime.live_callback_owners(), 0);
        assert_eq!(budget.status().reserved_bytes, 0);
        native(
            &runtime,
            &tables,
            &snapshot,
            "SELECT value FROM metrics",
            &options,
            &catalog,
        );
    }
    let small = QueryOptions {
        max_output_bytes: 1,
        ..options.clone()
    };
    assert!(
        runtime
            .execute(
                &tables,
                Some(&snapshot),
                "SELECT value FROM metrics",
                &small,
                &catalog,
                &budget,
                &AtomicBool::new(false)
            )
            .is_err()
    );
    runtime.set_before_execute_hook(|| panic!("reuse execution unwind"));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.execute(
                &tables,
                Some(&snapshot),
                "SELECT value FROM metrics",
                &options,
                &catalog,
                &budget,
                &AtomicBool::new(false),
            )
        }))
        .is_err()
    );
    assert_eq!(runtime.live_callback_owners(), 0);
    assert_eq!(budget.status().reserved_bytes, 0);
    native(
        &runtime,
        &tables,
        &snapshot,
        "SELECT value FROM metrics",
        &options,
        &catalog,
    );
    assert_eq!(runtime.stats().active, 0);
    assert_eq!(runtime.stats().idle, 1);
}

#[test]
fn native_reuse_failed_reset_closes_and_default_or_unscoped_execution_stays_fresh() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let catalog = QueryCatalog::default();
    let (tables, snapshot) = sample(17.0);
    let budget = native_budget();
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    for _ in 0..2 {
        assert_eq!(
            native(
                &runtime,
                &tables,
                &snapshot,
                "SELECT value FROM metrics",
                &options,
                &catalog
            ),
            json!([{"value":17.0}])
        );
    }
    assert_eq!(
        (
            runtime.stats().spawned,
            runtime.stats().reused,
            runtime.stats().idle
        ),
        (2, 0, 0)
    );
    drop(runtime);
    let runtime = NativeRuntime::new(&library, 1).unwrap().with_reuse(true);
    for _ in 0..2 {
        assert_eq!(
            runtime
                .execute(
                    &[],
                    None,
                    "SELECT 1 AS value",
                    &options,
                    &catalog,
                    &budget,
                    &AtomicBool::new(false)
                )
                .unwrap(),
            json!([{"value":1}])
        );
    }
    assert_eq!(
        (
            runtime.stats().spawned,
            runtime.stats().reused,
            runtime.stats().idle
        ),
        (2, 0, 0)
    );
    native(
        &runtime,
        &tables,
        &snapshot,
        "SELECT value FROM metrics",
        &options,
        &catalog,
    );
    runtime.fail_next_reset();
    // User output remains exact, but the failed cleanup cannot return a session.
    assert_eq!(
        native(
            &runtime,
            &tables,
            &snapshot,
            "SELECT value FROM metrics",
            &options,
            &catalog
        ),
        json!([{"value":17.0}])
    );
    assert_eq!(runtime.stats().idle, 0);
    assert_eq!(runtime.live_callback_owners(), 0);
    assert_eq!(runtime.stats().resets, 1);
    let before = runtime.stats().spawned;
    for _ in 0..2 {
        native(
            &runtime,
            &tables,
            &snapshot,
            "SELECT value FROM metrics",
            &options,
            &catalog,
        );
    }
    assert_eq!(runtime.stats().spawned, before + 1);
    assert_eq!(runtime.stats().idle, 1);
}

// Dropping this gate releases a paused worker even when the assertion path
// unwinds. Receives have timeouts only as a liveness bound, never as scheduling.
struct Release(Option<mpsc::SyncSender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn pause(runtime: &NativeRuntime) -> (mpsc::Receiver<()>, Release) {
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    runtime.set_before_execute_hook(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    (entered_rx, Release(Some(release_tx)))
}

#[test]
fn native_reuse_concurrent_leases_capacity_budget_and_cancellation_are_disjoint() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = Arc::new(NativeRuntime::new(&library, 2).unwrap().with_reuse(true));
    let budget = native_budget();
    let cancelled = Arc::new(AtomicBool::new(false));
    thread::scope(|scope| {
        let spawn = |value, signal: Arc<AtomicBool>| {
            let runtime = Arc::clone(&runtime);
            let options = options.clone();
            let budget = budget.clone();
            scope.spawn(move || {
                let (tables, snapshot) = sample(value);
                runtime.execute(
                    &tables,
                    Some(&snapshot),
                    "SELECT value FROM metrics",
                    &options,
                    &QueryCatalog::default(),
                    &budget,
                    &signal,
                )
            })
        };
        let (entered_a, release_a) = pause(&runtime);
        let a = spawn(11.0, Arc::clone(&cancelled));
        entered_a.recv_timeout(Duration::from_secs(10)).unwrap();
        let (tables, snapshot) = sample(33.0);
        let tiny = RawMemoryBudget::new(1, 1).unwrap();
        assert!(
            runtime
                .execute(
                    &tables,
                    Some(&snapshot),
                    "SELECT value FROM metrics",
                    &options,
                    &QueryCatalog::default(),
                    &tiny,
                    &AtomicBool::new(false)
                )
                .is_err()
        );
        assert_eq!(runtime.stats().spawned, 1, "quota must reject before open");
        let (entered_b, release_b) = pause(&runtime);
        let b = spawn(22.0, Arc::new(AtomicBool::new(false)));
        entered_b.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!((runtime.stats().active, runtime.stats().idle), (2, 0));
        let error = runtime
            .execute(
                &tables,
                Some(&snapshot),
                "SELECT value FROM metrics",
                &options,
                &QueryCatalog::default(),
                &budget,
                &AtomicBool::new(false),
            )
            .unwrap_err();
        assert!(format!("{error:#}").contains("capacity"));
        cancelled.store(true, Ordering::Release);
        drop(release_a);
        assert!(format!("{:#}", a.join().unwrap().unwrap_err()).contains("cancelled"));
        drop(release_b);
        assert_eq!(b.join().unwrap().unwrap(), json!([{"value":22.0}]));
        let spawned = runtime.stats().spawned;
        assert_eq!(
            native(
                &runtime,
                &tables,
                &snapshot,
                "SELECT value FROM metrics",
                &options,
                &QueryCatalog::default()
            ),
            json!([{"value":33.0}])
        );
        assert_eq!(runtime.stats().spawned, spawned);
        assert_eq!(runtime.live_callback_owners(), 0);
        assert_eq!(budget.status().reserved_bytes, 0);
        assert_eq!((runtime.stats().active, runtime.stats().idle), (0, 1));
    });
    drop(runtime);
    assert_eq!(budget.status().reserved_bytes, 0);
}
