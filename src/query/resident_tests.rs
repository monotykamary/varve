use super::*;

#[cfg(unix)]
#[path = "resident_process_tests.rs"]
mod process;

fn options() -> QueryOptions {
    QueryOptions {
        executable: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb"),
        timeout_ms: 5_000,
        ..QueryOptions::default()
    }
}

fn fixture(n: usize) -> (Vec<QueryTable>, ResidentSnapshot) {
    let rows = (0..n)
        .map(|i| StoredRow {
            row: crate::model::Row {
                timestamp_us: i as i64,
                tenant: "t'雪\\\n.shell echo no".into(),
                series: "s\"'); SELECT error('injected');--".into(),
                value: i as f64,
                tags: BTreeMap::from([("quote'雪".into(), "\t\\\"".into())]),
            },
            sequence: u64::MAX,
            ordinal: i as u32,
        })
        .collect();
    (
        vec![QueryTable {
            name: "metrics".into(),
            hot: vec![],
            files: vec![],
            rollups: vec![],
            cutoff_us: None,
        }],
        ResidentSnapshot {
            namespace: "db1".into(),
            sequence: 1,
            tables: vec![ResidentTable {
                name: "metrics".into(),
                batches: vec![ResidentBatch::new(
                    "batch1".into(),
                    crate::raw_memory::SharedRawRows::test_rows(rows),
                    0,
                )],
                files: vec![],
            }],
            lineage: vec![ResidentLineage {
                name: "metrics".into(),
                raw_stamp: 1,
                ids: vec!["batch1".into()],
            }],
        },
    )
}

fn named_fixture(
    name: &str,
    id: &str,
    value: f64,
    charged_bytes: usize,
    raw_stamp: u64,
) -> (Vec<QueryTable>, ResidentSnapshot) {
    let row = StoredRow {
        row: crate::model::Row {
            timestamp_us: 1,
            tenant: "tenant".into(),
            series: "series".into(),
            value,
            tags: BTreeMap::new(),
        },
        sequence: 1,
        ordinal: 0,
    };
    (
        vec![QueryTable {
            name: name.into(),
            hot: vec![],
            files: vec![],
            rollups: vec![],
            cutoff_us: None,
        }],
        ResidentSnapshot {
            namespace: "ledger".into(),
            sequence: 1,
            tables: vec![ResidentTable {
                name: name.into(),
                batches: vec![ResidentBatch::new(
                    id.into(),
                    crate::raw_memory::SharedRawRows::test_rows(vec![row]),
                    charged_bytes,
                )],
                files: vec![],
            }],
            lineage: vec![ResidentLineage {
                name: name.into(),
                raw_stamp,
                ids: vec![id.into()],
            }],
        },
    )
}

fn run(
    runtime: &QueryRuntime,
    tables: &[QueryTable],
    snapshot: &ResidentSnapshot,
    sql: &str,
    catalog: &QueryCatalog,
) -> Value {
    runtime
        .execute_resident_with_catalog(tables, snapshot, sql, &options(), catalog)
        .unwrap()
}

fn oracle(
    tables: &[QueryTable],
    snapshot: &ResidentSnapshot,
    sql: &str,
    catalog: &QueryCatalog,
) -> Value {
    let mut tables = tables.to_vec();
    for table in &mut tables {
        table.hot = snapshot
            .tables
            .iter()
            .find(|resident| resident.name == table.name)
            .unwrap()
            .batches
            .iter()
            .flat_map(|batch| batch.rows.iter().cloned())
            .collect();
    }
    execute_with_catalog(&tables, sql, &options(), catalog).unwrap()
}

#[test]
fn unchanged_resident_hit_skips_setup_but_not_selection_or_snapshot_validation() {
    let runtime = QueryRuntime::new(1);
    let (tables, mut a) = named_fixture("metrics", "a", 2.0, 1024, 1);
    let (_, mut b) = named_fixture("metrics", "b", 9.0, 1024, 1);
    for snapshot in [&mut a, &mut b] {
        snapshot.lineage[0].ids = vec!["a".into(), "b".into()];
    }
    let catalog = QueryCatalog::default();
    let sql = "SELECT count(*) AS n, sum(value) AS total FROM metrics";
    let calls = || {
        let pool = runtime.pool.lock().unwrap();
        assert_eq!(pool.idle.len(), 1);
        pool.idle[0].run_calls
    };
    for (snapshot, total, expected_calls) in [
        (&a, 2.0, 3),
        (&a, 2.0, 5),
        (&b, 9.0, 8),
        (&b, 9.0, 10),
        (&a, 2.0, 13),
        (&a, 2.0, 15),
    ] {
        assert_eq!(
            run(&runtime, &tables, snapshot, sql, &catalog),
            json!([{"n":1,"total":total}])
        );
        assert_eq!(calls(), expected_calls);
    }
    // A metadata-only frontier advance remains visible even without setup SQL.
    a.sequence += 1;
    run(&runtime, &tables, &a, sql, &catalog);
    assert_eq!(calls(), 17);
    let pool = runtime.pool.lock().unwrap();
    let state = pool.idle[0].resident.as_ref().unwrap();
    assert_eq!(state.sequence, a.sequence);
    assert!(state.batches[&("metrics".into(), "a".into())].selected);
    assert!(!state.batches[&("metrics".into(), "b".into())].selected);
    assert!(pool.idle[0].inputs.read_dir().unwrap().next().is_none());
    drop(pool);
    // The marker uses the existing fixed identity reserve, not another ID map.
    assert!(
        std::mem::size_of::<LoadedBatch>() + 2 * std::mem::size_of::<BatchKey>() + 64
            <= RETAINED_IDENTITY_RESERVE_BYTES
    );
    assert_eq!(runtime.stats().resident_raw_staged_rows, 2);
}

#[test]
fn legacy_cold_charge_uses_native_fallback_without_losing_hot_rows() {
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("cold.parquet");
    let unselected = directory.path().join("unselected.parquet");
    let (mut tables, mut snapshot) = fixture(2);
    let mut cold = snapshot.tables[0].batches[0].rows.to_vec();
    for (index, row) in cold.iter_mut().enumerate() {
        row.sequence = 0;
        row.row.timestamp_us = 10 + index as i64;
        row.row.value = 10.0 + index as f64;
    }
    crate::segment::write(&path, &cold).unwrap();
    crate::segment::write(&unselected, &cold).unwrap();
    let id = blake3::hash(&std::fs::read(&path).unwrap())
        .to_hex()
        .to_string();
    tables[0].files.push(path.clone());
    snapshot.tables[0].files.push(ResidentFile {
        id: id.clone(),
        path,
        rows: cold.len(),
        charged_bytes: 0,
        min_timestamp_us: 10,
        max_timestamp_us: 11,
    });
    snapshot.lineage[0].ids.push(id);
    let runtime = QueryRuntime::new(1);
    let sql = "SELECT count(*) AS n, sum(value) AS total FROM metrics";
    let result = runtime
        .execute_resident_with_catalog(
            &tables,
            &snapshot,
            sql,
            &options(),
            &QueryCatalog::default(),
        )
        .unwrap();
    assert_eq!(result, serde_json::json!([{"n": 4, "total": 22.0}]));
    assert_eq!(runtime.stats().resident_raw_staged_rows, 2);
    assert_eq!(
        runtime.stats().idle,
        0,
        "native fallback must be disposable"
    );

    let direct = format!(
        "SELECT count(*) AS n FROM read_parquet({})",
        quote_path(&tables[0].files[0]).unwrap()
    );
    assert_eq!(
        runtime
            .execute_resident_with_catalog(
                &tables,
                &snapshot,
                &direct,
                &options(),
                &QueryCatalog::default(),
            )
            .unwrap(),
        serde_json::json!([{"n": 2}])
    );
    let denied = runtime
        .execute_resident_with_catalog(
            &tables,
            &snapshot,
            &format!(
                "SELECT * FROM read_parquet({})",
                quote_path(&unselected).unwrap()
            ),
            &options(),
            &QueryCatalog::default(),
        )
        .unwrap_err();
    assert!(format!("{denied:#}").contains("disabled"), "{denied:#}");
    assert_eq!(runtime.stats().idle, 0);
}

#[test]
fn retained_capacity_uses_selected_only_fallback_for_cold_and_unrelated_lineage() {
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("cold.parquet");
    let unselected = directory.path().join("unselected.parquet");
    let (mut tables, mut snapshot) = fixture(1);
    let mut cold = snapshot.tables[0].batches[0].rows.to_vec();
    cold[0].row.timestamp_us = 10;
    cold[0].row.value = 10.0;
    crate::segment::write(&path, &cold).unwrap();
    crate::segment::write(&unselected, &cold).unwrap();
    let id = blake3::hash(&std::fs::read(&path).unwrap())
        .to_hex()
        .to_string();
    let cold_charge = 4096;
    assert!(cold_charge < MAX_QUERY_INPUT_BYTES - MAX_QUERY_SCRIPT_BYTES);
    tables[0].files.push(path.clone());
    snapshot.tables[0].files.push(ResidentFile {
        id: id.clone(),
        path,
        rows: 1,
        charged_bytes: cold_charge,
        min_timestamp_us: 10,
        max_timestamp_us: 10,
    });
    snapshot.lineage[0].ids.push(id);

    let cancelled = AtomicBool::new(false);
    let catalog = QueryCatalog::default();
    let deadline = Instant::now() + Duration::from_secs(5);
    let retained = ResidentRequest::new(
        &tables,
        &snapshot,
        &options(),
        &catalog,
        &[],
        false,
        deadline,
        &cancelled,
    )
    .unwrap();
    let native = ResidentRequest::new(
        &tables,
        &snapshot,
        &options(),
        &catalog,
        &[],
        true,
        deadline,
        &cancelled,
    )
    .unwrap();
    assert_eq!(native.lineage["metrics"].ids.len(), 1);
    assert!(
        !native.lineage["metrics"]
            .ids
            .contains(snapshot.tables[0].files[0].id.as_str())
    );
    let hot_charge = snapshot.tables[0].batches[0].charged_bytes;
    let retained_total = retained.retained_bytes + hot_charge + cold_charge;
    let native_total = native.retained_bytes + hot_charge;
    let resident_limit = native_total + (retained_total - native_total) / 2;
    assert!(cold_charge < resident_limit);
    assert!(retained_total > resident_limit && native_total <= resident_limit);
    drop((retained, native));

    let runtime = QueryRuntime::new(1);
    assert_eq!(
        runtime
            .execute_resident_with_limit(
                &tables,
                &snapshot,
                "SELECT count(*) AS n, sum(value) AS total FROM metrics",
                &options(),
                &QueryCatalog::default(),
                &cancelled,
                resident_limit,
            )
            .unwrap(),
        serde_json::json!([{"n": 2, "total": 10.0}])
    );
    assert_eq!(runtime.stats().idle, 0);
    let direct = format!(
        "SELECT count(*) AS n FROM read_parquet({})",
        quote_path(&tables[0].files[0]).unwrap()
    );
    assert_eq!(
        runtime
            .execute_resident_with_limit(
                &tables,
                &snapshot,
                &direct,
                &options(),
                &QueryCatalog::default(),
                &cancelled,
                resident_limit,
            )
            .unwrap(),
        json!([{"n": 1}])
    );
    let denied = runtime
        .execute_resident_with_limit(
            &tables,
            &snapshot,
            &format!(
                "SELECT * FROM read_parquet({})",
                quote_path(&unselected).unwrap()
            ),
            &options(),
            &QueryCatalog::default(),
            &cancelled,
            resident_limit,
        )
        .unwrap_err();
    assert!(format!("{denied:#}").contains("disabled"), "{denied:#}");
    assert_eq!(runtime.stats().idle, 0);

    // This is a new independent preflight, after three separately timed SQL calls.
    let deadline = Instant::now() + Duration::from_secs(5);
    let tables = Vec::<QueryTable>::new();
    let snapshot = ResidentSnapshot {
        namespace: "storage-free".into(),
        sequence: 1,
        tables: vec![],
        lineage: vec![ResidentLineage {
            name: "unrelated".into(),
            raw_stamp: 9,
            ids: vec!["x".repeat(4096)],
        }],
    };
    let retained = ResidentRequest::new(
        &tables,
        &snapshot,
        &options(),
        &catalog,
        &[],
        false,
        deadline,
        &cancelled,
    )
    .unwrap();
    let native = ResidentRequest::new(
        &tables,
        &snapshot,
        &options(),
        &catalog,
        &[],
        true,
        deadline,
        &cancelled,
    )
    .unwrap();
    assert!(native.lineage.is_empty());
    let resident_limit = native.retained_bytes + 512;
    assert!(retained.retained_bytes > resident_limit);
    drop((retained, native));
    assert_eq!(
        runtime
            .execute_resident_with_limit(
                &tables,
                &snapshot,
                "SELECT 1 AS answer",
                &options(),
                &QueryCatalog::default(),
                &cancelled,
                resident_limit,
            )
            .unwrap(),
        serde_json::json!([{"answer": 1}])
    );
    assert_eq!(runtime.stats().idle, 0);
}

#[test]
fn reusable_budget_tracks_worker_memory_but_does_not_raise_request_limits() {
    let mut options = options();
    for (memory_mb, expected_mb) in [(16, 4), (128, 32), (256, 64), (512, 128)] {
        options.memory_mb = memory_mb;
        assert_eq!(
            reusable_input_limit(&options, MAX_QUERY_INPUT_BYTES),
            expected_mb * 1024 * 1024
        );
        assert_eq!(reusable_input_limit(&options, 1024), 1024);
    }
    options.memory_mb = usize::MAX;
    assert_eq!(
        reusable_input_limit(&options, MAX_QUERY_INPUT_BYTES),
        MAX_QUERY_INPUT_BYTES
    );
}

#[test]
fn reusable_cache_leaves_execution_headroom_without_reducing_native_input_admission() {
    for cold in [true, false] {
        let directory = tempfile::TempDir::new().unwrap();
        let (mut tables, mut snapshot) = fixture(1);
        let charge = MAX_QUERY_INPUT_BYTES * 3 / 4;
        if cold {
            let path = directory.path().join("cold.parquet");
            let mut rows = snapshot.tables[0].batches[0].rows.to_vec();
            rows[0].row.timestamp_us = 10;
            rows[0].row.value = 10.0;
            crate::segment::write(&path, &rows).unwrap();
            let id = blake3::hash(&std::fs::read(&path).unwrap())
                .to_hex()
                .to_string();
            tables[0].files.push(path.clone());
            snapshot.tables[0].files.push(ResidentFile {
                id: id.clone(),
                path,
                rows: 1,
                charged_bytes: charge,
                min_timestamp_us: 10,
                max_timestamp_us: 10,
            });
            snapshot.lineage[0].ids.push(id);
        } else {
            snapshot.tables[0].batches[0].charged_bytes = charge;
        }
        let runtime = QueryRuntime::new(1);
        let options = QueryOptions {
            memory_mb: 256,
            ..options()
        };
        let result = runtime
            .execute_resident_with_catalog(
                &tables,
                &snapshot,
                "SELECT count(*) AS n, sum(value) AS total FROM metrics",
                &options,
                &QueryCatalog::default(),
            )
            .unwrap();
        let expected = if cold {
            json!([{"n": 2, "total": 10.0}])
        } else {
            json!([{"n": 1, "total": 0.0}])
        };
        assert_eq!(result, expected);
        // A cache admission miss must remain a valid bounded one-shot request.
        assert_eq!(runtime.stats().idle, 0, "oversized cache stayed resident");
        assert_eq!(runtime.stats().resets, 1);
        assert_eq!(runtime.stats().discarded, 1);
    }
}

#[test]
fn inactive_table_metadata_is_carried_evicted_and_reported() {
    let (tables_a, mut snapshot_a) = named_fixture("a", "batch-a", 10.0, 1024, 1);
    let (tables_b, mut snapshot_b) = named_fixture("b", "batch-b", 20.0, 2048, 1);
    snapshot_a.lineage.push(snapshot_b.lineage[0].clone());
    snapshot_b.lineage.insert(0, snapshot_a.lineage[0].clone());
    snapshot_b.sequence = 2;

    let cancelled = AtomicBool::new(false);
    let catalog = QueryCatalog::default();
    let deadline = Instant::now() + Duration::from_secs(5);
    let request_a = ResidentRequest::new(
        &tables_a,
        &snapshot_a,
        &options(),
        &catalog,
        &[],
        false,
        deadline,
        &cancelled,
    )
    .unwrap();
    let request_b = ResidentRequest::new(
        &tables_b,
        &snapshot_b,
        &options(),
        &catalog,
        &[],
        false,
        deadline,
        &cancelled,
    )
    .unwrap();
    let a_raw = snapshot_a.tables[0].batches[0].charged_bytes;
    let b_raw = snapshot_b.tables[0].batches[0].charged_bytes;
    let a_identity = 2 * ("a".len() + "batch-a".len()) + 256;
    let a_empty_coverage = "a".len() + 256;
    let initial_total = request_a.retained_bytes + a_raw;
    let old_incomplete_projection = request_b.retained_bytes + a_raw + b_raw;
    let complete_projection = old_incomplete_projection + a_identity + a_empty_coverage;
    let fresh_b_total = request_b.retained_bytes + b_raw;
    assert!(initial_total <= old_incomplete_projection);
    assert!(fresh_b_total < old_incomplete_projection);
    assert!(old_incomplete_projection < complete_projection);
    drop((request_a, request_b));

    let retained = QueryRuntime::new(1);
    assert_eq!(
        retained
            .execute_resident_with_limit(
                &tables_a,
                &snapshot_a,
                "SELECT count(*) AS n, sum(value) AS total FROM a",
                &options(),
                &catalog,
                &cancelled,
                complete_projection,
            )
            .unwrap(),
        json!([{"n": 1, "total": 10.0}])
    );
    assert_eq!(
        retained
            .execute_resident_with_limit(
                &tables_b,
                &snapshot_b,
                "SELECT count(*) AS n, sum(value) AS total FROM b",
                &options(),
                &catalog,
                &cancelled,
                complete_projection,
            )
            .unwrap(),
        json!([{"n": 1, "total": 20.0}])
    );
    let retained_stats = retained.stats();
    assert_eq!(retained_stats.reused, 1);
    assert_eq!(retained_stats.resident_idle_rows, 2);
    assert_eq!(retained_stats.resident_idle_bytes, complete_projection);

    let evicted = QueryRuntime::new(1);
    for (tables, snapshot, sql, expected) in [
        (&tables_a, &snapshot_a, "SELECT value FROM a", 10.0),
        (&tables_b, &snapshot_b, "SELECT value FROM b", 20.0),
    ] {
        assert_eq!(
            evicted
                .execute_resident_with_limit(
                    tables,
                    snapshot,
                    sql,
                    &options(),
                    &catalog,
                    &cancelled,
                    old_incomplete_projection,
                )
                .unwrap(),
            json!([{"value": expected}])
        );
    }
    let evicted_stats = evicted.stats();
    assert_eq!(evicted_stats.reused, 1);
    assert_eq!(evicted_stats.resident_idle_rows, 1);
    assert_eq!(evicted_stats.resident_idle_bytes, fresh_b_total);
    assert_eq!(evicted_stats.resident_raw_staged_rows, 2);
    let pool = evicted.pool.lock().unwrap();
    assert_eq!(std::fs::read_dir(&pool.idle[0].inputs).unwrap().count(), 0);
    assert!(
        !pool.idle[0]
            .resident
            .as_ref()
            .unwrap()
            .complete
            .contains_key("a")
    );
}

#[test]
fn unchanged_stamp_rewrite_charges_historical_identity_and_can_choose_fresh() {
    let (tables, snapshot) = named_fixture("metrics", "physical-old", 11.0, 1024, 7);
    let mut rewritten = snapshot.clone();
    rewritten.sequence = 2;
    rewritten.tables[0].batches[0].id = "physical-new".into();
    rewritten.lineage[0].ids = vec!["physical-new".into()];

    let cancelled = AtomicBool::new(false);
    let catalog = QueryCatalog::default();
    let deadline = Instant::now() + Duration::from_secs(5);
    let initial = ResidentRequest::new(
        &tables,
        &snapshot,
        &options(),
        &catalog,
        &[],
        false,
        deadline,
        &cancelled,
    )
    .unwrap();
    let current = ResidentRequest::new(
        &tables,
        &rewritten,
        &options(),
        &catalog,
        &[],
        false,
        deadline,
        &cancelled,
    )
    .unwrap();
    let raw = snapshot.tables[0].batches[0].charged_bytes;
    let initial_total = initial.retained_bytes + raw;
    let fresh_total = current.retained_bytes + raw;
    let historical_identity = 2 * ("metrics".len() + "physical-old".len()) + 256;
    let retained_total = fresh_total + historical_identity;
    assert!(initial_total <= fresh_total);
    assert!(fresh_total < retained_total);
    drop((initial, current));

    let retained = QueryRuntime::new(1);
    for snapshot in [&snapshot, &rewritten] {
        assert_eq!(
            retained
                .execute_resident_with_limit(
                    &tables,
                    snapshot,
                    "SELECT count(*) AS n, sum(value) AS total FROM metrics",
                    &options(),
                    &catalog,
                    &cancelled,
                    retained_total,
                )
                .unwrap(),
            json!([{"n": 1, "total": 11.0}])
        );
    }
    let retained_stats = retained.stats();
    assert_eq!(retained_stats.spawned, 1);
    assert_eq!(retained_stats.reused, 1);
    assert_eq!(retained_stats.resident_raw_staged_rows, 1);
    assert_eq!(retained_stats.resident_idle_rows, 1);
    assert_eq!(retained_stats.resident_idle_bytes, retained_total);

    let fresh = QueryRuntime::new(2);
    for snapshot in [&snapshot, &rewritten] {
        assert_eq!(
            fresh
                .execute_resident_with_limit(
                    &tables,
                    snapshot,
                    "SELECT count(*) AS n, sum(value) AS total FROM metrics",
                    &options(),
                    &catalog,
                    &cancelled,
                    fresh_total,
                )
                .unwrap(),
            json!([{"n": 1, "total": 11.0}])
        );
    }
    let fresh_stats = fresh.stats();
    assert_eq!((fresh_stats.spawned, fresh_stats.reused), (2, 0));
    assert_eq!(fresh_stats.resident_raw_staged_rows, 2);
    assert_eq!(fresh_stats.resident_idle_rows, 2);
    assert_eq!(fresh_stats.resident_idle_bytes, initial_total + fresh_total);
    let pool = fresh.pool.lock().unwrap();
    assert_eq!(pool.idle.len(), 2);
    assert!(
        pool.idle
            .iter()
            .all(|worker| std::fs::read_dir(&worker.inputs).unwrap().count() == 0)
    );
}

#[test]
fn fallback_preflight_rejects_malformed_scope_identity_and_cancellation() {
    let runtime = QueryRuntime::new(1);
    let (tables, mut snapshot) = fixture(1);
    let duplicate = snapshot.tables[0].batches[0].clone();
    snapshot.tables[0].batches.push(duplicate);
    let error = runtime
        .execute_resident_with_limit(
            &tables,
            &snapshot,
            "SELECT 1",
            &options(),
            &QueryCatalog::default(),
            &AtomicBool::new(false),
            1,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("duplicate resident batch identity")
    );

    let directory = tempfile::TempDir::new().unwrap();
    let unknown = directory.path().join("unknown.parquet");
    let (mut tables, mut snapshot) = fixture(1);
    tables[0].files.push(unknown.clone());
    snapshot.tables[0].files.push(ResidentFile {
        id: "unknown".into(),
        path: unknown,
        rows: 1,
        charged_bytes: 0,
        min_timestamp_us: 0,
        max_timestamp_us: 0,
    });
    snapshot.lineage[0].ids.push("unknown".into());
    snapshot.lineage.push(ResidentLineage {
        name: "unselected".into(),
        raw_stamp: 1,
        ids: vec!["duplicate".into(), "duplicate".into()],
    });
    let error = runtime
        .execute_resident_with_limit(
            &tables,
            &snapshot,
            "SELECT 1",
            &options(),
            &QueryCatalog::default(),
            &AtomicBool::new(false),
            MAX_QUERY_INPUT_BYTES,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("duplicate resident lineage identity"),
        "{error:#}"
    );

    let snapshot = ResidentSnapshot {
        namespace: "capacity-preflight".into(),
        sequence: 1,
        tables: vec![],
        lineage: vec![ResidentLineage {
            name: "unselected".into(),
            raw_stamp: 1,
            ids: vec![
                "padding".repeat(4096),
                "duplicate".into(),
                "duplicate".into(),
            ],
        }],
    };
    let error = runtime
        .execute_resident_with_limit(
            &[],
            &snapshot,
            "SELECT 1",
            &options(),
            &QueryCatalog::default(),
            &AtomicBool::new(false),
            1,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("duplicate resident lineage identity"),
        "{error:#}"
    );

    let (mut tables, snapshot) = fixture(1);
    tables[0].name = "other".into();
    let error = runtime
        .execute_resident_with_limit(
            &tables,
            &snapshot,
            "SELECT 1",
            &options(),
            &QueryCatalog::default(),
            &AtomicBool::new(false),
            1,
        )
        .unwrap_err();
    assert!(error.to_string().contains("resident table scope mismatch"));

    let (tables, snapshot) = fixture(1);
    let error = runtime
        .execute_resident_with_limit(
            &tables,
            &snapshot,
            "SELECT 1",
            &options(),
            &QueryCatalog::default(),
            &AtomicBool::new(true),
            1,
        )
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!((runtime.stats().active, runtime.stats().idle), (0, 0));
}

#[test]
fn unknown_cold_charge_does_not_mask_corrupt_selected_file() {
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("corrupt.parquet");
    std::fs::write(&path, b"not parquet").unwrap();
    let (mut tables, mut snapshot) = fixture(1);
    tables[0].files.push(path.clone());
    snapshot.tables[0].files.push(ResidentFile {
        id: "corrupt-file".into(),
        path,
        rows: 1,
        charged_bytes: 0,
        min_timestamp_us: 10,
        max_timestamp_us: 10,
    });
    snapshot.lineage[0].ids.push("corrupt-file".into());
    let runtime = QueryRuntime::new(1);
    assert!(
        runtime
            .execute_resident_with_catalog(
                &tables,
                &snapshot,
                "SELECT count(*) FROM metrics",
                &options(),
                &QueryCatalog::default(),
            )
            .is_err()
    );
    assert_eq!(runtime.stats().idle, 0);
}

#[test]
fn retained_large_snapshot_hit_delta_and_reusable_schema() {
    let runtime = QueryRuntime::new(1);
    let (tables, mut snapshot) = fixture(130);
    let catalog = QueryCatalog::default();
    let sql = "SELECT count(*) AS n, sum(value) AS total FROM metrics";
    assert_eq!(
        run(&runtime, &tables, &snapshot, sql, &catalog),
        oracle(&tables, &snapshot, sql, &catalog)
    );
    let first = runtime.stats();
    assert_eq!(
        (first.resident_full_loads, first.resident_raw_staged_rows),
        (1, 130)
    );
    assert!(first.resident_raw_staged_bytes > 0);
    let table_oid = run(
        &runtime,
        &tables,
        &snapshot,
        "SELECT table_oid FROM duckdb_tables() WHERE table_name = '__varve_input'",
        &catalog,
    );
    let pool = runtime.pool.lock().unwrap();
    let worker = &pool.idle[0];
    assert_eq!(std::fs::read_dir(&worker.inputs).unwrap().count(), 0);
    drop(pool);
    assert_eq!(
        run(&runtime, &tables, &snapshot, sql, &catalog),
        oracle(&tables, &snapshot, sql, &catalog)
    );
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT table_oid FROM duckdb_tables() WHERE table_name = '__varve_input'",
            &catalog
        ),
        table_oid
    );
    let repeated = runtime.stats();
    assert_eq!(
        repeated.resident_raw_staged_bytes,
        first.resident_raw_staged_bytes
    );
    assert_eq!(repeated.resident_raw_staged_rows, 130);
    assert_eq!(repeated.resident_hits, 3);
    let (_, delta) = fixture(2);
    let mut batch = delta.tables[0].batches[0].clone();
    batch.id = "delta".into();
    snapshot.tables[0].batches.push(batch);
    snapshot.lineage[0].ids.push("delta".into());
    snapshot.lineage[0].raw_stamp += 1;
    snapshot.sequence = 2;
    assert_eq!(
        run(&runtime, &tables, &snapshot, sql, &catalog),
        oracle(&tables, &snapshot, sql, &catalog)
    );
    let appended = runtime.stats();
    assert_eq!(appended.resident_delta_loads, 1);
    assert_eq!(appended.resident_raw_staged_rows, 132);
    assert_eq!(appended.resident_idle_rows, 132);
    assert!(
        appended.resident_raw_staged_bytes - first.resident_raw_staged_bytes
            < first.resident_raw_staged_bytes
    );
    assert!(appended.resident_idle_bytes <= MAX_QUERY_INPUT_BYTES);
}

#[test]
fn retained_inversion_and_namespace_rebuild_while_data_and_scope_reconcile() {
    let runtime = QueryRuntime::new(1);
    let (mut tables, mut snapshot) = fixture(3);
    let catalog = QueryCatalog::default();
    let sql = "SELECT count(*) AS n FROM metrics";
    run(&runtime, &tables, &snapshot, sql, &catalog);
    for case in 0..6 {
        match case {
            0 => snapshot.sequence = 0,
            1 => snapshot.namespace = "db2".into(),
            2 => {
                snapshot.tables[0].batches[0].rows =
                    crate::raw_memory::SharedRawRows::test_rows(vec![
                        snapshot.tables[0].batches[0].rows[0].clone(),
                    ]);
            }
            3 => tables[0].cutoff_us = Some(1),
            4 => {
                snapshot.tables[0].batches.clear();
                snapshot.lineage[0].ids.clear();
                snapshot.lineage[0].raw_stamp += 1;
            }
            5 => {
                tables[0].name = "renamed".into();
                snapshot.tables[0].name = "renamed".into();
                snapshot.lineage[0].name = "renamed".into();
            }
            _ => unreachable!(),
        }
        let sql = sql.replace("metrics", &tables[0].name);
        assert_eq!(
            run(&runtime, &tables, &snapshot, &sql, &catalog),
            oracle(&tables, &snapshot, &sql, &catalog)
        );
        let stats = runtime.stats();
        let expected_rebuilds = if case == 0 { 2 } else { 3 };
        let expected_invalidations = if case == 0 { 1 } else { 2 };
        assert_eq!(stats.resident_full_loads, expected_rebuilds);
        assert_eq!(stats.resident_invalidations, expected_invalidations);
    }
}

#[test]
fn retained_exact_numeric_unicode_catalog_and_independent_rollup_freshness() {
    let runtime = QueryRuntime::new(1);
    let (mut tables, mut snapshot) = fixture(136);
    let numbers = [
        -0.0,
        0.0,
        f64::from_bits(1),
        -f64::from_bits(1),
        f64::MIN_POSITIVE,
        f64::MAX,
        -f64::MAX,
        1.0 / 3.0,
    ];
    for (i, row) in snapshot.tables[0].batches[0]
        .rows
        .test_mut()
        .iter_mut()
        .enumerate()
    {
        row.row.value = numbers[i % numbers.len()];
        row.row.timestamp_us = if i % 2 == 0 { i64::MIN } else { i64::MAX };
        row.sequence = if i % 2 == 0 { 0 } else { u64::MAX };
        row.ordinal = u32::MAX - i as u32;
    }
    let mut catalog = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "metadata".into(),
            columns: vec![
                ("number".into(), "DOUBLE".into()),
                ("text".into(), "VARCHAR".into()),
            ],
            rows: vec![json!([-0.0, "quotes'雪\0"])],
        }],
        aggregates: vec![AggregateAlias {
            name: "minute \"雪".into(),
            source: "metrics".into(),
            width_us: 1,
        }],
    };
    tables[0].rollups =
        vec![RollupRow::from_row(1, &snapshot.tables[0].batches[0].rows[0]).unwrap()];
    let sql = "SELECT * FROM metrics ORDER BY ordinal DESC";
    let result = run(&runtime, &tables, &snapshot, sql, &catalog);
    assert_eq!(result, oracle(&tables, &snapshot, sql, &catalog));
    for (i, row) in result.as_array().unwrap().iter().enumerate() {
        assert_eq!(
            row["value"].as_f64().unwrap().to_bits(),
            numbers[i % numbers.len()].to_bits()
        );
    }
    let staged = runtime.stats().resident_raw_staged_bytes;
    for number in [-0.0, 0.0, -0.0] {
        catalog.relations[0].rows = vec![json!([number, "current\0雪"])];
        tables[0].rollups[0].sum = number;
        let sql = "SELECT sum, number, text FROM \"minute \"\"雪\" CROSS JOIN metadata()";
        let result = run(&runtime, &tables, &snapshot, sql, &catalog);
        assert_eq!(result, oracle(&tables, &snapshot, sql, &catalog));
        assert_eq!(
            result[0]["number"].as_f64().unwrap().to_bits(),
            number.to_bits()
        );
        assert_eq!(
            result[0]["sum"].as_f64().unwrap().to_bits(),
            number.to_bits()
        );
        assert_eq!(runtime.stats().resident_raw_staged_bytes, staged);
    }
    tables[0].cutoff_us = Some(i64::MAX);
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT count(*) AS n FROM metrics",
            &catalog
        )[0]["n"],
        68
    );
    let retained_raw_rows = snapshot.tables[0]
        .batches
        .iter()
        .map(|batch| batch.rows.len())
        .sum::<usize>();
    let raw = run(
        &runtime,
        &tables,
        &snapshot,
        "SELECT count(*) AS n, count(DISTINCT ordinal) AS distinct_n FROM __varve_input WHERE kind = 'hot'",
        &catalog,
    );
    assert_eq!(raw[0]["n"], retained_raw_rows);
    assert_eq!(raw[0]["distinct_n"], retained_raw_rows);
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT count FROM metrics__rollup",
            &catalog
        )[0]["count"],
        "1"
    );
    // Current empty nonraw sets replace old rows without evicting raw.
    tables[0].rollups.clear();
    catalog.relations[0].rows.clear();
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT * FROM metrics__rollup",
            &catalog
        ),
        json!([])
    );
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT * FROM metadata()",
            &catalog
        ),
        json!([])
    );
}

#[test]
fn retained_lifetime_charges_removed_raw_batches_but_not_unchanged_hits() {
    let runtime = QueryRuntime::new(1);
    let options = options();
    let cancelled = AtomicBool::new(false);
    let limit = 256 * 1024;
    let sql = "SELECT value FROM metrics";
    for index in 0..20 {
        let (tables, snapshot) = named_fixture(
            "metrics",
            &format!("batch-{index}"),
            index as f64,
            32 * 1024,
            index + 1,
        );
        let result = runtime
            .execute_resident_with_limit(
                &tables,
                &snapshot,
                sql,
                &options,
                &QueryCatalog::default(),
                &cancelled,
                limit,
            )
            .unwrap();
        assert_eq!(result, json!([{"value": index as f64}]));
        assert_eq!(runtime.stats().resident_idle_rows, 1);
        assert!(runtime.stats().resident_idle_bytes <= limit);
        assert!(runtime.stats().resident_idle_materialized_bytes <= limit);
    }
    let after_churn = runtime.stats();
    assert!(
        after_churn.spawned > 1,
        "removed input allocations stayed unbounded"
    );
    assert!(after_churn.discarded > 0);
    let (tables, snapshot) = named_fixture("metrics", "batch-19", 19.0, 32 * 1024, 20);
    // Warm this exact immutable memory identity before testing true no-op reuse.
    runtime
        .execute_resident_with_limit(
            &tables,
            &snapshot,
            sql,
            &options,
            &QueryCatalog::default(),
            &cancelled,
            limit,
        )
        .unwrap();
    let warm = runtime.stats();
    for _ in 0..4 {
        assert_eq!(
            runtime
                .execute_resident_with_limit(
                    &tables,
                    &snapshot,
                    sql,
                    &options,
                    &QueryCatalog::default(),
                    &cancelled,
                    limit
                )
                .unwrap(),
            json!([{"value": 19.0}])
        );
    }
    let hit = runtime.stats();
    assert_eq!(hit.spawned, warm.spawned);
    assert_eq!(
        hit.resident_idle_materialized_bytes,
        warm.resident_idle_materialized_bytes
    );
    assert_eq!(hit.resident_raw_staged_rows, warm.resident_raw_staged_rows);
    assert_eq!(
        hit.resident_dynamic_staged_bytes,
        warm.resident_dynamic_staged_bytes
    );
}

#[test]
fn retained_rollup_delta_matches_fresh_sql_through_duplicates_reordering_and_deletion() {
    let runtime = QueryRuntime::new(1);
    let (mut tables, snapshot) = fixture(16);
    let catalog = QueryCatalog::default();
    tables[0].rollups = snapshot.tables[0].batches[0]
        .rows
        .iter()
        .map(|row| RollupRow::from_row(1, row).unwrap())
        .collect();
    // Numeric sort keys normalize signed zero inside DuckDB; sort its text form
    // so this checks the adapter's exact stored DOUBLE rather than that SQL effect.
    let sql = "SELECT * FROM metrics__rollup ORDER BY bucket_us, CAST(sum AS VARCHAR)";
    assert_eq!(
        run(&runtime, &tables, &snapshot, sql, &catalog),
        oracle(&tables, &snapshot, sql, &catalog)
    );
    let initial = runtime.stats();
    tables[0].rollups[7].sum = -0.0;
    let changed = run(&runtime, &tables, &snapshot, sql, &catalog);
    assert_eq!(changed, oracle(&tables, &snapshot, sql, &catalog));
    assert_eq!(
        changed[7]["sum"].as_f64().unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
    let after = runtime.stats();
    assert_eq!(after.spawned, initial.spawned);
    assert_eq!(
        after.resident_raw_staged_rows,
        initial.resident_raw_staged_rows
    );
    assert!(after.resident_dynamic_staged_bytes - initial.resident_dynamic_staged_bytes < 1024);
    assert!(after.resident_idle_materialized_bytes > initial.resident_idle_materialized_bytes);
    assert!(
        after.resident_idle_materialized_bytes - initial.resident_idle_materialized_bytes < 4096
    );
    // Duplicate logical keys are legal at the adapter boundary. Position labels
    // must preserve both rows rather than coalescing them into a keyed UPSERT.
    let mut duplicate = tables[0].rollups[3].clone();
    duplicate.sum = 31.5;
    tables[0].rollups.insert(3, duplicate);
    assert_eq!(
        run(&runtime, &tables, &snapshot, sql, &catalog),
        oracle(&tables, &snapshot, sql, &catalog)
    );
    tables[0].rollups.reverse();
    assert_eq!(
        run(&runtime, &tables, &snapshot, sql, &catalog),
        oracle(&tables, &snapshot, sql, &catalog)
    );
    tables[0].rollups.truncate(4);
    assert_eq!(
        run(&runtime, &tables, &snapshot, sql, &catalog),
        oracle(&tables, &snapshot, sql, &catalog)
    );
    tables[0].rollups.clear();
    assert_eq!(run(&runtime, &tables, &snapshot, sql, &catalog), json!([]));
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT count(*) AS n FROM __varve_input WHERE kind = 'rollup_delete'",
            &catalog
        ),
        json!([{"n": 0}])
    );
    let empty = runtime.stats();
    assert_eq!(run(&runtime, &tables, &snapshot, sql, &catalog), json!([]));
    assert_eq!(
        runtime.stats().resident_idle_materialized_bytes,
        empty.resident_idle_materialized_bytes
    );
}

#[test]
fn retained_lifetime_charges_replaced_dynamic_relations() {
    let runtime = QueryRuntime::new(1);
    let (tables, snapshot) = fixture(0);
    let options = options();
    let cancelled = AtomicBool::new(false);
    let limit = 256 * 1024;
    let mut catalog = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "metadata".into(),
            columns: vec![("value".into(), "VARCHAR".into())],
            rows: vec![],
        }],
        aggregates: vec![],
    };
    for index in 0..24 {
        let value = format!("{index:04}{}", "x".repeat(16 * 1024));
        catalog.relations[0].rows = vec![json!([value])];
        assert_eq!(
            runtime
                .execute_resident_with_limit(
                    &tables,
                    &snapshot,
                    "SELECT value FROM metadata()",
                    &options,
                    &catalog,
                    &cancelled,
                    limit
                )
                .unwrap(),
            json!([{"value": value}])
        );
        assert!(runtime.stats().resident_idle_bytes <= limit);
        assert!(runtime.stats().resident_idle_materialized_bytes <= limit);
    }
    assert!(
        runtime.stats().spawned > 1,
        "replacement allocations were forgotten"
    );
    assert!(runtime.stats().discarded > 0);
    catalog.relations[0].rows.clear();
    assert_eq!(
        runtime
            .execute_resident_with_limit(
                &tables,
                &snapshot,
                "SELECT value FROM metadata()",
                &options,
                &catalog,
                &cancelled,
                limit
            )
            .unwrap(),
        json!([])
    );
}

#[test]
fn acknowledged_dynamic_counters_include_empty_replacement_with_zero_bytes() {
    let runtime = QueryRuntime::new(1);
    let (tables, snapshot) = fixture(0);
    let mut catalog = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "metadata".into(),
            columns: vec![("value".into(), "BIGINT".into())],
            rows: vec![json!([7])],
        }],
        aggregates: vec![],
    };
    let sql = "SELECT value FROM metadata()";
    assert_eq!(
        run(&runtime, &tables, &snapshot, sql, &catalog),
        json!([{"value": 7}])
    );
    let loaded = runtime.stats();
    assert_eq!(loaded.resident_dynamic_loads, 1);
    assert!(loaded.resident_dynamic_staged_bytes > 0);

    assert_eq!(
        run(&runtime, &tables, &snapshot, sql, &catalog),
        json!([{"value": 7}])
    );
    let hit = runtime.stats();
    assert_eq!(hit.resident_dynamic_loads, loaded.resident_dynamic_loads);
    assert_eq!(
        hit.resident_dynamic_staged_bytes,
        loaded.resident_dynamic_staged_bytes
    );

    catalog.relations[0].rows.clear();
    assert_eq!(run(&runtime, &tables, &snapshot, sql, &catalog), json!([]));
    let emptied = runtime.stats();
    assert_eq!(
        emptied.resident_dynamic_loads,
        hit.resident_dynamic_loads + 1
    );
    assert_eq!(
        emptied.resident_dynamic_staged_bytes,
        hit.resident_dynamic_staged_bytes
    );
}

#[test]
fn retained_fresh_only_sql_never_acquires_future_or_historical_raw() {
    let runtime = QueryRuntime::new(2);
    let (tables, mut snapshot) = fixture(130);
    snapshot.sequence = 2;
    run(
        &runtime,
        &tables,
        &snapshot,
        "SELECT count(*) FROM metrics",
        &QueryCatalog::default(),
    );
    let mut older = snapshot.clone();
    older.sequence = 1;
    older.tables[0].batches.clear();
    assert_eq!(
        run(
            &runtime,
            &tables,
            &older,
            "SELECT * FROM query('SELECT count(*) AS n FROM __varve_input WHERE kind = ''hot''')",
            &QueryCatalog::default()
        )[0]["n"],
        0
    );
    assert_eq!(
        (
            runtime.stats().spawned,
            runtime.stats().reused,
            runtime.stats().idle
        ),
        (2, 0, 1)
    );
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT count(*) AS n FROM metrics",
            &QueryCatalog::default()
        )[0]["n"],
        130
    );
}

#[test]
fn retained_total_not_delta_and_empty_batch_metadata_are_bounded() {
    assert!(charge(MAX_QUERY_INPUT_BYTES, 1).is_err());
    assert!(charge(0, usize::MAX).is_err());
    let runtime = QueryRuntime::new(1);
    let (tables, mut snapshot) = fixture(1);
    snapshot.tables[0].batches[0].charged_bytes = MAX_QUERY_INPUT_BYTES / 2;
    run(
        &runtime,
        &tables,
        &snapshot,
        "SELECT count(*) FROM metrics",
        &QueryCatalog::default(),
    );
    let mut delta = snapshot.tables[0].batches[0].clone();
    delta.id = "another".into();
    snapshot.tables[0].batches.push(delta);
    snapshot.lineage[0].ids.push("another".into());
    snapshot.lineage[0].raw_stamp += 1;
    let error = runtime
        .execute_resident_with_catalog(
            &tables,
            &snapshot,
            "SELECT count(*) FROM metrics",
            &options(),
            &QueryCatalog::default(),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("resident logical input"),
        "{error:#}"
    );
    assert_eq!(runtime.stats().idle, 0);
    assert_eq!(runtime.stats().resident_raw_staged_rows, 1);
    snapshot.tables[0].batches[0].rows = crate::raw_memory::SharedRawRows::test_rows(vec![]);
    snapshot.tables[0].batches.truncate(1);
    snapshot.lineage[0].ids.retain(|id| id != "another");
    snapshot.lineage[0].raw_stamp += 1;
    snapshot.tables[0].batches[0].charged_bytes = MAX_QUERY_INPUT_BYTES;
    assert!(
        runtime
            .execute_resident_with_catalog(
                &tables,
                &snapshot,
                "SELECT 1",
                &options(),
                &QueryCatalog::default()
            )
            .is_err()
    );
    snapshot.tables[0].batches[0].charged_bytes = 0;
    let duplicate = snapshot.tables[0].batches[0].clone();
    snapshot.tables[0].batches.push(duplicate);
    assert!(
        runtime
            .execute_resident_with_catalog(
                &tables,
                &snapshot,
                "SELECT 1",
                &options(),
                &QueryCatalog::default()
            )
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
}

#[test]
fn retained_remapped_negative_zero_and_invalid_nul_fail_closed() {
    let runtime = QueryRuntime::new(1);
    let (tables, mut snapshot) = fixture(1);
    let catalog = QueryCatalog::default();
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT value FROM metrics",
            &catalog
        )[0]["value"]
            .as_f64()
            .unwrap()
            .to_bits(),
        0.0_f64.to_bits()
    );
    let mut row = snapshot.tables[0].batches[0].rows[0].clone();
    row.row.value = -0.0;
    snapshot.tables[0].batches[0].rows =
        crate::raw_memory::SharedRawRows::test_rows(vec![row.clone()]);
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT value FROM metrics",
            &catalog
        )[0]["value"]
            .as_f64()
            .unwrap()
            .to_bits(),
        (-0.0_f64).to_bits()
    );
    assert_eq!(runtime.stats().resident_full_loads, 1);
    assert_eq!(runtime.stats().resident_delta_loads, 1);
    row.row
        .tags
        .insert("invalid".into(), "contains\0NUL".into());
    snapshot.tables[0].batches[0].rows = crate::raw_memory::SharedRawRows::test_rows(vec![row]);
    assert!(
        runtime
            .execute_resident_with_catalog(
                &tables,
                &snapshot,
                "SELECT value FROM metrics",
                &options(),
                &catalog
            )
            .is_err()
    );
    assert_eq!(runtime.stats().idle, 0);
}

#[test]
fn retained_errors_output_and_cancellation_discard_partial_state() {
    let runtime = QueryRuntime::new(1);
    let (tables, snapshot) = fixture(130);
    let catalog = QueryCatalog::default();
    for sql in [
        "SELECT error('expected')",
        "SELECT no_such_column FROM metrics",
    ] {
        assert!(
            runtime
                .execute_resident_with_catalog(&tables, &snapshot, sql, &options(), &catalog)
                .is_err()
        );
        assert_eq!(runtime.stats().idle, 0);
    }
    let mut bounded = options();
    bounded.max_output_bytes = 8;
    assert!(
        runtime
            .execute_resident_with_catalog(
                &tables,
                &snapshot,
                "SELECT * FROM metrics",
                &bounded,
                &catalog
            )
            .unwrap_err()
            .to_string()
            .contains("output exceeded")
    );
    assert_eq!(runtime.stats().idle, 0);
    assert!(
        runtime
            .execute_resident_with_catalog_cancellable(
                &tables,
                &snapshot,
                "SELECT 1",
                &options(),
                &catalog,
                &AtomicBool::new(true)
            )
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT count(*) AS n FROM metrics -- trailing comment",
            &catalog
        )[0]["n"],
        130
    );
    assert_eq!(runtime.stats().resident_invalidations, 3);
}
