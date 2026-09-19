use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use varve::remote::FileStore;
use varve::{Config, Database, Row, TableConfig, WriteRequest};

fn config() -> Config {
    Config {
        segmented_journal: true,
        checkpoint_frozen_prefix: true,
        derived_pages: true,
        duckdb_library: Some(PathBuf::from(
            std::env::var_os("VARVE_DUCKDB_V2_LIBRARY").expect("pinned native library required"),
        )),
        // Every query must stay native, including cold data and recovery.
        query_executable: PathBuf::from("/varve-test-no-cli-fallback"),
        query_retained_inputs: false,
        decoded_cache_bytes: 0,
        query_timeout_ms: 10_000,
        segment_rows: 16,
        compact_min_segments: 2,
        flush_interval_us: 1,
        ship_interval_us: 1,
        ..Config::default()
    }
}

fn row(i: i64) -> Row {
    Row {
        timestamp_us: i,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value: i as f64,
        tags: BTreeMap::from([("host".into(), "雪'\"".into())]),
    }
}

fn table() -> TableConfig {
    TableConfig {
        shards: 2,
        window_us: 100,
        rollup_widths_us: vec![10],
        archive_after_us: Some(5),
        ..TableConfig::default()
    }
}

fn result(db: &Database) -> Value {
    db.query(
        "SELECT timestamp_us, value, tags, sequence, ordinal FROM metrics ORDER BY timestamp_us",
    )
    .unwrap()
}

fn assert_native(db: &Database) {
    let status = db.status().unwrap();
    assert!(status.segmented_journal);
    assert_eq!(status.native_query.unwrap()["version"], "v2.0.0-alpha41533");
    assert_eq!(status.active_snapshots, 0);
    assert_eq!(status.active_queries, 0);
    let stats = db.query_worker_stats();
    assert_eq!(stats.spawned, 0, "native queries must not spawn the CLI");
    assert_eq!(stats.resident_raw_staged_rows, 0);
    assert_eq!(stats.resident_raw_staged_bytes, 0);
}

#[test]
fn native_sql_survives_journal_checkpoint_archive_restore_and_incremental_views() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let store = Arc::new(FileStore::new(temp.path().join("objects")).unwrap());
    let cfg = config();
    let db = Database::open_with_remote(&root, cfg.clone(), Some(store.clone())).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.create_continuous_aggregate("ten_us", "metrics", 10)
        .unwrap();
    let receipts = db.write_group(
        (0..4)
            .map(|batch| WriteRequest {
                table: "metrics".into(),
                request_id: format!("batch-{batch}"),
                rows: (batch * 16..(batch + 1) * 16).map(row).collect(),
                now_us: 64,
            })
            .collect(),
    );
    let receipts: Vec<_> = receipts.into_iter().map(Result::unwrap).collect();
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.sequence == receipts[0].sequence
                && receipt.durability == "local_fsync")
    );
    let expected = result(&db);
    assert_eq!(expected.as_array().unwrap().len(), 64);
    let aggregates = db
        .query("SELECT bucket_us, count, sum FROM ten_us ORDER BY bucket_us")
        .unwrap();
    let report = db.maintain(120).unwrap();
    assert!(
        report.evicted_files > 0,
        "maintenance must actually archive files: {report:?}"
    );
    assert_eq!(db.status().unwrap().hot_rows, 0);
    assert!(db.status().unwrap().segments > 0);
    assert_eq!(result(&db), expected);
    assert_eq!(
        db.query("SELECT bucket_us, count, sum FROM ten_us ORDER BY bucket_us")
            .unwrap(),
        aggregates
    );
    assert_native(&db);
    let tail = db.write("metrics", "tail", vec![row(64)], 101).unwrap();
    assert_eq!(
        db.query("SELECT count(*) AS count, sum(value) AS total FROM metrics")
            .unwrap(),
        json!([{"count":65,"total":2080.0}])
    );
    let mixed = result(&db);
    db.ship().unwrap();
    let restored =
        Database::restore(temp.path().join("restored"), cfg.clone(), store.clone()).unwrap();
    assert_eq!(result(&restored), mixed);
    assert_eq!(
        restored
            .write("metrics", "tail", vec![row(64)], 102)
            .unwrap()
            .sequence,
        tail.sequence
    );
    assert!(
        restored
            .write("metrics", "batch-1", (16..32).map(row).collect(), 102)
            .unwrap()
            .duplicate
    );
    assert_native(&restored);
    restored.checkpoint().unwrap();
    drop(restored);
    let reopened =
        Database::open_with_remote(temp.path().join("restored"), cfg, Some(store)).unwrap();
    assert_eq!(result(&reopened), mixed);
    assert_native(&reopened);
}

#[test]
fn native_checkpoints_preserve_budgeted_residency_without_cli_retained_inputs() {
    for frozen_prefix in [false, true] {
        let temp = TempDir::new().unwrap();
        let cfg = Config {
            checkpoint_frozen_prefix: frozen_prefix,
            decoded_cache_bytes: 128 * 1024,
            ..config()
        };
        let db = Database::open(temp.path(), cfg.clone()).unwrap();
        db.create_table("metrics", table()).unwrap();
        db.write("metrics", "first", (0..64).map(row).collect(), 64)
            .unwrap();
        let expected = result(&db);
        db.checkpoint().unwrap();
        let status = db.status().unwrap();
        assert_eq!(status.hot_rows, 0);
        assert!(status.decoded_cache_bytes > 0);
        assert!(status.decoded_cache_bytes <= cfg.decoded_cache_bytes);
        assert_eq!(result(&db), expected);
        db.write("metrics", "tail", (64..80).map(row).collect(), 80)
            .unwrap();
        assert_eq!(
            db.query("SELECT count(*) AS count, sum(value) AS total FROM metrics")
                .unwrap(),
            json!([{"count":80,"total":3160.0}])
        );
        assert_native(&db);
    }
}

#[test]
fn active_native_query_cancellation_releases_database_pins_after_join() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "seed", (0..64).map(row).collect(), 64)
        .unwrap();
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker = db.clone();
    let signal = Arc::clone(&cancelled);
    let handle = thread::spawn(move || {
        worker.query_cancellable(
            "SELECT sum(m.value + r.i) FROM metrics m CROSS JOIN range(1000000000) r(i)",
            &signal,
        )
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    while db.status().unwrap().active_snapshots == 0 {
        assert!(
            !handle.is_finished(),
            "query completed before a snapshot was pinned"
        );
        assert!(Instant::now() < deadline, "query did not pin its snapshot");
        thread::sleep(Duration::from_millis(2));
    }
    // Give the backend time to enter native execution, not just preflight.
    thread::sleep(Duration::from_millis(50));
    assert!(!handle.is_finished());
    assert_eq!(
        db.query_worker_stats().active,
        1,
        "native activity must be observable"
    );
    cancelled.store(true, Ordering::Release);
    let error = handle.join().unwrap().unwrap_err();
    assert!(format!("{error:#}").contains("cancelled"), "{error:#}");
    assert!(Instant::now() < deadline);
    assert_native(&db);
    assert_eq!(
        db.query("SELECT count(*) AS count FROM metrics").unwrap(),
        json!([{"count":64}])
    );
    db.checkpoint().unwrap();
    assert_native(&db);
}
