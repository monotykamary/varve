use std::sync::Arc;
use tempfile::TempDir;
use varve::remote::FileStore;
use varve::{Config, Database, FlushPolicy, Row, TableConfig, WriteRequest};

fn row(timestamp_us: i64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "series".into(),
        value: timestamp_us as f64,
        tags: Default::default(),
    }
}
fn config() -> Config {
    Config {
        flush_policy: FlushPolicy::PressureOnly,
        ..Config::default()
    }
}
fn table() -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 100,
        rollup_widths_us: vec![10],
        ..TableConfig::default()
    }
}

#[test]
fn pressure_policy_is_explicit_and_legacy_default_is_unchanged() {
    let legacy: Config = serde_json::from_str("{}").unwrap();
    assert_eq!(legacy.flush_policy, FlushPolicy::AgeOrPressure);
    assert_eq!(legacy.flush_interval_us, 5_000_000);
    let pressure: Config = serde_json::from_str(r#"{"flush_policy":"pressure_only"}"#).unwrap();
    pressure.validate().unwrap();
    assert_eq!(pressure.flush_policy, FlushPolicy::PressureOnly);
    assert_eq!(pressure.hot_max_rows, legacy.hot_max_rows);
    assert_eq!(pressure.hot_max_bytes, legacy.hot_max_bytes);
    assert_eq!(pressure.wal_max_bytes, legacy.wal_max_bytes);
    assert!(serde_json::from_str::<Config>(r#"{"flush_policy":"eager"}"#).is_err());
}

#[test]
fn idle_ticks_preserve_hot_data_but_pressure_and_explicit_checkpoint_still_work() {
    for derived_pages in [false, true] {
        let temp = TempDir::new().unwrap();
        let cfg = Config {
            derived_pages,
            hot_max_rows: 2,
            max_batch_rows: 2,
            ..config()
        };
        let db = Database::open(temp.path(), cfg.clone()).unwrap();
        db.create_table("metrics", table()).unwrap();
        assert_eq!(
            db.write("metrics", "first", vec![row(1)], 100)
                .unwrap()
                .durability,
            "local_fsync"
        );
        let before = db.status().unwrap();
        for now in [5_000_100, 50_000_100, i64::MAX] {
            assert!(!db.maintain(now).unwrap().flushed);
            let after = db.status().unwrap();
            assert_eq!(after.hot_rows, 1);
            assert_eq!(after.segments, 0);
            assert_eq!(after.sequence, before.sequence);
            assert_eq!(after.checkpoint_sequence, before.checkpoint_sequence);
            assert_eq!(after.wal_bytes, before.wal_bytes);
        }
        let results = db.write_group(
            (2..=3)
                .map(|timestamp| WriteRequest {
                    table: "metrics".into(),
                    request_id: format!("row{timestamp}"),
                    rows: vec![row(timestamp)],
                    now_us: 101,
                })
                .collect(),
        );
        assert!(results.iter().all(Result::is_ok));
        let pressure = db.status().unwrap();
        assert_eq!(pressure.hot_rows, 2);
        assert!(pressure.segments > 0);
        assert!(pressure.checkpoint_sequence >= before.sequence);
        assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
        db.checkpoint().unwrap();
        assert_eq!(db.status().unwrap().hot_rows, 0);
        let rollups = db.rollups("metrics").unwrap();
        drop(db);
        let reopened = Database::open(temp.path(), cfg).unwrap();
        assert_eq!(
            reopened
                .scan("metrics", None, None, None, None)
                .unwrap()
                .len(),
            3
        );
        assert_eq!(reopened.rollups("metrics").unwrap(), rollups);
    }
}

#[test]
fn pressure_policy_does_not_disable_retention() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            retention_us: Some(10),
            ..table()
        },
    )
    .unwrap();
    db.write("metrics", "rows", vec![row(1), row(15)], 15)
        .unwrap();
    let report = db.maintain(20).unwrap();
    assert_eq!(report.expired_rows, 1);
    let rows = db.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row.timestamp_us, 15);
    assert_eq!(
        db.rollups("metrics")
            .unwrap()
            .iter()
            .map(|r| r.count)
            .sum::<u64>(),
        2
    );
    drop(db);
    let reopened = Database::open(temp.path(), config()).unwrap();
    assert_eq!(
        reopened
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn pressure_policy_keeps_durable_timed_receipt_floors() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            idempotency_window_us: Some(50),
            ..table()
        },
    )
    .unwrap();
    db.write("metrics", "v1:100:first", vec![row(100)], 100)
        .unwrap();
    db.maintain(200).unwrap();
    assert!(
        db.write("metrics", "v1:100:first", vec![row(100)], 200)
            .is_err()
    );
    drop(db);
    let reopened = Database::open(temp.path(), config()).unwrap();
    assert!(
        reopened
            .write("metrics", "v1:100:first", vec![row(100)], 200)
            .is_err()
    );
    assert_eq!(
        reopened
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn asynchronous_wal_shipping_and_restore_do_not_require_columnarization() {
    for query_retained_inputs in [false, true] {
        for derived_pages in [false, true] {
            let temp = TempDir::new().unwrap();
            let store = Arc::new(FileStore::new(temp.path().join("objects")).unwrap());
            let cfg = Config {
                query_retained_inputs,
                derived_pages,
                ..config()
            };
            let db = Database::open_with_remote(
                temp.path().join("local"),
                cfg.clone(),
                Some(store.clone()),
            )
            .unwrap();
            db.create_table("metrics", table()).unwrap();
            db.write("metrics", "rows", vec![row(1), row(2)], 100)
                .unwrap();
            let before = db.status().unwrap();
            assert_eq!(db.ship().unwrap(), before.sequence);
            let report = db.maintain(50_000_100).unwrap();
            assert!(!report.flushed);
            let after = db.status().unwrap();
            assert_eq!(after.hot_rows, 2);
            assert_eq!(after.segments, 0);
            assert_eq!(after.checkpoint_sequence, before.checkpoint_sequence);
            let expected = db.rollups("metrics").unwrap();
            drop(db);
            let restored = Database::restore(temp.path().join("restored"), cfg, store).unwrap();
            assert_eq!(restored.status().unwrap().hot_rows, 2);
            assert_eq!(restored.status().unwrap().segments, 0);
            assert_eq!(
                restored
                    .scan("metrics", None, None, None, None)
                    .unwrap()
                    .len(),
                2
            );
            assert_eq!(restored.rollups("metrics").unwrap(), expected);
            for _ in 0..2 {
                assert_eq!(
                    restored
                        .query("SELECT count(*) AS count, sum(value) AS total FROM metrics")
                        .unwrap(),
                    serde_json::json!([{"count": 2, "total": 3.0}])
                );
            }
            assert_eq!(
                restored.query_worker_stats().resident_raw_staged_rows,
                if query_retained_inputs { 2 } else { 0 }
            );
            restored
                .write("metrics", "tail", vec![row(3)], 101)
                .unwrap();
            assert_eq!(
                restored
                    .query("SELECT count(*) AS count, sum(value) AS total FROM metrics")
                    .unwrap(),
                serde_json::json!([{"count": 3, "total": 6.0}])
            );
            assert_eq!(
                restored.query_worker_stats().resident_raw_staged_rows,
                if query_retained_inputs { 3 } else { 0 }
            );
        }
    }
}

#[test]
fn retained_checkpoint_offers_exact_rows_to_bounded_decoded_lru() {
    let row_bytes = row(1).estimated_bytes();
    for cache_bytes in [0, row_bytes, row_bytes * 4] {
        let temp = TempDir::new().unwrap();
        let cfg = Config {
            query_retained_inputs: true,
            decoded_cache_bytes: cache_bytes,
            segment_rows: 1,
            ..config()
        };
        let db = Database::open(temp.path(), cfg).unwrap();
        db.create_table("metrics", table()).unwrap();
        db.write("metrics", "first", vec![row(1)], 1).unwrap();
        db.checkpoint().unwrap();
        let first = db.status().unwrap();
        assert_eq!(first.hot_rows, 0);
        assert_eq!(first.decoded_cache_bytes, cache_bytes.min(row_bytes));

        db.write("metrics", "second", vec![row(101)], 101).unwrap();
        db.checkpoint().unwrap();
        let second = db.status().unwrap();
        assert_eq!(second.hot_rows, 0);
        assert_eq!(second.decoded_cache_bytes, cache_bytes.min(row_bytes * 2));
        assert_eq!(
            db.scan("metrics", None, None, None, None)
                .unwrap()
                .iter()
                .map(|stored| stored.row.timestamp_us)
                .collect::<Vec<_>>(),
            vec![1, 101]
        );
    }
}
