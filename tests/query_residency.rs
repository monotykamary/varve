#![cfg(unix)]

use std::path::PathBuf;
use tempfile::TempDir;
use varve::{Config, Database, FlushPolicy, Row, TableConfig};

fn config(retained: bool) -> Config {
    let executable = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb");
    assert!(executable.is_file(), "actual .tools/duckdb is required");
    Config {
        flush_policy: FlushPolicy::PressureOnly,
        query_executable: executable,
        query_workers: 1,
        query_retained_inputs: retained,
        max_batch_rows: 256,
        ..Config::default()
    }
}

fn rows(start: i64, count: usize) -> Vec<Row> {
    (0..count)
        .map(|offset| Row {
            timestamp_us: start + offset as i64,
            tenant: "tenant".into(),
            series: "series".into(),
            value: offset as f64,
            tags: Default::default(),
        })
        .collect()
}

#[test]
fn engine_retained_queries_hit_then_stage_only_the_append() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config(true)).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write("metrics", "initial", rows(0, 129), 129).unwrap();

    let sql = "SELECT count(*) AS count FROM metrics";
    assert_eq!(db.query(sql).unwrap(), serde_json::json!([{"count": 129}]));
    let first = db.query_worker_stats();
    assert_eq!(first.resident_full_loads, 1);
    assert_eq!(first.resident_raw_staged_rows, 129);

    assert_eq!(db.query(sql).unwrap(), serde_json::json!([{"count": 129}]));
    let hit = db.query_worker_stats();
    assert_eq!(hit.resident_hits, 1);
    assert_eq!(hit.resident_raw_staged_rows, 129);

    db.write("metrics", "append", rows(129, 1), 130).unwrap();
    assert_eq!(db.query(sql).unwrap(), serde_json::json!([{"count": 130}]));
    let delta = db.query_worker_stats();
    assert_eq!(delta.resident_delta_loads, 1);
    assert_eq!(delta.resident_raw_staged_rows, 130);
}

#[test]
fn legacy_query_path_remains_the_default_fallback() {
    let temp = TempDir::new().unwrap();
    let cfg = config(false);
    assert!(!cfg.query_retained_inputs);
    let db = Database::open(temp.path(), cfg).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write("metrics", "rows", rows(0, 2), 2).unwrap();
    assert_eq!(
        db.query("SELECT sum(value) AS total FROM metrics").unwrap(),
        serde_json::json!([{"total": 1.0}])
    );
    let stats = db.query_worker_stats();
    assert_eq!(stats.resident_full_loads, 0);
    assert_eq!(stats.resident_raw_staged_rows, 0);
}

#[test]
fn checkpoint_uses_each_selected_segment_as_resident_or_file_once() {
    for cache_bytes in [0, 1024 * 1024] {
        let temp = TempDir::new().unwrap();
        let mut cfg = config(true);
        cfg.decoded_cache_bytes = cache_bytes;
        cfg.segment_rows = 2;
        let db = Database::open(temp.path(), cfg).unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        db.write("metrics", "rows", rows(0, 3), 3).unwrap();
        db.checkpoint().unwrap();
        assert_eq!(db.status().unwrap().hot_rows, 0);
        assert_eq!(
            db.query("SELECT count(*) AS count, sum(value) AS total FROM metrics")
                .unwrap(),
            serde_json::json!([{"count": 3, "total": 3.0}])
        );
        assert_eq!(
            db.query("SELECT timestamp_us FROM metrics ORDER BY timestamp_us")
                .unwrap(),
            serde_json::json!([
                {"timestamp_us": 0},
                {"timestamp_us": 1},
                {"timestamp_us": 2}
            ])
        );
    }
}

#[test]
fn partial_retention_invalidates_old_resident_segment_selection() {
    let temp = TempDir::new().unwrap();
    let mut cfg = config(true);
    cfg.segment_rows = 16;
    let db = Database::open(temp.path(), cfg).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            retention_us: Some(10),
            ..TableConfig::default()
        },
    )
    .unwrap();
    db.write(
        "metrics",
        "rows",
        vec![rows(1, 1)[0].clone(), rows(15, 1)[0].clone()],
        15,
    )
    .unwrap();
    db.checkpoint().unwrap();
    assert!(db.status().unwrap().decoded_cache_bytes > 0);
    assert_eq!(db.maintain(20).unwrap().expired_rows, 1);
    assert_eq!(
        db.query("SELECT timestamp_us FROM metrics ORDER BY timestamp_us")
            .unwrap(),
        serde_json::json!([{"timestamp_us": 15}])
    );
}

#[test]
fn compaction_replaces_old_resident_ids_without_duplicates() {
    let temp = TempDir::new().unwrap();
    let mut cfg = config(true);
    cfg.compact_min_segments = 2;
    cfg.segment_rows = 16;
    let db = Database::open(temp.path(), cfg).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    for timestamp in 1..=4 {
        db.write(
            "metrics",
            &format!("row{timestamp}"),
            rows(timestamp, 1),
            timestamp,
        )
        .unwrap();
        db.checkpoint().unwrap();
    }
    assert_eq!(db.status().unwrap().segments, 4);
    assert_eq!(db.compact().unwrap(), 1);
    assert_eq!(db.status().unwrap().segments, 1);
    db.write("metrics", "hot", rows(5, 1), 5).unwrap();
    assert_eq!(
        db.query("SELECT count(*) AS count FROM metrics").unwrap(),
        serde_json::json!([{"count": 5}])
    );
}

#[test]
fn reopened_wal_rebuilds_retained_hot_batches() {
    let temp = TempDir::new().unwrap();
    let cfg = config(true);
    {
        let db = Database::open(temp.path(), cfg.clone()).unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        db.write("metrics", "rows", rows(0, 3), 3).unwrap();
    }
    let reopened = Database::open(temp.path(), cfg).unwrap();
    assert_eq!(
        reopened
            .query("SELECT count(*) AS count FROM metrics")
            .unwrap(),
        serde_json::json!([{"count": 3}])
    );
}
