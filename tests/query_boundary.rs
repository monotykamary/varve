#![cfg(unix)]

use std::path::PathBuf;
use tempfile::TempDir;
use varve::{Config, Database, FlushPolicy, Row, TableConfig};

const RESIDENT_LIMIT: usize = 128 * 1024 * 1024;

fn config() -> Config {
    let executable = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb");
    assert!(executable.is_file(), "actual .tools/duckdb is required");
    Config {
        flush_policy: FlushPolicy::PressureOnly,
        query_executable: executable,
        query_workers: 1,
        query_retained_inputs: true,
        decoded_cache_bytes: 0,
        segment_rows: 256,
        max_batch_rows: 256,
        ..Config::default()
    }
}

fn rows(start: i64, count: usize) -> Vec<Row> {
    (0..count)
        .map(|offset| {
            let timestamp_us = start + offset as i64;
            Row {
                timestamp_us,
                tenant: "tenant".into(),
                series: if timestamp_us % 2 == 0 {
                    "even".into()
                } else {
                    "odd".into()
                },
                value: timestamp_us as f64,
                tags: Default::default(),
            }
        })
        .collect()
}

#[test]
fn full_selective_checkpoint_append_reuses_complete_logical_coverage() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write("metrics", "initial", rows(0, 130), 130).unwrap();

    let full = "SELECT count(*) AS n, sum(value) AS total FROM metrics";
    let selective = "SELECT count(*) AS n, sum(value) AS total FROM metrics WHERE tenant = 'tenant' AND series = 'even'";
    assert_eq!(
        db.query(full).unwrap(),
        serde_json::json!([{"n": 130, "total": 8385.0}])
    );
    let initial = db.query_worker_stats();
    assert_eq!(initial.resident_full_loads, 1);
    assert_eq!(initial.resident_raw_staged_rows, 130);
    assert_eq!(initial.spawned, 1);

    assert_eq!(
        db.query(selective).unwrap(),
        serde_json::json!([{"n": 65, "total": 4160.0}])
    );
    let narrowed = db.query_worker_stats();
    assert_eq!(narrowed.resident_full_loads, 1);
    assert_eq!(narrowed.resident_raw_staged_rows, 130);
    assert_eq!(narrowed.spawned, 1);

    db.checkpoint().unwrap();
    assert_eq!(db.status().unwrap().hot_rows, 0);
    assert_eq!(
        db.query(full).unwrap(),
        serde_json::json!([{"n": 130, "total": 8385.0}])
    );
    let checkpointed = db.query_worker_stats();
    assert_eq!(checkpointed.resident_full_loads, 1);
    assert_eq!(checkpointed.resident_raw_staged_rows, 130);
    assert_eq!(checkpointed.spawned, 1);
    assert_eq!(checkpointed.resident_invalidations, 0);

    db.write("metrics", "append", rows(130, 1), 131).unwrap();
    assert_eq!(
        db.query(full).unwrap(),
        serde_json::json!([{"n": 131, "total": 8515.0}])
    );
    let appended = db.query_worker_stats();
    assert_eq!(appended.resident_full_loads, 1);
    assert_eq!(appended.resident_delta_loads, 1);
    assert_eq!(appended.resident_raw_staged_rows, 131);
    assert_eq!(appended.spawned, 1);
    assert_eq!(appended.resident_invalidations, 0);
    assert!(appended.resident_idle_bytes <= RESIDENT_LIMIT);

    assert_eq!(
        db.query(selective).unwrap(),
        serde_json::json!([{"n": 66, "total": 4290.0}])
    );
    let final_stats = db.query_worker_stats();
    assert_eq!(final_stats.resident_full_loads, 1);
    assert_eq!(final_stats.resident_raw_staged_rows, 131);
    assert_eq!(final_stats.spawned, 1);
    assert_eq!(final_stats.resident_invalidations, 0);
    assert!(final_stats.reused >= 4);
    assert!(final_stats.resident_idle_rows >= 131);
    assert!(final_stats.resident_idle_bytes <= RESIDENT_LIMIT);
}
