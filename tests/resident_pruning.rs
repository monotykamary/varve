#![cfg(unix)]

use serde_json::Value;
use std::path::PathBuf;
use tempfile::TempDir;
use varve::model::StoredRow;
use varve::query::{QueryOptions, QueryTable, execute};
use varve::{Config, Database, FlushPolicy, Row, TableConfig};

fn config() -> Config {
    let executable = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb");
    assert!(executable.is_file(), "actual .tools/duckdb is required");
    Config {
        flush_policy: FlushPolicy::PressureOnly,
        query_executable: executable,
        query_workers: 1,
        query_retained_inputs: true,
        max_batch_rows: 256,
        segment_rows: 256,
        ..Config::default()
    }
}

fn table() -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 10_000,
        rollup_widths_us: vec![],
        ..TableConfig::default()
    }
}

fn row(timestamp_us: i64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "series".into(),
        value: 1.0,
        tags: Default::default(),
    }
}

fn append(db: &Database, all: &mut Vec<StoredRow>, timestamps: impl IntoIterator<Item = i64>) {
    let rows: Vec<_> = timestamps.into_iter().map(row).collect();
    db.write(
        "metrics",
        &format!("batch-{}", all.len()),
        rows.clone(),
        20_000,
    )
    .unwrap();
    all.extend(rows.into_iter().map(|row| StoredRow {
        row,
        sequence: 1,
        ordinal: 0,
    }));
}

// Independent fresh-process oracle exposes every row, without planner pruning.
fn check(db: &Database, all: &[StoredRow], sql: &str) -> Value {
    let expected = execute(
        &[QueryTable {
            name: "metrics".into(),
            hot: all.to_vec(),
            files: vec![],
            rollups: vec![],
            cutoff_us: None,
        }],
        sql,
        &QueryOptions {
            executable: config().query_executable,
            ..QueryOptions::default()
        },
    )
    .unwrap();
    let actual = db.query(sql).unwrap();
    assert_eq!(actual, expected, "{sql}");
    actual
}

const NARROW: &str = "SELECT count(*) AS n, sum(value) AS total FROM metrics \
                      WHERE timestamp_us >= 1000 AND timestamp_us < 1129";
const BROAD: &str = "SELECT count(*) AS n, sum(value) AS total FROM metrics";

#[test]
fn disjoint_appends_stage_zero_rows_and_scope_changes_match_fresh_duckdb() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    let mut all = vec![];
    append(&db, &mut all, 1000..1129);
    let initial = check(&db, &all, NARROW);
    assert_eq!(initial[0]["n"], 129);
    let warm = db.query_worker_stats();
    assert_eq!(warm.resident_raw_staged_rows, 129);
    for timestamps in [vec![-100, -99], vec![20_000], vec![999]] {
        append(&db, &mut all, timestamps);
        assert_eq!(check(&db, &all, NARROW), initial);
        let stats = db.query_worker_stats();
        assert_eq!(
            stats.resident_raw_staged_rows,
            warm.resident_raw_staged_rows
        );
        assert_eq!(
            stats.resident_raw_staged_bytes,
            warm.resident_raw_staged_bytes
        );
        assert_eq!(stats.resident_full_loads, warm.resident_full_loads);
        assert_eq!(stats.resident_delta_loads, warm.resident_delta_loads);
    }
    assert_eq!(db.query_worker_stats().resident_hits, 3);
    assert_eq!(check(&db, &all, BROAD)[0]["n"], 133);
    assert_eq!(db.query_worker_stats().resident_raw_staged_rows, 133);
    assert_eq!(db.query_worker_stats().resident_delta_loads, 1);
    assert_eq!(check(&db, &all, NARROW), initial);
    // A narrower scope keeps bounded inactive data and does not rebuild it.
    let narrow = db.query_worker_stats();
    assert_eq!(narrow.resident_raw_staged_rows, 133);
    assert_eq!(narrow.resident_full_loads, 1);
    assert_eq!(narrow.resident_idle_rows, 133);
    assert!(narrow.resident_idle_bytes <= 128 * 1024 * 1024);
}

#[test]
fn partial_overlap_stages_whole_batches_not_filtered_copies() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    let mut all = vec![];
    append(&db, &mut all, [5000, 1000, -100]);
    assert_eq!(check(&db, &all, NARROW)[0]["n"], 1);
    assert_eq!(db.query_worker_stats().resident_raw_staged_rows, 3);
    append(&db, &mut all, [1129, 1050, 999]);
    assert_eq!(check(&db, &all, NARROW)[0]["n"], 2);
    assert_eq!(db.query_worker_stats().resident_raw_staged_rows, 6);
    assert_eq!(db.query_worker_stats().resident_delta_loads, 1);
}

#[test]
fn nested_window_and_unsupported_predicates_preserve_planner_proof_scope() {
    for (sql, staged) in [
        (
            "SELECT count(*) AS n FROM (SELECT *, row_number() OVER (ORDER BY timestamp_us) AS rn FROM metrics WHERE timestamp_us >= 1000 AND timestamp_us < 1129) nested WHERE rn <= 3",
            129,
        ),
        (
            "SELECT count(*) AS n FROM (SELECT *, row_number() OVER (ORDER BY timestamp_us) AS rn FROM metrics) nested WHERE timestamp_us >= 1000 AND timestamp_us < 1129 AND rn <= 3",
            131,
        ),
        (
            "SELECT count(*) AS n FROM metrics a JOIN metrics b USING (timestamp_us) WHERE a.timestamp_us >= 1000 AND a.timestamp_us < 1129",
            131,
        ),
        (
            "SELECT count(*) AS n FROM metrics WHERE timestamp_us >= (SELECT 1000) AND timestamp_us < 1129",
            131,
        ),
        (
            "SELECT count(*) AS n FROM metrics WHERE timestamp_us + 0 BETWEEN 1000 AND 1128",
            131,
        ),
    ] {
        let temp = TempDir::new().unwrap();
        let db = Database::open(temp.path(), config()).unwrap();
        db.create_table("metrics", table()).unwrap();
        let mut all = vec![];
        append(&db, &mut all, 1000..1129);
        append(&db, &mut all, [-1]);
        append(&db, &mut all, [20_000]);
        check(&db, &all, sql);
        assert_eq!(
            db.query_worker_stats().resident_raw_staged_rows,
            staged,
            "{sql}"
        );
    }
}

#[test]
fn inclusive_exclusive_negative_and_extreme_bounds_match_fresh_duckdb() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            window_us: 1,
            ..table()
        },
    )
    .unwrap();
    let mut all = vec![];
    for timestamp in [
        i64::MIN,
        i64::MIN + 1,
        -10,
        -1,
        0,
        1,
        i64::MAX - 1,
        i64::MAX,
    ] {
        append(&db, &mut all, [timestamp]);
    }
    for (predicate, count) in [
        ("timestamp_us = -9223372036854775808", 1),
        ("timestamp_us < -9223372036854775808", 0),
        ("timestamp_us > 9223372036854775807", 0),
        ("timestamp_us = 9223372036854775807", 1),
        ("timestamp_us >= -10 AND timestamp_us < 0", 2),
        ("timestamp_us > -10 AND timestamp_us <= 0", 2),
        ("timestamp_us BETWEEN -10 AND 0", 3),
        ("timestamp_us >= 0 AND timestamp_us < 0", 0),
        ("timestamp_us <= 9223372036854775807", 8),
        ("timestamp_us >= -9223372036854775808", 8),
    ] {
        let sql = format!("SELECT count(*) AS n FROM metrics WHERE {predicate}");
        assert_eq!(check(&db, &all, &sql)[0]["n"], count);
    }
}

#[test]
fn replay_checkpoint_and_decoded_cache_keep_bounds_and_exact_coverage() {
    for cache_bytes in [0, row(0).estimated_bytes(), 1024 * 1024] {
        let temp = TempDir::new().unwrap();
        let cfg = Config {
            decoded_cache_bytes: cache_bytes,
            ..config()
        };
        let mut all = vec![];
        {
            let db = Database::open(temp.path(), cfg.clone()).unwrap();
            db.create_table("metrics", table()).unwrap();
            append(&db, &mut all, 1000..1129);
            append(&db, &mut all, [-100, -99]);
            append(&db, &mut all, [20_000]);
        }
        {
            let db = Database::open(temp.path(), cfg.clone()).unwrap();
            assert_eq!(check(&db, &all, NARROW)[0]["n"], 129);
            assert_eq!(db.query_worker_stats().resident_raw_staged_rows, 129);
            db.checkpoint().unwrap();
            assert_eq!(db.status().unwrap().hot_rows, 0);
            assert!(db.status().unwrap().decoded_cache_bytes <= cache_bytes);
            check(&db, &all, NARROW);
            // Partial coverage cannot certify a rewritten identity. Both a file
            // and its decoded form are imported once, not direct-scanned forever.
            assert_eq!(db.query_worker_stats().resident_raw_staged_rows, 258);
            assert_eq!(check(&db, &all, BROAD)[0]["n"], 132);
            check(&db, &all, NARROW);
        }
        let db = Database::open(temp.path(), cfg).unwrap();
        check(&db, &all, NARROW);
        assert_eq!(db.query_worker_stats().resident_raw_staged_rows, 129);
        let cold = db.query_worker_stats();
        // Entering decoded memory does not change a verified segment's identity.
        assert_eq!(
            db.scan("metrics", None, None, None, None).unwrap().len(),
            132
        );
        check(&db, &all, NARROW);
        let decoded = db.query_worker_stats();
        assert_eq!(
            decoded.resident_raw_staged_rows,
            cold.resident_raw_staged_rows
        );
        assert_eq!(
            decoded.resident_raw_staged_bytes,
            cold.resident_raw_staged_bytes
        );
        assert_eq!(decoded.spawned, cold.spawned);
        assert_eq!(check(&db, &all, BROAD)[0]["n"], 132);
        assert!(db.status().unwrap().decoded_cache_bytes <= cache_bytes);
    }
}
