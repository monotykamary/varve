#![cfg(unix)]

use serde_json::Value;
use std::path::PathBuf;
use tempfile::TempDir;
use varve::query::{
    AggregateAlias, QueryCatalog, QueryOptions, QueryTable, execute, execute_with_catalog,
};
use varve::{Config, Database, FlushPolicy, Row, TableConfig};

fn executable() -> PathBuf {
    let executable = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb");
    assert!(executable.is_file(), "actual .tools/duckdb is required");
    executable
}

fn config(retained: bool, workers: usize) -> Config {
    Config {
        flush_policy: FlushPolicy::PressureOnly,
        query_executable: executable(),
        query_workers: workers,
        query_retained_inputs: retained,
        max_batch_rows: 256,
        ..Config::default()
    }
}

fn options() -> QueryOptions {
    QueryOptions {
        executable: executable(),
        timeout_ms: 5_000,
        ..QueryOptions::default()
    }
}

fn row(timestamp_us: i64, tenant: &str, series: &str, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: tenant.into(),
        series: series.into(),
        value,
        tags: Default::default(),
    }
}

fn fresh_raw(db: &Database, sql: &str) -> Value {
    execute(
        &[QueryTable {
            name: "metrics".into(),
            hot: db.scan("metrics", None, None, None, None).unwrap(),
            files: vec![],
            rollups: vec![],
            cutoff_us: None,
        }],
        sql,
        &options(),
    )
    .unwrap()
}

fn fresh_rollup(db: &Database, sql: &str) -> Value {
    execute_with_catalog(
        &[QueryTable {
            name: "metrics".into(),
            hot: vec![],
            files: vec![],
            rollups: db.rollups("metrics").unwrap(),
            cutoff_us: None,
        }],
        sql,
        &options(),
        &QueryCatalog {
            relations: vec![],
            aggregates: vec![AggregateAlias {
                name: "metrics_ten".into(),
                source: "metrics".into(),
                width_us: 10,
            }],
        },
    )
    .unwrap()
}

fn unsigned(value: &Value) -> u64 {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .expect("unsigned DuckDB JSON value")
}

fn current_status(db: &Database) -> Value {
    let status = db.status().unwrap();
    let result = db.query("SELECT * FROM varve_status()").unwrap();
    let current_aggregates = db
        .query("SELECT count(*)::UBIGINT AS n FROM varve_continuous_aggregates()")
        .unwrap();
    let current_jobs = db
        .query("SELECT count(*)::UBIGINT AS n FROM varve_jobs()")
        .unwrap();
    let row = &result[0];
    assert_eq!(
        row["database_id"].as_str(),
        Some(status.database_id.as_str())
    );
    assert_eq!(unsigned(&row["sequence"]), status.sequence);
    assert_eq!(
        unsigned(&row["checkpoint_sequence"]),
        status.checkpoint_sequence
    );
    assert_eq!(unsigned(&row["remote_sequence"]), status.remote_sequence);
    assert_eq!(
        unsigned(&row["unshipped_batches"]),
        status.unshipped_batches
    );
    assert_eq!(unsigned(&row["hot_rows"]), status.hot_rows as u64);
    assert_eq!(unsigned(&row["hot_bytes"]), status.hot_bytes as u64);
    assert_eq!(unsigned(&row["wal_bytes"]), status.wal_bytes);
    assert_eq!(
        unsigned(&row["metadata_bytes"]),
        status.metadata_bytes as u64
    );
    assert_eq!(unsigned(&row["tables"]), status.tables as u64);
    assert_eq!(
        unsigned(&row["continuous_aggregates"]),
        unsigned(&current_aggregates[0]["n"])
    );
    assert_eq!(unsigned(&row["jobs"]), unsigned(&current_jobs[0]["n"]));
    assert_eq!(row["healthy"].as_bool(), Some(status.fenced.is_none()));
    result
}

#[test]
fn retained_storage_proof_omits_only_inaccessible_catalog_rows() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config(true, 2)).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write(
        "metrics",
        "selected",
        vec![
            row(10, "tenant", "series", 1.0),
            row(11, "tenant", "series", 2.0),
        ],
        20,
    )
    .unwrap();

    let raw = "SELECT count(*) AS n, sum(value) AS total FROM metrics \
               WHERE timestamp_us >= 10 AND timestamp_us < 20";
    assert_eq!(db.query(raw).unwrap(), fresh_raw(&db, raw));
    let raw_warm = db.query_worker_stats();
    assert_eq!(raw_warm.resident_dynamic_loads, 0);
    assert_eq!(raw_warm.resident_dynamic_staged_bytes, 0);
    assert_eq!(raw_warm.resident_raw_staged_rows, 2);

    current_status(&db);
    let warm = db.query_worker_stats();
    assert_eq!(warm.resident_dynamic_loads, 1);
    assert!(warm.resident_dynamic_staged_bytes > 0);

    for (request_id, timestamp) in [("before", -10), ("after", 100)] {
        db.write(
            "metrics",
            request_id,
            vec![row(timestamp, "tenant", "series", 9.0)],
            100,
        )
        .unwrap();
        let before_raw = db.query_worker_stats();
        assert_eq!(db.query(raw).unwrap(), fresh_raw(&db, raw));
        let after_raw = db.query_worker_stats();
        // Scope reconciliation clears the prior catalog rows in the same worker.
        // This is a zero-byte empty replacement, not inaccessible payload staging.
        assert_eq!(
            after_raw.resident_dynamic_loads,
            before_raw.resident_dynamic_loads + 1
        );
        assert_eq!(after_raw.spawned, before_raw.spawned);
        assert_eq!(
            after_raw.resident_dynamic_staged_bytes,
            before_raw.resident_dynamic_staged_bytes
        );
        assert_eq!(
            after_raw.resident_raw_staged_rows,
            raw_warm.resident_raw_staged_rows
        );
        assert_eq!(after_raw.resident_hits, before_raw.resident_hits + 1);

        current_status(&db);
        let after_status = db.query_worker_stats();
        assert_eq!(
            after_status.resident_dynamic_loads,
            after_raw.resident_dynamic_loads + 1
        );
        assert!(
            after_status.resident_dynamic_staged_bytes > after_raw.resident_dynamic_staged_bytes
        );
    }

    let mixed = "SELECT count(*) AS n, \
                 (SELECT sequence FROM varve_status()) AS sequence \
                 FROM metrics WHERE timestamp_us >= 10 AND timestamp_us < 20";
    let sequence = db.status().unwrap().sequence;
    let before_mixed = db.query_worker_stats();
    let mixed_result = db.query(mixed).unwrap();
    assert_eq!(mixed_result[0]["n"], 2);
    assert_eq!(unsigned(&mixed_result[0]["sequence"]), sequence);
    // Generic mixed-source exposure also includes the previously omitted rollups.
    let after_mixed = db.query_worker_stats();
    assert_eq!(
        after_mixed.resident_dynamic_loads,
        before_mixed.resident_dynamic_loads + 1
    );
    assert_eq!(after_mixed.spawned, before_mixed.spawned);
    // Once that exact scope is current, neither raw nor dynamic input restages.
    assert_eq!(db.query(mixed).unwrap(), mixed_result);
    let repeated_mixed = db.query_worker_stats();
    assert_eq!(
        repeated_mixed.resident_dynamic_loads,
        after_mixed.resident_dynamic_loads
    );
    assert_eq!(
        repeated_mixed.resident_dynamic_staged_bytes,
        after_mixed.resident_dynamic_staged_bytes
    );
    assert_eq!(
        repeated_mixed.resident_raw_staged_rows,
        after_mixed.resident_raw_staged_rows
    );
    assert_eq!(repeated_mixed.spawned, after_mixed.spawned);

    db.write(
        "metrics",
        "unsupported-append",
        vec![row(200, "tenant", "series", 10.0)],
        200,
    )
    .unwrap();
    let unsupported = "WITH source AS (SELECT * FROM metrics) \
                       SELECT count(*) AS n, \
                       (SELECT sequence FROM varve_status()) AS sequence \
                       FROM source WHERE timestamp_us >= 10 AND timestamp_us < 20";
    let sequence = db.status().unwrap().sequence;
    let before_unsupported = db.query_worker_stats();
    let unsupported_result = db.query(unsupported).unwrap();
    assert_eq!(unsupported_result[0]["n"], 2);
    assert_eq!(unsigned(&unsupported_result[0]["sequence"]), sequence);
    assert_eq!(
        db.query_worker_stats().resident_dynamic_loads,
        before_unsupported.resident_dynamic_loads + 1
    );

    let fresh_temp = TempDir::new().unwrap();
    let fresh = Database::open(fresh_temp.path(), config(false, 1)).unwrap();
    fresh
        .create_table("metrics", TableConfig::default())
        .unwrap();
    fresh
        .write(
            "metrics",
            "fresh-row",
            vec![row(1, "tenant", "series", 1.0)],
            1,
        )
        .unwrap();
    current_status(&fresh);
    let fresh_stats = fresh.query_worker_stats();
    assert_eq!(fresh_stats.resident_full_loads, 0);
    assert_eq!(fresh_stats.resident_dynamic_loads, 0);
}

#[test]
fn selected_rollup_refreshes_only_for_relevant_exact_payload_changes() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config(true, 1)).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            ..TableConfig::default()
        },
    )
    .unwrap();
    db.create_continuous_aggregate("metrics_ten", "metrics", 10)
        .unwrap();
    db.write(
        "metrics",
        "negative-zero",
        vec![row(1, "selected", "cpu", -0.0)],
        10,
    )
    .unwrap();

    let sql = "SELECT count, sum, min, max, average, first, last, \
               open, high, low, close, first_timestamp_us, last_timestamp_us, \
               first_sequence, first_ordinal, last_sequence, last_ordinal \
               FROM metrics_ten WHERE tenant = 'selected' AND series = 'cpu'";
    let initial = db.query(sql).unwrap();
    assert_eq!(initial, fresh_rollup(&db, sql));
    assert_eq!(
        initial[0]["sum"].as_f64().unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
    let warm = db.query_worker_stats();
    assert_eq!(warm.resident_dynamic_loads, 1);
    assert!(warm.resident_dynamic_staged_bytes > 0);

    db.write(
        "metrics",
        "unrelated-series",
        vec![row(2, "other", "cpu", 7.0)],
        10,
    )
    .unwrap();
    assert_eq!(db.query(sql).unwrap(), initial);
    assert_eq!(db.query(sql).unwrap(), fresh_rollup(&db, sql));
    let unrelated = db.query_worker_stats();
    assert_eq!(
        unrelated.resident_dynamic_loads,
        warm.resident_dynamic_loads
    );
    assert_eq!(
        unrelated.resident_dynamic_staged_bytes,
        warm.resident_dynamic_staged_bytes
    );

    db.write(
        "metrics",
        "positive-zero",
        vec![row(3, "selected", "cpu", 0.0)],
        10,
    )
    .unwrap();
    let updated = db.query(sql).unwrap();
    assert_eq!(updated, fresh_rollup(&db, sql));
    assert_eq!(updated[0]["count"], 2);
    assert_eq!(
        updated[0]["first"].as_f64().unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
    assert_eq!(
        updated[0]["last"].as_f64().unwrap().to_bits(),
        0.0_f64.to_bits()
    );
    let relevant = db.query_worker_stats();
    assert_eq!(
        relevant.resident_dynamic_loads,
        unrelated.resident_dynamic_loads + 1
    );
    assert!(relevant.resident_dynamic_staged_bytes > unrelated.resident_dynamic_staged_bytes);
}
