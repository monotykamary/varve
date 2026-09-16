use std::collections::BTreeMap;
use std::path::PathBuf;

use tempfile::TempDir;
use varve::model::{RollupRow, Row, StoredRow};
use varve::query::{
    AggregateAlias, CatalogRelation, QueryCatalog, QueryOptions, QueryTable, execute,
    execute_with_catalog, version,
};
use varve::segment;

fn options() -> QueryOptions {
    QueryOptions {
        executable: PathBuf::from("duckdb"),
        timeout_ms: 5_000,
        ..QueryOptions::default()
    }
}

fn row(timestamp_us: i64, tenant: &str, series: &str, value: f64, sequence: u64) -> StoredRow {
    StoredRow {
        row: Row {
            timestamp_us,
            tenant: tenant.to_owned(),
            series: series.to_owned(),
            value,
            tags: BTreeMap::from([("说明'\"".to_owned(), "雪'\"".to_owned())]),
        },
        sequence,
        ordinal: 0,
    }
}

fn table(name: &str) -> QueryTable {
    QueryTable {
        name: name.to_owned(),
        hot: Vec::new(),
        files: Vec::new(),
        rollups: Vec::new(),
        cutoff_us: None,
    }
}

#[test]
fn reports_v2_and_empty_query_is_json_array() {
    assert!(version(&options()).unwrap().starts_with("v2."));
    let result = execute(&[table("metrics")], "SELECT * FROM metrics", &options()).unwrap();
    assert_eq!(result, serde_json::json!([]));
}

#[test]
fn queries_mixed_hot_and_cold_rows_with_filters_and_unicode_tags() {
    let directory = TempDir::new().unwrap();
    let cold_path = directory.path().join("cold ' 雪.parquet");
    segment::write(
        &cold_path,
        &[
            row(10, "tenant-a", "cpu", 0.5, 1),
            row(20, "tenant-a", "cpu", 1.5, 2),
            row(20, "tenant-b", "cpu", 2.5, 3),
        ],
    )
    .unwrap();
    let mut metrics = table("metrics");
    metrics.hot.push(row(30, "tenant-a", "cpu", 3.5, 3));
    metrics.hot.push(row(40, "tenant-a", "disk", 4.5, 4));
    metrics.files.push(cold_path);
    metrics.cutoff_us = Some(20);

    let result = execute(
        &[metrics],
        "SELECT timestamp_us, value, tags FROM metrics WHERE tenant = 'tenant-a' AND series = 'cpu' ORDER BY timestamp_us",
        &options(),
    )
    .unwrap();
    assert_eq!(
        result,
        serde_json::json!([
            {"timestamp_us": 20, "value": 1.5, "tags": "{\"说明'\\\"\":\"雪'\\\"\"}"},
            {"timestamp_us": 30, "value": 3.5, "tags": "{\"说明'\\\"\":\"雪'\\\"\"}"}
        ])
    );
}

#[test]
fn raw_and_rollup_relations_are_separate() {
    let stored = row(120, "tenant", "cpu", 8.0, 9);
    let mut metrics = table("metrics");
    metrics.hot.push(stored.clone());
    metrics
        .rollups
        .push(RollupRow::from_row(60, &stored).unwrap());

    let result = execute(
        &[metrics],
        "SELECT (SELECT count(*) FROM metrics) AS raw_count, count AS rollup_count, first, last FROM metrics__rollup",
        &options(),
    )
    .unwrap();
    assert_eq!(
        result,
        serde_json::json!([{"raw_count": 1, "rollup_count": "1", "first": 8.0, "last": 8.0}])
    );
}

#[test]
fn catalog_macros_use_typed_stdin_rows_and_quote_identifiers() {
    let catalog = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "varve_tables".to_string(),
            columns: vec![
                ("table name".to_string(), "VARCHAR".to_string()),
                ("row_count".to_string(), "UBIGINT".to_string()),
                ("ratio".to_string(), "DOUBLE".to_string()),
                ("select".to_string(), "BOOLEAN".to_string()),
                ("signed".to_string(), "BIGINT".to_string()),
            ],
            rows: vec![serde_json::json!({
                "table name": "metrics'); SELECT error('injected')--",
                "row_count": 7,
                "ratio": 1.5,
                "select": true,
                "signed": -9,
            })],
        }],
        aggregates: Vec::new(),
    };

    let result = execute_with_catalog(
        &[],
        "SELECT \"table name\", CAST(row_count AS BIGINT) AS row_count, ratio, \"select\", signed FROM varve_tables()",
        &options(),
        &catalog,
    )
    .unwrap();
    assert_eq!(
        result,
        serde_json::json!([{
            "table name": "metrics'); SELECT error('injected')--",
            "row_count": 7,
            "ratio": 1.5,
            "select": true,
            "signed": -9,
        }])
    );
}

#[test]
fn aggregate_alias_is_a_width_filtered_quoted_view() {
    let first = row(120, "tenant", "cpu", 8.0, 9);
    let second = row(600, "tenant", "cpu", 10.0, 10);
    let mut metrics = table("metrics");
    metrics
        .rollups
        .push(RollupRow::from_row(60, &first).unwrap());
    metrics
        .rollups
        .push(RollupRow::from_row(300, &second).unwrap());
    let catalog = QueryCatalog {
        relations: Vec::new(),
        aggregates: vec![AggregateAlias {
            name: "minute metrics".to_string(),
            source: "metrics".to_string(),
            width_us: 60,
        }],
    };

    let result = execute_with_catalog(
        &[metrics],
        "SELECT width_us, count, first, high, low FROM \"minute metrics\"",
        &options(),
        &catalog,
    )
    .unwrap();
    assert_eq!(
        result,
        serde_json::json!([{
            "width_us": 60,
            "count": 1,
            "first": 8.0,
            "high": 8.0,
            "low": 8.0
        }])
    );
}

#[test]
fn catalog_rejects_unapproved_types_and_malformed_rows() {
    let unsupported = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "metadata".to_string(),
            columns: vec![("payload".to_string(), "JSON".to_string())],
            rows: Vec::new(),
        }],
        aggregates: Vec::new(),
    };
    assert!(
        execute_with_catalog(&[], "SELECT * FROM metadata()", &options(), &unsupported).is_err()
    );

    let malformed = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "metadata".to_string(),
            columns: vec![("enabled".to_string(), "BOOLEAN".to_string())],
            rows: vec![serde_json::json!({"enabled": "yes"})],
        }],
        aggregates: Vec::new(),
    };
    assert!(execute_with_catalog(&[], "SELECT * FROM metadata()", &options(), &malformed).is_err());
}

#[test]
fn explain_is_returned_as_structured_json_instead_of_failing_on_cli_rendering() {
    for sql in [
        "EXPLAIN SELECT 1",
        "/* plan */ EXPLAIN ANALYZE SELECT 1",
        "EXPLAIN (FORMAT JSON) SELECT 1",
    ] {
        let result = execute(&[], sql, &options()).unwrap();
        assert!(!result[0]["plan"].as_str().unwrap().is_empty(), "{sql}");
    }
}

#[test]
fn malformed_sql_and_injections_are_rejected() {
    let metrics = table("metrics");
    for sql in [
        "SELEC * FROM metrics",
        "SELECT * FROM metrics; DELETE FROM metrics",
        "WITH changed AS (DELETE FROM metrics RETURNING *) SELECT * FROM changed",
        "EXPLAIN INSERT INTO metrics VALUES (1)",
        ".read /tmp/evil.sql",
        "SELECT 1\n.shell echo escaped",
        "SELECT 1\n.read /tmp/evil.sql",
    ] {
        assert!(
            execute(std::slice::from_ref(&metrics), sql, &options()).is_err(),
            "accepted {sql:?}"
        );
    }
    assert!(execute(&[metrics], "SELECT missing FROM metrics", &options()).is_err());
}

#[test]
fn large_sql_uses_a_bounded_file_instead_of_process_arguments() {
    let mut below_limit = String::from("SELECT 42 AS answer /*");
    below_limit.push_str(&"x".repeat(2 * 1024 * 1024));
    below_limit.push_str("*/");
    let result = execute(&[], &below_limit, &options()).unwrap();
    assert_eq!(result, serde_json::json!([{"answer": 42}]));

    let mut above_limit = String::from("SELECT 42 /*");
    above_limit.push_str(&"x".repeat(8 * 1024 * 1024));
    above_limit.push_str("*/");
    let error = execute(&[], &above_limit, &options()).unwrap_err();
    assert!(
        error.to_string().contains("SQL script exceeds"),
        "{error:#}"
    );
}

#[test]
fn enforces_output_and_elapsed_limits() {
    let mut output_options = options();
    output_options.max_output_bytes = 32;
    assert!(
        execute(
            &[],
            "SELECT i FROM range(10000) AS values(i)",
            &output_options
        )
        .is_err()
    );

    let mut timeout_options = options();
    timeout_options.timeout_ms = 25;
    let error = execute(
        &[],
        "SELECT sum(sin(i::DOUBLE)) FROM range(1000000000000) AS values(i)",
        &timeout_options,
    )
    .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error:#}");
}
