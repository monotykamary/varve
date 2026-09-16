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
fn empty_and_disk_only_match_scanner_backed_schemas_and_results() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("disk ' 雪.parquet");
    segment::write(
        &path,
        &[row(-1, "t", "s", 1.25, 1), row(1, "t", "s", 2.5, 2)],
    )
    .unwrap();
    let mut metrics = table("metrics");
    let catalog = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "metadata".into(),
            columns: vec![("n".into(), "BIGINT".into())],
            rows: vec![],
        }],
        aggregates: vec![AggregateAlias {
            name: "minute metrics".into(),
            source: "METRICS".into(),
            width_us: 60,
        }],
    };
    // An unrelated catalog payload forces the old scanner path without adding
    // rows to any relation under comparison.
    let mut scanner = catalog.clone();
    scanner.relations.push(CatalogRelation {
        name: "force_input".into(),
        columns: vec![("n".into(), "VARCHAR".into())],
        rows: vec![serde_json::json!(["\0"])],
    });
    for disk in [false, true] {
        if disk {
            metrics.files.push(path.clone());
            metrics.cutoff_us = Some(0);
        }
        for sql in [
            "SELECT 42 AS answer",
            "SELECT * FROM metrics ORDER BY timestamp_us",
            "SELECT * FROM metrics__rollup",
            "SELECT * FROM \"minute metrics\"",
            "SELECT * FROM metadata()",
            "SELECT table_name, column_name, data_type, is_nullable FROM duckdb_columns() WHERE table_name IN ('metrics', 'metrics__rollup', 'minute metrics', '__varve_input') ORDER BY table_name, column_index",
        ] {
            let plain =
                execute_with_catalog(std::slice::from_ref(&metrics), sql, &options(), &catalog)
                    .unwrap();
            let scanned =
                execute_with_catalog(std::slice::from_ref(&metrics), sql, &options(), &scanner)
                    .unwrap();
            assert_eq!(plain, scanned, "disk={disk}: {sql}");
        }
        let paths = execute_with_catalog(
            std::slice::from_ref(&metrics),
            "SELECT current_setting('allowed_paths') AS paths",
            &options(),
            &catalog,
        )
        .unwrap();
        assert_eq!(
            paths[0]["paths"],
            if disk {
                serde_json::json!([path.canonicalize().unwrap().to_str().unwrap()])
            } else {
                serde_json::json!([])
            }
        );
    }
}

#[test]
fn hot_matches_parquet_and_rollup_preserves_extremes_and_nulls() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("typed.parquet");
    let mut rows = vec![
        row(i64::MIN, "雪\"\\\n", "cpu", f64::MIN_POSITIVE, u64::MAX),
        row(i64::MAX, "t", "s", f64::MAX, 2),
    ];
    rows[0].ordinal = u32::MAX;
    segment::write(&path, &rows).unwrap();
    let mut hot = table("metrics");
    hot.hot = rows.clone();
    let mut cold = table("metrics");
    cold.files.push(path);
    let sql = "SELECT * FROM metrics ORDER BY timestamp_us";
    assert_eq!(
        execute(&[hot.clone()], sql, &options()).unwrap(),
        execute(&[cold], sql, &options()).unwrap()
    );
    let mut rollup = RollupRow::from_row(1, &rows[0]).unwrap();
    rollup.count = 0;
    hot.rollups.push(rollup);
    let result = execute(&[hot], "SELECT width_us, bucket_us, average, first_timestamp_us, last_timestamp_us, CAST(first_sequence AS VARCHAR) AS seq, CAST(first_ordinal AS BIGINT) AS ord, first = open AND last = close AND min = low AND max = high AS aliases FROM metrics__rollup", &options()).unwrap();
    assert_eq!(
        result,
        serde_json::json!([{"width_us": 1, "bucket_us": i64::MIN, "average": null, "first_timestamp_us": i64::MIN, "last_timestamp_us": i64::MIN, "seq": u64::MAX.to_string(), "ord": u32::MAX, "aliases": true}])
    );
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
fn catalog_macros_use_typed_rows_and_quote_identifiers() {
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
fn database_catalog_only_avoids_stdin_and_keeps_catalog_visible() {
    let dir = TempDir::new().unwrap();
    let db = varve::Database::open(dir.path(), varve::Config::default()).unwrap();
    db.create_table("metrics", varve::TableConfig::default())
        .unwrap();
    let result = db.query("SELECT current_setting('allowed_paths') AS paths, (SELECT count(*) FROM varve_status()) AS status_rows, (SELECT count(*) FROM varve_tables()) AS tables, (SELECT count(*) FROM varve_policies()) AS policies").unwrap();
    assert_eq!(
        result,
        serde_json::json!([{"paths": [], "status_rows": 1, "tables": 1, "policies": 1}])
    );
}

#[test]
fn catalog_literals_match_scanner_and_fall_back_without_losing_values() {
    use serde_json::json;
    let mut catalog = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "literal \" 雪".into(),
            columns: ["VARCHAR", "BIGINT", "UBIGINT", "DOUBLE", "BOOLEAN"]
                .into_iter()
                .enumerate()
                .map(|(i, kind)| (format!("c{i}"), kind.into()))
                .collect(),
            rows: vec![],
        }],
        aggregates: vec![],
    };
    let mut force = table("unrelated");
    force.hot.push(row(0, "t", "s", 1.0, 1));
    let sql = "SELECT *, typeof(c0) AS t0, typeof(c1) AS t1, typeof(c2) AS t2, typeof(c3) AS t3, typeof(c4) AS t4 FROM \"literal \"\" 雪\"() ORDER BY c1 NULLS FIRST";
    for text in [
        None,
        Some("雪'\"\\\n.shell echo not-a-command\n'); SELECT error('injected');--".to_owned()),
        Some("embedded\0nul".to_owned()),
        Some("'".repeat(17 * 1024)),
        Some("x".repeat(33 * 1024)),
    ] {
        catalog.relations[0].rows = text
            .as_ref()
            .map(|text| {
                vec![
                    json!([text, i64::MIN, u64::MAX, f64::MIN_POSITIVE, true]),
                    json!(["other", i64::MAX, 0, -1.25, false]),
                    json!([null, null, null, null, null]),
                ]
            })
            .unwrap_or_default();
        let direct = execute_with_catalog(&[], sql, &options(), &catalog).unwrap();
        let scanned =
            execute_with_catalog(std::slice::from_ref(&force), sql, &options(), &catalog).unwrap();
        assert_eq!(direct, scanned);
        if let Some(text) = &text {
            assert_eq!(direct[1]["c0"], *text);
            assert_eq!(direct[1]["c1"], json!(i64::MIN));
            // DuckDB CLI emits UBIGINT JSON values as strings.
            assert_eq!(direct[1]["c2"].as_str().unwrap(), u64::MAX.to_string());
            assert_eq!(direct[1]["t2"], "UBIGINT");
        }
        let paths = execute_with_catalog(
            &[],
            "SELECT current_setting('allowed_paths') AS paths",
            &options(),
            &catalog,
        )
        .unwrap();
        let fallback = text
            .as_ref()
            .is_some_and(|s| s.contains('\0') || s.len() >= 17 * 1024);
        assert_eq!(
            paths[0]["paths"],
            if fallback {
                json!([std::fs::canonicalize("/dev/stdin")
                    .unwrap()
                    .to_str()
                    .unwrap()])
            } else {
                json!([])
            }
        );
    }
}

#[test]
fn catalog_payload_preserves_nullable_types() {
    let catalog = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "nullable".into(),
            columns: ["VARCHAR", "BIGINT", "UBIGINT", "DOUBLE", "BOOLEAN"]
                .into_iter()
                .enumerate()
                .map(|(i, kind)| (format!("c{i}"), kind.into()))
                .collect(),
            rows: vec![serde_json::json!([null, null, null, null, null])],
        }],
        aggregates: vec![],
    };
    assert_eq!(
        execute_with_catalog(&[], "SELECT * FROM nullable()", &options(), &catalog).unwrap(),
        serde_json::json!([{"c0": null, "c1": null, "c2": null, "c3": null, "c4": null}])
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
