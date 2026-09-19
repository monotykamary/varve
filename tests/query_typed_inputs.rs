use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::PathBuf;
use varve::model::{RollupRow, Row, StoredRow};
use varve::query::{
    AggregateAlias, CatalogRelation, QueryCatalog, QueryOptions, QueryRuntime, QueryTable,
    execute_with_catalog,
};

fn options() -> QueryOptions {
    QueryOptions {
        executable: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb"),
        timeout_ms: 5_000,
        ..QueryOptions::default()
    }
}

fn table() -> QueryTable {
    let stored = StoredRow {
        row: Row {
            timestamp_us: i64::MIN,
            tenant: "tenant'雪\\\n.shell echo not-a-command\n".into(),
            series: "cpu\"'); SELECT error('injected');--".into(),
            value: -0.0,
            tags: BTreeMap::from([("'\"\\\n雪".into(), "\t'雪".into())]),
        },
        sequence: u64::MAX,
        ordinal: u32::MAX,
    };
    QueryTable {
        name: "measurements".into(),
        rollups: vec![RollupRow::from_row(1, &stored).unwrap()],
        hot: vec![stored],
        files: vec![],
        cutoff_us: None,
    }
}

fn catalog(text: &str) -> QueryCatalog {
    QueryCatalog {
        relations: vec![CatalogRelation {
            name: "metadata".into(),
            columns: [
                ("text", "VARCHAR"),
                ("signed", "BIGINT"),
                ("unsigned", "UBIGINT"),
                ("number", "DOUBLE"),
                ("flag", "BOOLEAN"),
                ("optional", "VARCHAR"),
            ]
            .into_iter()
            .map(|(name, kind)| (name.into(), kind.into()))
            .collect(),
            rows: vec![json!([text, i64::MIN, u64::MAX, -0.0, true, null])],
        }],
        aggregates: vec![AggregateAlias {
            name: "minute \"雪".into(),
            source: "measurements".into(),
            width_us: 1,
        }],
    }
}

fn both(runtime: &QueryRuntime, tables: &[QueryTable], sql: &str, catalog: &QueryCatalog) -> Value {
    let fresh = execute_with_catalog(tables, sql, &options(), catalog).unwrap();
    let resident = runtime
        .execute_with_catalog(tables, sql, &options(), catalog)
        .unwrap();
    assert_eq!(resident, fresh);
    resident
}

#[test]
fn typed_hot_and_rollup_roundtrip_exact_values_in_real_cli() {
    let runtime = QueryRuntime::new(1);
    let mut selected = table();
    let numbers = [
        -0.0,
        0.0,
        f64::from_bits(1),
        -f64::from_bits(1),
        f64::MIN_POSITIVE,
        f64::MAX,
        -f64::MAX,
        f64::from_bits(0x3fd5555555555555),
    ];
    let template = selected.hot[0].clone();
    selected.hot = numbers
        .iter()
        .enumerate()
        .map(|(i, &value)| {
            let mut stored = template.clone();
            stored.row.value = value;
            stored.row.timestamp_us = if i % 2 == 0 { i64::MIN } else { i64::MAX };
            stored.ordinal = u32::MAX - i as u32;
            stored.sequence = if i % 2 == 0 { u64::MAX } else { 0 };
            stored
        })
        .collect();
    selected.rollups = selected
        .hot
        .iter()
        .map(|stored| {
            let mut rollup = RollupRow::from_row(1, stored).unwrap();
            rollup.count = u64::MAX;
            rollup
        })
        .collect();
    let tables = [selected];
    let raw = both(
        &runtime,
        &tables,
        "SELECT *, typeof(timestamp_us) AS ts_type, typeof(value) AS value_type, typeof(sequence) AS seq_type, typeof(ordinal) AS ord_type FROM measurements ORDER BY ordinal DESC",
        &QueryCatalog::default(),
    );
    for (actual, expected) in raw.as_array().unwrap().iter().zip(&tables[0].hot) {
        assert_eq!(actual["timestamp_us"], expected.row.timestamp_us);
        assert_eq!(
            actual["value"].as_f64().unwrap().to_bits(),
            expected.row.value.to_bits()
        );
        assert_eq!(actual["tenant"], expected.row.tenant);
        assert_eq!(actual["series"], expected.row.series);
        assert_eq!(actual["sequence"], expected.sequence.to_string());
        assert_eq!(actual["ordinal"], expected.ordinal);
        assert_eq!(
            actual["tags"],
            serde_json::to_string(&expected.row.tags).unwrap()
        );
        assert_eq!(actual["ts_type"], "BIGINT");
        assert_eq!(actual["value_type"], "DOUBLE");
        assert_eq!(actual["seq_type"], "UBIGINT");
        assert_eq!(actual["ord_type"], "UINTEGER");
    }
    let derived = both(
        &runtime,
        &tables,
        "SELECT * FROM measurements__rollup ORDER BY first_ordinal DESC",
        &QueryCatalog::default(),
    );
    for (actual, expected) in derived.as_array().unwrap().iter().zip(&tables[0].rollups) {
        for name in [
            "sum", "min", "max", "first", "last", "open", "high", "low", "close",
        ] {
            assert_eq!(
                actual[name].as_f64().unwrap().to_bits(),
                expected.sum.to_bits(),
                "{name}"
            );
        }
        assert_eq!(actual["count"], u64::MAX.to_string());
        assert_eq!(actual["bucket_us"], expected.bucket_us);
        assert_eq!(actual["first_timestamp_us"], expected.first_timestamp_us);
        assert_eq!(actual["last_timestamp_us"], expected.last_timestamp_us);
        assert_eq!(
            actual["first_sequence"],
            expected.first_sequence.to_string()
        );
        assert_eq!(actual["last_sequence"], expected.last_sequence.to_string());
        assert_eq!(actual["first_ordinal"], expected.first_ordinal);
        assert_eq!(actual["last_ordinal"], expected.last_ordinal);
    }
    assert_eq!(raw.as_array().unwrap().len(), numbers.len());
    assert_eq!(derived.as_array().unwrap().len(), numbers.len());
}

#[test]
fn typed_scanner_and_empty_snapshots_refresh_named_aggregates_and_catalogs() {
    let runtime = QueryRuntime::new(1);
    let mut tables = [table()];
    let sql = "SELECT bucket_us, count, first, first_sequence, first_ordinal, text, signed, unsigned, number, flag, optional FROM \"minute \"\"雪\" CROSS JOIN metadata()";
    for text in ["typed'雪\\\n", "catalog\0NUL", "typed again"] {
        let catalog = catalog(text);
        let result = both(&runtime, &tables, sql, &catalog);
        assert_eq!(result[0]["text"], text);
        assert_eq!(result[0]["bucket_us"], i64::MIN);
        assert_eq!(result[0]["count"], 1);
        assert_eq!(result[0]["first_sequence"], u64::MAX.to_string());
        assert_eq!(result[0]["first_ordinal"], u32::MAX);
        assert_eq!(result[0]["signed"], i64::MIN);
        assert_eq!(result[0]["unsigned"], u64::MAX.to_string());
        assert_eq!(
            result[0]["number"].as_f64().unwrap().to_bits(),
            (-0.0f64).to_bits()
        );
        assert_eq!(result[0]["flag"], true);
        assert!(result[0]["optional"].is_null());
    }
    tables[0].cutoff_us = Some(i64::MIN + 1);
    assert_eq!(
        both(
            &runtime,
            &tables,
            "SELECT * FROM measurements",
            &catalog("cutoff")
        ),
        json!([])
    );
    // Independently retained rollups must not inherit the raw cutoff.
    assert_eq!(
        both(&runtime, &tables, sql, &catalog("retained"))[0]["count"],
        1
    );
    tables[0].hot.clear();
    tables[0].rollups.clear();
    assert_eq!(both(&runtime, &tables, sql, &catalog("empty")), json!([]));
    let result = both(
        &runtime,
        &[],
        "SELECT count(*) AS stale FROM duckdb_functions() WHERE function_name = 'metadata'",
        &QueryCatalog::default(),
    );
    assert_eq!(result[0]["stale"], 0);
    if cfg!(unix) {
        assert_eq!(runtime.stats().spawned, 1);
        assert_eq!(runtime.stats().reused, 6);
    }
    assert_eq!(runtime.stats().resets, 7);
}

#[test]
fn row_and_encoded_byte_fallbacks_keep_all_rows_and_exact_types() {
    let runtime = QueryRuntime::new(1);
    let mut selected = table();
    // 129 is just beyond the total-row ceiling; this is a correctness fixture.
    selected.hot = vec![selected.hot[0].clone(); 129];
    let tables = [selected];
    let result = both(
        &runtime,
        &tables,
        "SELECT count(*) AS n, max(sequence) AS seq, max(ordinal) AS ord FROM measurements",
        &QueryCatalog::default(),
    );
    assert_eq!(
        result,
        json!([{"n": 129, "seq": u64::MAX.to_string(), "ord": u32::MAX}])
    );
    for text in ["'".repeat(17 * 1024), "雪".repeat(11 * 1024)] {
        let result = both(
            &runtime,
            &[table()],
            "SELECT * FROM metadata()",
            &catalog(&text),
        );
        assert_eq!(result[0]["text"], text);
        assert_eq!(result[0]["unsigned"], u64::MAX.to_string());
    }
    // Values and schema agree when a NUL catalog forces the same hot/rollup rows
    // through copied NDJSON instead of SQL literals.
    let tables = [table()];
    for sql in [
        "SELECT * FROM measurements",
        "SELECT * FROM measurements__rollup",
    ] {
        let typed = both(&runtime, &tables, sql, &catalog("typed"));
        let scanned = both(&runtime, &tables, sql, &catalog("force\0scanner"));
        assert_eq!(typed, scanned);
    }
}

#[test]
fn typed_requests_stage_nothing_and_scanner_fallback_leaves_no_stale_access() {
    let runtime = QueryRuntime::new(1);
    let tables = [table()];
    let glob = "SELECT file FROM glob(current_setting('home_directory') || '/inputs/*')";
    assert_eq!(
        runtime
            .execute_with_catalog(&tables, glob, &options(), &catalog("typed"))
            .unwrap(),
        json!([])
    );
    let files = runtime
        .execute_with_catalog(&tables, glob, &options(), &catalog("force\0scanner"))
        .unwrap();
    assert_eq!(files.as_array().unwrap().len(), 1);
    let path = files[0]["file"].as_str().unwrap();
    assert!(
        !PathBuf::from(path).exists(),
        "staging is removed before success"
    );
    for scanner in ["read_blob", "read_json"] {
        let sql = format!("SELECT * FROM {scanner}('{}')", path.replace('\'', "''"));
        let error = runtime
            .execute_with_catalog(&[], &sql, &options(), &QueryCatalog::default())
            .unwrap_err();
        assert!(format!("{error:#}").contains("disabled"), "{error:#}");
    }
    let settings = both(
        &runtime,
        &tables,
        "SELECT current_setting('allowed_paths') AS paths, current_setting('enable_external_access') AS external, current_setting('lock_configuration') AS locked",
        &catalog("typed"),
    );
    assert_eq!(
        settings,
        json!([{"paths": [], "external": false, "locked": true}])
    );
}

#[test]
fn general_duckdb_sql_and_disposable_workers_still_accept_small_inputs() {
    let runtime = QueryRuntime::new(2);
    let tables = [table()];
    let catalog = catalog("catalog");
    let sql = "WITH selected AS (SELECT *, row_number() OVER (ORDER BY bucket_us) AS rank FROM \"minute \"\"雪\") SELECT sum(count) FILTER (WHERE rank = 1) AS n FROM selected";
    assert_eq!(both(&runtime, &tables, sql, &catalog)[0]["n"], "1");
    // Unknown/dynamic functions stay available through the fresh-only policy.
    assert_eq!(
        both(
            &runtime,
            &tables,
            "SELECT * FROM query('SELECT count(*) AS n FROM measurements')",
            &catalog
        )[0]["n"],
        1
    );
    assert_eq!(runtime.stats().discarded, 1);
    assert_eq!(both(&runtime, &tables, sql, &catalog)[0]["n"], "1");
    if cfg!(unix) {
        assert_eq!(runtime.stats().spawned, 2);
        assert_eq!(runtime.stats().reused, 1);
    }
}
