use super::native::NativeRuntime;
use super::*;
use crate::model::{RollupRow, Row, StoredRow};
use crate::segment;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

static NATIVE_TEST_LOCK: Mutex<()> = Mutex::new(());
pub(super) fn native_test_guard() -> MutexGuard<'static, ()> {
    NATIVE_TEST_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tempfile::TempDir;

pub(super) fn paths() -> (PathBuf, PathBuf) {
    let library = env::var_os("VARVE_DUCKDB_V2_LIBRARY")
        .map(PathBuf::from)
        .expect("VARVE_DUCKDB_V2_LIBRARY must name the exact pinned shared library");
    let cli = env::var_os("VARVE_DUCKDB_CLI")
        .map(PathBuf::from)
        .expect("VARVE_DUCKDB_CLI must name the exact pinned CLI parity oracle");
    assert!(
        library.is_absolute(),
        "native library test path must be absolute"
    );
    assert!(cli.is_absolute(), "DuckDB CLI test path must be absolute");
    (library, cli)
}

pub(super) fn options(cli: PathBuf) -> QueryOptions {
    QueryOptions {
        executable: cli,
        timeout_ms: 5_000,
        ..QueryOptions::default()
    }
}

pub(super) fn native_budget() -> crate::raw_memory::RawMemoryBudget {
    crate::raw_memory::RawMemoryBudget::new(64 * 1024 * 1024, 1).unwrap()
}

pub(super) fn stored(timestamp_us: i64, value: f64, sequence: u64, ordinal: u32) -> StoredRow {
    StoredRow {
        row: Row {
            timestamp_us,
            tenant: "tenant 雪'\"\\\n".into(),
            series: "cpu".into(),
            value,
            tags: BTreeMap::from([("说明'\"".into(), "雪'\"".into())]),
        },
        sequence,
        ordinal,
    }
}

pub(super) fn resident(
    rows: crate::raw_memory::SharedRawRows,
    files: Vec<ResidentFile>,
    rollups: Vec<RollupRow>,
) -> (Vec<QueryTable>, ResidentSnapshot) {
    let mut ids = vec!["hot-a".to_owned()];
    ids.extend(files.iter().map(|file| file.id.clone()));
    let files_for_query = files.iter().map(|file| file.path.clone()).collect();
    (
        vec![QueryTable {
            name: "metrics".into(),
            hot: vec![],
            files: files_for_query,
            rollups,
            cutoff_us: None,
        }],
        ResidentSnapshot {
            namespace: "native-tests".into(),
            sequence: 7,
            tables: vec![ResidentTable {
                name: "metrics".into(),
                batches: vec![ResidentBatch::new("hot-a".into(), rows, 1024)],
                files,
            }],
            lineage: vec![ResidentLineage {
                name: "metrics".into(),
                raw_stamp: 1,
                ids,
            }],
        },
    )
}

pub(super) fn oracle_tables(tables: &[QueryTable], snapshot: &ResidentSnapshot) -> Vec<QueryTable> {
    let mut oracle = tables.to_vec();
    for table in &mut oracle {
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
    oracle
}

pub(super) fn native(
    runtime: &NativeRuntime,
    tables: &[QueryTable],
    snapshot: &ResidentSnapshot,
    sql: &str,
    options: &QueryOptions,
    catalog: &QueryCatalog,
) -> Value {
    runtime
        .execute(
            tables,
            Some(snapshot),
            sql,
            options,
            catalog,
            &native_budget(),
            &AtomicBool::new(false),
        )
        .unwrap()
}

#[test]
fn real_library_empty_raw_rollup_catalog_and_json_edges_match_cli() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 2).unwrap();
    assert_eq!(runtime.identity().version, "v2.0.0-alpha41533");

    let rows = crate::raw_memory::SharedRawRows::test_rows(vec![
        stored(i64::MIN, -0.0, u64::MAX, u32::MAX),
        stored(i64::MAX, 2.5, 2, 1),
    ]);
    // Unit width keeps i64::MIN representable; width 60 would underflow
    // the mathematical bucket before either SQL adapter is exercised.
    let rollup = RollupRow::from_row(1, &rows[0]).unwrap();
    let (tables, snapshot) = resident(rows.clone(), vec![], vec![rollup]);
    let catalog = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "catalog 雪\"".into(),
            columns: vec![
                ("text".into(), "VARCHAR".into()),
                ("unsigned".into(), "UBIGINT".into()),
                ("maybe".into(), "DOUBLE".into()),
            ],
            rows: vec![json!(["value 雪'\"", u64::MAX, null])],
        }],
        aggregates: vec![AggregateAlias {
            name: "minute metrics".into(),
            source: "METRICS".into(),
            width_us: 1,
        }],
    };
    let oracle = oracle_tables(&tables, &snapshot);
    for sql in [
        "SELECT * FROM metrics WHERE false",
        "SELECT NULL AS missing",
        "SELECT timestamp_us, tenant, series, value, tags, sequence, ordinal FROM metrics ORDER BY timestamp_us",
        "SELECT width_us, bucket_us, count, first, open, high, low, close, first_sequence FROM metrics__rollup",
        "SELECT width_us, count, first, high, low FROM \"minute metrics\"",
        "SELECT text, unsigned, maybe FROM \"catalog 雪\"\"\"()",
        "SELECT value AS first_alias, value AS second_alias FROM metrics ORDER BY timestamp_us LIMIT 1",
    ] {
        let expected = execute_with_catalog(&oracle, sql, &options, &catalog).unwrap();
        let actual = native(&runtime, &tables, &snapshot, sql, &options, &catalog);
        assert_eq!(actual, expected, "{sql}");
    }

    let raw = native(
        &runtime,
        &tables,
        &snapshot,
        "SELECT value, sequence FROM metrics ORDER BY timestamp_us LIMIT 1",
        &options,
        &catalog,
    );
    assert_eq!(raw[0]["sequence"], u64::MAX.to_string());
    assert_eq!(
        raw[0]["value"].as_f64().unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
    assert_eq!(runtime.live_callback_owners(), 0);
}

#[test]
fn real_library_mixed_verified_parquet_union_matches_cli_without_staging() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("cold ' 雪.parquet");
    let cold = vec![stored(-1, 1.25, 1, 0), stored(20, 3.5, 2, 0)];
    segment::write(&path, &cold).unwrap();
    let path = path.canonicalize().unwrap();
    let before = std::fs::read_dir(directory.path()).unwrap().count();
    let hot = crate::raw_memory::SharedRawRows::test_rows(vec![stored(30, 4.5, 3, 0)]);
    let file = ResidentFile {
        id: "cold-a".into(),
        path: path.clone(),
        rows: cold.len(),
        charged_bytes: 4096,
        min_timestamp_us: -1,
        max_timestamp_us: 20,
    };
    let (mut tables, snapshot) = resident(hot, vec![file], vec![]);
    tables[0].cutoff_us = Some(0);
    let sql = "SELECT timestamp_us, value, sequence FROM metrics ORDER BY timestamp_us";
    let expected = execute(&oracle_tables(&tables, &snapshot), sql, &options).unwrap();
    let actual = native(
        &runtime,
        &tables,
        &snapshot,
        sql,
        &options,
        &QueryCatalog::default(),
    );
    assert_eq!(actual, expected);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), before);
    assert!(path.exists());
    assert_eq!(runtime.live_callback_owners(), 0);
}

#[test]
fn real_library_private_databases_keep_concurrent_allowlists_disjoint() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 2).unwrap();
    let left_dir = TempDir::new().unwrap();
    let right_dir = TempDir::new().unwrap();
    let left_path = left_dir.path().join("left.parquet");
    let right_path = right_dir.path().join("right.parquet");
    segment::write(&left_path, &[stored(1, 10.0, 1, 0)]).unwrap();
    segment::write(&right_path, &[stored(2, 20.0, 2, 0)]).unwrap();
    let left_path = left_path.canonicalize().unwrap();
    let right_path = right_path.canonicalize().unwrap();
    let file = |id: &str, path: PathBuf, timestamp_us: i64| ResidentFile {
        id: id.into(),
        path,
        rows: 1,
        charged_bytes: 4096,
        min_timestamp_us: timestamp_us,
        max_timestamp_us: timestamp_us,
    };
    let (left_tables, left_snapshot) = resident(
        crate::raw_memory::SharedRawRows::test_rows(vec![]),
        vec![file("left", left_path.clone(), 1)],
        vec![],
    );
    let (right_tables, right_snapshot) = resident(
        crate::raw_memory::SharedRawRows::test_rows(vec![]),
        vec![file("right", right_path.clone(), 2)],
        vec![],
    );
    let barrier = std::sync::Barrier::new(2);
    thread::scope(|scope| {
        let left = scope.spawn(|| {
            barrier.wait();
            runtime.execute(
                &left_tables,
                Some(&left_snapshot),
                &format!(
                    "SELECT count(*) AS n FROM read_parquet({})",
                    quote_path(&left_path).unwrap()
                ),
                &options,
                &QueryCatalog::default(),
                &native_budget(),
                &AtomicBool::new(false),
            )
        });
        let right = scope.spawn(|| {
            barrier.wait();
            runtime.execute(
                &right_tables,
                Some(&right_snapshot),
                &format!(
                    "SELECT count(*) AS n FROM read_parquet({})",
                    quote_path(&right_path).unwrap()
                ),
                &options,
                &QueryCatalog::default(),
                &native_budget(),
                &AtomicBool::new(false),
            )
        });
        assert_eq!(left.join().unwrap().unwrap(), json!([{"n": 1}]));
        assert_eq!(right.join().unwrap().unwrap(), json!([{"n": 1}]));
    });
    let denied = runtime
        .execute(
            &left_tables,
            Some(&left_snapshot),
            &format!(
                "SELECT * FROM read_parquet({})",
                quote_path(&right_path).unwrap()
            ),
            &options,
            &QueryCatalog::default(),
            &native_budget(),
            &AtomicBool::new(false),
        )
        .unwrap_err();
    assert!(
        format!("{denied:#}").contains("disabled") || format!("{denied:#}").contains("Permission"),
        "{denied:#}"
    );
    assert_eq!(runtime.live_callback_owners(), 0);
}

#[test]
fn real_library_explain_output_cap_deadline_and_destructor_lifetime() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let mut options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    let rows = crate::raw_memory::SharedRawRows::test_rows(vec![stored(1, 1.0, 1, 0)]);
    let baseline_refs = rows.strong_count();
    let (tables, snapshot) = resident(rows.clone(), vec![], vec![]);
    assert_eq!(rows.strong_count(), baseline_refs + 1);

    let plan = native(
        &runtime,
        &tables,
        &snapshot,
        "EXPLAIN SELECT value FROM metrics",
        &options,
        &QueryCatalog::default(),
    );
    assert_eq!(plan.as_array().unwrap().len(), 1);
    assert!(!plan[0]["plan"].as_str().unwrap().is_empty());
    assert_eq!(runtime.live_callback_owners(), 0);

    options.max_output_bytes = serde_json::to_vec(&plan).unwrap().len() - 1;
    let error = runtime
        .execute(
            &tables,
            Some(&snapshot),
            "EXPLAIN SELECT value FROM metrics",
            &options,
            &QueryCatalog::default(),
            &native_budget(),
            &AtomicBool::new(false),
        )
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("output exceeded"),
        "{error:#}"
    );
    assert_eq!(runtime.live_callback_owners(), 0);

    options.max_output_bytes = 8;
    let error = runtime
        .execute(
            &tables,
            Some(&snapshot),
            "SELECT repeat('雪', 1000) AS text",
            &options,
            &QueryCatalog::default(),
            &native_budget(),
            &AtomicBool::new(false),
        )
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("output exceeded"),
        "{error:#}"
    );
    assert_eq!(runtime.live_callback_owners(), 0);

    options.max_output_bytes = 8 * 1024 * 1024;
    options.timeout_ms = 25;
    let error = runtime
        .execute(
            &tables,
            Some(&snapshot),
            "SELECT sum(sin(i::DOUBLE)) FROM range(1000000000000) AS values(i)",
            &options,
            &QueryCatalog::default(),
            &native_budget(),
            &AtomicBool::new(false),
        )
        .unwrap_err();
    assert!(format!("{error:#}").contains("timed out"), "{error:#}");
    assert_eq!(runtime.live_callback_owners(), 0);

    let cancelled = AtomicBool::new(true);
    let error = runtime
        .execute(
            &tables,
            Some(&snapshot),
            "SELECT * FROM metrics",
            &options,
            &QueryCatalog::default(),
            &native_budget(),
            &cancelled,
        )
        .unwrap_err();
    assert!(format!("{error:#}").contains("cancelled"), "{error:#}");
    cancelled.store(false, Ordering::Release);
    drop((tables, snapshot));
    assert_eq!(rows.strong_count(), baseline_refs);
    assert_eq!(runtime.live_callback_owners(), 0);
}

#[test]
fn real_library_temporal_decimal_and_extended_scalars_match_cli() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    let rows = crate::raw_memory::SharedRawRows::test_rows(vec![stored(1, 1.0, 1, 0)]);
    let (tables, snapshot) = resident(rows, vec![], vec![]);
    let oracle = oracle_tables(&tables, &snapshot);

    for sql in [
        "SELECT DATE '2024-02-29' AS date_value, TIME '12:34:56.123456' AS time_value, TIMESTAMP_S '2024-02-29 12:34:56' AS timestamp_s_value, TIMESTAMP_MS '2024-02-29 12:34:56.123' AS timestamp_ms_value, TIMESTAMP '2024-02-29 12:34:56.123456' AS timestamp_value, TIMESTAMP_NS '2024-02-29 12:34:56.123456789' AS timestamp_ns_value, TIMESTAMPTZ '2024-02-29 12:34:56.123456+02:30' AS timestamptz_value, INTERVAL '1 year 2 months 3 days 04:05:06.000007' AS interval_value",
        "SELECT 12345678901234567890123456789012.345678::DECIMAL(38, 6) AS decimal_value, -0.000001::DECIMAL(38, 6) AS negative_decimal, '00112233-4455-6677-8899-aabbccddeeff'::UUID AS uuid_value, from_hex('00ff275c0a') AS blob_value, '101001'::BIT AS bit_value, '12345678901234567890123456789012345678901234567890'::BIGNUM AS bignum_value, 'happy'::ENUM ('sad', 'happy', '雪') AS enum_value",
        "SELECT 1.25 AS decimal_literal, 0.1::FLOAT AS float_value, 1.23456789::FLOAT AS rounded_float",
        "SELECT '{\"nested\":[1,null,\"雪\"]}'::JSON AS json_value, DATE 'infinity' AS infinite_date",
        "SELECT NULL::DATE AS null_date, NULL::TIMESTAMPTZ AS null_timestamptz, NULL::INTERVAL AS null_interval, NULL::DECIMAL(38, 9) AS null_decimal, NULL::UUID AS null_uuid, NULL::BLOB AS null_blob, NULL::BIGNUM AS null_bignum",
    ] {
        let expected = execute(&oracle, sql, &options).unwrap();
        let actual = native(
            &runtime,
            &tables,
            &snapshot,
            sql,
            &options,
            &QueryCatalog::default(),
        );
        assert_eq!(actual, expected, "{sql}");
    }
    assert_eq!(runtime.live_callback_owners(), 0);
}

#[test]
fn real_library_nested_and_empty_typed_results_match_cli() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    let rows = crate::raw_memory::SharedRawRows::test_rows(vec![stored(1, 1.0, 1, 0)]);
    let (tables, snapshot) = resident(rows, vec![], vec![]);
    let oracle = oracle_tables(&tables, &snapshot);

    for sql in [
        "SELECT [1, NULL, 3]::INTEGER[] AS list_value, [1, NULL, 3]::INTEGER[3] AS array_value, {'name': '雪', 'values': [1, NULL]::INTEGER[], 'empty': []::INTEGER[]} AS struct_value, map(['alpha', 'beta'], [[1, NULL]::INTEGER[], []::INTEGER[]]) AS map_value, union_value(text := '雪') AS union_value",
        "SELECT [{'key': 'a', 'values': [1, NULL]::INTEGER[]}, NULL, {'key': 'b', 'values': []::INTEGER[]}] AS nested_value, {'inside': {'items': [DATE '2024-02-29', NULL]::DATE[]}, 'maybe': NULL::STRUCT(note VARCHAR)} AS nested_struct, map(['first', 'second'], [{'n': 1, 's': NULL::VARCHAR}, {'n': 2, 's': '雪'}]) AS struct_map",
        "SELECT {'decimal': 1.25::DECIMAL(5,2), 'huge': 123456789012345678901234567890::HUGEINT, 'float': 3.125::DOUBLE} AS nested_numeric",
        "SELECT {'float32': 0.1::FLOAT, 'unsigned': 18446744073709551615::UBIGINT, 'bignum': '12345678901234567890123456789012345678901234567890'::BIGNUM, 'json': '{\"a\":1}'::JSON} AS extended_nested",
        "SELECT NULL::INTEGER[] AS null_list, NULL::INTEGER[3] AS null_array, NULL::STRUCT(name VARCHAR, values INTEGER[]) AS null_struct, NULL::MAP(VARCHAR, INTEGER[]) AS null_map, []::UUID[] AS empty_list, map([]::VARCHAR[], []::INTEGER[]) AS empty_map, {'items': []::UUID[]} AS empty_struct",
        "SELECT []::DATE[] AS empty_list, map([]::VARCHAR[], []::INTEGER[]) AS empty_map, {'items': []::UUID[]} AS empty_struct WHERE false",
    ] {
        let expected = execute(&oracle, sql, &options).unwrap();
        let actual = native(
            &runtime,
            &tables,
            &snapshot,
            sql,
            &options,
            &QueryCatalog::default(),
        );
        assert_eq!(actual, expected, "{sql}");
    }
    assert_eq!(runtime.live_callback_owners(), 0);
}
#[test]
fn native_scratch_handles_large_escaped_tags_without_callback_growth() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let mut options = options(cli);
    options.threads = 2;
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    let mut row = stored(1, 1.0, 1, 0);
    row.row.tags.clear();
    for index in 0..32 {
        row.row
            .tags
            .insert(format!("tag-{index:02}"), "\u{1}".repeat(1024));
    }
    let encoded = serde_json::to_string(&row.row.tags).unwrap();
    assert!(encoded.len() > 64 * 1024);
    let rows = crate::raw_memory::SharedRawRows::test_rows(vec![row]);
    assert_eq!(rows.max_tags_json_bytes(), encoded.len());
    let (tables, snapshot) = resident(rows, vec![], vec![]);
    let budget = native_budget();
    let result = runtime
        .execute(
            &tables,
            Some(&snapshot),
            "SELECT tags FROM metrics",
            &options,
            &QueryCatalog::default(),
            &budget,
            &AtomicBool::new(false),
        )
        .unwrap();
    assert_eq!(result, json!([{ "tags": encoded.clone() }]));
    let usage = runtime.last_scratch_usage().unwrap();
    assert_eq!(usage.tag_capacity_per_slot, encoded.len());
    assert_eq!(usage.max_tag_json_bytes, encoded.len());
    assert_eq!(usage.position_capacity_per_slot, 1024);
    assert!(usage.max_concurrent_callbacks <= options.threads);
    assert_eq!(usage.available_slots_after_close, options.threads);
    assert_eq!(usage.owner_live_after_close, 0);
    assert_eq!(usage.owner_bytes_after_close, 0);
    assert_eq!(budget.status().reserved_bytes, 0);
    assert_eq!(budget.status().working_bytes, 0);
}

#[test]
fn native_scratch_is_row_count_independent_and_batch_metadata_is_exact() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    let catalog = QueryCatalog::default();

    let mut fixed = Vec::new();
    for row_count in [1_usize, 4097] {
        let rows = crate::raw_memory::SharedRawRows::test_rows(
            (0..row_count)
                .map(|index| stored(index as i64, index as f64, 1, index as u32))
                .collect(),
        );
        let (tables, snapshot) = resident(rows, vec![], vec![]);
        let budget = native_budget();
        runtime
            .execute(
                &tables,
                Some(&snapshot),
                "SELECT count(*) AS n FROM metrics",
                &options,
                &catalog,
                &budget,
                &AtomicBool::new(false),
            )
            .unwrap();
        let usage = runtime.last_scratch_usage().unwrap();
        assert_eq!(usage.raw_batch_capacity, 1);
        assert_eq!(usage.column_capacity, 24);
        assert_eq!(usage.owner_live_after_close, 0);
        assert_eq!(budget.status().reserved_bytes, 0);
        fixed.push(usage.reserved_fixed_bytes);
    }
    assert_eq!(fixed[0], fixed[1]);

    let shared = crate::raw_memory::SharedRawRows::test_rows(vec![stored(1, 1.0, 1, 0)]);
    let batch_count = 1025_usize;
    let batches = (0..batch_count)
        .map(|index| ResidentBatch::new(format!("batch-{index}"), shared.clone(), 1024))
        .collect::<Vec<_>>();
    let ids = batches.iter().map(|batch| batch.id.clone()).collect();
    let tables = vec![QueryTable {
        name: "metrics".into(),
        hot: vec![],
        files: vec![],
        rollups: vec![],
        cutoff_us: None,
    }];
    let snapshot = ResidentSnapshot {
        namespace: "many-native-batches".into(),
        sequence: 1,
        tables: vec![ResidentTable {
            name: "metrics".into(),
            batches,
            files: vec![],
        }],
        lineage: vec![ResidentLineage {
            name: "metrics".into(),
            raw_stamp: 1,
            ids,
        }],
    };
    let budget = native_budget();
    runtime
        .execute(
            &tables,
            Some(&snapshot),
            "SELECT count(*) AS n FROM metrics",
            &options,
            &catalog,
            &budget,
            &AtomicBool::new(false),
        )
        .unwrap();
    let usage = runtime.last_scratch_usage().unwrap();
    assert_eq!(usage.raw_batch_capacity, batch_count);
    assert_eq!(usage.available_slots_after_close, options.threads);
    assert_eq!(budget.status().reserved_bytes, 0);
}

#[test]
fn native_zero_rows_and_repeated_bindings_refund_actual_owner_leases() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    let catalog = QueryCatalog::default();

    let empty = crate::raw_memory::SharedRawRows::test_rows(vec![]);
    let (tables, snapshot) = resident(empty, vec![], vec![]);
    let budget = native_budget();
    runtime
        .execute(
            &tables,
            Some(&snapshot),
            "SELECT count(*) AS n FROM metrics",
            &options(cli.clone()),
            &catalog,
            &budget,
            &AtomicBool::new(false),
        )
        .unwrap();
    let usage = runtime.last_scratch_usage().unwrap();
    assert_eq!(usage.tag_capacity_per_slot, 0);
    assert_eq!(usage.max_position_rows, 0);
    assert_eq!(usage.owner_live_after_close, 0);
    assert_eq!(budget.status().reserved_bytes, 0);

    let rows = crate::raw_memory::SharedRawRows::test_rows(
        (0..256)
            .map(|index| stored(index, index as f64, 1, index as u32))
            .collect(),
    );
    let (tables, snapshot) = resident(rows, vec![], vec![]);
    for threads in [1_usize, 2] {
        let mut query_options = options(cli.clone());
        query_options.threads = threads;
        let budget = native_budget();
        runtime
            .execute(
                &tables,
                Some(&snapshot),
                "SELECT count(*) AS n FROM metrics a JOIN metrics b USING (timestamp_us) UNION ALL SELECT count(*) AS n FROM metrics UNION ALL SELECT count(*) AS n FROM metrics",
                &query_options,
                &catalog,
                &budget,
                &AtomicBool::new(false),
            )
            .unwrap();
        let usage = runtime.last_scratch_usage().unwrap();
        assert!(usage.owner_live_highwater > usage.scanner_count);
        assert!(usage.owner_bytes_highwater > 0);
        assert!(usage.max_concurrent_callbacks <= threads);
        assert_eq!(usage.available_slots_after_close, threads);
        assert_eq!(usage.owner_live_after_close, 0);
        assert_eq!(usage.owner_bytes_after_close, 0);
        assert_eq!(budget.status().reserved_bytes, 0);
        assert_eq!(budget.status().working_bytes, 0);
    }
}
