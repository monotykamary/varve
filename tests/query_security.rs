use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use varve::model::{Row, StoredRow};
use varve::query::{QueryOptions, QueryTable, execute, version};
use varve::segment;

fn options() -> QueryOptions {
    QueryOptions {
        executable: PathBuf::from("duckdb"),
        timeout_ms: 5_000,
        ..QueryOptions::default()
    }
}

fn row(timestamp_us: i64, value: f64, sequence: u64) -> StoredRow {
    StoredRow {
        row: Row {
            timestamp_us,
            tenant: "tenant".to_string(),
            series: "cpu".to_string(),
            value,
            tags: BTreeMap::new(),
        },
        sequence,
        ordinal: 0,
    }
}

fn table(selected: PathBuf) -> QueryTable {
    QueryTable {
        name: "metrics".to_string(),
        hot: vec![row(20, 2.0, 2)],
        files: vec![selected],
        rollups: Vec::new(),
        cutoff_us: None,
    }
}

fn sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

#[test]
fn real_v2_worker_locks_security_settings_and_scrubs_environment() {
    assert!(version(&options()).unwrap().starts_with("v2."));
    let result = execute(
        &[],
        "SELECT current_setting('enable_external_access') AS external_access, \
         current_setting('lock_configuration') AS locked, \
         current_setting('autoload_known_extensions') AS autoload, \
         current_setting('autoinstall_known_extensions') AS autoinstall, \
         current_setting('allow_community_extensions') AS community",
        &options(),
    )
    .unwrap();
    let settings = &result[0];
    assert_eq!(settings["external_access"], false);
    assert_eq!(settings["locked"], true);
    assert_eq!(settings["autoload"], false);
    assert_eq!(settings["autoinstall"], false);
    assert_eq!(settings["community"], false);

    for variable in ["USER", "AWS_SECRET_ACCESS_KEY", "HOME"] {
        let sql = format!("SELECT getenv('{variable}')");
        let error = execute(&[], &sql, &options()).unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("getenv is disabled through configuration"),
            "unexpected getenv denial for {variable}: {message}"
        );
    }
}

#[test]
fn exact_path_allowlist_keeps_hot_and_selected_cold_reads_working() {
    let directory = TempDir::new().unwrap();
    let selected = directory.path().join("selected.parquet");
    segment::write(&selected, &[row(10, 1.0, 1)]).unwrap();
    let result = execute(
        &[table(selected.clone())],
        "SELECT timestamp_us, value FROM metrics ORDER BY timestamp_us",
        &options(),
    )
    .unwrap();
    assert_eq!(
        result,
        serde_json::json!([
            {"timestamp_us": 10, "value": 1.0},
            {"timestamp_us": 20, "value": 2.0}
        ])
    );

    let direct = format!(
        "SELECT count(*) AS count FROM read_parquet('{}')",
        sql_path(&selected)
    );
    assert_eq!(
        execute(&[table(selected)], &direct, &options()).unwrap(),
        serde_json::json!([{"count": 1}])
    );
}

#[test]
fn real_engine_denies_host_files_proc_remote_urls_and_unselected_files() {
    let directory = TempDir::new().unwrap();
    let selected = directory.path().join("selected.parquet");
    let unselected = directory.path().join("unselected.parquet");
    let unselected_database = directory.path().join("unselected.duckdb");
    segment::write(&selected, &[row(10, 1.0, 1)]).unwrap();
    segment::write(&unselected, &[row(30, 3.0, 3)]).unwrap();
    std::fs::write(&unselected_database, b"not exposed").unwrap();
    let metrics = table(selected);

    let attempts = [
        "SELECT * FROM read_text('/etc/passwd')".to_string(),
        "SELECT * FROM read_blob('/proc/1/environ')".to_string(),
        "SELECT * FROM read_csv('https://example.invalid/secret.csv')".to_string(),
        format!("SELECT * FROM read_parquet('{}')", sql_path(&unselected)),
        format!(
            "SELECT * FROM read_blob('{}')",
            sql_path(&unselected_database)
        ),
    ];
    for sql in attempts {
        let error = execute(std::slice::from_ref(&metrics), &sql, &options()).unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("external access")
                || message.contains("disabled")
                || message.contains("not allowed"),
            "unexpected denial for {sql:?}: {message}"
        );
    }
}

#[test]
fn select_wrapped_dynamic_mutations_are_denied_without_file_side_effects() {
    assert!(
        version(&options())
            .unwrap()
            .starts_with("v2.0.0-alpha41533"),
        "security probes require the pinned DuckDB alpha"
    );

    let directory = TempDir::new().unwrap();
    let selected = directory.path().join("selected.parquet");
    let attached = directory.path().join("attached.duckdb");
    segment::write(&selected, &[row(10, 1.0, 1)]).unwrap();
    let checksum = blake3::hash(&std::fs::read(&selected).unwrap());
    let metrics = table(selected.clone());

    let attempts = [
        format!(
            "COPY (SELECT 99 AS value) TO '{}' (FORMAT PARQUET)",
            sql_path(&selected)
        ),
        "CREATE TABLE injected(value INTEGER)".to_string(),
        "SET threads = 1".to_string(),
        "INSTALL json".to_string(),
        format!("ATTACH '{}' AS injected", sql_path(&attached)),
        "CREATE SECRET injected (TYPE S3, KEY_ID 'not-a-real-key')".to_string(),
        format!(
            "SELECT 1; COPY (SELECT 100) TO '{}' (FORMAT PARQUET)",
            sql_path(&selected)
        ),
    ];

    for inner in attempts {
        let sql = format!("SELECT * FROM query('{}')", inner.replace('\'', "''"));
        let error = execute(std::slice::from_ref(&metrics), &sql, &options()).unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("Expected a single SELECT statement"),
            "unexpected dynamic-statement denial for {inner:?}: {message}"
        );
        assert_eq!(
            blake3::hash(&std::fs::read(&selected).unwrap()),
            checksum,
            "selected input changed after {inner:?}"
        );
        assert!(!attached.exists(), "ATTACH created a database file");
    }

    assert_eq!(
        execute(
            std::slice::from_ref(&metrics),
            "SELECT count(*) AS count, sum(value) AS total FROM metrics",
            &options(),
        )
        .unwrap(),
        serde_json::json!([{"count": 2, "total": 3.0}])
    );
    assert_eq!(
        execute(
            &[metrics],
            &format!(
                "SELECT count(*) AS count, sum(value) AS total FROM read_parquet('{}')",
                sql_path(&selected)
            ),
            &options(),
        )
        .unwrap(),
        serde_json::json!([{"count": 1, "total": 1.0}])
    );
}

#[test]
fn cli_dot_command_injection_is_rejected_before_the_cli_runs_it() {
    let directory = TempDir::new().unwrap();
    let output = directory.path().join("injected-output.json");
    let sql = format!("SELECT 1 AS safe\n  .output '{}'", sql_path(&output));
    let error = execute(&[], &sql, &options()).unwrap_err();
    assert_eq!(error.to_string(), "DuckDB CLI dot commands are not allowed");
    assert!(!output.exists(), "CLI .output command created a file");
}

#[test]
fn dynamic_proc_and_environment_reads_stay_denied_and_runtime_state_is_private() {
    let attempts = [
        (
            "SELECT * FROM query('SELECT getenv(''AWS_SECRET_ACCESS_KEY'') AS leaked')",
            "getenv is disabled through configuration",
        ),
        (
            "SELECT * FROM query('SELECT * FROM read_blob(''/proc/self/environ'')')",
            "file system operations are disabled by configuration",
        ),
    ];
    for (sql, expected) in attempts {
        let error = execute(&[], sql, &options()).unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains(expected),
            "unexpected denial for {sql:?}: {message}"
        );
    }

    assert_eq!(
        execute(
            &[],
            "SELECT * FROM query('SELECT * FROM duckdb_secrets()')",
            &options(),
        )
        .unwrap(),
        serde_json::json!([])
    );
    assert_eq!(
        execute(
            &[],
            "SELECT count(*) AS variables FROM duckdb_variables()",
            &options(),
        )
        .unwrap(),
        serde_json::json!([{"variables": 0}])
    );
    assert_eq!(
        execute(
            &[],
            "SELECT string_agg(database_name, ',' ORDER BY database_name) AS names, \
             count(path) AS path_count FROM duckdb_databases()",
            &options(),
        )
        .unwrap(),
        serde_json::json!([{"names": "memory,system,temp", "path_count": 0}])
    );
}

#[cfg(unix)]
#[test]
fn exact_allowlist_rejects_symlink_and_traversal_aliases_to_unselected_files() {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new().unwrap();
    let selected = directory.path().join("selected.parquet");
    let unselected = directory.path().join("unselected.parquet");
    let unselected_link = directory.path().join("unselected-link.parquet");
    let child = directory.path().join("child");
    std::fs::create_dir(&child).unwrap();
    segment::write(&selected, &[row(10, 1.0, 1)]).unwrap();
    segment::write(&unselected, &[row(30, 30.0, 3)]).unwrap();
    symlink(&unselected, &unselected_link).unwrap();
    let traversal = child.join("..").join("unselected.parquet");
    let metrics = table(selected.clone());

    for path in [&unselected_link, &traversal] {
        let sql = format!("SELECT * FROM read_parquet('{}')", sql_path(path));
        let error = execute(std::slice::from_ref(&metrics), &sql, &options()).unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("file system operations are disabled by configuration"),
            "unexpected path denial for {}: {message}",
            path.display()
        );
    }

    assert_eq!(
        execute(
            &[metrics],
            &format!(
                "SELECT sum(value) AS total FROM read_parquet('{}')",
                sql_path(&selected)
            ),
            &options(),
        )
        .unwrap(),
        serde_json::json!([{"total": 1.0}])
    );
}

#[test]
fn dynamic_select_cannot_evade_the_worker_deadline() {
    assert_eq!(
        execute(
            &[],
            "SELECT * FROM query('SELECT 42 AS answer')",
            &options(),
        )
        .unwrap(),
        serde_json::json!([{"answer": 42}])
    );

    let mut bounded = options();
    bounded.timeout_ms = 50;
    let error = execute(
        &[],
        "SELECT * FROM query('SELECT sum(sin(i::DOUBLE)) FROM range(1000000000000) AS values(i)')",
        &bounded,
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "DuckDB query timed out");
}
