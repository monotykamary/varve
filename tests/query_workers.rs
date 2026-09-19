#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use varve::metrics::Metrics;
use varve::model::{RollupRow, Row, StoredRow};
use varve::query::{CatalogRelation, QueryCatalog, QueryOptions, QueryRuntime, QueryTable};
use varve::segment;

#[test]
fn stateless_time_series_windows_reuse_the_actual_child() {
    let fixture = Fixture::new(false);
    let runtime = QueryRuntime::new(1);
    let sql = "SELECT row_number() OVER (ORDER BY i) AS rn, lag(i) OVER (ORDER BY i) AS previous FROM range(4) AS x(i) ORDER BY i";
    let expected = serde_json::json!([
        {"rn":1,"previous":null}, {"rn":2,"previous":0},
        {"rn":3,"previous":1}, {"rn":4,"previous":2}
    ]);
    for _ in 0..2 {
        assert_eq!(
            runtime
                .execute_with_catalog(&[], sql, &fixture.options, &QueryCatalog::default())
                .unwrap(),
            expected
        );
    }
    assert_eq!(runtime.stats().spawned, 1);
    assert_eq!(runtime.stats().reused, 1);
    assert_eq!(fs::read_to_string(&fixture.log).unwrap().lines().count(), 1);
}

struct Fixture {
    directory: TempDir,
    options: QueryOptions,
    log: PathBuf,
}

impl Fixture {
    fn new(gated: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("children");
        let executable = directory.path().join("duckdb-wrapper");
        let duckdb = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb");
        assert!(duckdb.is_file(), "actual .tools/duckdb is required");
        let gate = directory.path().join("gate");
        let wait = if gated {
            assert!(
                Command::new("mkfifo")
                    .arg(&gate)
                    .status()
                    .unwrap()
                    .success()
            );
            format!("read -r release < '{}'\n", gate.display())
        } else {
            String::new()
        };
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nprintf '%s %s\\n' \"$$\" \"$PWD\" >> '{}'\n{wait}exec '{}' \"$@\"\n",
                log.display(),
                duckdb.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            directory,
            log,
            options: QueryOptions {
                executable,
                timeout_ms: 5_000,
                ..QueryOptions::default()
            },
        }
    }

    fn children(&self) -> Vec<(u32, PathBuf)> {
        fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .map(|line| {
                let (pid, path) = line.split_once(' ').unwrap();
                (pid.parse().unwrap(), PathBuf::from(path))
            })
            .collect()
    }

    fn wait_for_children(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.children().len() < count {
            assert!(Instant::now() < deadline, "child did not start");
            thread::sleep(Duration::from_millis(2));
        }
    }

    fn release(&self) {
        fs::OpenOptions::new()
            .write(true)
            .open(self.directory.path().join("gate"))
            .unwrap()
            .write_all(b"go\n")
            .unwrap();
    }
}

fn alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}

fn table(name: &str, value: f64) -> QueryTable {
    QueryTable {
        name: name.into(),
        hot: vec![StoredRow {
            row: Row {
                timestamp_us: i64::MIN,
                tenant: "tenant'\n雪".into(),
                series: "series".into(),
                value,
                tags: BTreeMap::from([("nul".into(), "a\nb雪".into())]),
            },
            sequence: u64::MAX,
            ordinal: u32::MAX,
        }],
        files: Vec::new(),
        rollups: Vec::new(),
        cutoff_us: None,
    }
}

fn catalog(value: &str) -> QueryCatalog {
    QueryCatalog {
        relations: vec![CatalogRelation {
            name: "catalog_probe".into(),
            columns: vec![
                ("text".into(), "VARCHAR".into()),
                ("unsigned".into(), "UBIGINT".into()),
                ("optional".into(), "DOUBLE".into()),
            ],
            rows: vec![serde_json::json!([value, u64::MAX, null])],
        }],
        aggregates: Vec::new(),
    }
}

#[test]
fn stateful_quoted_dynamic_and_unknown_sql_use_disposable_children() {
    let fixture = Fixture::new(false);
    let runtime = QueryRuntime::new(2);
    let catalog = QueryCatalog::default();
    runtime
        .execute_with_catalog(&[], "SELECT 42", &fixture.options, &catalog)
        .unwrap();
    let pooled = fixture.children()[0].clone();
    for sql in [
        "SELECT setseed(0.25)",
        "SELECT \"setseed\"(0.25)",
        "SELECT * FROM query('SELECT setseed(0.25)')",
        "SELECT * FROM \"query\"($$SELECT \"setseed\"(0.25)$$)",
        "SELECT random() AS value",
        "SELECT md5('still supported') AS value",
        "SELECT 1 AS value UNION ALL SELECT 2",
    ] {
        let before = runtime.stats();
        let result = runtime
            .execute_with_catalog(&[], sql, &fixture.options, &catalog)
            .unwrap();
        assert!(!result.as_array().unwrap().is_empty(), "{sql}");
        let after = runtime.stats();
        assert_eq!(after.spawned, before.spawned + 1, "fresh: {sql}");
        assert_eq!(
            after.reused, before.reused,
            "must not acquire pooled child: {sql}"
        );
        assert_eq!(after.resets, before.resets + 1);
        assert_eq!(after.discarded, before.discarded + 1);
        assert_eq!((after.active, after.idle), (0, 1));
        let fresh = fixture.children().last().unwrap().clone();
        assert_ne!(fresh.0, pooled.0);
        assert!(!alive(fresh.0), "fresh-only child was not reaped: {sql}");
        assert!(!fresh.1.exists());
        assert!(
            alive(pooled.0),
            "spare capacity must preserve reusable child"
        );
        assert_eq!(
            runtime
                .execute_with_catalog(&[], "SELECT 42 AS value", &fixture.options, &catalog)
                .unwrap()[0]["value"],
            42
        );
        assert_eq!(runtime.stats().reused, after.reused + 1);
    }
    drop(runtime);
    assert!(!alive(pooled.0));
    assert!(!pooled.1.exists());
}

#[test]
fn disposable_workers_share_output_deadline_and_failure_cleanup() {
    let fixture = Fixture::new(false);
    let runtime = QueryRuntime::new(2);
    let catalog = QueryCatalog::default();
    runtime
        .execute_with_catalog(&[], "SELECT 1", &fixture.options, &catalog)
        .unwrap();
    let pooled = fixture.children()[0].clone();
    let mut bounded = fixture.options.clone();
    bounded.max_output_bytes = 32;
    let error = runtime
        .execute_with_catalog(
            &[],
            "SELECT * FROM query('SELECT repeat(''x'', 4096) AS text')",
            &bounded,
            &catalog,
        )
        .unwrap_err();
    assert!(error.to_string().contains("output exceeded"), "{error:#}");
    // A deadline probe, not a load benchmark. The dynamic SQL must stay on the
    // same cancellable pipe loop and cannot delegate to standalone execution.
    bounded = fixture.options.clone();
    bounded.timeout_ms = 100;
    let error = runtime
        .execute_with_catalog(
            &[],
            "SELECT * FROM query('SELECT sum(sin(i::DOUBLE)) FROM range(1000000000000) AS t(i)')",
            &bounded,
            &catalog,
        )
        .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error:#}");
    let stats = runtime.stats();
    assert_eq!(
        (stats.spawned, stats.reused, stats.resets, stats.discarded),
        (3, 0, 1, 2)
    );
    assert_eq!((stats.active, stats.idle), (0, 1));
    assert!(alive(pooled.0));
    for (pid, path) in fixture.children().iter().skip(1) {
        assert!(!alive(*pid));
        assert!(!path.exists());
    }
    runtime
        .execute_with_catalog(&[], "SELECT 1", &fixture.options, &catalog)
        .unwrap();
    assert_eq!(runtime.stats().reused, 1);
}

#[test]
fn atomic_same_path_executable_replacement_changes_worker_generation() {
    use std::os::unix::fs::MetadataExt;
    let fixture = Fixture::new(false);
    let runtime = QueryRuntime::new(1);
    let query = || {
        runtime
            .execute_with_catalog(
                &[],
                "SELECT 42 AS value",
                &fixture.options,
                &QueryCatalog::default(),
            )
            .unwrap()
    };
    assert_eq!(query()[0]["value"], 42);
    let old = fixture.children()[0].clone();
    let old_inode = fs::metadata(&fixture.options.executable).unwrap().ino();
    let replacement = fixture.directory.path().join("replacement");
    let marker = fixture.directory.path().join("replacement-executed");
    let script = fs::read_to_string(&fixture.options.executable)
        .unwrap()
        .replace(
            "exec '",
            &format!(
                "printf 'new generation\\n' > '{}'\nexec '",
                marker.display()
            ),
        );
    fs::write(&replacement, script).unwrap();
    fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
    fs::rename(&replacement, &fixture.options.executable).unwrap();
    assert_ne!(
        old_inode,
        fs::metadata(&fixture.options.executable).unwrap().ino()
    );
    assert_eq!(query()[0]["value"], 42);
    assert_eq!(fs::read_to_string(marker).unwrap(), "new generation\n");
    assert!(!alive(old.0));
    assert!(!old.1.exists());
    assert_eq!(query()[0]["value"], 42);
    let stats = runtime.stats();
    assert_eq!(
        (stats.spawned, stats.reused, stats.discarded, stats.idle),
        (2, 1, 1, 1)
    );
    assert_eq!(fixture.children().len(), 2);
}

#[test]
fn capacity_two_keeps_alternating_keys_and_prefers_other_key_for_fresh_eviction() {
    let fixture = Fixture::new(false);
    let runtime = QueryRuntime::new(2);
    let mut other = fixture.options.clone();
    other.threads += 1;
    for options in [&fixture.options, &other, &fixture.options, &other] {
        runtime
            .execute_with_catalog(&[], "SELECT 1", options, &QueryCatalog::default())
            .unwrap();
    }
    let children = fixture.children();
    assert_eq!(children.len(), 2);
    assert!(children.iter().all(|(pid, _)| alive(*pid)));
    let stats = runtime.stats();
    assert_eq!(
        (stats.spawned, stats.reused, stats.discarded, stats.idle),
        (2, 2, 0, 2)
    );
    runtime
        .execute_with_catalog(
            &[],
            "SELECT setseed(0.25)",
            &fixture.options,
            &QueryCatalog::default(),
        )
        .unwrap();
    assert!(
        alive(children[0].0),
        "fresh work should preserve its matching reusable key"
    );
    assert!(!alive(children[1].0));
    assert!(!alive(fixture.children()[2].0));
    assert_eq!((runtime.stats().idle, runtime.stats().discarded), (1, 2));
    runtime
        .execute_with_catalog(&[], "SELECT 1", &fixture.options, &QueryCatalog::default())
        .unwrap();
    assert_eq!(runtime.stats().reused, 3);
}

#[test]
fn concurrent_key_replacement_and_fresh_admission_never_exceed_capacity() {
    let warm = Fixture::new(false);
    let runtime = Arc::new(QueryRuntime::new(2));
    let mut other = warm.options.clone();
    other.threads += 1;
    for options in [&warm.options, &other] {
        runtime
            .execute_with_catalog(&[], "SELECT 1", options, &QueryCatalog::default())
            .unwrap();
    }
    let old = warm.children();
    assert_eq!(old.len(), 2);
    let fixtures = [Fixture::new(true), Fixture::new(true)];
    // Observe actual old/new PIDs at wrapper entry, before publishing the new
    // child's ready log. The FIFO then holds each child for the admission probe.
    // Count growing (gated) new PIDs before shrinking old PIDs, so a concurrent
    // replacement cannot be counted twice across this non-atomic observation.
    let logs = [&fixtures[0].log, &fixtures[1].log, &warm.log]
        .iter()
        .map(|path| format!("'{}'", path.display()))
        .collect::<Vec<_>>()
        .join(" ");
    for fixture in &fixtures {
        let script = fs::read_to_string(&fixture.options.executable).unwrap();
        let probe = format!(
            "#!/bin/sh\nlive=1\nfor log in {logs}; do\n  if test -f \"$log\"; then\n    while read -r pid rest; do\n      if kill -0 \"$pid\" 2>/dev/null; then live=$((live + 1)); fi\n    done < \"$log\"\n  fi\ndone\nprintf '%s\\n' \"$live\" > '{}'\n",
            fixture.directory.path().join("population").display()
        );
        fs::write(
            &fixture.options.executable,
            script.replacen("#!/bin/sh\n", &probe, 1),
        )
        .unwrap();
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    let start = Arc::new(std::sync::Barrier::new(3));
    let mut jobs = Vec::new();
    for (fixture, sql) in fixtures.iter().zip(["SELECT 42", "SELECT setseed(0.25)"]) {
        let runtime = runtime.clone();
        let options = fixture.options.clone();
        let cancelled = cancelled.clone();
        let start = start.clone();
        jobs.push(thread::spawn(move || {
            start.wait();
            runtime.execute_with_catalog_cancellable(
                &[],
                sql,
                &options,
                &QueryCatalog::default(),
                &cancelled,
            )
        }));
    }
    start.wait();
    for fixture in &fixtures {
        fixture.wait_for_children(1);
        let live: usize = fs::read_to_string(fixture.directory.path().join("population"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            (1..=2).contains(&live),
            "spawn observed {live} live children"
        );
    }
    let stats = runtime.stats();
    assert_eq!(
        (stats.active, stats.idle, stats.spawned, stats.discarded),
        (2, 0, 4, 2)
    );
    assert!(old.iter().all(|(pid, path)| !alive(*pid) && !path.exists()));
    assert!(
        fixtures
            .iter()
            .all(|fixture| alive(fixture.children()[0].0))
    );
    let denied = runtime
        .execute_with_catalog(
            &[],
            "SELECT setseed(0.25)",
            &warm.options,
            &QueryCatalog::default(),
        )
        .unwrap_err();
    assert!(denied.to_string().contains("capacity exhausted"));
    cancelled.store(true, Ordering::Release);
    for job in jobs {
        assert!(
            job.join()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }
    assert_eq!((runtime.stats().active, runtime.stats().idle), (0, 0));
    for fixture in &fixtures {
        assert!(
            fixture
                .children()
                .iter()
                .all(|(pid, path)| !alive(*pid) && !path.exists())
        );
    }
}

#[test]
fn actual_child_reuse_refreshes_exact_hot_catalog_and_empty_relations() {
    let fixture = Fixture::new(false);
    let metrics = Arc::new(Metrics::default());
    let runtime = QueryRuntime::with_metrics(1, metrics.clone());
    let value = f64::from_bits(0x3fd5555555555555);
    for (name, text, number) in [
        ("old", "old\0catalog", value),
        ("fresh", "new\0catalog", -value),
    ] {
        let result = runtime.execute_with_catalog(&[table(name, number)],
            &format!("SELECT timestamp_us, tenant, value, sequence, ordinal, text, unsigned, optional FROM {name} CROSS JOIN catalog_probe()"),
            &fixture.options, &catalog(text)).unwrap();
        assert_eq!(result[0]["timestamp_us"], i64::MIN);
        assert_eq!(result[0]["sequence"], u64::MAX.to_string());
        assert_eq!(result[0]["ordinal"], u32::MAX);
        assert_eq!(
            result[0]["value"].as_f64().unwrap().to_bits(),
            number.to_bits()
        );
        assert_eq!(result[0]["tenant"], "tenant'\n雪");
        assert_eq!(result[0]["text"], text);
        assert_eq!(result[0]["unsigned"], u64::MAX.to_string());
        assert!(result[0]["optional"].is_null());
    }
    let result = runtime.execute_with_catalog(&[],
        "SELECT count(*) AS stale FROM duckdb_tables() WHERE table_name IN ('old', 'fresh', '__varve_input')",
        &fixture.options, &QueryCatalog::default()).unwrap();
    // Only the current request's internal input exists; previous user tables are gone.
    assert_eq!(result[0]["stale"], 1);
    assert_eq!(runtime.execute_with_catalog(&[], "SELECT count(*) AS stale FROM duckdb_functions() WHERE function_name = 'catalog_probe'",
        &fixture.options, &QueryCatalog::default()).unwrap()[0]["stale"], 0);
    let stats = runtime.stats();
    assert_eq!(
        (stats.spawned, stats.reused, stats.resets, stats.discarded),
        (1, 3, 4, 0)
    );
    assert_eq!((stats.active, stats.idle), (0, 1));
    let phases = metrics.snapshot().phases;
    for phase in ["query_wait", "query_build", "query_run", "query_reset"] {
        assert_eq!(phases[phase].count, 4, "phase {phase}");
    }
    assert_eq!(phases["query_spawn"].count, 1);
    let children = fixture.children();
    assert_eq!(
        children.len(),
        1,
        "queries must reuse the actual exec'd child"
    );
    assert!(alive(children[0].0));
    assert_eq!(
        fs::read_dir(children[0].1.join("inputs")).unwrap().count(),
        0
    );
    drop(runtime);
    assert!(
        !alive(children[0].0),
        "pool drop must reap the actual child"
    );
    assert!(
        !children[0].1.exists(),
        "pool drop must remove the private directory"
    );
}

#[test]
fn rollups_cutoffs_and_catalog_types_match_standalone_across_reuse() {
    let fixture = Fixture::new(false);
    let runtime = QueryRuntime::new(1);
    let mut standalone = fixture.options.clone();
    standalone.executable = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb");
    for value in [f64::MIN_POSITIVE, f64::MAX] {
        let mut selected = table("measurements", value);
        let mut rollup = RollupRow::from_row(1, &selected.hot[0]).unwrap();
        rollup.count = 0;
        selected.rollups.push(rollup);
        let catalog = catalog("exact\0metadata");
        for sql in [
            "SELECT * FROM measurements",
            "SELECT * FROM measurements__rollup",
            "SELECT * FROM catalog_probe()",
        ] {
            let expected = varve::query::execute_with_catalog(
                std::slice::from_ref(&selected),
                sql,
                &standalone,
                &catalog,
            )
            .unwrap();
            assert_eq!(
                runtime
                    .execute_with_catalog(
                        std::slice::from_ref(&selected),
                        sql,
                        &fixture.options,
                        &catalog
                    )
                    .unwrap(),
                expected
            );
        }
        selected.cutoff_us = Some(i64::MIN + 1);
        assert_eq!(
            runtime
                .execute_with_catalog(
                    &[selected],
                    "SELECT * FROM measurements",
                    &fixture.options,
                    &catalog
                )
                .unwrap(),
            serde_json::json!([])
        );
    }
    assert_eq!(runtime.stats().spawned, 1);
    assert_eq!(runtime.stats().resets, 8);
}

#[test]
fn concurrent_snapshots_are_isolated_and_last_arc_drop_reaps_both_children() {
    let fixture = Fixture::new(false);
    let duckdb = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb");
    // Concurrent shell read builtins may consume each other's bytes from a shared
    // FIFO. Each child publishes its own ready gate before it enters the CLI.
    fs::write(&fixture.options.executable, format!(
        "#!/bin/sh\ngate='{}/gate.'$$\nmkfifo \"$gate\" || exit 1\nprintf '%s %s\\n' \"$$\" \"$PWD\" >> '{}'\nread -r release < \"$gate\"\nexec '{}' \"$@\"\n",
        fixture.directory.path().display(), fixture.log.display(), duckdb.display()
    )).unwrap();
    let runtime = Arc::new(QueryRuntime::new(2));
    let mut jobs = Vec::new();
    for value in [11.0, 22.0] {
        let runtime = runtime.clone();
        let options = fixture.options.clone();
        jobs.push(thread::spawn(move || {
            let result = runtime
                .execute_with_catalog(
                    &[table("measurements", value)],
                    "SELECT value FROM measurements",
                    &options,
                    &QueryCatalog::default(),
                )
                .unwrap();
            assert_eq!(result[0]["value"], value);
        }));
    }
    fixture.wait_for_children(2);
    assert_eq!(runtime.stats().active, 2);
    for (pid, _) in fixture.children() {
        fs::OpenOptions::new()
            .write(true)
            .open(fixture.directory.path().join(format!("gate.{pid}")))
            .unwrap()
            .write_all(b"go\n")
            .unwrap();
    }
    drop(runtime);
    for job in jobs {
        job.join().unwrap();
    }
    for (pid, path) in fixture.children() {
        assert!(!alive(pid));
        assert!(!path.exists());
    }
}

#[test]
fn immutable_file_set_and_settings_changes_replace_child_without_stale_access() {
    let fixture = Fixture::new(false);
    let runtime = QueryRuntime::new(1);
    let first = fixture.directory.path().join("first.parquet");
    let second = fixture.directory.path().join("second.parquet");
    segment::write(&first, &table("first", 1.0).hot).unwrap();
    segment::write(&second, &table("second", 2.0).hot).unwrap();
    let mut selected = table("measurements", 3.0);
    selected.hot.clear();
    selected.files = vec![first.clone()];
    for _ in 0..2 {
        assert_eq!(
            runtime
                .execute_with_catalog(
                    std::slice::from_ref(&selected),
                    "SELECT value FROM measurements",
                    &fixture.options,
                    &QueryCatalog::default()
                )
                .unwrap()[0]["value"],
            1.0
        );
    }
    assert_eq!(fixture.children().len(), 1);
    selected.files = vec![second];
    assert_eq!(
        runtime
            .execute_with_catalog(
                std::slice::from_ref(&selected),
                "SELECT value FROM measurements",
                &fixture.options,
                &QueryCatalog::default()
            )
            .unwrap()[0]["value"],
        2.0
    );
    let children = fixture.children();
    assert_eq!(children.len(), 2);
    assert!(!alive(children[0].0));
    let denied = runtime
        .execute_with_catalog(
            std::slice::from_ref(&selected),
            &format!("SELECT * FROM read_parquet('{}')", first.display()),
            &fixture.options,
            &QueryCatalog::default(),
        )
        .unwrap_err();
    assert!(format!("{denied:#}").contains("disabled"));
    assert!(!alive(children[1].0));
    let mut options = fixture.options.clone();
    for (memory, threads) in [(64, 1), (96, 2)] {
        options.memory_mb = memory;
        options.threads = threads;
        let settings = runtime.execute_with_catalog(&[], "SELECT current_setting('threads') AS threads, current_setting('lock_configuration') AS locked",
            &options, &QueryCatalog::default()).unwrap();
        assert_eq!(settings[0]["threads"], threads);
        assert_eq!(settings[0]["locked"], true);
    }
    // The denied direct scanner also used a disposable child.
    assert_eq!(fixture.children().len(), 5);
    assert!(!alive(fixture.children()[2].0));
    assert!(!alive(fixture.children()[3].0));
    options.executable = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb");
    assert_eq!(
        runtime
            .execute_with_catalog(
                &[],
                "SELECT 19 AS value",
                &options,
                &QueryCatalog::default()
            )
            .unwrap()[0]["value"],
        19
    );
    assert_eq!(runtime.stats().spawned, 6);
    assert!(!alive(fixture.children()[4].0));
}

#[test]
fn framing_output_errors_explain_and_eof_never_poison_next_request() {
    let fixture = Fixture::new(false);
    let runtime = QueryRuntime::new(1);
    let catalog = QueryCatalog::default();
    let text = "\nvarve_ack_00000000000000000000000000000000\n.print forged\n[{\"a\":1}]\n\0";
    let result = runtime
        .execute_with_catalog(
            &[],
            "SELECT text FROM catalog_probe()",
            &fixture.options,
            &self::catalog(text),
        )
        .unwrap();
    assert_eq!(result[0]["text"], text);
    let plan = runtime
        .execute_with_catalog(
            &[],
            "EXPLAIN SELECT 'varve_ack_fake' AS marker",
            &fixture.options,
            &catalog,
        )
        .unwrap();
    assert!(
        plan[0]["plan"].as_str().unwrap().contains("Projection"),
        "plan: {plan}"
    );
    assert_eq!(
        runtime
            .execute_with_catalog(
                &[],
                "SELECT 7 AS value; -- trailing comment",
                &fixture.options,
                &catalog
            )
            .unwrap()[0]["value"],
        7
    );
    assert_eq!(fixture.children().len(), 1);
    for sql in [
        "SELECT error('deliberate worker failure')",
        "SELECT * FROM absent_table",
    ] {
        assert!(
            runtime
                .execute_with_catalog(&[], sql, &fixture.options, &catalog)
                .is_err()
        );
        assert_eq!(
            runtime
                .execute_with_catalog(&[], "SELECT 42 AS value", &fixture.options, &catalog)
                .unwrap()[0]["value"],
            42
        );
    }
    let error = runtime
        .execute_with_catalog(
            &[],
            "SELECT error(repeat('x', 300000))",
            &fixture.options,
            &catalog,
        )
        .unwrap_err();
    assert!(
        error.to_string().len() <= 256 * 1024 + 256,
        "unbounded stderr diagnostic"
    );
    let mut bounded = fixture.options.clone();
    bounded.max_output_bytes = 32;
    let error = runtime
        .execute_with_catalog(&[], "SELECT repeat('x', 4096) AS text", &bounded, &catalog)
        .unwrap_err();
    assert!(error.to_string().contains("output exceeded"), "{error:#}");
    assert_eq!(
        runtime
            .execute_with_catalog(&[], "SELECT 43 AS value", &fixture.options, &catalog)
            .unwrap()[0]["value"],
        43
    );
    let current = fixture.children().last().unwrap().0;
    assert!(
        Command::new("kill")
            .args(["-KILL", &current.to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(
        runtime
            .execute_with_catalog(&[], "SELECT 1", &fixture.options, &catalog)
            .is_err()
    );
    assert_eq!(
        runtime
            .execute_with_catalog(&[], "SELECT 44 AS value", &fixture.options, &catalog)
            .unwrap()[0]["value"],
        44
    );
}

#[test]
fn admission_cancellation_deadline_and_concurrent_drop_cleanup() {
    let fixture = Fixture::new(true);
    let runtime = Arc::new(QueryRuntime::new(2));
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut jobs = Vec::new();
    for _ in 0..2 {
        let runtime = runtime.clone();
        let options = fixture.options.clone();
        let cancelled = cancelled.clone();
        jobs.push(thread::spawn(move || {
            runtime.execute_with_catalog_cancellable(
                &[],
                "SELECT 42 AS value",
                &options,
                &QueryCatalog::default(),
                &cancelled,
            )
        }));
    }
    fixture.wait_for_children(2);
    let denied = runtime
        .execute_with_catalog(&[], "SELECT 1", &fixture.options, &QueryCatalog::default())
        .unwrap_err();
    assert!(denied.to_string().contains("capacity exhausted"));
    cancelled.store(true, Ordering::Release);
    for job in jobs {
        assert!(
            job.join()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }
    for (pid, path) in fixture.children() {
        assert!(!alive(pid));
        assert!(!path.exists());
    }

    let runtime_clone = runtime.clone();
    let options = fixture.options.clone();
    let job = thread::spawn(move || {
        runtime_clone.execute_with_catalog(
            &[],
            "SELECT 42 AS value",
            &options,
            &QueryCatalog::default(),
        )
    });
    fixture.wait_for_children(3);
    fixture.release();
    assert_eq!(job.join().unwrap().unwrap()[0]["value"], 42);
    // A tiny correctness cancellation probe, not a local load test or benchmark.
    let mut deadline = fixture.options.clone();
    deadline.timeout_ms = 20;
    let error = runtime
        .execute_with_catalog(
            &[],
            "SELECT sum(sin(i::DOUBLE)) FROM range(1000000000000) AS values(i)",
            &deadline,
            &QueryCatalog::default(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error:#}");
    assert!(!alive(fixture.children()[2].0));
    drop(runtime);
    assert!(
        QueryRuntime::new(0)
            .execute_with_catalog(&[], "SELECT 1", &fixture.options, &QueryCatalog::default())
            .unwrap_err()
            .to_string()
            .contains("capacity exhausted")
    );
}

#[test]
fn reused_worker_security_denies_host_network_mutation_and_old_staging_paths() {
    let fixture = Fixture::new(false);
    let runtime = QueryRuntime::new(1);
    let catalog = QueryCatalog::default();
    for sql in [
        "SELECT * FROM read_text('/etc/passwd')",
        "SELECT * FROM read_blob('/proc/self/environ')",
        "SELECT * FROM read_blob('/dev/stdin')",
        "SELECT * FROM read_blob(current_setting('home_directory') || '/inputs/../../../../../../../../etc/passwd')",
        "SELECT * FROM read_csv('https://example.invalid/secret.csv')",
        "SELECT getenv('AWS_SECRET_ACCESS_KEY')",
        "SELECT * FROM query('CREATE TABLE injected(x INT)')",
        "SELECT * FROM query('SET enable_external_access=true')",
        "SELECT * FROM query('INSTALL httpfs')",
        "SELECT 1\n.output /tmp/varve-forbidden-output",
    ] {
        runtime
            .execute_with_catalog(&[], "SELECT 1 AS warm", &fixture.options, &catalog)
            .unwrap();
        assert!(
            runtime
                .execute_with_catalog(&[], sql, &fixture.options, &catalog)
                .is_err(),
            "unexpected success: {sql}"
        );
    }
    // Exercise scanner cleanup deliberately; small inputs now use typed literals.
    let mut scanner_table = table("measurements", 1.0);
    scanner_table.hot = vec![scanner_table.hot[0].clone(); 129];
    let tables = [scanner_table];
    let input = runtime
        .execute_with_catalog(
            &tables,
            "SELECT file FROM glob(current_setting('home_directory') || '/inputs/*')",
            &fixture.options,
            &catalog,
        )
        .unwrap();
    let path = input[0]["file"].as_str().unwrap();
    assert!(
        !PathBuf::from(path).exists(),
        "request staging must be deleted before returning"
    );
    // A fresh scanner cannot access even the previous worker's allowlisted
    // staging directory; its entire private home has already been removed.
    let stale = runtime
        .execute_with_catalog(
            &[],
            &format!("SELECT * FROM read_blob('{path}')"),
            &fixture.options,
            &catalog,
        )
        .unwrap_err();
    assert!(format!("{stale:#}").contains("disabled"), "{stale:#}");
    assert!(
        runtime
            .execute_with_catalog(
                &[],
                &format!("SELECT * FROM read_json('{path}')"),
                &fixture.options,
                &catalog
            )
            .is_err()
    );
}
