use super::*;
use std::process::Command;

fn signal(pid: u32, signal: &str) {
    assert!(
        Command::new("kill")
            .args([signal, &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
}

fn idle_child(runtime: &QueryRuntime) -> (u32, PathBuf) {
    let pool = runtime.pool.lock().unwrap();
    let worker = &pool.idle[0];
    (worker.child.id(), worker.directory.path().to_owned())
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(Instant::now() < deadline, "protocol barrier timed out");
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn retained_paths_settings_schema_and_selected_file_permissions() {
    let runtime = QueryRuntime::new(1);
    let (mut tables, mut snapshot) = fixture(2);
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first'雪.parquet");
    let second = directory.path().join("second.parquet");
    crate::segment::write(&first, &snapshot.tables[0].batches[0].rows[..1]).unwrap();
    crate::segment::write(&second, &snapshot.tables[0].batches[0].rows).unwrap();
    let catalog = QueryCatalog::default();
    tables[0].files = vec![first.clone()];
    snapshot.tables[0].files = vec![ResidentFile {
        id: "first-file".into(),
        path: first.clone(),
        rows: 1,
        charged_bytes: snapshot.tables[0].batches[0].rows[0].row.estimated_bytes(),
        min_timestamp_us: 0,
        max_timestamp_us: 0,
    }];
    snapshot.lineage[0].ids.push("first-file".into());
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT count(*) AS n FROM metrics",
            &catalog
        )[0]["n"],
        3
    );
    let old = idle_child(&runtime);
    tables[0].files = vec![second.clone()];
    snapshot.tables[0].files = vec![ResidentFile {
        id: "second-file".into(),
        path: second,
        rows: 2,
        charged_bytes: snapshot.tables[0].batches[0]
            .rows
            .iter()
            .map(|row| row.row.estimated_bytes())
            .sum(),
        min_timestamp_us: 0,
        max_timestamp_us: 1,
    }];
    snapshot.lineage[0].ids.retain(|id| id != "first-file");
    snapshot.lineage[0].ids.push("second-file".into());
    snapshot.lineage[0].raw_stamp += 1;
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT count(*) AS n FROM metrics",
            &catalog
        )[0]["n"],
        4
    );
    let rebound = idle_child(&runtime);
    assert_eq!(rebound.0, old.0);
    assert_eq!(rebound.1, old.1);
    assert!(old.1.exists());
    let stats = runtime.stats();
    assert_eq!(stats.resident_full_loads, 1);
    assert_eq!(stats.resident_delta_loads, 1);
    assert_eq!(stats.resident_raw_staged_rows, 5);
    let denied = runtime
        .execute_resident_with_catalog(
            &tables,
            &snapshot,
            &format!(
                "SELECT * FROM read_parquet({})",
                quote_path(&first).unwrap()
            ),
            &options(),
            &catalog,
        )
        .unwrap_err();
    assert!(format!("{denied:#}").contains("disabled"), "{denied:#}");
    assert_eq!(runtime.stats().idle, 0);
    for threads in [1, 2] {
        let options = QueryOptions {
            threads,
            ..options()
        };
        assert_eq!(
            runtime
                .execute_resident_with_catalog(
                    &tables,
                    &snapshot,
                    "SELECT current_setting('threads') AS n",
                    &options,
                    &catalog
                )
                .unwrap()[0]["n"],
            threads
        );
    }
    let before = runtime.stats();
    let retained_worker = idle_child(&runtime);
    let catalog = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "metadata".into(),
            columns: vec![("x".into(), "BIGINT".into())],
            rows: vec![json!([7])],
        }],
        aggregates: vec![],
    };
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT x FROM metadata()",
            &catalog
        )[0]["x"],
        7
    );
    let after = runtime.stats();
    assert_eq!(after.resident_full_loads, before.resident_full_loads);
    assert_eq!(
        after.resident_dynamic_loads,
        before.resident_dynamic_loads + 1
    );
    assert_eq!(idle_child(&runtime), retained_worker);
    for sql in [
        "SELECT * FROM read_text('/etc/passwd')",
        "SELECT * FROM read_csv('https://example.invalid/no')",
        "SELECT set_config('enable_external_access', 'true', false)",
        "COPY metrics TO '/tmp/varve-unauthorized-output'",
        "SELECT 1; SELECT 2",
    ] {
        assert!(
            runtime
                .execute_resident_with_catalog(&tables, &snapshot, sql, &options(), &catalog)
                .is_err(),
            "{sql}"
        );
    }
}

#[test]
fn retained_cancel_delta_timeout_initial_and_eof_are_reaped() {
    let runtime = Arc::new(QueryRuntime::new(1));
    let (tables, mut snapshot) = fixture(130);
    run(
        &runtime,
        &tables,
        &snapshot,
        "SELECT count(*) FROM metrics",
        &QueryCatalog::default(),
    );
    let (pid, directory) = idle_child(&runtime);
    signal(pid, "-STOP");
    let mut delta = snapshot.tables[0].batches[0].clone();
    delta.id = "delta".into();
    snapshot.tables[0].batches.push(delta);
    snapshot.lineage[0].ids.push("delta".into());
    snapshot.lineage[0].raw_stamp += 1;
    snapshot.sequence += 1;
    let cancelled = Arc::new(AtomicBool::new(false));
    let executing = {
        let runtime = runtime.clone();
        let tables = tables.clone();
        let snapshot = snapshot.clone();
        let cancelled = cancelled.clone();
        thread::spawn(move || {
            runtime.execute_resident_with_catalog_cancellable(
                &tables,
                &snapshot,
                "SELECT count(*) FROM metrics",
                &options(),
                &QueryCatalog::default(),
                &cancelled,
            )
        })
    };
    wait_until(|| {
        std::fs::read_dir(directory.join("inputs")).is_ok_and(|entries| {
            entries.filter_map(Result::ok).any(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "parquet")
            })
        })
    });
    cancelled.store(true, Ordering::Release);
    assert!(
        executing
            .join()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert_eq!((runtime.stats().idle, runtime.stats().active), (0, 0));
    assert!(!directory.exists());
    assert_eq!(runtime.stats().resident_raw_staged_rows, 130);
    assert_eq!(
        run(
            &runtime,
            &tables,
            &snapshot,
            "SELECT count(*) AS n FROM metrics",
            &QueryCatalog::default()
        )[0]["n"],
        260
    );
    let (pid, directory) = idle_child(&runtime);
    signal(pid, "-KILL");
    assert!(
        runtime
            .execute_resident_with_catalog(
                &tables,
                &snapshot,
                "SELECT count(*) FROM metrics",
                &options(),
                &QueryCatalog::default()
            )
            .is_err()
    );
    assert!(!directory.exists());
    assert_eq!(runtime.stats().idle, 0);

    // A blocked wrapper tests the initial-load deadline without any CPU/load loop.
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let fifo = directory.path().join("gate");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let wrapper = directory.path().join("blocked-duckdb");
    std::fs::write(
        &wrapper,
        format!("#!/bin/sh\nread -r release < '{}'\n", fifo.display()),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let options = QueryOptions {
        executable: wrapper,
        timeout_ms: 100,
        ..options()
    };
    let error = runtime
        .execute_resident_with_catalog(
            &tables,
            &snapshot,
            "SELECT 1",
            &options,
            &QueryCatalog::default(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error:#}");
    assert_eq!((runtime.stats().idle, runtime.stats().active), (0, 0));
}

#[test]
fn retained_reversed_concurrent_snapshots_and_reset_failure() {
    let runtime = Arc::new(QueryRuntime::new(2));
    let (tables, older) = fixture(1);
    let mut newer = older.clone();
    newer.sequence = 2;
    let mut extra = newer.tables[0].batches[0].clone();
    extra.id = "newer".into();
    newer.tables[0].batches.push(extra);
    newer.lineage[0].ids.push("newer".into());
    newer.lineage[0].raw_stamp += 1;
    let (send, receive) = mpsc::channel();
    let old_query = {
        let runtime = runtime.clone();
        let tables = tables.clone();
        thread::spawn(move || {
            receive.recv().unwrap();
            run(
                &runtime,
                &tables,
                &older,
                "SELECT count(*) AS n FROM metrics",
                &QueryCatalog::default(),
            )
        })
    };
    assert_eq!(
        run(
            &runtime,
            &tables,
            &newer,
            "SELECT count(*) AS n FROM metrics",
            &QueryCatalog::default()
        )[0]["n"],
        2
    );
    send.send(()).unwrap();
    assert_eq!(old_query.join().unwrap()[0]["n"], 1);
    assert_eq!(runtime.stats().resident_full_loads, 2);
    // Exercise a reset failure under the same lease/drop boundary used by execute.
    let worker = runtime.pool.lock().unwrap().idle.pop().unwrap();
    let path = worker.directory.path().to_owned();
    runtime.pool.lock().unwrap().active += 1;
    let mut lease = Lease {
        runtime: &runtime,
        worker: Some(worker),
        reusable: false,
        reset_complete: false,
    };
    let worker = lease.worker.as_mut().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    worker
        .run(
            "BEGIN TRANSACTION;\n".into(),
            deadline,
            &AtomicBool::new(false),
            0,
        )
        .unwrap();
    signal(worker.child.id(), "-KILL");
    assert!(
        worker
            .run("ROLLBACK;\n".into(), deadline, &AtomicBool::new(false), 0)
            .is_err()
    );
    drop(lease);
    assert!(!path.exists());
    assert_eq!(runtime.stats().active, 0);
}
