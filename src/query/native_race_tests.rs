use super::native::NativeRuntime;
use super::native_tests::{
    native, native_budget, native_test_guard, options, oracle_tables, paths, resident, stored,
};
use super::{QueryCatalog, execute_with_catalog};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn cancellation_before_activation_keeps_interrupting_until_native_execution_stops() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let runtime = Arc::new(NativeRuntime::new(&library, 1).unwrap());
    let mut options = options(cli);
    options.timeout_ms = 2000;
    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    runtime.set_before_execute_hook(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    });
    let rows = crate::raw_memory::SharedRawRows::test_rows(vec![stored(1, 2.0, 1, 0)]);
    let (tables, snapshot) = resident(rows, vec![], vec![]);
    let budget = native_budget();
    let worker_budget = budget.clone();
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker = Arc::clone(&runtime);
    let signal = Arc::clone(&cancelled);
    let handle = thread::spawn(move || {
        worker.execute(
            &tables,
            Some(&snapshot),
            "SELECT sum(sin(i::DOUBLE)) FROM metrics, range(500000000) r(i)",
            &options,
            &QueryCatalog::default(),
            &worker_budget,
            &signal,
        )
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    cancelled.store(true, Ordering::Release);
    // The first interrupt has no active statement to stop at this barrier.
    thread::sleep(Duration::from_millis(25));
    let started = Instant::now();
    release_tx.send(()).unwrap();
    let error = handle.join().unwrap().unwrap_err();
    assert!(format!("{error:#}").contains("cancelled"), "{error:#}");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(runtime.live_callback_owners(), 0);
    let usage = runtime.last_scratch_usage().unwrap();
    assert_eq!(usage.owner_live_after_close, 0);
    assert_eq!(usage.owner_bytes_after_close, 0);
    assert_eq!(usage.available_slots_after_close, 2);
    assert_eq!(budget.status().reserved_bytes, 0);
    assert_eq!(budget.status().working_bytes, 0);
}

#[test]
fn nonfinite_results_are_rejected_without_leaking_native_callback_owners() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    for sql in [
        "SELECT 'Infinity'::DOUBLE AS v",
        "SELECT '-Infinity'::FLOAT AS v",
        "SELECT 'NaN'::DOUBLE AS v",
        "SELECT {'v': 'Infinity'::DOUBLE} AS nested",
    ] {
        // Neither public backend may return an invalid/non-finite JSON number.
        assert!(
            execute_with_catalog(&[], sql, &options, &QueryCatalog::default()).is_err(),
            "CLI accepted {sql}"
        );
        assert!(
            runtime
                .execute(
                    &[],
                    None,
                    sql,
                    &options,
                    &QueryCatalog::default(),
                    &native_budget(),
                    &AtomicBool::new(false)
                )
                .is_err(),
            "native accepted {sql}"
        );
        assert_eq!(runtime.live_callback_owners(), 0);
    }
}

#[test]
fn callback_errors_and_panics_after_handoff_destroy_all_owners() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    let (tables, snapshot) = resident(
        crate::raw_memory::SharedRawRows::test_rows(vec![stored(1, 2.0, 1, 0)]),
        vec![],
        vec![],
    );
    let budget = native_budget();
    for stage in 1..=6 {
        let _fault = runtime.inject_callback_failure(stage);
        let error = runtime
            .execute(
                &tables,
                Some(&snapshot),
                "SELECT timestamp_us, value FROM metrics",
                &options,
                &QueryCatalog::default(),
                &budget,
                &AtomicBool::new(false),
            )
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("injected callback")
                || message.contains("panic in native DuckDB scanner callback"),
            "stage {stage}: {message}"
        );
        assert_eq!(
            runtime.live_callback_owners(),
            0,
            "stage {stage} leaked owners"
        );
        assert_eq!(budget.status().reserved_bytes, 0, "stage {stage}");
        assert_eq!(budget.status().working_bytes, 0, "stage {stage}");
    }
}

#[test]
fn rust_execution_unwind_stops_and_joins_the_native_watcher() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    runtime.set_before_execute_hook(|| panic!("injected Rust execution unwind"));
    let started = Instant::now();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.execute(
            &[],
            None,
            "SELECT 1",
            &options,
            &QueryCatalog::default(),
            &native_budget(),
            &AtomicBool::new(false),
        )
    }));
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(runtime.live_callback_owners(), 0);
    assert_eq!(
        runtime
            .execute(
                &[],
                None,
                "SELECT 1 AS value",
                &options,
                &QueryCatalog::default(),
                &native_budget(),
                &AtomicBool::new(false)
            )
            .unwrap(),
        serde_json::json!([{"value": 1}])
    );
}

#[test]
fn shared_scans_preserve_multichunk_strings_through_sort_and_aggregation() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let options = options(cli);
    let runtime = NativeRuntime::new(&library, 2).unwrap();
    let rows = crate::raw_memory::SharedRawRows::test_rows(
        (0..5003)
            .map(|i| {
                let mut row = stored(i, i as f64, 1, i as u32);
                row.row.tenant = format!("tenant-{i:08}-雪");
                row.row
                    .tags
                    .insert("unique".into(), format!("tag-{i:08}-雪-escaped-\""));
                row
            })
            .collect(),
    );
    let (tables, snapshot) = resident(rows, vec![], vec![]);
    let oracle = oracle_tables(&tables, &snapshot);
    let catalog = QueryCatalog::default();
    for sql in [
        "SELECT timestamp_us, tenant, tags FROM metrics ORDER BY timestamp_us DESC",
        "SELECT count(*) AS count, sum(value) AS total, count(DISTINCT tags) AS unique_tags FROM metrics",
    ] {
        let expected = execute_with_catalog(&oracle, sql, &options, &catalog).unwrap();
        assert_eq!(
            native(&runtime, &tables, &snapshot, sql, &options, &catalog),
            expected,
            "{sql}"
        );
        assert_eq!(runtime.live_callback_owners(), 0);
    }
}
