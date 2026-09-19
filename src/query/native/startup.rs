use super::super::native_tests::{native_test_guard, paths};
use super::*;
use serde_json::json;

#[test]
fn open_applies_resource_limits_before_configure_and_execute() {
    let _guard = native_test_guard();
    let (library, _) = paths();
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    let options = QueryOptions {
        threads: 1,
        memory_mb: 64,
        ..QueryOptions::default()
    };

    let session = runtime.open_private_session(&options).unwrap();
    let mut result = execute_statement(
        &runtime.api,
        session.connection.get(),
        "SELECT current_setting('threads')::INTEGER AS threads, current_setting('memory_limit') AS memory_limit",
    )
    .unwrap();
    let actual = output::consume(
        &runtime.api,
        session.connection.get(),
        &mut result,
        false,
        options.max_output_bytes,
        &AtomicBool::new(false),
    )
    .unwrap();

    assert_eq!(actual, json!([{"threads": 1, "memory_limit": "61.0 MiB"}]));
}

#[test]
fn transaction_rollback_detaches_catalog_callbacks_without_closing_session() {
    let _guard = native_test_guard();
    let (library, cli) = paths();
    let runtime = NativeRuntime::new(&library, 1).unwrap();
    let options = super::super::native_tests::options(cli);
    let budget = super::super::native_tests::native_budget();
    let rows =
        crate::raw_memory::SharedRawRows::test_rows(vec![super::super::native_tests::stored(
            1, 5.0, 1, 0,
        )]);
    let (tables, snapshot) = super::super::native_tests::resident(rows, vec![], vec![]);
    let catalog = QueryCatalog::default();
    let cancelled = AtomicBool::new(false);
    let deadline = Instant::now() + Duration::from_secs(10);
    let lease = ScannerScratch::reserve(
        ScratchPlan::for_query(&tables, Some(&snapshot), &catalog, options.threads).unwrap(),
        &budget,
    )
    .unwrap()
    .unwrap();
    let prepared = PreparedQuery::new(
        &tables,
        Some(&snapshot),
        &catalog,
        options.threads,
        Some(lease.scratch()),
        deadline,
        &cancelled,
    )
    .unwrap();
    // Session is declared last: even a failed ABI assertion must close before
    // releasing borrowed callback sources and fixed scanner credit.
    let mut session = runtime.open_private_session(&options).unwrap();
    let value = unsafe {
        runtime.configure_and_execute(
            &mut session,
            true,
            &prepared,
            "SELECT value FROM metrics",
            &options,
            deadline,
            &cancelled,
            Arc::new(AtomicBool::new(false)),
        )
    }
    .unwrap();
    assert_eq!(value, json!([{"value":5.0}]));
    assert!(
        !lease.detached(),
        "must first witness registered catalog owners"
    );
    runtime.rollback_session(session.connection.get()).unwrap();
    assert!(
        lease.detached(),
        "pinned v2 cannot detach request registrations: stop reuse qualification"
    );
    let mut result = execute_statement(
        &runtime.api, session.connection.get(),
        "SELECT (SELECT count(*) FROM duckdb_functions() WHERE function_name LIKE '__varve_%') AS callbacks, (SELECT count(*) FROM duckdb_views() WHERE view_name = 'metrics' OR view_name = 'metrics__rollup') AS views",
    ).unwrap();
    let remaining = output::consume(
        &runtime.api,
        session.connection.get(),
        &mut result,
        false,
        options.max_output_bytes,
        &cancelled,
    )
    .unwrap();
    assert_eq!(remaining, json!([{"callbacks":0,"views":0}]));
    drop(result);
    drop(session);
    drop(prepared);
    drop(lease);
    assert_eq!(budget.status().reserved_bytes, 0);
    assert_eq!(runtime.live_callback_owners(), 0);
}
