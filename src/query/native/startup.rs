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
