use std::collections::BTreeMap;

use varve_client::{RequestId, Row, ServerError, WriteError};

#[test]
fn row_timestamps_serialize_as_exact_i64_literals() {
    let rows = vec![
        Row {
            timestamp_us: i64::MIN,
            tenant: "tenant".into(),
            series: "cpu".into(),
            value: 0.10000000000000002,
            tags: BTreeMap::new(),
        },
        Row {
            timestamp_us: i64::MAX,
            tenant: "tenant".into(),
            series: "cpu".into(),
            value: 1.5,
            tags: BTreeMap::new(),
        },
    ];
    let json = serde_json::to_string(&rows).unwrap();
    assert!(json.contains("-9223372036854775808"));
    assert!(json.contains("9223372036854775807"));
    let decoded: Vec<Row> = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, rows);
}

#[test]
fn request_ids_enforce_the_wire_contract() {
    assert_eq!(RequestId::new("stable-1").unwrap().as_str(), "stable-1");
    assert!(RequestId::new("").is_err());
    assert!(RequestId::new("é").is_err());
    assert!(RequestId::new("x".repeat(129)).is_err());
}

#[test]
fn write_error_marks_only_unknown_outcomes_as_maybe_committed() {
    let rejected = WriteError::AdmissionRejected(ServerError {
        code: ServerError::ADMISSION_REJECTED,
        message: "busy".into(),
    });
    assert!(!rejected.may_have_committed());
    let unknown = WriteError::OutcomeUnknown {
        request_id: RequestId::new("stable").unwrap(),
        cause: varve_client::AmbiguousWrite::Timeout,
    };
    assert!(unknown.may_have_committed());
}
