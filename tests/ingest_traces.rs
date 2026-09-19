use std::time::Duration;
use tempfile::TempDir;
use varve::{
    Config, Database, FlushPolicy, IngestConfig, Ingestor, Row, TableConfig, WriteRequest,
};

fn request(id: &str) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: id.into(),
        now_us: 1,
        rows: vec![Row {
            timestamp_us: 1,
            tenant: "t".into(),
            series: "s".into(),
            value: 1.25,
            tags: Default::default(),
        }],
    }
}

fn setup(capacity: usize) -> (TempDir, Database, Ingestor) {
    let root = TempDir::new().unwrap();
    let db = Database::open(
        root.path(),
        Config {
            hot_max_rows: 2,
            checkpoint_frozen_prefix: true,
            flush_policy: FlushPolicy::PressureOnly,
            ..Config::default()
        },
    )
    .unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_group_requests: 1,
            max_delay: Duration::ZERO,
            trace_capacity: capacity,
            ..IngestConfig::default()
        },
    )
    .unwrap();
    (root, db, ingest)
}

#[test]
fn traces_bind_receipts_to_pressure_checkpoint_and_two_syncs_without_changing_recovery() {
    let (root, db, ingest) = setup(8);
    let mut receipts = Vec::new();
    for id in ["a", "b", "c"] {
        receipts.push(
            ingest
                .submit(request(id))
                .unwrap()
                .blocking_recv()
                .unwrap()
                .unwrap(),
        );
    }
    ingest.shutdown().unwrap();
    let traces = ingest.traces();
    assert_eq!(
        (traces.capacity, traces.evicted, traces.groups.len()),
        (8, 0, 3)
    );
    for (trace, receipt) in traces.groups.iter().zip(&receipts) {
        assert_eq!(trace.sequences, vec![receipt.sequence]);
        assert_eq!(
            (trace.requests, trace.rows, trace.failed, trace.duplicate),
            (1, 1, 0, 0)
        );
        assert_eq!(
            trace
                .phases
                .iter()
                .find(|p| p.phase == "wal_sync")
                .unwrap()
                .count,
            2
        );
        assert!(
            trace.service_ns
                >= trace
                    .phases
                    .iter()
                    .find(|p| p.phase == "wal_write")
                    .unwrap()
                    .total_ns
        );
        assert!(trace.oldest_queue_ns >= trace.newest_queue_ns);
    }
    assert!(
        !traces.groups[0]
            .phases
            .iter()
            .any(|p| p.phase == "admission_checkpoint")
    );
    assert!(
        traces.groups[2]
            .phases
            .iter()
            .any(|p| p.phase == "admission_checkpoint")
    );
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
    drop(ingest);
    drop(db);
    let db = Database::open(root.path(), Config::default()).unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
    assert!(
        db.write("metrics", "c", request("c").rows, 1)
            .unwrap()
            .duplicate
    );
}

#[test]
fn traces_bound_history_and_disclose_failures_and_duplicates() {
    let (_root, _db, ingest) = setup(2);
    let first = ingest
        .submit(request("a"))
        .unwrap()
        .blocking_recv()
        .unwrap()
        .unwrap();
    let duplicate = ingest
        .submit(request("a"))
        .unwrap()
        .blocking_recv()
        .unwrap()
        .unwrap();
    assert!(duplicate.duplicate);
    let mut conflict = request("a");
    conflict.rows[0].value = 9.0;
    assert!(
        ingest
            .submit(conflict)
            .unwrap()
            .blocking_recv()
            .unwrap()
            .is_err()
    );
    ingest.shutdown().unwrap();
    let traces = ingest.traces();
    assert_eq!(traces.evicted, 1);
    assert_eq!(traces.groups.len(), 2);
    assert_eq!(traces.groups[0].sequences, vec![first.sequence]);
    assert_eq!(traces.groups[0].duplicate, 1);
    assert_eq!(traces.groups[1].failed, 1);
    assert!(traces.groups[1].sequences.is_empty());
    assert!(
        traces
            .groups
            .iter()
            .all(|t| !t.phases.iter().any(|p| p.phase == "wal_sync"))
    );
}

#[test]
fn tracing_defaults_off_and_rejects_excessive_capacity() {
    let (_root, db, ingest) = setup(0);
    ingest
        .submit(request("a"))
        .unwrap()
        .blocking_recv()
        .unwrap()
        .unwrap();
    ingest.shutdown().unwrap();
    assert!(ingest.traces().groups.is_empty());
    for config in [
        IngestConfig {
            trace_capacity: 1025,
            ..IngestConfig::default()
        },
        IngestConfig {
            trace_capacity: 1024,
            max_group_requests: 128,
            ..IngestConfig::default()
        },
    ] {
        assert!(Ingestor::new(db.clone(), config).is_err());
    }
}
