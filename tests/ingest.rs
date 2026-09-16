use std::{
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};
use tempfile::TempDir;
use varve::{Config, Database, IngestConfig, Ingestor, Row, TableConfig, WriteRequest};

fn request(id: usize, rows: usize) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: format!("r{id}"),
        now_us: 1,
        rows: (0..rows)
            .map(|_| Row {
                timestamp_us: 1,
                tenant: "t".into(),
                series: "s".into(),
                value: 1.0,
                tags: Default::default(),
            })
            .collect(),
    }
}
fn open() -> (TempDir, Database) {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), Config::default()).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            ..Default::default()
        },
    )
    .unwrap();
    (temp, db)
}
fn config() -> IngestConfig {
    IngestConfig {
        max_delay: Duration::from_secs(60),
        ..Default::default()
    }
}

#[test]
fn count_and_row_thresholds_flush_without_waiting_for_delay() {
    for by_rows in [false, true] {
        let (_temp, db) = open();
        let before = db.status().unwrap().sequence;
        let ingest = Ingestor::new(
            db.clone(),
            IngestConfig {
                max_group_requests: if by_rows { 128 } else { 4 },
                max_group_rows: if by_rows { 8 } else { 10_000 },
                ..config()
            },
        )
        .unwrap();
        let receivers: Vec<_> = (0..4)
            .map(|i| ingest.submit(request(i, 2)).unwrap())
            .collect();
        for receiver in receivers {
            let receipt = receiver.blocking_recv().unwrap().unwrap();
            assert_eq!(receipt.sequence, before + 1);
            assert_eq!(receipt.durability, "local_fsync");
        }
        assert_eq!(db.status().unwrap().sequence, before + 1);
        assert_eq!(ingest.stats().groups, 1);
        ingest.shutdown().unwrap();
        assert_eq!(ingest.stats().pending_bytes, 0);
    }
}

#[test]
fn byte_threshold_and_time_threshold_flush() {
    let (_temp, db) = open();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_group_bytes: 4000,
            ..config()
        },
    )
    .unwrap();
    let receivers: Vec<_> = (0..3)
        .map(|i| ingest.submit(request(i, 1)).unwrap())
        .collect();
    // The third request crosses the byte threshold and flushes the first two.
    let mut receivers = receivers.into_iter();
    let first = receivers.next().unwrap().blocking_recv().unwrap().unwrap();
    let second = receivers.next().unwrap().blocking_recv().unwrap().unwrap();
    assert_eq!(first.sequence, second.sequence);
    ingest.shutdown().unwrap();
    let third = receivers.next().unwrap().blocking_recv().unwrap().unwrap();
    assert!(third.sequence > first.sequence);
    assert_eq!(ingest.stats().groups, 2);
    let timed = Ingestor::new(
        db,
        IngestConfig {
            max_delay: Duration::from_millis(5),
            ..config()
        },
    )
    .unwrap();
    let start = Instant::now();
    timed
        .submit(request(9, 1))
        .unwrap()
        .blocking_recv()
        .unwrap()
        .unwrap();
    assert!(start.elapsed() < Duration::from_secs(5));
    timed.shutdown().unwrap();
}

#[test]
fn byte_budget_covers_pending_group_and_rejection_never_enqueues() {
    let (_temp, db) = open();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_pending_bytes: 2000,
            ..config()
        },
    )
    .unwrap();
    let first = ingest.submit(request(0, 1)).unwrap();
    assert!(ingest.submit(request(1, 1)).is_err());
    let mut invalid = request(2, 1);
    invalid.rows[0].value = f64::NAN;
    assert!(ingest.submit(invalid).is_err());
    assert_eq!(ingest.stats().submitted, 1);
    assert_eq!(ingest.stats().pending_requests, 1);
    assert!(ingest.stats().pending_bytes <= 2000);
    assert!(first.is_empty());
    ingest.shutdown().unwrap();
    first.blocking_recv().unwrap().unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
    assert_eq!(ingest.stats().pending_bytes, 0);
    assert!(ingest.submit(request(3, 1)).is_err());
    assert_eq!(ingest.stats().submitted, 1);
    serde_json::to_value(ingest.stats()).unwrap();
}

#[test]
fn queue_full_dropped_receivers_and_concurrent_shutdown_drain() {
    let (_temp, db) = open();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            queue_capacity: 1,
            ..config()
        },
    )
    .unwrap();
    let mut accepted = 0;
    for id in 0..10000 {
        match ingest.submit(request(id, 1)) {
            Ok(receiver) => {
                accepted += 1;
                drop(receiver);
            }
            Err(_) => break,
        }
    }
    assert!(accepted > 0 && accepted < 10000);
    let clone = ingest.clone();
    let join = std::thread::spawn(move || clone.shutdown().unwrap());
    ingest.shutdown().unwrap();
    join.join().unwrap();
    assert_eq!(
        db.scan("metrics", None, None, None, None).unwrap().len(),
        accepted
    );
    let stats = ingest.stats();
    assert_eq!(stats.completed, accepted as u64);
    assert_eq!(stats.dropped_receivers, accepted as u64);
    assert_eq!(stats.pending_bytes, 0);
    assert!(stats.closed);
}

#[test]
fn concurrent_producers_amortize_publication_and_final_drop_drains() {
    let (_temp, db) = open();
    let before = db.status().unwrap().sequence;
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_group_requests: 32,
            max_delay: Duration::from_millis(20),
            ..IngestConfig::default()
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(4));
    let joins: Vec<_> = (0..4)
        .map(|producer| {
            let ingest = ingest.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                (0..32)
                    .map(|i| ingest.submit(request(producer * 32 + i, 1)).unwrap())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let receivers: Vec<_> = joins.into_iter().flat_map(|j| j.join().unwrap()).collect();
    drop(ingest);
    for receiver in receivers {
        assert!(!receiver.blocking_recv().unwrap().unwrap().duplicate);
    }
    let status = db.status().unwrap();
    assert_eq!(status.hot_rows, 128);
    assert!(status.sequence - before < 128);
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 128);
}

#[test]
fn per_item_completion_errors_and_retries_are_isolated() {
    let (_temp, db) = open();
    let ingest = Ingestor::new(db.clone(), config()).unwrap();
    let a = ingest.submit(request(0, 1)).unwrap();
    let duplicate = ingest.submit(request(0, 1)).unwrap();
    let mut bad = request(1, 1);
    bad.table = "missing".into();
    let bad = ingest.submit(bad).unwrap();
    let mut conflict = request(0, 1);
    conflict.rows[0].value = 2.0;
    let conflict = ingest.submit(conflict).unwrap();
    let b = ingest.submit(request(2, 1)).unwrap();
    ingest.shutdown().unwrap();
    let sequence = a.blocking_recv().unwrap().unwrap().sequence;
    let duplicate = duplicate.blocking_recv().unwrap().unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.sequence, sequence);
    assert!(bad.blocking_recv().unwrap().is_err());
    assert!(conflict.blocking_recv().unwrap().is_err());
    assert_eq!(b.blocking_recv().unwrap().unwrap().sequence, sequence);
    let stats = ingest.stats();
    assert_eq!(stats.succeeded, 3);
    assert_eq!(stats.failed, 2);
    assert_eq!(stats.pending_requests, 0);
}

#[test]
fn invalid_configuration_and_oversized_requests_are_upfront_errors() {
    let (_temp, db) = open();
    for bad in [
        IngestConfig {
            queue_capacity: 0,
            ..config()
        },
        IngestConfig {
            max_group_requests: 1025,
            ..config()
        },
        IngestConfig {
            max_group_bytes: 128,
            ..config()
        },
    ] {
        assert!(Ingestor::new(db.clone(), bad).is_err());
    }
    let ingest = Ingestor::new(
        db,
        IngestConfig {
            max_group_rows: 1,
            ..config()
        },
    )
    .unwrap();
    assert!(ingest.submit(request(0, 2)).is_err());
    ingest.shutdown().unwrap();
    assert_eq!(ingest.stats().submitted, 0);
}
