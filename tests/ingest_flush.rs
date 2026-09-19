use std::time::Duration;
use tempfile::TempDir;
use varve::{
    Config, Database, FlushPolicy, IngestConfig, Ingestor, Row, TableConfig, WriteRequest,
};

fn request(id: &str, value: f64) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: id.into(),
        now_us: 1,
        rows: vec![Row {
            timestamp_us: 1,
            tenant: "t".into(),
            series: "s".into(),
            value,
            tags: Default::default(),
        }],
    }
}

fn setup() -> (TempDir, Database, Ingestor) {
    let root = TempDir::new().unwrap();
    let db = Database::open(
        root.path(),
        Config {
            flush_policy: FlushPolicy::PressureOnly,
            ..Config::default()
        },
    )
    .unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_delay: Duration::from_secs(60),
            ..IngestConfig::default()
        },
    )
    .unwrap();
    (root, db, ingest)
}

#[tokio::test]
async fn fifo_flush_publishes_partial_group_without_checkpointing_or_waiting_for_deadline() {
    let (root, db, ingest) = setup();
    let before = db.status().unwrap();
    let first = ingest.submit(request("a", 1.0)).unwrap();
    let second = ingest.submit(request("b", 2.0)).unwrap();
    let flushed = tokio::time::timeout(Duration::from_secs(5), ingest.flush().unwrap())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(first.sequence, second.sequence);
    assert_eq!(first.durability, "local_fsync");
    assert_eq!(flushed.sequence, first.sequence);
    assert_eq!(
        (flushed.completed, flushed.succeeded, flushed.failed),
        (2, 2, 0)
    );
    let after = db.status().unwrap();
    assert!(after.sequence > before.sequence);
    assert_eq!(
        after.hot_rows, 2,
        "durable flush must not evict/checkpoint hot rows"
    );
    assert_eq!(ingest.stats().pending_bytes, 0);
    ingest.shutdown().unwrap();
    drop(ingest);
    drop(db);
    let reopened = Database::open(root.path(), Config::default()).unwrap();
    assert_eq!(
        reopened
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        2
    );
    assert!(
        reopened
            .write("metrics", "a", request("a", 1.0).rows, 1)
            .unwrap()
            .duplicate
    );
}

#[tokio::test]
async fn flush_preserves_fifo_boundary_and_reports_failed_writes() {
    let (_root, db, ingest) = setup();
    let first = ingest.submit(request("same", 1.0)).unwrap();
    let conflict = ingest.submit(request("same", 2.0)).unwrap();
    let before_later = ingest.flush().unwrap();
    let later = ingest.submit(request("later", 3.0)).unwrap();
    let after_later = ingest.flush().unwrap();
    let before = tokio::time::timeout(Duration::from_secs(5), before_later)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(first.await.unwrap().is_ok());
    assert!(conflict.await.unwrap().is_err());
    assert_eq!(
        (before.completed, before.succeeded, before.failed),
        (2, 1, 1)
    );
    let after = after_later.await.unwrap().unwrap();
    assert!(later.await.unwrap().is_ok());
    assert_eq!((after.completed, after.succeeded, after.failed), (3, 2, 1));
    assert!(after.sequence > before.sequence);
    assert_eq!(db.status().unwrap().hot_rows, 2);
    ingest.shutdown().unwrap();
    assert!(ingest.flush().is_err());
}

#[tokio::test]
async fn dropped_flush_receiver_and_shutdown_do_not_drop_writes() {
    let (_root, db, ingest) = setup();
    drop(ingest.flush().unwrap());
    let write = ingest.submit(request("kept", 1.0)).unwrap();
    let flush = ingest.flush().unwrap();
    ingest.shutdown().unwrap();
    assert!(write.await.unwrap().is_ok());
    assert_eq!(flush.await.unwrap().unwrap().succeeded, 1);
    assert_eq!(db.status().unwrap().hot_rows, 1);
    assert_eq!(ingest.stats().pending_requests, 0);
}
