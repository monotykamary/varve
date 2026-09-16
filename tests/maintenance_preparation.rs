#![cfg(feature = "fault-injection")]

use std::collections::BTreeMap;
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;
use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};
use varve::{Config, Database, Row, TableConfig};

fn config() -> Config {
    Config {
        segment_rows: 4,
        compact_min_segments: 2,
        flush_interval_us: 1,
        ..Default::default()
    }
}

fn table() -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 1_000,
        ..Default::default()
    }
}

fn row(value: f64) -> Row {
    Row {
        timestamp_us: 10,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value,
        tags: BTreeMap::from([("host".into(), "a".into())]),
    }
}

fn bits(rows: &[varve::model::StoredRow]) -> Vec<u64> {
    rows.iter()
        .map(|stored| stored.row.value.to_bits())
        .collect()
}

#[test]
fn checkpoint_preparation_allows_progress_and_rejects_a_stale_generation() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    let first = vec![row(f64::from_bits(0x3ff0_0000_0000_0001)), row(-0.0)];
    db.write("metrics", "first", first.clone(), 10).unwrap();

    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::CheckpointPrepare);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let worker = db.clone();
    let checkpoint = std::thread::spawn(move || worker.checkpoint());
    assert!(hook.wait_until_blocked(Duration::from_secs(5)));

    let (sent, received) = mpsc::sync_channel(1);
    let progress_db = db.clone();
    let second = vec![row(f64::from_bits(0xbfe0_0000_0000_0001))];
    let second_for_thread = second.clone();
    let progress = std::thread::spawn(move || {
        let before = progress_db.status().unwrap();
        assert_eq!(
            progress_db
                .scan("metrics", None, None, None, None)
                .unwrap()
                .len(),
            2
        );
        let receipt = progress_db
            .write("metrics", "second", second_for_thread, 10)
            .unwrap();
        let after = progress_db.status().unwrap();
        sent.send((before.sequence, receipt.sequence, after.sequence))
            .unwrap();
    });
    let observed = received.recv_timeout(Duration::from_secs(5));
    hook.release();
    progress.join().unwrap();
    checkpoint.join().unwrap().unwrap();
    let (before, written, after) = observed.unwrap();
    assert_eq!(written, before + 1);
    assert_eq!(after, written);

    let status = db.status().unwrap();
    assert_eq!(status.sequence, status.checkpoint_sequence);
    assert_eq!(status.hot_rows, 0);
    let expected = vec![
        first[0].value.to_bits(),
        first[1].value.to_bits(),
        second[0].value.to_bits(),
    ];
    let stored = db.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(bits(&stored), expected);
    assert_eq!(
        stored
            .iter()
            .map(|row| (row.sequence, row.ordinal))
            .collect::<Vec<_>>(),
        vec![(before, 0), (before, 1), (written, 0)]
    );
    assert!(db.write("metrics", "first", first, 10).unwrap().duplicate);
    assert!(db.write("metrics", "second", second, 10).unwrap().duplicate);
    let metrics = db.performance();
    assert!(metrics.phases["checkpoint_prepare"].count >= 2);
    assert!(metrics.phases["checkpoint_publish"].count >= 1);

    drop(db);
    let reopened = Database::open(temp.path(), config()).unwrap();
    let stored = reopened.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(bits(&stored), expected);
    assert_eq!(stored.len(), 3);
}

#[test]
fn compaction_preparation_never_publishes_over_a_concurrent_write() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    let first = vec![row(1.0), row(f64::from_bits(0x3ff0_0000_0000_0001))];
    let second = vec![row(-0.0), row(-1.0)];
    let third = vec![row(f64::MIN_POSITIVE)];
    let first_receipt = db.write("metrics", "first", first.clone(), 10).unwrap();
    db.checkpoint().unwrap();
    let second_receipt = db.write("metrics", "second", second.clone(), 10).unwrap();
    db.checkpoint().unwrap();

    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::CompactionPrepare);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let worker = db.clone();
    let compaction = std::thread::spawn(move || worker.compact());
    assert!(hook.wait_until_blocked(Duration::from_secs(5)));

    let (sent, received) = mpsc::sync_channel(1);
    let progress_db = db.clone();
    let third_for_thread = third.clone();
    let progress = std::thread::spawn(move || {
        assert_eq!(progress_db.status().unwrap().hot_rows, 0);
        assert_eq!(
            progress_db
                .scan("metrics", None, None, None, None)
                .unwrap()
                .len(),
            4
        );
        let receipt = progress_db
            .write("metrics", "third", third_for_thread, 10)
            .unwrap();
        assert_eq!(progress_db.status().unwrap().hot_rows, 1);
        sent.send(receipt.sequence).unwrap();
    });
    let third_sequence = received.recv_timeout(Duration::from_secs(5));
    hook.release();
    progress.join().unwrap();
    assert_eq!(compaction.join().unwrap().unwrap(), 0);
    let third_sequence = third_sequence.unwrap();
    assert_eq!(third_sequence, second_receipt.sequence + 1);

    db.checkpoint().unwrap();
    assert_eq!(db.compact().unwrap(), 1);
    let expected = first
        .iter()
        .chain(&second)
        .chain(&third)
        .map(|row| row.value.to_bits())
        .collect::<Vec<_>>();
    let stored = db.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(bits(&stored), expected);
    assert_eq!(
        stored
            .iter()
            .map(|row| (row.sequence, row.ordinal))
            .collect::<Vec<_>>(),
        vec![
            (first_receipt.sequence, 0),
            (first_receipt.sequence, 1),
            (second_receipt.sequence, 0),
            (second_receipt.sequence, 1),
            (third_sequence, 0)
        ]
    );
    assert!(db.write("metrics", "first", first, 10).unwrap().duplicate);
    assert!(db.write("metrics", "second", second, 10).unwrap().duplicate);
    assert!(db.write("metrics", "third", third, 10).unwrap().duplicate);
    assert!(db.performance().phases["compaction_prepare"].count >= 2);

    drop(db);
    let reopened = Database::open(temp.path(), config()).unwrap();
    let stored = reopened.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(bits(&stored), expected);
    assert_eq!(stored.len(), 5);
}

#[test]
fn prepared_output_pin_survives_foreground_checkpoint_gc_until_revalidation() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    let first = vec![row(1.0), row(2.0)];
    let concurrent = vec![row(3.0)];
    db.write("metrics", "first", first.clone(), 10).unwrap();

    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::CheckpointBeforePublish);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let worker = db.clone();
    let checkpoint = std::thread::spawn(move || worker.checkpoint());
    assert!(hook.wait_until_blocked(Duration::from_secs(5)));

    let prepared_files = std::fs::read_dir(temp.path().join("segments"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(prepared_files.len(), 1);
    db.write("metrics", "concurrent", concurrent.clone(), 10)
        .unwrap();
    // The explicit checkpoint takes the documented synchronous fallback while
    // the preparation gate is occupied. Its GC must respect the output guard.
    db.checkpoint().unwrap();
    assert!(prepared_files[0].exists());
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);

    hook.release();
    checkpoint.join().unwrap().unwrap();
    assert!(!prepared_files[0].exists());
    let expected = first
        .iter()
        .chain(&concurrent)
        .map(|row| row.value.to_bits())
        .collect::<Vec<_>>();
    assert_eq!(
        bits(&db.scan("metrics", None, None, None, None).unwrap()),
        expected
    );

    drop(db);
    let reopened = Database::open(temp.path(), config()).unwrap();
    assert_eq!(
        bits(&reopened.scan("metrics", None, None, None, None).unwrap()),
        expected
    );
}

#[test]
fn scheduled_stale_checkpoint_defers_without_rebuilding_in_the_same_tick() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "first", vec![row(1.0)], 10).unwrap();

    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::CheckpointBeforePublish);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let worker = db.clone();
    let maintenance = std::thread::spawn(move || worker.maintain(11));
    assert!(hook.wait_until_blocked(Duration::from_secs(5)));
    db.write("metrics", "concurrent", vec![row(2.0)], 11)
        .unwrap();
    hook.release();

    let report = maintenance.join().unwrap().unwrap();
    assert!(!report.flushed);
    let status = db.status().unwrap();
    assert_eq!(status.hot_rows, 2);
    assert!(status.checkpoint_sequence < status.sequence);
    assert_eq!(db.performance().phases["checkpoint_prepare"].count, 1);

    assert!(db.maintain(12).unwrap().flushed);
    let status = db.status().unwrap();
    assert_eq!(status.hot_rows, 0);
    assert_eq!(status.checkpoint_sequence, status.sequence);
    drop(db);
    let reopened = Database::open(temp.path(), config()).unwrap();
    assert_eq!(
        reopened
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        2
    );
}
