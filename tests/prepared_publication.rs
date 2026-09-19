#![cfg(feature = "fault-injection")]

use std::collections::BTreeMap;
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;
use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};
use varve::{Config, Database, Row, TableConfig};

fn config(pages: bool) -> Config {
    Config {
        derived_pages: pages,
        derived_page_bytes: 4096,
        segment_rows: 4,
        compact_min_segments: 2,
        flush_interval_us: 1,
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

fn open(temp: &TempDir, pages: bool) -> Database {
    let db = Database::open(temp.path(), config(pages)).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            shards: 1,
            window_us: 1000,
            rollup_widths_us: vec![100],
            ..Default::default()
        },
    )
    .unwrap();
    db
}

struct Block(MaintenanceTestHook);
impl Block {
    fn new(db: &Database, phase: MaintenanceHookPhase) -> Self {
        let hook = MaintenanceTestHook::new(phase);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        Self(hook)
    }
    fn wait(&self) {
        assert!(self.0.wait_until_blocked(Duration::from_secs(5)));
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn assert_released(db: &Database) {
    let status = db.status().unwrap();
    assert_eq!(status.derived_working_bytes, 0);
    assert_eq!(status.active_snapshots, 0);
    assert!(status.fenced.is_none());
}

fn values(db: &Database) -> Vec<u64> {
    db.scan("metrics", None, None, None, None)
        .unwrap()
        .iter()
        .map(|r| r.row.value.to_bits())
        .collect()
}

#[test]
fn root_encoding_allows_progress_and_scheduled_stale_roots_never_publish() {
    for pages in [false, true] {
        let temp = TempDir::new().unwrap();
        let db = open(&temp, pages);
        db.write("metrics", "first", vec![row(-0.0)], 10).unwrap();
        let before = std::fs::read(temp.path().join("manifest.bin")).unwrap();
        let block = Block::new(&db, MaintenanceHookPhase::RootPrepare);
        let worker = db.clone();
        let maintenance = std::thread::spawn(move || worker.maintain(11));
        block.wait();
        assert!(db.status().unwrap().derived_working_bytes > 0);

        let (send, recv) = mpsc::sync_channel(1);
        let writer = db.clone();
        let progress = std::thread::spawn(move || {
            assert_eq!(values(&writer), vec![(-0.0f64).to_bits()]);
            writer
                .write("metrics", "second", vec![row(f64::MIN_POSITIVE)], 11)
                .unwrap();
            send.send(()).unwrap();
        });
        let observed = recv.recv_timeout(Duration::from_secs(5));
        block.0.release();
        progress.join().unwrap();
        observed.unwrap();
        assert!(!maintenance.join().unwrap().unwrap().flushed);
        assert_eq!(
            std::fs::read(temp.path().join("manifest.bin")).unwrap(),
            before
        );
        assert_eq!(db.status().unwrap().hot_rows, 2);
        assert_released(&db);
        db.set_maintenance_test_hook(None).unwrap();
        db.checkpoint().unwrap();
        let expected = values(&db);
        let rollups = serde_json::to_value(db.rollups("metrics").unwrap()).unwrap();
        drop(db);
        // Sticky v2 is checked with the opt-in flag disabled on recovery.
        let reopened = Database::open(temp.path(), config(false)).unwrap();
        assert_eq!(values(&reopened), expected);
        assert_eq!(
            serde_json::to_value(reopened.rollups("metrics").unwrap()).unwrap(),
            rollups
        );
        assert!(
            reopened
                .write("metrics", "first", vec![row(-0.0)], 12)
                .unwrap()
                .duplicate
        );
        assert!(
            reopened
                .write("metrics", "second", vec![row(f64::MIN_POSITIVE)], 12)
                .unwrap()
                .duplicate
        );
        let bytes = std::fs::read(temp.path().join("manifest.bin")).unwrap();
        assert_eq!(&bytes[..8], if pages { b"VARVEM02" } else { b"VARVEM01" });
    }
}

#[test]
fn compaction_root_never_rebases_over_a_new_clean_checkpoint() {
    for pages in [false, true] {
        let temp = TempDir::new().unwrap();
        let db = open(&temp, pages);
        db.write("metrics", "first", vec![row(1.0)], 10).unwrap();
        db.checkpoint().unwrap();
        db.write("metrics", "second", vec![row(-0.0)], 10).unwrap();
        db.checkpoint().unwrap();
        let block = Block::new(&db, MaintenanceHookPhase::RootPrepare);
        let worker = db.clone();
        let compaction = std::thread::spawn(move || worker.compact());
        block.wait();

        let (send, recv) = mpsc::sync_channel(1);
        let writer = db.clone();
        let progress = std::thread::spawn(move || {
            assert_eq!(values(&writer).len(), 2);
            writer
                .write(
                    "metrics",
                    "third",
                    vec![row(f64::from_bits(0x3ff0_0000_0000_0001))],
                    10,
                )
                .unwrap();
            writer.checkpoint().unwrap();
            send.send(()).unwrap();
        });
        let observed = recv.recv_timeout(Duration::from_secs(5));
        block.0.release();
        progress.join().unwrap();
        observed.unwrap();
        assert_eq!(compaction.join().unwrap().unwrap(), 0);
        assert_eq!(db.status().unwrap().segments, 3);
        assert_released(&db);
        db.set_maintenance_test_hook(None).unwrap();
        assert_eq!(db.compact().unwrap(), 1);
        let expected = values(&db);
        assert_eq!(expected.len(), 3);
        let rollups = serde_json::to_value(db.rollups("metrics").unwrap()).unwrap();
        drop(db);
        let reopened = Database::open(temp.path(), config(pages)).unwrap();
        assert_eq!(values(&reopened), expected);
        assert_eq!(
            serde_json::to_value(reopened.rollups("metrics").unwrap()).unwrap(),
            rollups
        );
    }
}

#[test]
fn pending_derived_dependency_survives_foreground_publication_and_gc() {
    let temp = TempDir::new().unwrap();
    let db = open(&temp, true);
    db.write("metrics", "first", vec![row(1.0)], 10).unwrap();
    let block = Block::new(&db, MaintenanceHookPhase::DerivedPagePrepared);
    let worker = db.clone();
    let checkpoint = std::thread::spawn(move || worker.checkpoint());
    block.wait();
    let pending: Vec<_> = std::fs::read_dir(temp.path().join("derived"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(pending.len(), 1);
    let pending_digest = pending[0].file_stem().unwrap().to_str().unwrap().to_owned();

    let (send, recv) = mpsc::sync_channel(1);
    let writer = db.clone();
    let progress = std::thread::spawn(move || {
        assert_eq!(values(&writer).len(), 1);
        writer
            .write("metrics", "second", vec![row(2.0)], 10)
            .unwrap();
        // The occupied preparation gate forces the safe synchronous fallback.
        writer.checkpoint().unwrap();
        send.send(()).unwrap();
    });
    let observed = recv.recv_timeout(Duration::from_secs(5));
    if observed.is_err() {
        block.0.release();
    }
    progress.join().unwrap();
    observed.unwrap();
    assert!(pending[0].exists());
    let manifest = std::fs::read(temp.path().join("manifest.bin")).unwrap();
    assert!(
        !manifest
            .windows(pending_digest.len())
            .any(|w| w == pending_digest.as_bytes())
    );
    block.0.release();
    checkpoint.join().unwrap().unwrap();
    assert_released(&db);
    db.set_maintenance_test_hook(None).unwrap();
    db.maintain(12).unwrap();
    assert!(
        !pending[0].exists(),
        "released orphan must become collectable"
    );
    let expected = values(&db);
    drop(db);
    let reopened = Database::open(temp.path(), config(false)).unwrap();
    assert_eq!(values(&reopened), expected);
    assert_eq!(reopened.rollups("metrics").unwrap()[0].count, 2);
}

#[test]
fn duplicate_advancing_only_the_rejection_floor_invalidates_the_root() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config(true)).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![100],
            idempotency_window_us: Some(100),
            ..Default::default()
        },
    )
    .unwrap();
    db.write("metrics", "v1:10:first", vec![row(1.0)], 10)
        .unwrap();
    let before = std::fs::read(temp.path().join("manifest.bin")).unwrap();
    let sequence = db.status().unwrap().sequence;
    let block = Block::new(&db, MaintenanceHookPhase::RootPrepare);
    let worker = db.clone();
    let maintenance = std::thread::spawn(move || worker.maintain(11));
    block.wait();
    assert!(
        db.write("metrics", "v1:10:first", vec![row(1.0)], 20)
            .unwrap()
            .duplicate
    );
    assert_eq!(db.status().unwrap().sequence, sequence);
    block.0.release();
    assert!(!maintenance.join().unwrap().unwrap().flushed);
    assert_eq!(
        std::fs::read(temp.path().join("manifest.bin")).unwrap(),
        before
    );
    assert_released(&db);
    db.set_maintenance_test_hook(None).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let reopened = Database::open(temp.path(), config(false)).unwrap();
    assert_eq!(reopened.idempotency_floor_us("metrics").unwrap(), Some(-80));
    assert!(
        reopened
            .write("metrics", "v1:-81:old", vec![row(2.0)], 0)
            .is_err()
    );
}

#[test]
fn preparation_faults_release_charges_and_recover_the_acknowledged_wal() {
    for (pages, phase) in [
        (false, MaintenanceHookPhase::RootPrepare),
        (true, MaintenanceHookPhase::RootPrepare),
        (true, MaintenanceHookPhase::DerivedPagePrepared),
        (false, MaintenanceHookPhase::CheckpointBeforePublish),
        (true, MaintenanceHookPhase::CheckpointBeforePublish),
    ] {
        let temp = TempDir::new().unwrap();
        let db = open(&temp, pages);
        db.write("metrics", "first", vec![row(-0.0)], 10).unwrap();
        let before = std::fs::read(temp.path().join("manifest.bin")).unwrap();
        let block = Block::new(&db, phase);
        let worker = db.clone();
        let checkpoint = std::thread::spawn(move || worker.checkpoint());
        block.wait();
        block.0.release_with_error();
        let error = checkpoint.join().unwrap().unwrap_err();
        assert!(format!("{error:#}").contains("injected maintenance preparation failure"));
        assert_eq!(
            std::fs::read(temp.path().join("manifest.bin")).unwrap(),
            before
        );
        assert_released(&db);
        assert_eq!(db.status().unwrap().hot_rows, 1);
        assert!(db.status().unwrap().wal_bytes > 0);
        drop(block);
        drop(db);
        let reopened = Database::open(temp.path(), config(pages)).unwrap();
        assert_eq!(values(&reopened), vec![(-0.0f64).to_bits()]);
        assert!(
            reopened
                .write("metrics", "first", vec![row(-0.0)], 10)
                .unwrap()
                .duplicate
        );
        reopened.checkpoint().unwrap();
        assert_released(&reopened);
        let expected = values(&reopened);
        drop(reopened);
        assert_eq!(
            values(&Database::open(temp.path(), config(pages)).unwrap()),
            expected
        );
    }
}
