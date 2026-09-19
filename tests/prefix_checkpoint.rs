#![cfg(feature = "fault-injection")]

use std::collections::BTreeMap;
use std::fs;
use std::process::Command;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tempfile::TempDir;
use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};
use varve::remote::{FileStore, HeadObject, RemoteStore};
use varve::{Config, Database, Row, TableConfig, WriteRequest};

fn row(timestamp_us: i64, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value,
        tags: BTreeMap::from([("host".into(), "a".into())]),
    }
}

fn config(pages: bool) -> Config {
    Config {
        checkpoint_frozen_prefix: true,
        derived_pages: pages,
        derived_page_bytes: 4096,
        segment_rows: 4,
        flush_interval_us: 1,
        ..Default::default()
    }
}

fn wal_sequences(temp: &TempDir) -> Vec<u64> {
    let mut sequences = fs::read_dir(temp.path().join("wal"))
        .unwrap()
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            name.strip_suffix(".wal")?.parse().ok()
        })
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    sequences
}

fn values(db: &Database) -> Vec<u64> {
    db.scan("metrics", None, None, None, None)
        .unwrap()
        .into_iter()
        .map(|stored| stored.row.value.to_bits())
        .collect()
}

#[test]
fn committed_prefix_reclamation_does_not_hold_the_writer_gate() {
    for pages in [false, true] {
        let temp = TempDir::new().unwrap();
        let db = Database::open(temp.path(), config(pages)).unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        let first = db
            .write("metrics", "prefix", vec![row(10, 1.0)], 10)
            .unwrap();
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::CheckpointReclaim);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let worker = db.clone();
        let checkpoint = std::thread::spawn(move || worker.checkpoint());
        assert!(hook.wait_until_blocked(Duration::from_secs(5)));
        assert_eq!(db.status().unwrap().checkpoint_sequence, first.sequence);
        assert_eq!(values(&db), vec![1.0f64.to_bits()]);
        let (send, receive) = mpsc::channel();
        let writer_db = db.clone();
        let writer = std::thread::spawn(move || {
            send.send(writer_db.write("metrics", "tail", vec![row(11, -0.0)], 11))
                .unwrap();
        });
        let during_reclaim = receive.recv_timeout(Duration::from_secs(2));
        hook.release();
        checkpoint.join().unwrap().unwrap();
        writer.join().unwrap();
        db.set_maintenance_test_hook(None).unwrap();
        let receipt = during_reclaim
            .expect("durable append blocked behind retired checkpoint ownership")
            .unwrap();
        assert_eq!(receipt.sequence, first.sequence + 1);
        assert_eq!(receipt.durability, "local_fsync");
        assert_eq!(db.status().unwrap().hot_rows, 1);
        assert_eq!(wal_sequences(&temp), vec![receipt.sequence]);
        assert_eq!(values(&db), vec![1.0f64.to_bits(), (-0.0f64).to_bits()]);
        drop(db);
        let reopened = Database::open(temp.path(), config(pages)).unwrap();
        assert_eq!(
            values(&reopened),
            vec![1.0f64.to_bits(), (-0.0f64).to_bits()]
        );
        assert!(
            reopened
                .write("metrics", "tail", vec![row(11, -0.0)], 11)
                .unwrap()
                .duplicate
        );
        assert_eq!(
            reopened
                .rollups("metrics")
                .unwrap()
                .iter()
                .map(|r| r.count)
                .sum::<u64>(),
            2
        );
    }
}

#[test]
fn append_during_root_prepare_installs_only_the_frozen_prefix() {
    for pages in [false, true] {
        let temp = TempDir::new().unwrap();
        let db = Database::open(temp.path(), config(pages)).unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                shards: 1,
                window_us: 1_000,
                rollup_widths_us: vec![100],
                ..Default::default()
            },
        )
        .unwrap();
        db.write("metrics", "prefix", vec![row(10, 1.0)], 10)
            .unwrap();
        let locked_before = db.performance().phases["checkpoint_locked"].count;

        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let worker = db.clone();
        let checkpoint = std::thread::spawn(move || worker.checkpoint());
        assert!(hook.wait_until_blocked(Duration::from_secs(5)));

        let tail = db
            .write("metrics", "tail", vec![row(11, -0.0)], 11)
            .unwrap();
        assert_eq!(tail.sequence, 3);
        hook.release();
        checkpoint.join().unwrap().unwrap();
        db.set_maintenance_test_hook(None).unwrap();

        let status = db.status().unwrap();
        assert_eq!(status.sequence, 3);
        assert_eq!(status.checkpoint_sequence, 2);
        assert_eq!(status.hot_rows, 1);
        assert_eq!(wal_sequences(&temp), vec![3]);
        assert_eq!(values(&db), vec![1.0f64.to_bits(), (-0.0f64).to_bits()]);
        assert_eq!(db.rollups("metrics").unwrap()[0].count, 2);
        let phases = db.performance().phases;
        assert!(phases["root_prepare"].count > 0);
        assert!(phases["manifest_commit"].count > 0);
        assert_eq!(phases["checkpoint_locked"].count, locked_before);

        let metadata_bytes = status.metadata_bytes;
        let resident_bytes = status.derived_resident_bytes;
        drop(db);
        let reopened = Database::open(temp.path(), config(pages)).unwrap();
        assert_eq!(
            values(&reopened),
            vec![1.0f64.to_bits(), (-0.0f64).to_bits()]
        );
        assert_eq!(reopened.rollups("metrics").unwrap()[0].count, 2);
        assert!(
            reopened
                .write("metrics", "prefix", vec![row(10, 1.0)], 12)
                .unwrap()
                .duplicate
        );
        assert!(
            reopened
                .write("metrics", "tail", vec![row(11, -0.0)], 12)
                .unwrap()
                .duplicate
        );
        assert_eq!(reopened.status().unwrap().metadata_bytes, metadata_bytes);
        assert_eq!(
            reopened.status().unwrap().derived_resident_bytes,
            resident_bytes
        );
    }
}

#[test]
fn timed_pruning_is_patched_against_a_same_bucket_tail_at_digit_growth() {
    for pages in [false, true] {
        let temp = TempDir::new().unwrap();
        let db = Database::open(temp.path(), config(pages)).unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                shards: 1,
                window_us: 1_000,
                rollup_widths_us: vec![100],
                idempotency_window_us: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
        for issued in 0..8 {
            db.write(
                "metrics",
                &format!("v1:{issued}:prefix"),
                vec![row(10 + issued, issued as f64)],
                issued,
            )
            .unwrap();
        }
        assert_eq!(db.status().unwrap().sequence, 9);

        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let worker = db.clone();
        let maintenance = std::thread::spawn(move || worker.maintain(200));
        assert!(hook.wait_until_blocked(Duration::from_secs(5)));
        let tail = db
            .write("metrics", "v1:100:tail", vec![row(19, 99.0)], 200)
            .unwrap();
        assert_eq!(tail.sequence, 10);
        hook.release();
        assert!(maintenance.join().unwrap().unwrap().flushed);
        db.set_maintenance_test_hook(None).unwrap();

        let status = db.status().unwrap();
        assert_eq!(status.sequence, 10);
        assert_eq!(status.checkpoint_sequence, 9);
        assert_eq!(status.hot_rows, 1);
        assert_eq!(status.idempotency_keys, 1);
        assert_eq!(wal_sequences(&temp), vec![10]);
        let rollup = &db.rollups("metrics").unwrap()[0];
        assert_eq!(rollup.count, 9);
        assert_eq!(rollup.first, 0.0);
        assert_eq!(rollup.last, 99.0);
        assert!(
            db.write("metrics", "v1:100:tail", vec![row(19, 99.0)], 200)
                .unwrap()
                .duplicate
        );
        let metadata_bytes = status.metadata_bytes;
        let resident_bytes = status.derived_resident_bytes;
        drop(db);

        let reopened = Database::open(temp.path(), config(pages)).unwrap();
        assert_eq!(reopened.status().unwrap().metadata_bytes, metadata_bytes);
        assert_eq!(
            reopened.status().unwrap().derived_resident_bytes,
            resident_bytes
        );
        assert_eq!(reopened.status().unwrap().idempotency_keys, 1);
        assert_eq!(reopened.rollups("metrics").unwrap()[0].count, 9);
        assert!(
            reopened
                .write("metrics", "v1:100:tail", vec![row(19, 99.0)], 200)
                .unwrap()
                .duplicate
        );
    }
}

#[test]
fn grouped_hot_pressure_uses_one_off_lock_prefix_without_locked_fallback() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(
        temp.path(),
        Config {
            checkpoint_frozen_prefix: true,
            hot_max_rows: 1,
            segment_rows: 1,
            ..Default::default()
        },
    )
    .unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write("metrics", "prefix", vec![row(1, 1.0)], 1).unwrap();
    let result = db.write_group(vec![WriteRequest {
        table: "metrics".into(),
        request_id: "tail".into(),
        rows: vec![row(2, 2.0)],
        now_us: 2,
    }]);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].as_ref().unwrap().sequence, 3);
    let status = db.status().unwrap();
    assert_eq!(status.checkpoint_sequence, 2);
    assert_eq!(status.hot_rows, 1);
    let phases = db.performance().phases;
    assert!(phases["root_prepare"].count > 0);
    assert!(phases["manifest_commit"].count > 0);
    assert_eq!(phases["checkpoint_locked"].count, 0);
}

#[test]
fn control_floor_and_competing_root_changes_stale_the_candidate() {
    {
        let temp = TempDir::new().unwrap();
        let db = Database::open(temp.path(), config(false)).unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        db.write("metrics", "prefix", vec![row(1, 1.0)], 1).unwrap();
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let worker = db.clone();
        let checkpoint = std::thread::spawn(move || worker.checkpoint());
        assert!(hook.wait_until_blocked(Duration::from_secs(5)));
        db.create_table("other", TableConfig::default()).unwrap();
        hook.release();
        let error = checkpoint.join().unwrap().unwrap_err();
        assert!(format!("{error:#}").contains("stale frozen-prefix"));
        db.set_maintenance_test_hook(None).unwrap();
        assert_eq!(values(&db), vec![1.0f64.to_bits()]);
        db.checkpoint().unwrap();
    }
    {
        let temp = TempDir::new().unwrap();
        let db = Database::open(temp.path(), config(false)).unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                idempotency_window_us: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
        db.write("metrics", "v1:10:prefix", vec![row(1, 1.0)], 10)
            .unwrap();
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let worker = db.clone();
        let checkpoint = std::thread::spawn(move || worker.checkpoint());
        assert!(hook.wait_until_blocked(Duration::from_secs(5)));
        assert!(
            db.write("metrics", "v1:10:prefix", vec![row(1, 1.0)], 20)
                .unwrap()
                .duplicate
        );
        hook.release();
        assert!(checkpoint.join().unwrap().is_err());
        db.set_maintenance_test_hook(None).unwrap();
        db.checkpoint().unwrap();
    }
    {
        let temp = TempDir::new().unwrap();
        let db = Database::open(
            temp.path(),
            Config {
                checkpoint_frozen_prefix: true,
                hot_max_rows: 1,
                segment_rows: 1,
                ..Default::default()
            },
        )
        .unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        db.write("metrics", "prefix", vec![row(1, 1.0)], 1).unwrap();
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let worker = db.clone();
        let checkpoint = std::thread::spawn(move || worker.checkpoint());
        assert!(hook.wait_until_blocked(Duration::from_secs(5)));
        // Single pressure now waits on the frozen preparation gate, so it
        // cannot publish a competing root while this candidate is blocked.
        // Aggregate creation still uses the synchronous control checkpoint.
        // Detach only future hook visits; the captured hook remains blocked.
        db.set_maintenance_test_hook(None).unwrap();
        let competing = db.create_continuous_aggregate("rollup", "metrics", 10);
        hook.release();
        let error = checkpoint.join().unwrap().unwrap_err();
        assert!(format!("{error:#}").contains("stale frozen-prefix"));
        assert_eq!(competing.unwrap(), 3);
        assert_eq!(db.status().unwrap().checkpoint_sequence, 2);
        let receipt = db.write("metrics", "tail", vec![row(2, 2.0)], 2).unwrap();
        assert_eq!(receipt.sequence, 4);
        assert!(!receipt.duplicate);
        assert_eq!(db.status().unwrap().checkpoint_sequence, 2);
        assert_eq!(values(&db), vec![1.0f64.to_bits(), 2.0f64.to_bits()]);
        drop(db);
        let db = Database::open(
            temp.path(),
            Config {
                checkpoint_frozen_prefix: true,
                hot_max_rows: 1,
                segment_rows: 1,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(values(&db), vec![1.0f64.to_bits(), 2.0f64.to_bits()]);
        assert!(
            db.write("metrics", "tail", vec![row(2, 2.0)], 2)
                .unwrap()
                .duplicate
        );
        db.checkpoint().unwrap();
    }
}

#[test]
fn direct_pressure_waits_for_inflight_frozen_prefix_then_reopens_exactly() {
    for pages in [false, true] {
        let temp = TempDir::new().unwrap();
        let config = Config {
            checkpoint_frozen_prefix: true,
            derived_pages: pages,
            hot_max_rows: 1,
            segment_rows: 1,
            ..Default::default()
        };
        let db = Database::open(temp.path(), config.clone()).unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        db.write("metrics", "prefix", vec![row(1, 1.0)], 1).unwrap();
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let worker = db.clone();
        let checkpoint = std::thread::spawn(move || worker.checkpoint());
        assert!(hook.wait_until_blocked(Duration::from_secs(5)));
        let before = db.performance();
        let (done_tx, done_rx) = mpsc::channel();
        let writer = db.clone();
        let write = std::thread::spawn(move || {
            let result = writer.write("metrics", "tail", vec![row(2, 2.0)], 2);
            done_tx.send(()).unwrap();
            result
        });
        // RootPrepare is blocked outside state and this writer is the only
        // remaining state-lock caller. Its completed hold proves it reached
        // pressure admission and released state before waiting for preparation.
        // The deadline is only a deadlock watchdog, not a latency assertion.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while db.performance().phases["state_lock_hold"].count
            == before.phases["state_lock_hold"].count
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        let entered = db.performance().phases["state_lock_hold"].count
            > before.phases["state_lock_hold"].count;
        let waiting = matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        hook.release();
        checkpoint.join().unwrap().unwrap();
        let receipt = write.join().unwrap().unwrap();
        assert!(entered, "direct writer never reached pressure admission");
        assert!(
            waiting,
            "pressure write completed before the blocked root released"
        );
        assert_eq!(receipt.sequence, 3);
        assert!(!receipt.duplicate);
        assert_eq!(db.status().unwrap().checkpoint_sequence, 2);
        assert_eq!(
            db.performance().phases["checkpoint_locked"].count,
            before.phases["checkpoint_locked"].count,
        );
        assert_eq!(db.performance().phases["group_prepare"].count, 0);
        assert_eq!(values(&db), vec![1.0f64.to_bits(), 2.0f64.to_bits()]);
        db.set_maintenance_test_hook(None).unwrap();
        drop(db);
        let db = Database::open(temp.path(), config).unwrap();
        assert_eq!(values(&db), vec![1.0f64.to_bits(), 2.0f64.to_bits()]);
        assert!(
            db.write("metrics", "tail", vec![row(2, 2.0)], 2)
                .unwrap()
                .duplicate
        );
        db.checkpoint().unwrap();
    }
}

#[test]
fn frozen_tail_age_and_recovered_unknown_age_are_conservative() {
    let temp = TempDir::new().unwrap();
    let age_config = Config {
        checkpoint_frozen_prefix: true,
        flush_interval_us: 100,
        segment_rows: 1,
        ..Default::default()
    };
    let db = Database::open(temp.path(), age_config.clone()).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write("metrics", "prefix", vec![row(1, 1.0)], 0).unwrap();
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let worker = db.clone();
    let checkpoint = std::thread::spawn(move || worker.checkpoint());
    assert!(hook.wait_until_blocked(Duration::from_secs(5)));
    db.write("metrics", "tail", vec![row(2, 2.0)], 50).unwrap();
    hook.release();
    checkpoint.join().unwrap().unwrap();
    db.set_maintenance_test_hook(None).unwrap();
    assert!(!db.maintain(149).unwrap().flushed);
    assert!(db.maintain(150).unwrap().flushed);

    db.write("metrics", "unknown-after-reopen", vec![row(3, 3.0)], 1_000)
        .unwrap();
    drop(db);
    let reopened = Database::open(temp.path(), age_config).unwrap();
    // Legacy single-append WAL carries no admission clock. Recovery marks it
    // unknown/old rather than deriving age from event time or making it young.
    assert!(reopened.maintain(1_000).unwrap().flushed);

    let pressure_temp = TempDir::new().unwrap();
    let pressure_config = Config {
        checkpoint_frozen_prefix: true,
        flush_policy: varve::FlushPolicy::PressureOnly,
        flush_interval_us: 1,
        ..Default::default()
    };
    let pressure = Database::open(pressure_temp.path(), pressure_config.clone()).unwrap();
    pressure
        .create_table("metrics", TableConfig::default())
        .unwrap();
    pressure
        .write("metrics", "tail", vec![row(1, 1.0)], 0)
        .unwrap();
    drop(pressure);
    let pressure = Database::open(pressure_temp.path(), pressure_config).unwrap();
    assert!(!pressure.maintain(i64::MAX).unwrap().flushed);
    assert_eq!(pressure.status().unwrap().hot_rows, 1);
}

#[test]
fn floor_only_checkpoint_can_publish_at_the_existing_frontier() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config(true)).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            idempotency_window_us: Some(100),
            ..Default::default()
        },
    )
    .unwrap();
    db.write("metrics", "v1:10:first", vec![row(1, 1.0)], 10)
        .unwrap();
    db.checkpoint().unwrap();
    let frontier = db.status().unwrap().checkpoint_sequence;
    assert!(
        db.write("metrics", "v1:10:first", vec![row(1, 1.0)], 20)
            .unwrap()
            .duplicate
    );
    db.checkpoint().unwrap();
    assert_eq!(db.status().unwrap().checkpoint_sequence, frontier);
    assert_eq!(db.status().unwrap().hot_rows, 0);
    drop(db);
    let reopened = Database::open(temp.path(), config(true)).unwrap();
    assert_eq!(reopened.idempotency_floor_us("metrics").unwrap(), Some(-80));
}

#[test]
fn file_store_restores_frozen_root_plus_contiguous_tail() {
    let temp = TempDir::new().unwrap();
    let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let cfg = config(true);
    let db =
        Database::open_with_remote(temp.path().join("local"), cfg.clone(), Some(store.clone()))
            .unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![100],
            ..Default::default()
        },
    )
    .unwrap();
    db.write("metrics", "prefix", vec![row(1, 1.0)], 1).unwrap();
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let worker = db.clone();
    let checkpoint = std::thread::spawn(move || worker.checkpoint());
    assert!(hook.wait_until_blocked(Duration::from_secs(5)));
    db.write("metrics", "tail", vec![row(2, 2.0)], 2).unwrap();
    hook.release();
    checkpoint.join().unwrap().unwrap();
    db.set_maintenance_test_hook(None).unwrap();
    assert_eq!(db.status().unwrap().checkpoint_sequence, 2);
    assert_eq!(db.ship().unwrap(), 3);

    let restored = Database::restore(temp.path().join("restored"), cfg, store).unwrap();
    assert_eq!(values(&restored), vec![1.0f64.to_bits(), 2.0f64.to_bits()]);
    assert_eq!(restored.rollups("metrics").unwrap()[0].count, 2);
    assert!(
        restored
            .write("metrics", "prefix", vec![row(1, 1.0)], 3)
            .unwrap()
            .duplicate
    );
    assert!(
        restored
            .write("metrics", "tail", vec![row(2, 2.0)], 3)
            .unwrap()
            .duplicate
    );
}

struct BlockingPutStore {
    inner: FileStore,
    blocked: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
}

impl BlockingPutStore {
    fn new(path: &std::path::Path) -> Arc<Self> {
        Arc::new(Self {
            inner: FileStore::new(path).unwrap(),
            blocked: Mutex::new(None),
        })
    }

    fn block_next_put(&self) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        *self.blocked.lock().unwrap() = Some((started_tx, release_rx));
        (started_rx, release_tx)
    }
}

impl RemoteStore for BlockingPutStore {
    fn local_root(&self) -> Option<&std::path::Path> {
        self.inner.local_root()
    }

    fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        self.inner.get(key)
    }

    fn get_bounded(&self, key: &str, max_bytes: usize) -> anyhow::Result<Vec<u8>> {
        self.inner.get_bounded(key, max_bytes)
    }

    fn put_immutable(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        if let Some((started, release)) = self.blocked.lock().unwrap().take() {
            started.send(())?;
            release.recv_timeout(Duration::from_secs(5))?;
        }
        self.inner.put_immutable(key, bytes)
    }

    fn head(&self) -> anyhow::Result<Option<HeadObject>> {
        self.inner.head()
    }

    fn compare_and_swap_head(
        &self,
        expected: Option<&str>,
        bytes: &[u8],
    ) -> anyhow::Result<String> {
        self.inner.compare_and_swap_head(expected, bytes)
    }

    fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key)
    }
}

#[test]
fn captured_ship_survives_newer_prefix_publication_and_wal_retirement() {
    let temp = TempDir::new().unwrap();
    let store = BlockingPutStore::new(&temp.path().join("remote"));
    let cfg = config(false);
    let db =
        Database::open_with_remote(temp.path().join("local"), cfg.clone(), Some(store.clone()))
            .unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write("metrics", "captured", vec![row(1, 1.0)], 1)
        .unwrap();

    let (started, release) = store.block_next_put();
    let shipper = db.clone();
    let shipping = std::thread::spawn(move || shipper.ship());
    started.recv_timeout(Duration::from_secs(5)).unwrap();

    db.checkpoint().unwrap();
    assert_eq!(db.status().unwrap().checkpoint_sequence, 2);
    assert!(
        fs::read_dir(temp.path().join("local/wal"))
            .unwrap()
            .next()
            .is_none()
    );
    db.write("metrics", "newer-local", vec![row(2, 2.0)], 2)
        .unwrap();

    release.send(()).unwrap();
    assert_eq!(shipping.join().unwrap().unwrap(), 2);
    let restored = Database::restore(temp.path().join("restored-old"), cfg, store).unwrap();
    assert_eq!(values(&restored), vec![1.0f64.to_bits()]);
    assert!(
        restored
            .write("metrics", "captured", vec![row(1, 1.0)], 3)
            .unwrap()
            .duplicate
    );
    assert_eq!(restored.status().unwrap().sequence, 2);
}

#[test]
fn frozen_prefix_crash_child() {
    let Ok(root) = std::env::var("VARVE_PREFIX_CRASH_ROOT") else {
        return;
    };
    let pages = std::env::var("VARVE_PREFIX_PAGES").unwrap() == "true";
    let db = Database::open(root, config(pages)).unwrap();
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::CheckpointPrepare);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let worker = db.clone();
    let checkpoint = std::thread::spawn(move || worker.checkpoint());
    assert!(hook.wait_until_blocked(Duration::from_secs(5)));
    db.write("metrics", "tail", vec![row(2, 2.0)], 2).unwrap();
    hook.release();
    let result = checkpoint.join().unwrap();
    if std::env::var_os("VARVE_IO_FAILPOINT").is_some() {
        assert!(result.is_err());
        assert!(
            db.status()
                .unwrap()
                .fenced
                .as_ref()
                .unwrap()
                .contains("manifest")
        );
        assert!(
            db.write("metrics", "after-fence", vec![row(3, 3.0)], 3)
                .is_err()
        );
        assert!(db.write("metrics", "prefix", vec![row(1, 1.0)], 3).is_err());
        assert!(db.checkpoint().is_err());
    } else {
        result.unwrap();
    }
}

#[test]
fn frozen_prefix_crash_boundaries_recover_root_and_tail_exactly() {
    for pages in [false, true] {
        for (point, io) in [
            ("segments_published", false),
            ("manifest_published", false),
            ("frozen_prefix_installed", false),
            ("frozen_prefix_wal_retired", false),
            ("atomic_manifest.bin_before_write", true),
            ("atomic_manifest.bin_before_rename", true),
            ("atomic_manifest.bin_before_dir_sync", true),
        ] {
            let temp = TempDir::new().unwrap();
            let db = Database::open(temp.path(), config(pages)).unwrap();
            db.create_table(
                "metrics",
                TableConfig {
                    rollup_widths_us: vec![100],
                    ..TableConfig::default()
                },
            )
            .unwrap();
            db.write("metrics", "prefix", vec![row(1, 1.0)], 1).unwrap();
            drop(db);

            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "frozen_prefix_crash_child", "--nocapture"])
                .env("VARVE_PREFIX_CRASH_ROOT", temp.path())
                .env("VARVE_PREFIX_PAGES", pages.to_string())
                .env_remove(if io {
                    "VARVE_FAILPOINT"
                } else {
                    "VARVE_IO_FAILPOINT"
                })
                .env(
                    if io {
                        "VARVE_IO_FAILPOINT"
                    } else {
                        "VARVE_FAILPOINT"
                    },
                    point,
                )
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(if io { 0 } else { 86 }),
                "pages={pages}, {point}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );

            let db = Database::open(temp.path(), config(pages)).unwrap();
            assert_eq!(
                values(&db),
                vec![1.0f64.to_bits(), 2.0f64.to_bits()],
                "{point}"
            );
            assert_eq!(
                db.rollups("metrics")
                    .unwrap()
                    .iter()
                    .map(|row| row.count)
                    .sum::<u64>(),
                2,
                "{point}"
            );
            assert!(
                db.write("metrics", "prefix", vec![row(1, 1.0)], 3)
                    .unwrap()
                    .duplicate
            );
            assert!(
                db.write("metrics", "tail", vec![row(2, 2.0)], 3)
                    .unwrap()
                    .duplicate
            );
            db.checkpoint().unwrap();
            let manifest = fs::read(temp.path().join("manifest.bin")).unwrap();
            assert_eq!(
                &manifest[..8],
                if pages { b"VARVEM02" } else { b"VARVEM01" }
            );
            drop(db);
            assert_eq!(
                values(&Database::open(temp.path(), config(pages)).unwrap()),
                vec![1.0f64.to_bits(), 2.0f64.to_bits()]
            );
        }
    }
}

#[test]
fn pinned_sql_snapshots_cover_exactly_once_before_and_after_prefix_switch_and_gc() {
    for pages in [false, true] {
        for retained in [false, true] {
            for cache in [0, 1024 * 1024] {
                let temp = TempDir::new().unwrap();
                let config = Config {
                    query_retained_inputs: retained,
                    decoded_cache_bytes: cache,
                    query_workers: 2,
                    query_executable: std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join(".tools/duckdb"),
                    compact_min_segments: 2,
                    segment_rows: 8,
                    ..config(pages)
                };
                let db = Database::open(temp.path(), config.clone()).unwrap();
                db.create_table(
                    "metrics",
                    TableConfig {
                        shards: 1,
                        window_us: 100,
                        rollup_widths_us: vec![10],
                        ..Default::default()
                    },
                )
                .unwrap();
                db.write("metrics", "cold", vec![row(1, 10.0)], 1).unwrap();
                db.checkpoint().unwrap();
                db.write("metrics", "prefix", vec![row(2, 20.0)], 2)
                    .unwrap();
                let root = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
                db.set_maintenance_test_hook(Some(root.clone())).unwrap();
                let worker = db.clone();
                let checkpoint = std::thread::spawn(move || worker.checkpoint());
                assert!(root.wait_until_blocked(Duration::from_secs(5)));
                let sql = "SELECT timestamp_us, value, sequence, ordinal FROM metrics ORDER BY timestamp_us, sequence, ordinal";
                let before = MaintenanceTestHook::new(MaintenanceHookPhase::SqlSnapshotCaptured);
                db.set_maintenance_test_hook(Some(before.clone())).unwrap();
                let worker = db.clone();
                let query_before = std::thread::spawn(move || worker.query(sql));
                assert!(before.wait_until_blocked(Duration::from_secs(5)));
                db.write("metrics", "tail", vec![row(3, 30.0)], 3).unwrap();
                root.release();
                checkpoint.join().unwrap().unwrap();
                assert_eq!(db.status().unwrap().checkpoint_sequence, 3);
                assert_eq!(db.status().unwrap().hot_rows, 1);
                let after = MaintenanceTestHook::new(MaintenanceHookPhase::SqlSnapshotCaptured);
                db.set_maintenance_test_hook(Some(after.clone())).unwrap();
                let worker = db.clone();
                let query_after = std::thread::spawn(move || worker.query(sql));
                assert!(after.wait_until_blocked(Duration::from_secs(5)));
                let pinned_files: Vec<_> = fs::read_dir(temp.path().join("segments"))
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
                    .collect();
                assert_eq!(pinned_files.len(), 2);
                // Retire the tail and replace every old segment while both SQL
                // snapshots still own their pre-/post-switch files and batches.
                db.checkpoint().unwrap();
                let old_files: Vec<_> = fs::read_dir(temp.path().join("segments"))
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
                    .collect();
                assert_eq!(old_files.len(), 3);
                assert_eq!(db.compact().unwrap(), 1);
                db.maintain(4).unwrap();
                if !retained || cache == 0 {
                    assert!(
                        pinned_files.iter().all(|path| path.exists()),
                        "pinned file coverage was removed"
                    );
                }
                before.release();
                after.release();
                let expected_before = serde_json::json!([
                    {"timestamp_us":1,"value":10.0,"sequence":"2","ordinal":0},
                    {"timestamp_us":2,"value":20.0,"sequence":"3","ordinal":0}
                ]);
                let expected_after = serde_json::json!([
                    {"timestamp_us":1,"value":10.0,"sequence":"2","ordinal":0},
                    {"timestamp_us":2,"value":20.0,"sequence":"3","ordinal":0},
                    {"timestamp_us":3,"value":30.0,"sequence":"4","ordinal":0}
                ]);
                assert_eq!(query_before.join().unwrap().unwrap(), expected_before);
                assert_eq!(query_after.join().unwrap().unwrap(), expected_after);
                db.set_maintenance_test_hook(None).unwrap();
                db.maintain(5).unwrap();
                assert!(old_files.iter().all(|path| !path.exists()));
                assert_eq!(db.status().unwrap().active_snapshots, 0);
                assert_eq!(db.status().unwrap().derived_working_bytes, 0);
                assert_eq!(db.query(sql).unwrap(), expected_after);
                drop(db);
                let db = Database::open(temp.path(), config).unwrap();
                assert_eq!(db.query(sql).unwrap(), expected_after);
            }
        }
    }
}
