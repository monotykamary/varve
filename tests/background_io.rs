use anyhow::{Result, ensure};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
#[cfg(feature = "fault-injection")]
use std::process::Command;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::Duration;
use tempfile::TempDir;
use varve::remote::{FileStore, HeadObject, RemoteStore};
use varve::{Config, Database, Row, TableConfig};

struct BlockingStore {
    inner: FileStore,
    block_next_put: AtomicBool,
    block_next_get: AtomicBool,
    fail_put: AtomicBool,
    started: Mutex<Option<mpsc::SyncSender<()>>>,
    released: Mutex<bool>,
    release_cv: Condvar,
}

impl BlockingStore {
    fn new(path: &Path) -> (Arc<Self>, mpsc::Receiver<()>) {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        (
            Arc::new(Self {
                inner: FileStore::new(path).unwrap(),
                block_next_put: AtomicBool::new(true),
                block_next_get: AtomicBool::new(false),
                fail_put: AtomicBool::new(false),
                started: Mutex::new(Some(started_tx)),
                released: Mutex::new(false),
                release_cv: Condvar::new(),
            }),
            started_rx,
        )
    }

    fn block_if_armed(&self, armed: &AtomicBool) {
        if !armed.swap(false, Ordering::SeqCst) {
            return;
        }
        if let Some(started) = self.started.lock().unwrap().take() {
            started.send(()).unwrap();
        }
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.release_cv.wait(released).unwrap();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release_cv.notify_all();
    }

    fn reset(&self, fail_put: bool) -> mpsc::Receiver<()> {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        *self.started.lock().unwrap() = Some(started_tx);
        *self.released.lock().unwrap() = false;
        self.fail_put.store(fail_put, Ordering::SeqCst);
        self.block_next_get.store(false, Ordering::SeqCst);
        self.block_next_put.store(true, Ordering::SeqCst);
        started_rx
    }

    fn reset_get(&self) -> mpsc::Receiver<()> {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        *self.started.lock().unwrap() = Some(started_tx);
        *self.released.lock().unwrap() = false;
        self.fail_put.store(false, Ordering::SeqCst);
        self.block_next_put.store(false, Ordering::SeqCst);
        self.block_next_get.store(true, Ordering::SeqCst);
        started_rx
    }

    fn head_json(&self) -> Value {
        serde_json::from_slice(&self.inner.head().unwrap().unwrap().bytes).unwrap()
    }

    fn checkpoint_json(&self, head: &Value) -> Value {
        let key = head["checkpoint"]["key"].as_str().unwrap();
        let bytes = self.inner.get(key).unwrap();
        assert_eq!(&bytes[..8], b"VARVEM01");
        assert_eq!(
            blake3::hash(&bytes[..bytes.len() - 32]).as_bytes(),
            &bytes[bytes.len() - 32..]
        );
        serde_json::from_slice(&bytes[8..bytes.len() - 32]).unwrap()
    }
}

impl RemoteStore for BlockingStore {
    fn local_root(&self) -> Option<&Path> {
        self.inner.local_root()
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.block_if_armed(&self.block_next_get);
        self.inner.get(key)
    }

    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.block_if_armed(&self.block_next_put);
        ensure!(
            !self.fail_put.load(Ordering::SeqCst),
            "injected upload failure"
        );
        self.inner.put_immutable(key, bytes)
    }

    fn head(&self) -> Result<Option<HeadObject>> {
        self.inner.head()
    }

    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        self.inner.compare_and_swap_head(expected, bytes)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }
}

fn config() -> Config {
    Config {
        flush_interval_us: 1,
        ship_interval_us: 1,
        compact_min_segments: 8,
        segment_rows: 32,
        ..Default::default()
    }
}

fn table(archive_after_us: Option<i64>) -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 100,
        archive_after_us,
        ..Default::default()
    }
}

fn row(timestamp_us: i64, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value,
        tags: BTreeMap::new(),
    }
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn blocked_background_ship_does_not_block_local_ack_or_status_and_publishes_a_prefix() {
    let temp = TempDir::new().unwrap();
    let (store, upload_started) = BlockingStore::new(&temp.path().join("remote"));
    let db = Database::open_with_remote(temp.path().join("local"), config(), Some(store.clone()))
        .unwrap();
    db.create_table("metrics", table(None)).unwrap();
    db.write("metrics", "first", vec![row(1, 1.0)], 1).unwrap();

    let maintainer = db.clone();
    let maintenance = std::thread::spawn(move || maintainer.maintain(10));
    upload_started
        .recv_timeout(Duration::from_secs(5))
        .expect("maintenance did not reach the blocked upload");

    let writer = db.clone();
    let (write_tx, write_rx) = mpsc::sync_channel(1);
    let (checkpoint_tx, checkpoint_rx) = mpsc::sync_channel(1);
    let local = std::thread::spawn(move || {
        let receipt = writer
            .write("metrics", "second", vec![row(2, 2.0)], 2)
            .unwrap();
        let status = writer.status().unwrap();
        write_tx
            .send((receipt.sequence, status.sequence, status.remote_sequence))
            .unwrap();
        writer.checkpoint().unwrap();
        checkpoint_tx.send(()).unwrap();
    });

    let local_status = write_rx.recv_timeout(Duration::from_secs(2));
    if local_status.is_err() {
        store.release();
    }
    let (receipt_sequence, local_sequence, remote_sequence) =
        local_status.expect("local write/status waited for remote upload");
    assert_eq!(
        (receipt_sequence, local_sequence, remote_sequence),
        (3, 3, 0)
    );
    let checkpointed = checkpoint_rx.recv_timeout(Duration::from_secs(2));
    if checkpointed.is_err() {
        store.release();
    }
    checkpointed.expect("local checkpoint waited for remote upload");

    store.release();
    local.join().unwrap();
    let report = maintenance.join().unwrap().unwrap();
    assert_eq!(report.shipped_sequence, Some(2));
    let status = db.status().unwrap();
    assert_eq!((status.remote_sequence, status.sequence), (2, 3));

    let old_head = store.head_json();
    let old_checkpoint = store.checkpoint_json(&old_head);
    assert_eq!(old_head["sequence"], 2);
    assert_eq!(old_head["wal"].as_array().unwrap().len(), 0);
    assert_eq!(old_checkpoint["checkpoint_sequence"], 2);
    assert!(old_checkpoint["tables"]["metrics"]["receipts"]["first"].is_object());
    assert!(old_checkpoint["tables"]["metrics"]["receipts"]["second"].is_null());

    assert_eq!(db.ship().unwrap(), 3);
    let new_head = store.head_json();
    let new_checkpoint = store.checkpoint_json(&new_head);
    assert_eq!(new_head["sequence"], 3);
    assert_eq!(new_checkpoint["checkpoint_sequence"], 3);
    assert!(new_checkpoint["tables"]["metrics"]["receipts"]["second"].is_object());

    drop(db);
    let restored =
        Database::restore(temp.path().join("restored"), config(), store.clone()).unwrap();
    assert_eq!(
        restored
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn blocked_archived_segment_fetch_does_not_block_local_ack_or_status() {
    let temp = TempDir::new().unwrap();
    let (store, _) = BlockingStore::new(&temp.path().join("remote"));
    store.block_next_put.store(false, Ordering::SeqCst);
    let local_root = temp.path().join("local");
    let db = Database::open_with_remote(&local_root, config(), Some(store.clone())).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            retention_us: Some(100),
            ..table(Some(1))
        },
    )
    .unwrap();
    db.write("metrics", "first", vec![row(1, 1.0), row(9, 9.0)], 9)
        .unwrap();
    assert_eq!(db.maintain(20).unwrap().evicted_files, 1);
    assert_eq!(local_root.join("segments").read_dir().unwrap().count(), 0);

    let fetch_started = store.reset_get();
    let maintainer = db.clone();
    let maintenance = std::thread::spawn(move || maintainer.maintain(105));
    fetch_started
        .recv_timeout(Duration::from_secs(5))
        .expect("maintenance did not reach the blocked archived-segment fetch");

    let writer = db.clone();
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let local = std::thread::spawn(move || {
        let receipt = writer
            .write("metrics", "concurrent", vec![row(106, 106.0)], 106)
            .unwrap();
        let status = writer.status().unwrap();
        done_tx
            .send((receipt.sequence, status.sequence, status.remote_sequence))
            .unwrap();
    });
    let observed = done_rx.recv_timeout(Duration::from_secs(2));
    if observed.is_err() {
        store.release();
    }
    assert_eq!(
        observed.expect("local write/status waited for remote fetch"),
        (3, 3, 2)
    );

    store.release();
    local.join().unwrap();
    maintenance.join().unwrap().unwrap();
    assert_eq!(
        db.scan("metrics", None, None, None, None)
            .unwrap()
            .iter()
            .map(|stored| stored.row.timestamp_us)
            .collect::<Vec<_>>(),
        vec![9, 106]
    );
}

#[test]
fn divergent_local_clone_cannot_rebind_an_owned_head() {
    let temp = TempDir::new().unwrap();
    let original_root = temp.path().join("original");
    let remote_root = temp.path().join("remote");
    let store = Arc::new(FileStore::new(&remote_root).unwrap());
    let original =
        Database::open_with_remote(&original_root, config(), Some(store.clone())).unwrap();
    original.create_table("metrics", table(None)).unwrap();
    original
        .write("metrics", "first", vec![row(1, 1.0)], 1)
        .unwrap();
    original.checkpoint().unwrap();
    original.ship().unwrap();
    drop(original);

    let left_root = temp.path().join("left");
    let right_root = temp.path().join("right");
    copy_tree(&original_root, &left_root);
    copy_tree(&original_root, &right_root);

    let left = Database::open_with_remote(&left_root, config(), Some(store.clone())).unwrap();
    left.write("metrics", "left", vec![row(2, 2.0)], 2).unwrap();
    left.checkpoint().unwrap();
    assert_eq!(left.ship().unwrap(), 3);
    drop(left);

    let right = Database::open_with_remote(&right_root, config(), Some(store.clone())).unwrap();
    right
        .write("metrics", "right", vec![row(3, 3.0)], 3)
        .unwrap();
    right.checkpoint().unwrap();
    let error = right.ship().unwrap_err();
    assert!(
        error.to_string().contains("prefix proof failed"),
        "{error:#}"
    );
    assert!(right.status().unwrap().fenced.is_some());
    drop(right);

    let right = Database::open_with_remote(&right_root, config(), Some(store.clone())).unwrap();
    assert_eq!(
        right.scan("metrics", None, None, None, None).unwrap().len(),
        2,
        "the rejected clone retains its divergent local acknowledgement"
    );
    drop(right);
    let restored = Database::restore(temp.path().join("restored"), config(), store).unwrap();
    let rows = restored.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|stored| stored.row.value == 2.0));
    assert!(!rows.iter().any(|stored| stored.row.value == 3.0));
}

#[test]
#[cfg(feature = "fault-injection")]
fn async_ship_crash_worker() {
    let Ok(local_root) = std::env::var("VARVE_ASYNC_CRASH_LOCAL") else {
        return;
    };
    let remote_root = std::env::var("VARVE_ASYNC_CRASH_REMOTE").unwrap();
    let (store, upload_started) = BlockingStore::new(Path::new(&remote_root));
    let db = Database::open_with_remote(&local_root, config(), Some(store.clone())).unwrap();
    let shipper = db.clone();
    let shipping = std::thread::spawn(move || shipper.ship());
    upload_started
        .recv_timeout(Duration::from_secs(5))
        .expect("ship did not reach blocked upload");
    db.write("metrics", "concurrent", vec![row(2, 2.0)], 2)
        .unwrap();
    db.checkpoint().unwrap();
    store.release();
    let _ = shipping.join();
    panic!("remote_head_published failpoint did not terminate the process");
}

#[test]
#[cfg(feature = "fault-injection")]
fn crash_after_older_head_cas_rebinds_proven_prefix_and_ships_local_tail() {
    let temp = TempDir::new().unwrap();
    let local_root = temp.path().join("local");
    let remote_root = temp.path().join("remote");
    let initial_store = Arc::new(FileStore::new(&remote_root).unwrap());
    let db =
        Database::open_with_remote(&local_root, config(), Some(initial_store.clone())).unwrap();
    db.create_table("metrics", table(None)).unwrap();
    db.write("metrics", "first", vec![row(1, 1.0)], 1).unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.ship().unwrap(), 2);
    db.write("metrics", "captured", vec![row(2, 2.0)], 2)
        .unwrap();
    drop(db);

    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "async_ship_crash_worker", "--nocapture"])
        .env("VARVE_ASYNC_CRASH_LOCAL", &local_root)
        .env("VARVE_ASYNC_CRASH_REMOTE", &remote_root)
        .env("VARVE_FAILPOINT", "remote_head_published")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(86),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let store = Arc::new(FileStore::new(&remote_root).unwrap());
    let local = Database::open_with_remote(&local_root, config(), Some(store.clone())).unwrap();
    let before_resume = local.status().unwrap();
    assert_eq!(
        (before_resume.remote_sequence, before_resume.sequence),
        (3, 4)
    );
    assert_eq!(
        local.scan("metrics", None, None, None, None).unwrap().len(),
        3
    );
    assert_eq!(local.ship().unwrap(), 4);
    let resumed = local.status().unwrap();
    assert_eq!((resumed.remote_sequence, resumed.sequence), (4, 4));
    assert!(resumed.fenced.is_none());
    drop(local);

    let restored = Database::restore(temp.path().join("restored"), config(), store).unwrap();
    assert_eq!(restored.status().unwrap().sequence, 4);
    assert_eq!(
        restored
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        3
    );
}

#[test]
#[cfg(feature = "fault-injection")]
fn first_publication_crash_recovers_the_exact_namespace_and_preserves_local_tail() {
    let temp = TempDir::new().unwrap();
    let local_root = temp.path().join("local");
    let remote_root = temp.path().join("old-remote");
    let original_store = Arc::new(FileStore::new(&remote_root).unwrap());
    let db =
        Database::open_with_remote(&local_root, config(), Some(original_store.clone())).unwrap();
    db.create_table("metrics", table(None)).unwrap();
    db.write("metrics", "first", vec![row(1, 1.0)], 1).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "async_ship_crash_worker", "--nocapture"])
        .env("VARVE_ASYNC_CRASH_LOCAL", &local_root)
        .env("VARVE_ASYNC_CRASH_REMOTE", &remote_root)
        .env("VARVE_FAILPOINT", "remote_head_published")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(86),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let db =
        Database::open_with_remote(&local_root, config(), Some(original_store.clone())).unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
    let recovered = db.status().unwrap();
    assert_eq!((recovered.remote_sequence, recovered.sequence), (2, 3));
    assert!(recovered.fenced.is_none());
    assert_eq!(db.ship().unwrap(), 3);
    drop(db);
    let restored =
        Database::restore(temp.path().join("restored"), config(), original_store).unwrap();
    assert_eq!(
        restored
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn failed_background_upload_keeps_local_segments_and_retries_without_advancing_timer() {
    let temp = TempDir::new().unwrap();
    let (store, _) = BlockingStore::new(&temp.path().join("remote"));
    let upload_started = store.reset(true);
    let local_root = temp.path().join("local");
    let db = Database::open_with_remote(&local_root, config(), Some(store.clone())).unwrap();
    db.create_table("metrics", table(Some(1))).unwrap();
    db.write("metrics", "first", vec![row(1, 1.0)], 1).unwrap();

    let maintainer = db.clone();
    let maintenance = std::thread::spawn(move || maintainer.maintain(10));
    upload_started
        .recv_timeout(Duration::from_secs(5))
        .expect("maintenance did not reach the blocked upload");
    assert_eq!(db.status().unwrap().remote_sequence, 0);
    assert_eq!(local_root.join("segments").read_dir().unwrap().count(), 1);
    store.release();
    let error = maintenance.join().unwrap().unwrap_err();
    assert!(error.to_string().contains("injected upload failure"));
    let status = db.status().unwrap();
    assert_eq!(status.remote_sequence, 0);
    assert!(status.last_maintenance_error.is_some());
    assert_eq!(local_root.join("segments").read_dir().unwrap().count(), 1);

    store.fail_put.store(false, Ordering::SeqCst);
    let retried = db.maintain(10).unwrap();
    assert_eq!(retried.shipped_sequence, Some(2));
    assert_eq!(retried.evicted_files, 1);
    assert_eq!(local_root.join("segments").read_dir().unwrap().count(), 0);
    assert!(db.status().unwrap().last_maintenance_error.is_none());
}
