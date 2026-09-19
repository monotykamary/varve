use anyhow::{Result, bail};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;
use tempfile::TempDir;
use varve::remote::{FileStore, HeadObject, ListPage, RemoteStore};
use varve::{Config, Database, Row, TableConfig};

fn config() -> Config {
    Config {
        segmented_journal: true,
        segment_rows: 16,
        compact_min_segments: 2,
        flush_interval_us: 1,
        ship_interval_us: 1,
        ..Config::default()
    }
}

fn table() -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 100,
        rollup_widths_us: vec![10],
        ..TableConfig::default()
    }
}

fn row(timestamp_us: i64, value: f64) -> Row {
    Row {
        timestamp_us,
        value,
        tenant: "tenant".into(),
        series: "cpu".into(),
        tags: BTreeMap::from([("host".into(), "a".into())]),
    }
}

fn head(store: &dyn RemoteStore) -> Value {
    serde_json::from_slice(&store.head().unwrap().unwrap().bytes).unwrap()
}

fn rows(db: &Database) -> Value {
    serde_json::to_value(db.scan("metrics", None, None, None, None).unwrap()).unwrap()
}

fn value_bits(db: &Database) -> Vec<u64> {
    db.scan("metrics", None, None, None, None)
        .unwrap()
        .into_iter()
        .map(|stored| stored.row.value.to_bits())
        .collect()
}
fn rollups(db: &Database) -> Value {
    serde_json::to_value(db.rollups("metrics").unwrap()).unwrap()
}

fn files(root: &Path, extension: &str) -> Vec<PathBuf> {
    let mut result: Vec<_> = fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == extension))
        .collect();
    result.sort();
    result
}

#[test]
fn filesystem_roundtrip_exact_uncheckpointed_and_checkpoint_tails() {
    for checkpoint in [false, true] {
        for derived_pages in [false, true] {
            let temp = TempDir::new().unwrap();
            let root = temp.path().join("local");
            let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
            let cfg = Config {
                derived_pages,
                ..config()
            };
            let db = Database::open_with_remote(&root, cfg.clone(), Some(store.clone())).unwrap();
            db.create_table("metrics", table()).unwrap();
            let first = vec![row(2, 2.0), row(1, -0.0), row(2, 3.125)];
            let first_receipt = db.write("metrics", "first", first.clone(), 3).unwrap();
            if checkpoint {
                db.checkpoint().unwrap();
            }
            let second = vec![row(2, -8.5), row(3, 1.0 / 3.0)];
            let second_receipt = db.write("metrics", "second", second.clone(), 3).unwrap();
            db.ship().unwrap();
            let third = vec![row(4, 7.0)];
            let third_receipt = db.write("metrics", "third", third.clone(), 4).unwrap();
            assert_eq!(third_receipt.durability, "local_fsync");
            assert_eq!(db.ship().unwrap(), third_receipt.sequence);
            let expected_rows = rows(&db);
            let expected_bits = value_bits(&db);
            let expected_rollups = rollups(&db);
            let published = head(store.as_ref());
            assert_eq!(published["wal"], json!([]));
            assert_eq!(published["journal"].as_array().unwrap().len(), 2);
            let mut exact = Vec::new();
            for reference in published["journal"].as_array().unwrap() {
                let name = reference["file_name"].as_str().unwrap();
                let key = reference["object"]["key"].as_str().unwrap();
                let local_bytes = fs::read(root.join("journal").join(name)).unwrap();
                assert_eq!(store.get(key).unwrap(), local_bytes);
                exact.push((name.to_owned(), local_bytes));
            }
            assert_eq!(db.status().unwrap().unshipped_batches, 0);
            let destination = temp.path().join("restored");
            // The persisted marker, not the caller's opt-in, selects replay.
            let restore_config = Config {
                segmented_journal: false,
                ..cfg
            };
            let restored =
                Database::restore(&destination, restore_config.clone(), store.clone()).unwrap();
            assert_eq!(rows(&restored), expected_rows);
            assert_eq!(value_bits(&restored), expected_bits);
            assert_eq!(rollups(&restored), expected_rollups);
            assert!(restored.journal_stats().unwrap().is_some());
            for (name, bytes) in exact {
                assert_eq!(
                    fs::read(destination.join("journal").join(name)).unwrap(),
                    bytes
                );
            }
            for (id, payload, sequence) in [
                ("first", first, first_receipt.sequence),
                ("second", second, second_receipt.sequence),
                ("third", third, third_receipt.sequence),
            ] {
                let receipt = restored.write("metrics", id, payload, 20).unwrap();
                assert!(receipt.duplicate);
                assert_eq!(receipt.sequence, sequence);
            }
            assert!(
                db.ship().is_err(),
                "restore must transfer namespace ownership"
            );
            drop(restored);
            let reopened =
                Database::open_with_remote(&destination, restore_config, Some(store)).unwrap();
            assert_eq!(rows(&reopened), expected_rows);
            assert_eq!(rollups(&reopened), expected_rollups);
        }
    }
}

#[test]
fn missing_corrupt_unsealed_and_unaligned_remote_segments_fail_closed() {
    for fault in [
        "missing",
        "digest",
        "frame",
        "unsealed",
        "range",
        "descriptor",
        "file-id",
        "name",
        "mixed",
        "omitted",
    ] {
        let temp = TempDir::new().unwrap();
        let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
        let db =
            Database::open_with_remote(temp.path().join("local"), config(), Some(store.clone()))
                .unwrap();
        db.create_table("metrics", table()).unwrap();
        db.write("metrics", "first", vec![row(1, 1.0)], 1).unwrap();
        db.ship().unwrap();
        let current = store.head().unwrap().unwrap();
        let mut published: Value = serde_json::from_slice(&current.bytes).unwrap();
        let original_owner = published["owner"].clone();
        let object = published["journal"][0]["object"].clone();
        let key = object["key"].as_str().unwrap();
        match fault {
            "missing" => store.delete(key).unwrap(),
            "digest" => {
                let mut bytes = store.get(key).unwrap();
                bytes[80] ^= 1;
                store.delete(key).unwrap();
                store.put_immutable(key, &bytes).unwrap();
            }
            "frame" | "unsealed" => {
                let mut bytes = store.get(key).unwrap();
                if fault == "frame" {
                    bytes[80] ^= 1;
                } else {
                    bytes.truncate(bytes.len() - 64);
                }
                // Rebind the outer digest so the native journal validator, not
                // merely the object checksum, must reject the corrupted bytes.
                let digest = blake3::hash(&bytes).to_hex().to_string();
                let new_key = format!("journal/{digest}.jrn");
                store.put_immutable(&new_key, &bytes).unwrap();
                published["journal"][0]["object"] =
                    json!({"key": new_key, "digest": digest, "bytes": bytes.len()});
            }
            "range" => published["journal"][0]["first_sequence"] = json!(2),
            "descriptor" => {
                published["journal"][0]["last_sequence"] = json!(3);
                published["sequence"] = json!(3);
            }
            "file-id" => {
                published["journal"][0]["file_name"] = json!("segment-00000000000000000099.jrn")
            }
            "name" => published["journal"][0]["file_name"] = json!("../outside.jrn"),
            "mixed" => {
                published["wal"] = json!([{"key": format!("wal/{}.wal", object["digest"].as_str().unwrap()), "digest": object["digest"], "bytes": object["bytes"]}]);
            }
            "omitted" => {
                published.as_object_mut().unwrap().remove("journal");
            }
            _ => unreachable!(),
        }
        store
            .compare_and_swap_head(
                Some(&current.token),
                &serde_json::to_vec(&published).unwrap(),
            )
            .unwrap();
        let destination = temp.path().join("bad");
        assert!(
            Database::restore(&destination, config(), store.clone()).is_err(),
            "{fault}"
        );
        assert!(!destination.exists(), "partial restore survived {fault}");
        let after = head(store.as_ref());
        assert!(after["lock"].is_null(), "lease leaked for {fault}");
        assert_eq!(
            after["owner"], original_owner,
            "failed restore transferred ownership for {fault}"
        );
    }
}

struct ControlledStore {
    inner: FileStore,
    block: AtomicBool,
    fail_journal: AtomicBool,
    started: Mutex<Option<mpsc::SyncSender<()>>>,
    released: Mutex<bool>,
    release_cv: Condvar,
}

impl ControlledStore {
    fn new(path: &Path, block: bool) -> (Arc<Self>, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::sync_channel(1);
        (
            Arc::new(Self {
                inner: FileStore::new(path).unwrap(),
                block: AtomicBool::new(block),
                fail_journal: AtomicBool::new(false),
                started: Mutex::new(Some(tx)),
                released: Mutex::new(false),
                release_cv: Condvar::new(),
            }),
            rx,
        )
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release_cv.notify_all();
    }
}

struct ReleaseOnDrop(Arc<ControlledStore>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl RemoteStore for ControlledStore {
    fn local_root(&self) -> Option<&Path> {
        self.inner.local_root()
    }
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.get(key)
    }
    fn get_bounded(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>> {
        self.inner.get_bounded(key, max_bytes)
    }
    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        if self.block.swap(false, Ordering::SeqCst) {
            if let Some(tx) = self.started.lock().unwrap().take() {
                tx.send(()).unwrap();
            }
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.release_cv.wait(released).unwrap();
            }
        }
        if key.starts_with("journal/") && self.fail_journal.load(Ordering::SeqCst) {
            bail!("injected journal upload failure");
        }
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
    fn list_page(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<ListPage> {
        self.inner.list_page(prefix, after, limit)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }
    fn delete_batch(&self, keys: &[String]) -> Result<usize> {
        self.inner.delete_batch(keys)
    }
}

#[test]
fn slow_upload_allows_ingest_and_checkpoint_without_retiring_pins() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let (store, started) = ControlledStore::new(&temp.path().join("remote"), true);
    let db = Database::open_with_remote(&root, config(), Some(store.clone())).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "base-one", vec![row(1, 1.0)], 1)
        .unwrap();
    db.checkpoint().unwrap();
    db.write("metrics", "base-two", vec![row(2, 2.0)], 2)
        .unwrap();
    db.checkpoint().unwrap();
    db.write("metrics", "captured-tail", vec![row(3, 3.0)], 3)
        .unwrap();
    let expected_rows = rows(&db);
    let expected_rollups = rollups(&db);
    let frontier = db.status().unwrap().sequence;
    let shipping_db = db.clone();
    let shipping = std::thread::spawn(move || shipping_db.ship());
    let release = ReleaseOnDrop(store.clone());
    started
        .recv_timeout(Duration::from_secs(10))
        .expect("upload did not start");
    let captured_journal = files(&root.join("journal"), "jrn");
    let captured_raw = files(&root.join("segments"), "parquet");
    assert!(!captured_journal.is_empty());
    assert_eq!(captured_raw.len(), 2);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let writer_db = db.clone();
    let writer = std::thread::spawn(move || {
        let result = (|| -> Result<()> {
            writer_db.write("metrics", "concurrent-tail", vec![row(4, 4.0)], 4)?;
            writer_db.checkpoint()?;
            Ok(())
        })();
        done_tx.send(result).unwrap();
    });
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("upload held commit/state locks")
        .unwrap();
    writer.join().unwrap();
    assert_eq!(db.status().unwrap().active_snapshots, 1);
    assert!(db.status().unwrap().sequence > frontier);
    for path in captured_journal.iter().chain(&captured_raw) {
        assert!(path.exists(), "retired pinned {}", path.display());
    }
    assert_eq!(fs::read_dir(root.join("staging")).unwrap().count(), 0);
    store.release();
    assert_eq!(shipping.join().unwrap().unwrap(), frontier);
    drop(release);
    assert_eq!(db.status().unwrap().active_snapshots, 0);
    assert_eq!(db.status().unwrap().unshipped_batches, 1);
    // A no-op checkpoint need not GC. Force a new frontier after pin release.
    db.write("metrics", "reclaim-trigger", vec![row(5, 5.0)], 5)
        .unwrap();
    db.checkpoint().unwrap();
    assert!(files(&root.join("journal"), "jrn").is_empty());
    let restored = Database::restore(temp.path().join("restored"), config(), store).unwrap();
    assert_eq!(rows(&restored), expected_rows);
    assert_eq!(rollups(&restored), expected_rollups);
}

#[test]
fn failed_journal_upload_never_advances_remote_receipt_or_leaks_pins() {
    let temp = TempDir::new().unwrap();
    let (store, _) = ControlledStore::new(&temp.path().join("remote"), false);
    let db = Database::open_with_remote(temp.path().join("local"), config(), Some(store.clone()))
        .unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "a", vec![row(1, 1.0)], 1).unwrap();
    store.fail_journal.store(true, Ordering::SeqCst);
    assert!(db.ship().is_err());
    assert!(store.head().unwrap().is_none());
    assert_eq!(db.status().unwrap().unshipped_batches, 2);
    assert_eq!(db.status().unwrap().active_snapshots, 0);
    assert!(db.status().unwrap().fenced.is_none());
    store.fail_journal.store(false, Ordering::SeqCst);
    db.ship().unwrap();
    assert_eq!(db.status().unwrap().unshipped_batches, 0);
    assert!(!temp.path().join("local/remote-publication.intent").exists());
}

#[test]
fn retention_and_remote_vacuum_preserve_rollups_and_receipts_not_obsolete_journals() {
    let temp = TempDir::new().unwrap();
    let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let db = Database::open_with_remote(temp.path().join("local"), config(), Some(store.clone()))
        .unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            retention_us: Some(10),
            ..table()
        },
    )
    .unwrap();
    let payload = vec![row(1, 3.5), row(2, -1.25)];
    let receipt = db
        .write("metrics", "retained-id", payload.clone(), 2)
        .unwrap();
    let expected_rollups = rollups(&db);
    db.ship().unwrap();
    let old_journals = store.list("journal").unwrap();
    assert!(!old_journals.is_empty());
    db.maintain(100).unwrap();
    assert_eq!(rows(&db), json!([]));
    assert_eq!(rollups(&db), expected_rollups);
    db.vacuum_remote().unwrap();
    assert!(store.list("journal").unwrap().is_empty());
    assert!(store.list("segments").unwrap().is_empty());
    let published = head(store.as_ref());
    assert!(
        published.get("journal").is_none(),
        "empty optional transport must be omitted"
    );
    let restored = Database::restore(temp.path().join("restored"), config(), store).unwrap();
    assert_eq!(rows(&restored), json!([]));
    assert_eq!(rollups(&restored), expected_rollups);
    let duplicate = restored
        .write("metrics", "retained-id", payload, 100)
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.sequence, receipt.sequence);
}

#[test]
fn stale_binding_reconciles_a_segmented_remote_prefix_without_owner_reset() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let db = Database::open_with_remote(&root, config(), Some(store.clone())).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "a", vec![row(1, 1.0)], 1).unwrap();
    db.ship().unwrap();
    let old_binding = fs::read(root.join("remote-binding.json")).unwrap();
    let owner = head(store.as_ref())["owner"].clone();
    db.write("metrics", "b", vec![row(2, 2.0)], 2).unwrap();
    db.ship().unwrap();
    db.write("metrics", "local-only", vec![row(3, 3.0)], 3)
        .unwrap();
    db.checkpoint().unwrap();
    let expected_rows = rows(&db);
    let expected_rollups = rollups(&db);
    drop(db);
    fs::write(root.join("remote-binding.json"), old_binding).unwrap();
    let reopened = Database::open_with_remote(&root, config(), Some(store.clone())).unwrap();
    reopened.ship().unwrap();
    assert_eq!(head(store.as_ref())["owner"], owner);
    assert!(reopened.status().unwrap().fenced.is_none());
    let restored = Database::restore(temp.path().join("restored"), config(), store).unwrap();
    assert_eq!(rows(&restored), expected_rows);
    assert_eq!(rollups(&restored), expected_rollups);
}

#[test]
fn legacy_tail_restore_does_not_silently_migrate_the_head_transport() {
    let temp = TempDir::new().unwrap();
    let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let legacy = Config {
        segmented_journal: false,
        ..config()
    };
    let db =
        Database::open_with_remote(temp.path().join("local"), legacy, Some(store.clone())).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "legacy", vec![row(1, 1.0)], 1).unwrap();
    db.ship().unwrap();
    let expected_rows = rows(&db);
    let restored =
        Database::restore(temp.path().join("restored"), config(), store.clone()).unwrap();
    assert!(restored.journal_stats().unwrap().is_none());
    assert_eq!(rows(&restored), expected_rows);
    restored.ship().unwrap();
    assert!(head(store.as_ref()).get("journal").is_none());
    assert_eq!(head(store.as_ref())["wal"].as_array().unwrap().len(), 2);
}

#[test]
fn corrupt_local_sealed_bytes_cannot_advance_the_remote_head() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let db = Database::open_with_remote(&root, config(), Some(store.clone())).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "a", vec![row(1, 1.0)], 1).unwrap();
    db.ship().unwrap();
    let before = store.head().unwrap().unwrap();
    let sealed = files(&root.join("journal"), "jrn").remove(0);
    let mut bytes = fs::read(&sealed).unwrap();
    bytes[80] ^= 1;
    fs::write(&sealed, bytes).unwrap();
    db.write("metrics", "b", vec![row(2, 2.0)], 2).unwrap();
    assert!(db.ship().is_err());
    assert_eq!(store.head().unwrap().unwrap(), before);
    assert_eq!(db.status().unwrap().unshipped_batches, 1);
    assert_eq!(db.status().unwrap().active_snapshots, 0);
}

#[cfg(feature = "fault-injection")]
#[test]
fn seal_sync_releases_reader_state_and_seal_failure_fences_without_upload() {
    use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};
    struct ReleaseHook(MaintenanceTestHook);
    impl Drop for ReleaseHook {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    for fail in [false, true] {
        let temp = TempDir::new().unwrap();
        let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
        let db =
            Database::open_with_remote(temp.path().join("local"), config(), Some(store.clone()))
                .unwrap();
        db.create_table("metrics", table()).unwrap();
        db.write("metrics", "visible", vec![row(1, 3.5)], 1)
            .unwrap();
        let expected_rows = rows(&db);
        let expected_rollups = rollups(&db);
        let sequence = db.status().unwrap().sequence;
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::WalBeforeSync);
        let release = ReleaseHook(hook.clone());
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let shipping_db = db.clone();
        let shipping = std::thread::spawn(move || shipping_db.ship());
        assert!(
            hook.wait_until_blocked(Duration::from_secs(10)),
            "seal did not reach sync"
        );
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let reader_db = db.clone();
        let reader = std::thread::spawn(move || {
            let result = (|| -> Result<_> {
                Ok((
                    reader_db.status()?.sequence,
                    serde_json::to_value(reader_db.scan("metrics", None, None, None, None)?)?,
                    serde_json::to_value(reader_db.rollups("metrics")?)?,
                ))
            })();
            done_tx.send(result).unwrap();
        });
        let observed = done_rx.recv_timeout(Duration::from_secs(10));
        assert!(
            store.head().unwrap().is_none(),
            "head published before seal durability"
        );
        if fail {
            hook.release_with_error();
        } else {
            hook.release();
        }
        reader.join().unwrap();
        let observed = observed
            .expect("reader state remained held across seal fsync")
            .unwrap();
        assert_eq!(observed, (sequence, expected_rows, expected_rollups));
        let shipped = shipping.join().unwrap();
        drop(release);
        db.set_maintenance_test_hook(None).unwrap();
        assert_eq!(db.status().unwrap().active_snapshots, 0);
        if fail {
            assert!(shipped.is_err());
            assert!(store.head().unwrap().is_none());
            assert!(db.status().unwrap().fenced.is_some());
        } else {
            assert_eq!(shipped.unwrap(), sequence);
            assert!(db.status().unwrap().fenced.is_none());
        }
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn sealed_segment_straddling_a_frozen_checkpoint_restores_exactly_once() {
    use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};
    struct ReleaseHook(MaintenanceTestHook);
    impl Drop for ReleaseHook {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let temp = TempDir::new().unwrap();
    let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let cfg = Config {
        checkpoint_frozen_prefix: true,
        ..config()
    };
    let db =
        Database::open_with_remote(temp.path().join("local"), cfg.clone(), Some(store.clone()))
            .unwrap();
    db.create_table("metrics", table()).unwrap();
    let prefix = db.write("metrics", "prefix", vec![row(1, 1.0)], 1).unwrap();
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::RootPrepare);
    let release = ReleaseHook(hook.clone());
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let worker = db.clone();
    let checkpoint = std::thread::spawn(move || worker.checkpoint());
    assert!(hook.wait_until_blocked(Duration::from_secs(10)));
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let writer_db = db.clone();
    let writer = std::thread::spawn(move || {
        done_tx
            .send(writer_db.write("metrics", "tail", vec![row(2, -0.0)], 2))
            .unwrap();
    });
    let tail = done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("frozen preparation blocked ingest")
        .unwrap();
    writer.join().unwrap();
    hook.release();
    checkpoint.join().unwrap().unwrap();
    drop(release);
    db.set_maintenance_test_hook(None).unwrap();
    assert_eq!(db.status().unwrap().checkpoint_sequence, prefix.sequence);
    db.ship().unwrap();
    let published = head(store.as_ref());
    assert_eq!(published["journal"].as_array().unwrap().len(), 1);
    assert!(published["journal"][0]["first_sequence"].as_u64().unwrap() <= prefix.sequence);
    assert_eq!(
        published["journal"][0]["last_sequence"],
        json!(tail.sequence)
    );
    let restored = Database::restore(temp.path().join("restored"), cfg, store).unwrap();
    assert_eq!(rows(&restored), rows(&db));
    assert_eq!(value_bits(&restored), value_bits(&db));
    assert_eq!(rollups(&restored), rollups(&db));
    assert!(
        restored
            .write("metrics", "prefix", vec![row(1, 1.0)], 3)
            .unwrap()
            .duplicate
    );
    assert!(
        restored
            .write("metrics", "tail", vec![row(2, -0.0)], 3)
            .unwrap()
            .duplicate
    );
}

#[test]
fn legacy_head_remains_a_valid_predecessor_after_explicit_local_upgrade() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let legacy = Config {
        segmented_journal: false,
        ..config()
    };
    let db = Database::open_with_remote(&root, legacy, Some(store.clone())).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "legacy", vec![row(1, 1.0)], 1).unwrap();
    db.checkpoint().unwrap();
    db.ship().unwrap();
    let owner = head(store.as_ref())["owner"].clone();
    assert!(head(store.as_ref()).get("journal").is_none());
    drop(db);
    let upgraded = Database::open_with_remote(&root, config(), Some(store.clone())).unwrap();
    upgraded
        .write("metrics", "segmented", vec![row(2, 2.0)], 2)
        .unwrap();
    upgraded.ship().unwrap();
    assert_eq!(head(store.as_ref())["owner"], owner);
    assert!(
        !head(store.as_ref())["journal"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let expected_rows = rows(&upgraded);
    let restored = Database::restore(temp.path().join("restored"), config(), store).unwrap();
    assert_eq!(rows(&restored), expected_rows);
    assert!(
        restored
            .write("metrics", "legacy", vec![row(1, 1.0)], 20)
            .unwrap()
            .duplicate
    );
}
