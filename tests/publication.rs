use anyhow::{Result, bail};
#[cfg(feature = "fault-injection")]
use fs2::FileExt;
use std::collections::BTreeMap;
#[cfg(feature = "fault-injection")]
use std::fs;
use std::path::Path;
#[cfg(feature = "fault-injection")]
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tempfile::TempDir;
use varve::remote::{FileStore, HeadObject, ListPage, RemoteStore};
use varve::{Config, Database, Row, StoredRow, TableConfig, segment};

fn row(timestamp_us: i64, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value,
        tags: BTreeMap::new(),
    }
}

fn config() -> Config {
    Config {
        segment_rows: 16,
        ..Default::default()
    }
}

fn initialize(root: &Path, remote: Arc<dyn RemoteStore>) -> Database {
    let db = Database::open_with_remote(root, config(), Some(remote)).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write("metrics", "batch-1", vec![row(1, 1.0)], 1)
        .unwrap();
    db
}

#[cfg(feature = "fault-injection")]
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

#[cfg(feature = "fault-injection")]
fn run_crash_worker(
    root: &Path,
    remote: &Path,
    mode: &str,
    failpoint: &str,
) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "publication_fault_worker", "--nocapture"])
        .env("VARVE_PUBLICATION_ROOT", root)
        .env("VARVE_PUBLICATION_REMOTE", remote)
        .env("VARVE_PUBLICATION_MODE", mode)
        .env("VARVE_FAILPOINT", failpoint)
        .output()
        .unwrap()
}

#[test]
#[cfg(feature = "fault-injection")]
fn publication_fault_worker() {
    let Ok(root) = std::env::var("VARVE_PUBLICATION_ROOT") else {
        return;
    };
    let remote_path = std::env::var("VARVE_PUBLICATION_REMOTE").unwrap();
    let mode = std::env::var("VARVE_PUBLICATION_MODE").unwrap();
    let remote = Arc::new(FileStore::new(remote_path).unwrap());
    match mode.as_str() {
        "initial" | "stale" => {
            let db = initialize(Path::new(&root), remote);
            db.ship().unwrap();
        }
        "later" => {
            let db = Database::open_with_remote(&root, config(), Some(remote)).unwrap();
            db.write("metrics", "batch-2", vec![row(2, 2.0)], 2)
                .unwrap();
            db.ship().unwrap();
        }
        "gc" => {
            let db = Database::open_with_remote(&root, config(), Some(remote)).unwrap();
            db.vacuum_remote().unwrap();
        }
        _ => panic!("unknown worker mode"),
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn initial_cas_crash_adopts_only_the_exact_intended_head() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let remote_path = temp.path().join("remote");
    let output = run_crash_worker(&root, &remote_path, "initial", "remote_head_published");
    assert_eq!(output.status.code(), Some(86), "{output:?}");
    assert!(root.join("remote-publication.intent").exists());
    let reservation = std::fs::File::open(root.join("remote-binding.reserve")).unwrap();
    assert!(reservation.allocated_size().unwrap() >= reservation.metadata().unwrap().len());
    assert!(!root.join("remote-binding.json").exists());

    let remote = Arc::new(FileStore::new(&remote_path).unwrap());
    let db = Database::open_with_remote(&root, config(), Some(remote)).unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
    assert!(root.join("remote-binding.json").exists());
    assert!(!root.join("remote-publication.intent").exists());
    assert!(!root.join("remote-binding.reserve").exists());
    db.ship().unwrap();
}

#[test]
#[cfg(feature = "fault-injection")]
fn later_cas_crash_preserves_newer_local_acknowledgements() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let remote_path = temp.path().join("remote");
    let remote = Arc::new(FileStore::new(&remote_path).unwrap());
    let db = initialize(&root, remote);
    db.ship().unwrap();
    drop(db);

    let output = run_crash_worker(&root, &remote_path, "later", "remote_head_published");
    assert_eq!(output.status.code(), Some(86), "{output:?}");
    let remote = Arc::new(FileStore::new(&remote_path).unwrap());
    let db = Database::open_with_remote(&root, config(), Some(remote)).unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
    assert_eq!(db.ship().unwrap(), db.status().unwrap().sequence);
}

#[test]
#[cfg(feature = "fault-injection")]
fn gc_lock_acquire_and_release_crashes_reconcile_exactly() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let remote_path = temp.path().join("remote");
    let remote = Arc::new(FileStore::new(&remote_path).unwrap());
    let db = initialize(&root, remote.clone());
    db.ship().unwrap();
    remote
        .put_immutable("segments/orphan.parquet", b"orphan")
        .unwrap();
    drop(db);

    let acquire = run_crash_worker(&root, &remote_path, "gc", "remote_gc_lock_acquired");
    assert_eq!(acquire.status.code(), Some(86), "{acquire:?}");
    let locked: serde_json::Value =
        serde_json::from_slice(&remote.head().unwrap().unwrap().bytes).unwrap();
    assert_eq!(locked["lock"]["kind"], "gc");

    let release = run_crash_worker(&root, &remote_path, "gc", "remote_gc_lock_released");
    assert_eq!(release.status.code(), Some(86), "{release:?}");
    let released: serde_json::Value =
        serde_json::from_slice(&remote.head().unwrap().unwrap().bytes).unwrap();
    assert!(released["lock"].is_null());

    let db = Database::open_with_remote(&root, config(), Some(remote.clone())).unwrap();
    assert!(remote.get("segments/orphan.parquet").is_err());
    db.ship().unwrap();
}

#[test]
#[cfg(feature = "fault-injection")]
fn pre_cas_crash_is_cleaned_as_stale_when_the_predecessor_is_unchanged() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let remote_path = temp.path().join("remote");
    let output = run_crash_worker(
        &root,
        &remote_path,
        "stale",
        "remote_publication_intent_persisted",
    );
    assert_eq!(output.status.code(), Some(86), "{output:?}");
    let remote = Arc::new(FileStore::new(&remote_path).unwrap());
    assert!(remote.head().unwrap().is_none());
    let db = Database::open_with_remote(&root, config(), Some(remote.clone())).unwrap();
    assert!(!root.join("remote-publication.intent").exists());
    db.ship().unwrap();
    assert!(remote.head().unwrap().is_some());
}

#[test]
#[cfg(feature = "fault-injection")]
fn corrupt_intent_and_sequence_ahead_fail_closed() {
    let temp = TempDir::new().unwrap();
    let corrupt_root = temp.path().join("corrupt-local");
    let corrupt_remote = temp.path().join("corrupt-remote");
    let output = run_crash_worker(
        &corrupt_root,
        &corrupt_remote,
        "stale",
        "remote_publication_intent_persisted",
    );
    assert_eq!(output.status.code(), Some(86));
    let intent = corrupt_root.join("remote-publication.intent");
    let mut bytes = fs::read(&intent).unwrap();
    bytes[20] ^= 1;
    fs::write(&intent, bytes).unwrap();
    let remote = Arc::new(FileStore::new(&corrupt_remote).unwrap());
    assert!(Database::open_with_remote(&corrupt_root, config(), Some(remote)).is_err());

    let ahead_root = temp.path().join("ahead-local");
    let ahead_remote = temp.path().join("ahead-remote");
    let output = run_crash_worker(
        &ahead_root,
        &ahead_remote,
        "initial",
        "remote_head_published",
    );
    assert_eq!(output.status.code(), Some(86));
    let mut wal_files = fs::read_dir(ahead_root.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .collect::<Vec<_>>();
    wal_files.sort();
    fs::remove_file(wal_files.pop().unwrap()).unwrap();
    let remote = Arc::new(FileStore::new(&ahead_remote).unwrap());
    let error = Database::open_with_remote(&ahead_root, config(), Some(remote))
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("intent sequence is ahead"));
}

#[test]
#[cfg(feature = "fault-injection")]
fn copied_root_and_foreign_head_are_never_adopted() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let clone = temp.path().join("clone");
    let remote_path = temp.path().join("remote");
    let output = run_crash_worker(&root, &remote_path, "initial", "remote_head_published");
    assert_eq!(output.status.code(), Some(86));
    copy_tree(&root, &clone);
    let remote = Arc::new(FileStore::new(&remote_path).unwrap());
    let error = Database::open_with_remote(&clone, config(), Some(remote.clone()))
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("different database root"));
    Database::open_with_remote(&root, config(), Some(remote)).unwrap();

    let foreign_root = temp.path().join("foreign-local");
    let foreign_remote_path = temp.path().join("foreign-remote");
    let output = run_crash_worker(
        &foreign_root,
        &foreign_remote_path,
        "stale",
        "remote_publication_intent_persisted",
    );
    assert_eq!(output.status.code(), Some(86));
    let foreign_remote = Arc::new(FileStore::new(&foreign_remote_path).unwrap());
    foreign_remote
        .compare_and_swap_head(None, br#"{"foreign":true}"#)
        .unwrap();
    let db = Database::open_with_remote(&foreign_root, config(), Some(foreign_remote)).unwrap();
    assert!(db.status().unwrap().fenced.is_some());
    assert!(!foreign_root.join("remote-binding.json").exists());
}

struct AmbiguousStore {
    inner: FileStore,
    fail_once: AtomicBool,
    apply_before_failure: bool,
}

impl RemoteStore for AmbiguousStore {
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
        self.inner.put_immutable(key, bytes)
    }

    fn head(&self) -> Result<Option<HeadObject>> {
        self.inner.head()
    }

    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        let fail = self.fail_once.swap(false, Ordering::SeqCst);
        if fail && !self.apply_before_failure {
            bail!("injected rejected CAS");
        }
        let token = self.inner.compare_and_swap_head(expected, bytes)?;
        if fail {
            bail!("injected ambiguous CAS response");
        }
        Ok(token)
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
fn exact_head_resolves_an_ambiguous_cas_response() {
    let temp = TempDir::new().unwrap();
    let remote = Arc::new(AmbiguousStore {
        inner: FileStore::new(temp.path().join("remote")).unwrap(),
        fail_once: AtomicBool::new(true),
        apply_before_failure: true,
    });
    let db = initialize(&temp.path().join("local"), remote);
    db.ship().unwrap();
    assert!(temp.path().join("local/remote-binding.json").exists());
    assert!(!temp.path().join("local/remote-publication.intent").exists());
}

#[test]
fn rejected_cas_cleans_intent_only_when_the_predecessor_is_unchanged() {
    let temp = TempDir::new().unwrap();
    let remote = Arc::new(AmbiguousStore {
        inner: FileStore::new(temp.path().join("remote")).unwrap(),
        fail_once: AtomicBool::new(true),
        apply_before_failure: false,
    });
    let root = temp.path().join("local");
    let db = initialize(&root, remote.clone());
    assert!(db.ship().is_err());
    assert!(remote.head().unwrap().is_none());
    assert!(!root.join("remote-publication.intent").exists());
    assert!(!root.join("remote-binding.reserve").exists());
    db.ship().unwrap();
}

#[test]
fn binding_reservation_is_admitted_before_remote_cas() {
    let temp = TempDir::new().unwrap();
    let remote = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let constrained = Config {
        wal_max_bytes: 8 * 1024,
        max_disk_bytes: 64 * 1024,
        ..config()
    };
    let db =
        Database::open_with_remote(temp.path().join("local"), constrained, Some(remote.clone()))
            .unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write("metrics", "batch", vec![row(1, 1.0)], 1).unwrap();
    let error = db.ship().unwrap_err();
    assert!(format!("{error:#}").contains("disk admission budget exhausted"));
    assert!(remote.head().unwrap().is_none());
    assert!(!temp.path().join("local/remote-publication.intent").exists());
}

#[test]
fn bounded_segment_writer_removes_partial_output() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("bounded.parquet");
    let rows = vec![StoredRow {
        row: row(1, 1.0),
        sequence: 1,
        ordinal: 0,
    }];
    assert!(segment::write_with_limit(&path, &rows, 16).is_err());
    assert!(!path.exists());
    segment::write_with_limit(&path, &rows, 1024 * 1024).unwrap();
    assert_eq!(segment::read(&path).unwrap(), rows);
}

#[test]
#[cfg(feature = "fault-injection")]
fn io_fault_worker() {
    let Ok(root) = std::env::var("VARVE_IO_ROOT") else {
        return;
    };
    let mode = std::env::var("VARVE_IO_MODE").unwrap();
    let remote_path = std::env::var("VARVE_IO_REMOTE").ok();
    let remote = remote_path
        .as_ref()
        .map(|path| Arc::new(FileStore::new(path).unwrap()) as Arc<dyn RemoteStore>);
    let db = Database::open_with_remote(&root, config(), remote).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write("metrics", "acknowledged", vec![row(1, 1.0)], 1)
        .unwrap();
    match mode.as_str() {
        "wal" => {
            unsafe { std::env::set_var("VARVE_IO_FAILPOINT", "wal_before_write:enospc") };
            assert!(
                db.write("metrics", "rejected", vec![row(2, 2.0), row(3, 3.0)], 3,)
                    .is_err()
            );
        }
        "manifest" => {
            unsafe {
                std::env::set_var("VARVE_IO_FAILPOINT", "atomic_manifest.bin_before_write:io")
            };
            assert!(db.checkpoint().is_err());
        }
        "reservation" => {
            unsafe {
                std::env::set_var(
                    "VARVE_IO_FAILPOINT",
                    "remote_binding_reservation_during_write:enospc",
                )
            };
            assert!(db.ship().is_err());
        }
        "binding" => {
            unsafe {
                std::env::set_var("VARVE_IO_FAILPOINT", "remote_binding_before_write:enospc")
            };
            assert!(db.ship().is_err());
        }
        _ => panic!("unknown I/O mode"),
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn injected_io_errors_preserve_acknowledgements_and_atomic_batches() {
    for mode in ["wal", "manifest", "reservation", "binding"] {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("local");
        let remote = temp.path().join("remote");
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "io_fault_worker", "--nocapture"])
            .env("VARVE_IO_ROOT", &root)
            .env("VARVE_IO_REMOTE", &remote)
            .env("VARVE_IO_MODE", mode)
            .status()
            .unwrap();
        assert!(status.success(), "mode {mode}");
        let remote = Arc::new(FileStore::new(&remote).unwrap());
        let db = Database::open_with_remote(&root, config(), Some(remote.clone())).unwrap();
        let rows = db.scan("metrics", None, None, None, None).unwrap();
        assert_eq!(rows.len(), 1, "mode {mode}");
        assert_eq!(rows[0].row.timestamp_us, 1);
        if mode == "reservation" {
            assert!(remote.head().unwrap().is_none());
        }
        if matches!(mode, "reservation" | "binding") {
            db.ship().unwrap();
        }
    }
}
