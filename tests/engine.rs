use anyhow::Result;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
#[cfg(feature = "fault-injection")]
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tempfile::TempDir;
use varve::model::{shard_for, window_start};
use varve::remote::{FileStore, HeadObject, RemoteStore};
use varve::{Config, Database, Row, TableConfig};

fn row(ts: i64, value: f64) -> Row {
    Row {
        timestamp_us: ts,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value,
        tags: BTreeMap::new(),
    }
}
fn table() -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 100,
        rollup_widths_us: vec![10],
        ..Default::default()
    }
}
fn config() -> Config {
    Config {
        flush_interval_us: 1,
        ship_interval_us: 1,
        compact_min_segments: 2,
        segment_rows: 32,
        ..Default::default()
    }
}
fn open(root: &Path) -> Database {
    Database::open(root, config()).unwrap()
}

#[test]
fn atomic_validation_idempotency_and_replay() {
    let temp = TempDir::new().unwrap();
    let db = open(temp.path());
    db.create_table("metrics", table()).unwrap();
    db.create_table("other", table()).unwrap();
    let rows = vec![row(2, 2.0), row(1, 1.0), row(2, 3.0)];
    let receipt = db.write("metrics", "batch", rows.clone(), 3).unwrap();
    assert_eq!(receipt.durability, "local_fsync");
    assert!(!receipt.duplicate);
    assert!(
        db.write("metrics", "batch", rows.clone(), 100_000)
            .unwrap()
            .duplicate
    );
    assert!(db.write("metrics", "batch", vec![row(1, 9.0)], 3).is_err());
    let before = db.status().unwrap().sequence;
    assert!(
        db.write("metrics", "invalid", vec![row(1, 7.0), row(1, f64::NAN)], 3)
            .is_err()
    );
    assert_eq!(db.status().unwrap().sequence, before);
    assert!(db.scan("other", None, None, None, None).unwrap().is_empty());
    let rollup = db.rollups("metrics").unwrap().pop().unwrap();
    assert_eq!(
        (rollup.count, rollup.sum, rollup.first, rollup.last),
        (3, 6.0, 1.0, 3.0)
    );
    drop(db);
    let db = open(temp.path());
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
    assert_eq!(db.rollups("metrics").unwrap()[0], rollup);
    assert!(db.write("metrics", "batch", rows, 9).unwrap().duplicate);
    db.checkpoint().unwrap();
    assert_eq!(db.status().unwrap().hot_rows, 0);
    assert_eq!(db.status().unwrap().wal_bytes, 0);
    drop(db);
    let db = open(temp.path());
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 3);
}

#[test]
fn event_time_partitioning_and_raw_expiration() {
    assert_eq!(window_start(-1, 10).unwrap(), -10);
    assert_eq!(window_start(-10, 10).unwrap(), -10);
    assert!(window_start(i64::MIN, 3).is_err());
    assert_eq!(shard_for("ab", "c", 16), shard_for("ab", "c", 16));
    let tmp = TempDir::new().unwrap();
    let db = open(tmp.path());
    let t = TableConfig {
        window_us: 10,
        retention_us: Some(25),
        late_after_us: Some(100),
        ..table()
    };
    db.create_table("metrics", t).unwrap();
    let rows: Vec<_> = [-11, -10, -1, 0, 1, 11]
        .into_iter()
        .map(|t| row(t, 1.0))
        .collect();
    db.write("metrics", "a", rows.clone(), 20).unwrap();
    assert!(
        db.write("metrics", "too_late", vec![row(-81, 1.0)], 20)
            .is_err()
    );
    let report = db.maintain(20).unwrap();
    assert_eq!(report.expired_rows, 2);
    assert_eq!(
        db.scan("metrics", None, None, None, None)
            .unwrap()
            .iter()
            .map(|r| r.row.timestamp_us)
            .collect::<Vec<_>>(),
        vec![-1, 0, 1, 11]
    );
    assert_eq!(
        db.rollups("metrics")
            .unwrap()
            .iter()
            .map(|r| r.count)
            .sum::<u64>(),
        6
    );
    assert!(db.write("metrics", "a", rows, 1000).unwrap().duplicate);
    assert!(
        db.write("metrics", "resurrect", vec![row(-10, 1.0)], 20)
            .is_err()
    );
    db.maintain(10).unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 4);
    drop(db);
    let db = open(tmp.path());
    assert_eq!(
        db.scan("metrics", Some(0), Some(11), Some("tenant"), Some("cpu"))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        db.rollups("metrics")
            .unwrap()
            .iter()
            .map(|r| r.count)
            .sum::<u64>(),
        6
    );
}

#[test]
fn rollup_boundaries_and_finite_state() {
    let tmp = TempDir::new().unwrap();
    let db = open(tmp.path());
    let t = TableConfig {
        rollup_retention_us: Some(15),
        ..table()
    };
    db.create_table("metrics", t).unwrap();
    db.write(
        "metrics",
        "a",
        vec![row(1, 1.0), row(10, 2.0), row(20, 3.0)],
        20,
    )
    .unwrap();
    db.maintain(25).unwrap();
    assert_eq!(db.rollups("metrics").unwrap().len(), 2);
    db.write("metrics", "late", vec![row(1, 4.0)], 25).unwrap();
    assert_eq!(db.rollups("metrics").unwrap().len(), 2);
    db.create_table("huge", table()).unwrap();
    db.write("huge", "a", vec![row(1, f64::MAX)], 2).unwrap();
    let seq = db.status().unwrap().sequence;
    assert!(
        db.write("huge", "overflow", vec![row(1, f64::MAX)], 2)
            .is_err()
    );
    assert_eq!(seq, db.status().unwrap().sequence);
    assert_eq!(db.rollups("huge").unwrap()[0].count, 1);
}

#[test]
fn metadata_hot_and_disk_admission_are_explicit() {
    let tmp = TempDir::new().unwrap();
    let c = Config {
        max_idempotency_keys: 2,
        max_rollup_groups: 2,
        hot_max_rows: 2,
        ..config()
    };
    let db = Database::open(tmp.path(), c).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "a", vec![row(1, 1.0), row(2, 2.0)], 2)
        .unwrap();
    db.write("metrics", "b", vec![row(3, 3.0)], 3).unwrap();
    assert_eq!(db.status().unwrap().hot_rows, 1);
    assert!(db.status().unwrap().segments > 0);
    assert!(db.write("metrics", "c", vec![row(4, 4.0)], 4).is_err());
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
    assert!(Database::open(tmp.path(), config()).is_err());
    let small = TempDir::new().unwrap();
    let c = Config {
        max_disk_bytes: 4096,
        wal_max_bytes: 2048,
        ..config()
    };
    let db = Database::open(small.path(), c).unwrap();
    db.create_table("metrics", table()).unwrap();
    let rows = vec![row(1, 1.0); 100];
    assert!(db.write("metrics", "large", rows, 1).is_err());
    assert_eq!(db.status().unwrap().hot_rows, 0);
}

#[test]
fn committed_corruption_and_sequence_gaps_fail_closed() {
    let tmp = TempDir::new().unwrap();
    let db = open(tmp.path());
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "a", vec![row(1, 1.0)], 1).unwrap();
    db.write("metrics", "b", vec![row(2, 2.0)], 2).unwrap();
    drop(db);
    let path = tmp.path().join("wal/00000000000000000002.wal");
    let original = fs::read(&path).unwrap();
    let mut bad = original.clone();
    bad[25] ^= 1;
    fs::write(&path, &bad).unwrap();
    assert!(Database::open(tmp.path(), config()).is_err());
    fs::write(&path, original).unwrap();
    fs::remove_file(&path).unwrap();
    assert!(Database::open(tmp.path(), config()).is_err());
    let other = TempDir::new().unwrap();
    let db = open(other.path());
    db.create_table("metrics", table()).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let path = other.path().join("manifest.bin");
    let mut bad = fs::read(&path).unwrap();
    bad[12] ^= 1;
    fs::write(path, bad).unwrap();
    assert!(Database::open(other.path(), config()).is_err());
}

#[test]
fn compaction_sql_and_snapshot_consistency() {
    let tmp = TempDir::new().unwrap();
    let db = open(tmp.path());
    db.create_table("metrics", table()).unwrap();
    for i in 1..=4 {
        db.write("metrics", &format!("b{i}"), vec![row(i, i as f64)], i)
            .unwrap();
        db.checkpoint().unwrap();
    }
    assert_eq!(db.status().unwrap().segments, 4);
    assert_eq!(db.compact().unwrap(), 1);
    assert_eq!(db.status().unwrap().segments, 1);
    db.write("metrics", "hot", vec![row(5, 5.0)], 5).unwrap();
    let result = db
        .query("SELECT count(*) AS n, sum(value) AS total FROM metrics")
        .unwrap();
    assert_eq!(result[0]["n"], 5);
    assert_eq!(result[0]["total"], 15.0);
    let sql_rollup = db
        .query("SELECT CAST(sum(count) AS BIGINT) AS n, sum(sum) AS total FROM metrics__rollup")
        .unwrap();
    assert_eq!(sql_rollup[0]["n"], 5);
    let writer = db.clone();
    let handle = std::thread::spawn(move || {
        for i in 6..=12 {
            writer
                .write("metrics", &format!("b{i}"), vec![row(i, i as f64)], i)
                .unwrap();
            writer.checkpoint().unwrap();
            writer.compact().unwrap();
        }
    });
    for _ in 0..4 {
        let rows = db
            .query("SELECT count(*) AS n, sum(value) AS total FROM metrics")
            .unwrap();
        let n = rows[0]["n"].as_u64().unwrap();
        assert_eq!(rows[0]["total"].as_f64().unwrap(), (n * (n + 1) / 2) as f64);
    }
    handle.join().unwrap();
    assert_eq!(
        db.scan("metrics", None, None, None, None).unwrap().len(),
        12
    );
}

struct FaultStore {
    inner: FileStore,
    offline: AtomicBool,
    fail_head: AtomicBool,
    corrupt_wal: AtomicBool,
}
impl FaultStore {
    fn new(path: &Path) -> Self {
        Self {
            inner: FileStore::new(path).unwrap(),
            offline: AtomicBool::new(false),
            fail_head: AtomicBool::new(false),
            corrupt_wal: AtomicBool::new(false),
        }
    }
    fn check(&self) -> Result<()> {
        anyhow::ensure!(!self.offline.load(Ordering::SeqCst), "injected outage");
        Ok(())
    }
}
impl RemoteStore for FaultStore {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.check()?;
        let mut b = self.inner.get(key)?;
        if key.starts_with("wal/") && self.corrupt_wal.load(Ordering::SeqCst) {
            b[24] ^= 1;
        }
        Ok(b)
    }
    fn put_immutable(&self, key: &str, b: &[u8]) -> Result<()> {
        self.check()?;
        self.inner.put_immutable(key, b)
    }
    fn head(&self) -> Result<Option<HeadObject>> {
        self.check()?;
        self.inner.head()
    }
    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        self.check()?;
        anyhow::ensure!(
            !self.fail_head.load(Ordering::SeqCst),
            "injected head failure"
        );
        self.inner.compare_and_swap_head(expected, bytes)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.check()?;
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.check()?;
        self.inner.delete(key)
    }
}

#[test]
fn filesystem_remote_and_database_roots_must_not_overlap() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().join("objects");
    let store = Arc::new(FileStore::new(&root).unwrap());
    assert!(Database::open_with_remote(&root, config(), Some(store.clone())).is_err());
    assert!(!root.join("wal").exists());
    assert!(
        Database::open_with_remote(root.join("database"), config(), Some(store.clone())).is_err()
    );
    assert!(Database::restore(root.join("restored"), config(), store).is_err());
    assert!(!root.join("restored").exists());
    let local = tmp.path().join("local");
    let store = Arc::new(FileStore::new(local.join("objects")).unwrap());
    assert!(Database::open_with_remote(&local, config(), Some(store)).is_err());
}

#[test]
fn asynchronous_wal_restore_and_publisher_fencing() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(FileStore::new(tmp.path().join("remote")).unwrap());
    let db = Database::open_with_remote(tmp.path().join("local"), config(), Some(store.clone()))
        .unwrap();
    db.create_table("metrics", table()).unwrap();
    let rows = vec![row(1, 1.0), row(2, 2.0)];
    db.write("metrics", "a", rows.clone(), 2).unwrap();
    assert_eq!(db.status().unwrap().unshipped_batches, 2);
    assert_eq!(db.ship().unwrap(), 2);
    assert_eq!(db.status().unwrap().hot_rows, 2);
    drop(db);
    let db = Database::open_with_remote(tmp.path().join("local"), config(), Some(store.clone()))
        .unwrap();
    assert_eq!(db.status().unwrap().unshipped_batches, 0);
    let restored = Database::restore(tmp.path().join("restored"), config(), store.clone()).unwrap();
    assert_eq!(
        restored
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(restored.rollups("metrics").unwrap()[0].count, 2);
    assert!(restored.write("metrics", "a", rows, 200).unwrap().duplicate);
    assert!(db.ship().is_err());
    assert!(db.status().unwrap().fenced.is_some());
    restored
        .write("metrics", "b", vec![row(3, 3.0)], 3)
        .unwrap();
    restored.ship().unwrap();
}

#[test]
fn archive_cache_cold_restore_and_remote_retention_gc() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(FileStore::new(tmp.path().join("remote")).unwrap());
    let db = Database::open_with_remote(tmp.path().join("local"), config(), Some(store.clone()))
        .unwrap();
    let t = TableConfig {
        archive_after_us: Some(10),
        retention_us: Some(100),
        ..table()
    };
    db.create_table("metrics", t).unwrap();
    db.write("metrics", "a", vec![row(1, 1.0), row(2, 2.0)], 2)
        .unwrap();
    let report = db.maintain(20).unwrap();
    assert_eq!(report.evicted_files, 1);
    assert_eq!(
        fs::read_dir(tmp.path().join("local/segments"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
    assert!(db.status().unwrap().decoded_cache_bytes > 0);
    assert!(db.status().unwrap().disk_cache_bytes > 0);
    assert_eq!(
        db.query("SELECT sum(value) AS total FROM metrics").unwrap()[0]["total"],
        3.0
    );
    let old = store.list("segments").unwrap();
    assert_eq!(old.len(), 1);
    db.maintain(200).unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 0);
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 2);
    assert!(store.list("segments").unwrap().is_empty());
    let restored = Database::restore(tmp.path().join("restored"), config(), store).unwrap();
    assert_eq!(
        restored.query("SELECT count(*) AS n FROM metrics").unwrap()[0]["n"],
        0
    );
    assert_eq!(restored.rollups("metrics").unwrap()[0].count, 2);
}

#[test]
fn failed_upload_never_evicts_and_corrupt_restore_fails() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(FaultStore::new(&tmp.path().join("remote")));
    let db = Database::open_with_remote(tmp.path().join("local"), config(), Some(store.clone()))
        .unwrap();
    let t = TableConfig {
        archive_after_us: Some(1),
        ..table()
    };
    db.create_table("metrics", t).unwrap();
    db.write("metrics", "a", vec![row(1, 1.0)], 1).unwrap();
    store.offline.store(true, Ordering::SeqCst);
    assert!(db.maintain(20).is_err());
    assert_eq!(db.status().unwrap().segments, 1);
    assert_eq!(
        fs::read_dir(tmp.path().join("local/segments"))
            .unwrap()
            .count(),
        1
    );
    db.write("metrics", "b", vec![row(2, 2.0)], 2).unwrap();
    store.offline.store(false, Ordering::SeqCst);
    store.fail_head.store(true, Ordering::SeqCst);
    assert!(db.ship().is_err());
    assert!(store.head().unwrap().is_none());
    assert!(Database::restore(tmp.path().join("missing"), config(), store.clone()).is_err());
    store.fail_head.store(false, Ordering::SeqCst);
    db.ship().unwrap();
    store.corrupt_wal.store(true, Ordering::SeqCst);
    let error = Database::restore(tmp.path().join("bad"), config(), store.clone())
        .err()
        .unwrap();
    assert!(error.to_string().contains("integrity"), "{error:#}");
    assert!(!tmp.path().join("bad").exists());
    let head: serde_json::Value =
        serde_json::from_slice(&store.head().unwrap().unwrap().bytes).unwrap();
    assert!(head["lock"].is_null());
}

#[test]
fn deterministic_mixed_workload_matches_reference() {
    let tmp = TempDir::new().unwrap();
    let db = open(tmp.path());
    db.create_table(
        "metrics",
        TableConfig {
            shards: 4,
            ..table()
        },
    )
    .unwrap();
    let mut seed = 7u64;
    let mut expected = Vec::new();
    for batch in 0..40 {
        let mut rows = Vec::new();
        for _ in 0..5 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let ts = (seed % 199) as i64 - 99;
            let value = (seed % 100) as f64;
            rows.push(row(ts, value));
        }
        expected.extend(rows.clone());
        db.write("metrics", &format!("batch{batch}"), rows, 100)
            .unwrap();
        if batch % 7 == 0 {
            db.checkpoint().unwrap();
        }
        if batch % 13 == 0 {
            db.compact().unwrap();
        }
    }
    db.checkpoint().unwrap();
    drop(db);
    let db = open(tmp.path());
    let raw = db.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(raw.len(), expected.len());
    let sum: f64 = expected.iter().map(|r| r.value).sum();
    assert_eq!(raw.iter().map(|r| r.row.value).sum::<f64>(), sum);
    assert_eq!(
        db.rollups("metrics")
            .unwrap()
            .iter()
            .map(|r| r.count)
            .sum::<u64>(),
        200
    );
    assert_eq!(
        db.rollups("metrics")
            .unwrap()
            .iter()
            .map(|r| r.sum)
            .sum::<f64>(),
        sum
    );
    let query = db
        .query("SELECT count(*) AS n, sum(value) AS total FROM metrics")
        .unwrap();
    assert_eq!(query[0]["n"], 200);
    assert_eq!(query[0]["total"], sum);
}

#[test]
fn crash_worker() {
    let Ok(root) = std::env::var("VARVE_CRASH_ROOT") else {
        return;
    };
    let remote = std::env::var("VARVE_CRASH_REMOTE")
        .ok()
        .map(|p| Arc::new(FileStore::new(p).unwrap()) as Arc<dyn RemoteStore>);
    let db = Database::open_with_remote(&root, config(), remote).unwrap();
    db.write("metrics", "crash_batch", vec![row(1, 42.0)], 1)
        .unwrap();
    if std::env::var("VARVE_CRASH_ACTION").as_deref() == Ok("ship") {
        db.ship().unwrap();
    } else {
        db.checkpoint().unwrap();
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn crash_matrix_local_publication_boundaries() {
    for point in [
        "wal_synced",
        "wal_published",
        "segment_written",
        "segments_published",
        "manifest_published",
    ] {
        let tmp = TempDir::new().unwrap();
        let db = open(tmp.path());
        db.create_table("metrics", table()).unwrap();
        drop(db);
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_worker", "--nocapture"])
            .env("VARVE_CRASH_ROOT", tmp.path())
            .env("VARVE_FAILPOINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let db = open(tmp.path());
        let count = db.scan("metrics", None, None, None, None).unwrap().len();
        let expected = usize::from(point != "wal_synced");
        assert_eq!(count, expected, "{point}");
        assert_eq!(
            db.rollups("metrics")
                .unwrap()
                .iter()
                .map(|r| r.count)
                .sum::<u64>(),
            expected as u64,
            "{point}"
        );
        if expected == 1 {
            assert!(
                db.write("metrics", "crash_batch", vec![row(1, 42.0)], 1)
                    .unwrap()
                    .duplicate
            );
        }
        db.checkpoint().unwrap();
        drop(db);
        assert_eq!(
            open(tmp.path())
                .scan("metrics", None, None, None, None)
                .unwrap()
                .len(),
            expected
        );
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn crash_matrix_remote_publication_boundaries() {
    for point in ["remote_objects_uploaded", "remote_head_published"] {
        let tmp = TempDir::new().unwrap();
        let local = tmp.path().join("local");
        let remote = tmp.path().join("remote");
        let db = open(&local);
        db.create_table("metrics", table()).unwrap();
        drop(db);
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_worker", "--nocapture"])
            .env("VARVE_CRASH_ROOT", &local)
            .env("VARVE_CRASH_REMOTE", &remote)
            .env("VARVE_CRASH_ACTION", "ship")
            .env("VARVE_FAILPOINT", point)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(86), "{point}");
        assert_eq!(
            open(&local)
                .scan("metrics", None, None, None, None)
                .unwrap()
                .len(),
            1
        );
        let restored = Database::restore(
            tmp.path().join("restored"),
            config(),
            Arc::new(FileStore::new(&remote).unwrap()),
        );
        if point == "remote_head_published" {
            assert_eq!(
                restored
                    .unwrap()
                    .scan("metrics", None, None, None, None)
                    .unwrap()
                    .len(),
                1
            );
        } else {
            assert!(restored.is_err());
        }
    }
}
