use anyhow::Result;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;
use tempfile::TempDir;
use varve::remote::{FileStore, HeadObject, RemoteStore};
use varve::{Config, Database, Row, TableConfig};

fn config() -> Config {
    Config {
        flush_interval_us: 5_000_000,
        ship_interval_us: 10_000_000,
        segment_rows: 4,
        compact_min_segments: 2,
        ..Default::default()
    }
}

fn table() -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 100,
        ..Default::default()
    }
}

fn row(timestamp_us: i64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value: timestamp_us as f64,
        tags: BTreeMap::new(),
    }
}

#[test]
fn early_tick_preserves_hot_rows_and_due_tick_checkpoints() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.checkpoint().unwrap();
    db.write("metrics", "first", vec![row(1)], 100).unwrap();
    let before = db.status().unwrap();
    let early = db.maintain(1_000_100).unwrap();
    let after = db.status().unwrap();
    assert!(!early.flushed);
    assert_eq!(after.hot_rows, before.hot_rows);
    assert_eq!(after.checkpoint_sequence, before.checkpoint_sequence);
    assert_eq!(after.sequence, before.sequence);
    assert_eq!(after.wal_bytes, before.wal_bytes);
    assert!(db.maintain(5_000_100).unwrap().flushed);
    let due = db.status().unwrap();
    assert_eq!(due.hot_rows, 0);
    assert_eq!(due.checkpoint_sequence, before.sequence);
    drop(db);
    let db = Database::open(temp.path(), config()).unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
}

#[test]
fn retention_checkpoints_before_the_flush_deadline() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            retention_us: Some(10),
            ..table()
        },
    )
    .unwrap();
    db.write("metrics", "first", vec![row(1), row(15)], 15)
        .unwrap();
    let report = db.maintain(20).unwrap();
    assert!(report.flushed);
    assert_eq!(report.expired_rows, 1);
    let status = db.status().unwrap();
    assert_eq!(status.checkpoint_sequence, status.sequence);
    assert_eq!(status.hot_rows, 0);
    drop(db);
    let db = Database::open(temp.path(), config()).unwrap();
    let rows = db.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row.timestamp_us, 15);
}

struct CountedStore {
    inner: FileStore,
    parquet_gets: AtomicUsize,
    block_get: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
}

impl CountedStore {
    fn new(path: &Path) -> Arc<Self> {
        Arc::new(Self {
            inner: FileStore::new(path).unwrap(),
            parquet_gets: AtomicUsize::new(0),
            block_get: Mutex::new(None),
        })
    }

    fn block_next_get(&self) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        *self.block_get.lock().unwrap() = Some((started_tx, release_rx));
        (started_rx, release_tx)
    }
}

impl RemoteStore for CountedStore {
    fn local_root(&self) -> Option<&Path> {
        self.inner.local_root()
    }
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.get_bounded(key, usize::MAX)
    }
    fn get_bounded(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>> {
        if key.starts_with("segments/") && key.ends_with(".parquet") {
            self.parquet_gets.fetch_add(1, Ordering::SeqCst);
            let blocked = self.block_get.lock().unwrap().take();
            if let Some((started, release)) = blocked {
                started.send(())?;
                release.recv_timeout(Duration::from_secs(10))?;
            }
        }
        self.inner.get_bounded(key, max_bytes)
    }
    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
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

fn cold_database(
    temp: &TempDir,
    store: &Arc<CountedStore>,
    config: Config,
    table: TableConfig,
    batches: &[Vec<Row>],
) -> Database {
    let db = Database::open_with_remote(
        temp.path().join("source"),
        config.clone(),
        Some(store.clone()),
    )
    .unwrap();
    db.create_table("metrics", table).unwrap();
    for (i, batch) in batches.iter().enumerate() {
        db.write("metrics", &format!("batch-{i}"), batch.clone(), 10)
            .unwrap();
        db.checkpoint().unwrap();
    }
    db.ship().unwrap();
    drop(db);
    Database::restore(temp.path().join("restored"), config, store.clone()).unwrap()
}

#[test]
fn early_dirty_tick_does_not_prefetch_an_otherwise_eligible_group() {
    let temp = TempDir::new().unwrap();
    let store = CountedStore::new(&temp.path().join("remote"));
    let db = Database::open_with_remote(
        temp.path().join("local"),
        Config {
            compact_min_segments: 3,
            ..config()
        },
        Some(store.clone()),
    )
    .unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            archive_after_us: Some(1),
            ..table()
        },
    )
    .unwrap();
    for i in 1..=2 {
        db.write("metrics", &format!("batch-{i}"), vec![row(i)], 10)
            .unwrap();
        db.checkpoint().unwrap();
    }
    assert_eq!(db.maintain(20).unwrap().evicted_files, 2);
    db.write("metrics", "third", vec![row(3)], 21).unwrap();
    db.checkpoint().unwrap();
    // Three segments now form an eligible group, but the hot write prevents
    // compaction on this early tick. Shipping is not yet due either.
    db.write("metrics", "hot", vec![row(100)], 21).unwrap();
    let before = db.status().unwrap();
    let report = db.maintain(22).unwrap();
    assert!(!report.flushed);
    assert_eq!(report.compacted, 0);
    assert_eq!(store.parquet_gets.load(Ordering::SeqCst), 0);
    let after = db.status().unwrap();
    assert_eq!(after.hot_rows, 1);
    assert_eq!(after.checkpoint_sequence, before.checkpoint_sequence);
}

#[test]
fn cold_compaction_fetches_only_the_selected_prefix() {
    let temp = TempDir::new().unwrap();
    let store = CountedStore::new(&temp.path().join("remote"));
    let db = cold_database(
        &temp,
        &store,
        Config {
            hot_max_rows: 3,
            ..config()
        },
        table(),
        &[vec![row(1)], vec![row(2)], vec![row(3), row(4), row(5)]],
    );
    assert_eq!(store.parquet_gets.load(Ordering::SeqCst), 0);
    assert_eq!(db.compact().unwrap(), 1);
    assert_eq!(store.parquet_gets.load(Ordering::SeqCst), 2);
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 5);
}

#[test]
fn prefetch_rechecks_the_hot_clock_and_retention_after_concurrent_writes() {
    for retention in [false, true] {
        let temp = TempDir::new().unwrap();
        let store = CountedStore::new(&temp.path().join("remote"));
        let db = cold_database(
            &temp,
            &store,
            config(),
            TableConfig {
                retention_us: retention.then_some(10),
                ..table()
            },
            &[vec![row(1)], vec![row(9)]],
        );
        let now = if retention { 15 } else { 5_000_010 };
        // Without retention, the first lock flushes this due batch. The second lock
        // must not reuse that decision for the newer write arriving during prefetch.
        if !retention {
            db.write("metrics", "due", vec![row(100)], 10).unwrap();
        }
        let (started, release) = store.block_next_get();
        let worker = db.clone();
        let maintenance = std::thread::spawn(move || worker.maintain(now));
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let write = db.write("metrics", "concurrent", vec![row(200)], now + 1);
        release.send(()).unwrap();
        let report = maintenance.join().unwrap().unwrap();
        let receipt = write.unwrap();
        let status = db.status().unwrap();
        assert!(report.flushed);
        assert_eq!(status.sequence, receipt.sequence);
        if retention {
            assert_eq!(report.expired_rows, 1);
            assert_eq!(status.hot_rows, 0);
            assert_eq!(status.checkpoint_sequence, receipt.sequence);
        } else {
            assert_eq!(report.compacted, 0);
            assert_eq!(status.hot_rows, 1);
            assert_eq!(status.checkpoint_sequence, receipt.sequence - 1);
        }
        drop(db);
        let db =
            Database::open_with_remote(temp.path().join("restored"), config(), Some(store.clone()))
                .unwrap();
        let rows = db.scan("metrics", None, None, None, None).unwrap();
        let timestamps: Vec<_> = rows.iter().map(|r| r.row.timestamp_us).collect();
        assert_eq!(
            timestamps,
            if retention {
                vec![9, 200]
            } else {
                vec![1, 9, 100, 200]
            }
        );
    }
}

#[test]
fn no_op_cold_maintenance_does_not_get_parquet() {
    // Too few segments, no reduction, and a row-limited prefix below the minimum.
    for (sizes, hot_max_rows) in [(vec![1], 16), (vec![3, 3], 16), (vec![2, 2], 3)] {
        let temp = TempDir::new().unwrap();
        let store = CountedStore::new(&temp.path().join("remote"));
        let config = Config {
            hot_max_rows,
            ..config()
        };
        let db = Database::open_with_remote(temp.path().join("local"), config, Some(store.clone()))
            .unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                archive_after_us: Some(1),
                ..table()
            },
        )
        .unwrap();
        for (i, size) in sizes.iter().enumerate() {
            db.write(
                "metrics",
                &format!("batch-{i}"),
                (0..*size).map(|n| row(n as i64 + 1)).collect(),
                10,
            )
            .unwrap();
            db.checkpoint().unwrap();
        }
        assert_eq!(db.maintain(20).unwrap().evicted_files, sizes.len());
        store.parquet_gets.store(0, Ordering::SeqCst);
        let report = db.maintain(21).unwrap();
        assert_eq!(report.compacted, 0);
        assert_eq!(store.parquet_gets.load(Ordering::SeqCst), 0);
        assert_eq!(db.compact().unwrap(), 0);
        assert_eq!(store.parquet_gets.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn newly_eligible_cold_group_is_deferred_until_next_prefetch() {
    for explicit_compact in [false, true] {
        let temp = TempDir::new().unwrap();
        let store = CountedStore::new(&temp.path().join("remote"));
        let db =
            Database::open_with_remote(temp.path().join("local"), config(), Some(store.clone()))
                .unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                archive_after_us: Some(1),
                ..table()
            },
        )
        .unwrap();
        for (id, timestamp) in [("a", 1), ("b", 101)] {
            db.write("metrics", id, vec![row(timestamp)], 10).unwrap();
            db.checkpoint().unwrap();
        }
        // Establish last_ship and evict both ineligible groups. No shipping is
        // due during the regression, so every GET belongs to compaction.
        assert_eq!(db.maintain(200).unwrap().evicted_files, 2);
        db.write("metrics", "a-second", vec![row(2)], 201).unwrap();
        db.checkpoint().unwrap();
        let (started, release) = store.block_next_get();
        let worker = db.clone();
        let maintenance = std::thread::spawn(move || {
            if explicit_compact {
                worker.compact()
            } else {
                worker.maintain(201).map(|report| report.compacted)
            }
        });
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        // A is eligible and blocked in unlocked prefetch. B's only cold input
        // was not selected; this checkpoint makes B eligible on the second lock.
        db.write("metrics", "concurrent", vec![row(102)], 201)
            .unwrap();
        db.checkpoint().unwrap();
        release.send(()).unwrap();
        let compacted = maintenance.join().unwrap().unwrap();
        // A needs exactly one GET. Any second GET is the newly eligible B
        // falling through read_segment_locked while the state mutex is held.
        assert_eq!(store.parquet_gets.load(Ordering::SeqCst), 1);
        assert_eq!(compacted, 1);
        let status = db.status().unwrap();
        assert_eq!(status.checkpoint_sequence, status.sequence);
        assert_eq!(db.maintain(202).unwrap().compacted, 1);
        assert_eq!(store.parquet_gets.load(Ordering::SeqCst), 2);
        drop(db);
        let db =
            Database::open_with_remote(temp.path().join("local"), config(), Some(store.clone()))
                .unwrap();
        let timestamps: Vec<_> = db
            .scan("metrics", None, None, None, None)
            .unwrap()
            .iter()
            .map(|r| r.row.timestamp_us)
            .collect();
        assert_eq!(timestamps, vec![1, 2, 101, 102]);
    }
}

#[test]
fn retention_created_eligibility_defers_cold_but_compacts_local_inputs() {
    for local in [false, true] {
        let temp = TempDir::new().unwrap();
        let store = CountedStore::new(&temp.path().join("remote"));
        let db = cold_database(
            &temp,
            &store,
            config(),
            TableConfig {
                retention_us: Some(10),
                ..table()
            },
            &[
                vec![row(1), row(2), row(9)],
                vec![row(10), row(11), row(12)],
            ],
        );
        // Six rows cannot reduce two four-row output segments. Retention
        // shrinks the first input, making the remaining four rows compactable.
        if local {
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 6);
        }
        store.parquet_gets.store(0, Ordering::SeqCst);
        let report = db.maintain(15).unwrap();
        assert_eq!(report.expired_rows, 2);
        // Cold case: one retention prefetch plus one unlocked shipping GET.
        // The deferred input is fetched for compaction on the following tick.
        assert_eq!(
            store.parquet_gets.load(Ordering::SeqCst),
            if local { 0 } else { 2 }
        );
        assert_eq!(report.compacted, usize::from(local));
        assert_eq!(db.maintain(15).unwrap().compacted, usize::from(!local));
        assert_eq!(
            store.parquet_gets.load(Ordering::SeqCst),
            if local { 0 } else { 3 }
        );
        drop(db);
        let db =
            Database::open_with_remote(temp.path().join("restored"), config(), Some(store.clone()))
                .unwrap();
        let timestamps: Vec<_> = db
            .scan("metrics", None, None, None, None)
            .unwrap()
            .iter()
            .map(|r| r.row.timestamp_us)
            .collect();
        assert_eq!(timestamps, vec![9, 10, 11, 12]);
    }
}
