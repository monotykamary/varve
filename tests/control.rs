use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::TempDir;
use varve::remote::{FileStore, HeadObject, RemoteStore};
use varve::{Config, Database, JobKind, LifecyclePolicy, Row, TableConfig};

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
        flush_interval_us: 1,
        maintenance_interval_ms: 1_000,
        segment_rows: 8,
        ..Default::default()
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

#[test]
fn sql_controls_backfill_retained_raw_and_survive_restart() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    assert!(db.is_ready());
    let table_json = serde_json::to_string(&table()).unwrap().replace('\'', "''");
    assert_eq!(
        db.query(&format!(
            "CALL varve_create_table('metrics', '{table_json}')"
        ))
        .unwrap()["sequence"],
        1
    );
    db.write("metrics", "initial", vec![row(1, 1.0), row(11, 2.0)], 12)
        .unwrap();
    db.checkpoint().unwrap();
    db.query("CALL varve_create_continuous_aggregate('metrics_five', 'metrics', 5)")
        .unwrap();
    assert_eq!(
        db.rollups("metrics")
            .unwrap()
            .iter()
            .filter(|r| r.width_us == 5)
            .map(|r| r.count)
            .sum::<u64>(),
        2
    );
    db.write("metrics", "late", vec![row(3, 4.0)], 12).unwrap();
    assert_eq!(
        db.rollups("metrics")
            .unwrap()
            .iter()
            .filter(|r| r.width_us == 5)
            .map(|r| r.count)
            .sum::<u64>(),
        3
    );
    db.create_continuous_aggregate("metrics_five_shared", "metrics", 5)
        .unwrap();
    db.drop_continuous_aggregate("metrics_five").unwrap();
    assert!(
        db.rollups("metrics")
            .unwrap()
            .iter()
            .any(|r| r.width_us == 5)
    );
    db.query("CALL varve_set_policy('metrics', '{\"retention_us\":5}')")
        .unwrap();
    db.maintain(20).unwrap();
    assert!(
        db.scan("metrics", None, None, None, None)
            .unwrap()
            .is_empty()
    );
    db.create_continuous_aggregate("metrics_seven", "metrics", 7)
        .unwrap();
    assert!(
        !db.rollups("metrics")
            .unwrap()
            .iter()
            .any(|r| r.width_us == 7)
    );
    db.write("metrics", "future", vec![row(19, 9.0)], 20)
        .unwrap();
    assert_eq!(
        db.rollups("metrics")
            .unwrap()
            .iter()
            .filter(|r| r.width_us == 7)
            .map(|r| r.count)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        db.query("SELECT * FROM varve_continuous_aggregates()")
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let named = db
        .query("SELECT count, sum, average, open, high, low, close FROM metrics_seven")
        .unwrap();
    let count = named[0]["count"]
        .as_u64()
        .or_else(|| named[0]["count"].as_str()?.parse().ok());
    assert_eq!(count, Some(1));
    assert_eq!(named[0]["sum"], 9.0);
    let before = db.status().unwrap().sequence;
    assert!(
        db.query("CALL varve_set_policy('metrics', '{}'); SELECT 1")
            .is_err()
    );
    assert!(
        db.query("CALL varve_set_policy('metrics', lower('{}'))")
            .is_err()
    );
    assert_eq!(db.status().unwrap().sequence, before);
    drop(db);
    let db = Database::open(temp.path(), config()).unwrap();
    assert_eq!(db.policy("metrics").unwrap().retention_us, Some(5));
    assert_eq!(db.continuous_aggregates().unwrap().len(), 2);
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
}

struct BlockingStore {
    inner: FileStore,
    block_next: std::sync::atomic::AtomicBool,
    state: std::sync::Mutex<(bool, bool)>,
    wake: std::sync::Condvar,
}
impl BlockingStore {
    fn new(path: impl AsRef<std::path::Path>) -> Self {
        Self {
            inner: FileStore::new(path).unwrap(),
            block_next: std::sync::atomic::AtomicBool::new(true),
            state: std::sync::Mutex::new((false, false)),
            wake: std::sync::Condvar::new(),
        }
    }
    fn wait_until_blocked(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.0 {
            state = self.wake.wait(state).unwrap();
        }
    }
    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        self.wake.notify_all();
    }
}
impl RemoteStore for BlockingStore {
    fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        self.inner.get(key)
    }
    fn get_bounded(&self, key: &str, max: usize) -> anyhow::Result<Vec<u8>> {
        self.inner.get_bounded(key, max)
    }
    fn put_immutable(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        if self
            .block_next
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            let mut state = self.state.lock().unwrap();
            state.0 = true;
            self.wake.notify_all();
            while !state.1 {
                state = self.wake.wait(state).unwrap();
            }
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
fn control_mutation_during_ship_publishes_a_safe_prefix() {
    let temp = TempDir::new().unwrap();
    let remote = Arc::new(BlockingStore::new(temp.path().join("remote")));
    let db = Database::open_with_remote(temp.path().join("local"), config(), Some(remote.clone()))
        .unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "batch", vec![row(1, 1.0)], 1).unwrap();
    db.checkpoint().unwrap();
    let shipping = {
        let db = db.clone();
        std::thread::spawn(move || db.ship().unwrap())
    };
    remote.wait_until_blocked();
    let policy_sequence = db
        .set_policy(
            "metrics",
            LifecyclePolicy {
                retention_us: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
    remote.release();
    let prefix = shipping.join().unwrap();
    assert!(prefix < policy_sequence);
    assert_eq!(db.ship().unwrap(), policy_sequence);
    drop(db);
    let restored =
        Database::restore(temp.path().join("restored_control_ship"), config(), remote).unwrap();
    assert_eq!(restored.policy("metrics").unwrap().retention_us, Some(100));
}

#[test]
fn control_state_survives_remote_restore() {
    let temp = TempDir::new().unwrap();
    let remote = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let db = Database::open_with_remote(temp.path().join("local"), config(), Some(remote.clone()))
        .unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "batch", vec![row(1, 2.0)], 1).unwrap();
    db.set_policy(
        "metrics",
        LifecyclePolicy {
            retention_us: Some(100),
            ..Default::default()
        },
    )
    .unwrap();
    db.create_continuous_aggregate("metrics_five", "metrics", 5)
        .unwrap();
    db.create_job("checkpoint_job", JobKind::Checkpoint, 100)
        .unwrap();
    db.pause_job("checkpoint_job").unwrap();
    db.checkpoint().unwrap();
    let shipped = db.ship().unwrap();
    assert_eq!(shipped, db.status().unwrap().sequence);
    drop(db);

    let restored = Database::restore(temp.path().join("restored"), config(), remote).unwrap();
    assert_eq!(restored.policy("metrics").unwrap().retention_us, Some(100));
    assert_eq!(
        restored.continuous_aggregates().unwrap()[0].name,
        "metrics_five"
    );
    assert!(
        restored
            .jobs()
            .unwrap()
            .iter()
            .any(|job| { job.name == "checkpoint_job" && job.paused })
    );
    assert_eq!(
        restored
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        1
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn control_ship_crash_rebinds_only_a_proven_prefix() {
    if let (Ok(local), Ok(remote_path)) = (
        std::env::var("VARVE_CONTROL_CRASH_LOCAL"),
        std::env::var("VARVE_CONTROL_CRASH_REMOTE"),
    ) {
        let remote = Arc::new(FileStore::new(remote_path).unwrap());
        let db = Database::open_with_remote(local, config(), Some(remote)).unwrap();
        db.ship().unwrap();
        return;
    }

    let temp = TempDir::new().unwrap();
    let local = temp.path().join("local_crash");
    let remote_path = temp.path().join("remote_crash");
    let remote = Arc::new(FileStore::new(&remote_path).unwrap());
    let db = Database::open_with_remote(&local, config(), Some(remote.clone())).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.ship().unwrap();
    db.set_policy(
        "metrics",
        LifecyclePolicy {
            retention_us: Some(100),
            ..Default::default()
        },
    )
    .unwrap();
    drop(db);

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "control_ship_crash_rebinds_only_a_proven_prefix",
            "--nocapture",
        ])
        .env("VARVE_CONTROL_CRASH_LOCAL", &local)
        .env("VARVE_CONTROL_CRASH_REMOTE", &remote_path)
        .env("VARVE_FAILPOINT", "remote_head_published")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(86));

    let db = Database::open_with_remote(&local, config(), Some(remote.clone())).unwrap();
    assert_eq!(db.ship().unwrap(), db.status().unwrap().sequence);
    drop(db);
    let restored = Database::restore(temp.path().join("restored_crash"), config(), remote).unwrap();
    assert_eq!(restored.policy("metrics").unwrap().retention_us, Some(100));
}

#[test]
fn current_v01_manifest_migrates_only_documented_missing_fields() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.checkpoint().unwrap();
    drop(db);

    let path = temp.path().join("manifest.bin");
    let bytes = std::fs::read(&path).unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&bytes[8..bytes.len() - 32]).unwrap();
    let object = json.as_object_mut().unwrap();
    object.remove("continuous_aggregates");
    object.remove("jobs");
    object.remove("control_history");
    object["tables"]["metrics"]
        .as_object_mut()
        .unwrap()
        .remove("creation_config");
    let mut migrated = b"VARVEM01".to_vec();
    migrated.extend(serde_json::to_vec(&json).unwrap());
    migrated.extend_from_slice(blake3::hash(&migrated).as_bytes());
    std::fs::write(&path, migrated).unwrap();

    let db = Database::open(temp.path(), config()).unwrap();
    assert_eq!(db.create_table("metrics", table()).unwrap(), 1);
    assert!(
        db.jobs()
            .unwrap()
            .iter()
            .any(|job| job.name == "varve_maintenance")
    );
    db.checkpoint().unwrap();
    drop(db);

    let bytes = std::fs::read(&path).unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&bytes[8..bytes.len() - 32]).unwrap();
    json.as_object_mut()
        .unwrap()
        .insert("future_field".into(), serde_json::json!(true));
    let mut future = b"VARVEM01".to_vec();
    future.extend(serde_json::to_vec(&json).unwrap());
    future.extend_from_slice(blake3::hash(&future).as_bytes());
    std::fs::write(&path, future).unwrap();
    assert!(Database::open(temp.path(), config()).is_err());
}

#[test]
fn immutable_partitioning_and_width_admission_are_atomic() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.set_policy(
        "metrics",
        LifecyclePolicy {
            late_after_us: Some(50),
            retention_us: Some(100),
            archive_after_us: None,
            rollup_retention_us: Some(1_000),
            idempotency_window_us: None,
        },
    )
    .unwrap();
    assert_eq!(db.create_table("metrics", table()).unwrap(), 1);
    for width in 1..=16 {
        if width != 10 {
            db.create_continuous_aggregate(&format!("width_{width}"), "metrics", width)
                .unwrap();
        }
    }
    let before = db.status().unwrap().sequence;
    assert!(
        db.create_continuous_aggregate("width_seventeen", "metrics", 17)
            .is_err()
    );
    assert_eq!(db.status().unwrap().sequence, before);
}
