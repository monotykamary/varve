use super::*;
use crate::model::{JobAlter, JobKind, LifecyclePolicy};
use tempfile::TempDir;

fn config(journal: bool, pages: bool) -> Config {
    Config {
        segmented_journal: journal,
        derived_pages: pages,
        derived_page_bytes: 4096,
        segment_rows: 256,
        ..Config::default()
    }
}

fn table(widths: Vec<i64>) -> TableConfig {
    TableConfig { shards: 1, window_us: 1000, rollup_widths_us: widths, ..TableConfig::default() }
}

fn rows(start: usize, count: usize) -> Vec<Row> {
    (start..start + count).map(|i| Row {
        timestamp_us: 1,
        tenant: "tenant".into(),
        series: format!("series_{i:03}"),
        value: i as f64 / 4.0,
        tags: BTreeMap::new(),
    }).collect()
}

// 128 groups / 37 receipts approximates the live 12076 / 3504 ratio.
// Every row is deterministic, tiny, native-independent and tempfile-owned.
fn seeded(journal: bool, pages: bool) -> (TempDir, Config) {
    let dir = TempDir::new().unwrap();
    let mut config = config(journal, pages);
    let db = Database::open(dir.path(), config.clone()).unwrap();
    db.create_table("old", table(vec![10])).unwrap();
    let mut start = 0;
    for batch in 0..37 {
        let count = if batch < 20 { 3 } else { 4 };
        db.write("old", &format!("ack_{batch}"), rows(start, count), 10).unwrap();
        start += count;
    }
    assert_eq!(start, 128);
    db.checkpoint().unwrap();
    let s = db.lock().unwrap();
    let resident = s.derived_resident_bytes;
    let maps = derived_root::resident_bytes(&s.catalog, false);
    config.derived_max_bytes = resident * 296 / 100;
    // On the unchanged implementation, preflight fits but the index admission
    // after WAL does not. Assert the algebra rather than searching for a quota.
    let speculative = ((config.derived_max_bytes - resident) / 8) * 4;
    assert!(resident + maps + speculative + config.derived_page_bytes * 4 < config.derived_max_bytes);
    assert!(2 * resident + speculative > config.derived_max_bytes);
    assert!(2 * resident + config.derived_page_bytes * 4 < config.derived_max_bytes);
    drop(s);
    drop(db);
    (dir, config)
}

fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn collect(base: &Path, path: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect(base, &path, out);
            } else {
                out.insert(path.strip_prefix(base).unwrap().to_path_buf(), fs::read(path).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    collect(root, root, &mut out);
    out
}

fn logical(db: &Database) -> serde_json::Value {
    let s = db.lock().unwrap();
    serde_json::json!({
        "catalog": s.catalog,
        "sequence": s.sequence,
        "metadata": s.metadata_bytes,
        "resident": s.derived_resident_bytes,
        "wal": s.wal_bytes,
        "generation": s.generation,
        "control_epoch": s.control_epoch,
        "raw_stamps": s.raw_stamps,
        "runtime": s.job_runtime,
    })
}

fn drained(db: &Database) {
    let status = db.status().unwrap();
    assert_eq!(status.derived_working_bytes, 0);
    assert_eq!(status.raw_memory.working_bytes, 0);
    assert_eq!(status.raw_memory.pinned_bytes, 0);
    assert!(status.fenced.is_none(), "{:?}", status.fenced);
    assert!(db.is_ready());
}

fn old_acks(db: &Database) {
    let mut start = 0;
    for batch in 0..37 {
        let count = if batch < 20 { 3 } else { 4 };
        let receipt = db.write("old", &format!("ack_{batch}"), rows(start, count), 10).unwrap();
        assert!(receipt.duplicate);
        assert_eq!(receipt.rows, count);
        start += count;
    }
    let raw = db.scan("old", None, None, None, None).unwrap();
    assert_eq!(raw.len(), 128);
    for stored in &raw {
        let i: usize = stored.row.series.strip_prefix("series_").unwrap().parse().unwrap();
        assert_eq!(stored.row, rows(i, 1).remove(0));
        assert_eq!(stored.sequence, 2 + (if i < 60 { i / 3 } else { 20 + (i - 60) / 4 }) as u64);
    }
    let rollups = db.rollups("old").unwrap();
    assert_eq!(rollups.len(), 128);
    for rollup in rollups {
        let i: usize = rollup.series.strip_prefix("series_").unwrap().parse().unwrap();
        assert_eq!(rollup.count, 1);
        assert_eq!(rollup.sum.to_bits(), (i as f64 / 4.0).to_bits());
        assert_eq!(rollup.min.to_bits(), rollup.sum.to_bits());
        assert_eq!(rollup.max.to_bits(), rollup.sum.to_bits());
        assert_eq!(rollup.first.to_bits(), rollup.sum.to_bits());
        assert_eq!(rollup.last.to_bits(), rollup.sum.to_bits());
    }
}

#[test]
fn empty_new_table_aggregate_near_budget() {
    for journal in [false, true] {
        for pages in [false, true] {
            let (dir, config) = seeded(journal, pages);
            let db = Database::open(dir.path(), config.clone()).unwrap();
            db.create_table("empty", table(vec![])).unwrap();
            db.checkpoint().unwrap();
            let before = db.status().unwrap().sequence;
            let result = db.create_continuous_aggregate("empty_minute", "empty", 60);
            assert!(result.is_ok(), "empty aggregate must fit: {result:?}; status={:?}", db.status());
            assert_eq!(result.unwrap(), before + 1);
            drained(&db);
            let catalog = serde_json::to_value(&db.lock().unwrap().catalog).unwrap();
            drop(db);
            let db = Database::open(dir.path(), config).unwrap();
            assert_eq!(serde_json::to_value(&db.lock().unwrap().catalog).unwrap(), catalog);
            old_acks(&db);
            drained(&db);
        }
    }
}

#[test]
fn all_public_control_families_tight_budget_reopen_exact() {
    for journal in [false, true] {
        let (dir, config) = seeded(journal, true);
        let db = Database::open(dir.path(), config.clone()).unwrap();
        let created = db.create_table("empty", table(vec![])).unwrap();
        assert_eq!(db.create_table("empty", table(vec![])).unwrap(), created);
        db.set_policy("empty", LifecyclePolicy { retention_us: Some(100), ..Default::default() }).unwrap();
        db.create_continuous_aggregate("agg", "empty", 60).unwrap();
        db.create_continuous_aggregate("shared", "empty", 60).unwrap();
        db.drop_continuous_aggregate("agg").unwrap();
        db.drop_continuous_aggregate("shared").unwrap();
        db.create_job("job", JobKind::Checkpoint, 100).unwrap();
        db.alter_job("job", JobAlter { interval_us: Some(50), paused: None }).unwrap();
        db.pause_job("job").unwrap();
        db.resume_job("job").unwrap();
        db.create_job("dropme", JobKind::Checkpoint, 100).unwrap();
        db.drop_job("dropme").unwrap();
        let before = logical(&db);
        let disk = files(dir.path());
        assert!(db.create_job("job", JobKind::Checkpoint, 100).is_err());
        assert!(db.create_table("empty", table(vec![3])).is_err());
        assert_eq!(logical(&db), before);
        assert_eq!(files(dir.path()), disk);
        drained(&db);
        let catalog = serde_json::to_value(&db.lock().unwrap().catalog).unwrap();
        let jobs = serde_json::to_value(db.jobs().unwrap()).unwrap();
        drop(db);
        let db = Database::open(dir.path(), config).unwrap();
        assert_eq!(serde_json::to_value(&db.lock().unwrap().catalog).unwrap(), catalog);
        assert_eq!(serde_json::to_value(db.jobs().unwrap()).unwrap(), jobs);
        old_acks(&db);
        drained(&db);
    }
}

#[test]
fn index_admission_rejection_is_before_wal_for_every_family() {
    for journal in [false, true] {
        let (dir, config) = seeded(journal, true);
        let db = Database::open(dir.path(), config.clone()).unwrap();
        db.create_table("empty", table(vec![])).unwrap();
        db.create_continuous_aggregate("agg", "empty", 60).unwrap();
        db.create_job("job", JobKind::Checkpoint, 100).unwrap();
        db.checkpoint().unwrap();
        let before = logical(&db);
        let disk = files(dir.path());
        for family in 0..9 {
            let lease = {
                let s = db.lock().unwrap();
                let bytes = config.derived_max_bytes - 2 * s.derived_resident_bytes + 1;
                reserve_derived(&s, &config, bytes).unwrap()
            };
            let result = match family {
                0 => db.create_table("newtable", table(vec![])),
                1 => db.set_policy("empty", LifecyclePolicy::default()),
                2 => db.create_continuous_aggregate("other", "empty", 30),
                3 => db.drop_continuous_aggregate("agg"),
                4 => db.create_job("otherjob", JobKind::Checkpoint, 10),
                5 => db.alter_job("job", JobAlter { interval_us: Some(5), paused: None }),
                6 => db.pause_job("job"),
                7 => db.resume_job("job"),
                _ => db.drop_job("job"),
            };
            let error = result.unwrap_err().to_string();
            assert!(error.contains("derived resident/working byte budget exceeded"), "family={family}: {error}");
            assert_eq!(db.status().unwrap().derived_working_bytes, lease.bytes);
            drop(lease);
            drained(&db);
            assert_eq!(logical(&db), before, "family={family}");
            assert_eq!(files(dir.path()), disk, "family={family}");
        }
        drop(db);
        let db = Database::open(dir.path(), config).unwrap();
        old_acks(&db);
        drained(&db);
    }
}

#[test]
fn growing_backfill_group_rejection_preserves_hot_wal_and_files() {
    let dir = TempDir::new().unwrap();
    let mut config = config(false, true);
    config.max_rollup_groups = 1;
    let db = Database::open(dir.path(), config.clone()).unwrap();
    db.create_table("source", table(vec![])).unwrap();
    db.write("source", "old_ack", rows(0, 2), 10).unwrap();
    let before = logical(&db);
    let disk = files(dir.path());
    let error = db.create_continuous_aggregate("too_many", "source", 10).unwrap_err();
    assert!(error.to_string().contains("control metadata group limits exceeded"));
    assert_eq!(logical(&db), before);
    assert_eq!(files(dir.path()), disk);
    drained(&db);
    drop(db);
    let db = Database::open(dir.path(), config).unwrap();
    assert!(db.write("source", "old_ack", rows(0, 2), 10).unwrap().duplicate);
    assert!(db.continuous_aggregates().unwrap().is_empty());
    assert_eq!(db.scan("source", None, None, None, None).unwrap().len(), 2);
    drained(&db);
}

#[test]
fn control_wal_capacity_rejection_does_not_checkpoint_or_mutate() {
    let (dir, config) = seeded(false, true);
    let db = Database::open(dir.path(), config).unwrap();
    db.create_table("pending", table(vec![])).unwrap();
    let original = db.lock().unwrap().wal_bytes;
    // Deterministically exercise capacity without a large WAL fixture.
    db.lock().unwrap().wal_bytes = db.inner.config.wal_max_bytes;
    let before = logical(&db);
    let disk = files(dir.path());
    let error = db.create_job("over_capacity", JobKind::Checkpoint, 10).unwrap_err();
    assert!(error.to_string().contains("control WAL capacity exhausted"));
    assert_eq!(logical(&db), before);
    assert_eq!(files(dir.path()), disk);
    drained(&db);
    db.lock().unwrap().wal_bytes = original;
    db.checkpoint().unwrap();
    db.create_job("over_capacity", JobKind::Checkpoint, 10).unwrap();
    drained(&db);
}

#[test]
fn replay_still_rejects_forged_control_stamp() {
    let (dir, config) = seeded(false, true);
    let db = Database::open(dir.path(), config.clone()).unwrap();
    let sequence = db.status().unwrap().sequence + 1;
    drop(db);
    wal::append(dir.path(), &wal::Record::new(sequence, wal::Operation::SetPolicy {
        table: "old".into(),
        policy: LifecyclePolicy::default(),
        stamp: "0".repeat(64),
    })).unwrap();
    let disk = files(dir.path());
    let error = Database::open(dir.path(), config).err().expect("forged stamp opened");
    assert!(format!("{error:#}").contains("control WAL state digest mismatch"));
    assert_eq!(files(dir.path()), disk);
}

#[test]
fn nonempty_backfill_covers_hot_and_persisted_inputs_without_checkpoint() {
    for journal in [false, true] {
        let dir = TempDir::new().unwrap();
        let config = config(journal, true);
        let db = Database::open(dir.path(), config.clone()).unwrap();
        db.create_table("source", table(vec![])).unwrap();
        db.write("source", "cold", rows(0, 2), 10).unwrap();
        db.checkpoint().unwrap();
        db.write("source", "hot", rows(2, 2), 10).unwrap();
        let checkpoint = db.status().unwrap().checkpoint_sequence;
        db.create_continuous_aggregate("agg", "source", 10).unwrap();
        assert_eq!(db.status().unwrap().checkpoint_sequence, checkpoint);
        let expected = serde_json::to_value(db.rollups("source").unwrap()).unwrap();
        assert_eq!(expected.as_array().unwrap().len(), 4);
        drained(&db);
        drop(db);
        let db = Database::open(dir.path(), config).unwrap();
        assert_eq!(serde_json::to_value(db.rollups("source").unwrap()).unwrap(), expected);
        assert_eq!(db.scan("source", None, None, None, None).unwrap().len(), 4);
        drained(&db);
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn durable_control_interruption_fences_and_recovers_old_acks() {
    for journal in [false, true] {
        let (dir, config) = seeded(journal, true);
        let db = Database::open(dir.path(), config.clone()).unwrap();
        let before = db.status().unwrap().sequence;
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::EpochBeforeInstall);
        hook.release_with_error();
        db.set_maintenance_test_hook(Some(hook)).unwrap();
        assert!(db.create_table("durable", table(vec![])).is_err());
        let status = db.status().unwrap();
        assert!(status.fenced.is_some());
        assert_eq!(status.sequence, before);
        assert_eq!(status.derived_working_bytes, 0);
        assert!(!db.is_ready());
        let disk = files(dir.path());
        assert!(db.create_table("forbidden", table(vec![])).is_err());
        assert_eq!(files(dir.path()), disk);
        drop(db);
        let db = Database::open(dir.path(), config).unwrap();
        assert_eq!(db.status().unwrap().sequence, before + 1);
        assert!(db.lock().unwrap().catalog.tables.contains_key("durable"));
        old_acks(&db);
        drained(&db);
    }
}

#[test]
fn recovery_rejects_insufficient_derived_budget_without_changing_files() {
    let (dir, mut config) = seeded(false, true);
    let db = Database::open(dir.path(), config.clone()).unwrap();
    config.derived_max_bytes = db.status().unwrap().derived_resident_bytes - 1;
    drop(db);
    let before = files(dir.path());
    assert!(Database::open(dir.path(), config).is_err());
    assert_eq!(files(dir.path()), before);
}

#[cfg(feature = "fault-injection")]
#[test]
fn backfill_conflict_drops_pins_and_credit_without_publication() {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path(), config(false, true)).unwrap();
    db.create_table("source", table(vec![])).unwrap();
    db.write("source", "before", rows(0, 2), 10).unwrap();
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::ControlBackfillCaptured);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let writer = db.clone();
    let thread = std::thread::spawn(move || writer.create_continuous_aggregate("agg", "source", 10));
    let blocked = hook.wait_until_blocked(std::time::Duration::from_secs(10));
    if !blocked {
        hook.release();
        panic!("backfill did not reach capture barrier");
    }
    db.write("source", "concurrent", rows(2, 1), 10).unwrap();
    let before = logical(&db);
    let disk = files(dir.path());
    hook.release();
    let error = thread.join().unwrap().unwrap_err().to_string();
    assert!(error.contains("changed during aggregate backfill"), "{error}");
    assert_eq!(logical(&db), before);
    assert_eq!(files(dir.path()), disk);
    drained(&db);
    db.set_maintenance_test_hook(None).unwrap();
    db.create_continuous_aggregate("agg", "source", 10).unwrap();
    assert_eq!(db.rollups("source").unwrap().len(), 3);
    drained(&db);
}

#[cfg(feature = "fault-injection")]
#[test]
fn ambiguous_control_wal_child() {
    let Some(path) = std::env::var_os("VARVE_CONTROL_FAULT_CHILD") else { return; };
    let limit: usize = std::env::var("VARVE_CONTROL_LIMIT").unwrap().parse().unwrap();
    let mut config = config(false, true);
    config.derived_max_bytes = limit;
    let db = Database::open(path, config).unwrap();
    let before = db.status().unwrap().sequence;
    assert!(db.create_table("ambiguous", table(vec![])).is_err());
    let status = db.status().unwrap();
    assert_eq!(status.sequence, before);
    assert!(status.fenced.is_some());
    assert_eq!(status.derived_working_bytes, 0);
    assert!(!db.is_ready());
    assert!(db.create_table("forbidden", table(vec![])).is_err());
}

#[cfg(feature = "fault-injection")]
#[test]
fn ambiguous_control_wal_preserves_every_old_ack() {
    let (dir, config) = seeded(false, true);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "engine::control_admission_tests::ambiguous_control_wal_child", "--nocapture"])
        .env("VARVE_CONTROL_FAULT_CHILD", dir.path())
        .env("VARVE_CONTROL_LIMIT", config.derived_max_bytes.to_string())
        .env("VARVE_IO_FAILPOINT", "wal_before_dir_sync")
        .status().unwrap();
    assert!(status.success());
    let db = Database::open(dir.path(), config).unwrap();
    assert!(db.lock().unwrap().catalog.tables.contains_key("ambiguous"));
    assert!(!db.lock().unwrap().catalog.tables.contains_key("forbidden"));
    old_acks(&db);
    drained(&db);
}

#[cfg(feature = "fault-injection")]
#[test]
fn prepared_control_keeps_all_simultaneous_owner_credit() {
    let (dir, config) = seeded(false, true);
    let db = Database::open(dir.path(), config.clone()).unwrap();
    let (counter, resident) = {
        let s = db.lock().unwrap();
        (Arc::clone(&s.derived_working), s.derived_resident_bytes)
    };
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::EpochBeforeInstall);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let writer = db.clone();
    let thread = std::thread::spawn(move || writer.create_table("prepared", table(vec![])));
    let blocked = hook.wait_until_blocked(std::time::Duration::from_secs(10));
    let working = counter.load(Ordering::SeqCst);
    hook.release();
    thread.join().unwrap().unwrap();
    assert!(blocked);
    // One private map set + one private index set, plus old committed owners.
    assert_eq!(working, resident);
    assert!(resident + working <= config.derived_max_bytes);
    drained(&db);
}
