use std::collections::BTreeMap;
use tempfile::TempDir;
use varve::{Config, Database, JobAlter, JobKind, Row, TableConfig};

fn config() -> Config {
    Config {
        maintenance_interval_ms: 1_000,
        flush_interval_us: 1_000_000,
        ..Default::default()
    }
}
fn row() -> Row {
    Row {
        timestamp_us: 1,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value: 1.0,
        tags: BTreeMap::new(),
    }
}

#[test]
fn jobs_pause_retry_run_now_and_replay_latest_run() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    assert!(
        db.jobs()
            .unwrap()
            .iter()
            .any(|j| j.name == "varve_maintenance")
    );
    db.pause_job("varve_maintenance").unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write("metrics", "batch", vec![row()], 1).unwrap();
    db.create_job("flush_job", JobKind::Checkpoint, 10).unwrap();
    db.tick(9).unwrap();
    assert_eq!(db.status().unwrap().hot_rows, 1);
    db.tick(10).unwrap();
    assert_eq!(db.status().unwrap().hot_rows, 0);
    let flush = db
        .jobs()
        .unwrap()
        .into_iter()
        .find(|j| j.name == "flush_job")
        .unwrap();
    let run_id = flush.latest_run.unwrap().run_id;
    db.pause_job("flush_job").unwrap();
    db.tick(1_000).unwrap();
    assert_eq!(
        db.jobs()
            .unwrap()
            .into_iter()
            .find(|j| j.name == "flush_job")
            .unwrap()
            .latest_run
            .unwrap()
            .run_id,
        run_id
    );
    db.run_job_now("flush_job", 1_001).unwrap();
    assert!(
        db.jobs()
            .unwrap()
            .into_iter()
            .find(|j| j.name == "flush_job")
            .unwrap()
            .latest_run
            .unwrap()
            .run_id
            > run_id
    );
    db.create_job("ship_job", JobKind::Ship, 20).unwrap();
    assert!(db.tick(20).is_err());
    let failed = db
        .jobs()
        .unwrap()
        .into_iter()
        .find(|j| j.name == "ship_job")
        .unwrap();
    assert_eq!(failed.attempts, 1);
    assert_eq!(failed.latest_run.as_ref().unwrap().success, Some(false));
    let retry_at = failed.next_run_us.unwrap();
    assert!(db.tick(retry_at - 1).is_ok());
    assert!(db.tick(retry_at).is_err());
    assert_eq!(
        db.jobs()
            .unwrap()
            .into_iter()
            .find(|j| j.name == "ship_job")
            .unwrap()
            .attempts,
        2
    );
    db.alter_job(
        "ship_job",
        JobAlter {
            interval_us: Some(100),
            paused: Some(true),
        },
    )
    .unwrap();
    let metadata = db.query("SELECT kind, interval_us, paused, attempts, latest_success FROM varve_jobs() WHERE name = 'ship_job'").unwrap();
    assert_eq!(metadata[0]["kind"], "ship");
    assert_eq!(metadata[0]["latest_success"], false);
    drop(db);
    let db = Database::open(temp.path(), config()).unwrap();
    let ship = db
        .jobs()
        .unwrap()
        .into_iter()
        .find(|j| j.name == "ship_job")
        .unwrap();
    assert!(ship.paused);
    assert_eq!(ship.attempts, 2);
    db.resume_job("ship_job").unwrap();
    db.drop_job("ship_job").unwrap();
    assert!(db.drop_job("varve_maintenance").is_err());
}

#[test]
fn idle_ticks_do_not_advance_authoritative_sequences_or_publish_remote_objects() {
    fn files(root: &std::path::Path) -> usize {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                if path.is_dir() { files(&path) } else { 1 }
            })
            .sum()
    }

    let temp = TempDir::new().unwrap();
    let remote_root = temp.path().join("remote");
    let remote = std::sync::Arc::new(varve::remote::FileStore::new(&remote_root).unwrap());
    let local = temp.path().join("local");
    let db = Database::open_with_remote(&local, config(), Some(remote)).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.checkpoint().unwrap();
    db.ship().unwrap();
    let before = db.status().unwrap();
    let remote_files = files(&remote_root);

    db.tick(1_000_000).unwrap();
    db.tick(2_000_000).unwrap();
    let after = db.status().unwrap();
    assert_eq!(after.sequence, before.sequence);
    assert_eq!(after.checkpoint_sequence, before.checkpoint_sequence);
    assert_eq!(files(&remote_root), remote_files);
    let maintenance = db
        .jobs()
        .unwrap()
        .into_iter()
        .find(|job| job.name == "varve_maintenance")
        .unwrap();
    assert_eq!(maintenance.latest_run.unwrap().success, Some(true));
    drop(db);

    let reopened = Database::open(&local, config()).unwrap();
    let maintenance = reopened
        .jobs()
        .unwrap()
        .into_iter()
        .find(|job| job.name == "varve_maintenance")
        .unwrap();
    assert_eq!(maintenance.latest_run.unwrap().run_id, 2);
}

#[test]
fn dropped_job_runtime_is_not_applied_to_recreated_generation() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.create_job("generation_job", JobKind::Checkpoint, 10)
        .unwrap();
    db.run_job_now("generation_job", 1).unwrap();
    let first = db
        .jobs()
        .unwrap()
        .into_iter()
        .find(|job| job.name == "generation_job")
        .unwrap();
    assert!(first.latest_run.is_some());
    let first_generation = first.created_sequence;
    db.drop_job("generation_job").unwrap();
    db.create_job("generation_job", JobKind::Checkpoint, 20)
        .unwrap();
    let recreated = db
        .jobs()
        .unwrap()
        .into_iter()
        .find(|job| job.name == "generation_job")
        .unwrap();
    assert!(recreated.created_sequence > first_generation);
    assert!(recreated.latest_run.is_none());
    assert!(!recreated.running);
}

#[test]
fn corrupted_job_runtime_journal_fails_closed() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.run_job_now("varve_maintenance", 1).unwrap();
    drop(db);

    let path = temp.path().join("job-runtime.bin");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[12] ^= 0x40;
    std::fs::write(path, bytes).unwrap();
    assert!(Database::open(temp.path(), config()).is_err());
}

#[test]
fn remote_restore_omits_private_runtime_and_can_rerun_idempotent_job() {
    let temp = TempDir::new().unwrap();
    let remote =
        std::sync::Arc::new(varve::remote::FileStore::new(temp.path().join("remote")).unwrap());
    let local = temp.path().join("local");
    let db = Database::open_with_remote(&local, config(), Some(remote.clone())).unwrap();
    db.create_job("restored_job", JobKind::Checkpoint, 10)
        .unwrap();
    db.run_job_now("restored_job", 1).unwrap();
    assert!(
        db.jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.name == "restored_job")
            .unwrap()
            .latest_run
            .is_some()
    );
    db.checkpoint().unwrap();
    db.ship().unwrap();
    drop(db);

    let restored = Database::restore(temp.path().join("restored"), config(), remote).unwrap();
    let before = restored.status().unwrap().sequence;
    assert!(
        restored
            .jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.name == "restored_job")
            .unwrap()
            .latest_run
            .is_none()
    );
    restored.run_job_now("restored_job", 2).unwrap();
    assert_eq!(restored.status().unwrap().sequence, before);
    assert!(
        restored
            .jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.name == "restored_job")
            .unwrap()
            .latest_run
            .is_some()
    );
}

#[test]
fn sql_jobs_reject_expressions_without_partial_publication() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), config()).unwrap();
    db.query("CALL varve_create_job('manual_checkpoint', 'checkpoint', 100)")
        .unwrap();
    db.query("CALL varve_alter_job('manual_checkpoint', '{\"paused\":true}')")
        .unwrap();
    let before = db.status().unwrap().sequence;
    assert!(
        db.query("CALL varve_run_job(concat('manual', '_checkpoint'), 7)")
            .is_err()
    );
    assert!(
        db.query("CALL varve_create_job('bad', 'checkpoint', 0)")
            .is_err()
    );
    assert_eq!(db.status().unwrap().sequence, before);
    db.query("CALL varve_run_job('manual_checkpoint', 7)")
        .unwrap();
    assert_eq!(
        db.jobs()
            .unwrap()
            .into_iter()
            .find(|j| j.name == "manual_checkpoint")
            .unwrap()
            .latest_run
            .unwrap()
            .success,
        Some(true)
    );
}
