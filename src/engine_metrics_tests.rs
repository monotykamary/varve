//! Integration tests for Main's S12-O engine wiring; no elapsed-time assertions.
use super::*;
use tempfile::TempDir;

fn row(value: f64) -> Row {
    Row {
        timestamp_us: 0,
        tenant: "tenant".into(),
        series: "series".into(),
        value,
        tags: BTreeMap::new(),
    }
}

fn request(id: &str, value: f64) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: id.into(),
        rows: vec![row(value)],
        now_us: 0,
    }
}

fn database(config: Config) -> Result<(TempDir, Database)> {
    let dir = TempDir::new()?;
    let db = Database::open(dir.path(), config)?;
    db.create_table("metrics", TableConfig::default())?;
    Ok((dir, db))
}

fn delta(before: &PerformanceSnapshot, after: &PerformanceSnapshot, phase: Phase) -> u64 {
    after.phases[phase.as_str()].count - before.phases[phase.as_str()].count
}

fn assert_counts(
    before: &PerformanceSnapshot,
    after: &PerformanceSnapshot,
    expected: &[(Phase, u64)],
) {
    for &(phase, count) in expected {
        assert_eq!(delta(before, after, phase), count, "{}", phase.as_str());
    }
}

fn assert_disk_scopes_completed(before: &PerformanceSnapshot, after: &PerformanceSnapshot) {
    let waits = delta(before, after, Phase::DiskLockWait);
    assert!(waits > 0);
    assert_eq!(waits, delta(before, after, Phase::DiskLockHold));
}

#[test]
fn actual_direct_group_retry_and_invalid_paths_have_distinct_counts() -> Result<()> {
    let (_dir, db) = database(Config::default())?;
    let (_other_dir, other) = database(Config::default())?;
    let isolated = other.performance();
    let before = db.performance();
    let direct = db.write("metrics", "direct", vec![row(1.0)], 0)?;
    assert!(!direct.duplicate);
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::WalEncode, 1),
            // Create/write and rename have separate admitted namespace scopes.
            (Phase::WalDiskLockWait, 2),
            (Phase::DiskLockWait, 2),
            (Phase::DiskLockHold, 2),
            (Phase::CommitLockWait, 1),
            (Phase::CommitDetach, 1),
            (Phase::CommitInstall, 1),
            (Phase::WalWrite, 1),
            (Phase::WalSync, 2),
            (Phase::GroupPrepare, 0),
        ],
    );

    let requests = vec![request("a", 2.0), request("b", 3.0)];
    let before = db.performance();
    let receipts = db
        .write_group(requests.clone())
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].sequence, direct.sequence + 1);
    assert_eq!(receipts[0].sequence, receipts[1].sequence);
    assert!(receipts.iter().all(|receipt| !receipt.duplicate));
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::GroupPrepare, 1),
            (Phase::WalEncode, 1),
            (Phase::WalDiskLockWait, 2),
            (Phase::WalWrite, 1),
            (Phase::WalSync, 2),
            (Phase::DiskLockWait, 2),
            (Phase::DiskLockHold, 2),
            (Phase::CommitLockWait, 1),
            (Phase::CommitDetach, 1),
            (Phase::CommitInstall, 1),
            (Phase::DerivedVerify, 0),
            (Phase::DerivedPublish, 0),
            (Phase::RawVerify, 0),
            (Phase::RawPublish, 0),
        ],
    );

    let before = db.performance();
    let retries = db
        .write_group(requests)
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    assert!(retries.iter().all(|receipt| receipt.duplicate));
    assert_eq!(retries[0].sequence, receipts[0].sequence);
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::GroupPrepare, 1),
            (Phase::WalEncode, 0),
            (Phase::WalDiskLockWait, 0),
            (Phase::WalWrite, 0),
            (Phase::DiskLockWait, 0),
            (Phase::DiskLockHold, 0),
        ],
    );

    let before = db.performance();
    let mut invalid = request("invalid", 4.0);
    invalid.table = "missing".into();
    let results = db.write_group(vec![invalid]);
    assert_eq!(results.len(), 1);
    assert!(results[0].is_err());
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::GroupPrepare, 1),
            (Phase::WalEncode, 0),
            (Phase::WalDiskLockWait, 0),
            (Phase::WalWrite, 0),
        ],
    );
    for phase in Phase::ALL {
        assert_eq!(
            delta(&isolated, &other.performance(), phase),
            0,
            "{}",
            phase.as_str()
        );
    }
    Ok(())
}

#[test]
fn wal_gate_attempt_precedes_disk_budget_rejection_not_wal_write() -> Result<()> {
    let (dir, db) = database(Config {
        wal_max_bytes: 16 * 1024,
        max_disk_bytes: 32 * 1024,
        max_batch_bytes: 16 * 1024,
        ..Config::default()
    })?;
    // An ordinary charged file forces admission failure without permissions or failpoints.
    fs::write(dir.path().join("budget-filler"), vec![0u8; 32 * 1024])?;
    let before = db.performance();
    let error = db
        .write("metrics", "rejected", vec![row(1.0)], 0)
        .unwrap_err();
    assert!(format!("{error:#}").contains("disk"));
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::WalDiskLockWait, 1),
            (Phase::DiskLockWait, 1),
            (Phase::DiskLockHold, 1),
            (Phase::WalWrite, 0),
            (Phase::WalSync, 0),
        ],
    );
    let s = db.lock()?;
    assert_eq!(hot_count(&s), 0);
    assert!(
        !s.catalog.tables["metrics"]
            .receipts
            .contains_key("rejected")
    );
    Ok(())
}

#[test]
fn actual_checkpoint_modes_attribute_new_raw_and_derived_objects() -> Result<()> {
    for (pages, frozen, locked) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (true, false, true),
    ] {
        let (_dir, db) = database(Config {
            derived_pages: pages,
            derived_page_bytes: 4096,
            checkpoint_frozen_prefix: frozen,
            ..Config::default()
        })?;
        db.write("metrics", "seed", vec![row(1.0)], 0)?;
        let before = db.performance();
        if locked {
            let mut s = db.lock()?;
            checkpoint_locked(&db.inner, &mut s)?;
        } else {
            db.checkpoint()?;
        }
        let after = db.performance();
        let s = db.lock()?;
        assert_eq!(s.catalog.checkpoint_sequence, s.sequence);
        assert_eq!(hot_count(&s), 0);
        assert_eq!(s.catalog.tables["metrics"].segments.len(), 1);
        let root = CheckpointRoot {
            catalog: s.catalog.clone(),
            derived: s.derived_refs.clone(),
        };
        let page_count = root.page_refs().count() as u64;
        assert_eq!(page_count, if pages { 2 } else { 0 });
        assert_counts(
            &before,
            &after,
            &[
                (Phase::CheckpointPrepare, 1),
                (Phase::CheckpointPublish, 1),
                (Phase::CheckpointLocked, u64::from(locked)),
                (Phase::ManifestCommit, 1),
                (Phase::DerivedPublish, page_count),
                (Phase::DerivedVerify, 0),
                (Phase::RawPublish, 1),
                (Phase::RawVerify, 0),
                (Phase::WalDiskLockWait, 0),
                (Phase::WalWrite, 0),
                (Phase::GroupPrepare, 0),
            ],
        );
        assert_disk_scopes_completed(&before, &after);
    }
    Ok(())
}

#[test]
fn group_initial_pressure_splits_planning_at_checkpoint_in_both_modes() -> Result<()> {
    for frozen in [false, true] {
        let (_dir, db) = database(Config {
            hot_max_rows: 2,
            derived_pages: true,
            derived_page_bytes: 4096,
            checkpoint_frozen_prefix: frozen,
            ..Config::default()
        })?;
        let seed = db.write("metrics", "seed", vec![row(1.0), row(2.0)], 0)?;
        let before = db.performance();
        let receipts = db
            .write_group(vec![request("a", 3.0), request("b", 4.0)])
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0].sequence, seed.sequence + 1);
        assert_eq!(receipts[0].sequence, receipts[1].sequence);
        assert_counts(
            &before,
            &db.performance(),
            &[
                (Phase::GroupPrepare, 2),
                (Phase::WalEncode, 1),
                (Phase::WalDiskLockWait, 2),
                (Phase::WalWrite, 1),
                (Phase::CheckpointPrepare, 1),
                (Phase::ManifestCommit, 1),
            ],
        );
        assert_eq!(db.lock()?.catalog.checkpoint_sequence, seed.sequence);
        assert_eq!(db.scan("metrics", None, None, None, None)?.len(), 4);
    }
    Ok(())
}

#[cfg(feature = "fault-injection")]
#[test]
fn group_prepare_is_closed_at_existing_publication_and_checkpoint_hooks() -> Result<()> {
    for pressure in [false, true] {
        let (_dir, db) = database(Config {
            hot_max_rows: 2,
            checkpoint_frozen_prefix: true,
            ..Config::default()
        })?;
        if pressure {
            db.write("metrics", "seed", vec![row(1.0), row(2.0)], 0)?;
        }
        let hook = MaintenanceTestHook::new(if pressure {
            MaintenanceHookPhase::CheckpointPrepare
        } else {
            MaintenanceHookPhase::GroupBeforePublish
        });
        db.set_maintenance_test_hook(Some(hook.clone()))?;
        let before = db.performance();
        let (blocked, during, results) = std::thread::scope(|scope| {
            let worker = scope.spawn(|| db.write_group(vec![request("a", 3.0), request("b", 4.0)]));
            // Timeout is only a deadlock guard, not a performance assertion.
            let blocked = hook.wait_until_blocked(std::time::Duration::from_secs(10));
            let during = db.performance();
            hook.release();
            (
                blocked,
                during,
                worker.join().expect("group writer panicked"),
            )
        });
        db.set_maintenance_test_hook(None)?;
        assert!(blocked, "existing maintenance boundary was not reached");
        assert_counts(
            &before,
            &during,
            &[
                (Phase::GroupPrepare, 1),
                (Phase::WalEncode, u64::from(!pressure)),
                (Phase::WalDiskLockWait, 0),
                (Phase::WalWrite, 0),
                (Phase::RawPublish, 0),
                (Phase::DerivedPublish, 0),
            ],
        );
        let receipts = results.into_iter().collect::<Result<Vec<_>>>()?;
        assert_eq!(receipts.len(), 2);
        assert_counts(
            &before,
            &db.performance(),
            &[
                (Phase::GroupPrepare, if pressure { 2 } else { 1 }),
                (Phase::WalDiskLockWait, 2),
                (Phase::WalWrite, 1),
            ],
        );
    }
    Ok(())
}

fn prepare_current_root(db: &Database) -> Result<PreparedRoot> {
    let candidate = {
        let s = db.lock()?;
        let mut candidate = capture_root(&s, &db.inner.config)?;
        candidate.next.checkpoint_sequence = s.sequence;
        candidate
    };
    // Preparation only: no root installation or assertion of checkpoint durability.
    prepare_root(&db.inner, candidate)
}

#[test]
fn actual_derived_pages_publish_then_verify_and_reject_corrupt_reuse() -> Result<()> {
    let (dir, db) = database(Config {
        derived_pages: true,
        derived_page_bytes: 4096,
        ..Config::default()
    })?;
    db.write("metrics", "seed", vec![row(1.0)], 0)?;
    let before = db.performance();
    let first = prepare_current_root(&db)?;
    let pages = first.root.page_refs().cloned().collect::<Vec<_>>();
    assert_eq!(pages.len(), 2);
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::DerivedPublish, pages.len() as u64),
            (Phase::DerivedVerify, 0),
            (Phase::RawPublish, 0),
            (Phase::RawVerify, 0),
            (Phase::ManifestCommit, 0),
        ],
    );
    for page in &pages {
        page.verify(&fs::read(dir.path().join(page.key()))?)?;
    }

    let before = db.performance();
    let reused = prepare_current_root(&db)?;
    assert_eq!(first.bytes, reused.bytes);
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::DerivedPublish, 0),
            (Phase::DerivedVerify, pages.len() as u64),
            (Phase::DiskLockWait, pages.len() as u64),
            (Phase::DiskLockHold, pages.len() as u64),
        ],
    );

    // The first emitted page is corrupt at its real content-addressed path.
    let path = dir.path().join(pages[0].key());
    let mut corrupt = fs::read(&path)?;
    corrupt[0] ^= 1;
    fs::write(&path, &corrupt)?;
    let before = db.performance();
    assert!(prepare_current_root(&db).is_err());
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::DerivedVerify, 1),
            (Phase::DerivedPublish, 0),
            (Phase::DiskLockWait, 1),
            (Phase::DiskLockHold, 1),
            (Phase::ManifestCommit, 0),
        ],
    );
    assert_eq!(fs::read(path)?, corrupt, "corrupt reuse must not be healed");
    Ok(())
}

#[test]
fn actual_raw_output_publishes_then_verifies_and_rejects_collision() -> Result<()> {
    let (dir, db) = database(Config::default())?;
    db.write("metrics", "seed", vec![row(1.0)], 0)?;
    let batches = db.lock()?.hot["metrics"].clone();
    let table = TableConfig::default();
    let before = db.performance();
    let first = write_resident_partitioned_with_pin(&db.inner, &table, &batches, None)?;
    assert_eq!(first.len(), 1);
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::RawPublish, 1),
            (Phase::RawVerify, 0),
            (Phase::DerivedPublish, 0),
            (Phase::DerivedVerify, 0),
            (Phase::DiskLockWait, 1),
            (Phase::DiskLockHold, 1),
        ],
    );
    let path = dir.path().join(first[0].descriptor.key());
    let bytes = fs::read(&path)?;
    let before = db.performance();
    let reused = write_resident_partitioned_with_pin(&db.inner, &table, &batches, None)?;
    assert_eq!(reused.len(), 1);
    assert_eq!(first[0].descriptor.id, reused[0].descriptor.id);
    assert_eq!(fs::read(&path)?, bytes);
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::RawVerify, 1),
            (Phase::RawPublish, 0),
            (Phase::DiskLockWait, 1),
            (Phase::DiskLockHold, 1),
        ],
    );

    let mut corrupt = bytes;
    corrupt[0] ^= 1;
    fs::write(&path, &corrupt)?;
    let before = db.performance();
    let error = write_resident_partitioned_with_pin(&db.inner, &table, &batches, None)
        .err()
        .expect("corrupt raw object must be rejected");
    assert!(format!("{error:#}").contains("immutable local segment collision"));
    assert_counts(
        &before,
        &db.performance(),
        &[
            (Phase::RawVerify, 1),
            (Phase::RawPublish, 0),
            (Phase::DiskLockWait, 1),
            (Phase::DiskLockHold, 1),
        ],
    );
    assert_eq!(fs::read(path)?, corrupt);
    Ok(())
}
