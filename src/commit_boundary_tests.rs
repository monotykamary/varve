use super::*;
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(10);

#[test]
fn readiness_detects_commit_poison_without_waiting_for_commit() {
    let (_dir, db, _) = setup(false);
    {
        let _commit = db.lock_commit().unwrap();
        assert!(db.is_ready(), "a busy, healthy commit gate is still ready");
    }
    let writer = db.clone();
    assert!(
        std::thread::spawn(move || {
            let _commit = writer.lock_commit().unwrap();
            // Model an unwind during unlocked WAL I/O: no state guard is held.
            panic!("injected unlocked publication panic");
        })
        .join()
        .is_err()
    );
    assert!(db.inner.commit.is_poisoned());
    assert!(!db.inner.state.is_poisoned());
    assert!(db.lock().unwrap().fenced.is_none());
    assert!(!db.is_ready());
    assert!(append(&db, false).iter().all(Result::is_err));
}

fn pending_frame(db: &Database) -> wal::EncodedRecord {
    let sequence = db.lock().unwrap().sequence;
    let mut record = wal::decode(&fs::read(wal::path(&db.inner.root, sequence)).unwrap()).unwrap();
    record.sequence += 1;
    let wal::Operation::Append { request_id, .. } = &mut record.operation else {
        panic!("expected seed append");
    };
    *request_id = "v1:150:race".into();
    wal::EncodedRecord::new(&record).unwrap()
}

// Exercise the real WAL publisher and the same disk transitions as publish_append,
// using its existing barrier callback rather than a production accounting hook.
fn namespace_transition_waits_for_accounting(fail_sync: bool) {
    use wal::AppendBarrier;

    let (dir, db, config) = setup(false);
    let encoded = pending_frame(&db);
    let bytes = encoded.as_bytes().to_vec();
    let sequence = wal::decode(&bytes).unwrap().sequence;
    let before = directory_bytes(dir.path()).unwrap();
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::WalBeforeSync);
    let writer = db.clone();
    let pause = hook.clone();
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let target = if fail_sync {
        AppendBarrier::TempCleanup
    } else {
        AppendBarrier::Rename
    };
    let write = std::thread::spawn(move || {
        let _commit = writer.lock_commit().unwrap();
        let mut disk = Some(lock_disk_admission(&writer.inner).unwrap());
        ensure_budget(&writer.inner, encoded.len() as u64).unwrap();
        let result = wal::append_encoded_with_barriers(
            &writer.inner.root,
            &encoded,
            Some(&writer.inner.metrics),
            |phase| {
                if phase == target {
                    assert!(disk.is_none());
                    // This is an actual acquisition attempt after the accounting
                    // thread captured its DirEntry, not merely a paused writer.
                    assert!(matches!(
                        writer.inner.disk_admission.try_lock(),
                        Err(std::sync::TryLockError::WouldBlock)
                    ));
                    attempt_tx.send(phase).unwrap();
                }
                append_disk_barrier(&writer.inner, &mut disk, phase)?;
                match phase {
                    AppendBarrier::FileSync | AppendBarrier::DirectorySync => {
                        assert!(disk.is_none());
                        assert!(writer.inner.disk_admission.try_lock().is_ok());
                        assert!(writer.inner.state.try_lock().is_ok());
                    }
                    AppendBarrier::Rename | AppendBarrier::TempCleanup => {
                        // Pin the production transition invariant as well as the
                        // schedule: omitting its acquisition must fail this test.
                        assert!(disk.is_some());
                        assert!(matches!(
                            writer.inner.disk_admission.try_lock(),
                            Err(std::sync::TryLockError::WouldBlock)
                        ));
                    }
                }
                if phase == AppendBarrier::FileSync {
                    pause.block(MaintenanceHookPhase::WalBeforeSync)?;
                }
                Ok(())
            },
        );
        drop(disk);
        done_tx.send(result).unwrap();
    });
    assert!(hook.wait_until_blocked(WAIT));
    let disk = lock_disk_admission(&db.inner).unwrap();
    let entry = fs::read_dir(dir.path().join("wal"))
        .unwrap()
        .map(Result::unwrap)
        .find(|entry| entry.path().extension().is_some_and(|ext| ext == "tmp"))
        .expect("complete temporary WAL");
    assert!(entry.file_type().unwrap().is_file());
    let temporary = entry.path();
    assert_eq!(fs::read(&temporary).unwrap(), bytes);
    if fail_sync {
        hook.release_with_error();
    } else {
        hook.release();
    }
    assert_eq!(attempt_rx.recv_timeout(WAIT).unwrap(), target);
    // Complete the captured-entry walk while rename/removal is attempting to
    // enter. NotFound is not ignored, and the pending bytes are not free space.
    assert_eq!(entry.metadata().unwrap().len(), bytes.len() as u64);
    let used = directory_bytes(dir.path()).unwrap();
    assert_eq!(used, before + bytes.len() as u64);
    ensure_budget(&db.inner, config.max_disk_bytes - used).unwrap();
    assert!(ensure_budget(&db.inner, config.max_disk_bytes - used + 1).is_err());
    assert!(ensure_budget(&db.inner, config.max_disk_bytes - before).is_err());
    assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    drop(disk);
    let result = done_rx
        .recv_timeout(WAIT)
        .expect("namespace change did not complete");
    assert_eq!(result.is_err(), fail_sync);
    write.join().unwrap();
    assert!(!temporary.exists());
    let disk = lock_disk_admission(&db.inner).unwrap();
    if fail_sync {
        assert!(!wal::path(dir.path(), sequence).exists());
        assert_eq!(directory_bytes(dir.path()).unwrap(), before);
    } else {
        assert_eq!(fs::read(wal::path(dir.path(), sequence)).unwrap(), bytes);
        assert_eq!(directory_bytes(dir.path()).unwrap(), used);
    }
    drop(disk);
    // This low-level probe publishes WAL only; reopen exercises independent replay.
    drop(db);
    let reopened = Database::open(dir.path(), config).unwrap();
    assert_eq!(
        reopened
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        if fail_sync { 1 } else { 2 }
    );
}

#[test]
fn wal_rename_waits_for_captured_accounting_entry_and_preserves_full_charge() {
    namespace_transition_waits_for_accounting(false);
}

#[test]
fn wal_error_cleanup_waits_for_captured_accounting_entry() {
    namespace_transition_waits_for_accounting(true);
}

#[test]
fn wal_error_cleanup_reuses_admission_already_held() {
    use wal::AppendBarrier;

    for fail_before_release in [true, false] {
        let (dir, db, _) = setup(false);
        let encoded = pending_frame(&db);
        let before = directory_bytes(dir.path()).unwrap();
        let worker = db.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let write = std::thread::spawn(move || {
            let _commit = worker.lock_commit().unwrap();
            let mut disk = Some(lock_disk_admission(&worker.inner).unwrap());
            let mut cleaned = false;
            let result =
                wal::append_encoded_with_barriers(&worker.inner.root, &encoded, None, |phase| {
                    if fail_before_release && phase == AppendBarrier::FileSync {
                        // Same guard state as a write failure before the first barrier.
                        anyhow::bail!("injected failure before releasing admission");
                    }
                    if phase == AppendBarrier::TempCleanup {
                        assert!(disk.is_some(), "failure must retain admission");
                        cleaned = true;
                    }
                    append_disk_barrier(&worker.inner, &mut disk, phase)?;
                    if !fail_before_release && phase == AppendBarrier::Rename {
                        anyhow::bail!("injected rename failure with admission held");
                    }
                    Ok(())
                });
            assert!(cleaned);
            drop(disk);
            done_tx.send(result).unwrap();
        });
        assert!(
            done_rx
                .recv_timeout(WAIT)
                .expect("recursive disk lock")
                .is_err()
        );
        write.join().unwrap();
        assert_eq!(directory_bytes(dir.path()).unwrap(), before);
        assert!(fs::read_dir(dir.path().join("wal")).unwrap().all(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_none_or(|ext| ext != "tmp")
        }));
    }
}

#[test]
fn private_resident_growth_is_not_admitted_as_snapshot_headroom() {
    let (_dir, db, config) = setup(true);
    let _commit = db.lock_commit().unwrap();
    let mut s = db.lock().unwrap();
    let input = AdmittedWrite::new(request("v1:150:a", 2.0, 150))
        .unwrap()
        .prepare(&config)
        .unwrap();
    let prepared = prepare_live_append(
        &s,
        &input,
        next_sequence(&s).unwrap(),
        0,
        None,
        &config,
        None,
    )
    .unwrap();
    let charged = prepared.derived.resident + s.derived_working.load(Ordering::SeqCst);
    let mut overlay = AppendOverlay::new(&s);
    overlay.push(prepared);
    let pending = overlay.into_pending();
    assert_eq!(
        s.derived_resident_bytes + s.derived_working.load(Ordering::SeqCst),
        charged
    );
    let snapshot = reserve_derived(&s, &config, config.derived_max_bytes - charged).unwrap();
    pending.install(&mut s);
    assert!(
        s.derived_resident_bytes + s.derived_working.load(Ordering::SeqCst)
            <= config.derived_max_bytes
    );
    drop(snapshot);
    assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
}

fn row(value: f64) -> Row {
    Row {
        timestamp_us: 1,
        tenant: "tenant".into(),
        series: "series".into(),
        value,
        tags: BTreeMap::new(),
    }
}

fn setup(pages: bool) -> (TempDir, Database, Config) {
    let dir = TempDir::new().unwrap();
    let config = Config {
        derived_pages: pages,
        checkpoint_frozen_prefix: true,
        query_retained_inputs: true,
        decoded_cache_bytes: 0,
        ..Config::default()
    };
    let db = Database::open(dir.path(), config.clone()).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            shards: 1,
            rollup_widths_us: vec![10],
            idempotency_window_us: Some(100),
            ..Default::default()
        },
    )
    .unwrap();
    db.write("metrics", "v1:100:seed", vec![row(1.0)], 100)
        .unwrap();
    (dir, db, config)
}

fn request(id: &str, value: f64, now_us: i64) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: id.into(),
        rows: vec![row(value)],
        now_us,
    }
}

fn append(db: &Database, grouped: bool) -> Vec<Result<WriteReceipt>> {
    if grouped {
        db.write_group(vec![
            request("v1:150:a", 2.0, 150),
            request("v1:150:b", 3.0, 150),
        ])
    } else {
        vec![db.write("metrics", "v1:150:a", vec![row(2.0)], 150)]
    }
}

type CommittedObservation = (u64, usize, usize, u64, Option<i64>, Vec<ResidentBatch>);

fn observe(db: &Database, sql: bool) -> Result<CommittedObservation> {
    let status = db.status()?;
    let raw = db.scan("metrics", None, None, None, None)?;
    let rollups = db.rollups("metrics")?;
    let floor = db.idempotency_floor_us("metrics")?;
    if sql {
        let result = db.query("SELECT (SELECT count(*) FROM metrics) AS raw_n, (SELECT CAST(sum(count) AS BIGINT) FROM metrics__rollup) AS derived_n")?;
        assert_eq!(result[0]["raw_n"], 1);
        assert_eq!(result[0]["derived_n"], 1);
    }
    let s = db.lock()?;
    assert_eq!(s.catalog.tables["metrics"].receipts.len(), 1);
    assert!(
        !s.catalog.tables["metrics"]
            .receipts
            .contains_key("v1:150:a")
    );
    Ok((
        status.sequence,
        status.hot_rows,
        raw.len(),
        rollups[0].count,
        floor,
        s.hot["metrics"].clone(),
    ))
}

#[test]
fn paused_wal_sync_keeps_committed_readers_and_snapshots_live_until_atomic_install() {
    for pages in [false, true] {
        for grouped in [false, true] {
            for phase in [
                MaintenanceHookPhase::WalBeforeSync,
                MaintenanceHookPhase::WalBeforeDirectorySync,
            ] {
                let (dir, db, config) = setup(pages);
                let hook = MaintenanceTestHook::new(phase);
                db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
                let writer = db.clone();
                let (ack_tx, ack_rx) = mpsc::channel();
                let write = std::thread::spawn(move || {
                    let result = append(&writer, grouped);
                    ack_tx.send(()).unwrap();
                    result
                });
                assert!(hook.wait_until_blocked(WAIT));
                assert!(ack_rx.try_recv().is_err(), "ack preceded durability");
                assert!(db.inner.commit.try_lock().is_err());
                assert!(db.is_ready(), "normal WAL sync must not block readiness");
                // Disk admission is also free once the complete temporary is charged.
                assert!(db.inner.disk_admission.try_lock().is_ok());
                let reader = db.clone();
                let (tx, rx) = mpsc::channel();
                let read = std::thread::spawn(move || {
                    tx.send(observe(&reader, true)).unwrap();
                });
                let observed = rx.recv_timeout(WAIT);
                hook.release();
                let (_, hot, raw, derived, floor, snapshot) =
                    observed.expect("read blocked on WAL sync").unwrap();
                read.join().unwrap();
                assert_eq!((hot, raw, derived, floor), (1, 1, 1, Some(0)));
                let receipts = write.join().unwrap();
                let rows = if grouped { 3 } else { 2 };
                for receipt in receipts {
                    let receipt = receipt.unwrap();
                    assert_eq!(receipt.durability, "local_fsync");
                    assert_eq!(receipt.sequence, 3);
                }
                let s = db.lock().unwrap();
                assert_eq!(s.sequence, 3);
                assert_eq!(hot_count(&s), rows);
                assert_eq!(s.catalog.tables["metrics"].receipts.len(), rows);
                assert_eq!(
                    s.catalog.tables["metrics"]
                        .rollups
                        .values()
                        .next()
                        .unwrap()
                        .count,
                    rows as u64
                );
                assert_eq!(
                    snapshot.iter().map(|batch| batch.rows.len()).sum::<usize>(),
                    1,
                    "old immutable snapshot changed"
                );
                assert_eq!(s.idempotency_floors["metrics"], 50);
                drop(s);
                db.set_maintenance_test_hook(None).unwrap();
                drop(db);
                let reopened = Database::open(dir.path(), config).unwrap();
                assert_eq!(
                    reopened
                        .scan("metrics", None, None, None, None)
                        .unwrap()
                        .len(),
                    rows
                );
                assert_eq!(reopened.rollups("metrics").unwrap()[0].count, rows as u64);
                assert!(
                    reopened
                        .write("metrics", "v1:150:a", vec![row(2.0)], 150)
                        .unwrap()
                        .duplicate
                );
            }
        }
    }
}

#[test]
fn checkpointed_file_snapshot_progresses_during_pending_directory_sync() {
    let (_dir, db, _config) = setup(true);
    db.checkpoint().unwrap();
    assert_eq!(db.status().unwrap().decoded_cache_bytes, 0);
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::WalBeforeDirectorySync);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let writer = db.clone();
    let write = std::thread::spawn(move || append(&writer, false));
    assert!(hook.wait_until_blocked(WAIT));
    let reader = db.clone();
    let (tx, rx) = mpsc::channel();
    let read = std::thread::spawn(move || {
        let observed = (|| -> Result<_> {
            let status = reader.status()?;
            let raw = reader.scan("metrics", None, None, None, None)?;
            let query = reader.query("SELECT (SELECT count(*) FROM metrics) AS raw_n, (SELECT CAST(sum(count) AS BIGINT) FROM metrics__rollup) AS derived_n")?;
            Ok((status, raw, query))
        })();
        tx.send(observed).unwrap();
    });
    let observed = rx.recv_timeout(WAIT);
    hook.release();
    let (status, raw, query) = observed
        .expect("file snapshot blocked on append sync")
        .unwrap();
    read.join().unwrap();
    assert_eq!(
        (status.sequence, status.checkpoint_sequence, status.hot_rows),
        (2, 2, 0)
    );
    assert_eq!(raw.len(), 1);
    assert_eq!(query[0]["raw_n"], 1);
    assert_eq!(query[0]["derived_n"], 1);
    assert!(write.join().unwrap().iter().all(Result::is_ok));
    assert_eq!(
        db.query("SELECT count(*) AS n FROM metrics").unwrap()[0]["n"],
        2
    );
}

#[test]
fn paused_wal_failure_fences_without_install_and_reopen_resolves_publication() {
    for pages in [false, true] {
        for grouped in [false, true] {
            for phase in [
                MaintenanceHookPhase::WalBeforeSync,
                MaintenanceHookPhase::WalBeforeDirectorySync,
            ] {
                let (dir, db, config) = setup(pages);
                let hook = MaintenanceTestHook::new(phase);
                db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
                let writer = db.clone();
                let write = std::thread::spawn(move || append(&writer, grouped));
                assert!(hook.wait_until_blocked(WAIT));
                let reader = db.clone();
                let (tx, rx) = mpsc::channel();
                let read = std::thread::spawn(move || {
                    tx.send(observe(&reader, false)).unwrap();
                });
                let observed = rx.recv_timeout(WAIT);
                hook.release_with_error();
                assert_eq!(
                    observed.expect("read blocked on failing sync").unwrap().1,
                    1
                );
                read.join().unwrap();
                assert!(write.join().unwrap().iter().all(Result::is_err));
                let status = db.status().unwrap();
                assert!(status.fenced.is_some());
                assert_eq!(
                    (status.sequence, status.hot_rows, status.idempotency_keys),
                    (2, 1, 1)
                );
                assert_eq!(status.derived_working_bytes, 0);
                let s = db.lock().unwrap();
                assert_eq!(s.idempotency_floors["metrics"], 0);
                assert_eq!(
                    s.catalog.tables["metrics"]
                        .rollups
                        .values()
                        .next()
                        .unwrap()
                        .count,
                    1
                );
                drop(s);
                assert!(
                    db.write("metrics", "v1:150:later", vec![row(9.0)], 150)
                        .is_err()
                );
                assert!(db.checkpoint().is_err());
                drop(db);
                let reopened = Database::open(dir.path(), config).unwrap();
                let published = phase == MaintenanceHookPhase::WalBeforeDirectorySync;
                let rows = 1 + if published {
                    if grouped { 2 } else { 1 }
                } else {
                    0
                };
                assert_eq!(
                    reopened
                        .scan("metrics", None, None, None, None)
                        .unwrap()
                        .len(),
                    rows
                );
                assert_eq!(reopened.rollups("metrics").unwrap()[0].count, rows as u64);
                let retry = reopened
                    .write("metrics", "v1:150:a", vec![row(2.0)], 150)
                    .unwrap();
                assert_eq!(retry.duplicate, published);
            }
        }
    }
}

#[test]
fn concurrent_direct_group_control_and_maintenance_wait_outside_reader_state() {
    let (dir, db, config) = setup(true);
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::WalBeforeSync);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    let writer = db.clone();
    let first = std::thread::spawn(move || append(&writer, true));
    assert!(hook.wait_until_blocked(WAIT));
    let direct = db.clone();
    let direct =
        std::thread::spawn(move || direct.write("metrics", "v1:150:c", vec![row(4.0)], 150));
    let group = db.clone();
    let group = std::thread::spawn(move || {
        group.write_group(vec![
            request("v1:150:d", 5.0, 150),
            request("v1:150:e", 6.0, 150),
        ])
    });
    let control = db.clone();
    let control = std::thread::spawn(move || control.create_table("other", TableConfig::default()));
    let maintenance = db.clone();
    let maintenance = std::thread::spawn(move || maintenance.maintain(150));
    let reader = db.clone();
    let (tx, rx) = mpsc::channel();
    let read = std::thread::spawn(move || {
        tx.send(observe(&reader, false)).unwrap();
    });
    let observed = rx.recv_timeout(WAIT);
    hook.release();
    assert_eq!(
        observed
            .expect("queued structural writer held reader state")
            .unwrap()
            .1,
        1
    );
    read.join().unwrap();
    assert!(first.join().unwrap().iter().all(Result::is_ok));
    direct.join().unwrap().unwrap();
    assert!(group.join().unwrap().iter().all(Result::is_ok));
    control.join().unwrap().unwrap();
    maintenance.join().unwrap().unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 6);
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 6);
    drop(db);
    let reopened = Database::open(dir.path(), config).unwrap();
    assert_eq!(
        reopened
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        6
    );
    assert_eq!(reopened.status().unwrap().tables, 2);
}

#[test]
fn frozen_prefix_publication_waits_for_private_append_then_preserves_tail() {
    for pages in [false, true] {
        let (dir, db, config) = setup(pages);
        let checkpoint_hook =
            MaintenanceTestHook::new(MaintenanceHookPhase::CheckpointBeforePublish);
        db.set_maintenance_test_hook(Some(checkpoint_hook.clone()))
            .unwrap();
        let checkpointer = db.clone();
        let checkpoint = std::thread::spawn(move || checkpointer.checkpoint());
        assert!(checkpoint_hook.wait_until_blocked(WAIT));
        let append_hook = MaintenanceTestHook::new(MaintenanceHookPhase::WalBeforeDirectorySync);
        db.set_maintenance_test_hook(Some(append_hook.clone()))
            .unwrap();
        let writer = db.clone();
        let write = std::thread::spawn(move || {
            writer.write_group(vec![
                request("v1:100:a", 2.0, 100),
                request("v1:100:b", 3.0, 100),
            ])
        });
        assert!(append_hook.wait_until_blocked(WAIT));
        checkpoint_hook.release();
        let reader = db.clone();
        let (tx, rx) = mpsc::channel();
        let read = std::thread::spawn(move || {
            tx.send(reader.status()).unwrap();
        });
        let observed = rx.recv_timeout(WAIT);
        append_hook.release();
        let status = observed
            .expect("prefix publisher blocked readers behind sync")
            .unwrap();
        assert_eq!((status.sequence, status.hot_rows), (2, 1));
        read.join().unwrap();
        assert!(write.join().unwrap().iter().all(Result::is_ok));
        checkpoint.join().unwrap().unwrap();
        let status = db.status().unwrap();
        assert_eq!(
            (status.sequence, status.checkpoint_sequence, status.hot_rows),
            (3, 2, 2)
        );
        assert!(wal::path(dir.path(), 3).exists());
        assert!(!wal::path(dir.path(), 2).exists());
        assert_eq!(db.rollups("metrics").unwrap()[0].count, 3);
        drop(db);
        let reopened = Database::open(dir.path(), config).unwrap();
        assert_eq!(
            reopened
                .scan("metrics", None, None, None, None)
                .unwrap()
                .len(),
            3
        );
        assert_eq!(reopened.rollups("metrics").unwrap()[0].count, 3);
    }
}

#[test]
fn mixed_epoch_failure_keeps_only_durable_duplicate_clocks_and_successes() {
    for pages in [false, true] {
        for fail in [false, true] {
            let dir = TempDir::new().unwrap();
            let config = Config {
                derived_pages: pages,
                ..Default::default()
            };
            let db = Database::open(dir.path(), config.clone()).unwrap();
            db.create_table(
                "metrics",
                TableConfig {
                    shards: 2,
                    rollup_widths_us: vec![10],
                    late_after_us: Some(50),
                    idempotency_window_us: Some(100),
                    ..Default::default()
                },
            )
            .unwrap();
            let request = |id: &str, value, now_us, timestamp_us| {
                let mut r = request(id, value, now_us);
                r.rows[0].timestamp_us = timestamp_us;
                r
            };
            db.write_group(vec![request("v1:180:seed", 1.0, 180, 150)])
                .pop()
                .unwrap()
                .unwrap();
            let raw_before = db.lock().unwrap().raw_memory.status().reserved_bytes;
            if fail {
                let hook = MaintenanceTestHook::new(MaintenanceHookPhase::GroupBeforePublish);
                hook.release_with_error();
                db.set_maintenance_test_hook(Some(hook)).unwrap();
            }
            let outcomes = db.write_group(vec![
                request("v1:200:a", 2.0, 200, 200),
                request("v1:200:a", 2.0, 210, 200),
                // This conflict's clock must not invalidate the later durable retry.
                request("v1:200:a", 99.0, 290, 200),
                request("v1:280:late", 99.0, 280, 0),
                request("v1:180:seed", 1.0, 270, 150),
                request("v1:240:b", 3.0, 240, 200),
            ]);
            assert!(
                outcomes[2]
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains("conflicts")
            );
            assert!(
                outcomes[3]
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains("lateness")
            );
            let durable = outcomes[4].as_ref().unwrap();
            assert_eq!((durable.sequence, durable.duplicate), (2, true));
            for i in [0, 1, 5] {
                assert_eq!(outcomes[i].is_err(), fail);
            }
            let s = db.lock().unwrap();
            assert_eq!(
                s.idempotency_floors["metrics"],
                if fail { 170 } else { 180 }
            );
            assert_eq!(hot_count(&s), if fail { 1 } else { 3 });
            assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
            if fail {
                assert_eq!(s.raw_memory.status().reserved_bytes, raw_before);
            }
            drop(s);
            if !fail {
                assert!(outcomes[1].as_ref().unwrap().duplicate);
                let decoded = wal::decode(&fs::read(wal::path(dir.path(), 3)).unwrap()).unwrap();
                let wal::Operation::AppendGroup { items } = decoded.operation else {
                    panic!("group")
                };
                assert_eq!(
                    items
                        .iter()
                        .map(|i| i.request_id.as_str())
                        .collect::<Vec<_>>(),
                    ["v1:200:a", "v1:240:b"]
                );
            }
        }
    }
}

#[test]
fn no_wal_candidate_retains_only_clock_valid_advances_explicitly() {
    let (_dir, db, _) = setup(false);
    // The existing seed uses timestamp 1; reject new rows after clock validation
    // with a hot capacity ceiling, without changing any raw or derived state.
    let mut config = db.inner.config.clone();
    config.hot_max_rows = 1;
    let s = db.lock().unwrap();
    let baseline = s.idempotency_floors.clone();
    let mut overlay = AppendOverlay::new(&s);
    let valid = AdmittedWrite::new(request("v1:160:capacity", 2.0, 160))
        .unwrap()
        .prepare(&config)
        .unwrap();
    assert!(
        overlay
            .prepare_group_item(&valid, 3, &config, None)
            .is_err()
    );
    assert_eq!(overlay.delta.floors["metrics"], 60);
    for (id, now) in [
        ("malformed", 900),
        ("v1:20:old", 900),
        ("v1:999999999:future", 900),
    ] {
        let input = AdmittedWrite::new(request(id, 3.0, now))
            .unwrap()
            .prepare(&config)
            .unwrap();
        assert!(overlay.retry(&input).is_err());
        assert_eq!(overlay.delta.floors["metrics"], 60);
    }
    assert!(overlay.items.is_empty());
    assert!(overlay.delta.durable_floors.is_empty());
    assert_eq!(s.idempotency_floors, baseline);
    drop(overlay);
    drop(s);
    // Public no-WAL policy exercised through an actual lateness rejection.
    db.create_table(
        "late",
        TableConfig {
            late_after_us: Some(10),
            idempotency_window_us: Some(100),
            ..Default::default()
        },
    )
    .unwrap();
    let mut late = request("v1:250:late", 2.0, 250);
    late.table = "late".into();
    let sequence = db.status().unwrap().sequence;
    assert!(db.write_group(vec![late]).pop().unwrap().is_err());
    let s = db.lock().unwrap();
    assert_eq!(s.idempotency_floors["late"], 150);
    assert_eq!(s.sequence, sequence);
    assert!(s.catalog.tables["late"].receipts.is_empty());
    assert!(s.catalog.tables["late"].rollups.is_empty());
    assert!(!s.hot.contains_key("late"));
}
