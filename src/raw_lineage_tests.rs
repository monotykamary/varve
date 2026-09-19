use super::*;
use tempfile::TempDir;

fn row(timestamp_us: i64, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "series".into(),
        value,
        tags: BTreeMap::new(),
    }
}

fn setup(prefix: bool, pages: bool, cache: usize) -> (TempDir, Database, Config) {
    let dir = TempDir::new().unwrap();
    let config = Config {
        checkpoint_frozen_prefix: prefix,
        derived_pages: pages,
        decoded_cache_bytes: cache,
        query_retained_inputs: true,
        query_workers: 1,
        query_executable: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tools/duckdb"),
        segment_rows: 64,
        compact_min_segments: 2,
        ..Config::default()
    };
    let db = Database::open(dir.path(), config.clone()).unwrap();
    for name in ["metrics", "other"] {
        db.create_table(
            name,
            TableConfig {
                shards: 1,
                window_us: 1000,
                rollup_widths_us: vec![10],
                ..TableConfig::default()
            },
        )
        .unwrap();
    }
    (dir, db, config)
}

fn lineage(db: &Database) -> Vec<ResidentLineage> {
    capture_resident_snapshot(&db.lock().unwrap(), Vec::new()).lineage
}

fn stamp(db: &Database, name: &str) -> u64 {
    db.lock().unwrap().raw_stamps[name]
}

#[test]
fn raw_lineage_checkpoint_cache_and_compaction_preserve_logical_stamp() {
    for prefix in [false, true] {
        for pages in [false, true] {
            let (_dir, db, _) = setup(prefix, pages, 64 * 1024);
            db.write("metrics", "a", vec![row(10, 1.0)], 100).unwrap();
            let hot = lineage(&db);
            db.checkpoint().unwrap();
            let cold = lineage(&db);
            assert_eq!(hot[0].raw_stamp, cold[0].raw_stamp);
            assert_ne!(hot[0].ids, cold[0].ids);
            assert_eq!(hot[1].raw_stamp, cold[1].raw_stamp);
            let raw_stamp = stamp(&db, "metrics");
            let descriptor = db.lock().unwrap().catalog.tables["metrics"].segments[0].clone();
            {
                let mut s = db.lock().unwrap();
                assert!(touch_decoded(&mut s, &descriptor.id).is_some());
                s.decoded.clear();
            }
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
            assert_eq!(stamp(&db, "metrics"), raw_stamp);
            db.write("metrics", "b", vec![row(20, 2.0)], 100).unwrap();
            let appended = stamp(&db, "metrics");
            assert_ne!(appended, raw_stamp);
            // Exercise synchronous admission/fallback checkpoint as well as the
            // explicit exact-stamp and frozen-prefix implementations above.
            {
                let _commit = db.lock_commit().unwrap();
                checkpoint_locked(&db.inner, &mut db.lock().unwrap()).unwrap();
            }
            assert_eq!(stamp(&db, "metrics"), appended);
            let ids = lineage(&db)[0].ids.clone();
            assert_eq!(ids.len(), 2);
            assert_eq!(db.compact().unwrap(), 1);
            assert_eq!(stamp(&db, "metrics"), appended);
            assert_ne!(lineage(&db)[0].ids, ids);
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
        }
    }
}

#[test]
fn raw_lineage_direct_group_duplicate_and_rejected_append() {
    let (_dir, db, _) = setup(true, true, 0);
    let empty = stamp(&db, "metrics");
    let other = stamp(&db, "other");
    db.write("metrics", "a", vec![row(10, 1.0)], 100).unwrap();
    let direct = stamp(&db, "metrics");
    assert_ne!(direct, empty);
    assert_eq!(stamp(&db, "other"), other);
    assert!(
        db.write("metrics", "a", vec![row(10, 1.0)], 100)
            .unwrap()
            .duplicate
    );
    assert_eq!(stamp(&db, "metrics"), direct);
    assert!(db.write("metrics", "a", vec![row(10, 2.0)], 100).is_err());
    assert_eq!(stamp(&db, "metrics"), direct);
    let results = db.write_group(
        ["metrics", "other"]
            .into_iter()
            .map(|name| WriteRequest {
                table: name.into(),
                request_id: "group".into(),
                rows: vec![row(20, 3.0)],
                now_us: 100,
            })
            .collect(),
    );
    for result in results {
        result.unwrap();
    }
    assert_ne!(stamp(&db, "metrics"), direct);
    assert_ne!(stamp(&db, "other"), other);
}

#[test]
fn raw_lineage_selected_subset_keeps_every_table_and_all_live_ids() {
    let (_dir, db, _) = setup(true, false, 0);
    db.write("metrics", "cold", vec![row(10, 1.0)], 100)
        .unwrap();
    db.write("other", "other", vec![row(10, 1.0)], 100).unwrap();
    db.checkpoint().unwrap();
    for (id, timestamp) in [("hot-a", 20), ("hot-b", 30)] {
        db.write("metrics", id, vec![row(timestamp, 2.0)], 100)
            .unwrap();
    }
    let captured = {
        let s = db.lock().unwrap();
        let names = s.catalog.tables.keys().cloned().collect::<Vec<_>>();
        let plan =
            crate::plan::plan("SELECT * FROM metrics WHERE timestamp_us >= 30", &names).unwrap();
        let batches = s.hot["metrics"]
            .iter()
            .filter(|batch| batch.overlaps(Some(&plan), None))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1);
        let snapshot = capture_resident_snapshot(
            &s,
            vec![ResidentTable {
                name: "metrics".into(),
                batches,
                files: Vec::new(),
            }],
        );
        assert_eq!(snapshot.tables.len(), 1);
        assert_eq!(snapshot.lineage.len(), 2);
        let metrics = &snapshot.lineage[0];
        assert_eq!(metrics.name, "metrics");
        assert_eq!(metrics.ids.len(), 3);
        assert!(
            metrics
                .ids
                .contains(&s.catalog.tables["metrics"].segments[0].id)
        );
        for batch in &s.hot["metrics"] {
            assert!(metrics.ids.contains(&batch.id));
        }
        assert_eq!(snapshot.lineage[1].ids.len(), 1);
        snapshot
    };
    db.write("metrics", "later", vec![row(40, 4.0)], 100)
        .unwrap();
    assert_ne!(captured.lineage[0].raw_stamp, stamp(&db, "metrics"));
    assert_eq!(
        captured.lineage[0].ids.len(),
        3,
        "captured lineage changed after unlock"
    );
}

#[test]
fn raw_lineage_retention_removes_rows_and_invalidates_cutoff() {
    let (_dir, db, _) = setup(true, true, 0);
    db.create_table(
        "expiring",
        TableConfig {
            shards: 1,
            retention_us: Some(100),
            ..TableConfig::default()
        },
    )
    .unwrap();
    db.write("expiring", "seed", vec![row(10, 1.0), row(90, 2.0)], 100)
        .unwrap();
    let before = stamp(&db, "expiring");
    assert_eq!(db.maintain(150).unwrap().expired_rows, 1);
    let partial = stamp(&db, "expiring");
    assert_ne!(before, partial);
    assert_eq!(
        db.scan("expiring", None, None, None, None).unwrap().len(),
        1
    );
    assert_eq!(db.maintain(250).unwrap().expired_rows, 1);
    assert_ne!(stamp(&db, "expiring"), partial);
    assert!(
        db.scan("expiring", None, None, None, None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn raw_lineage_unproven_replacement_removal_recreation_and_rebind_invalidate() {
    let (_dir, db, _) = setup(false, false, 0);
    db.write("metrics", "a", vec![row(10, 1.0)], 100).unwrap();
    db.checkpoint().unwrap();
    let original = stamp(&db, "metrics");
    let _commit = db.lock_commit().unwrap();
    let mut s = db.lock().unwrap();
    let descriptor = s.catalog.tables["metrics"].segments[0].clone();
    let mut rows = read_segment_locked(&db.inner, &mut s, &descriptor)
        .unwrap()
        .to_vec();
    rows[0].row.value = 9.0;
    let replacements =
        write_partitioned(&db.inner, &s.catalog.tables["metrics"].config, &rows).unwrap();
    let mut next = s.catalog.clone();
    next.tables.get_mut("metrics").unwrap().segments = replacements;
    persist_manifest(&db.inner, &mut s, next).unwrap();
    assert_ne!(
        s.raw_stamps["metrics"], original,
        "same-count logical replacement reused stamp"
    );
    let replaced = s.raw_stamps["metrics"];
    let mut next = s.catalog.clone();
    next.tables.get_mut("metrics").unwrap().segments.clear();
    persist_manifest(&db.inner, &mut s, next).unwrap();
    assert_ne!(s.raw_stamps["metrics"], replaced);
    // Creation identity and recovery binding override even the physical-only
    // hint. These transitions have no public live table-replacement API today.
    let old = s.catalog.clone();
    let removed = s.raw_stamps["metrics"];
    s.catalog
        .tables
        .get_mut("metrics")
        .unwrap()
        .created_sequence += 1;
    reconcile_raw_stamps(&mut s, &old, true);
    assert_ne!(s.raw_stamps["metrics"], removed);
    let old = s.catalog.clone();
    let recreated = s.raw_stamps["metrics"];
    s.catalog.database_id = uuid::Uuid::new_v4().to_string();
    reconcile_raw_stamps(&mut s, &old, true);
    assert_ne!(s.raw_stamps["metrics"], recreated);
}

#[test]
fn raw_lineage_reopen_uses_fresh_stamps_and_workers() {
    let (dir, db, config) = setup(true, true, 0);
    db.write("metrics", "cold", vec![row(10, 1.0)], 100)
        .unwrap();
    db.checkpoint().unwrap();
    db.write("other", "replay", vec![row(20, 2.0)], 100)
        .unwrap();
    let before = lineage(&db);
    drop(db);
    let reopened = Database::open(dir.path(), config).unwrap();
    for (old, new) in before.iter().zip(lineage(&reopened)) {
        assert_eq!(old.name, new.name);
        assert_ne!(old.raw_stamp, new.raw_stamp);
    }
    assert_eq!(reopened.inner.query_runtime.stats().spawned, 0);
    assert_eq!(
        reopened
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        reopened
            .scan("other", None, None, None, None)
            .unwrap()
            .len(),
        1
    );
}

#[cfg(feature = "fault-injection")]
mod publication {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    const WAIT: Duration = Duration::from_secs(10);

    fn append(db: &Database, grouped: bool) -> Vec<Result<WriteReceipt>> {
        if grouped {
            db.write_group(
                ["metrics", "other"]
                    .into_iter()
                    .map(|name| WriteRequest {
                        table: name.into(),
                        request_id: "pending".into(),
                        rows: vec![row(20, 2.0)],
                        now_us: 100,
                    })
                    .collect(),
            )
        } else {
            vec![db.write("metrics", "pending", vec![row(20, 2.0)], 100)]
        }
    }

    #[test]
    fn raw_lineage_pending_append_stamp_changes_only_on_atomic_install() {
        for grouped in [false, true] {
            for fail in [false, true] {
                for phase in [
                    MaintenanceHookPhase::WalBeforeSync,
                    MaintenanceHookPhase::WalBeforeDirectorySync,
                ] {
                    let (_dir, db, _) = setup(true, true, 0);
                    let before = lineage(&db);
                    let hook = MaintenanceTestHook::new(phase);
                    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
                    let writer = db.clone();
                    let write = std::thread::spawn(move || append(&writer, grouped));
                    assert!(hook.wait_until_blocked(WAIT));
                    let during = lineage(&db);
                    for (a, b) in before.iter().zip(&during) {
                        assert_eq!(a.raw_stamp, b.raw_stamp);
                        assert_eq!(a.ids, b.ids);
                    }
                    if fail {
                        hook.release_with_error();
                    } else {
                        hook.release();
                    }
                    let results = write.join().unwrap();
                    assert!(results.iter().all(|result| result.is_err() == fail));
                    let after = lineage(&db);
                    for (a, b) in before.iter().zip(after) {
                        if fail || (!grouped && a.name == "other") {
                            assert_eq!(a.raw_stamp, b.raw_stamp);
                            assert_eq!(a.ids, b.ids);
                        } else {
                            assert_ne!(a.raw_stamp, b.raw_stamp);
                            assert_eq!(b.ids.len(), a.ids.len() + 1);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn raw_lineage_frozen_prefix_retirement_preserves_appended_tail_stamp() {
        let (_dir, db, _) = setup(true, true, 0);
        db.write("metrics", "prefix", vec![row(10, 1.0)], 100)
            .unwrap();
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::CheckpointPrepare);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let worker = db.clone();
        let checkpoint = std::thread::spawn(move || worker.checkpoint());
        assert!(hook.wait_until_blocked(WAIT));
        db.write("metrics", "tail", vec![row(20, 2.0)], 100)
            .unwrap();
        let before = lineage(&db);
        assert_eq!(before[0].ids.len(), 2);
        hook.release();
        checkpoint.join().unwrap().unwrap();
        let after = lineage(&db);
        assert_eq!(before[0].raw_stamp, after[0].raw_stamp);
        assert_ne!(before[0].ids, after[0].ids);
        let s = db.lock().unwrap();
        assert_eq!(s.hot["metrics"].len(), 1);
        assert_eq!(s.catalog.tables["metrics"].segments.len(), 1);
        assert!(after[0].ids.contains(&s.hot["metrics"][0].id));
        assert!(
            after[0]
                .ids
                .contains(&s.catalog.tables["metrics"].segments[0].id)
        );
    }

    #[test]
    fn raw_lineage_sql_capture_remains_coherent_while_resolver_is_paused() {
        let (_dir, db, _) = setup(true, true, 0);
        db.write("metrics", "prefix", vec![row(10, 1.0)], 100)
            .unwrap();
        db.checkpoint().unwrap();
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::SqlSnapshotCaptured);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let worker = db.clone();
        let query = std::thread::spawn(move || worker.query("SELECT count(*) AS n FROM metrics"));
        assert!(hook.wait_until_blocked(WAIT));
        db.write("metrics", "tail", vec![row(20, 2.0)], 100)
            .unwrap();
        hook.release();
        assert_eq!(query.join().unwrap().unwrap(), json!([{"n": 1}]));
        db.set_maintenance_test_hook(None).unwrap();
        assert_eq!(
            db.query("SELECT count(*) AS n FROM metrics").unwrap(),
            json!([{"n": 2}])
        );
    }

    #[test]
    fn raw_lineage_complete_pending_wal_is_disk_charged_and_gc_excluded() {
        for grouped in [false, true] {
            for phase in [
                MaintenanceHookPhase::WalBeforeSync,
                MaintenanceHookPhase::WalBeforeDirectorySync,
            ] {
                let (dir, db, config) = setup(true, true, 0);
                let before_bytes = directory_bytes(dir.path()).unwrap();
                let wal_before = directory_bytes(&dir.path().join("wal")).unwrap();
                let names_before = fs::read_dir(dir.path().join("wal"))
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .collect::<BTreeSet<_>>();
                let hook = MaintenanceTestHook::new(phase);
                db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
                let writer = db.clone();
                let write = std::thread::spawn(move || append(&writer, grouped));
                assert!(hook.wait_until_blocked(WAIT));
                let added = fs::read_dir(dir.path().join("wal"))
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| !names_before.contains(path))
                    .collect::<Vec<_>>();
                assert_eq!(added.len(), 1);
                let pending = &added[0];
                assert_eq!(
                    pending.extension().unwrap(),
                    if phase == MaintenanceHookPhase::WalBeforeSync {
                        "tmp"
                    } else {
                        "wal"
                    }
                );
                let bytes = fs::read(pending).unwrap();
                let record = wal::decode(&bytes)
                    .expect("complete checksummed WAL before releasing disk admission");
                assert_eq!(record.sequence, db.lock().unwrap().sequence + 1);
                match record.operation {
                    wal::Operation::Append { rows, .. } => {
                        assert!(!grouped);
                        assert_eq!(rows.len(), 1);
                    }
                    wal::Operation::AppendGroup { items } => {
                        assert!(grouped);
                        assert_eq!(items.len(), 2);
                    }
                    _ => panic!("unexpected pending operation"),
                }
                {
                    let _disk = db
                        .inner
                        .disk_admission
                        .try_lock()
                        .expect("disk admission held during sync");
                    let used = directory_bytes(dir.path()).unwrap();
                    assert_eq!(used, before_bytes + bytes.len() as u64);
                    assert_eq!(
                        directory_bytes(&dir.path().join("wal")).unwrap(),
                        wal_before + bytes.len() as u64
                    );
                    let remaining = config.max_disk_bytes - used;
                    ensure_budget(&db.inner, remaining).unwrap();
                    assert!(ensure_budget(&db.inner, remaining + 1).is_err());
                    assert!(
                        ensure_budget(&db.inner, config.max_disk_bytes - before_bytes).is_err(),
                        "pending WAL was admitted as free space"
                    );
                }
                let collector = db.clone();
                let (attempt_tx, attempt_rx) = mpsc::channel();
                let (done_tx, done_rx) = mpsc::channel();
                let gc = std::thread::spawn(move || {
                    attempt_tx
                        .send(collector.inner.commit.try_lock().is_err())
                        .unwrap();
                    let _commit = collector.lock_commit().unwrap();
                    let result = gc_locked(&collector.inner, &mut collector.lock().unwrap());
                    done_tx.send(result).unwrap();
                });
                assert!(
                    attempt_rx.recv_timeout(WAIT).unwrap(),
                    "GC entered private append epoch"
                );
                assert!(done_rx.try_recv().is_err());
                assert_eq!(
                    fs::read(pending).unwrap(),
                    bytes,
                    "GC deleted or modified pending WAL"
                );
                hook.release();
                for result in write.join().unwrap() {
                    result.unwrap();
                }
                done_rx.recv_timeout(WAIT).unwrap().unwrap();
                gc.join().unwrap();
                drop(db);
                let reopened = Database::open(dir.path(), config).unwrap();
                assert_eq!(
                    reopened
                        .scan("metrics", None, None, None, None)
                        .unwrap()
                        .len(),
                    1
                );
                assert_eq!(
                    reopened
                        .scan("other", None, None, None, None)
                        .unwrap()
                        .len(),
                    usize::from(grouped)
                );
            }
        }
    }
}

#[test]
fn raw_lineage_complete_table_sql_reuses_without_reload_across_physical_changes() {
    for prefix in [false, true] {
        for cache in [0, 64 * 1024] {
            let (_dir, db, _) = setup(prefix, true, cache);
            db.write("metrics", "a", vec![row(10, 1.0)], 100).unwrap();
            db.checkpoint().unwrap();
            db.write("metrics", "b", vec![row(20, 2.0)], 100).unwrap();
            let sql = "SELECT count(*) AS n, sum(value) AS total FROM metrics";
            let expected = db.query(sql).unwrap();
            let before = db.inner.query_runtime.stats();
            let raw_stamp = stamp(&db, "metrics");
            db.checkpoint().unwrap();
            assert_eq!(db.query(sql).unwrap(), expected);
            assert_eq!(db.compact().unwrap(), 1);
            assert_eq!(db.query(sql).unwrap(), expected);
            let after = db.inner.query_runtime.stats();
            assert_eq!(stamp(&db, "metrics"), raw_stamp);
            assert_eq!(after.spawned, before.spawned);
            assert_eq!(after.resident_invalidations, before.resident_invalidations);
            assert_eq!(after.resident_full_loads, before.resident_full_loads);
            assert_eq!(after.resident_delta_loads, before.resident_delta_loads);
            assert_eq!(
                after.resident_raw_staged_rows,
                before.resident_raw_staged_rows
            );
            assert_eq!(
                after.resident_raw_staged_bytes,
                before.resident_raw_staged_bytes
            );
            assert_eq!(after.resident_hits, before.resident_hits + 2);
        }
    }
}
