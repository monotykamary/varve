use super::*;
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;

// Ship captures under CommitLease before pausing its real FileStore CAS. The
// competing CAS changes only the bytes/token, not the validity of the JSON head.
struct FenceRaceStore {
    store: crate::remote::FileStore,
    pause: Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>,
    races: AtomicUsize,
}

impl RemoteStore for FenceRaceStore {
    fn local_root(&self) -> Option<&Path> {
        self.store.local_root()
    }
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.store.get(key)
    }
    fn get_bounded(&self, key: &str, max: usize) -> Result<Vec<u8>> {
        self.store.get_bounded(key, max)
    }
    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.store.put_immutable(key, bytes)
    }
    fn head(&self) -> Result<Option<crate::remote::HeadObject>> {
        self.store.head()
    }
    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        let pause = self.pause.lock().unwrap().take();
        if let Some((entered, release)) = pause {
            entered.send(())?;
            release.recv_timeout(Duration::from_secs(10))?;
            let mut competing = bytes.to_vec();
            competing.push(b' ');
            self.store.compare_and_swap_head(expected, &competing)?;
            self.races.fetch_add(1, Ordering::SeqCst);
        }
        self.store.compare_and_swap_head(expected, bytes)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.store.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.store.delete(key)
    }
}

fn fence_request(id: &str, now_us: i64) -> WriteRequest {
    let mut input = request("a", id);
    input.now_us = now_us;
    input
}

fn remote_fence_setup(journal: bool) -> (TempDir, TempDir, Database, Arc<FenceRaceStore>) {
    let dir = TempDir::new().unwrap();
    let remote = TempDir::new().unwrap();
    let store = Arc::new(FenceRaceStore {
        store: crate::remote::FileStore::new(remote.path()).unwrap(),
        pause: Mutex::new(None),
        races: AtomicUsize::new(0),
    });
    let db = Database::open_with_remote(
        dir.path(),
        Config {
            segmented_journal: journal,
            ..Default::default()
        },
        Some(store.clone()),
    )
    .unwrap();
    db.create_table(
        "a",
        TableConfig {
            shards: 4,
            window_us: 10,
            rollup_widths_us: vec![10],
            idempotency_window_us: Some(100),
            ..Default::default()
        },
    )
    .unwrap();
    db.write_group(vec![fence_request("v1:180:seed", 180)])[0]
        .as_ref()
        .unwrap();
    db.ship().unwrap();
    (dir, remote, db, store)
}

fn pause_fence_ship(
    db: &Database,
    store: &FenceRaceStore,
) -> (mpsc::Sender<()>, std::thread::JoinHandle<Result<u64>>) {
    let (entered, wait) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    *store.pause.lock().unwrap() = Some((entered, resume));
    let shipper = db.clone();
    let ship = std::thread::spawn(move || shipper.ship());
    wait.recv_timeout(Duration::from_secs(10)).unwrap();
    (release, ship)
}

fn observe_remote_fence(db: &Database, store: &FenceRaceStore, release: mpsc::Sender<()>) {
    // Only ship can contend here. Its tier CAS error sets State.fenced before
    // the outer handler waits on our lease; do not join ship while retaining it.
    let (waiting, wait) = mpsc::channel();
    db.inner.commit.notify_next_wait(waiting);
    release.send(()).unwrap();
    wait.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(store.races.load(Ordering::SeqCst), 1);
    let s = db.lock().unwrap();
    assert!(
        s.fenced
            .as_deref()
            .is_some_and(|reason| reason.starts_with("remote publication fenced:")),
        "the real tier CAS path must fence State before the outer handler"
    );
}

fn fence_wal_namespace(db: &Database) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn collect(path: &Path, files: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        if !path.exists() {
            return;
        }
        if path.is_dir() {
            files.insert(path.to_path_buf(), None);
            for entry in fs::read_dir(path).unwrap() {
                collect(&entry.unwrap().path(), files);
            }
        } else {
            files.insert(path.to_path_buf(), Some(fs::read(path).unwrap()));
        }
    }
    let mut files = BTreeMap::new();
    collect(&db.inner.root.join("wal"), &mut files);
    collect(&db.inner.root.join("journal"), &mut files);
    files
}

#[test]
fn real_remote_cas_fence_before_sync_and_after_sync_preserves_only_old_receipts() {
    for journal in [false, true] {
        for synced in [false, true] {
            let (_dir, _remote, db, store) = remote_fence_setup(journal);
            let (release_cas, ship) = pause_fence_ship(&db, &store);
            let before = snapshot(&db);
            let (sequence, generation, wal_bytes, old_receipt) = {
                let s = db.lock().unwrap();
                assert_eq!(s.idempotency_floors["a"], 80);
                (
                    s.sequence,
                    s.generation,
                    s.wal_bytes,
                    s.catalog.tables["a"].receipts["v1:180:seed"].clone(),
                )
            };
            let writes = db.performance().phases["wal_write"].count;
            // Capture after ship has sealed/captured any journal segment. Remote
            // intent files may change; every WAL/journal name and byte is checked.
            let wal_before = fence_wal_namespace(&db);
            let epoch = prepare(
                &db,
                vec![
                    fence_request("v1:180:seed", 270),
                    fence_request("v1:200:new", 200),
                    fence_request("v1:200:new", 290),
                ],
            );
            assert!(epoch.publication.is_some());
            assert!(epoch.results.iter().all(Result::is_ok));
            assert_eq!(epoch.results[0].as_ref().unwrap().sequence, sequence);
            assert!(epoch.results[0].as_ref().unwrap().duplicate);
            assert_eq!(epoch.results[1].as_ref().unwrap().sequence, sequence + 1);
            assert!(!epoch.results[1].as_ref().unwrap().duplicate);
            assert_eq!(epoch.results[2].as_ref().unwrap().sequence, sequence + 1);
            assert!(epoch.results[2].as_ref().unwrap().duplicate);
            assert_eq!(snapshot(&db), before);
            assert_eq!(fence_wal_namespace(&db), wal_before);
            let (results, durable_wal) = if synced {
                let durable = epoch.sync();
                assert_eq!(snapshot(&db), before, "sync must not install");
                let durable_wal = fence_wal_namespace(&db);
                assert_ne!(durable_wal, wal_before, "sync must perform real WAL I/O");
                observe_remote_fence(&db, &store, release_cas);
                (durable.install(), durable_wal)
            } else {
                observe_remote_fence(&db, &store, release_cas);
                (epoch.publish(), wal_before)
            };
            assert!(ship.join().unwrap().is_err());
            assert_eq!(results.len(), 3);
            let old = results[0].as_ref().unwrap();
            assert!(old.duplicate);
            assert_eq!(old.sequence, sequence);
            assert_eq!(old.rows, old_receipt.rows);
            assert_eq!(old.durability, "local_fsync");
            for result in &results[1..] {
                assert!(result.as_ref().unwrap_err().to_string().contains("fenced"));
            }
            // Only the independently durable retry's floor (270 - 100), never
            // the same-epoch duplicate's clock (290 - 100), survives failure.
            let mut expected: serde_json::Value = serde_json::from_slice(&before).unwrap();
            expected[3]["a"] = 170.into();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&snapshot(&db)).unwrap(),
                expected
            );
            {
                let s = db.lock().unwrap();
                assert_eq!(
                    (s.sequence, s.generation, s.wal_bytes),
                    (sequence, generation, wal_bytes)
                );
                assert_eq!(s.catalog.tables["a"].receipts["v1:180:seed"], old_receipt);
                assert!(!s.catalog.tables["a"].receipts.contains_key("v1:200:new"));
                assert_eq!(hot_count(&s), 2);
                assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
            }
            assert_eq!(
                db.performance().phases["wal_write"].count,
                writes + u64::from(synced)
            );
            assert_eq!(
                fence_wal_namespace(&db),
                durable_wal,
                "never truncate durable bytes"
            );
            assert_eq!(db.inner.commit.is_poisoned(), synced);
            assert!(!db.is_ready());
            let raw = db.inner.raw_memory.status();
            assert_eq!(raw.reserved_bytes, raw.live_bytes);
            assert_eq!(raw.working_bytes, 0);
        }
    }
}

#[test]
fn real_remote_cas_fence_does_not_change_prepared_no_publication_outcomes() {
    for journal in [false, true] {
        let (_dir, _remote, db, store) = remote_fence_setup(journal);
        let sequence = db.status().unwrap().sequence;
        let (release_cas, ship) = pause_fence_ship(&db, &store);
        let epoch = prepare(
            &db,
            vec![
                fence_request("v1:180:seed", 270),
                fence_request("v1:180:seed", 275),
            ],
        );
        assert!(epoch.publication.is_none());
        assert_eq!(db.idempotency_floor_us("a").unwrap(), Some(175));
        let before = snapshot(&db);
        let wal_before = fence_wal_namespace(&db);
        let writes = db.performance().phases["wal_write"].count;
        // This epoch has no lease. Hold one only to observe ship's post-fence
        // wait deterministically, then let ship finish before returning outcomes.
        let observer = db.lock_commit().unwrap();
        observe_remote_fence(&db, &store, release_cas);
        drop(observer);
        assert!(ship.join().unwrap().is_err());
        let results = epoch.publish();
        assert_eq!(results.len(), 2);
        for result in results {
            let receipt = result.unwrap();
            assert!(receipt.duplicate);
            assert_eq!(receipt.sequence, sequence);
            assert_eq!(receipt.durability, "local_fsync");
        }
        assert_eq!(snapshot(&db), before);
        assert_eq!(fence_wal_namespace(&db), wal_before);
        assert_eq!(db.performance().phases["wal_write"].count, writes);
        assert!(!db.inner.commit.is_poisoned());
        assert!(!db.is_ready());
    }
}

#[test]
fn real_remote_cas_fence_drains_ingestor_senders_and_barrier_without_install() {
    struct ReleaseInstall(MaintenanceTestHook);
    impl Drop for ReleaseInstall {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    for journal in [false, true] {
        let (_dir, _remote, db, store) = remote_fence_setup(journal);
        let sequence = db.status().unwrap().sequence;
        let (release_cas, ship) = pause_fence_ship(&db, &store);
        let before = snapshot(&db);
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::EpochBeforeInstall);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let ingestor = crate::ingest::Ingestor::new(
            db.clone(),
            crate::ingest::IngestConfig {
                max_group_requests: 1,
                max_delay: Duration::ZERO,
                ..Default::default()
            },
        )
        .unwrap();
        // Drop before ingestor on assertion unwind, so its shutdown cannot
        // strand the install worker on this test's unreleased pause.
        let release_install = ReleaseInstall(hook.clone());
        let mut first = ingestor.submit(fence_request("v1:200:new", 200)).unwrap();
        assert!(hook.wait_until_blocked(Duration::from_secs(10)));
        let durable_wal = fence_wal_namespace(&db);
        observe_remote_fence(&db, &store, release_cas);
        // Enqueue later work only after observing ship, so it cannot consume the
        // one-shot waiter probe intended for the real tier error handler.
        let mut later = ingestor.submit(fence_request("v1:201:later", 201)).unwrap();
        let mut barrier = ingestor.flush().unwrap();
        assert!(matches!(
            first.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            later.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            barrier.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(snapshot(&db), before);
        // No injected error: only the observed real remote fence rejects install.
        drop(release_install);
        let draining = ingestor.clone();
        let (tx, rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            assert!(first.blocking_recv().unwrap().is_err());
            assert!(later.blocking_recv().unwrap().is_err());
            let terminal = barrier.blocking_recv().unwrap().unwrap();
            assert_eq!(
                (
                    terminal.sequence,
                    terminal.completed,
                    terminal.succeeded,
                    terminal.failed
                ),
                (sequence, 2, 0, 2)
            );
            draining.shutdown().unwrap();
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(10)).unwrap();
        waiter.join().unwrap();
        assert!(ship.join().unwrap().is_err());
        db.set_maintenance_test_hook(None).unwrap();
        assert_eq!(snapshot(&db), before);
        assert_eq!(fence_wal_namespace(&db), durable_wal);
        let stats = ingestor.stats();
        assert_eq!((stats.submitted, stats.completed, stats.failed), (2, 2, 2));
        assert_eq!(
            (
                stats.pending_requests,
                stats.pending_bytes,
                stats.dropped_receivers
            ),
            (0, 0, 0)
        );
        let flow = ingestor.flow_stats();
        assert_eq!(
            (flow.claimed, flow.reclaimed, flow.charged_bytes),
            (3, 3, 0)
        );
        assert_eq!(flow.stage_finished, vec![3; 4]);
        assert!(
            !flow.fenced,
            "ordinary database errors must drain the flow ring"
        );
        assert!(db.inner.commit.is_poisoned());
        assert!(!db.is_ready());
        assert_eq!(db.lock().unwrap().derived_working.load(Ordering::SeqCst), 0);
        let raw = db.inner.raw_memory.status();
        assert_eq!(raw.reserved_bytes, raw.live_bytes);
        assert_eq!(raw.working_bytes, 0);
    }
}

#[test]
fn followup_gate_only_poison_is_visible_in_status_until_reopen() {
    for state_reason in [None, Some("specific independently recorded fence")] {
        let (dir, db, config) = setup(false);
        let mut lease = db.lock_commit().unwrap();
        lease.arm();
        let mut state = db.lock().unwrap();
        state.fenced = state_reason.map(str::to_owned);
        drop(lease);
        assert_eq!(
            state.fenced.as_deref(),
            state_reason,
            "gate drop must not acquire or mutate State"
        );
        drop(state);
        assert!(!db.is_ready());
        let status = db.status().unwrap();
        if let Some(reason) = state_reason {
            assert_eq!(status.fenced.as_deref(), Some(reason));
        } else {
            let reason = status.fenced.expect("gate-only poison must be observable");
            assert!(
                reason.contains("publication")
                    && reason.contains("reopen")
                    && reason.contains("recovery")
            );
        }
        drop(db);
        let reopened = Database::open(dir.path(), config).unwrap();
        assert!(reopened.is_ready());
        assert!(reopened.status().unwrap().fenced.is_none());
    }
}

#[test]
fn followup_published_prefix_then_completion_error_rechecks_new_root() {
    for journal in [false, true] {
        for pages in [false, true] {
            for case in 0..3 {
                let dir = TempDir::new().unwrap();
                let config = Config {
                    hot_max_rows: 4,
                    checkpoint_frozen_prefix: true,
                    segmented_journal: journal,
                    derived_pages: pages,
                    ..Default::default()
                };
                let db = Database::open(dir.path(), config.clone()).unwrap();
                db.create_table(
                    "a",
                    TableConfig {
                        idempotency_window_us: Some(100),
                        rollup_widths_us: vec![10],
                        ..Default::default()
                    },
                )
                .unwrap();
                let input = |id: &str, value: f64, now_us| {
                    let mut r = request("a", id);
                    r.rows.truncate(1);
                    r.rows[0].value = value;
                    r.now_us = now_us;
                    r
                };
                db.write_group(vec![
                    input("v1:180:seed", 1.0, 180),
                    input("v1:250:seed", 2.0, 250),
                    input("v1:250:filler", 3.0, 250),
                ])
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .unwrap();
                let sequence = db.status().unwrap().sequence;
                let root_epoch = db.lock().unwrap().root_epoch;
                let old_root = fs::read(dir.path().join("manifest.bin")).unwrap();
                let hook = MaintenanceTestHook::new(MaintenanceHookPhase::GroupCheckpointComplete);
                db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
                // Four physical inputs fit the configured row-group ceiling;
                // adding a fifth would split away the durable clock donor.
                let retry = match case {
                    0 => input("v1:180:seed", 1.0, 270),
                    1 => input("v1:180:seed", 1.0, 300),
                    _ => input("v1:250:seed", 99.0, 350),
                };
                let requests = vec![
                    input("v1:200:new", 4.0, 200),
                    input("v1:200:new", 99.0, 290),
                    retry,
                    input("v1:250:seed", 2.0, 350),
                ];
                let worker_db = db.clone();
                let worker = std::thread::spawn(move || worker_db.write_group(requests));
                assert!(hook.wait_until_blocked(Duration::from_secs(10)));
                let durable_floor = if case == 0 { 180 } else { 200 };
                {
                    let state = db.lock().unwrap();
                    assert!(state.root_epoch > root_epoch);
                    assert_eq!(state.catalog.checkpoint_sequence, sequence);
                    assert_eq!(
                        state.catalog.tables["a"].idempotency_floor_us,
                        Some(durable_floor)
                    );
                    assert_eq!(state.idempotency_floors["a"], durable_floor);
                    assert_eq!(
                        state.catalog.tables["a"]
                            .receipts
                            .contains_key("v1:180:seed"),
                        case == 0
                    );
                    assert!(
                        !state.catalog.tables["a"]
                            .receipts
                            .contains_key("v1:200:new")
                    );
                    assert_eq!(hot_count(&state), 0);
                }
                let published_root = fs::read(dir.path().join("manifest.bin")).unwrap();
                assert_ne!(published_root, old_root);
                hook.release_with_error();
                let results = worker.join().unwrap();
                assert!(
                    format!("{:#}", results[0].as_ref().unwrap_err())
                        .contains("injected maintenance")
                );
                assert!(results[1].is_err());
                if case != 0 {
                    assert!(
                        results[2]
                            .as_ref()
                            .unwrap_err()
                            .to_string()
                            .contains(if case == 1 { "window" } else { "conflicts" })
                    );
                } else {
                    let receipt = results[2].as_ref().unwrap();
                    assert!(receipt.duplicate);
                    assert_eq!(receipt.sequence, sequence);
                }
                assert!(results[3].as_ref().unwrap().duplicate);
                assert_eq!(results[3].as_ref().unwrap().sequence, sequence);
                assert_eq!(db.status().unwrap().sequence, sequence);
                let terminal_floor = db.idempotency_floor_us("a").unwrap().unwrap();
                assert_eq!(terminal_floor, 250);
                assert!(terminal_floor >= durable_floor);
                assert_eq!(
                    fs::read(dir.path().join("manifest.bin")).unwrap(),
                    published_root
                );
                assert!(db.is_ready());
                let raw = db.inner.raw_memory.status();
                assert_eq!(raw.reserved_bytes, raw.live_bytes);
                assert_eq!(raw.working_bytes, 0);
                assert_eq!(db.lock().unwrap().derived_working.load(Ordering::SeqCst), 0);
                db.set_maintenance_test_hook(None).unwrap();
                assert_eq!(db.scan("a", None, None, None, None).unwrap().len(), 3);
                db.checkpoint().unwrap();
                drop(db);
                let reopened = Database::open(dir.path(), config).unwrap();
                assert_eq!(reopened.idempotency_floor_us("a").unwrap(), Some(250));
                assert_eq!(reopened.scan("a", None, None, None, None).unwrap().len(), 3);
                assert!(
                    reopened.write_group(vec![input("v1:250:seed", 2.0, 350)])[0]
                        .as_ref()
                        .unwrap()
                        .duplicate
                );
                assert!(reopened.write_group(vec![input("v1:180:seed", 1.0, 270)])[0].is_err());
            }
        }
    }
}

fn setup(journal: bool) -> (TempDir, Database, Config) {
    let dir = TempDir::new().unwrap();
    let config = Config {
        segmented_journal: journal,
        derived_pages: true,
        ..Config::default()
    };
    let db = Database::open(dir.path(), config.clone()).unwrap();
    for name in ["a", "b"] {
        db.create_table(
            name,
            TableConfig {
                shards: 4,
                window_us: 10,
                rollup_widths_us: vec![10],
                ..Default::default()
            },
        )
        .unwrap();
    }
    (dir, db, config)
}

fn request(table: &str, id: &str) -> WriteRequest {
    WriteRequest {
        table: table.into(),
        request_id: id.into(),
        now_us: 20,
        rows: [1, 11]
            .into_iter()
            .map(|timestamp_us| Row {
                timestamp_us,
                tenant: "tenant".into(),
                series: format!("s{timestamp_us}"),
                value: f64::from_bits(0x3ff0000000000001),
                tags: BTreeMap::new(),
            })
            .collect(),
    }
}

fn prepare(db: &Database, requests: Vec<WriteRequest>) -> PreparedEpoch {
    let inputs = requests
        .into_iter()
        .map(|r| {
            let mut input = AdmittedWrite::new(r).unwrap();
            input.reserve(&db.inner.raw_memory).unwrap();
            input.prepare(&db.inner.config)
        })
        .collect();
    db.prepare_epoch(inputs)
}

fn snapshot(db: &Database) -> Vec<u8> {
    let s = db.lock().unwrap();
    let hot: BTreeMap<_, Vec<_>> = s
        .hot
        .iter()
        .map(|(name, batches)| {
            (
                name,
                batches.iter().flat_map(|batch| batch.rows.iter()).collect(),
            )
        })
        .collect();
    serde_json::to_vec(&(
        s.sequence,
        &s.catalog,
        hot,
        &s.idempotency_floors,
        s.metadata_bytes,
        s.derived_resident_bytes,
    ))
    .unwrap()
}

#[test]
fn real_owned_epoch_is_send_static_and_preserves_atomic_state_across_threads() {
    fn send_static<T: Send + 'static>() {}
    send_static::<PreparedEpoch>();
    send_static::<commit_boundary::DurableEpoch>();
    for journal in [false, true] {
        let (dir, db, config) = setup(journal);
        let seed = db
            .write_group(vec![request("a", "seed")])
            .pop()
            .unwrap()
            .unwrap();
        let before = snapshot(&db);
        let bytes = directory_bytes(dir.path()).unwrap();
        let writer = db.clone();
        let epoch = std::thread::spawn(move || {
            prepare(
                &writer,
                vec![
                    request("a", "seed"),
                    request("missing", "bad"),
                    request("a", "new"),
                    request("a", "new"),
                    request("b", "new"),
                ],
            )
        })
        .join()
        .unwrap();
        assert!(epoch.publication.is_some(), "not a static validation token");
        assert_eq!(
            epoch
                .publication
                .as_ref()
                .unwrap()
                .pending
                .delta
                .batches
                .len(),
            2
        );
        assert_eq!(epoch.results.len(), 5);
        assert_eq!(snapshot(&db), before);
        assert_eq!(
            directory_bytes(dir.path()).unwrap(),
            bytes,
            "P performed WAL I/O"
        );
        assert!(db.inner.raw_memory.status().reserved_bytes > 0);
        assert!(db.lock().unwrap().derived_working.load(Ordering::SeqCst) > 0);
        assert!(db.inner.commit.try_lock().is_err());
        assert!(db.is_ready());
        // D occurs on another thread, before V; authority and credit still live.
        let durable = std::thread::spawn(move || epoch.sync()).join().unwrap();
        assert_eq!(snapshot(&db), before);
        assert!(directory_bytes(dir.path()).unwrap() > bytes);
        assert!(db.inner.commit.try_lock().is_err());
        assert!(db.inner.raw_memory.status().reserved_bytes > 0);
        let results = std::thread::spawn(move || durable.install())
            .join()
            .unwrap();
        assert_eq!(results[0].as_ref().unwrap().sequence, seed.sequence);
        assert!(results[0].as_ref().unwrap().duplicate);
        assert!(results[1].is_err());
        let seq = results[2].as_ref().unwrap().sequence;
        assert_eq!(results[3].as_ref().unwrap().sequence, seq);
        assert!(results[3].as_ref().unwrap().duplicate);
        assert_eq!(results[4].as_ref().unwrap().sequence, seq);
        let raw = db.inner.raw_memory.status();
        assert_eq!(
            raw.reserved_bytes, raw.live_bytes,
            "only installed rows retain raw credit"
        );
        assert_eq!(raw.working_bytes, 0);
        assert_eq!(db.lock().unwrap().derived_working.load(Ordering::SeqCst), 0);
        let a = db.scan("a", None, None, None, None).unwrap();
        let b = db.scan("b", None, None, None, None).unwrap();
        let mut ordinals: Vec<_> = a
            .iter()
            .chain(&b)
            .filter(|r| r.sequence == seq)
            .map(|r| r.ordinal)
            .collect();
        ordinals.sort_unstable();
        assert_eq!(
            ordinals,
            vec![0, 1, 2, 3],
            "only accepted rows consume ordinals"
        );
        let expected =
            serde_json::to_vec(&(a, b, db.rollups("a").unwrap(), db.rollups("b").unwrap()))
                .unwrap();
        drop(db);
        let reopened = Database::open(dir.path(), config).unwrap();
        assert_eq!(
            serde_json::to_vec(&(
                reopened.scan("a", None, None, None, None).unwrap(),
                reopened.scan("b", None, None, None, None).unwrap(),
                reopened.rollups("a").unwrap(),
                reopened.rollups("b").unwrap()
            ))
            .unwrap(),
            expected
        );
        assert!(
            reopened
                .write_group(vec![request("a", "new"), request("b", "new")])
                .iter()
                .all(|r| r.as_ref().unwrap().duplicate)
        );
    }
}

#[test]
fn unpublished_epoch_drop_under_state_releases_authority_and_all_private_credit() {
    let (_dir, db, _) = setup(false);
    let before = snapshot(&db);
    let epoch = prepare(&db, vec![request("a", "new"), request("b", "new")]);
    let s = db.lock().unwrap();
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        drop(epoch);
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("drop tried to acquire State");
    drop(s);
    worker.join().unwrap();
    assert!(db.is_ready());
    assert_eq!(snapshot(&db), before);
    assert_eq!(db.inner.raw_memory.status().reserved_bytes, 0);
    assert_eq!(db.inner.raw_memory.status().live_bytes, 0);
    assert_eq!(db.lock().unwrap().derived_working.load(Ordering::SeqCst), 0);
    assert!(db.write_group(vec![request("a", "new")])[0].is_ok());
}

#[test]
fn durable_epoch_normal_drop_and_caught_panic_fence_without_state_deadlock() {
    for journal in [false, true] {
        for panic in [false, true] {
            let (dir, db, config) = setup(journal);
            let before = snapshot(&db);
            let durable = prepare(&db, vec![request("a", "new"), request("b", "new")]).sync();
            let s = db.lock().unwrap();
            let (tx, rx) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _owned = durable;
                    assert!(!panic, "caught worker panic after durability");
                }));
                assert_eq!(result.is_err(), panic);
                tx.send(()).unwrap();
            });
            rx.recv_timeout(Duration::from_secs(10))
                .expect("durable drop tried to lock State");
            drop(s);
            worker.join().unwrap();
            assert!(!db.is_ready());
            assert!(!db.inner.state.is_poisoned());
            assert_eq!(snapshot(&db), before);
            assert_eq!(db.inner.raw_memory.status().reserved_bytes, 0);
            assert_eq!(db.inner.raw_memory.status().live_bytes, 0);
            assert!(db.write_group(vec![request("a", "reuse")])[0].is_err());
            assert!(
                db.create_table("forbidden", TableConfig::default())
                    .is_err()
            );
            assert!(db.maintain(20).is_err());
            drop(db);
            let reopened = Database::open(dir.path(), config).unwrap();
            for table in ["a", "b"] {
                assert_eq!(
                    reopened.scan(table, None, None, None, None).unwrap().len(),
                    2
                );
                assert!(
                    reopened.write_group(vec![request(table, "new")])[0]
                        .as_ref()
                        .unwrap()
                        .duplicate
                );
            }
        }
    }
}

#[test]
fn successful_initial_checkpoint_preserves_recheckable_receipt_without_prepaying_success() {
    for frozen in [false, true] {
        for pages in [false, true] {
            for case in 0..4 {
                let dir = TempDir::new().unwrap();
                let config = Config {
                    hot_max_rows: 4,
                    checkpoint_frozen_prefix: frozen,
                    derived_pages: pages,
                    ..Default::default()
                };
                let db = Database::open(dir.path(), config.clone()).unwrap();
                db.create_table(
                    "a",
                    TableConfig {
                        idempotency_window_us: Some(100),
                        rollup_widths_us: vec![10],
                        ..Default::default()
                    },
                )
                .unwrap();
                let input = |id: &str, value: f64, now_us| {
                    let mut r = request("a", id);
                    r.rows.truncate(1);
                    r.rows[0].value = value;
                    r.now_us = now_us;
                    r
                };
                db.write_group(vec![
                    input("v1:180:seed", 1.0, 180),
                    input("v1:250:seed", 2.0, 250),
                    input("v1:250:filler", 3.0, 250),
                ])
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .unwrap();
                assert_eq!(db.idempotency_floor_us("a").unwrap(), Some(150));
                let before = db.status().unwrap().sequence;
                // case 3 is independently valid but truly expired by an accepted
                // earlier row: retaining its receipt must NOT prepay success.
                let (new_id, new_now, conflict_now) = if case == 3 {
                    ("v1:220:a", 300, 310)
                } else {
                    ("v1:200:a", 200, 290)
                };
                let results = db.write_group(vec![
                    input(new_id, 4.0, new_now),
                    input(new_id, 99.0, conflict_now),
                    input(
                        "v1:180:seed",
                        if case == 2 { 99.0 } else { 1.0 },
                        if case == 1 { 300 } else { 270 },
                    ),
                    input("v1:250:seed", 2.0, 350),
                ]);
                assert!(results[0].is_ok());
                assert!(
                    results[1]
                        .as_ref()
                        .unwrap_err()
                        .to_string()
                        .contains("conflicts")
                );
                assert_eq!(results[2].is_ok(), case == 0, "case {case}: {results:?}");
                if case == 0 {
                    assert!(results[2].as_ref().unwrap().duplicate);
                }
                assert!(results[3].as_ref().unwrap().duplicate);
                let s = db.lock().unwrap();
                let table = &s.catalog.tables["a"];
                assert_eq!(
                    s.catalog.checkpoint_sequence, before,
                    "must take initial pressure root"
                );
                assert_eq!(
                    table.idempotency_floor_us,
                    Some(if case == 0 || case == 3 { 180 } else { 200 })
                );
                assert_eq!(
                    table.receipts.contains_key("v1:180:seed"),
                    case == 0 || case == 3
                );
                assert_eq!(s.idempotency_floors["a"], 250);
                drop(s);
                let raw = db.scan("a", None, None, None, None).unwrap();
                let rollups = db.rollups("a").unwrap();
                assert_eq!(raw.len(), 4);
                assert_eq!(rollups.iter().map(|r| r.count).sum::<u64>(), 4);
                // Persist the terminal duplicate clock, then prove exact recovery
                // and rejection rather than accidental reinsertion of expired IDs.
                db.checkpoint().unwrap();
                drop(db);
                let reopened = Database::open(dir.path(), config).unwrap();
                assert_eq!(reopened.scan("a", None, None, None, None).unwrap(), raw);
                assert_eq!(reopened.rollups("a").unwrap(), rollups);
                assert_eq!(reopened.idempotency_floor_us("a").unwrap(), Some(250));
                assert!(reopened.write_group(vec![input(new_id, 4.0, new_now)])[0].is_err());
                assert!(
                    reopened.write_group(vec![input("v1:250:seed", 2.0, 350)])[0]
                        .as_ref()
                        .unwrap()
                        .duplicate
                );
            }
        }
    }
}

#[test]
fn initial_pressure_checkpoint_failure_preserves_only_valid_durable_retry_and_floor() {
    for frozen in [false, true] {
        for pages in [false, true] {
            for invalid in [false, true] {
                let dir = TempDir::new().unwrap();
                let config = Config {
                    hot_max_rows: 3,
                    checkpoint_frozen_prefix: frozen,
                    derived_pages: pages,
                    ..Default::default()
                };
                let db = Database::open(dir.path(), config.clone()).unwrap();
                db.create_table(
                    "a",
                    TableConfig {
                        rollup_widths_us: vec![10],
                        idempotency_window_us: Some(100),
                        ..Default::default()
                    },
                )
                .unwrap();
                let input = |id: &str, value: f64, now_us| {
                    let mut r = request("a", id);
                    r.rows.truncate(1);
                    r.rows[0].value = value;
                    r.now_us = now_us;
                    r
                };
                let seed = input("v1:180:seed", 1.0, 180);
                db.write_group(vec![seed, input("v1:180:filler", 2.0, 180)])
                    .into_iter()
                    .collect::<Result<Vec<_>>>()
                    .unwrap();
                let before = snapshot(&db);
                let sequence = db.status().unwrap().sequence;
                let wal = fs::read(wal::path(dir.path(), sequence)).unwrap();
                let root = fs::read(dir.path().join("manifest.bin")).unwrap();
                let hook = MaintenanceTestHook::new(if frozen {
                    MaintenanceHookPhase::RootPrepare
                } else {
                    MaintenanceHookPhase::CheckpointLockedPrepare
                });
                hook.release_with_error();
                db.set_maintenance_test_hook(Some(hook)).unwrap();
                // Two pending rows plus two committed rows force INITIAL pressure,
                // before the overlay has corrected the conflicting clock at 290.
                let results = db.write_group(vec![
                    input("v1:200:a", 2.0, 200),
                    input("v1:200:a", 99.0, 290),
                    input("v1:180:seed", 1.0, if invalid { 300 } else { 270 }),
                ]);
                assert!(results[0].is_err() && results[1].is_err());
                assert!(
                    format!("{:#}", results[0].as_ref().unwrap_err())
                        .contains("injected maintenance")
                );
                if !frozen {
                    assert!(db.performance().phases["checkpoint_locked"].count > 0);
                }
                if invalid {
                    assert!(
                        results[2]
                            .as_ref()
                            .unwrap_err()
                            .to_string()
                            .contains("window")
                    );
                    assert_eq!(db.idempotency_floor_us("a").unwrap(), Some(80));
                    assert_eq!(snapshot(&db), before);
                } else {
                    let receipt = results[2].as_ref().unwrap();
                    assert!(receipt.duplicate);
                    assert_eq!(receipt.sequence, sequence);
                    assert_eq!(db.idempotency_floor_us("a").unwrap(), Some(170));
                    // Only this already-durable clock is allowed to change.
                    let mut expected: serde_json::Value = serde_json::from_slice(&before).unwrap();
                    expected[3]["a"] = 170.into();
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(&snapshot(&db)).unwrap(),
                        expected
                    );
                }
                assert_eq!(db.status().unwrap().sequence, sequence);
                assert_eq!(fs::read(wal::path(dir.path(), sequence)).unwrap(), wal);
                assert_eq!(fs::read(dir.path().join("manifest.bin")).unwrap(), root);
                assert!(!wal::path(dir.path(), sequence + 1).exists());
                let raw = db.inner.raw_memory.status();
                assert_eq!(raw.reserved_bytes, raw.live_bytes);
                assert_eq!(raw.working_bytes, 0);
                assert_eq!(db.lock().unwrap().derived_working.load(Ordering::SeqCst), 0);
                // Same failure path must not rescue conflicts or new IDs.
                for (id, value) in [("v1:180:seed", 99.0), ("v1:180:uncommitted", 1.0)] {
                    let results = db.write_group(vec![
                        input("v1:200:x", 2.0, 200),
                        input("v1:200:x", 99.0, 290),
                        input(id, value, 270),
                    ]);
                    assert!(results.iter().all(Result::is_err));
                }
                db.set_maintenance_test_hook(None).unwrap();
                db.checkpoint().unwrap();
                drop(db);
                let db = Database::open(dir.path(), config).unwrap();
                assert_eq!(db.scan("a", None, None, None, None).unwrap().len(), 2);
                assert_eq!(
                    db.rollups("a")
                        .unwrap()
                        .iter()
                        .map(|r| r.count)
                        .sum::<u64>(),
                    2
                );
                assert_eq!(
                    db.idempotency_floor_us("a").unwrap(),
                    Some(if invalid { 80 } else { 170 })
                );
            }
        }
    }
}

#[test]
fn post_sync_install_failure_preserves_old_duplicate_but_fences_new_epoch() {
    for journal in [false, true] {
        let (dir, db, config) = setup(journal);
        db.write_group(vec![request("a", "seed")])[0]
            .as_ref()
            .unwrap();
        let before = snapshot(&db);
        let epoch = prepare(
            &db,
            vec![
                request("a", "seed"),
                request("a", "new"),
                request("a", "new"),
                request("b", "new"),
            ],
        );
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::EpochBeforeInstall);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let worker = std::thread::spawn(move || epoch.publish());
        assert!(hook.wait_until_blocked(Duration::from_secs(10)));
        assert_eq!(snapshot(&db), before);
        hook.release_with_error();
        let results = worker.join().unwrap();
        assert!(results[0].as_ref().unwrap().duplicate);
        assert!(results[1..].iter().all(Result::is_err));
        assert!(!db.is_ready());
        assert_eq!(snapshot(&db), before);
        drop(db);
        let reopened = Database::open(dir.path(), config).unwrap();
        assert_eq!(reopened.scan("a", None, None, None, None).unwrap().len(), 4);
        assert_eq!(reopened.scan("b", None, None, None, None).unwrap().len(), 2);
        assert!(
            reopened
                .write_group(vec![request("a", "new"), request("b", "new")])
                .iter()
                .all(|r| r.as_ref().unwrap().duplicate)
        );
    }
}
