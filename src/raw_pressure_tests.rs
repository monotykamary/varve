use super::*;
use crate::raw_memory::{RawReservationError, row_charge};
use tempfile::TempDir;

thread_local! {
    static FRAME_CLEANUP: std::cell::RefCell<Option<(RawMemoryBudget, Arc<AtomicUsize>)>> = const { std::cell::RefCell::new(None) };
}
pub(super) fn observe_frame_cleanup(encoded: &wal::EncodedRecord) {
    FRAME_CLEANUP.with(|hook| {
        if let Some((budget, observed)) = hook.borrow_mut().take() {
            assert!(
                encoded.len() > 0,
                "probe requires a still-live encoded frame"
            );
            observed.store(budget.status().reserved_bytes, Ordering::SeqCst);
        }
    });
}

#[test]
fn encoded_frame_keeps_credit_through_materialization_cleanup() -> Result<()> {
    for direct in [false, true] {
        for stage in [1, 2, 3, 4] {
            for panic in [false, true] {
                let (dir, db) = open(Config::default())?;
                let make = |id: &str| WriteRequest {
                    table: "metrics".into(),
                    request_id: format!("{id}{}", "x".repeat(220)),
                    now_us: 20,
                    rows: vec![Row {
                        timestamp_us: 10,
                        tenant: "t".into(),
                        series: "s".into(),
                        value: 1.0,
                        tags: BTreeMap::new(),
                    }],
                };
                let inputs = if direct {
                    vec![make("a")]
                } else {
                    vec![make("a"), make("b"), make("c")]
                };
                let required = inputs
                    .iter()
                    .try_fold(0usize, |sum, input| -> Result<usize> {
                        let admitted = AdmittedWrite::new(input.clone())?;
                        let retained =
                            row_charge(input.rows.iter().map(Row::estimated_bytes).sum());
                        Ok(sum + admitted.raw_bytes() - retained)
                    })?;
                let observed = Arc::new(AtomicUsize::new(usize::MAX));
                FRAME_CLEANUP.with(|hook| {
                    *hook.borrow_mut() = Some((db.inner.raw_memory.clone(), observed.clone()))
                });
                write_input::MATERIALIZATION_FAULT
                    .with(|fault| fault.set(Some((stage, if direct { 1 } else { 2 }, panic))));
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if direct {
                        let input = inputs.into_iter().next().unwrap();
                        vec![db.write(&input.table, &input.request_id, input.rows, input.now_us)]
                    } else {
                        db.write_group(inputs)
                    }
                }));
                assert_eq!(result.is_err(), panic);
                assert_eq!(db.inner.commit.is_poisoned(), panic);
                if let Ok(results) = result {
                    assert!(results.iter().all(Result::is_err));
                    assert!(results.iter().all(|result| {
                        format!("{:#}", result.as_ref().unwrap_err())
                            .contains("injected materialization failure")
                    }));
                }
                let actual = observed.load(Ordering::SeqCst);
                assert!(
                    actual != usize::MAX && actual >= required,
                    "direct={direct} stage={stage} panic={panic}: encoded frame still live, but held {actual} < frame envelopes {required}"
                );
                let state = db
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                let sequence = state.sequence;
                assert_eq!(hot_count(&state), 0);
                assert!(state.catalog.tables["metrics"].receipts.is_empty());
                drop(state);
                assert_eq!(db.inner.raw_memory.status().reserved_bytes, 0);
                drop(db);
                let reopened = Database::open(dir.path(), Config::default())?;
                assert_eq!(
                    reopened.status()?.sequence,
                    sequence,
                    "pre-I/O failure published WAL"
                );
                assert_eq!(hot_count(&*reopened.lock()?), 0);
            }
        }
    }
    Ok(())
}

#[test]
fn pre_io_materialization_failure_preserves_durable_duplicate_floors_in_all_modes() -> Result<()> {
    for derived_pages in [false, true] {
        for checkpoint_frozen_prefix in [false, true] {
            for segmented_journal in [false, true] {
                for stage in [1, 2, 3, 4] {
                    let config = Config {
                        derived_pages,
                        checkpoint_frozen_prefix,
                        segmented_journal,
                        ..Config::default()
                    };
                    let dir = TempDir::new()?;
                    let db = Database::open(dir.path(), config.clone())?;
                    db.create_table(
                        "metrics",
                        TableConfig {
                            shards: 1,
                            rollup_widths_us: vec![],
                            idempotency_window_us: Some(100),
                            ..TableConfig::default()
                        },
                    )?;
                    let row = Row {
                        timestamp_us: 150,
                        tenant: "t".into(),
                        series: "s".into(),
                        value: 1.0,
                        tags: BTreeMap::new(),
                    };
                    let seed = db.write("metrics", "v1:150:seed", vec![row.clone()], 150)?;
                    let before = db.status()?;
                    let make = |id: &str, now_us| WriteRequest {
                        table: "metrics".into(),
                        request_id: id.into(),
                        rows: vec![row.clone()],
                        now_us,
                    };
                    write_input::MATERIALIZATION_FAULT
                        .with(|fault| fault.set(Some((stage, 2, false))));
                    let results = db.write_group(vec![
                        make("v1:200:a", 200),
                        make("v1:150:seed", 220),
                        make("v1:230:b", 230),
                        make("v1:240:c", 240),
                    ]);
                    assert_eq!(results.len(), 4);
                    assert!(results[0].is_err() && results[2].is_err() && results[3].is_err());
                    let duplicate = results[1].as_ref().unwrap();
                    assert!(duplicate.duplicate);
                    assert_eq!(duplicate.sequence, seed.sequence);
                    let after = db.status()?;
                    assert_eq!(
                        after.sequence, before.sequence,
                        "failed materialization reached WAL"
                    );
                    assert_eq!(after.wal_bytes, before.wal_bytes);
                    assert!(
                        after.fenced.is_none(),
                        "ordinary pre-I/O failure fenced authority"
                    );
                    {
                        let state = db.lock()?;
                        assert_eq!(hot_count(&state), 1);
                        assert_eq!(state.catalog.tables["metrics"].receipts.len(), 1);
                        assert_eq!(state.idempotency_floors["metrics"], 120);
                    }
                    // The independently durable duplicate clock survives a later root;
                    // no rejected new row/receipt is installed or recovered.
                    db.checkpoint()?;
                    drop(db);
                    let reopened = Database::open(dir.path(), config)?;
                    let state = reopened.lock()?;
                    assert_eq!(state.sequence, seed.sequence);
                    assert_eq!(
                        state.catalog.tables["metrics"].idempotency_floor_us,
                        Some(120)
                    );
                    assert_eq!(state.catalog.tables["metrics"].receipts.len(), 1);
                    assert!(
                        state.catalog.tables["metrics"]
                            .receipts
                            .contains_key("v1:150:seed")
                    );
                }
            }
        }
    }
    Ok(())
}

fn row() -> Row {
    Row {
        timestamp_us: 1,
        tenant: "tenant".into(),
        series: "series".into(),
        value: -0.0,
        tags: BTreeMap::from([("tag".into(), "payload".repeat(128))]),
    }
}
fn request(id: &str, count: usize) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: id.into(),
        rows: (0..count).map(|_| row()).collect(),
        now_us: 10,
    }
}
fn open(config: Config) -> Result<(TempDir, Database)> {
    let dir = TempDir::new()?;
    let db = Database::open(dir.path(), config)?;
    db.create_table(
        "metrics",
        TableConfig {
            shards: 1,
            rollup_widths_us: vec![],
            ..Default::default()
        },
    )?;
    Ok((dir, db))
}

#[test]
fn stored_layout_and_escaped_tag_maximum_fit_admission_contract() -> Result<()> {
    assert!(std::mem::size_of::<StoredRow>() <= 128);
    assert!(std::mem::size_of::<Row>() <= 128);
    let mut row = row();
    row.tags = (0..32)
        .map(|i| (format!("k{i}"), "\u{1}".repeat(1024)))
        .collect();
    row.validate()?;
    let expected = serde_json::to_vec(&row.tags)?.len();
    assert!(expected > 64 * 1024);
    let charge = row_charge(row.estimated_bytes());
    let budget = RawMemoryBudget::new(charge, 1)?;
    let shared = SharedRawRows::build(budget.reserve(charge)?, || {
        Ok(vec![StoredRow {
            row,
            sequence: 1,
            ordinal: 0,
        }])
    })?;
    assert_eq!(shared.max_tags_json_bytes(), expected);
    let pin = shared.pin();
    assert_eq!(pin.max_tags_json_bytes(), expected);
    assert_eq!(budget.status().reserved_bytes, charge);
    drop((pin, shared));
    assert_eq!(budget.status().reserved_bytes, 0);
    Ok(())
}

#[test]
fn borrowed_rollup_updates_match_independent_model_numeric_and_tie_rules() -> Result<()> {
    let seed = StoredRow {
        row: row(),
        sequence: 9,
        ordinal: 3,
    };
    let mut expected = RollupRow::from_row(10, &seed)?;
    let mut borrowed = expected.clone();
    for (timestamp, sequence, ordinal, value) in [
        (1, 9, 2, 0.0),
        (1, 9, 4, 0.30000000000000004),
        (i64::MIN, 0, 0, -0.25),
        (i64::MAX, u64::MAX, u32::MAX, f64::MIN_POSITIVE),
    ] {
        let mut input = row();
        input.timestamp_us = timestamp;
        input.value = value;
        let stored = StoredRow {
            row: input,
            sequence,
            ordinal,
        };
        expected.add(&stored)?;
        add_borrowed_rollup(&mut borrowed, &stored.row, sequence, ordinal)?;
        assert_eq!(
            serde_json::to_vec(&borrowed)?,
            serde_json::to_vec(&expected)?
        );
    }
    for count_overflow in [false, true] {
        let mut expected = expected.clone();
        expected.count = if count_overflow { u64::MAX } else { 1 };
        expected.sum = f64::MAX;
        let mut borrowed = expected.clone();
        let mut input = seed.clone();
        input.row.value = if count_overflow { 0.0 } else { f64::MAX };
        assert!(expected.add(&input).is_err());
        assert!(
            add_borrowed_rollup(&mut borrowed, &input.row, input.sequence, input.ordinal).is_err()
        );
        assert_eq!(
            serde_json::to_vec(&borrowed)?,
            serde_json::to_vec(&expected)?
        );
    }
    Ok(())
}

#[test]
fn linear_resize_split_failure_and_unlocked_release() -> Result<()> {
    let budget = RawMemoryBudget::new(100, 1)?;
    assert_eq!(
        budget.reserve(101).unwrap_err(),
        RawReservationError::TooLarge
    );
    let mut a = budget.reserve(70)?;
    let b = budget.reserve(30)?;
    assert_eq!(
        budget.reserve(1).unwrap_err(),
        RawReservationError::Pressure
    );
    assert_eq!(a.resize(71).unwrap_err(), RawReservationError::Pressure);
    assert_eq!(a.bytes(), 70);
    assert_eq!(budget.status().reserved_bytes, 100);
    assert_eq!(
        a.resize(usize::MAX).unwrap_err(),
        RawReservationError::TooLarge
    );
    let split = a.split(20)?;
    assert_eq!(a.bytes(), 50);
    assert_eq!(budget.status().reserved_bytes, 100);
    assert!(a.split(51).is_err());
    assert!(a.shrink(51).is_err());
    struct ReadOnWake(RawMemoryBudget, std::sync::atomic::AtomicUsize);
    impl std::task::Wake for ReadOnWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            // This synchronously takes the accounting lock: notification under
            // that lock would deadlock, not merely lose a diagnostic assertion.
            self.1
                .store(self.0.status().reserved_bytes, Ordering::SeqCst);
        }
    }
    let wake = Arc::new(ReadOnWake(
        budget.clone(),
        std::sync::atomic::AtomicUsize::new(usize::MAX),
    ));
    let waker = std::task::Waker::from(wake.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    let mut notified = Box::pin(budget.released().notified());
    assert!(std::future::Future::poll(notified.as_mut(), &mut cx).is_pending());
    a.shrink(40)?;
    assert_eq!(wake.1.load(Ordering::SeqCst), 90);
    assert!(std::future::Future::poll(notified.as_mut(), &mut cx).is_ready());
    drop(notified);
    let mut notified = Box::pin(budget.released().notified());
    assert!(std::future::Future::poll(notified.as_mut(), &mut cx).is_pending());
    drop(split);
    assert_eq!(wake.1.load(Ordering::SeqCst), 70);
    drop((a, b));
    assert_eq!(budget.status().reserved_bytes, 0);
    assert_eq!(budget.status().peak_bytes, 100);
    Ok(())
}

#[test]
fn full_admitted_burst_moves_payloads_without_second_pool_reservation() -> Result<()> {
    for journal in [false, true] {
        let requests: Vec<_> = (0..3).map(|id| request(&format!("id{id}"), 256)).collect();
        let pointers: Vec<_> = requests
            .iter()
            .flat_map(|r| r.rows.iter())
            .map(|r| {
                let (key, value) = r.tags.first_key_value().unwrap();
                (
                    r.tenant.as_ptr() as usize,
                    r.series.as_ptr() as usize,
                    key.as_ptr() as usize,
                    value.as_ptr() as usize,
                )
            })
            .collect();
        let former_peak: usize = requests
            .iter()
            .map(|r| {
                r.admission_bytes().unwrap()
                    + row_charge(r.rows.iter().map(Row::estimated_bytes).sum())
            })
            .sum();
        let admitted: Vec<_> = requests
            .into_iter()
            .map(AdmittedWrite::new)
            .collect::<Result<_>>()?;
        let envelope: usize = admitted.iter().map(AdmittedWrite::raw_bytes).sum();
        assert!(
            envelope < former_peak,
            "new envelope {envelope} must fit below old double-copy peak {former_peak}"
        );
        let (dir, db) = open(Config {
            raw_memory_max_bytes: envelope,
            segmented_journal: journal,
            ..Default::default()
        })?;
        let prepared = admitted
            .into_iter()
            .map(|mut input| {
                input.reserve(&db.inner.raw_memory)?;
                input.prepare(&db.inner.config)
            })
            .collect();
        assert_eq!(db.inner.raw_memory.status().reserved_bytes, envelope);
        let epoch = db.prepare_epoch(prepared);
        assert!(epoch.results.iter().all(Result::is_ok));
        assert_eq!(db.inner.raw_memory.status().peak_bytes, envelope);
        let private = &epoch.publication.as_ref().unwrap().pending.delta.batches;
        let moved: Vec<_> = private
            .iter()
            .flat_map(|(_, b)| b.rows.iter())
            .map(|r| {
                let (key, value) = r.row.tags.first_key_value().unwrap();
                (
                    r.row.tenant.as_ptr() as usize,
                    r.row.series.as_ptr() as usize,
                    key.as_ptr() as usize,
                    value.as_ptr() as usize,
                )
            })
            .collect();
        assert_eq!(moved, pointers);
        let results = epoch.publish();
        assert!(results.iter().all(Result::is_ok));
        let rows: Vec<_> = db.lock()?.hot["metrics"]
            .iter()
            .flat_map(|b| b.rows.iter().cloned())
            .collect();
        assert_eq!(rows.len(), 768);
        assert_eq!(
            rows.iter().map(|r| r.ordinal).collect::<Vec<_>>(),
            (0..768).collect::<Vec<_>>()
        );
        assert!(
            rows.iter()
                .all(|r| r.row.value.to_bits() == (-0.0f64).to_bits())
        );
        let config = db.inner.config.clone();
        drop(db);
        let reopened = Database::open(dir.path(), config)?;
        let replayed: Vec<_> = reopened.lock()?.hot["metrics"]
            .iter()
            .flat_map(|b| b.rows.iter().cloned())
            .collect();
        assert_eq!(replayed, rows);
    }
    Ok(())
}

#[test]
fn moved_excess_string_and_input_vector_capacities_remain_charged() -> Result<()> {
    let mut input = request("capacity", 1);
    input.rows.reserve_exact(10_000);
    let row = &mut input.rows[0];
    row.tenant.reserve_exact(32_768);
    row.series.reserve_exact(65_536);
    let mut key = String::with_capacity(131_072);
    key.push('k');
    let mut value = String::with_capacity(262_144);
    value.push('v');
    row.tags = BTreeMap::from([(key, value)]);
    let (key, value) = row.tags.first_key_value().unwrap();
    let pointers = (
        row.tenant.as_ptr(),
        row.series.as_ptr(),
        key.as_ptr(),
        value.as_ptr(),
    );
    let payload_capacity =
        row.tenant.capacity() + row.series.capacity() + key.capacity() + value.capacity();
    let excess = payload_capacity - row.tenant.len() - row.series.len() - key.len() - value.len();
    let retained = row_charge(row.estimated_bytes()) + excess;
    let input_vector_bytes = input.rows.capacity() * std::mem::size_of::<Row>();
    let mut admitted = AdmittedWrite::new(input)?;
    let envelope = admitted.raw_bytes();
    assert!(envelope >= retained + input_vector_bytes);
    let (_dir, db) = open(Config {
        raw_memory_max_bytes: envelope,
        ..Default::default()
    })?;
    admitted.reserve(&db.inner.raw_memory)?;
    let prepared = admitted.prepare(&db.inner.config)?;
    let epoch = db.prepare_epoch(vec![Ok(prepared)]);
    assert!(epoch.results[0].is_ok());
    assert_eq!(db.inner.raw_memory.status().reserved_bytes, envelope);
    assert_eq!(db.inner.raw_memory.status().live_bytes, retained);
    assert!(epoch.publish()[0].is_ok());
    let s = db.lock()?;
    let batch = &s.hot["metrics"][0].rows;
    assert_eq!(
        batch.test_capacity(),
        1,
        "excess caller vector must not survive its envelope"
    );
    let row = &batch[0].row;
    let (key, value) = row.tags.first_key_value().unwrap();
    assert_eq!(
        (
            row.tenant.as_ptr(),
            row.series.as_ptr(),
            key.as_ptr(),
            value.as_ptr()
        ),
        pointers
    );
    assert_eq!(
        row.tenant.capacity() + row.series.capacity() + key.capacity() + value.capacity(),
        payload_capacity
    );
    assert_eq!(db.inner.raw_memory.status().reserved_bytes, retained);
    assert!(retained >= payload_capacity + std::mem::size_of::<StoredRow>());
    Ok(())
}

#[test]
fn checkpoint_pressure_materializes_only_once_after_retry() -> Result<()> {
    let (_dir, db) = open(Config {
        hot_max_rows: 2,
        checkpoint_frozen_prefix: true,
        ..Default::default()
    })?;
    db.write_group(vec![request("seed", 2)]).pop().unwrap()?;
    let before = write_input::MATERIALIZATION_PASSES.with(std::cell::Cell::get);
    db.write_group(vec![request("next", 2)]).pop().unwrap()?;
    assert_eq!(
        write_input::MATERIALIZATION_PASSES.with(std::cell::Cell::get) - before,
        1
    );
    let s = db.lock()?;
    assert_eq!(s.catalog.checkpoint_sequence, 2);
    assert_eq!(
        s.hot["metrics"].iter().map(|b| b.rows.len()).sum::<usize>(),
        2
    );
    Ok(())
}

#[test]
fn scan_actual_construction_at_default_pool_and_16mib_output_cap() -> Result<()> {
    let (_dir, db) = open(Config {
        query_max_output_bytes: 16 * 1024 * 1024,
        decoded_cache_bytes: 0,
        ..Default::default()
    })?;
    assert!(db.scan("metrics", None, None, None, None)?.is_empty());
    assert_eq!(db.inner.raw_memory.status().reserved_bytes, 0);
    db.write_group(vec![request("one", 1)]).pop().unwrap()?;
    let baseline = db.inner.raw_memory.status().reserved_bytes;
    assert_eq!(db.scan("metrics", None, None, None, None)?.len(), 1);
    assert_eq!(db.inner.raw_memory.status().reserved_bytes, baseline);
    db.checkpoint()?;
    let baseline = db.inner.raw_memory.status().reserved_bytes;
    assert_eq!(db.scan("metrics", None, None, None, None)?.len(), 1);
    assert_eq!(db.inner.raw_memory.status().reserved_bytes, baseline);
    Ok(())
}

#[test]
fn raw_pressure_after_partial_hot_or_cold_scan_refunds_every_private_credit() -> Result<()> {
    for cold in [false, true] {
        let (_dir, db) = open(Config {
            decoded_cache_bytes: 0,
            ..Default::default()
        })?;
        db.write_group(vec![request("two", 2)]).pop().unwrap()?;
        if cold {
            db.checkpoint()?;
        }
        let before = db.inner.raw_memory.status().reserved_bytes;
        let one = row_charge(row().estimated_bytes());
        let decoded = if cold {
            row_charge(2 * row().estimated_bytes())
        } else {
            0
        };
        let held = db
            .inner
            .raw_memory
            .reserve(db.inner.config.raw_memory_max_bytes - before - one - decoded)?;
        let baseline = db.inner.raw_memory.status().reserved_bytes;
        let error = db.scan("metrics", None, None, None, None).unwrap_err();
        assert_eq!(error.downcast_ref(), Some(&RawReservationError::Pressure));
        assert_eq!(db.inner.raw_memory.status().reserved_bytes, baseline);
        drop(held);
        assert_eq!(db.inner.raw_memory.status().reserved_bytes, before);
        assert_eq!(db.scan("metrics", None, None, None, None)?.len(), 2);
    }
    Ok(())
}

#[test]
fn failed_scan_discards_output_and_returns_all_credit() -> Result<()> {
    for cold in [false, true] {
        let (_dir, db) = open(Config {
            query_max_output_bytes: row().estimated_bytes(),
            decoded_cache_bytes: 0,
            ..Default::default()
        })?;
        db.write_group(vec![request("two", 2)]).pop().unwrap()?;
        if cold {
            db.checkpoint()?;
        }
        let baseline = db.inner.raw_memory.status().reserved_bytes;
        assert!(
            db.scan("metrics", None, None, None, None)
                .unwrap_err()
                .to_string()
                .contains("scan output budget")
        );
        assert_eq!(db.inner.raw_memory.status().reserved_bytes, baseline);
    }
    Ok(())
}
