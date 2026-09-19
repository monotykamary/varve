use super::*;

// Pure domain state only: no Database, files, workers, sockets or external engine.
fn state(config: &Config, timed: bool) -> Result<State> {
    let catalog = Manifest {
        format_version: FORMAT_VERSION,
        database_id: "00000000-0000-4000-8000-000000000001".into(),
        checkpoint_sequence: 1,
        segmented_journal: false,
        tables: BTreeMap::from([(
            "metrics".into(),
            empty_table(
                TableConfig {
                    window_us: 100,
                    rollup_widths_us: vec![10, 20],
                    idempotency_window_us: timed.then_some(100),
                    ..Default::default()
                },
                1,
            ),
        )]),
        continuous_aggregates: BTreeMap::new(),
        jobs: BTreeMap::new(),
        control_history: Vec::new(),
    };
    Ok(State {
        raw_memory: RawMemoryBudget::new(
            config.raw_memory_max_bytes,
            config.raw_working_max_bytes,
        )?,
        sequence: 1,
        replaying: false,
        generation: 0,
        root_epoch: 0,
        control_epoch: 0,
        rollup_indexes: build_rollup_indexes(&catalog, config)?,
        derived_working: Arc::new(AtomicUsize::new(0)),
        control_root_bytes: encode_manifest(&catalog)?.len(),
        derived_resident_bytes: derived_root::resident_bytes(&catalog, true),
        derived_accounting: DerivedAccounting::build(&catalog, config)?,
        metadata_bytes: logical_metadata_bytes(&catalog)?,
        catalog,
        derived_refs: None,
        raw_stamps: BTreeMap::new(),
        hot: BTreeMap::new(),
        hot_epochs: Vec::new(),
        hot_bytes: 0,
        wal_bytes: 0,
        fenced: None,
        decoded: BTreeMap::new(),
        cache_clock: 0,
        first_hot_us: None,
        last_ship_us: None,
        remote_token: None,
        remote_owner: "offline".into(),
        remote_head: None,
        remote_segment_ids: BTreeSet::new(),
        remote_vacuum_pending: false,
        last_maintenance_error: None,
        idempotency_floors: BTreeMap::new(),
        job_runtime: BTreeMap::new(),
    })
}

fn input(id: &str, values: &[f64], config: &Config) -> Result<PreparedWrite> {
    AdmittedWrite::new(WriteRequest {
        table: "metrics".into(),
        request_id: id.into(),
        now_us: 20,
        rows: values
            .iter()
            .enumerate()
            .map(|(i, &value)| Row {
                timestamp_us: 2 + (i % 2) as i64,
                tenant: "東京".into(),
                series: "cpu\"\\".into(),
                value,
                tags: BTreeMap::from([("escaped".into(), "line\nvalue".into())]),
            })
            .collect(),
    })?
    .prepare(config)
}

#[test]
fn offline_direct_undo_uses_reserved_derived_space_not_group_only_ceiling() -> Result<()> {
    let config = Config {
        derived_pages: true,
        metadata_max_bytes: 64 * 1024,
        ..Default::default()
    };
    let s = state(&config, false)?;
    let request = WriteRequest {
        table: "metrics".into(),
        request_id: "many-series".into(),
        now_us: 20,
        rows: (0..50)
            .map(|i| Row {
                timestamp_us: 2,
                tenant: "t".into(),
                series: format!("series-{i}"),
                value: 1.0,
                tags: BTreeMap::new(),
            })
            .collect(),
    };
    let input = AdmittedWrite::new(request)?.prepare(&config)?;
    let prepared = prepare_live_append(&s, &input, 2, 0, None, &config, None)?;
    assert!(
        prepared.working_bytes() > config.metadata_max_bytes,
        "witness must exceed the group-only ceiling"
    );
    assert!(prepared.working_bytes() <= prepared.derived.working.bytes);
    assert_eq!(
        WriteMode::Single.admit_private(0, &prepared, &config)?,
        prepared.working_bytes()
    );
    assert!(
        WriteMode::Group
            .admit_private(0, &prepared, &config)
            .is_err()
    );
    assert!(
        WriteMode::Single
            .admit_private(1, &prepared, &config)
            .is_err()
    );
    drop(prepared);
    assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
    Ok(())
}

fn rows(s: &State) -> Vec<StoredRow> {
    s.hot
        .values()
        .flatten()
        .flat_map(|batch| batch.rows.iter().cloned())
        .collect()
}

#[test]
fn offline_live_replanning_does_not_repeat_canonical_row_work() -> Result<()> {
    let config = Config::default();
    let mut s = state(&config, false)?;
    let input = input("first", &[-0.0, 1.0, 2.0], &config)?;
    let before = write_input::ROW_IDENTITY_PASSES.with(std::cell::Cell::get);
    for _ in 0..3 {
        let prepared = prepare_live_append(
            &s,
            &input,
            2,
            0,
            Some(GROUP_PROOF_RESERVATION),
            &config,
            None,
        )?;
        drop(prepared);
    }
    assert_eq!(
        write_input::ROW_IDENTITY_PASSES.with(std::cell::Cell::get),
        before
    );
    for _ in 0..2 {
        drop(prepare_group_append(
            &s,
            &input,
            2,
            0,
            Some(GROUP_PROOF_RESERVATION),
            &config,
            None,
        )?);
    }
    assert_eq!(
        write_input::ROW_IDENTITY_PASSES.with(std::cell::Cell::get) - before,
        2
    );
    // A proof is not a cache of current table eligibility or resource admission.
    s.catalog.tables.get_mut("metrics").unwrap().cutoff_us = Some(99);
    assert!(prepare_live_append(&s, &input, 2, 0, None, &config, None).is_err());
    s.catalog.tables.get_mut("metrics").unwrap().cutoff_us = None;
    let smaller = Config {
        hot_max_rows: 2,
        ..config.clone()
    };
    assert!(prepare_live_append(&s, &input, 2, 0, None, &smaller, None).is_err());
    assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn offline_shared_live_install_matches_independent_direct_and_group_replay() -> Result<()> {
    for derived_pages in [false, true] {
        let config = Config {
            derived_pages,
            ..Default::default()
        };
        for mode in [WriteMode::Single, WriteMode::Group] {
            let mut live = state(&config, false)?;
            let mut recovered = state(&config, false)?;
            let inputs = if mode.grouped() {
                vec![
                    input("first", &[-0.0, 1.0], &config)?,
                    input("second", &[3.0, 0.30000000000000004], &config)?,
                ]
            } else {
                vec![input("first", &[-0.0, 1.0], &config)?]
            };
            let refs = inputs.iter().collect::<Vec<_>>();
            let encoded = mode.encode(2, &refs)?;
            let fingerprint = encoded.group_fingerprint();
            let mut ordinal = 0;
            for input in &inputs {
                let prepared = prepare_live_append(
                    &live,
                    input,
                    2,
                    ordinal,
                    fingerprint.as_deref(),
                    &config,
                    None,
                )?;
                ordinal += input.rows.len();
                drop(apply_group_append(&mut live, prepared));
            }
            live.sequence = 2;
            replay(&mut recovered, wal::decode(encoded.as_bytes())?, &config)?;
            assert_eq!(
                serde_json::to_vec(&live.catalog)?,
                serde_json::to_vec(&recovered.catalog)?
            );
            assert_eq!(
                serde_json::to_vec(&rows(&live))?,
                serde_json::to_vec(&rows(&recovered))?
            );
            for (a, b) in rows(&live).iter().zip(rows(&recovered)) {
                assert_eq!(a.row.value.to_bits(), b.row.value.to_bits());
            }
            assert_eq!(live.hot_bytes, recovered.hot_bytes);
            assert_eq!(live.metadata_bytes, recovered.metadata_bytes);
            assert_eq!(
                live.derived_resident_bytes,
                recovered.derived_resident_bytes
            );
            assert_eq!(live.derived_working.load(Ordering::SeqCst), 0);
            assert_eq!(recovered.derived_working.load(Ordering::SeqCst), 0);
        }
    }
    Ok(())
}

#[test]
fn offline_undo_restores_state_and_recovery_rejects_forged_inner_digest() -> Result<()> {
    for derived_pages in [false, true] {
        let config = Config {
            derived_pages,
            ..Default::default()
        };
        let mut s = state(&config, false)?;
        let before = serde_json::to_vec(&s.catalog)?;
        let bytes = s.metadata_bytes;
        let input = input("first", &[1.0, 2.0], &config)?;
        let prepared = prepare_live_append(
            &s,
            &input,
            2,
            0,
            Some(GROUP_PROOF_RESERVATION),
            &config,
            None,
        )?;
        let undo = apply_group_append(&mut s, prepared);
        assert_eq!(rows(&s).len(), 2);
        undo_group_append(&mut s, undo);
        assert_eq!(serde_json::to_vec(&s.catalog)?, before);
        assert!(s.hot.is_empty());
        assert_eq!(s.hot_bytes, 0);
        assert_eq!(s.metadata_bytes, bytes);
        assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
        let bytes = WriteMode::Group.encode(2, &[&input])?;
        let mut forged = wal::decode(bytes.as_bytes())?;
        let wal::Operation::AppendGroup { items } = &mut forged.operation else {
            unreachable!()
        };
        items[0].rows[0].value = 9.0;
        // A valid outer frame checksum must not bless a forged inner identity.
        let forged = wal::decode(&wal::encode(&forged)?)?;
        assert!(replay(&mut s, forged, &config).is_err());
        assert!(s.hot.is_empty());
    }
    Ok(())
}

#[test]
fn offline_ordered_duplicates_and_retention_clocks_use_current_state() -> Result<()> {
    let config = Config::default();
    let mut s = state(&config, true)?;
    let first = input("v1:20:first", &[1.0], &config)?;
    assert!(group_retry(&mut s, &first, &first.digest)?.is_none());
    let prepared = prepare_live_append(&s, &first, 2, 0, None, &config, None)?;
    drop(apply_group_append(&mut s, prepared));
    s.sequence = 2;
    let same = input("v1:20:first", &[1.0], &config)?;
    assert!(group_retry(&mut s, &same, &same.digest)?.unwrap().duplicate);
    let different = input("v1:20:first", &[2.0], &config)?;
    assert!(group_retry(&mut s, &different, &different.digest).is_err());
    s.idempotency_floors.insert("metrics".into(), 21);
    assert!(group_retry(&mut s, &same, &same.digest).is_err());
    assert_eq!(rows(&s).len(), 1);
    Ok(())
}

fn committed_image(s: &State) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&(
        &s.catalog,
        rows(s),
        &s.idempotency_floors,
        format!("{:?}", s.rollup_indexes),
        format!("{:?}", s.derived_accounting.tables),
        &s.raw_stamps,
        s.hot_bytes,
        s.metadata_bytes,
        s.derived_resident_bytes,
        s.derived_accounting.root_bound,
        s.derived_accounting.encoded_bound,
        s.derived_accounting.oversized_sets,
        s.sequence,
        s.generation,
    ))?)
}

#[test]
fn private_overlay_mixed_epoch_is_invisible_discardable_and_replay_exact() -> Result<()> {
    for derived_pages in [false, true] {
        let config = Config {
            derived_pages,
            ..Config::default()
        };
        let make_state = || -> Result<State> {
            let mut s = state(&config, true)?;
            s.catalog
                .tables
                .get_mut("metrics")
                .unwrap()
                .config
                .late_after_us = Some(50);
            s.metadata_bytes = logical_metadata_bytes(&s.catalog)?;
            s.derived_accounting = DerivedAccounting::build(&s.catalog, &config)?;
            Ok(s)
        };
        let make = |id: &str, now_us, timestamp_us, values: &[f64]| -> Result<PreparedWrite> {
            AdmittedWrite::new(WriteRequest {
                table: "metrics".into(),
                request_id: id.into(),
                now_us,
                rows: values
                    .iter()
                    .map(|&value| Row {
                        timestamp_us,
                        tenant: "東京".into(),
                        series: "cpu".into(),
                        value,
                        tags: BTreeMap::from([("quote".into(), "\"\n".into())]),
                    })
                    .collect(),
            })?
            .prepare(&config)
        };
        let seed = make("v1:180:seed", 180, 150, &[1.0])?;
        let seed_frame = WriteMode::Group.encode(2, &[&seed])?;
        let mut live = make_state()?;
        let mut recovered = make_state()?;
        for s in [&mut live, &mut recovered] {
            replay(s, wal::decode(seed_frame.as_bytes())?, &config)?;
            s.idempotency_floors.insert("metrics".into(), 80);
        }
        let inputs = [
            make("v1:200:a", 200, 200, &[-0.0, 0.30000000000000004])?,
            make("v1:200:a", 210, 200, &[-0.0, 0.30000000000000004])?,
            make("v1:200:a", 230, 200, &[99.0])?,
            make("v1:280:late", 280, 0, &[99.0])?,
            make("v1:180:seed", 270, 150, &[1.0])?,
            make("v1:240:b", 240, 200, &[7.0])?,
        ];
        let before = committed_image(&live)?;
        let raw_before = live.raw_memory.status().reserved_bytes;
        let derived_before = live.derived_working.load(Ordering::SeqCst);
        for install in [false, true] {
            let mut overlay = AppendOverlay::new(&live);
            for (i, input) in inputs.iter().enumerate() {
                let result = if i == 4 {
                    let receipt = overlay.retry(input)?.expect("durable duplicate");
                    overlay.durable_duplicate(input);
                    Ok(receipt)
                } else {
                    overlay.prepare_group_item(input, 3, &config, None)
                };
                overlay.outcomes.push(result);
                assert_eq!(
                    overlay.delta.floors["metrics"],
                    [100, 110, 110, 180, 180, 180][i]
                );
                assert_eq!(
                    committed_image(&live)?,
                    before,
                    "preparation changed baseline at input {i}"
                );
            }
            assert_eq!(
                overlay.delta.durable_floors,
                BTreeMap::from([("metrics".into(), 170)])
            );
            let categories: Vec<_> = overlay
                .outcomes
                .iter()
                .map(|r| match r {
                    Ok(r) => (r.sequence, if r.duplicate { "duplicate" } else { "new" }),
                    Err(e) if e.to_string().contains("conflicts") => (0, "conflict"),
                    Err(e) if e.to_string().contains("lateness") => (0, "late"),
                    Err(e) => panic!("unexpected result {e:#}"),
                })
                .collect();
            assert_eq!(
                categories,
                [
                    (3, "new"),
                    (3, "duplicate"),
                    (0, "conflict"),
                    (0, "late"),
                    (2, "duplicate"),
                    (3, "new")
                ]
            );
            assert_eq!(
                overlay
                    .items
                    .iter()
                    .map(|i| i.request_id.as_str())
                    .collect::<Vec<_>>(),
                ["v1:200:a", "v1:240:b"]
            );
            assert_eq!(overlay.ordinals, [0, 2]);
            assert_eq!(overlay.accepted_rows, 3);
            assert_eq!(overlay.delta.tables.len(), 1);
            assert_eq!(overlay.delta.tables["metrics"].receipts.len(), 2);
            assert_eq!(overlay.delta.tables["metrics"].rollups.len(), 2);
            assert!(
                overlay.delta.batches.is_empty(),
                "borrow-only preflight must not materialize"
            );
            assert_eq!(
                overlay
                    .items
                    .iter()
                    .zip(&overlay.ordinals)
                    .flat_map(|(item, start)| *start..*start + item.rows.len())
                    .collect::<Vec<_>>(),
                [0, 1, 2]
            );
            check_recovery_budget_view(&config, &overlay.view())?;
            let encoded = WriteMode::Group.encode(3, &overlay.items)?;
            let expected = wal::Record::new(
                3,
                wal::Operation::AppendGroup {
                    items: vec![(*inputs[0]).clone(), (*inputs[5]).clone()],
                },
            );
            assert_eq!(encoded.as_bytes(), wal::encode(&expected)?);
            overlay.fingerprint(3, encoded.group_fingerprint().as_deref())?;
            assert_eq!(
                live.raw_memory.status().reserved_bytes,
                raw_before,
                "borrow-only plans acquire no second raw lease"
            );
            let projected_metadata = overlay.delta.metadata_bytes;
            let mut pending = overlay.into_pending();
            assert_eq!(
                committed_image(&live)?,
                before,
                "descriptor construction changed baseline"
            );
            assert!(live.derived_working.load(Ordering::SeqCst) > derived_before);
            if !install {
                drop(pending);
                assert_eq!(live.raw_memory.status().reserved_bytes, raw_before);
                assert_eq!(live.derived_working.load(Ordering::SeqCst), derived_before);
                assert_eq!(committed_image(&live)?, before);
                continue;
            }
            for (input, ordinal) in [
                (make("v1:200:a", 200, 200, &[-0.0, 0.30000000000000004])?, 0),
                (make("v1:240:b", 240, 200, &[7.0])?, 2),
            ] {
                let bytes = input.validated().resident_bytes();
                let (rows, _envelope) = input.materialize(&live.raw_memory, 3, ordinal)?;
                pending
                    .delta
                    .batches
                    .push(("metrics".into(), resident_batch(rows, bytes)));
            }
            assert_eq!(
                pending
                    .delta
                    .batches
                    .iter()
                    .flat_map(|(_, b)| b.rows.iter())
                    .map(|r| r.ordinal)
                    .collect::<Vec<_>>(),
                [0, 1, 2]
            );
            pending.install(&mut live);
            live.sequence = 3;
            replay(&mut recovered, wal::decode(encoded.as_bytes())?, &config)?;
            assert_eq!(live.idempotency_floors["metrics"], 180);
            assert_eq!(
                serde_json::to_vec(&live.catalog)?,
                serde_json::to_vec(&recovered.catalog)?
            );
            assert_eq!(
                serde_json::to_vec(&rows(&live))?,
                serde_json::to_vec(&rows(&recovered))?
            );
            for (a, b) in rows(&live).iter().zip(rows(&recovered)) {
                assert_eq!(a.row.value.to_bits(), b.row.value.to_bits());
            }
            assert_eq!(live.hot_bytes, recovered.hot_bytes);
            assert_eq!(live.metadata_bytes, projected_metadata);
            assert_eq!(live.metadata_bytes, recovered.metadata_bytes);
            assert_eq!(live.metadata_bytes, logical_metadata_bytes(&live.catalog)?);
            assert_eq!(
                live.derived_resident_bytes,
                recovered.derived_resident_bytes
            );
            assert_eq!(
                live.derived_accounting.tables,
                recovered.derived_accounting.tables
            );
            assert_eq!(
                format!("{:?}", live.rollup_indexes),
                format!("{:?}", recovered.rollup_indexes)
            );
            for row in live.catalog.tables["metrics"]
                .rollups
                .values()
                .filter(|r| r.bucket_us == 200)
            {
                assert_eq!(row.first.to_bits(), (-0.0_f64).to_bits());
                assert_eq!(row.last, 7.0);
                assert_eq!(
                    (
                        row.first_sequence,
                        row.first_ordinal,
                        row.last_sequence,
                        row.last_ordinal
                    ),
                    (3, 0, 3, 2)
                );
            }
            assert_eq!(live.derived_working.load(Ordering::SeqCst), derived_before);
            assert_eq!(
                live.raw_memory.status().reserved_bytes,
                recovered.raw_memory.status().reserved_bytes
            );
        }
    }
    Ok(())
}
