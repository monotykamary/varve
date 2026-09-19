use super::*;
use std::cell::Cell;
use tempfile::TempDir;

// Independent pre-fusion serializer oracle: allocations and repeated sizing are
// intentional here, never a production fallback.
fn legacy_projection(
    s: &State,
    id: &str,
    receipt: &ReceiptEntry,
    updates: &BTreeMap<String, RollupRow>,
    config: &Config,
) -> Result<DerivedProjection> {
    let table = &s.catalog.tables["metrics"];
    let mut accounting = s.derived_accounting.tables["metrics"].clone();
    accounting.receipts.json_bytes = apply_encoded_delta(
        accounting.receipts.json_bytes,
        serialized_map_upsert_delta(
            &table.receipts,
            id,
            receipt,
            accounting.receipts.entries > 0,
        )?,
    )?;
    accounting.receipts.max_entry_bytes = accounting
        .receipts
        .max_entry_bytes
        .max(serde_json::to_vec(&(id, receipt))?.len());
    let mut resident = s.derived_resident_bytes;
    let mut working = derived::receipt_resident_bytes(id, receipt);
    if !table.receipts.contains_key(id) {
        resident = resident.saturating_add(working);
        accounting.receipts.entries += 1;
    }
    for (key, row) in updates {
        let bytes = derived::rollup_resident_bytes(key, row);
        working = working.saturating_add(bytes.saturating_mul(2));
        accounting.rollups.json_bytes = apply_encoded_delta(
            accounting.rollups.json_bytes,
            serialized_map_upsert_delta(&table.rollups, key, row, accounting.rollups.entries > 0)?,
        )?;
        accounting.rollups.max_entry_bytes = accounting
            .rollups
            .max_entry_bytes
            .max(serde_json::to_vec(&(key, row))?.len());
        if let Some(old) = table.rollups.get(key) {
            resident = resident.saturating_sub(derived::rollup_resident_bytes(key, old));
        } else {
            resident = resident.saturating_add(RollupIndex::entry_bytes(key, row));
            accounting.rollups.entries += 1;
        }
        resident = resident.saturating_add(bytes);
    }
    accounting.bound("metrics", config)?;
    Ok(DerivedProjection {
        accounting,
        resident,
        working,
    })
}

fn row(timestamp_us: i64) -> Row {
    Row {
        timestamp_us,
        tenant: "租户\n".into(),
        series: "cpu\"\\".into(),
        value: -0.0,
        tags: BTreeMap::from([("城市".into(), "東京\t".into())]),
    }
}

fn rollup(sequence: u64, value: f64) -> RollupRow {
    RollupRow {
        width_us: 10,
        bucket_us: -10,
        tenant: "租户\0\n".into(),
        series: "cpu\"\\".into(),
        tags: BTreeMap::from([("城市\0".into(), "東京\t".into())]),
        count: sequence,
        sum: value,
        min: -0.0,
        max: 1e100,
        first: 1e-100,
        last: value,
        first_timestamp_us: i64::MIN,
        last_timestamp_us: i64::MAX,
        first_sequence: 0,
        last_sequence: sequence,
        first_ordinal: 9,
        last_ordinal: 10,
    }
}

fn receipt(sequence: u64) -> ReceiptEntry {
    ReceiptEntry {
        sequence,
        rows: sequence as usize,
        digest: "a".repeat(64),
        issued_us: None,
        group_fingerprint: None,
    }
}

fn database(pages: bool, timed: bool) -> (TempDir, Database, Config) {
    let temp = TempDir::new().unwrap();
    let config = Config {
        derived_pages: pages,
        ..Default::default()
    };
    let db = Database::open(temp.path(), config.clone()).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            shards: 1,
            rollup_widths_us: vec![10],
            idempotency_window_us: timed.then_some(100),
            ..Default::default()
        },
    )
    .unwrap();
    db.checkpoint().unwrap();
    (temp, db, config)
}

#[test]
fn projection_matches_legacy_and_full_maps_with_exact_call_counts() {
    for pages in [false, true] {
        for timed in [false, true] {
            let (_temp, db, config) = database(pages, timed);
            let mut s = db.lock().unwrap();
            for (index, sequence) in [9, 10, 99, 100, u64::MAX, 0].into_iter().enumerate() {
                let id = if timed {
                    format!("v1:100:城市-\\\"-\0-{}", index / 2)
                } else {
                    format!("legacy-\\\"-\0-{}", index / 2)
                };
                let mut receipt = receipt(sequence);
                receipt.issued_us = timed.then_some(100);
                receipt.group_fingerprint = match index / 2 {
                    0 => None,
                    1 => Some(GROUP_PROOF_RESERVATION.into()),
                    _ => Some("f".repeat(64)),
                };
                let value = [-0.0, 1e-100, 1e100, f64::MIN_POSITIVE, -1e100, 0.0][index];
                let updates = BTreeMap::from([
                    ("a-\0-\"-\\-城市".into(), rollup(sequence, value)),
                    ("b-\n-\t-東京".into(), rollup(sequence, -value)),
                    (format!("c-{}", index / 2), rollup(sequence, value)),
                ]);
                let previous = usize::from(s.catalog.tables["metrics"].receipts.contains_key(&id))
                    + updates
                        .keys()
                        .filter(|key| s.catalog.tables["metrics"].rollups.contains_key(*key))
                        .count();
                JSON_COUNT_CALLS.with(|calls| calls.set(0));
                let projection = project_append_accounting(
                    &s,
                    "metrics",
                    &id,
                    &receipt,
                    &updates,
                    Some(&db.inner.metrics),
                )
                .unwrap();
                assert_eq!(
                    JSON_COUNT_CALLS.with(Cell::get),
                    2 * (1 + updates.len()) + previous
                );
                assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
                let metadata = projection.metadata_bytes(&s, sequence).unwrap();
                assert_eq!(
                    metadata,
                    append_metadata_bytes(&s, "metrics", &id, &receipt, &updates, sequence)
                        .unwrap()
                );
                let legacy = legacy_projection(&s, &id, &receipt, &updates, &config).unwrap();
                let mut fused = projection.derived.unwrap();
                fused.accounting.bound("metrics", &config).unwrap();
                assert_eq!(fused.accounting, legacy.accounting);
                assert_eq!(fused.resident, legacy.resident);
                assert_eq!(fused.working, legacy.working);
                let mut full = s.catalog.clone();
                full.checkpoint_sequence = sequence;
                let table = full.tables.get_mut("metrics").unwrap();
                table.receipts.insert(id, receipt);
                table.rollups.extend(updates);
                assert_eq!(metadata, encode_manifest(&full).unwrap().len());
                let mut recomputed = DerivedAccounting::build(&full, &config)
                    .unwrap()
                    .tables
                    .remove("metrics")
                    .unwrap();
                // A shrinking replacement must not lower a historical maximum.
                recomputed.rollups.max_entry_bytes = recomputed.rollups.max_entry_bytes.max(
                    s.derived_accounting.tables["metrics"]
                        .rollups
                        .max_entry_bytes,
                );
                recomputed.receipts.max_entry_bytes = recomputed.receipts.max_entry_bytes.max(
                    s.derived_accounting.tables["metrics"]
                        .receipts
                        .max_entry_bytes,
                );
                recomputed.bound("metrics", &config).unwrap();
                assert_eq!(fused.accounting, recomputed);
                assert_eq!(fused.resident, derived_root::resident_bytes(&full, true));
                s.catalog = full;
                s.metadata_bytes = metadata;
                s.derived_resident_bytes = fused.resident;
                s.derived_accounting
                    .replace("metrics".into(), fused.accounting);
            }
            assert_eq!(db.performance().phases["append_accounting"].count, 6);
        }
    }
}

struct Counted<'a, T> {
    calls: &'a Cell<usize>,
    value: T,
}
impl<T: Serialize> Serialize for Counted<'_, T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.calls.set(self.calls.get() + 1);
        self.value.serialize(serializer)
    }
}

#[test]
fn touched_key_new_and_previous_serializers_each_run_once() {
    for replacing in [false, true] {
        let key_calls = Cell::new(0);
        let new_calls = Cell::new(0);
        let old_calls = Cell::new(0);
        let key = Counted {
            calls: &key_calls,
            value: "\0城市\"\\\n",
        };
        let new = Counted {
            calls: &new_calls,
            value: -0.0,
        };
        let old = Counted {
            calls: &old_calls,
            value: 1e100,
        };
        let encoded = encoded_upsert(&key, &new, replacing.then_some(&old)).unwrap();
        assert_eq!(
            (key_calls.get(), new_calls.get(), old_calls.get()),
            (1, 1, usize::from(replacing))
        );
        assert_eq!(
            encoded.entry_bytes,
            serde_json::to_vec(&(key.value, new.value)).unwrap().len()
        );
        let map = if replacing {
            BTreeMap::from([(key.value.to_owned(), old.value)])
        } else {
            BTreeMap::new()
        };
        assert_eq!(
            encoded.delta(replacing),
            serialized_map_upsert_delta(&map, key.value, &new.value, replacing).unwrap()
        );
    }
}

#[test]
fn empty_updates_and_saturating_accounting_preserve_legacy_math() {
    let (_temp, db, config) = database(false, false);
    let mut s = db.lock().unwrap();
    let receipt = receipt(10);
    for resident in [0, usize::MAX - 1, usize::MAX] {
        s.derived_resident_bytes = resident;
        for updates in [
            BTreeMap::new(),
            BTreeMap::from([("key".into(), rollup(10, -0.0))]),
        ] {
            let fused =
                project_append_accounting(&s, "metrics", "id", &receipt, &updates, None).unwrap();
            let mut fused = fused.derived.unwrap();
            fused.accounting.bound("metrics", &config).unwrap();
            let legacy = legacy_projection(&s, "id", &receipt, &updates, &config).unwrap();
            assert_eq!(fused.accounting, legacy.accounting);
            assert_eq!(fused.resident, legacy.resident);
            assert_eq!(fused.working, legacy.working);
        }
    }
}

#[test]
fn append_accounting_phase_covers_direct_group_and_open_replay_not_retries() {
    let (temp, db, config) = database(false, false);
    db.write("metrics", "direct", vec![row(1)], 1).unwrap();
    assert_eq!(db.performance().phases["append_accounting"].count, 1);
    assert!(
        db.write("metrics", "direct", vec![row(1)], 1)
            .unwrap()
            .duplicate
    );
    assert_eq!(db.performance().phases["append_accounting"].count, 1);
    let requests = ["group-a", "group-b"]
        .map(|id| WriteRequest {
            table: "metrics".into(),
            request_id: id.into(),
            rows: vec![row(1)],
            now_us: 1,
        })
        .to_vec();
    assert!(
        db.write_group(requests)
            .into_iter()
            .all(|result| result.is_ok())
    );
    // One fused projection per new input; durable retries do no accounting.
    assert_eq!(db.performance().phases["append_accounting"].count, 3);
    drop(db);
    let db = Database::open(temp.path(), config).unwrap();
    let performance = db.performance();
    assert_eq!(performance.phases["append_accounting"].count, 3);
    assert_eq!(performance.phases["group_prepare"].count, 0);
    assert_eq!(performance.phases["wal_encode"].count, 0);
    assert!(
        performance
            .prometheus()
            .contains("varve_phase_duration_seconds_count{phase=\"append_accounting\"} 3\n")
    );
    let s = db.lock().unwrap();
    assert_eq!(s.metadata_bytes, encode_manifest(&s.catalog).unwrap().len());
    assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
}

fn item(id: &str) -> wal::AppendItem {
    let rows = vec![row(1)];
    wal::AppendItem {
        table: "metrics".into(),
        request_id: id.into(),
        digest: blake3::hash(&serde_json::to_vec(&rows).unwrap())
            .to_hex()
            .to_string(),
        rows,
        now_us: Some(1),
    }
}

#[test]
fn metadata_headroom_precedes_deferred_derived_errors() {
    let (_temp, db, mut config) = database(false, false);
    let mut s = db.lock().unwrap();
    let item = item("id");
    let sequence = next_sequence(&s).unwrap();
    let prepared = prepare_group_append(&s, &item, sequence, 0, None, &config, None).unwrap();
    let required = append_metadata_bytes(
        &s,
        "metrics",
        "id",
        &prepared.receipt,
        &prepared.updates,
        sequence,
    )
    .unwrap()
        + 512;
    drop(prepared);
    // Corrupt cached accounting must still fail, but not ahead of the existing
    // metadata headroom classification that enables the bounded checkpoint retry.
    s.derived_accounting.tables.remove("metrics");
    config.metadata_max_bytes = required - 1;
    let error = prepare_group_append(&s, &item, sequence, 0, None, &config, None)
        .err()
        .unwrap();
    assert!(error.is::<CheckpointHeadroom>(), "{error:#}");
    config.metadata_max_bytes = required;
    let error = prepare_group_append(&s, &item, sequence, 0, None, &config, None)
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("missing derived accounting"));
    assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
    assert_eq!(hot_count(&s), 0);
}

#[test]
fn derived_limits_accept_exact_bytes_reject_one_less_and_release_charges() {
    for gate in [
        "resident/working",
        "checkpoint working",
        "page target",
        "encoded",
        "recovery working",
        "control",
    ] {
        let pages = !matches!(gate, "resident/working" | "checkpoint working");
        let (_temp, db, mut config) = database(pages, false);
        let mut s = db.lock().unwrap();
        let receipt = receipt(10);
        // A legal minimum-sized page boundary can be hit with a wide entry.
        let mut wide = rollup(10, 1e100);
        if gate == "page target" {
            wide.tenant = "t".repeat(4_096);
        }
        let updates = BTreeMap::from([("key".into(), wide)]);
        if gate == "checkpoint working" {
            s.derived_resident_bytes += 10_000;
        }
        let baseline = legacy_projection(&s, "id", &receipt, &updates, &config).unwrap();
        let mut external = 0;
        match gate {
            "resident/working" => {
                external = baseline.resident * 2;
                s.derived_working.store(external, Ordering::SeqCst);
                config.derived_max_bytes = baseline.resident + baseline.working + external;
            }
            "checkpoint working" => config.derived_max_bytes = baseline.resident * 2,
            "page target" => {
                config.derived_page_bytes = [
                    baseline.accounting.rollups.max_entry_bytes
                        + derived::page_overhead("metrics", derived::PageKind::Rollup).unwrap(),
                    baseline.accounting.receipts.max_entry_bytes
                        + derived::page_overhead("metrics", derived::PageKind::Receipt).unwrap(),
                ]
                .into_iter()
                .max()
                .unwrap();
            }
            "encoded" => {
                // Isolate the global encoded bound, including other page sets.
                s.derived_accounting.encoded_bound = config.derived_max_bytes
                    + s.derived_accounting.tables["metrics"].encoded_bound
                    - baseline.accounting.encoded_bound;
            }
            "recovery working" => {
                config.derived_max_bytes = baseline.resident + config.derived_page_bytes * 64
            }
            "control" => {
                config.metadata_max_bytes = s.derived_accounting.control_base
                    + s.derived_accounting.root_bound
                    - s.derived_accounting.tables["metrics"].root_bound
                    + baseline.accounting.root_bound
                    + 20
                    + 512
            }
            _ => unreachable!(),
        }
        for short_by in [0, 1] {
            let mut limit = config.clone();
            match gate {
                "page target" => limit.derived_page_bytes -= short_by,
                "control" => limit.metadata_max_bytes -= short_by,
                _ => limit.derived_max_bytes -= short_by,
            }
            let fused =
                project_append_accounting(&s, "metrics", "id", &receipt, &updates, None).unwrap();
            let legacy = legacy_projection(&s, "id", &receipt, &updates, &limit);
            let mut results = Vec::new();
            for projected in [fused.derived, legacy] {
                let result = check_derived_append(&s, &limit, "metrics", projected, 1);
                if short_by == 0 {
                    let accepted = result.unwrap_or_else(|error| panic!("{gate}: {error:#}"));
                    assert_eq!(
                        s.derived_working.load(Ordering::SeqCst),
                        external + baseline.working
                    );
                    drop(accepted);
                    results.push(None);
                } else {
                    let error = result
                        .err()
                        .unwrap_or_else(|| panic!("{gate} admitted one byte below limit"));
                    if gate == "control" {
                        assert!(error.is::<CheckpointHeadroom>());
                    } else {
                        assert!(format!("{error:#}").contains(gate), "{gate}: {error:#}");
                    }
                    results.push(Some(format!("{error:#}")));
                }
                assert_eq!(s.derived_working.load(Ordering::SeqCst), external);
            }
            assert_eq!(results[0], results[1], "{gate}");
        }
        s.derived_working.store(0, Ordering::SeqCst);
    }
}

#[test]
fn direct_reprojects_after_wal_pressure_prunes_at_same_sequence() {
    for (pages, frozen) in [(false, false), (true, false), (false, true), (true, true)] {
        let (temp, db, mut config) = database(pages, true);
        drop(db);
        config.checkpoint_frozen_prefix = frozen;
        let db = Database::open(temp.path(), config.clone()).unwrap();
        db.write("metrics", "v1:10:old", vec![row(10)], 10).unwrap();
        db.write("metrics", "v1:90:recent", vec![row(90)], 90)
            .unwrap();
        db.checkpoint().unwrap();
        let (sequence, root_epoch, timers) = {
            let mut s = db.lock().unwrap();
            assert_eq!(s.sequence, s.catalog.checkpoint_sequence);
            // Deterministically force only the exact WAL-pressure boundary;
            // no hot/receipt/metadata limit is reached before it.
            s.wal_bytes = config.wal_max_bytes;
            (
                s.sequence,
                s.root_epoch,
                db.performance().phases["append_accounting"].count,
            )
        };
        let admission_passes = write_input::ADMISSION_PASSES.with(Cell::get);
        let identity_passes = write_input::ROW_IDENTITY_PASSES.with(Cell::get);
        db.write("metrics", "v1:150:next", vec![row(150)], 150)
            .unwrap();
        assert_eq!(
            write_input::ADMISSION_PASSES.with(Cell::get),
            admission_passes + 1
        );
        assert_eq!(
            write_input::ROW_IDENTITY_PASSES.with(Cell::get),
            identity_passes + 1
        );
        {
            let s = db.lock().unwrap();
            assert_eq!(s.sequence, sequence + 1);
            assert_eq!(s.catalog.checkpoint_sequence, sequence);
            assert_eq!(s.root_epoch, root_epoch + 1);
            assert_eq!(s.catalog.tables["metrics"].idempotency_floor_us, Some(50));
            assert!(
                !s.catalog.tables["metrics"]
                    .receipts
                    .contains_key("v1:10:old")
            );
            assert_eq!(s.catalog.tables["metrics"].receipts.len(), 2);
            assert_eq!(s.metadata_bytes, encode_manifest(&s.catalog).unwrap().len());
            assert_eq!(
                s.derived_resident_bytes,
                derived_root::resident_bytes(&s.catalog, true)
            );
            let full = DerivedAccounting::build(&s.catalog, &config).unwrap();
            assert_eq!(
                s.derived_accounting.tables["metrics"],
                full.tables["metrics"]
            );
            assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
        }
        assert_eq!(
            db.performance().phases["append_accounting"].count,
            timers + 2
        );
        drop(db);
        let db = Database::open(temp.path(), config).unwrap();
        let s = db.lock().unwrap();
        assert_eq!(s.metadata_bytes, encode_manifest(&s.catalog).unwrap().len());
        assert_eq!(s.catalog.tables["metrics"].receipts.len(), 2);
        assert_eq!(s.catalog.tables["metrics"].idempotency_floor_us, Some(50));
    }
}

#[test]
fn direct_new_clock_reclaims_full_receipt_registry_at_same_frontier() {
    for (pages, frozen) in [(false, false), (true, false), (false, true), (true, true)] {
        let (temp, db, mut config) = database(pages, true);
        drop(db);
        config.checkpoint_frozen_prefix = frozen;
        config.max_idempotency_keys = 1;
        let db = Database::open(temp.path(), config.clone()).unwrap();
        db.write("metrics", "v1:10:old", vec![row(10)], 10).unwrap();
        db.checkpoint().unwrap();
        let before = db.status().unwrap();
        let receipt = db
            .write("metrics", "v1:150:new", vec![row(150)], 150)
            .unwrap();
        assert_eq!(receipt.sequence, before.sequence + 1);
        assert_eq!(
            db.status().unwrap().checkpoint_sequence,
            before.checkpoint_sequence
        );
        assert_eq!(db.status().unwrap().idempotency_keys, 1);
        assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(50));
        assert!(db.write("metrics", "v1:10:old", vec![row(10)], 10).is_err());
        assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
        drop(db);
        let db = Database::open(temp.path(), config).unwrap();
        assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(50));
        assert!(
            db.write("metrics", "v1:150:new", vec![row(150)], 150)
                .unwrap()
                .duplicate
        );
        assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
    }
}

#[test]
fn direct_hot_wal_disk_limits_are_exact_and_prepublication() {
    for gate in ["hot-tier", "WAL capacity", "local disk"] {
        for short_by in [0, 1] {
            let (temp, db, mut config) = database(false, false);
            let item = item("id");
            let sequence = next_sequence(&db.lock().unwrap()).unwrap();
            let record = wal::Record::new(
                sequence,
                wal::Operation::Append {
                    table: item.table.clone(),
                    request_id: item.request_id.clone(),
                    digest: item.digest.clone(),
                    rows: item.rows.clone(),
                },
            );
            let encoded_bytes = wal::EncodedRecord::new(&record).unwrap().len() as u64;
            match gate {
                "hot-tier" => {
                    config.hot_max_bytes =
                        item.rows.iter().map(Row::estimated_bytes).sum::<usize>() - short_by
                }
                "WAL capacity" => config.wal_max_bytes = encoded_bytes - short_by as u64,
                "local disk" => {
                    config.wal_max_bytes = encoded_bytes;
                    config.max_disk_bytes =
                        directory_bytes(temp.path()).unwrap() + encoded_bytes - short_by as u64;
                }
                _ => unreachable!(),
            }
            drop(db);
            let db = Database::open(temp.path(), config).unwrap();
            let result = db.write("metrics", "id", item.rows, 1);
            if short_by == 0 {
                assert_eq!(result.unwrap().sequence, sequence);
                assert!(wal::path(temp.path(), sequence).exists());
            } else {
                let error = result.unwrap_err();
                assert!(format!("{error:#}").contains(gate), "{gate}: {error:#}");
                assert!(!wal::path(temp.path(), sequence).exists());
                let s = db.lock().unwrap();
                assert!(s.catalog.tables["metrics"].receipts.is_empty());
                assert!(s.catalog.tables["metrics"].rollups.is_empty());
                assert_eq!(s.sequence, sequence - 1);
                assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
                assert!(s.fenced.is_none());
            }
        }
    }
}

#[test]
fn direct_wal_failure_precedes_deferred_derived_accounting_failure() {
    let (temp, db, mut config) = database(false, false);
    let item = item("id");
    let sequence = next_sequence(&db.lock().unwrap()).unwrap();
    let record = wal::Record::new(
        sequence,
        wal::Operation::Append {
            table: item.table,
            request_id: item.request_id,
            digest: item.digest,
            rows: item.rows.clone(),
        },
    );
    config.wal_max_bytes = wal::EncodedRecord::new(&record).unwrap().len() as u64 - 1;
    drop(db);
    let db = Database::open(temp.path(), config).unwrap();
    db.lock()
        .unwrap()
        .derived_accounting
        .tables
        .remove("metrics");
    let error = db.write("metrics", "id", item.rows, 1).unwrap_err();
    assert!(format!("{error:#}").contains("batch exceeds WAL capacity"));
    assert_eq!(db.performance().phases["append_accounting"].count, 1);
    assert!(!wal::path(temp.path(), sequence).exists());
    assert_eq!(db.lock().unwrap().derived_working.load(Ordering::SeqCst), 0);
}

#[test]
fn group_rollback_charge_accepts_exact_bytes_and_rejects_one_less() {
    for short_by in [0, 1] {
        let temp = TempDir::new().unwrap();
        let mut config = Config::default();
        let db = Database::open(temp.path(), config.clone()).unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                rollup_widths_us: vec![10, 20, 30, 40, 50, 60],
                ..Default::default()
            },
        )
        .unwrap();
        db.checkpoint().unwrap();
        let item = item("id");
        let s = db.lock().unwrap();
        let sequence = next_sequence(&s).unwrap();
        let prepared = prepare_group_append(
            &s,
            &item,
            sequence,
            0,
            Some(GROUP_PROOF_RESERVATION),
            &config,
            None,
        )
        .unwrap();
        let required = prepared.working_bytes();
        assert!(
            required
                > append_metadata_bytes(
                    &s,
                    "metrics",
                    "id",
                    &prepared.receipt,
                    &prepared.updates,
                    sequence
                )
                .unwrap()
                    + 512
        );
        drop(prepared);
        drop(s);
        drop(db);
        config.metadata_max_bytes = required - short_by;
        let db = Database::open(temp.path(), config).unwrap();
        let result = db
            .write_group(vec![WriteRequest {
                table: "metrics".into(),
                request_id: "id".into(),
                rows: item.rows,
                now_us: 1,
            }])
            .pop()
            .unwrap();
        if short_by == 0 {
            assert_eq!(result.unwrap().sequence, sequence);
        } else {
            assert!(
                format!("{:#}", result.unwrap_err())
                    .contains("group rollback metadata byte budget exceeded")
            );
            assert!(!wal::path(temp.path(), sequence).exists());
        }
        let s = db.lock().unwrap();
        assert_eq!(s.derived_working.load(Ordering::SeqCst), 0);
        assert_eq!(s.metadata_bytes, encode_manifest(&s.catalog).unwrap().len());
        assert_eq!(s.catalog.tables["metrics"].receipts.len(), 1 - short_by);
    }
}
