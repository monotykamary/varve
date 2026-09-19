use super::*;
use crate::raw_memory::{codec_charge, logical_bytes, row_charge};
use tempfile::TempDir;

#[test]
fn raw_budget_configuration_and_metrics_are_documented() -> Result<()> {
    let config = Config::default();
    let docs = include_str!("../docs/CONFIGURATION.md");
    for key in serde_json::to_value(&config)?.as_object().unwrap().keys() {
        assert!(
            docs.contains(&format!("| `{key}` |")),
            "undocumented Config field {key}"
        );
    }
    assert_eq!(config.raw_memory_max_bytes, 128 * 1024 * 1024);
    assert_eq!(config.raw_working_max_bytes, 512 * 1024 * 1024);
    assert!(
        Config {
            raw_memory_max_bytes: 0,
            ..config.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        Config {
            raw_working_max_bytes: 0,
            ..config.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        Config {
            raw_memory_max_bytes: usize::MAX,
            ..config
        }
        .validate()
        .is_err()
    );
    let metrics = include_str!("../docs/METRICS.md");
    let service = include_str!("service.rs");
    let status = serde_json::to_value(RawMemoryBudget::new(1, 1)?.status())?;
    for key in status.as_object().unwrap().keys() {
        let suffix = if key == "rejections" {
            "rejections_total"
        } else {
            key
        };
        let metric = format!("varve_raw_memory_{suffix}");
        assert!(metrics.contains(&metric));
        assert!(service.contains(&format!("# TYPE {metric} ")));
        assert!(service.contains(&format!("status.raw_memory.{key}")));
    }
    Ok(())
}

#[test]
fn raw_budget_concurrent_admission_cannot_overbook() -> Result<()> {
    let budget = RawMemoryBudget::new(1024, 1)?;
    let barrier = std::sync::Barrier::new(4);
    let successes = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                let credit = budget.reserve(1024);
                if credit.is_ok() {
                    successes.fetch_add(1, Ordering::SeqCst);
                }
                barrier.wait();
                drop(credit);
            });
        }
    });
    assert_eq!(successes.load(Ordering::SeqCst), 1);
    assert_eq!(budget.status().peak_bytes, 1024);
    assert_eq!(budget.status().reserved_bytes, 0);
    assert_eq!(budget.status().rejections, 3);
    Ok(())
}

fn row(value: f64) -> Row {
    Row {
        timestamp_us: 1,
        tenant: "t".into(),
        series: "s".into(),
        value,
        tags: BTreeMap::new(),
    }
}
fn config() -> Config {
    Config {
        raw_memory_max_bytes: 64 * 1024,
        query_max_output_bytes: 4096,
        query_retained_inputs: true,
        ..Config::default()
    }
}
fn database(config: Config) -> Result<(TempDir, Database)> {
    let dir = TempDir::new()?;
    let db = Database::open(dir.path(), config)?;
    db.create_table(
        "metrics",
        TableConfig {
            shards: 1,
            rollup_widths_us: vec![],
            ..TableConfig::default()
        },
    )?;
    Ok((dir, db))
}

#[test]
fn raw_budget_linear_admission_final_release_and_weak_identity() -> Result<()> {
    let logical = row(1.0).estimated_bytes();
    let charge = row_charge(logical);
    let budget = RawMemoryBudget::new(charge, 4096)?;
    let credit = budget.reserve(charge)?;
    assert_eq!(budget.status().live_bytes, 0);
    assert!(budget.reserve(1).is_err());
    let rows = SharedRawRows::build(credit, || {
        Ok(vec![StoredRow {
            row: row(-0.0),
            sequence: 7,
            ordinal: 2,
        }])
    })?;
    let weak = rows.downgrade();
    let query = rows.pin();
    let checkpoint = rows.pin();
    assert!(weak.ptr_eq(&query.downgrade()));
    assert_eq!(budget.status().pinned_bytes, charge);
    drop(rows);
    assert_eq!(budget.status().live_bytes, charge);
    assert_eq!(budget.status().pinned_bytes, charge);
    let working = budget.reserve_working(4096)?;
    assert!(budget.reserve_working(1).is_err());
    assert_eq!(budget.status().reserved_bytes, charge + 4096);
    drop(query);
    assert_eq!(checkpoint[0].row.value.to_bits(), (-0.0f64).to_bits());
    assert_eq!(budget.status().pinned_bytes, charge);
    drop(checkpoint);
    assert_eq!(budget.status().live_bytes, 0);
    assert_eq!(budget.status().pinned_bytes, 0);
    drop(working);
    assert_eq!(budget.status().reserved_bytes, 0);
    assert_eq!(budget.status().peak_bytes, charge + 4096);
    assert_eq!(budget.status().rejections, 2);
    // Weak CLI identities do not retain credit or permit uncharged upgrades.
    let again = budget.reserve(charge)?;
    drop(again);
    assert!(
        weak.ptr_eq(&weak),
        "identity token remains valid after payload release"
    );
    assert_eq!(budget.status().reserved_bytes, 0);
    Ok(())
}

#[test]
fn raw_budget_failed_and_panicking_allocators_return_credit() -> Result<()> {
    let budget = RawMemoryBudget::new(4096, 1)?;
    let failed = SharedRawRows::build(budget.reserve(4096)?, || anyhow::bail!("decode failed"));
    assert!(failed.is_err());
    assert_eq!(budget.status().reserved_bytes, 0);
    let panicked = std::panic::catch_unwind(|| {
        SharedRawRows::build(budget.reserve(4096).unwrap(), || {
            panic!("cancelled preparation")
        })
    });
    assert!(panicked.is_err());
    assert_eq!(budget.status().reserved_bytes, 0);
    assert_eq!(budget.status().live_bytes, 0);
    let called = std::cell::Cell::new(false);
    let result = budget
        .reserve(4097)
        .map_err(anyhow::Error::from)
        .and_then(|credit| {
            SharedRawRows::build(credit, || {
                called.set(true);
                Ok(Vec::new())
            })
        });
    assert!(result.is_err());
    assert!(!called.get(), "admission must precede allocation");
    Ok(())
}

#[test]
fn raw_budget_checkpoint_retires_designations_not_query_or_capture_owners() -> Result<()> {
    for frozen in [false, true] {
        let mut config = config();
        config.checkpoint_frozen_prefix = frozen;
        let (_dir, db) = database(config)?;
        let receipt = db.write("metrics", "one", vec![row(-0.0)], 1)?;
        let capture = {
            let s = db.lock()?;
            capture_checkpoint(&s, &db.inner.config)?.unwrap()
        };
        let query = {
            let s = db.lock()?;
            s.hot["metrics"][0].pinned()
        };
        let charge = db.status()?.raw_memory.live_bytes;
        let used = db.status()?.raw_memory.reserved_bytes;
        let full = db
            .inner
            .raw_memory
            .reserve(db.inner.config.raw_memory_max_bytes - used)?;
        assert!(db.write("metrics", "blocked", vec![row(2.0)], 2).is_err());
        assert_eq!(db.status()?.sequence, receipt.sequence);
        // Mandatory checkpoint work has its own headroom. Retained copies are
        // skipped BEFORE allocation when the owned pool is full.
        db.checkpoint()?;
        let status = db.status()?;
        assert_eq!(status.hot_bytes, 0);
        assert_eq!(status.decoded_cache_bytes, 0);
        assert_eq!(status.raw_memory.working_bytes, 0);
        assert_eq!(status.raw_memory.live_bytes, charge);
        assert_eq!(status.raw_memory.pinned_bytes, charge);
        assert!(status.raw_memory.rejections >= 2);
        drop(full);
        drop(query);
        assert_eq!(db.status()?.raw_memory.live_bytes, charge);
        assert_eq!(
            capture.hot[0].2[0].rows[0].row.value.to_bits(),
            (-0.0f64).to_bits()
        );
        drop(capture);
        assert_eq!(db.status()?.raw_memory.reserved_bytes, 0);
        let retry = db.write("metrics", "one", vec![row(-0.0)], 1)?;
        assert!(retry.duplicate);
        assert_eq!(retry.sequence, receipt.sequence);
        let s = db.lock()?;
        let descriptor = &s.catalog.tables["metrics"].segments[0];
        let rows = segment::read(&db.inner.root.join(descriptor.key()))?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row.value.to_bits(), (-0.0f64).to_bits());
        assert_eq!(rows[0].sequence, receipt.sequence);
    }
    Ok(())
}

#[test]
fn raw_budget_scan_handoff_and_decode_cache_have_distinct_owners() -> Result<()> {
    let (_dir, db) = database(Config {
        raw_memory_max_bytes: 16 * 1024 * 1024,
        query_max_output_bytes: 2 * row(1.0).estimated_bytes(),
        query_retained_inputs: false,
        ..config()
    })?;
    db.write("metrics", "two", vec![row(-0.0), row(2.0)], 1)?;
    let before = db.status()?.raw_memory.reserved_bytes;
    let caller_rows = db.scan("metrics", None, None, None, None)?;
    assert_eq!(db.status()?.raw_memory.reserved_bytes, before);
    db.checkpoint()?;
    assert_eq!(db.status()?.raw_memory.reserved_bytes, 0);
    assert_eq!(caller_rows[0].row.value.to_bits(), (-0.0f64).to_bits());
    let decoded = db.scan("metrics", None, None, None, None)?;
    assert_eq!(decoded, caller_rows);
    let charge = row_charge(logical_bytes(&decoded));
    assert_eq!(db.status()?.raw_memory.reserved_bytes, charge);
    assert_eq!(db.status()?.raw_memory.live_bytes, charge);
    db.lock()?.decoded.clear();
    assert_eq!(db.status()?.raw_memory.reserved_bytes, 0);
    db.write("metrics", "overflow", vec![row(3.0); 3], 2)?;
    let before = db.status()?.raw_memory.reserved_bytes;
    assert!(db.scan("metrics", None, None, None, None).is_err());
    assert_eq!(db.status()?.raw_memory.reserved_bytes, before);
    Ok(())
}

#[test]
fn raw_budget_cache_eviction_does_not_free_a_pinned_allocation() -> Result<()> {
    let (_dir, db) = database(config())?;
    db.write("metrics", "one", vec![row(1.0)], 1)?;
    db.checkpoint()?;
    let (pin, descriptor) = {
        let mut s = db.lock()?;
        let descriptor = s.catalog.tables["metrics"].segments[0].clone();
        let rows = touch_decoded(&mut s, &descriptor.id).expect("retained checkpoint copy");
        // Actual LRU removal, not an accounting-only decrement.
        offer_decoded(
            &mut s,
            0,
            "empty".into(),
            SharedRawRows::build(db.inner.raw_memory.reserve(row_charge(0))?, || {
                Ok(Vec::new())
            })?,
            0,
        );
        assert!(!s.decoded.contains_key(&descriptor.id));
        s.decoded.clear();
        (rows, descriptor)
    };
    let charge = row_charge(logical_bytes(&pin));
    assert_eq!(db.status()?.raw_memory.live_bytes, charge);
    assert_eq!(db.status()?.raw_memory.pinned_bytes, charge);
    let full = db
        .inner
        .raw_memory
        .reserve(db.inner.config.raw_memory_max_bytes - charge)?;
    assert!(read_raw_segment(&db.inner, &descriptor, false).is_err());
    assert_eq!(pin[0].row.value, 1.0);
    drop(full);
    drop(pin);
    assert_eq!(db.status()?.raw_memory.reserved_bytes, 0);
    Ok(())
}

#[test]
fn raw_budget_working_rejection_leaves_committed_state_retryable() -> Result<()> {
    let (_dir, db) = database(config())?;
    let receipt = db.write("metrics", "one", vec![row(1.0)], 1)?;
    let baseline = db.status()?.raw_memory.reserved_bytes;
    let full = db
        .inner
        .raw_memory
        .reserve_working(db.inner.config.raw_working_max_bytes)?;
    assert!(db.checkpoint().is_err());
    let status = db.status()?;
    assert_eq!(status.sequence, receipt.sequence);
    assert_eq!(status.hot_rows, 1);
    assert!(status.fenced.is_none());
    assert_eq!(
        status.raw_memory.reserved_bytes,
        baseline + db.inner.config.raw_working_max_bytes
    );
    drop(full);
    db.checkpoint()?;
    db.lock()?.decoded.clear();
    assert_eq!(db.status()?.raw_memory.reserved_bytes, 0);
    Ok(())
}

#[test]
fn raw_budget_retention_preserves_pinned_old_rows_and_releases_copies() -> Result<()> {
    let (_dir, db) = database(config())?;
    db.create_table(
        "retained",
        TableConfig {
            shards: 1,
            retention_us: Some(10),
            rollup_widths_us: vec![],
            ..TableConfig::default()
        },
    )?;
    let mut fresh = row(2.0);
    fresh.timestamp_us = 11;
    db.write("retained", "both", vec![row(1.0), fresh.clone()], 11)?;
    db.checkpoint()?;
    let old = {
        let mut s = db.lock()?;
        let id = s.catalog.tables["retained"].segments[0].id.clone();
        let old = touch_decoded(&mut s, &id).unwrap();
        s.decoded.clear();
        old
    };
    let charge = row_charge(logical_bytes(&old));
    let full = db
        .inner
        .raw_memory
        .reserve(db.inner.config.raw_memory_max_bytes - charge)?;
    let report = db.maintain(15)?;
    assert_eq!(report.expired_rows, 1);
    assert_eq!(old.len(), 2);
    assert_eq!(old[0].row.value, 1.0);
    assert_eq!(db.status()?.raw_memory.live_bytes, charge);
    assert_eq!(db.status()?.raw_memory.working_bytes, 0);
    assert_eq!(db.status()?.raw_memory.pinned_bytes, charge);
    let descriptor = db.lock()?.catalog.tables["retained"].segments[0].clone();
    let retained = segment::read(&db.inner.root.join(descriptor.key()))?;
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].row, fresh);
    drop(full);
    drop(old);
    assert_eq!(db.status()?.raw_memory.reserved_bytes, 0);
    Ok(())
}

#[test]
fn raw_budget_wal_admission_failure_returns_input_and_prepared_credit() -> Result<()> {
    let (dir, db) = database(Config {
        wal_max_bytes: 16 * 1024,
        max_disk_bytes: 32 * 1024,
        max_batch_bytes: 16 * 1024,
        ..config()
    })?;
    let before = db.status()?.sequence;
    fs::write(dir.path().join("budget-filler"), vec![0u8; 32 * 1024])?;
    let error = db
        .write("metrics", "failed", vec![row(1.0)], 1)
        .unwrap_err();
    assert!(format!("{error:#}").contains("disk"));
    assert_eq!(db.status()?.raw_memory.reserved_bytes, 0);
    assert_eq!(db.status()?.sequence, before);
    assert_eq!(db.status()?.hot_rows, 0);
    fs::remove_file(dir.path().join("budget-filler"))?;
    let receipt = db.write("metrics", "failed", vec![row(1.0)], 1)?;
    assert!(!receipt.duplicate);
    assert_eq!(receipt.sequence, before + 1);
    Ok(())
}

#[test]
fn raw_budget_decode_failure_and_retention_compaction_release_workspaces() -> Result<()> {
    let (_dir, db) = database(Config {
        compact_min_segments: 2,
        ..config()
    })?;
    for i in 0..2 {
        db.write("metrics", &format!("id-{i}"), vec![row(i as f64)], i + 1)?;
        db.checkpoint()?;
    }
    db.lock()?.decoded.clear();
    let mut forged = db.lock()?.catalog.tables["metrics"].segments[0].clone();
    forged.rows += 1;
    assert!(read_raw_segment(&db.inner, &forged, true).is_err());
    assert_eq!(db.status()?.raw_memory.working_bytes, 0);
    assert_eq!(db.compact()?, 1);
    assert_eq!(db.status()?.raw_memory.working_bytes, 0);
    assert_eq!(db.status()?.raw_memory.reserved_bytes, 0);
    assert!(codec_charge(130, 1000) > row_charge(130));
    Ok(())
}

#[cfg(feature = "fault-injection")]
#[test]
fn raw_budget_stale_checkpoint_preparation_drops_optional_copies() -> Result<()> {
    use std::time::Duration;
    let (_dir, db) = database(config())?;
    db.write("metrics", "one", vec![row(1.0)], 1)?;
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::CheckpointBeforePublish);
    db.set_maintenance_test_hook(Some(hook.clone()))?;
    let worker = db.clone();
    let handle = std::thread::spawn(move || checkpoint_prepared_scheduled(&worker));
    assert!(hook.wait_until_blocked(Duration::from_secs(10)));
    let hot = db.status()?.raw_memory.live_bytes;
    assert!(
        hot > row_charge(row(1.0).estimated_bytes()),
        "prepared retained copy is owned"
    );
    db.write("metrics", "two", vec![row(2.0)], 2)?;
    hook.release();
    assert!(!handle.join().unwrap()?);
    db.set_maintenance_test_hook(None)?;
    let status = db.status()?;
    assert_eq!(status.hot_rows, 2);
    assert_eq!(status.decoded_cache_bytes, 0);
    assert_eq!(
        status.raw_memory.live_bytes,
        2 * row_charge(row(1.0).estimated_bytes())
    );
    assert_eq!(
        status.raw_memory.reserved_bytes,
        status.raw_memory.live_bytes
    );
    assert_eq!(status.raw_memory.working_bytes, 0);
    assert_eq!(status.raw_memory.pinned_bytes, 0);
    Ok(())
}

#[cfg(feature = "fault-injection")]
#[test]
fn raw_budget_cancelled_native_query_keeps_retired_hot_until_teardown() -> Result<()> {
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    let library = std::env::var_os("VARVE_DUCKDB_V2_LIBRARY").expect("pinned native test library");
    let (_dir, db) = database(Config {
        duckdb_library: Some(library.into()),
        raw_memory_max_bytes: 1024 * 1024,
        ..config()
    })?;
    db.write("metrics", "one", vec![row(-0.0)], 1)?;
    let cancelled = Arc::new(AtomicBool::new(false));
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::SqlSnapshotCaptured);
    db.set_maintenance_test_hook(Some(hook.clone()))?;
    let worker = db.clone();
    let cancel = cancelled.clone();
    let handle =
        std::thread::spawn(move || worker.query_cancellable("SELECT * FROM metrics", &cancel));
    assert!(hook.wait_until_blocked(Duration::from_secs(10)));
    db.checkpoint()?;
    db.lock()?.decoded.clear();
    let status = db.status()?;
    assert_eq!(status.hot_rows, 0);
    assert_eq!(status.decoded_cache_bytes, 0);
    assert_eq!(
        status.raw_memory.live_bytes,
        row_charge(row(-0.0).estimated_bytes())
    );
    assert_eq!(status.raw_memory.pinned_bytes, status.raw_memory.live_bytes);
    cancelled.store(true, Ordering::Release);
    hook.release();
    assert!(handle.join().unwrap().is_err());
    db.set_maintenance_test_hook(None)?;
    assert_eq!(db.status()?.raw_memory.reserved_bytes, 0);
    assert_eq!(db.status()?.active_queries, 0);
    Ok(())
}
