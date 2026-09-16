use anyhow::Result;
use std::collections::BTreeMap;
use varve::{Config, Database, Row, TableConfig, WriteRequest};

fn row(timestamp_us: i64, value: f64, tag: &str) -> Row {
    Row {
        timestamp_us,
        tenant: "e\u{301}".into(),
        series: "雪".into(),
        value,
        tags: BTreeMap::from([("tag".into(), tag.into())]),
    }
}

fn wide_row(timestamp_us: i64) -> Row {
    let mut result = row(timestamp_us, 1.0, "a");
    result.tags = (0..3)
        .map(|i| (format!("tag{i}"), "x".repeat(1024)))
        .collect();
    result
}

fn root_json(path: &std::path::Path) -> Result<serde_json::Value> {
    let bytes = std::fs::read(path.join("manifest.bin"))?;
    assert_eq!(&bytes[..8], b"VARVEM02");
    Ok(serde_json::from_slice(&bytes[8..bytes.len() - 32])?)
}

fn paged() -> Config {
    Config {
        derived_pages: true,
        derived_page_bytes: 4096,
        ..Config::default()
    }
}

fn checkpoint_budget_rows(batch: usize) -> Vec<Row> {
    (0..64)
        .map(|i| {
            let mut row = row(0, 1.0, "budget");
            row.series = format!("series-{:04}", batch * 64 + i);
            row
        })
        .collect()
}

fn accepted_appends_can_checkpoint(derived_pages: bool, grouped: bool) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = Config {
        derived_pages,
        derived_max_bytes: 1024 * 1024,
        derived_page_bytes: 4096,
        ..Config::default()
    };
    let db = Database::open(dir.path(), config.clone())?;
    db.create_table(
        "metrics",
        TableConfig {
            shards: 1,
            rollup_widths_us: vec![10],
            ..TableConfig::default()
        },
    )?;
    db.checkpoint()?;
    let mut accepted = 0;
    let mut rejected = false;
    for batch in 0..32 {
        let before = db.status()?;
        assert_eq!(before.derived_working_bytes, 0);
        let rows = checkpoint_budget_rows(batch);
        let id = format!("batch-{batch}");
        let result = if grouped {
            db.write_group(vec![WriteRequest {
                table: "metrics".into(),
                request_id: id,
                rows,
                now_us: 0,
            }])
            .pop()
            .unwrap()
        } else {
            db.write("metrics", &id, rows, 0)
        };
        let status = db.status()?;
        assert_eq!(status.derived_working_bytes, 0);
        assert!(status.fenced.is_none());
        match result {
            Ok(receipt) => {
                assert!(!receipt.duplicate);
                assert_eq!(receipt.sequence, before.sequence + 1);
                accepted += 64;
                let checkpoint = db.checkpoint();
                assert!(
                    checkpoint.is_ok(),
                    "accepted {accepted} rows: v2={derived_pages}, grouped={grouped}, resident={}, checkpoint={checkpoint:?}",
                    status.derived_resident_bytes
                );
                assert_eq!(db.status()?.checkpoint_sequence, receipt.sequence);
            }
            Err(error) => {
                assert!(format!("{error:#}").contains("derived"), "{error:#}");
                assert!(format!("{error:#}").contains("budget"), "{error:#}");
                assert_eq!(status.sequence, before.sequence);
                assert_eq!(status.checkpoint_sequence, before.checkpoint_sequence);
                assert_eq!(status.wal_bytes, before.wal_bytes);
                assert_eq!(status.hot_rows, before.hot_rows);
                assert_eq!(status.derived_resident_bytes, before.derived_resident_bytes);
                rejected = true;
                break;
            }
        }
    }
    assert!(
        accepted > 0 && rejected,
        "must reach derived budget admission"
    );
    db.checkpoint()?;
    let raw = db.scan("metrics", None, None, None, None)?;
    let rollups = db.rollups("metrics")?;
    assert_eq!(raw.len(), accepted);
    assert_eq!(rollups.len(), accepted);
    assert!(rollups.iter().all(|row| row.count == 1 && row.sum == 1.0));
    assert_eq!(db.status()?.derived_working_bytes, 0);
    let sequence = db.status()?.sequence;
    drop(db);
    let db = Database::open(dir.path(), config)?;
    assert_eq!(db.status()?.sequence, sequence);
    assert!(db.status()?.fenced.is_none());
    assert_eq!(db.scan("metrics", None, None, None, None)?, raw);
    assert_eq!(db.rollups("metrics")?, rollups);
    assert!(
        db.write("metrics", "batch-0", checkpoint_budget_rows(0), 0)?
            .duplicate
    );
    db.checkpoint()?;
    Ok(())
}

#[test]
fn checkpoint_budget_liveness_v1_append() -> Result<()> {
    accepted_appends_can_checkpoint(false, false)
}

#[test]
fn checkpoint_budget_liveness_v2_append() -> Result<()> {
    accepted_appends_can_checkpoint(true, false)
}

#[test]
fn checkpoint_budget_liveness_v1_group() -> Result<()> {
    accepted_appends_can_checkpoint(false, true)
}

#[test]
fn checkpoint_budget_liveness_v2_group() -> Result<()> {
    accepted_appends_can_checkpoint(true, true)
}

#[test]
fn historical_wal_replay_does_not_apply_live_checkpoint_headroom() -> Result<()> {
    for derived_pages in [false, true] {
        for grouped in [false, true] {
            let dir = tempfile::tempdir()?;
            let large = Config {
                derived_pages,
                derived_max_bytes: 2 * 1024 * 1024,
                derived_page_bytes: 4096,
                ..Config::default()
            };
            let small = Config {
                derived_max_bytes: 1024 * 1024,
                ..large.clone()
            };
            let db = Database::open(dir.path(), large.clone())?;
            db.create_table(
                "metrics",
                TableConfig {
                    shards: 1,
                    rollup_widths_us: vec![10],
                    ..TableConfig::default()
                },
            )?;
            db.checkpoint()?;
            for batch in 0..5 {
                let id = format!("batch-{batch}");
                let rows = checkpoint_budget_rows(batch);
                if grouped {
                    db.write_group(vec![WriteRequest {
                        table: "metrics".into(),
                        request_id: id,
                        rows,
                        now_us: 0,
                    }])
                    .pop()
                    .unwrap()?;
                } else {
                    db.write("metrics", &id, rows, 0)?;
                }
            }
            let status = db.status()?;
            assert!(status.sequence > status.checkpoint_sequence);
            assert!(status.derived_resident_bytes * 2 > small.derived_max_bytes);
            let raw = db.scan("metrics", None, None, None, None)?;
            let rollups = db.rollups("metrics")?;
            drop(db);
            // Historical acknowledgments replay even though a future checkpoint
            // now needs an explicitly larger budget. Actual replay caps remain.
            let db = Database::open(dir.path(), small.clone())?;
            assert_eq!(db.status()?.sequence, status.sequence);
            assert_eq!(db.scan("metrics", None, None, None, None)?, raw);
            assert_eq!(db.rollups("metrics")?, rollups);
            let error = db
                .write("metrics", "new", vec![row(0, 1.0, "new")], 0)
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("derived checkpoint working budget")
            );
            assert_eq!(db.status()?.sequence, status.sequence);
            assert_eq!(db.status()?.wal_bytes, status.wal_bytes);
            assert!(db.checkpoint().is_err());
            assert!(db.status()?.fenced.is_none());
            assert_eq!(db.status()?.derived_working_bytes, 0);
            drop(db);
            let too_small = Config {
                derived_max_bytes: 512 * 1024,
                ..small
            };
            let error = Database::open(dir.path(), too_small)
                .err()
                .expect("actual replay memory guard");
            assert!(format!("{error:#}").contains("derived"), "{error:#}");
            let db = Database::open(dir.path(), large.clone())?;
            db.checkpoint()?;
            drop(db);
            let db = Database::open(dir.path(), large)?;
            assert_eq!(db.scan("metrics", None, None, None, None)?, raw);
            assert_eq!(db.rollups("metrics")?, rollups);
        }
    }
    Ok(())
}

#[test]
fn v1_migration_is_opt_in_and_v2_never_silently_downgrades() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Database::open(dir.path(), Config::default())?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            ..TableConfig::default()
        },
    )?;
    db.write("metrics", "first", vec![row(-1, -0.0, "tag")], 0)?;
    db.checkpoint()?;
    let expected = serde_json::to_vec(&db.rollups("metrics")?)?;
    assert_eq!(
        &std::fs::read(dir.path().join("manifest.bin"))?[..8],
        b"VARVEM01"
    );
    drop(db);
    let db = Database::open(dir.path(), paged())?;
    let root = root_json(dir.path())?;
    assert!(root["tables"]["metrics"].get("rollups").is_none());
    assert!(root["tables"]["metrics"].get("receipts").is_none());
    assert!(
        !root["tables"]["metrics"]["derived"]["rollups"]["pages"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(serde_json::to_vec(&db.rollups("metrics")?)?, expected);
    drop(db);
    let db = Database::open(dir.path(), Config::default())?;
    db.write("metrics", "second", vec![row(1, 1.0, "tag")], 1)?;
    db.checkpoint()?;
    root_json(dir.path())?;
    assert!(
        db.write("metrics", "first", vec![row(-1, -0.0, "tag")], 1)?
            .duplicate
    );
    drop(db);
    let mut bytes = std::fs::read(dir.path().join("manifest.bin"))?;
    bytes[..8].copy_from_slice(b"VARVEM01");
    let end = bytes.len() - 32;
    let digest = blake3::hash(&bytes[..end]);
    bytes[end..].copy_from_slice(digest.as_bytes());
    std::fs::write(dir.path().join("manifest.bin"), bytes)?;
    assert!(Database::open(dir.path(), Config::default()).is_err());
    Ok(())
}

#[test]
fn public_index_matches_canonical_scan_and_rebuilds_after_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = paged();
    let db = Database::open(dir.path(), config.clone())?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10, 20],
            ..TableConfig::default()
        },
    )?;
    let mut rows = Vec::new();
    for tenant in ["é", "e\u{301}"] {
        for series in ["雪", "CPU"] {
            for tag in ["a", "b"] {
                for ts in [-21, -1, 0, 20] {
                    let mut r = row(ts, 1.0, tag);
                    r.tenant = tenant.into();
                    r.series = series.into();
                    rows.push(r);
                }
            }
        }
    }
    db.write("metrics", "seed", rows, 20)?;
    let check = |db: &Database| -> Result<()> {
        let all = db.rollups("metrics")?;
        for tenant in [None, Some("é")] {
            for series in [None, Some("雪")] {
                for width in [None, Some(10), Some(20)] {
                    let filter = varve::RollupSelection {
                        tenant,
                        series,
                        width_us: width,
                        start_us: Some(-20),
                        end_us: Some(20),
                    };
                    let selected = db.select_rollups("metrics", filter)?;
                    let expected: Vec<_> = all
                        .iter()
                        .filter(|r| {
                            tenant.is_none_or(|t| r.tenant == t)
                                && series.is_none_or(|s| r.series == s)
                                && width.is_none_or(|w| r.width_us == w)
                                && r.bucket_us >= -20
                                && r.bucket_us < 20
                        })
                        .cloned()
                        .collect();
                    assert_eq!(selected, expected);
                }
            }
        }
        assert!(
            db.select_rollups(
                "metrics",
                varve::RollupSelection {
                    start_us: Some(1),
                    end_us: Some(-1),
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            db.select_rollups(
                "metrics",
                varve::RollupSelection {
                    start_us: Some(i64::MAX),
                    end_us: Some(i64::MAX),
                    ..Default::default()
                }
            )?
            .is_empty()
        );
        assert_eq!(
            db.select_rollups(
                "metrics",
                varve::RollupSelection {
                    start_us: Some(i64::MIN),
                    end_us: Some(i64::MAX),
                    ..Default::default()
                }
            )?,
            all
        );
        Ok(())
    };
    check(&db)?;
    db.checkpoint()?;
    drop(db);
    check(&Database::open(dir.path(), config)?)?;
    Ok(())
}

#[test]
fn retention_replaces_rollups_reuses_receipts_and_cannot_resurrect_raw() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = paged();
    let db = Database::open(dir.path(), config.clone())?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            retention_us: Some(10),
            rollup_retention_us: Some(20),
            ..TableConfig::default()
        },
    )?;
    let input = vec![row(0, 2.0, "tag")];
    db.write("metrics", "receipt", input.clone(), 0)?;
    db.checkpoint()?;
    let before = root_json(dir.path())?;
    db.maintain(100)?;
    let after = root_json(dir.path())?;
    assert_eq!(
        before["tables"]["metrics"]["derived"]["receipts"],
        after["tables"]["metrics"]["derived"]["receipts"]
    );
    assert_ne!(
        before["tables"]["metrics"]["derived"]["rollups"],
        after["tables"]["metrics"]["derived"]["rollups"]
    );
    assert!(db.rollups("metrics")?.is_empty());
    drop(db);
    let db = Database::open(dir.path(), config)?;
    assert!(db.write("metrics", "receipt", input, 100)?.duplicate);
    assert!(db.scan("metrics", None, None, None, None)?.is_empty());
    assert!(db.rollups("metrics")?.is_empty());
    Ok(())
}

#[test]
fn file_store_shipping_restore_gc_and_missing_page_are_closed() -> Result<()> {
    use std::sync::Arc;
    use varve::remote::{FileStore, RemoteStore};
    let dir = tempfile::tempdir()?;
    let local = dir.path().join("local");
    let remote = Arc::new(FileStore::new(dir.path().join("remote"))?);
    let db = Database::open_with_remote(&local, paged(), Some(remote.clone()))?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            ..TableConfig::default()
        },
    )?;
    db.write("metrics", "first", vec![row(-1, 2.0, "a")], 0)?;
    db.checkpoint()?;
    db.ship()?;
    let old: Vec<_> = remote.list("derived")?;
    assert!(!old.is_empty());
    db.write("metrics", "second", vec![row(-1, 3.0, "a")], 0)?;
    db.checkpoint()?;
    db.ship()?;
    let current = root_json(&local)?;
    let live = current["tables"]["metrics"]["derived"]["receipts"]["pages"][0]["digest"]
        .as_str()
        .unwrap();
    let live = format!("derived/{live}.page");
    db.vacuum_remote()?;
    assert!(remote.get(&live).is_ok());
    assert!(old.iter().any(|key| remote.get(key).is_err()));
    let restored = Database::restore(
        dir.path().join("restored"),
        Config::default(),
        remote.clone(),
    )?;
    assert_eq!(
        serde_json::to_vec(&restored.rollups("metrics")?)?,
        serde_json::to_vec(&db.rollups("metrics")?)?
    );
    assert!(
        restored
            .write("metrics", "first", vec![row(-1, 2.0, "a")], 0)?
            .duplicate
    );
    drop(restored);
    drop(db);
    remote.delete(&live)?;
    assert!(Database::restore(dir.path().join("missing"), Config::default(), remote).is_err());
    assert!(!dir.path().join("missing").exists());
    Ok(())
}

struct FailDerivedUpload {
    store: varve::remote::FileStore,
}
impl varve::remote::RemoteStore for FailDerivedUpload {
    fn local_root(&self) -> Option<&std::path::Path> {
        self.store.local_root()
    }
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.store.get(key)
    }
    fn get_bounded(&self, key: &str, max: usize) -> Result<Vec<u8>> {
        self.store.get_bounded(key, max)
    }
    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        if key.starts_with("derived/") {
            anyhow::bail!("injected page upload failure");
        }
        self.store.put_immutable(key, bytes)
    }
    fn head(&self) -> Result<Option<varve::remote::HeadObject>> {
        self.store.head()
    }
    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        self.store.compare_and_swap_head(expected, bytes)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.store.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.store.delete(key)
    }
}

#[test]
fn failed_page_upload_never_publishes_remote_head() -> Result<()> {
    use std::sync::Arc;
    use varve::remote::{FileStore, RemoteStore};
    let dir = tempfile::tempdir()?;
    let remote = Arc::new(FailDerivedUpload {
        store: FileStore::new(dir.path().join("remote"))?,
    });
    let db = Database::open_with_remote(dir.path().join("local"), paged(), Some(remote.clone()))?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            ..TableConfig::default()
        },
    )?;
    db.write("metrics", "first", vec![row(-1, 2.0, "a")], 0)?;
    db.checkpoint()?;
    assert!(db.ship().is_err());
    assert!(remote.head()?.is_none());
    assert_eq!(db.rollups("metrics")?[0].count, 1);
    Ok(())
}

#[cfg(feature = "fault-injection")]
#[test]
fn derived_checkpoint_fault_worker() -> Result<()> {
    let Ok(path) = std::env::var("VARVE_DERIVED_TEST_ROOT") else {
        return Ok(());
    };
    let db = Database::open(path, paged())?;
    let result = db.write_group(vec![WriteRequest {
        table: "metrics".into(),
        request_id: "grouped".into(),
        now_us: 1,
        rows: vec![row(1, 2.0, "a"), row(1, 3.0, "a"), row(1, 4.0, "a")],
    }]);
    result.into_iter().next().unwrap()?;
    db.checkpoint()?;
    anyhow::bail!("failpoint not reached")
}

#[cfg(feature = "fault-injection")]
#[test]
fn page_and_root_crash_points_replay_one_shared_frontier() -> Result<()> {
    for failpoint in [
        "derived_page_published",
        "derived_pages_published",
        "manifest_published",
        "group_before_apply",
    ] {
        let dir = tempfile::tempdir()?;
        let db = Database::open(dir.path(), paged())?;
        db.create_table(
            "metrics",
            TableConfig {
                rollup_widths_us: vec![10],
                ..TableConfig::default()
            },
        )?;
        db.write("metrics", "seed", vec![row(1, 1.0, "a")], 1)?;
        db.checkpoint()?;
        drop(db);
        let status = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", "derived_checkpoint_fault_worker", "--nocapture"])
            .env("VARVE_DERIVED_TEST_ROOT", dir.path())
            .env("VARVE_FAILPOINT", failpoint)
            .status()?;
        assert_eq!(status.code(), Some(86), "{failpoint}");
        let db = Database::open(dir.path(), paged())?;
        let rollup = &db.rollups("metrics")?[0];
        assert_eq!(rollup.count, 4, "{failpoint}");
        assert_eq!(rollup.sum, 10.0, "{failpoint}");
        assert!(
            db.write(
                "metrics",
                "grouped",
                vec![row(1, 2.0, "a"), row(1, 3.0, "a"), row(1, 4.0, "a")],
                1
            )?
            .duplicate
        );
        db.checkpoint()?;
        assert_eq!(db.status()?.sequence, db.status()?.checkpoint_sequence);
    }
    Ok(())
}

#[test]
fn v2_control_and_derived_admission_are_independent_and_pre_wal() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = Config {
        metadata_max_bytes: 8192,
        ..paged()
    };
    let db = Database::open(dir.path(), config.clone())?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            ..TableConfig::default()
        },
    )?;
    for i in 0..10 {
        db.write(
            "metrics",
            &format!("r{i}"),
            vec![row(i * 10, 1.0, &"x".repeat(160))],
            i * 10,
        )?;
        db.checkpoint()?;
    }
    let status = db.status()?;
    assert!(status.metadata_bytes > config.metadata_max_bytes);
    assert!(status.control_root_bytes <= config.metadata_max_bytes);
    assert!(status.derived_resident_bytes > status.derived_encoded_bytes);
    assert_eq!(status.derived_working_bytes, 0);
    let sequence = status.sequence;
    assert!(
        db.write("metrics", "too-wide", vec![wide_row(100)], 100)
            .is_err()
    );
    assert_eq!(db.status()?.sequence, sequence);
    drop(db);
    assert_eq!(
        Database::open(dir.path(), config)?
            .rollups("metrics")?
            .len(),
        10
    );
    Ok(())
}

#[test]
fn reader_page_hard_cap_does_not_become_new_writer_target() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = Config {
        derived_page_bytes: 16384,
        ..paged()
    };
    let db = Database::open(dir.path(), config)?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            ..TableConfig::default()
        },
    )?;
    db.write("metrics", "wide", vec![wide_row(0)], 0)?;
    db.checkpoint()?;
    drop(db);
    let db = Database::open(dir.path(), paged())?;
    assert_eq!(db.rollups("metrics")?.len(), 1);
    Ok(())
}

#[test]
fn timed_receipt_floor_prunes_atomically_without_losing_aggregates() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Database::open(dir.path(), paged())?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            idempotency_window_us: Some(10),
            ..TableConfig::default()
        },
    )?;
    db.write("metrics", "v1:100:first", vec![row(1, 1.0, "a")], 100)?;
    db.write("metrics", "v1:111:second", vec![row(2, 2.0, "a")], 111)?;
    db.checkpoint()?;
    assert_eq!(db.status()?.idempotency_keys, 1);
    drop(db);
    let db = Database::open(dir.path(), Config::default())?;
    assert_eq!(db.idempotency_floor_us("metrics")?, Some(101));
    assert!(
        db.write("metrics", "v1:100:first", vec![row(1, 1.0, "a")], 90)
            .is_err()
    );
    assert_eq!(db.rollups("metrics")?[0].count, 2);
    assert_eq!(db.scan("metrics", None, None, None, None)?.len(), 2);
    Ok(())
}

fn copy_database(source: &std::path::Path, target: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let destination = target.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_database(&entry.path(), &destination)?;
        } else {
            std::fs::copy(entry.path(), destination)?;
        }
    }
    Ok(())
}

#[test]
fn owned_remote_prefix_cannot_regress_pruned_idempotency_floor() -> Result<()> {
    use std::sync::Arc;
    use varve::remote::{FileStore, RemoteStore};
    for derived_pages in [false, true] {
        let dir = tempfile::tempdir()?;
        let remote = Arc::new(FileStore::new(dir.path().join("remote"))?);
        let original = dir.path().join("original");
        let config = Config {
            derived_pages,
            ..paged()
        };
        let db = Database::open_with_remote(&original, config.clone(), Some(remote.clone()))?;
        db.create_table(
            "metrics",
            TableConfig {
                rollup_widths_us: vec![10],
                idempotency_window_us: Some(20),
                ..TableConfig::default()
            },
        )?;
        db.write("metrics", "v1:100:first", vec![row(1, 1.0, "a")], 100)?;
        db.write("metrics", "v1:300:last", vec![row(2, 2.0, "a")], 100)?;
        db.checkpoint()?;
        db.ship()?;
        drop(db);
        let left_path = dir.path().join("left");
        let right_path = dir.path().join("right");
        copy_database(&original, &left_path)?;
        copy_database(&original, &right_path)?;
        let left = Database::open_with_remote(&left_path, config.clone(), Some(remote.clone()))?;
        assert!(
            left.write("metrics", "v1:300:last", vec![row(2, 2.0, "a")], 310)?
                .duplicate
        );
        assert_eq!(left.idempotency_floor_us("metrics")?, Some(290));
        left.checkpoint()?;
        left.ship()?;
        let sequence = left.status()?.sequence;
        let committed_head = remote.head()?.unwrap().bytes;
        drop(left);
        let right = Database::open_with_remote(&right_path, config, Some(remote.clone()))?;
        assert!(
            right
                .write("metrics", "v1:300:last", vec![row(2, 2.0, "a")], 210)?
                .duplicate
        );
        assert_eq!(right.idempotency_floor_us("metrics")?, Some(190));
        assert_eq!(right.status()?.sequence, sequence);
        right.checkpoint()?;
        assert_eq!(right.status()?.idempotency_keys, 1);
        assert!(
            right.ship().is_err(),
            "must reject regressed floor even when remaining receipts agree (pages={derived_pages})"
        );
        assert_eq!(remote.head()?.unwrap().bytes, committed_head);
    }
    Ok(())
}

#[test]
fn owned_remote_prefix_hydrates_group_receipts_and_rejects_loss() -> Result<()> {
    use std::sync::Arc;
    use varve::remote::{FileStore, RemoteStore};
    for mode in ["matching", "different", "missing_page"] {
        let dir = tempfile::tempdir()?;
        let remote = Arc::new(FileStore::new(dir.path().join("remote"))?);
        let original = dir.path().join("original");
        let db = Database::open_with_remote(&original, paged(), Some(remote.clone()))?;
        db.create_table(
            "metrics",
            TableConfig {
                rollup_widths_us: vec![10],
                ..TableConfig::default()
            },
        )?;
        db.write("metrics", "seed", vec![row(1, 1.0, "a")], 1)?;
        db.checkpoint()?;
        db.ship()?;
        drop(db);
        let left_path = dir.path().join("left");
        let right_path = dir.path().join("right");
        copy_database(&original, &left_path)?;
        copy_database(&original, &right_path)?;
        let requests = |value: f64| {
            vec![
                WriteRequest {
                    table: "metrics".into(),
                    request_id: "a".into(),
                    now_us: 1,
                    rows: vec![row(1, 2.0, "a")],
                },
                WriteRequest {
                    table: "metrics".into(),
                    request_id: "b".into(),
                    now_us: 1,
                    rows: vec![row(1, value, "a")],
                },
            ]
        };
        let left = Database::open_with_remote(&left_path, paged(), Some(remote.clone()))?;
        for result in left.write_group(requests(3.0)) {
            result?;
        }
        left.checkpoint()?;
        left.ship()?;
        drop(left);
        let right = Database::open_with_remote(&right_path, paged(), Some(remote.clone()))?;
        for result in right.write_group(requests(if mode == "different" { 9.0 } else { 3.0 })) {
            result?;
        }
        right.checkpoint()?;
        if mode == "missing_page" {
            let root = root_json(&left_path)?;
            let digest = root["tables"]["metrics"]["derived"]["receipts"]["pages"][0]["digest"]
                .as_str()
                .unwrap();
            remote.delete(&format!("derived/{digest}.page"))?;
        }
        if mode == "matching" {
            right.ship()?;
            assert!(right.status()?.fenced.is_none());
        } else {
            assert!(right.ship().is_err(), "{mode}");
        }
    }
    Ok(())
}

#[test]
fn committed_missing_or_corrupt_page_fails_closed() -> Result<()> {
    for missing in [false, true] {
        let dir = tempfile::tempdir()?;
        let db = Database::open(dir.path(), paged())?;
        db.create_table(
            "metrics",
            TableConfig {
                rollup_widths_us: vec![10],
                ..TableConfig::default()
            },
        )?;
        db.write("metrics", "receipt", vec![row(0, 2.0, "tag")], 0)?;
        db.checkpoint()?;
        drop(db);
        let root = root_json(dir.path())?;
        let digest = root["tables"]["metrics"]["derived"]["rollups"]["pages"][0]["digest"]
            .as_str()
            .unwrap();
        let path = dir.path().join(format!("derived/{digest}.page"));
        if missing {
            std::fs::remove_file(path)?;
        } else {
            let mut bytes = std::fs::read(&path)?;
            bytes.truncate(bytes.len() - 1);
            std::fs::write(path, bytes)?;
        }
        assert!(Database::open(dir.path(), Config::default()).is_err());
    }
    Ok(())
}

#[test]
fn grouped_rollback_and_reopen_preserve_sequential_float_and_tie_state() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = paged();
    let db = Database::open(dir.path(), config.clone())?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            ..TableConfig::default()
        },
    )?;
    let requests = vec![
        WriteRequest {
            table: "metrics".into(),
            request_id: "first".into(),
            now_us: 10,
            rows: vec![row(-1, 1e16, "a"), row(-1, -1e16, "a"), row(-1, 1.0, "a")],
        },
        WriteRequest {
            table: "metrics".into(),
            request_id: "invalid".into(),
            now_us: 10,
            rows: vec![row(-1, f64::NAN, "a")],
        },
        WriteRequest {
            table: "metrics".into(),
            request_id: "second".into(),
            now_us: 10,
            rows: vec![row(-1, -0.0, "b")],
        },
    ];
    let results = db.write_group(requests.clone());
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());
    let rows = db.rollups("metrics")?;
    let a = rows.iter().find(|r| r.tags["tag"] == "a").unwrap();
    assert_eq!(a.sum.to_bits(), 1.0f64.to_bits());
    assert_eq!(a.count, 3);
    assert_eq!(a.bucket_us, -10);
    assert_eq!(a.first.to_bits(), 1e16f64.to_bits());
    assert_eq!(a.last.to_bits(), 1.0f64.to_bits());
    let b = rows.iter().find(|r| r.tags["tag"] == "b").unwrap();
    assert_eq!(b.sum.to_bits(), (-0.0f64).to_bits());
    let before = serde_json::to_vec(&rows)?;
    db.checkpoint()?;
    drop(db);
    let db = Database::open(dir.path(), config)?;
    assert_eq!(serde_json::to_vec(&db.rollups("metrics")?)?, before);
    for request in [&requests[0], &requests[2]] {
        assert!(
            db.write(
                &request.table,
                &request.request_id,
                request.rows.clone(),
                10
            )?
            .duplicate
        );
    }
    assert_eq!(db.scan("metrics", None, None, None, None)?.len(), 4);
    Ok(())
}
