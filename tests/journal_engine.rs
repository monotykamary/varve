use std::collections::BTreeMap;
use std::fs;
use tempfile::TempDir;
use varve::{Config, Database, Row, TableConfig, WriteRequest};

fn config() -> Config {
    Config {
        segmented_journal: true,
        checkpoint_frozen_prefix: true,
        ..Config::default()
    }
}
fn row(timestamp_us: i64, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "series".into(),
        value,
        tags: BTreeMap::new(),
    }
}
fn table() -> TableConfig {
    TableConfig {
        shards: 2,
        window_us: 100,
        rollup_widths_us: vec![10],
        ..TableConfig::default()
    }
}

#[test]
fn real_grouped_commit_replays_rows_rollups_and_idempotency_from_journal() {
    let root = TempDir::new().unwrap();
    let db = Database::open(root.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    let before = db.journal_stats().unwrap().unwrap();
    let receipts = db.write_group(
        (0..4)
            .map(|i| WriteRequest {
                table: "metrics".into(),
                request_id: format!("r-{i}"),
                rows: vec![row(i, i as f64)],
                now_us: 10,
            })
            .collect(),
    );
    let receipts: Vec<_> = receipts.into_iter().map(Result::unwrap).collect();
    assert!(receipts.iter().all(|r| !r.duplicate
        && r.durability == "local_fsync"
        && r.sequence == receipts[0].sequence));
    let after = db.journal_stats().unwrap().unwrap();
    assert_eq!(after.namespace_barriers, before.namespace_barriers);
    assert_eq!(after.file_syncs - before.file_syncs, 1);
    assert_eq!(fs::read_dir(root.path().join("wal")).unwrap().count(), 0);
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 4);
    let rollups = db.rollups("metrics").unwrap();
    assert_eq!(rollups[0].count, 4);
    assert_eq!(rollups[0].sum, 6.0);
    drop(db);
    // Durable format authority, not the process's opt-in bit, chooses replay.
    let db = Database::open(root.path(), Config::default()).unwrap();
    assert!(db.journal_stats().unwrap().is_some());
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 4);
    assert_eq!(db.rollups("metrics").unwrap(), rollups);
    assert!(
        db.write("metrics", "r-2", vec![row(2, 2.0)], 10)
            .unwrap()
            .duplicate
    );
    assert!(db.write("metrics", "r-2", vec![row(2, 9.0)], 10).is_err());
}

fn journal_tree(root: &std::path::Path) -> BTreeMap<std::path::PathBuf, Option<Vec<u8>>> {
    fn visit(
        root: &std::path::Path,
        path: &std::path::Path,
        entries: &mut BTreeMap<std::path::PathBuf, Option<Vec<u8>>>,
    ) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let kind = entry.file_type().unwrap();
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            if kind.is_dir() {
                entries.insert(relative, None);
                visit(root, &path, entries);
            } else {
                assert!(kind.is_file());
                entries.insert(relative, Some(fs::read(path).unwrap()));
            }
        }
    }
    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

fn assert_missing_manifest_rejected(root: &std::path::Path, segmented_journal: bool) {
    let error = Database::open(
        root,
        Config {
            segmented_journal,
            ..config()
        },
    )
    .err()
    .expect("journal artifacts must prevent initialization");
    assert!(
        error
            .to_string()
            .contains("missing manifest in nonempty database"),
        "wrong failure: {error:#}"
    );
    assert!(
        !root.join("manifest.bin").try_exists().unwrap(),
        "failed open must not publish replacement authority"
    );
}

#[test]
fn missing_manifest_preserves_acknowledged_journal_history_and_restored_replay() {
    for sealed in [false, true] {
        for segmented_journal in [false, true] {
            let root = TempDir::new().unwrap();
            let db = Database::open(root.path(), config()).unwrap();
            db.create_table("metrics", table()).unwrap();
            let inputs = vec![row(1, -0.0), row(2, f64::from_bits(0x3ff0000000000001))];
            let receipts: Vec<_> = db
                .write_group(
                    inputs
                        .iter()
                        .enumerate()
                        .map(|(i, row)| WriteRequest {
                            table: "metrics".into(),
                            request_id: format!("ack-{i}"),
                            rows: vec![row.clone()],
                            now_us: 10,
                        })
                        .collect(),
                )
                .into_iter()
                .map(Result::unwrap)
                .collect();
            let status = db.status().unwrap();
            assert!(receipts.iter().all(|receipt| {
                !receipt.duplicate
                    && receipt.durability == "local_fsync"
                    && receipt.sequence > status.checkpoint_sequence
            }));
            let rows = db.scan("metrics", None, None, None, None).unwrap();
            let rollups = db.rollups("metrics").unwrap();
            assert_eq!(rows.len(), inputs.len());
            assert_eq!(status.hot_rows, inputs.len());
            for directory in ["wal", "segments", "derived"] {
                assert_eq!(
                    fs::read_dir(root.path().join(directory)).unwrap().count(),
                    0
                );
            }
            drop(db);
            let journal_path = root.path().join("journal");
            if sealed {
                // Seal actual engine history without checkpointing any of it.
                let mut journal = varve::journal::Journal::open(
                    &journal_path,
                    varve::journal::JournalConfig::default(),
                )
                .unwrap();
                assert!(!journal.seal_snapshot().unwrap().is_empty());
            }
            let before = journal_tree(&journal_path);
            assert!(before.values().flatten().any(|bytes| !bytes.is_empty()));
            let manifest_path = root.path().join("manifest.bin");
            let manifest = fs::read(&manifest_path).unwrap();
            fs::remove_file(&manifest_path).unwrap();

            assert_missing_manifest_rejected(root.path(), segmented_journal);
            assert_eq!(journal_tree(&journal_path), before);

            fs::write(&manifest_path, &manifest).unwrap();
            let db = Database::open(
                root.path(),
                Config {
                    segmented_journal,
                    ..config()
                },
            )
            .unwrap();
            let restored = db.status().unwrap();
            assert_eq!(restored.database_id, status.database_id);
            assert_eq!(restored.sequence, status.sequence);
            assert_eq!(restored.checkpoint_sequence, status.checkpoint_sequence);
            let replayed = db.scan("metrics", None, None, None, None).unwrap();
            assert_eq!(replayed, rows);
            assert_eq!(
                replayed
                    .iter()
                    .map(|row| row.row.value.to_bits())
                    .collect::<Vec<_>>(),
                rows.iter()
                    .map(|row| row.row.value.to_bits())
                    .collect::<Vec<_>>()
            );
            assert_eq!(db.rollups("metrics").unwrap(), rollups);
            for (i, (input, receipt)) in inputs.into_iter().zip(receipts).enumerate() {
                let duplicate = db
                    .write("metrics", &format!("ack-{i}"), vec![input], 10)
                    .unwrap();
                assert!(duplicate.duplicate);
                assert_eq!(duplicate.sequence, receipt.sequence);
                assert_eq!(duplicate.rows, receipt.rows);
                assert_eq!(duplicate.durability, receipt.durability);
                assert!(
                    db.write(
                        "metrics",
                        &format!("ack-{i}"),
                        vec![row(i as i64 + 1, 9.0)],
                        10,
                    )
                    .is_err()
                );
            }
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap(), rows);
            assert_eq!(fs::read(&manifest_path).unwrap(), manifest);
            assert_eq!(journal_tree(&journal_path), before);
        }
    }
}

#[test]
fn missing_manifest_rejects_malformed_and_unrecognized_journal_entries_unchanged() {
    for segmented_journal in [false, true] {
        for artifact in [
            "malformed",
            "unknown",
            "empty-file",
            "lock",
            "directory",
            "nested",
        ] {
            let root = TempDir::new().unwrap();
            let journal_path = root.path().join("journal");
            fs::create_dir(&journal_path).unwrap();
            match artifact {
                "malformed" => fs::write(
                    journal_path.join("segment-00000000000000000001.jrn"),
                    b"not a valid journal header",
                )
                .unwrap(),
                "unknown" => fs::write(journal_path.join("unrecognized"), b"history").unwrap(),
                "empty-file" => fs::write(journal_path.join("unrecognized"), b"").unwrap(),
                "lock" => fs::write(journal_path.join("LOCK"), b"").unwrap(),
                "directory" => fs::create_dir(journal_path.join("unrecognized")).unwrap(),
                "nested" => {
                    fs::create_dir(journal_path.join("unrecognized")).unwrap();
                    fs::write(journal_path.join("unrecognized/history"), b"history").unwrap();
                }
                _ => unreachable!(),
            }
            let before = journal_tree(&journal_path);
            assert_missing_manifest_rejected(root.path(), segmented_journal);
            assert_eq!(journal_tree(&journal_path), before, "artifact: {artifact}");
        }
    }
}

#[test]
fn missing_manifest_rejects_non_directory_journal_path_unchanged() {
    for segmented_journal in [false, true] {
        let root = TempDir::new().unwrap();
        let journal_path = root.path().join("journal");
        fs::write(&journal_path, b"history").unwrap();
        assert_missing_manifest_rejected(root.path(), segmented_journal);
        assert_eq!(fs::read(journal_path).unwrap(), b"history");
    }
}

#[cfg(unix)]
#[test]
fn missing_manifest_rejects_dangling_journal_symlink_unchanged() {
    for segmented_journal in [false, true] {
        let root = TempDir::new().unwrap();
        let journal_path = root.path().join("journal");
        let target = root.path().join("absent-journal");
        std::os::unix::fs::symlink(&target, &journal_path).unwrap();
        assert_missing_manifest_rejected(root.path(), segmented_journal);
        assert_eq!(fs::read_link(journal_path).unwrap(), target);
        assert!(!target.try_exists().unwrap());
    }
}

#[test]
fn missing_manifest_allows_only_empty_new_journal_directory() {
    for segmented_journal in [false, true] {
        let root = TempDir::new().unwrap();
        fs::create_dir(root.path().join("journal")).unwrap();
        let cfg = Config {
            segmented_journal,
            ..config()
        };
        let db = Database::open(root.path(), cfg.clone()).unwrap();
        let initial = db.status().unwrap();
        assert_eq!(initial.sequence, 0);
        assert_eq!(initial.checkpoint_sequence, 0);
        assert_eq!(initial.tables, 0);
        assert_eq!(initial.wal_bytes, 0);
        db.create_table("metrics", table()).unwrap();
        let receipt = db.write("metrics", "new", vec![row(1, 2.0)], 10).unwrap();
        assert!(!receipt.duplicate);
        assert_eq!(receipt.durability, "local_fsync");
        drop(db);
        let db = Database::open(root.path(), cfg).unwrap();
        assert_eq!(db.status().unwrap().database_id, initial.database_id);
        let rows = db.scan("metrics", None, None, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row, row(1, 2.0));
        assert_eq!(rows[0].sequence, receipt.sequence);
        let duplicate = db.write("metrics", "new", vec![row(1, 2.0)], 10).unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.sequence, receipt.sequence);
        assert_eq!(duplicate.rows, receipt.rows);
        assert_eq!(duplicate.durability, receipt.durability);
    }
}

#[test]
fn journal_checkpoint_reclaims_all_then_reopens_and_appends_at_next_sequence() {
    for derived_pages in [false, true] {
        let root = TempDir::new().unwrap();
        let mut cfg = config();
        cfg.derived_pages = derived_pages;
        let db = Database::open(root.path(), cfg.clone()).unwrap();
        db.create_table("metrics", table()).unwrap();
        let receipt = db.write("metrics", "first", vec![row(0, -0.0)], 1).unwrap();
        db.checkpoint().unwrap();
        assert_eq!(db.status().unwrap().wal_bytes, 0);
        assert_eq!(db.journal_stats().unwrap().unwrap().disk_bytes, 0);
        drop(db);
        let db = Database::open(root.path(), cfg).unwrap();
        let second = db.write("metrics", "second", vec![row(1, 2.0)], 2).unwrap();
        assert_eq!(second.sequence, receipt.sequence + 1);
        let rows = db.scan("metrics", None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .find(|row| row.row.timestamp_us == 0)
                .unwrap()
                .row
                .value
                .to_bits(),
            (-0.0f64).to_bits()
        );
        drop(db);
        let db = Database::open(root.path(), Config::default()).unwrap();
        assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
    }
}

#[test]
fn explicit_legacy_migration_requires_checkpoint_and_preserves_receipts() {
    let root = TempDir::new().unwrap();
    let db = Database::open(root.path(), Config::default()).unwrap();
    db.create_table("metrics", table()).unwrap();
    let first = db.write("metrics", "legacy", vec![row(1, 3.0)], 2).unwrap();
    drop(db);
    assert!(Database::open(root.path(), config()).is_err());
    let db = Database::open(root.path(), Config::default()).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(root.path(), config()).unwrap();
    assert!(
        db.write("metrics", "legacy", vec![row(1, 3.0)], 2)
            .unwrap()
            .duplicate
    );
    assert_eq!(
        db.write("metrics", "new", vec![row(2, 4.0)], 3)
            .unwrap()
            .sequence,
        first.sequence + 1
    );
    drop(db);
    assert_eq!(
        Database::open(root.path(), Config::default())
            .unwrap()
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn bounded_journal_pressure_checkpoints_without_losing_acknowledged_rows() {
    let root = TempDir::new().unwrap();
    let cfg = Config {
        segmented_journal: true,
        checkpoint_frozen_prefix: true,
        wal_max_bytes: 8192,
        max_batch_bytes: 2048,
        hot_max_bytes: 65536,
        segment_rows: 32,
        ..Config::default()
    };
    let db = Database::open(root.path(), cfg.clone()).unwrap();
    db.create_table("metrics", table()).unwrap();
    for i in 0..80 {
        db.write("metrics", &format!("r-{i}"), vec![row(i, i as f64)], i + 1)
            .unwrap();
    }
    assert_eq!(
        db.scan("metrics", None, None, None, None).unwrap().len(),
        80
    );
    assert!(db.status().unwrap().wal_bytes <= cfg.wal_max_bytes);
    drop(db);
    let db = Database::open(root.path(), cfg).unwrap();
    assert_eq!(
        db.scan("metrics", None, None, None, None).unwrap().len(),
        80
    );
    for i in 0..80 {
        assert!(
            db.write("metrics", &format!("r-{i}"), vec![row(i, i as f64)], i + 1)
                .unwrap()
                .duplicate
        );
    }
}
