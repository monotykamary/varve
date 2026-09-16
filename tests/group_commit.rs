use std::{collections::BTreeSet, fs, path::Path, sync::Arc};
use tempfile::TempDir;
use varve::remote::FileStore;
use varve::{Config, Database, Row, TableConfig, WriteRequest};

// Fork/exec during fault tests can transiently inherit another test's file locks.
static TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn request(id: &str, ts: i64, value: f64) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: id.into(),
        now_us: 10,
        rows: vec![Row {
            timestamp_us: ts,
            tenant: "tenant".into(),
            series: "cpu".into(),
            value,
            tags: Default::default(),
        }],
    }
}
fn table() -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 100,
        rollup_widths_us: vec![10],
        ..Default::default()
    }
}
fn open(path: &Path, config: Config) -> Database {
    let db = Database::open(path, config).unwrap();
    db.create_table("metrics", table()).unwrap();
    db
}
fn frames(path: &Path) -> usize {
    fs::read_dir(path.join("wal"))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|s| s == "wal")
        })
        .count()
}

#[test]
fn one_frame_isolates_errors_retries_and_orders_ohlc() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let temp = TempDir::new().unwrap();
    let db = open(temp.path(), Config::default());
    db.create_table("other", table()).unwrap();
    let before = frames(temp.path());
    let mut unknown = request("bad-table", 2, 99.0);
    unknown.table = "missing".into();
    let mut other = request("other", 2, 12.0);
    other.table = "other".into();
    let results = db.write_group(vec![
        request("a", 2, 2.0),
        unknown,
        request("a", 2, 2.0),
        request("a", 2, 9.0),
        request("b", 1, 1.0),
        other,
        request("c", 2, 3.0),
    ]);
    assert_eq!(results.len(), 7);
    assert!(results[1].is_err());
    assert!(results[3].is_err());
    assert!(results[2].as_ref().unwrap().duplicate);
    let seq = results[0].as_ref().unwrap().sequence;
    for index in [0, 2, 4, 5, 6] {
        assert_eq!(results[index].as_ref().unwrap().sequence, seq);
    }
    // One immutable frame invokes one file sync and one directory sync, not one per item.
    assert_eq!(frames(temp.path()) - before, 1);
    let bytes = fs::read(temp.path().join("wal").join(format!("{seq:020}.wal"))).unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&bytes[16..bytes.len() - 32]).unwrap();
    assert_eq!(payload["operation"]["operation"], "append_group");
    assert_eq!(payload["operation"]["items"].as_array().unwrap().len(), 4);
    let mut all = db.scan("metrics", None, None, None, None).unwrap();
    all.extend(db.scan("other", None, None, None, None).unwrap());
    assert_eq!(
        all.iter()
            .map(|r| (r.sequence, r.ordinal))
            .collect::<BTreeSet<_>>()
            .len(),
        4
    );
    let rollup = db.rollups("metrics").unwrap().remove(0);
    assert_eq!(
        (rollup.count, rollup.sum, rollup.first, rollup.last),
        (3, 6.0, 1.0, 3.0)
    );
    let before_retry = frames(temp.path());
    assert!(
        db.write_group(vec![request("a", 2, 2.0)])[0]
            .as_ref()
            .unwrap()
            .duplicate
    );
    assert!(db.write_group(vec![request("a", 2, 8.0)])[0].is_err());
    assert_eq!(frames(temp.path()), before_retry);
    drop(db);
    let db = open(temp.path(), Config::default());
    assert_eq!(db.rollups("metrics").unwrap()[0], rollup);
    assert!(
        db.write_group(vec![request("c", 2, 3.0)])[0]
            .as_ref()
            .unwrap()
            .duplicate
    );
    db.checkpoint().unwrap();
    assert_eq!(frames(temp.path()), 0);
    drop(db);
    let db = open(temp.path(), Config::default());
    assert_eq!(db.rollups("metrics").unwrap()[0], rollup);
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
    assert!(
        db.write_group(vec![request("b", 1, 1.0)])[0]
            .as_ref()
            .unwrap()
            .duplicate
    );
}

#[test]
fn invalid_rows_and_aggregate_overflow_do_not_poison_other_requests() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let temp = TempDir::new().unwrap();
    let db = open(temp.path(), Config::default());
    let results = db.write_group(vec![
        request("a", 1, 1.0),
        request("nan", 1, f64::NAN),
        request("large", 1, f64::MAX),
        request("overflow", 1, f64::MAX),
        request("b", 1, -1.0),
    ]);
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());
    assert!(results[3].is_err());
    assert!(results[4].is_ok());
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 3);
    assert!(db.status().unwrap().fenced.is_none());
    drop(db);
    let db = open(temp.path(), Config::default());
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
}

#[test]
fn durable_retry_at_hot_capacity_needs_no_publication() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let temp = TempDir::new().unwrap();
    let db = open(
        temp.path(),
        Config {
            hot_max_rows: 2,
            max_batch_rows: 2,
            ..Default::default()
        },
    );
    assert!(
        db.write_group(vec![request("a", 1, 1.0), request("b", 1, 2.0)])
            .iter()
            .all(Result::is_ok)
    );
    let before = db.status().unwrap();
    let receipts = db.write_group(vec![request("a", 1, 1.0), request("b", 1, 2.0)]);
    assert!(receipts.iter().all(|r| r.as_ref().unwrap().duplicate));
    let after = db.status().unwrap();
    assert_eq!(after.sequence, before.sequence);
    assert_eq!(after.checkpoint_sequence, before.checkpoint_sequence);
    assert_eq!(after.wal_bytes, before.wal_bytes);
    assert_eq!(after.hot_rows, 2);
}

#[test]
fn bounds_split_groups_and_reject_only_excess_metadata() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let temp = TempDir::new().unwrap();
    let config = Config {
        max_batch_rows: 2,
        max_idempotency_keys: 3,
        ..Default::default()
    };
    let db = open(temp.path(), config.clone());
    let before = frames(temp.path());
    let results = db.write_group(
        (0..4)
            .map(|i| request(&format!("r{i}"), 1, i as f64))
            .collect(),
    );
    assert!(results[..3].iter().all(Result::is_ok));
    assert!(results[3].is_err());
    assert_eq!(frames(temp.path()) - before, 2);
    assert_eq!(db.status().unwrap().idempotency_keys, 3);
    drop(db);
    let db = open(temp.path(), config);
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
}

#[test]
fn hot_wal_checkpoint_and_remote_restore_preserve_group_receipts() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let temp = TempDir::new().unwrap();
    let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let config = Config {
        hot_max_rows: 3,
        max_batch_rows: 3,
        ..Default::default()
    };
    let root = temp.path().join("local");
    let db = Database::open_with_remote(&root, config.clone(), Some(store.clone())).unwrap();
    db.create_table("metrics", table()).unwrap();
    for base in [0, 3] {
        assert!(
            db.write_group(
                (base..base + 3)
                    .map(|i| request(&format!("r{i}"), 1, i as f64))
                    .collect()
            )
            .iter()
            .all(Result::is_ok)
        );
    }
    assert!(db.status().unwrap().checkpoint_sequence > 0);
    assert_eq!(db.status().unwrap().hot_rows, 3);
    db.ship().unwrap();
    drop(db);
    // Reopening with a bound remote head proves each group item is a local prefix.
    let db = Database::open_with_remote(&root, config.clone(), Some(store.clone())).unwrap();
    db.ship().unwrap();
    let restored = Database::restore(temp.path().join("restored"), config, store).unwrap();
    assert_eq!(
        restored
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        6
    );
    assert_eq!(restored.rollups("metrics").unwrap()[0].count, 6);
    assert!(
        restored.write_group(vec![request("r5", 1, 5.0)])[0]
            .as_ref()
            .unwrap()
            .duplicate
    );
    assert!(db.ship().is_err());
}

fn copy_tree(source: &Path, target: &Path) {
    fs::create_dir_all(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &destination);
        } else {
            fs::copy(entry.path(), destination).unwrap();
        }
    }
}

#[test]
fn reconciliation_binds_every_group_item_not_just_the_first_receipt() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for difference in ["matching", "digest", "missing", "rows"] {
        let temp = TempDir::new().unwrap();
        let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
        let original = temp.path().join("original");
        let db =
            Database::open_with_remote(&original, Config::default(), Some(store.clone())).unwrap();
        db.create_table("metrics", table()).unwrap();
        // Establish an owned, bound prefix using the unchanged single-write operation.
        db.write("metrics", "seed", request("seed", 0, 0.0).rows, 0)
            .unwrap();
        db.checkpoint().unwrap();
        db.ship().unwrap();
        drop(db);
        let left_path = temp.path().join("left");
        let right_path = temp.path().join("right");
        copy_tree(&original, &left_path);
        copy_tree(&original, &right_path);
        let left =
            Database::open_with_remote(&left_path, Config::default(), Some(store.clone())).unwrap();
        let left_results = left.write_group(vec![request("a", 1, 1.0), request("b", 2, 2.0)]);
        assert!(left_results.iter().all(Result::is_ok));
        left.ship().unwrap();
        drop(left);
        let right = Database::open_with_remote(&right_path, Config::default(), Some(store.clone()))
            .unwrap();
        let mut later = request("b", 2, 2.0);
        match difference {
            "digest" => later.rows[0].value = 9.0,
            "missing" => later.request_id = "different-id".into(),
            "rows" => later.rows.push(later.rows[0].clone()),
            _ => (),
        }
        let results = right.write_group(vec![request("a", 1, 1.0), later]);
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(
            results[0].as_ref().unwrap().sequence,
            left_results[0].as_ref().unwrap().sequence
        );
        if difference == "matching" {
            right.ship().unwrap();
            assert!(right.status().unwrap().fenced.is_none());
            // Both legacy and group receipts remain durable after reconciliation.
            assert!(
                right
                    .write("metrics", "seed", request("seed", 0, 0.0).rows, 10)
                    .unwrap()
                    .duplicate
            );
            assert!(
                right.write_group(vec![request("b", 2, 2.0)])[0]
                    .as_ref()
                    .unwrap()
                    .duplicate
            );
        } else {
            let error = right.ship().unwrap_err();
            let message = format!("{error:#}");
            assert!(
                message.contains("prefix proof failed")
                    && message.contains("metrics/a")
                    && message.contains("group proof required"),
                "{difference}: {message}"
            );
            assert!(right.status().unwrap().fenced.is_some());
        }
        drop(right);
        let restored =
            Database::restore(temp.path().join("restored"), Config::default(), store).unwrap();
        assert_eq!(
            restored
                .scan("metrics", None, None, None, None)
                .unwrap()
                .len(),
            3
        );
        assert!(
            restored.write_group(vec![request("b", 2, 2.0)])[0]
                .as_ref()
                .unwrap()
                .duplicate
        );
        assert!(
            restored
                .write("metrics", "seed", request("seed", 0, 0.0).rows, 10)
                .unwrap()
                .duplicate
        );
    }
}

fn divergent_group_prefix(difference: &str, remote_checkpoint: bool) {
    let temp = TempDir::new().unwrap();
    let store = Arc::new(FileStore::new(temp.path().join("remote")).unwrap());
    let original = temp.path().join("original");
    let db = Database::open_with_remote(&original, Config::default(), Some(store.clone())).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            idempotency_window_us: (difference == "subset").then_some(10),
            ..table()
        },
    )
    .unwrap();
    db.checkpoint().unwrap();
    db.ship().unwrap();
    drop(db);
    let left_path = temp.path().join("left");
    let right_path = temp.path().join("right");
    copy_tree(&original, &left_path);
    copy_tree(&original, &right_path);
    let left =
        Database::open_with_remote(&left_path, Config::default(), Some(store.clone())).unwrap();
    let right =
        Database::open_with_remote(&right_path, Config::default(), Some(store.clone())).unwrap();
    let (a, b) = if difference == "subset" {
        (
            WriteRequest {
                now_us: 100,
                ..request("v1:100:a", 1, 1.0)
            },
            WriteRequest {
                now_us: 100,
                ..request("v1:90:b", 1, 2.0)
            },
        )
    } else {
        (request("a", 1, 1.0), request("b", 1, 2.0))
    };
    let remote_items = if difference == "subset" {
        vec![a.clone()]
    } else {
        vec![a.clone(), b.clone()]
    };
    let local_items = if difference == "order" {
        vec![b, a]
    } else if difference == "clock" {
        vec![WriteRequest { now_us: 11, ..a }, b]
    } else {
        vec![a, b]
    };
    let remote_results = left.write_group(remote_items);
    let local_results = right.write_group(local_items);
    assert!(remote_results.iter().all(Result::is_ok));
    assert!(local_results.iter().all(Result::is_ok));
    assert_eq!(
        remote_results[0].as_ref().unwrap().sequence,
        local_results[0].as_ref().unwrap().sequence
    );
    let remote_rollup = left.rollups("metrics").unwrap().remove(0);
    let local_rollup = right.rollups("metrics").unwrap().remove(0);
    if difference == "order" {
        assert_eq!((remote_rollup.first, remote_rollup.last), (1.0, 2.0));
        assert_eq!((local_rollup.first, local_rollup.last), (2.0, 1.0));
    } else if difference == "subset" {
        assert_eq!((remote_rollup.count, remote_rollup.last), (1, 1.0));
        assert_eq!((local_rollup.count, local_rollup.last), (2, 2.0));
    } else {
        assert_eq!(remote_rollup, local_rollup);
    }
    // Pruning the extra member defeats a reverse scan of retained receipts.
    if difference == "subset" {
        assert!(
            right.write_group(vec![WriteRequest {
                now_us: 105,
                ..request("v1:100:a", 1, 1.0)
            }])[0]
                .as_ref()
                .unwrap()
                .duplicate
        );
    }
    // The proof must survive removal of every local group WAL frame.
    right.checkpoint().unwrap();
    if difference == "subset" {
        assert_eq!(right.status().unwrap().idempotency_keys, 1);
    }
    assert_eq!(frames(&right_path), 0);
    drop(right);
    if difference == "missing-proof" {
        // Simulate a readable legacy checkpoint that cannot prove a group.
        let path = right_path.join("manifest.bin");
        let bytes = fs::read(&path).unwrap();
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&bytes[8..bytes.len() - 32]).unwrap();
        for receipt in manifest["tables"]["metrics"]["receipts"]
            .as_object_mut()
            .unwrap()
            .values_mut()
        {
            receipt.as_object_mut().unwrap().remove("group_fingerprint");
        }
        let mut bytes = b"VARVEM01".to_vec();
        bytes.extend(serde_json::to_vec(&manifest).unwrap());
        bytes.extend(blake3::hash(&bytes).as_bytes());
        fs::write(path, bytes).unwrap();
    }
    let right =
        Database::open_with_remote(&right_path, Config::default(), Some(store.clone())).unwrap();
    if remote_checkpoint {
        left.checkpoint().unwrap();
    }
    left.ship().unwrap();
    let error = right
        .ship()
        .expect_err("divergent group must not reconcile");
    assert!(
        format!("{error:#}").contains("prefix proof failed"),
        "{error:#}"
    );
    assert!(right.status().unwrap().fenced.is_some());
    let restored =
        Database::restore(temp.path().join("restored"), Config::default(), store).unwrap();
    assert_eq!(restored.rollups("metrics").unwrap()[0], remote_rollup);
}

#[test]
fn reconciliation_rejects_reordered_group_after_local_checkpoint() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for remote_checkpoint in [false, true] {
        divergent_group_prefix("order", remote_checkpoint);
    }
}

#[test]
fn reconciliation_rejects_proper_subset_group_after_local_checkpoint() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for remote_checkpoint in [false, true] {
        divergent_group_prefix("subset", remote_checkpoint);
    }
}

#[test]
fn reconciliation_requires_group_clocks_and_durable_proof() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for difference in ["clock", "missing-proof"] {
        for remote_checkpoint in [false, true] {
            divergent_group_prefix(difference, remote_checkpoint);
        }
    }
}

#[test]
fn timed_group_receipts_keep_monotonic_checkpoint_floor() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), Config::default()).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            idempotency_window_us: Some(10),
            ..table()
        },
    )
    .unwrap();
    let timed = |id: &str, now_us: i64| WriteRequest {
        now_us,
        ..request(id, 1, 1.0)
    };
    let results = db.write_group(vec![
        timed("v1:100:a", 100),
        timed("v1:100:b", 100),
        timed("v1:100:a", 100),
    ]);
    assert!(results.iter().all(Result::is_ok));
    assert!(results[2].as_ref().unwrap().duplicate);
    drop(db);
    let db = Database::open(temp.path(), Config::default()).unwrap();
    assert!(
        db.write_group(vec![timed("v1:100:a", 105)])[0]
            .as_ref()
            .unwrap()
            .duplicate
    );
    let results = db.write_group(vec![timed("v1:100:a", 111), timed("v1:111:c", 111)]);
    assert!(results[0].is_err());
    assert!(results[1].is_ok());
    db.checkpoint().unwrap();
    assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(101));
    assert_eq!(db.status().unwrap().idempotency_keys, 1);
    drop(db);
    let db = Database::open(temp.path(), Config::default()).unwrap();
    let results = db.write_group(vec![timed("v1:100:a", 90), timed("v1:111:c", 90)]);
    assert!(results[0].is_err());
    assert!(results[1].as_ref().unwrap().duplicate);
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
}

#[test]
fn corruption_and_semantically_invalid_groups_fail_closed() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for mutation in ["checksum", "duplicate", "digest", "empty", "oversized"] {
        let temp = TempDir::new().unwrap();
        let db = open(temp.path(), Config::default());
        let seq = db.write_group(vec![request("a", 1, 1.0), request("b", 1, 2.0)])[0]
            .as_ref()
            .unwrap()
            .sequence;
        drop(db);
        let path = temp.path().join("wal").join(format!("{seq:020}.wal"));
        let mut bytes = fs::read(&path).unwrap();
        if mutation == "checksum" {
            bytes[20] ^= 1;
        } else {
            let mut payload: serde_json::Value =
                serde_json::from_slice(&bytes[16..bytes.len() - 32]).unwrap();
            let items = payload["operation"]["items"].as_array_mut().unwrap();
            match mutation {
                "duplicate" => items[1] = items[0].clone(),
                "digest" => items[1]["digest"] = "wrong".into(),
                "empty" => items.clear(),
                "oversized" => *items = vec![items[0].clone(); 1025],
                _ => unreachable!(),
            }
            let payload = serde_json::to_vec(&payload).unwrap();
            bytes = b"VARVEW01".to_vec();
            bytes.extend((payload.len() as u64).to_le_bytes());
            bytes.extend(payload);
            bytes.extend(blake3::hash(&bytes).as_bytes());
        }
        fs::write(path, bytes).unwrap();
        assert!(
            Database::open(temp.path(), Config::default()).is_err(),
            "{mutation}"
        );
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn group_fault_child() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let Ok(root) = std::env::var("VARVE_GROUP_CHILD_ROOT") else {
        return;
    };
    let db = open(Path::new(&root), Config::default());
    let results = db.write_group(vec![
        request("a", 1, 1.0),
        request("a", 1, 1.0),
        request("a", 1, 9.0),
        request("b", 1, 2.0),
        request("seed", 0, 0.0),
    ]);
    assert!(results[0].is_err());
    assert!(results[1].is_err());
    assert!(results[2].is_err());
    assert!(results[3].is_err());
    assert!(results[4].as_ref().unwrap().duplicate);
    assert!(db.status().unwrap().fenced.is_some());
    assert_eq!(db.status().unwrap().hot_rows, 1);
    assert_eq!(db.status().unwrap().idempotency_keys, 1);
}

#[cfg(feature = "fault-injection")]
#[test]
fn group_publication_crash_and_io_matrix() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for (point, present, io) in [
        ("wal_synced", false, false),
        ("wal_published", true, false),
        ("group_before_apply", true, false),
        ("group_applied", true, false),
        ("wal_before_write", false, true),
        ("wal_before_sync", false, true),
        ("wal_before_rename", false, true),
        ("wal_before_dir_sync", true, true),
    ] {
        let temp = TempDir::new().unwrap();
        let db = open(temp.path(), Config::default());
        db.write("metrics", "seed", request("seed", 0, 0.0).rows, 0)
            .unwrap();
        drop(db);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "group_fault_child", "--nocapture"])
            .env("VARVE_GROUP_CHILD_ROOT", temp.path())
            .env(
                if io {
                    "VARVE_IO_FAILPOINT"
                } else {
                    "VARVE_FAILPOINT"
                },
                point,
            )
            .output()
            .unwrap();
        assert_eq!(
            status.status.code(),
            Some(if io { 0 } else { 86 }),
            "{point}: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        let db = open(temp.path(), Config::default());
        assert_eq!(
            db.scan("metrics", None, None, None, None).unwrap().len(),
            if present { 3 } else { 1 },
            "{point}"
        );
        let retry = db.write_group(vec![request("a", 1, 1.0), request("b", 1, 2.0)]);
        for result in retry {
            assert_eq!(result.unwrap().duplicate, present, "{point}");
        }
    }
}
