use super::*;
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn private_multitable_deltas_restore_absent_hot_and_rollup_entries() {
    for fail in [false, true] {
        let dir = TempDir::new().unwrap();
        let config = Config {
            derived_pages: true,
            ..Config::default()
        };
        let db = Database::open(dir.path(), config.clone()).unwrap();
        let table = TableConfig {
            shards: 1,
            rollup_widths_us: vec![10],
            ..Default::default()
        };
        db.create_table("a", table.clone()).unwrap();
        db.create_table("b", table).unwrap();
        let row = Row {
            timestamp_us: 1,
            tenant: "t".into(),
            series: "s".into(),
            value: 2.0,
            tags: BTreeMap::new(),
        };
        db.write("a", "seed", vec![row.clone()], 1).unwrap();
        let before = db.status().unwrap();
        let catalog = serde_json::to_vec(&db.lock().unwrap().catalog).unwrap();
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::WalBeforeSync);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let writer = db.clone();
        let write = std::thread::spawn(move || {
            writer.write_group(vec![
                WriteRequest {
                    table: "a".into(),
                    request_id: "next".into(),
                    rows: vec![row.clone()],
                    now_us: 1,
                },
                WriteRequest {
                    table: "b".into(),
                    request_id: "first".into(),
                    rows: vec![row],
                    now_us: 1,
                },
            ])
        });
        assert!(hook.wait_until_blocked(Duration::from_secs(10)));
        let reader = db.clone();
        let (tx, rx) = mpsc::channel();
        let read = std::thread::spawn(move || {
            let s = reader.lock().unwrap();
            tx.send((
                serde_json::to_vec(&s.catalog).unwrap(),
                s.hot.contains_key("b"),
                s.rollup_indexes["b"].resident_bytes(),
                s.metadata_bytes,
                s.derived_resident_bytes,
            ))
            .unwrap();
        });
        let observed = rx.recv_timeout(Duration::from_secs(10));
        if fail {
            hook.release_with_error();
        } else {
            hook.release();
        }
        let (live_catalog, b_hot, index_bytes, metadata, derived) =
            observed.expect("multitable snapshot blocked");
        read.join().unwrap();
        assert_eq!(live_catalog, catalog);
        assert!(!b_hot);
        assert_eq!(index_bytes, 0);
        assert_eq!(metadata, before.metadata_bytes);
        assert_eq!(derived, before.derived_resident_bytes);
        let result = write.join().unwrap();
        assert!(result.iter().all(|result| result.is_err() == fail));
        if !fail {
            assert_eq!(
                result[0].as_ref().unwrap().sequence,
                result[1].as_ref().unwrap().sequence
            );
            assert_eq!(db.scan("a", None, None, None, None).unwrap().len(), 2);
            assert_eq!(db.scan("b", None, None, None, None).unwrap().len(), 1);
            assert_eq!(db.rollups("b").unwrap()[0].count, 1);
        }
        drop(db);
        let reopened = Database::open(dir.path(), config).unwrap();
        assert_eq!(
            reopened.scan("a", None, None, None, None).unwrap().len(),
            if fail { 1 } else { 2 }
        );
        assert_eq!(
            reopened.scan("b", None, None, None, None).unwrap().len(),
            if fail { 0 } else { 1 }
        );
        assert_eq!(
            reopened.rollups("b").unwrap().len(),
            if fail { 0 } else { 1 }
        );
    }
}
