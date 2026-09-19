use tempfile::TempDir;
use varve::{Config, Database, Row, TableConfig, WriteRequest};

fn row(timestamp_us: i64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "series".into(),
        value: 1.0,
        tags: Default::default(),
    }
}

#[test]
fn grouped_receipt_pressure_accepts_reclamation_without_sequence_advance() {
    for (frozen, pages, mixed) in [false, true].into_iter().flat_map(|frozen| {
        [false, true].into_iter().flat_map(move |pages| {
            [false, true]
                .into_iter()
                .map(move |mixed| (frozen, pages, mixed))
        })
    }) {
        let temp = TempDir::new().unwrap();
        let config = Config {
            checkpoint_frozen_prefix: frozen,
            derived_pages: pages,
            max_idempotency_keys: 2,
            ..Config::default()
        };
        let db = Database::open(temp.path(), config.clone()).unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                idempotency_window_us: Some(100),
                ..TableConfig::default()
            },
        )
        .unwrap();
        db.write("metrics", "v1:10:old", vec![row(10)], 10).unwrap();
        db.write("metrics", "v1:90:recent", vec![row(90)], 90)
            .unwrap();
        db.checkpoint().unwrap();
        let checkpoint = db.status().unwrap().checkpoint_sequence;
        assert_eq!(db.status().unwrap().idempotency_keys, 2);
        if !mixed {
            assert!(
                db.write("metrics", "v1:90:recent", vec![row(90)], 150)
                    .unwrap()
                    .duplicate
            );
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(50));
        } else {
            // The mixed group's own durable duplicate must advance the floor
            // and make same-frontier receipt reclamation eligible.
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(-10));
        }
        assert_eq!(db.status().unwrap().sequence, checkpoint);

        let mut requests = if mixed {
            vec![
                request("v1:90:recent", 90, 150),
                request("v1:90:recent", 91, 150),
            ]
        } else {
            Vec::new()
        };
        requests.push(WriteRequest {
            table: "metrics".into(),
            request_id: "v1:150:next".into(),
            rows: vec![row(150)],
            now_us: 150,
        });
        let mut result = db.write_group(requests);
        if mixed {
            assert!(result[0].as_ref().unwrap().duplicate);
            assert!(format!("{:#}", result[1].as_ref().unwrap_err()).contains("conflicts"));
            result.drain(..2);
        }
        assert!(result[0].is_ok(), "frozen={frozen}: {:?}", result[0]);
        assert_eq!(result[0].as_ref().unwrap().sequence, checkpoint + 1);
        assert_eq!(db.status().unwrap().checkpoint_sequence, checkpoint);
        assert_eq!(db.status().unwrap().idempotency_keys, 2);
        assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
        drop(db);

        let reopened = Database::open(temp.path(), config).unwrap();
        assert_eq!(reopened.idempotency_floor_us("metrics").unwrap(), Some(50));
        assert_eq!(reopened.status().unwrap().idempotency_keys, 2);
        assert_eq!(
            reopened
                .scan("metrics", None, None, None, None)
                .unwrap()
                .len(),
            3
        );
    }
}

fn request(id: &str, timestamp: i64, now_us: i64) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: id.into(),
        rows: vec![row(timestamp)],
        now_us,
    }
}

#[cfg(feature = "fault-injection")]
mod faults {
    use super::*;
    use std::time::Duration;
    use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};

    fn block(db: &Database, phase: MaintenanceHookPhase) -> MaintenanceTestHook {
        let hook = MaintenanceTestHook::new(phase);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        hook
    }

    #[test]
    fn durable_duplicates_survive_stale_capacity_and_clean_admission_in_input_order() {
        for failure in ["stale", "capacity", "clean"] {
            let temp = TempDir::new().unwrap();
            let config = Config {
                checkpoint_frozen_prefix: true,
                hot_max_rows: 4,
                max_idempotency_keys: if failure == "capacity" { 1 } else { 100 },
                ..Default::default()
            };
            let db = Database::open(temp.path(), config.clone()).unwrap();
            db.create_table("metrics", TableConfig::default()).unwrap();
            let receipt = db.write("metrics", "durable", vec![row(1); 4], 1).unwrap();
            if failure == "clean" {
                db.checkpoint().unwrap();
            }
            let requests = vec![
                WriteRequest {
                    rows: vec![row(1); 4],
                    ..request("durable", 1, 1)
                },
                request("durable", 2, 1),
                WriteRequest {
                    table: "missing".into(),
                    ..request("invalid", 1, 1)
                },
                request("new", 2, 2),
            ];
            // Call the public path with enough envelope to keep the mixed group
            // together: duplicate rows count toward the grouping envelope too.
            drop(db);
            let config = Config {
                hot_max_rows: 8,
                ..config
            };
            let db = Database::open(temp.path(), config.clone()).unwrap();
            if failure != "capacity" && failure != "clean" {
                db.write("metrics", "fill", vec![row(3); 4], 3).unwrap();
            }
            let hook = if failure == "stale" {
                Some(block(&db, MaintenanceHookPhase::RootPrepare))
            } else {
                None
            };
            let worker = db.clone();
            let group = std::thread::spawn(move || worker.write_group(requests));
            if let Some(hook) = &hook {
                assert!(hook.wait_until_blocked(Duration::from_secs(5)));
                db.create_table("control", TableConfig::default()).unwrap();
                hook.release();
            }
            let results = group.join().unwrap();
            assert_eq!(results.len(), 4);
            assert_eq!(results[0].as_ref().unwrap().sequence, receipt.sequence);
            assert!(results[0].as_ref().unwrap().duplicate);
            assert!(format!("{:#}", results[1].as_ref().unwrap_err()).contains("conflicts"));
            assert!(format!("{:#}", results[2].as_ref().unwrap_err()).contains("unknown table"));
            if failure == "clean" {
                // A clean root needs no checkpoint; this new request is admissible.
                assert!(!results[3].as_ref().unwrap().duplicate);
            } else {
                assert!(results[3].is_err(), "{failure}: {:?}", results[3]);
            }
            assert!(db.status().unwrap().fenced.is_none());
            drop(db);
            Database::open(temp.path(), config).unwrap();
        }
    }

    fn headroom_seed(temp: &TempDir, pages: bool, initial: bool) -> (Database, Config, u64) {
        let config = Config {
            checkpoint_frozen_prefix: true,
            derived_pages: pages,
            segment_rows: 64,
            hot_max_rows: if initial {
                16
            } else {
                Config::default().hot_max_rows
            },
            ..Default::default()
        };
        let db = Database::open(temp.path(), config.clone()).unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                shards: 1,
                window_us: 100,
                rollup_widths_us: vec![10],
                idempotency_window_us: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
        db.create_continuous_aggregate("metrics_10", "metrics", 10)
            .unwrap();
        db.checkpoint().unwrap();
        let receipt = db
            .write("metrics", "v1:90:seed", vec![row(1); 16], 90)
            .unwrap();
        let cap = db.status().unwrap().metadata_bytes + 20 * 512;
        drop(db);
        let config = Config {
            metadata_max_bytes: cap,
            ..config
        };
        let db = Database::open(temp.path(), config.clone()).unwrap();
        (db, config, receipt.sequence)
    }

    fn headroom_requests() -> Vec<WriteRequest> {
        ["v1:150:a", "v1:150:b"]
            .into_iter()
            .map(|id| WriteRequest {
                rows: vec![row(1); 2],
                ..request(id, 1, 150)
            })
            .collect()
    }

    #[test]
    fn grouped_metadata_headroom_retries_only_durable_prefix() {
        for pages in [false, true] {
            let temp = TempDir::new().unwrap();
            let (db, config, seed) = headroom_seed(&temp, pages, false);
            let hook = block(&db, MaintenanceHookPhase::GroupCheckpointComplete);
            let worker = db.clone();
            let group = std::thread::spawn(move || worker.write_group(headroom_requests()));
            assert!(hook.wait_until_blocked(Duration::from_secs(5)));
            let status = db.status().unwrap();
            assert_eq!(status.sequence, seed);
            assert_eq!(status.checkpoint_sequence, seed);
            assert_eq!(status.hot_rows, 0);
            assert_eq!(status.idempotency_keys, 1);
            // Single-append legacy WAL has no clock; reopen starts without a floor.
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), None);
            assert_eq!(db.rollups("metrics").unwrap()[0].count, 16);
            hook.release();
            let results = group.join().unwrap();
            assert!(results.iter().all(Result::is_ok), "{results:?}");
            assert!(
                results
                    .iter()
                    .all(|r| r.as_ref().unwrap().sequence == seed + 1)
            );
            assert_eq!(db.status().unwrap().hot_rows, 4);
            assert_eq!(db.rollups("metrics").unwrap()[0].count, 20);
            assert_eq!(db.performance().phases["checkpoint_locked"].count, 0);
            drop(db);
            let db = Database::open(temp.path(), config).unwrap();
            assert_eq!(db.rollups("metrics").unwrap()[0].count, 20);
            assert!(
                db.write_group(headroom_requests())
                    .iter()
                    .all(|r| r.as_ref().unwrap().duplicate)
            );
        }
    }

    #[test]
    fn retry_rollback_cannot_lower_a_concurrently_persisted_floor() {
        for (pages, initial) in [(false, false), (false, true), (true, false), (true, true)] {
            let temp = TempDir::new().unwrap();
            let (db, config, seed) = headroom_seed(&temp, pages, initial);
            let checkpoint = block(&db, MaintenanceHookPhase::GroupCheckpointComplete);
            let worker = db.clone();
            let group = std::thread::spawn(move || worker.write_group(headroom_requests()));
            assert!(checkpoint.wait_until_blocked(Duration::from_secs(5)));
            assert_eq!(db.status().unwrap().sequence, seed);
            assert_eq!(db.rollups("metrics").unwrap()[0].count, 16);
            // This call changes no sequence: it advances a durable retry's floor,
            // then persists that floor in a second root at the same frontier.
            assert!(
                db.write("metrics", "v1:90:seed", vec![row(1); 16], 190)
                    .unwrap()
                    .duplicate
            );
            db.checkpoint().unwrap();
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(90));
            let failure = block(&db, MaintenanceHookPhase::GroupBeforePublish);
            failure.release_with_error();
            checkpoint.release();
            let results = group.join().unwrap();
            assert!(results.iter().all(Result::is_err), "{results:?}");
            assert!(
                results
                    .iter()
                    .all(|r| format!("{:#}", r.as_ref().unwrap_err()).contains("injected"))
            );
            db.set_maintenance_test_hook(None).unwrap();
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(90));
            assert_eq!(db.status().unwrap().sequence, seed);
            assert_eq!(db.status().unwrap().idempotency_keys, 1);
            assert_eq!(db.rollups("metrics").unwrap()[0].count, 16);
            assert!(db.write("metrics", "v1:89:old", vec![row(1)], 89).is_err());
            assert!(db.status().unwrap().fenced.is_none());
            drop(db);
            let db = Database::open(temp.path(), config).unwrap();
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(90));
            assert_eq!(db.status().unwrap().sequence, seed);
            assert_eq!(
                db.scan("metrics", None, None, None, None).unwrap().len(),
                16
            );
            assert!(db.write("metrics", "v1:89:old", vec![row(1)], 89).is_err());
        }
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn manifest_fence_duplicate_child() {
    let Ok(root) = std::env::var("VARVE_PREFIX_ADMISSION_FAULT_ROOT") else {
        return;
    };
    let pages = std::env::var("VARVE_PREFIX_PAGES").unwrap() == "true";
    let config = Config {
        checkpoint_frozen_prefix: true,
        derived_pages: pages,
        hot_max_rows: 4,
        ..Config::default()
    };
    let db = Database::open(root, config).unwrap();
    let results = db.write_group(vec![
        request("durable", 1, 1),
        request("durable", 2, 1),
        request("new", 3, 3),
    ]);
    assert_eq!(results.len(), 3);
    assert!(results[0].as_ref().unwrap().duplicate);
    assert_eq!(results[0].as_ref().unwrap().sequence, 2);
    assert!(format!("{:#}", results[1].as_ref().unwrap_err()).contains("conflicts"));
    assert!(results[2].is_err());
    assert!(db.status().unwrap().fenced.is_some());
    // A receipt validated before the ambiguous fence acknowledges only old WAL.
    // New calls cannot bypass the fence, even for a previously durable receipt.
    assert!(db.write_group(vec![request("durable", 1, 1)])[0].is_err());
    assert!(db.write("metrics", "other", vec![row(4)], 4).is_err());
}

#[cfg(feature = "fault-injection")]
#[test]
fn manifest_ambiguity_keeps_prevalidated_duplicates_but_fences_new_calls() {
    for pages in [false, true] {
        for point in [
            "atomic_manifest.bin_before_write",
            "atomic_manifest.bin_before_rename",
            "atomic_manifest.bin_before_dir_sync",
        ] {
            let temp = TempDir::new().unwrap();
            let config = Config {
                checkpoint_frozen_prefix: true,
                derived_pages: pages,
                hot_max_rows: 4,
                ..Config::default()
            };
            let db = Database::open(temp.path(), config.clone()).unwrap();
            db.create_table("metrics", TableConfig::default()).unwrap();
            db.write("metrics", "durable", vec![row(1)], 1).unwrap();
            db.write("metrics", "fill", vec![row(2); 3], 2).unwrap();
            drop(db);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "manifest_fence_duplicate_child", "--nocapture"])
                .env("VARVE_PREFIX_ADMISSION_FAULT_ROOT", temp.path())
                .env("VARVE_PREFIX_PAGES", pages.to_string())
                .env_remove("VARVE_FAILPOINT")
                .env("VARVE_IO_FAILPOINT", point)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "pages={pages}, {point}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let db = Database::open(temp.path(), config.clone()).unwrap();
            assert_eq!(db.status().unwrap().sequence, 3);
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 4);
            assert!(
                db.write_group(vec![request("durable", 1, 1)])[0]
                    .as_ref()
                    .unwrap()
                    .duplicate
            );
            assert!(
                !db.write_group(vec![request("new", 3, 3)])[0]
                    .as_ref()
                    .unwrap()
                    .duplicate
            );
            drop(db);
            assert_eq!(
                Database::open(temp.path(), config)
                    .unwrap()
                    .scan("metrics", None, None, None, None)
                    .unwrap()
                    .len(),
                5
            );
        }
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn durable_duplicate_floor_advancement_survives_new_group_failure() {
    use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};
    for frozen in [false, true] {
        for pages in [false, true] {
            let temp = TempDir::new().unwrap();
            let config = Config {
                checkpoint_frozen_prefix: frozen,
                derived_pages: pages,
                ..Config::default()
            };
            let db = Database::open(temp.path(), config.clone()).unwrap();
            db.create_table(
                "metrics",
                TableConfig {
                    idempotency_window_us: Some(100),
                    ..Default::default()
                },
            )
            .unwrap();
            db.write("metrics", "v1:90:seed", vec![row(1)], 90).unwrap();
            let failure = MaintenanceTestHook::new(MaintenanceHookPhase::GroupBeforePublish);
            failure.release_with_error();
            db.set_maintenance_test_hook(Some(failure)).unwrap();
            let results = db.write_group(vec![
                request("v1:90:seed", 1, 190),
                request("v1:150:new", 2, 150),
            ]);
            assert!(results[0].as_ref().unwrap().duplicate);
            assert!(results[1].is_err());
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(90));
            assert!(db.write("metrics", "v1:89:old", vec![row(1)], 89).is_err());
            db.set_maintenance_test_hook(None).unwrap();
            db.checkpoint().unwrap();
            drop(db);
            let db = Database::open(temp.path(), config).unwrap();
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(90));
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
        }
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn retry_discovered_duplicate_keeps_its_own_floor_when_new_staging_fails() {
    use std::time::Duration;
    use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};
    for pages in [false, true] {
        let temp = TempDir::new().unwrap();
        let config = Config {
            checkpoint_frozen_prefix: true,
            derived_pages: pages,
            hot_max_rows: 4,
            ..Config::default()
        };
        let db = Database::open(temp.path(), config.clone()).unwrap();
        db.create_table(
            "metrics",
            TableConfig {
                idempotency_window_us: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
        db.write("metrics", "v1:90:seed", vec![row(1); 4], 90)
            .unwrap();
        let checkpoint = MaintenanceTestHook::new(MaintenanceHookPhase::GroupCheckpointComplete);
        db.set_maintenance_test_hook(Some(checkpoint.clone()))
            .unwrap();
        let worker = db.clone();
        let group = std::thread::spawn(move || {
            worker.write_group(vec![
                request("v1:150:a", 2, 220),
                request("v1:240:b", 3, 240),
            ])
        });
        assert!(checkpoint.wait_until_blocked(Duration::from_secs(5)));
        let concurrent = db.write("metrics", "v1:150:a", vec![row(2)], 190).unwrap();
        db.checkpoint().unwrap();
        assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(90));
        let failure = MaintenanceTestHook::new(MaintenanceHookPhase::GroupBeforePublish);
        failure.release_with_error();
        db.set_maintenance_test_hook(Some(failure)).unwrap();
        checkpoint.release();
        let results = group.join().unwrap();
        assert!(results[0].as_ref().unwrap().duplicate);
        assert_eq!(results[0].as_ref().unwrap().sequence, concurrent.sequence);
        assert!(results[1].is_err());
        assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(120));
        assert_eq!(db.status().unwrap().sequence, concurrent.sequence);
        assert!(
            db.write("metrics", "v1:119:old", vec![row(1)], 119)
                .is_err()
        );
        db.set_maintenance_test_hook(None).unwrap();
        db.checkpoint().unwrap();
        drop(db);
        let db = Database::open(temp.path(), config).unwrap();
        assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(120));
        assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 5);
        assert_eq!(db.status().unwrap().idempotency_keys, 1);
        assert!(
            db.write("metrics", "v1:119:old", vec![row(1)], 119)
                .is_err()
        );
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn later_duplicate_terminal_clocks_survive_retry_errors_and_external_advancement() {
    use std::time::Duration;
    use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};
    for frozen in [false, true] {
        for pages in [false, true] {
            for failure in ["publish", "stale", "concurrent"] {
                if !frozen && failure != "publish" {
                    continue;
                }
                let temp = TempDir::new().unwrap();
                let config = Config {
                    checkpoint_frozen_prefix: frozen,
                    derived_pages: pages,
                    hot_max_rows: 2,
                    ..Default::default()
                };
                let db = Database::open(temp.path(), config.clone()).unwrap();
                db.create_table(
                    "metrics",
                    TableConfig {
                        idempotency_window_us: Some(100),
                        ..Default::default()
                    },
                )
                .unwrap();
                db.write("metrics", "v1:190:seed", vec![row(1)], 190)
                    .unwrap();
                db.write("metrics", "v1:190:fill", vec![row(3)], 190)
                    .unwrap();
                let phase = match failure {
                    "publish" => MaintenanceHookPhase::GroupBeforePublish,
                    "stale" => MaintenanceHookPhase::RootPrepare,
                    _ => MaintenanceHookPhase::GroupCheckpointComplete,
                };
                let hook = MaintenanceTestHook::new(phase);
                db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
                if failure == "publish" {
                    hook.release_with_error();
                }
                let worker = db.clone();
                let group = std::thread::spawn(move || {
                    worker.write_group(vec![
                        request("v1:100:new", 2, 100),
                        request("v1:190:seed", 1, 250),
                    ])
                });
                if failure != "publish" {
                    assert!(hook.wait_until_blocked(Duration::from_secs(5)));
                    if failure == "stale" {
                        db.create_table("control", TableConfig::default()).unwrap();
                    } else {
                        assert!(
                            db.write("metrics", "v1:190:seed", vec![row(1)], 280)
                                .unwrap()
                                .duplicate
                        );
                        db.checkpoint().unwrap();
                        assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(180));
                    }
                    hook.release();
                }
                let results = group.join().unwrap();
                assert!(results[0].is_err(), "{failure}: {results:?}");
                assert!(results[1].as_ref().unwrap().duplicate);
                assert_eq!(results[1].as_ref().unwrap().sequence, 2);
                let floor = if failure == "concurrent" { 180 } else { 150 };
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(floor));
                assert!(db.write_group(vec![request("v1:149:old", 4, 149)])[0].is_err());
                assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
                assert!(db.status().unwrap().fenced.is_none());
                db.set_maintenance_test_hook(None).unwrap();
                db.checkpoint().unwrap();
                drop(db);
                let db = Database::open(temp.path(), config).unwrap();
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(floor));
                assert!(db.write_group(vec![request("v1:149:old", 4, 149)])[0].is_err());
                assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 2);
                assert_eq!(db.status().unwrap().idempotency_keys, 2);
            }
        }
    }
}

#[test]
fn reclaimable_hot_pressure_matches_roomy_input_order() {
    let input = |table: &str, id: &str, timestamp, now| WriteRequest {
        table: table.into(),
        ..request(id, timestamp, now)
    };
    let new = || input("metrics", "v1:100:new", 5, 100);
    let duplicate = |now| input("metrics", "v1:190:seed", 1, now);
    let cases = [
        vec![new(), duplicate(250)],
        vec![duplicate(250), new()],
        vec![
            new(),
            input("metrics", "v1:190:seed", 99, 250),
            input("metrics", "v1:110:next", 6, 110),
        ],
        vec![
            input("metrics", "v1:80:invalid", 7, 300),
            new(),
            duplicate(250),
        ],
        vec![
            input("metrics", "invalid", 7, 100),
            duplicate(250),
            input("metrics", "v1:160:next", 6, 160),
        ],
        vec![
            new(),
            duplicate(240),
            input("metrics", "v1:145:next", 6, 145),
            duplicate(260),
        ],
        vec![
            new(),
            input("other", "v1:190:seed", 3, 250),
            input("metrics", "v1:110:next", 6, 110),
        ],
        vec![
            duplicate(250),
            input("other", "v1:100:new", 5, 100),
            input("metrics", "v1:160:next", 6, 160),
        ],
        vec![
            new(),
            duplicate(300),
            input("metrics", "v1:110:next", 6, 110),
        ],
        vec![
            input("metrics", "v1:900000000:future", 7, 100),
            new(),
            new(),
            duplicate(250),
        ],
    ];
    for frozen in [false, true] {
        for pages in [false, true] {
            for (case, inputs) in cases.iter().enumerate() {
                assert!(inputs.len() <= 4);
                let mut evidence = Vec::new();
                for pressure in [false, true] {
                    let temp = TempDir::new().unwrap();
                    let config = Config {
                        checkpoint_frozen_prefix: frozen,
                        derived_pages: pages,
                        hot_max_rows: if pressure {
                            4
                        } else {
                            Config::default().hot_max_rows
                        },
                        ..Default::default()
                    };
                    let db = Database::open(temp.path(), config.clone()).unwrap();
                    for table in ["metrics", "other"] {
                        db.create_table(
                            table,
                            TableConfig {
                                idempotency_window_us: Some(100),
                                ..Default::default()
                            },
                        )
                        .unwrap();
                    }
                    for (table, seed, fill) in [("metrics", 1, 2), ("other", 3, 4)] {
                        db.write(table, "v1:190:seed", vec![row(seed)], 190)
                            .unwrap();
                        db.write(table, "v1:190:fill", vec![row(fill)], 190)
                            .unwrap();
                    }
                    let results = db.write_group(inputs.clone());
                    let classes: Vec<_> = results
                        .iter()
                        .map(|r| match r {
                            Ok(r) => Ok((r.sequence, r.duplicate, r.rows)),
                            Err(e) => Err(format!("{e:#}")),
                        })
                        .collect();
                    let successful: Vec<_> = inputs
                        .iter()
                        .zip(&results)
                        .filter(|(_, r)| r.is_ok())
                        .map(|(r, _)| r.clone())
                        .collect();
                    let raw = ["metrics", "other"]
                        .map(|table| db.scan(table, None, None, None, None).unwrap());
                    if pressure
                        && results
                            .iter()
                            .any(|r| r.as_ref().is_ok_and(|r| !r.duplicate))
                    {
                        assert_eq!(db.status().unwrap().checkpoint_sequence, 6);
                    }
                    drop(db);
                    // Floors may differ: checkpoints persist different portions of
                    // volatile duplicate clocks. Rows and accepted receipts may not.
                    let db = Database::open(temp.path(), config).unwrap();
                    let recovered = ["metrics", "other"]
                        .map(|table| db.scan(table, None, None, None, None).unwrap());
                    assert_eq!(raw, recovered, "case={case} pressure={pressure}");
                    let retries = db.write_group(successful);
                    let expected: Vec<_> = results
                        .iter()
                        .filter_map(|r| r.as_ref().ok())
                        .map(|r| (r.sequence, r.rows))
                        .collect();
                    let actual: Vec<_> = retries
                        .iter()
                        .map(|r| {
                            let receipt = r.as_ref().unwrap_or_else(|e| {
                                panic!("case={case} pressure={pressure}: {e:#}")
                            });
                            assert!(receipt.duplicate);
                            (receipt.sequence, receipt.rows)
                        })
                        .collect();
                    assert_eq!(actual, expected);
                    assert_eq!(
                        recovered,
                        ["metrics", "other"]
                            .map(|table| db.scan(table, None, None, None, None).unwrap())
                    );
                    evidence.push((classes, raw, actual));
                }
                assert_eq!(
                    evidence[0], evidence[1],
                    "case={case} frozen={frozen} pages={pages}"
                );
            }
        }
    }
}

#[test]
fn later_duplicate_reclaims_receipt_capacity_without_expiring_pending_input() {
    for frozen in [false, true] {
        for pages in [false, true] {
            let temp = TempDir::new().unwrap();
            let config = Config {
                checkpoint_frozen_prefix: frozen,
                derived_pages: pages,
                max_idempotency_keys: 2,
                ..Default::default()
            };
            let db = Database::open(temp.path(), config.clone()).unwrap();
            db.create_table(
                "metrics",
                TableConfig {
                    idempotency_window_us: Some(100),
                    ..Default::default()
                },
            )
            .unwrap();
            db.write("metrics", "v1:10:old", vec![row(10)], 10).unwrap();
            db.write("metrics", "v1:90:seed", vec![row(90)], 90)
                .unwrap();
            db.checkpoint().unwrap();
            assert_eq!(db.status().unwrap().idempotency_keys, 2);
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(-10));
            let results = db.write_group(vec![
                request("v1:45:new", 45, 45),
                request("v1:90:seed", 90, 150),
            ]);
            assert!(
                results[0].is_ok(),
                "frozen={frozen} pages={pages}: {results:?}"
            );
            assert_eq!(results[0].as_ref().unwrap().sequence, 4);
            assert!(!results[0].as_ref().unwrap().duplicate);
            assert!(results[1].as_ref().unwrap().duplicate);
            assert_eq!(results[1].as_ref().unwrap().sequence, 3);
            assert_eq!(db.status().unwrap().checkpoint_sequence, 3);
            assert_eq!(db.status().unwrap().idempotency_keys, 2);
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(50));
            assert!(db.write_group(vec![request("v1:45:new", 45, 45)])[0].is_err());
            drop(db);
            let db = Database::open(temp.path(), config.clone()).unwrap();
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
            assert_eq!(db.status().unwrap().idempotency_keys, 2);
            assert!(
                db.write_group(vec![request("v1:45:new", 45, 45)])[0]
                    .as_ref()
                    .unwrap()
                    .duplicate
            );
            assert!(
                db.write_group(vec![request("v1:90:seed", 90, 150)])[0]
                    .as_ref()
                    .unwrap()
                    .duplicate
            );
            db.checkpoint().unwrap();
            drop(db);
            let db = Database::open(temp.path(), config).unwrap();
            assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(50));
            assert_eq!(db.status().unwrap().idempotency_keys, 1);
            assert!(db.write_group(vec![request("v1:45:new", 45, 45)])[0].is_err());
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
        }
    }
}

#[test]
fn later_duplicate_floor_survives_metadata_retry_in_input_order() {
    for frozen in [false, true] {
        for pages in [false, true] {
            for final_root in [false, true] {
                let temp = TempDir::new().unwrap();
                let config = Config {
                    checkpoint_frozen_prefix: frozen,
                    derived_pages: pages,
                    segment_rows: 64,
                    ..Default::default()
                };
                let db = Database::open(temp.path(), config.clone()).unwrap();
                db.create_table(
                    "metrics",
                    TableConfig {
                        shards: 1,
                        window_us: 100,
                        rollup_widths_us: vec![10],
                        idempotency_window_us: Some(100),
                        ..Default::default()
                    },
                )
                .unwrap();
                db.create_continuous_aggregate("metrics_10", "metrics", 10)
                    .unwrap();
                db.checkpoint().unwrap();
                let seed = db
                    .write("metrics", "v1:190:seed", vec![row(1); 16], 190)
                    .unwrap();
                let cap = db.status().unwrap().metadata_bytes + 20 * 512;
                drop(db);
                let config = Config {
                    metadata_max_bytes: cap,
                    ..config
                };
                let db = Database::open(temp.path(), config.clone()).unwrap();
                assert!(
                    db.write("metrics", "v1:190:seed", vec![row(1); 16], 190)
                        .unwrap()
                        .duplicate
                );
                let inputs = || {
                    vec![
                        WriteRequest {
                            rows: vec![row(1); 2],
                            ..request("v1:100:a", 1, 100)
                        },
                        WriteRequest {
                            rows: vec![row(1); 2],
                            ..request("v1:100:b", 1, 100)
                        },
                        WriteRequest {
                            rows: vec![row(1); 16],
                            ..request("v1:190:seed", 1, 250)
                        },
                    ]
                };
                let results = db.write_group(inputs());
                assert!(
                    results.iter().all(Result::is_ok),
                    "frozen={frozen} pages={pages}: {results:?}"
                );
                assert_eq!(results[0].as_ref().unwrap().sequence, seed.sequence + 1);
                assert_eq!(results[1].as_ref().unwrap().sequence, seed.sequence + 1);
                assert!(!results[0].as_ref().unwrap().duplicate);
                assert!(!results[1].as_ref().unwrap().duplicate);
                assert!(results[2].as_ref().unwrap().duplicate);
                assert_eq!(results[2].as_ref().unwrap().sequence, seed.sequence);
                assert_eq!(db.status().unwrap().checkpoint_sequence, seed.sequence);
                assert_eq!(db.status().unwrap().hot_rows, 4);
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
                assert!(db.write_group(vec![request("v1:100:a", 1, 100)])[0].is_err());
                if final_root {
                    db.checkpoint().unwrap();
                }
                drop(db);
                // Open the actual retry root plus WAL, not just a subsequent root
                // that could mask an incompatible floor/accepted-record pair.
                let db = Database::open(temp.path(), config.clone()).unwrap();
                assert_eq!(
                    db.scan("metrics", None, None, None, None).unwrap().len(),
                    20
                );
                assert_eq!(db.rollups("metrics").unwrap()[0].count, 20);
                if final_root {
                    assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
                    assert_eq!(db.status().unwrap().idempotency_keys, 1);
                    assert!(db.write_group(inputs())[0].is_err());
                } else {
                    assert_eq!(db.status().unwrap().idempotency_keys, 3);
                    assert!(
                        db.write_group(inputs())
                            .iter()
                            .all(|r| r.as_ref().is_ok_and(|r| r.duplicate))
                    );
                    assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
                    db.checkpoint().unwrap();
                    drop(db);
                    let db = Database::open(temp.path(), config).unwrap();
                    assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
                    assert_eq!(
                        db.scan("metrics", None, None, None, None).unwrap().len(),
                        20
                    );
                }
            }
        }
    }
}

#[test]
fn later_durable_duplicate_floor_does_not_retroactively_reject_earlier_new_input() {
    for frozen in [false, true] {
        for pages in [false, true] {
            for pressure in [false, true] {
                let temp = TempDir::new().unwrap();
                let config = Config {
                    checkpoint_frozen_prefix: frozen,
                    derived_pages: pages,
                    hot_max_rows: if pressure {
                        2
                    } else {
                        Config::default().hot_max_rows
                    },
                    ..Config::default()
                };
                let db = Database::open(temp.path(), config.clone()).unwrap();
                db.create_table(
                    "metrics",
                    TableConfig {
                        idempotency_window_us: Some(100),
                        ..Default::default()
                    },
                )
                .unwrap();
                db.write("metrics", "v1:190:seed", vec![row(1)], 190)
                    .unwrap();
                if pressure {
                    db.write("metrics", "v1:190:fill", vec![row(3)], 190)
                        .unwrap();
                }
                let results = db.write_group(vec![
                    request("v1:100:new", 2, 100),
                    request("v1:190:seed", 1, 250),
                ]);
                assert!(
                    results[0].is_ok(),
                    "frozen={frozen} pages={pages} pressure={pressure}: {:?}",
                    results[0]
                );
                assert!(!results[0].as_ref().unwrap().duplicate);
                assert_eq!(
                    results[0].as_ref().unwrap().sequence,
                    if pressure { 4 } else { 3 }
                );
                assert!(results[1].as_ref().unwrap().duplicate);
                assert_eq!(results[1].as_ref().unwrap().sequence, 2);
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
                assert!(db.write_group(vec![request("v1:100:new", 2, 100)])[0].is_err());
                // Also recover the actual root/WAL pair before a final checkpoint.
                // Duplicate-only clock advancement is intentionally volatile here.
                drop(db);
                let db = Database::open(temp.path(), config.clone()).unwrap();
                assert_eq!(
                    db.scan("metrics", None, None, None, None).unwrap().len(),
                    if pressure { 3 } else { 2 }
                );
                let retries = db.write_group(vec![
                    request("v1:100:new", 2, 100),
                    request("v1:190:seed", 1, 250),
                ]);
                assert!(
                    retries
                        .iter()
                        .all(|r| r.as_ref().is_ok_and(|r| r.duplicate))
                );
                assert_eq!(
                    retries[0].as_ref().unwrap().sequence,
                    if pressure { 4 } else { 3 }
                );
                assert_eq!(retries[1].as_ref().unwrap().sequence, 2);
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
                assert!(db.write_group(vec![request("v1:100:new", 2, 100)])[0].is_err());
                db.checkpoint().unwrap();
                drop(db);
                let db = Database::open(temp.path(), config).unwrap();
                assert_eq!(db.idempotency_floor_us("metrics").unwrap(), Some(150));
                assert_eq!(
                    db.scan("metrics", None, None, None, None).unwrap().len(),
                    if pressure { 3 } else { 2 }
                );
                assert_eq!(
                    db.status().unwrap().idempotency_keys,
                    if pressure { 2 } else { 1 }
                );
            }
        }
    }
}
