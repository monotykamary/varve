use std::{
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};
use tempfile::TempDir;
use varve::{Config, Database, IngestConfig, Ingestor, Row, TableConfig, WriteRequest};

#[cfg(feature = "fault-injection")]
#[test]
fn owned_epoch_flow_waits_for_sync_and_atomic_multitable_install() {
    use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};
    struct Release(MaintenanceTestHook);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    for journal in [false, true] {
        for phase in [
            MaintenanceHookPhase::WalBeforeSync,
            MaintenanceHookPhase::EpochBeforeInstall,
        ] {
            let temp = TempDir::new().unwrap();
            let config = Config {
                segmented_journal: journal,
                ..Config::default()
            };
            let db = Database::open(temp.path(), config.clone()).unwrap();
            for name in ["metrics", "other"] {
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
            let before = db.status().unwrap().sequence;
            let ingest = Ingestor::new(
                db.clone(),
                IngestConfig {
                    queue_capacity: 4,
                    max_group_requests: 2,
                    max_delay: Duration::from_secs(60),
                    ..Default::default()
                },
            )
            .unwrap();
            let hook = MaintenanceTestHook::new(phase);
            db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
            let release = Release(hook.clone());
            let a = request(0, 2);
            let mut b = request(1, 2);
            b.table = "other".into();
            b.rows[1].timestamp_us = 11;
            b.now_us = 11;
            let first = ingest.submit(a).unwrap();
            let second = ingest.submit(b).unwrap();
            assert!(hook.wait_until_blocked(Duration::from_secs(10)));
            let flow = ingest.flow_stats();
            assert_eq!(
                flow.stage_finished,
                vec![2, 2, 0, 0],
                "P must be a real owned epoch; D alone is not V"
            );
            assert_eq!(flow.reclaimed, 0);
            assert!(flow.charged_bytes > 0);
            assert!(first.is_empty() && second.is_empty());
            assert_eq!(db.status().unwrap().sequence, before);
            for name in ["metrics", "other"] {
                assert!(db.scan(name, None, None, None, None).unwrap().is_empty());
                assert!(db.rollups(name).unwrap().is_empty());
            }
            // Dropping an admitted receiver must not discard its group member.
            drop(second);
            hook.release();
            let receipt = first.blocking_recv().unwrap().unwrap();
            let drained = ingest.flush().unwrap().blocking_recv().unwrap().unwrap();
            ingest.shutdown().unwrap();
            assert_eq!((drained.succeeded, drained.failed), (2, 0));
            assert_eq!(ingest.stats().dropped_receivers, 1);
            assert_eq!(ingest.stats().pending_bytes, 0);
            assert_eq!(ingest.flow_stats().stage_finished, vec![3; 4]);
            let raw_a = db.scan("metrics", None, None, None, None).unwrap();
            let raw_b = db.scan("other", None, None, None, None).unwrap();
            assert_eq!((raw_a.len(), raw_b.len()), (2, 2));
            assert!(
                raw_a
                    .iter()
                    .chain(&raw_b)
                    .all(|r| r.sequence == receipt.sequence)
            );
            assert_eq!(
                db.rollups("metrics")
                    .unwrap()
                    .iter()
                    .map(|r| r.count)
                    .sum::<u64>(),
                2
            );
            assert_eq!(
                db.rollups("other")
                    .unwrap()
                    .iter()
                    .map(|r| r.count)
                    .sum::<u64>(),
                2
            );
            drop(release);
            drop(ingest);
            drop(db);
            let db = Database::open(temp.path(), config).unwrap();
            assert_eq!(db.scan("metrics", None, None, None, None).unwrap(), raw_a);
            assert_eq!(db.scan("other", None, None, None, None).unwrap(), raw_b);
            let mut b = request(1, 2);
            b.table = "other".into();
            b.rows[1].timestamp_us = 11;
            b.now_us = 11;
            for result in db.write_group(vec![request(0, 2), b]) {
                let result = result.unwrap();
                assert!(result.duplicate);
                assert_eq!(result.sequence, receipt.sequence);
            }
        }
    }
}

fn request(id: usize, rows: usize) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: format!("r{id}"),
        now_us: 1,
        rows: (0..rows)
            .map(|_| Row {
                timestamp_us: 1,
                tenant: "t".into(),
                series: "s".into(),
                value: 1.0,
                tags: Default::default(),
            })
            .collect(),
    }
}
fn open() -> (TempDir, Database) {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), Config::default()).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            ..Default::default()
        },
    )
    .unwrap();
    (temp, db)
}
fn config() -> IngestConfig {
    IngestConfig {
        max_delay: Duration::from_secs(60),
        ..Default::default()
    }
}

#[test]
fn count_and_row_thresholds_flush_without_waiting_for_delay() {
    for by_rows in [false, true] {
        let (_temp, db) = open();
        let before = db.status().unwrap().sequence;
        let ingest = Ingestor::new(
            db.clone(),
            IngestConfig {
                max_group_requests: if by_rows { 128 } else { 4 },
                max_group_rows: if by_rows { 8 } else { 10_000 },
                ..config()
            },
        )
        .unwrap();
        let receivers: Vec<_> = (0..4)
            .map(|i| ingest.submit(request(i, 2)).unwrap())
            .collect();
        for receiver in receivers {
            let receipt = receiver.blocking_recv().unwrap().unwrap();
            assert_eq!(receipt.sequence, before + 1);
            assert_eq!(receipt.durability, "local_fsync");
        }
        assert_eq!(db.status().unwrap().sequence, before + 1);
        assert_eq!(ingest.stats().groups, 1);
        ingest.shutdown().unwrap();
        assert_eq!(ingest.stats().pending_bytes, 0);
    }
}

#[test]
fn byte_threshold_and_time_threshold_flush() {
    let (_temp, db) = open();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_group_bytes: 4000,
            ..config()
        },
    )
    .unwrap();
    let receivers: Vec<_> = (0..3)
        .map(|i| ingest.submit(request(i, 1)).unwrap())
        .collect();
    // The third request crosses the byte threshold and flushes the first two.
    let mut receivers = receivers.into_iter();
    let first = receivers.next().unwrap().blocking_recv().unwrap().unwrap();
    let second = receivers.next().unwrap().blocking_recv().unwrap().unwrap();
    assert_eq!(first.sequence, second.sequence);
    ingest.shutdown().unwrap();
    let third = receivers.next().unwrap().blocking_recv().unwrap().unwrap();
    assert!(third.sequence > first.sequence);
    assert_eq!(ingest.stats().groups, 2);
    let timed = Ingestor::new(
        db,
        IngestConfig {
            max_delay: Duration::from_millis(5),
            ..config()
        },
    )
    .unwrap();
    let start = Instant::now();
    timed
        .submit(request(9, 1))
        .unwrap()
        .blocking_recv()
        .unwrap()
        .unwrap();
    assert!(start.elapsed() < Duration::from_secs(5));
    timed.shutdown().unwrap();
}

#[test]
fn byte_budget_covers_pending_group_and_rejection_never_enqueues() {
    let (_temp, db) = open();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_pending_bytes: 2000,
            ..config()
        },
    )
    .unwrap();
    let first = ingest.submit(request(0, 1)).unwrap();
    assert!(ingest.submit(request(1, 1)).is_err());
    let mut invalid = request(2, 1);
    invalid.rows[0].value = f64::NAN;
    assert!(ingest.submit(invalid).is_err());
    assert_eq!(ingest.stats().submitted, 1);
    assert_eq!(ingest.stats().pending_requests, 1);
    assert!(ingest.stats().pending_bytes <= 2000);
    assert!(first.is_empty());
    ingest.shutdown().unwrap();
    first.blocking_recv().unwrap().unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
    assert_eq!(ingest.stats().pending_bytes, 0);
    assert!(ingest.submit(request(3, 1)).is_err());
    assert_eq!(ingest.stats().submitted, 1);
    serde_json::to_value(ingest.stats()).unwrap();
}

#[test]
fn queue_full_dropped_receivers_and_concurrent_shutdown_drain() {
    let (_temp, db) = open();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            queue_capacity: 1,
            ..config()
        },
    )
    .unwrap();
    let mut accepted = 0;
    for id in 0..10000 {
        match ingest.submit(request(id, 1)) {
            Ok(receiver) => {
                accepted += 1;
                drop(receiver);
            }
            Err(_) => break,
        }
    }
    assert!(accepted > 0 && accepted < 10000);
    let clone = ingest.clone();
    let join = std::thread::spawn(move || clone.shutdown().unwrap());
    ingest.shutdown().unwrap();
    join.join().unwrap();
    assert_eq!(
        db.scan("metrics", None, None, None, None).unwrap().len(),
        accepted
    );
    let stats = ingest.stats();
    assert_eq!(stats.completed, accepted as u64);
    assert_eq!(stats.dropped_receivers, accepted as u64);
    assert_eq!(stats.pending_bytes, 0);
    assert!(stats.closed);
}

#[test]
fn concurrent_producers_amortize_publication_and_final_drop_drains() {
    let (_temp, db) = open();
    let before = db.status().unwrap().sequence;
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_group_requests: 32,
            max_delay: Duration::from_millis(20),
            ..IngestConfig::default()
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(4));
    let joins: Vec<_> = (0..4)
        .map(|producer| {
            let ingest = ingest.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                (0..32)
                    .map(|i| ingest.submit(request(producer * 32 + i, 1)).unwrap())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let receivers: Vec<_> = joins.into_iter().flat_map(|j| j.join().unwrap()).collect();
    drop(ingest);
    for receiver in receivers {
        assert!(!receiver.blocking_recv().unwrap().unwrap().duplicate);
    }
    let status = db.status().unwrap();
    assert_eq!(status.hot_rows, 128);
    assert!(status.sequence - before < 128);
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 128);
}

#[test]
fn per_item_completion_errors_and_retries_are_isolated() {
    let (_temp, db) = open();
    let ingest = Ingestor::new(db.clone(), config()).unwrap();
    let a = ingest.submit(request(0, 1)).unwrap();
    let duplicate = ingest.submit(request(0, 1)).unwrap();
    let mut bad = request(1, 1);
    bad.table = "missing".into();
    let bad = ingest.submit(bad).unwrap();
    let mut conflict = request(0, 1);
    conflict.rows[0].value = 2.0;
    let conflict = ingest.submit(conflict).unwrap();
    let b = ingest.submit(request(2, 1)).unwrap();
    ingest.shutdown().unwrap();
    let sequence = a.blocking_recv().unwrap().unwrap().sequence;
    let duplicate = duplicate.blocking_recv().unwrap().unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.sequence, sequence);
    assert!(bad.blocking_recv().unwrap().is_err());
    assert!(conflict.blocking_recv().unwrap().is_err());
    assert_eq!(b.blocking_recv().unwrap().unwrap().sequence, sequence);
    let stats = ingest.stats();
    assert_eq!(stats.succeeded, 3);
    assert_eq!(stats.failed, 2);
    assert_eq!(stats.pending_requests, 0);
}

#[test]
fn invalid_configuration_and_oversized_requests_are_upfront_errors() {
    let (_temp, db) = open();
    for bad in [
        IngestConfig {
            queue_capacity: 0,
            ..config()
        },
        IngestConfig {
            max_group_requests: 1025,
            ..config()
        },
        IngestConfig {
            max_group_bytes: 128,
            ..config()
        },
    ] {
        assert!(Ingestor::new(db.clone(), bad).is_err());
    }
    let ingest = Ingestor::new(
        db,
        IngestConfig {
            max_group_rows: 1,
            ..config()
        },
    )
    .unwrap();
    assert!(ingest.submit(request(0, 2)).is_err());
    ingest.shutdown().unwrap();
    assert_eq!(ingest.stats().submitted, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_waiting_producers_commit_every_request_and_reopen_exactly() {
    let (temp, db) = open();
    let byte_limit = 2000;
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            queue_capacity: 1,
            max_pending_bytes: byte_limit,
            max_group_requests: 1,
            max_delay: Duration::ZERO,
            ..Default::default()
        },
    )
    .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(32));
    let mut tasks = Vec::new();
    for id in 0..32 {
        let ingest = ingest.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let receipt = ingest
                .submit_wait(request(id, 1))
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(receipt.durability, "local_fsync");
            assert!(!receipt.duplicate);
        }));
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .unwrap();
    let clone = ingest.clone();
    tokio::task::spawn_blocking(move || clone.shutdown())
        .await
        .unwrap()
        .unwrap();
    let stats = ingest.stats();
    assert_eq!(stats.submitted, 32);
    assert_eq!(stats.completed, 32);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.dropped_receivers, 0);
    assert_eq!(stats.waiting_requests, 0);
    assert_eq!(stats.pending_bytes, 0);
    assert!(stats.peak_pending_bytes <= byte_limit);
    assert_eq!(
        db.scan("metrics", None, None, None, None).unwrap().len(),
        32
    );
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 32);
    drop(ingest);
    drop(db);
    let db = Database::open(temp.path(), Config::default()).unwrap();
    assert_eq!(
        db.scan("metrics", None, None, None, None).unwrap().len(),
        32
    );
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 32);
    for id in 0..32 {
        let r = request(id, 1);
        assert!(
            db.write(&r.table, &r.request_id, r.rows, r.now_us)
                .unwrap()
                .duplicate
        );
    }
}

#[tokio::test]
async fn shutdown_wakes_waiting_producer_and_drains_only_admitted_requests() {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    let (_temp, db) = open();
    let byte_limit = 2000;
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_pending_bytes: byte_limit,
            ..config()
        },
    )
    .unwrap();
    let first = ingest.submit(request(0, 1)).unwrap();
    let mut waiting = Box::pin(ingest.submit_wait(request(1, 1)));
    assert!(matches!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    let clone = ingest.clone();
    tokio::task::spawn_blocking(move || clone.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(waiting.await.unwrap_err().to_string().contains("closed"));
    first.await.unwrap().unwrap();
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
    assert_eq!(ingest.stats().waits_without_admission, 1);
    assert_eq!(ingest.stats().waiting_requests, 0);
    assert_eq!(ingest.stats().pending_bytes, 0);
}

#[test]
fn production_flow_has_four_required_stages_and_releases_one_slot_before_receipt() {
    let (_temp, db) = open();
    let byte_limit = 2000;
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            queue_capacity: 1,
            max_pending_bytes: byte_limit,
            max_group_requests: 1,
            max_delay: Duration::ZERO,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(ingest.flow_stats().stage_finished, vec![0, 0, 0, 0]);
    for id in 0..64 {
        // Use fixed-width IDs so every request has the exact same byte charge.
        let mut input = request(0, 1);
        input.request_id = format!("{id:02}");
        let receipt = ingest
            .submit(input)
            .unwrap()
            .blocking_recv()
            .unwrap()
            .unwrap();
        assert_eq!(receipt.durability, "local_fsync");
        let flow = ingest.flow_stats();
        assert_eq!(flow.claimed, id + 1);
        assert_eq!(flow.reclaimed, flow.claimed);
        assert_eq!(flow.charged_bytes, 0);
        assert_eq!(flow.stage_finished, vec![flow.claimed; 4]);
        assert_eq!(ingest.stats().pending_bytes, 0);
        assert_eq!(db.status().unwrap().hot_rows, (id + 1) as usize);
    }
    let barrier = ingest.flush().unwrap().blocking_recv().unwrap().unwrap();
    assert_eq!(
        (barrier.completed, barrier.succeeded, barrier.failed),
        (64, 64, 0)
    );
    // The barrier's slot, too, is reusable immediately after its notification.
    ingest.flush().unwrap().blocking_recv().unwrap().unwrap();
    ingest.shutdown().unwrap();
    let flow = ingest.flow_stats();
    assert_eq!((flow.claimed, flow.reclaimed), (66, 66));
    assert!(flow.admission_closed);
    assert!(!flow.fenced, "intentional close must drain, not fence");
    assert_eq!(ingest.stats().peak_pending_requests, 1);
    assert!(ingest.stats().peak_pending_bytes > 0);
    assert!(ingest.stats().peak_pending_bytes <= byte_limit);
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 64);
}

#[tokio::test]
async fn flow_barriers_preserve_fifo_counts_with_errors_and_dropped_receivers() {
    let (_temp, db) = open();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            queue_capacity: 8,
            ..config()
        },
    )
    .unwrap();
    let first = ingest.submit(request(0, 1)).unwrap();
    let mut bad = request(1, 1);
    bad.table = "missing".into();
    let bad = ingest.submit(bad).unwrap();
    drop(ingest.submit(request(2, 1)).unwrap());
    let before = ingest.flush().unwrap();
    let later = ingest.submit(request(3, 1)).unwrap();
    let after = ingest.flush().unwrap();
    let before = tokio::time::timeout(Duration::from_secs(5), before)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let first = first.await.unwrap().unwrap();
    assert!(bad.await.unwrap().is_err());
    assert_eq!(
        (before.completed, before.succeeded, before.failed),
        (3, 2, 1)
    );
    assert_eq!(before.sequence, first.sequence);
    let after = tokio::time::timeout(Duration::from_secs(5), after)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(after.sequence, later.await.unwrap().unwrap().sequence);
    assert!(after.sequence > before.sequence);
    assert_eq!((after.completed, after.succeeded, after.failed), (4, 3, 1));
    ingest.shutdown().unwrap();
    assert_eq!(ingest.stats().dropped_receivers, 1);
    assert_eq!(ingest.flow_stats().claimed, 6);
    assert_eq!(ingest.flow_stats().stage_finished, vec![6, 6, 6, 6]);
    assert_eq!(ingest.flow_stats().charged_bytes, 0);
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
}

#[tokio::test]
async fn full_flow_wait_deadlines_and_cancelled_waiters_never_publish_claims() {
    use std::future::Future;
    use std::task::{Context, Waker};
    let (_temp, db) = open();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            queue_capacity: 1,
            ..config()
        },
    )
    .unwrap();
    let admitted = ingest.submit(request(0, 1)).unwrap();
    let mut waiters: Vec<_> = (1..9)
        .map(|id| Box::pin(ingest.submit_wait(request(id, 1))))
        .collect();
    for waiter in &mut waiters {
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(ingest.stats().waiting_requests, 8);
    assert_eq!(ingest.flow_stats().claimed, 1);
    let timed = waiters.pop().unwrap();
    assert!(tokio::time::timeout(Duration::ZERO, timed).await.is_err());
    drop(waiters);
    assert_eq!(ingest.stats().waiting_requests, 0);
    assert_eq!(ingest.stats().waits_without_admission, 8);
    assert_eq!(ingest.flow_stats().claimed, 1);
    drop(admitted);
    ingest.shutdown().unwrap();
    assert_eq!(ingest.flow_stats().reclaimed, 1);
    assert_eq!(ingest.stats().dropped_receivers, 1);
    assert_eq!(ingest.stats().pending_bytes, 0);
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 1);
}

#[cfg(feature = "fault-injection")]
#[test]
fn preparation_overlaps_blocked_fsync_but_required_stages_keep_full_flow_charged() {
    use varve::engine::{MaintenanceHookPhase, MaintenanceTestHook};
    struct ReleaseHook(MaintenanceTestHook);
    impl Drop for ReleaseHook {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let (_temp, db) = open();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            queue_capacity: 4,
            max_pending_bytes: 6000,
            max_group_requests: 1,
            max_delay: Duration::ZERO,
            ..Default::default()
        },
    )
    .unwrap();
    let hook = MaintenanceTestHook::new(MaintenanceHookPhase::WalBeforeSync);
    db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
    // Declared after the ingestor so assertion unwinding releases the hook before
    // Ingestor's draining Drop joins a worker blocked on it.
    let _release = ReleaseHook(hook.clone());
    let first = ingest.submit(request(0, 1)).unwrap();
    assert!(hook.wait_until_blocked(Duration::from_secs(5)));
    let bytes = ingest.stats().pending_bytes;
    assert!(bytes > 0 && 3 * bytes <= 6000);
    let second = ingest.submit(request(1, 1)).unwrap();
    let third = ingest.submit(request(2, 1)).unwrap();
    let barrier = ingest.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while ingest.flow_stats().stage_finished[0] < 4 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let flow = ingest.flow_stats();
    assert_eq!(
        flow.stage_finished,
        vec![4, 1, 0, 0],
        "static validation runs ahead; one real epoch retains serialized authority"
    );
    assert_eq!(
        (flow.claimed, flow.reclaimed, flow.charged_bytes),
        (4, 0, 3 * bytes)
    );
    assert!(ingest.submit(request(3, 1)).is_err());
    assert!(ingest.flush().is_err());
    assert!(first.is_empty() && second.is_empty() && third.is_empty() && barrier.is_empty());
    assert_eq!(ingest.stats().pending_requests, 3);
    assert_eq!(ingest.stats().pending_bytes, 3 * bytes);
    assert!(
        db.scan("metrics", None, None, None, None)
            .unwrap()
            .is_empty()
    );
    assert!(db.rollups("metrics").unwrap().is_empty());
    hook.release();
    for receiver in [first, second, third] {
        assert_eq!(
            receiver.blocking_recv().unwrap().unwrap().durability,
            "local_fsync"
        );
    }
    assert_eq!(barrier.blocking_recv().unwrap().unwrap().succeeded, 3);
    ingest.shutdown().unwrap();
    db.set_maintenance_test_hook(None).unwrap();
    assert_eq!(ingest.flow_stats().stage_finished, vec![4, 4, 4, 4]);
    assert_eq!(ingest.flow_stats().charged_bytes, 0);
    assert_eq!(ingest.stats().peak_pending_bytes, 3 * bytes);
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 3);
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 3);
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn completion_worker_panic_fails_all_receivers_without_dropping_ingestor() {
    let (_temp, db) = open();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            queue_capacity: 8,
            ..config()
        },
    )
    .unwrap();
    let receivers: Vec<_> = (0..4)
        .map(|id| ingest.submit(request(id, 1)).unwrap())
        .collect();
    ingest.inject_worker_panic(3).unwrap();
    let barrier = ingest.flush().unwrap();
    for receiver in receivers {
        let terminal = tokio::time::timeout(Duration::from_secs(5), receiver)
            .await
            .unwrap();
        assert!(
            terminal
                .expect("explicit error, not oneshot cancellation")
                .is_err()
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(5), barrier)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(ingest.shutdown().is_err());
    let stats = ingest.stats();
    assert_eq!((stats.submitted, stats.completed, stats.failed), (4, 4, 4));
    assert_eq!((stats.pending_requests, stats.pending_bytes), (0, 0));
    assert!(ingest.flow_stats().fenced);
    assert!(ingest.submit(request(4, 1)).is_err());
    // Stage 3 failed AFTER visibility. Errors explicitly require same-ID retries,
    // not an assertion that the admitted requests could not have committed.
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 4);
    for id in 0..4 {
        let input = request(id, 1);
        assert!(
            db.write(&input.table, &input.request_id, input.rows, input.now_us)
                .unwrap()
                .duplicate
        );
    }
}

#[test]
fn flow_ingestion_with_explicit_segmented_journal_reopens_exact_rows_and_receipts() {
    let root = TempDir::new().unwrap();
    let db = Database::open(
        root.path(),
        Config {
            segmented_journal: true,
            ..Config::default()
        },
    )
    .unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![10],
            ..Default::default()
        },
    )
    .unwrap();
    let ingest = Ingestor::new(
        db.clone(),
        IngestConfig {
            max_group_requests: 4,
            queue_capacity: 16,
            ..config()
        },
    )
    .unwrap();
    let receivers: Vec<_> = (0..8)
        .map(|id| {
            let mut input = request(id, 1);
            input.rows[0].value = id as f64;
            ingest.submit(input).unwrap()
        })
        .collect();
    let barrier = ingest.flush().unwrap();
    ingest.shutdown().unwrap();
    let receipts: Vec<_> = receivers
        .into_iter()
        .map(|receiver| receiver.blocking_recv().unwrap().unwrap())
        .collect();
    assert_eq!(barrier.blocking_recv().unwrap().unwrap().succeeded, 8);
    assert_eq!(ingest.flow_stats().stage_finished, vec![9, 9, 9, 9]);
    assert_eq!(ingest.flow_stats().reclaimed, 9);
    assert_eq!(ingest.flow_stats().charged_bytes, 0);
    assert!(!ingest.flow_stats().fenced);
    assert_eq!(ingest.stats().groups, 2);
    assert!(
        receipts[..4]
            .iter()
            .all(|r| r.sequence == receipts[0].sequence)
    );
    assert!(
        receipts[4..]
            .iter()
            .all(|r| r.sequence == receipts[4].sequence)
    );
    assert!(receipts[4].sequence > receipts[0].sequence);
    drop(ingest);
    drop(db);
    // The explicit initial selection is persisted; reopening must not silently
    // reinterpret the acknowledged journal as an empty legacy WAL directory.
    let db = Database::open(root.path(), Config::default()).unwrap();
    let mut values: Vec<_> = db
        .scan("metrics", None, None, None, None)
        .unwrap()
        .into_iter()
        .map(|row| row.row.value)
        .collect();
    values.sort_by(f64::total_cmp);
    assert_eq!(values, (0..8).map(|id| id as f64).collect::<Vec<_>>());
    assert_eq!(db.rollups("metrics").unwrap()[0].count, 8);
    for (id, receipt) in receipts.into_iter().enumerate() {
        let mut input = request(id, 1);
        input.rows[0].value = id as f64;
        let retry = db
            .write(&input.table, &input.request_id, input.rows, input.now_us)
            .unwrap();
        assert!(retry.duplicate);
        assert_eq!(retry.sequence, receipt.sequence);
    }
}
