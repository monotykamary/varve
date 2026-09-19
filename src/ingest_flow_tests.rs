use super::flow::{Event, Payload};
use super::*;
use crate::flow::Stage;
use crate::{Config, Row, TableConfig};

fn test_request(id: usize) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: format!("r{id}"),
        now_us: 0,
        rows: vec![Row {
            timestamp_us: 0,
            tenant: "t".into(),
            series: "s".into(),
            value: 1.0,
            tags: Default::default(),
        }],
    }
}

fn manual_flow(bytes: usize, slots: usize) -> (Ingestor, Vec<Stage<Arc<Event>>>) {
    let config = IngestConfig {
        queue_capacity: slots,
        max_pending_bytes: bytes,
        max_delay: Duration::ZERO,
        ..Default::default()
    };
    let stats = Arc::new(Mutex::new(IngestStats::default()));
    let capacity = Arc::new(Notify::new());
    let (flow, stages) = Runtime::new(&config, stats.clone(), capacity.clone()).unwrap();
    (
        Ingestor {
            inner: Arc::new(Owner {
                raw_memory: crate::raw_memory::RawMemoryBudget::new(
                    128 * 1024 * 1024,
                    512 * 1024 * 1024,
                )
                .unwrap(),
                flow,
                workers: Mutex::new(Vec::new()),
                config,
                stats,
                traces: Arc::new(Mutex::new(TraceBuffer::new(0))),
                capacity,
            }),
        },
        stages,
    )
}

fn poll_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
    future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

fn finish_manual_write(ingest: &Ingestor, stages: &mut [Stage<Arc<Event>>]) {
    for stage in &mut stages[..3] {
        let delivery = stage
            .next_until(Instant::now() + Duration::from_secs(1))
            .unwrap()
            .unwrap();
        if has_input(&delivery) {
            let event = delivery.value().unwrap();
            let Payload::Input(input) = event.take() else {
                panic!("owned input")
            };
            assert_eq!(input.rows(), 1);
            drop(input);
            *lock(&event.payload) = Payload::Written(Err(anyhow!("explicit test completion")));
        }
        delivery.finish();
    }
    finish_manual_completion(ingest, &mut stages[3]);
}

fn has_input(delivery: &crate::flow::Delivery<'_, Arc<Event>>) -> bool {
    matches!(
        &*lock(&delivery.value().unwrap().payload),
        Payload::Input(_)
    )
}

fn finish_manual_completion(ingest: &Ingestor, stage: &mut Stage<Arc<Event>>) {
    let delivery = stage
        .next_until(Instant::now() + Duration::from_secs(1))
        .unwrap()
        .unwrap();
    let result = delivery.value().unwrap().take();
    ingest
        .inner
        .flow
        .complete(delivery.sequence(), result, || delivery.finish())
        .unwrap();
}

#[derive(Default)]
struct WakeCount(std::sync::atomic::AtomicUsize);
impl std::task::Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn raw_pressure_waits_permanent_oversize_and_sync_submit_rejects() {
    let (mut ingest, mut stages) = manual_flow(1_000_000, 2);
    let charge = AdmittedWrite::new(test_request(0)).unwrap().raw_bytes();
    Arc::get_mut(&mut ingest.inner).unwrap().raw_memory =
        crate::raw_memory::RawMemoryBudget::new(charge, 1).unwrap();
    let held = ingest.inner.raw_memory.reserve(charge).unwrap();
    assert!(
        ingest
            .submit(test_request(0))
            .unwrap_err()
            .is::<crate::raw_memory::RawReservationError>()
    );
    let mut waiting = Box::pin(ingest.submit_wait(test_request(0)));
    assert!(poll_once(waiting.as_mut()).is_pending());
    assert_eq!(ingest.stats().submitted, 0);
    drop(held);
    let std::task::Poll::Ready(Ok(receipt)) = poll_once(waiting.as_mut()) else {
        panic!("raw release must admit")
    };
    drop(waiting);
    finish_manual_write(&ingest, &mut stages);
    assert!(receipt.blocking_recv().unwrap().is_err());
    assert_eq!(ingest.inner.raw_memory.status().reserved_bytes, 0);
    let mut impossible = test_request(1);
    impossible.rows[0].tenant.reserve(charge);
    let mut waiting = Box::pin(ingest.submit_wait(impossible));
    let std::task::Poll::Ready(Err(error)) = poll_once(waiting.as_mut()) else {
        panic!("oversize cannot wait")
    };
    assert_eq!(
        error.downcast_ref(),
        Some(&crate::raw_memory::RawReservationError::TooLarge)
    );
}

#[test]
fn raw_release_between_failed_check_and_await_is_not_lost() {
    let (ingest, mut stages) = manual_flow(1_000_000, 2);
    let held = ingest
        .inner
        .raw_memory
        .reserve(ingest.inner.raw_memory.status().limit_bytes)
        .unwrap();
    RAW_WAIT_HOOK.with(|hook| *hook.borrow_mut() = Some(Box::new(move || drop(held))));
    let mut waiting = Box::pin(ingest.submit_wait(test_request(0)));
    let std::task::Poll::Ready(Ok(receipt)) = poll_once(waiting.as_mut()) else {
        panic!("release before await must retry immediately")
    };
    drop(waiting);
    finish_manual_write(&ingest, &mut stages);
    assert!(receipt.blocking_recv().unwrap().is_err());
    assert_eq!(ingest.stats().admission_waits, 1);
}

#[test]
fn raw_only_wait_cancellation_and_closure_release_no_unowned_credit() {
    for close in [false, true] {
        let (ingest, _stages) = manual_flow(1_000_000, 2);
        let held = ingest
            .inner
            .raw_memory
            .reserve(ingest.inner.raw_memory.status().limit_bytes)
            .unwrap();
        let mut waiting = Box::pin(ingest.submit_wait(test_request(0)));
        assert!(poll_once(waiting.as_mut()).is_pending());
        if close {
            ingest.shutdown().unwrap();
            let std::task::Poll::Ready(Err(error)) = poll_once(waiting.as_mut()) else {
                panic!("closed raw-only waiter must wake")
            };
            assert!(error.to_string().contains("closed"));
        }
        drop(waiting);
        assert_eq!(ingest.stats().submitted, 0);
        assert_eq!(ingest.stats().waiting_requests, 0);
        assert_eq!(
            ingest.inner.raw_memory.status().reserved_bytes,
            held.bytes()
        );
        drop(held);
        assert_eq!(ingest.inner.raw_memory.status().reserved_bytes, 0);
    }
}

#[test]
fn full_queue_refund_does_not_wake_its_own_raw_waiter() {
    let (ingest, mut stages) = manual_flow(1_000_000, 1);
    let first = ingest.submit(test_request(0)).unwrap();
    let baseline = ingest.inner.raw_memory.status().reserved_bytes;
    let wakes = Arc::new(WakeCount::default());
    let waker = std::task::Waker::from(wakes.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    let mut waiting = Box::pin(ingest.submit_wait(test_request(1)));
    for _ in 0..5 {
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert_eq!(ingest.inner.raw_memory.status().reserved_bytes, baseline);
        assert_eq!(wakes.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
    finish_manual_write(&ingest, &mut stages);
    assert!(wakes.0.load(std::sync::atomic::Ordering::SeqCst) > 0);
    let std::task::Poll::Ready(Ok(second)) = waiting.as_mut().poll(&mut cx) else {
        panic!("flow release must admit")
    };
    drop(waiting);
    finish_manual_write(&ingest, &mut stages);
    assert!(first.blocking_recv().unwrap().is_err());
    assert!(second.blocking_recv().unwrap().is_err());
}

#[test]
fn waiting_admission_preserves_input_and_credit_across_queue_and_byte_pressure() {
    for byte_pressure in [false, true] {
        let bytes = AdmittedWrite::new(test_request(0)).unwrap().bytes();
        let (ingest, mut stages) = manual_flow(
            if byte_pressure { bytes } else { 4 * bytes },
            if byte_pressure { 4 } else { 1 },
        );
        let first = ingest.submit(test_request(0)).unwrap();
        let mut waiting = Box::pin(ingest.submit_wait(test_request(1)));
        assert!(poll_once(waiting.as_mut()).is_pending());
        assert_eq!(
            ingest.inner.raw_memory.status().reserved_bytes,
            AdmittedWrite::new(test_request(0)).unwrap().raw_bytes()
        );
        assert_eq!(ingest.stats().waiting_requests, 1);
        assert_eq!(ingest.stats().submitted, 1);
        // Neither preparation nor visibility alone releases a required slot.
        for stage in &mut stages[..3] {
            let delivery = stage
                .next_until(Instant::now() + Duration::from_secs(1))
                .unwrap()
                .unwrap();
            if has_input(&delivery) {
                let event = delivery.value().unwrap();
                drop(event.take());
                *lock(&event.payload) = Payload::Written(Err(anyhow!("explicit test completion")));
            }
            delivery.finish();
            assert!(poll_once(waiting.as_mut()).is_pending());
        }
        finish_manual_completion(&ingest, &mut stages[3]);
        let std::task::Poll::Ready(Ok(second)) = poll_once(waiting.as_mut()) else {
            panic!("released credit must admit")
        };
        drop(waiting);
        finish_manual_write(&ingest, &mut stages);
        assert!(first.blocking_recv().unwrap().is_err());
        assert!(second.blocking_recv().unwrap().is_err());
        let stats = ingest.stats();
        assert_eq!(stats.submitted, 2);
        assert_eq!(stats.rejected, 0);
        assert_eq!(stats.admission_waits, 1);
        assert_eq!(stats.waiting_requests, 0);
        assert_eq!(stats.waits_without_admission, 0);
        assert_eq!(stats.pending_bytes, 0);
        assert_eq!(ingest.flow_stats().reclaimed, 2);
        assert_eq!(ingest.inner.raw_memory.status().reserved_bytes, 0);
        ingest.shutdown().unwrap();
    }
}

#[test]
fn waiting_cancellation_and_shutdown_never_enqueue_the_unadmitted_input() {
    for shutdown in [false, true] {
        let bytes = AdmittedWrite::new(test_request(0)).unwrap().bytes();
        let (ingest, mut stages) = manual_flow(bytes, 1);
        let first = ingest.submit(test_request(0)).unwrap();
        let mut waiting = Box::pin(ingest.submit_wait(test_request(1)));
        assert!(poll_once(waiting.as_mut()).is_pending());
        assert_eq!(
            ingest.inner.raw_memory.status().reserved_bytes,
            AdmittedWrite::new(test_request(0)).unwrap().raw_bytes()
        );
        if shutdown {
            ingest.shutdown().unwrap();
            let std::task::Poll::Ready(Err(error)) = poll_once(waiting.as_mut()) else {
                panic!("shutdown must wake admission")
            };
            assert!(error.to_string().contains("closed"));
        }
        drop(waiting);
        finish_manual_write(&ingest, &mut stages);
        assert!(first.blocking_recv().unwrap().is_err());
        let stats = ingest.stats();
        assert_eq!(stats.submitted, 1);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.waiting_requests, 0);
        assert_eq!(stats.waits_without_admission, 1);
        assert_eq!(stats.rejected, u64::from(shutdown));
        assert_eq!(stats.pending_bytes, 0);
        assert_eq!(ingest.flow_stats().claimed, 1);
        assert_eq!(ingest.inner.raw_memory.status().reserved_bytes, 0);
        ingest.shutdown().unwrap();
    }
}

#[test]
fn impossible_waiting_request_is_rejected_without_waiting() {
    let bytes = AdmittedWrite::new(test_request(0)).unwrap().bytes();
    let (ingest, _stages) = manual_flow(bytes - 1, 1);
    let mut waiting = Box::pin(ingest.submit_wait(test_request(0)));
    assert!(matches!(
        poll_once(waiting.as_mut()),
        std::task::Poll::Ready(Err(_))
    ));
    let stats = ingest.stats();
    assert_eq!(stats.admission_waits, 0);
    assert_eq!(stats.rejected, 1);
    assert_eq!(stats.submitted, 0);
    ingest.shutdown().unwrap();
}

#[test]
fn flush_barriers_share_the_bounded_flow_without_row_credit() {
    let (ingest, stages) = manual_flow(2000, 1);
    let mut first = ingest.flush().unwrap();
    assert!(
        ingest
            .flush()
            .unwrap_err()
            .to_string()
            .contains("queue is full")
    );
    assert_eq!(ingest.stats().pending_requests, 0);
    assert_eq!(ingest.stats().pending_bytes, 0);
    assert_eq!(ingest.flow_stats().charged_bytes, 0);
    drop(stages);
    // Required-stage abandonment fences. Terminal senders do not depend on
    // destroying the retained ring or on dropping the last Ingestor clone.
    for _ in 0..4 {
        ingest
            .inner
            .flow
            .worker_finished(Err(anyhow!("test stage abandoned")));
    }
    assert!(first.try_recv().unwrap().is_err());
    assert!(ingest.shutdown().is_err());
    assert!(ingest.flush().unwrap_err().to_string().contains("closed"));
}

#[test]
fn expired_deadline_still_coalesces_ready_backlog() {
    let temp = tempfile::TempDir::new().unwrap();
    let db = Database::open(temp.path(), Config::default()).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    let (ingest, mut stages) = manual_flow(16_000, 4);
    let receivers: Vec<_> = (0..4)
        .map(|id| ingest.submit(test_request(id)).unwrap())
        .collect();
    ingest.inner.flow.close();
    workers::validate(db.clone(), stages.remove(0), &ingest.inner.flow).unwrap();
    let preparation = stages.remove(0);
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            workers::prepare(
                db.clone(),
                preparation,
                &ingest.inner.flow,
                &ingest.inner.config,
            )
        });
        workers::commit(
            db.clone(),
            stages.remove(0),
            &ingest.inner.flow,
            &ingest.inner.config,
            &ingest.inner.stats,
            &ingest.inner.traces,
        )
        .unwrap();
        worker.join().unwrap().unwrap();
    });
    for _ in 0..4 {
        finish_manual_completion(&ingest, &mut stages[0]);
    }
    let receipts: Vec<_> = receivers
        .into_iter()
        .map(|r| r.blocking_recv().unwrap().unwrap())
        .collect();
    assert!(receipts.iter().all(|r| r.sequence == receipts[0].sequence));
    assert_eq!(ingest.stats().groups, 1);
    assert_eq!(ingest.stats().pending_bytes, 0);
    assert_eq!(db.status().unwrap().hot_rows, 4);
    ingest.shutdown().unwrap();
    assert!(!ingest.flow_stats().fenced);
}

#[cfg(feature = "fault-injection")]
#[test]
fn followup_post_sync_install_fault_terminates_ingestion_and_barriers() {
    use crate::engine::{MaintenanceHookPhase, MaintenanceTestHook};
    for journal in [false, true] {
        let temp = tempfile::TempDir::new().unwrap();
        let config = Config {
            segmented_journal: journal,
            ..Default::default()
        };
        let db = Database::open(temp.path(), config.clone()).unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        let before = db.status().unwrap().sequence;
        let hook = MaintenanceTestHook::new(MaintenanceHookPhase::EpochBeforeInstall);
        db.set_maintenance_test_hook(Some(hook.clone())).unwrap();
        let (mut ingest, stages) = manual_flow(16_000, 6);
        let owner = Arc::get_mut(&mut ingest.inner).unwrap();
        owner.raw_memory = db.inner.raw_memory.clone();
        // Exactly one durable request reaches the hook; later accepted writes
        // must become terminal errors without any new publication authority.
        owner.config.max_group_requests = 1;
        let mut receivers = vec![
            ingest.submit(test_request(0)).unwrap(),
            ingest.submit(test_request(1)).unwrap(),
        ];
        let mut first_barrier = ingest.flush().unwrap();
        receivers.push(ingest.submit(test_request(2)).unwrap());
        receivers.push(ingest.submit(test_request(3)).unwrap());
        let mut final_barrier = ingest.flush().unwrap();
        let workers = workers::start(
            db.clone(),
            stages,
            ingest.inner.flow.clone(),
            ingest.inner.config.clone(),
            ingest.inner.stats.clone(),
            ingest.inner.traces.clone(),
        )
        .unwrap();
        *lock(&ingest.inner.workers) = workers;
        assert!(hook.wait_until_blocked(Duration::from_secs(10)));
        assert_eq!(db.status().unwrap().sequence, before);
        assert_eq!(db.status().unwrap().hot_rows, 0);
        assert!(db.inner.raw_memory.status().reserved_bytes > 0);
        for receiver in &mut receivers {
            assert!(matches!(
                receiver.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        assert!(matches!(
            first_barrier.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            final_barrier.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        hook.release_with_error();
        ingest.shutdown().unwrap();
        for receiver in &mut receivers {
            assert!(
                receiver
                    .try_recv()
                    .expect("accepted sender must terminate, not close or strand")
                    .is_err()
            );
        }
        let first = first_barrier
            .try_recv()
            .expect("first barrier must terminate")
            .unwrap();
        let last = final_barrier
            .try_recv()
            .expect("final barrier must terminate")
            .unwrap();
        assert_eq!(
            (
                first.sequence,
                first.completed,
                first.succeeded,
                first.failed
            ),
            (before, 2, 0, 2)
        );
        assert_eq!(
            (last.sequence, last.completed, last.succeeded, last.failed),
            (before, 4, 0, 4)
        );
        let stats = ingest.stats();
        assert_eq!(
            (
                stats.pending_requests,
                stats.pending_bytes,
                stats.dropped_receivers
            ),
            (0, 0, 0)
        );
        assert_eq!((stats.submitted, stats.completed, stats.failed), (4, 4, 4));
        let flow = ingest.flow_stats();
        assert_eq!(
            (flow.claimed, flow.reclaimed, flow.charged_bytes),
            (6, 6, 0)
        );
        assert_eq!(flow.stage_finished, vec![6; 4]);
        assert!(
            !flow.fenced,
            "ordinary terminal database errors drain the ring"
        );
        assert_eq!(db.inner.raw_memory.status().reserved_bytes, 0);
        assert_eq!(db.inner.raw_memory.status().live_bytes, 0);
        assert!(!db.is_ready());
        assert!(db.status().unwrap().fenced.is_some());
        assert!(db.write_group(vec![test_request(4)])[0].is_err());
        drop(ingest);
        drop(db);
        let recovered = Database::open(temp.path(), config).unwrap();
        assert!(recovered.is_ready());
        assert_eq!(
            recovered
                .scan("metrics", None, None, None, None)
                .unwrap()
                .len(),
            1
        );
        assert!(
            recovered.write_group(vec![test_request(0)])[0]
                .as_ref()
                .unwrap()
                .duplicate
        );
    }
}

#[test]
fn every_worker_panic_completes_all_retained_write_and_barrier_senders() {
    for failed_stage in 0..4 {
        let temp = tempfile::TempDir::new().unwrap();
        let db = Database::open(temp.path(), Config::default()).unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        let (ingest, stages) = manual_flow(16_000, 5);
        let mut receivers: Vec<_> = (0..4)
            .map(|id| ingest.submit(test_request(id)).unwrap())
            .collect();
        let mut barrier = ingest.flush().unwrap();
        ingest.inner.flow.inject_worker_panic(failed_stage).unwrap();
        let workers = workers::start(
            db,
            stages,
            ingest.inner.flow.clone(),
            ingest.inner.config.clone(),
            ingest.inner.stats.clone(),
            ingest.inner.traces.clone(),
        )
        .unwrap();
        *lock(&ingest.inner.workers) = workers;
        assert!(ingest.shutdown().is_err());
        for receiver in &mut receivers {
            assert!(
                receiver
                    .try_recv()
                    .expect("explicit terminal result, not a closed or stranded channel")
                    .is_err()
            );
        }
        assert!(barrier.try_recv().unwrap().is_err());
        let stats = ingest.stats();
        assert_eq!((stats.submitted, stats.completed, stats.failed), (4, 4, 4));
        assert_eq!((stats.pending_requests, stats.pending_bytes), (0, 0));
        assert!(stats.closed);
        assert!(ingest.flow_stats().fenced);
    }
}
