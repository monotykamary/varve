use super::*;
use std::sync::atomic::{AtomicU8, AtomicUsize};
use std::thread;
use std::time::Duration;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[test]
fn review_ineligible_waiter_cannot_block_an_eligible_producer() {
    let (producer, mut stages, control) = bounded::<u8>(2, 10, &[vec![]]).unwrap();
    producer.try_claim(4).unwrap().publish(1);
    producer.try_claim(6).unwrap().publish(2);
    let large = producer.clone();
    let large = thread::spawn(move || large.claim_until(7, deadline()).unwrap().publish(4));
    let small = producer.clone();
    let (ready, wait) = wake_channel(1);
    let small = thread::spawn(move || {
        small.claim_until(4, deadline()).unwrap().publish(3);
        ready.send(()).unwrap();
    });
    let until = deadline();
    while producer.core.blocking_waiters.load(Ordering::SeqCst) < 2 {
        assert!(
            Instant::now() < until,
            "both producers must enter bounded waiting"
        );
        thread::yield_now();
    }
    stages[0].next_until(deadline()).unwrap().unwrap().finish();
    // The six-byte event remains unreclaimed. Four bytes fit, seven do not.
    wait.recv_timeout(Duration::from_secs(1))
        .expect("eligible waiter lost its wake");
    small.join().unwrap();
    assert_eq!(control.stats().charged_bytes, 10);
    assert_eq!(control.stats().reclaimed, 1);
    for value in [2, 3] {
        let delivery = stages[0].next_until(deadline()).unwrap().unwrap();
        assert_eq!(delivery.value(), Some(&value));
        delivery.finish();
    }
    assert_eq!(large.join().unwrap(), 4);
    control.close();
    stages[0].next_until(deadline()).unwrap().unwrap().finish();
    assert!(stages[0].next_until(deadline()).unwrap().is_none());
    assert_eq!(control.stats().charged_bytes, 0);
}

#[test]
fn review_independent_final_consumers_cannot_collectively_miss_reclamation() {
    let (producer, stages, control) = bounded::<usize>(1, 1, &[vec![], vec![]]).unwrap();
    let boundary = Arc::new(std::sync::Barrier::new(3));
    let workers: Vec<_> = stages
        .into_iter()
        .map(|mut stage| {
            let boundary = Arc::clone(&boundary);
            thread::spawn(move || {
                for expected in 0..256 {
                    let delivery = stage.next_until(deadline()).unwrap().unwrap();
                    assert_eq!(delivery.value(), Some(&expected));
                    boundary.wait();
                    delivery.finish();
                    boundary.wait();
                }
                assert!(stage.next_until(deadline()).unwrap().is_none());
            })
        })
        .collect();
    for value in 0..256 {
        producer.claim_until(1, deadline()).unwrap().publish(value);
        boundary.wait();
        boundary.wait();
        assert_eq!(control.stats().reclaimed, value as u64 + 1);
        assert_eq!(control.stats().charged_bytes, 0);
    }
    control.close();
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(!control.stats().fenced);
}

#[test]
fn review_coalescing_must_stop_when_fenced() {
    for fence_inside_predicate in [false, true] {
        let (producer, mut stages, control) = bounded::<u8>(3, 3, &[vec![]]).unwrap();
        for value in 1..=3 {
            producer.try_claim(1).unwrap().publish(value);
        }
        let first = stages[0].next_until(deadline()).unwrap().unwrap();
        if !fence_inside_predicate {
            control.fence("stop");
        }
        let result = first.coalesce(3, |_| {
            if fence_inside_predicate {
                control.fence("stop");
            }
            true
        });
        assert!(matches!(result, Err(FlowError::Failed(_))));
    }
}

#[test]
fn rejects_unbounded_and_cyclic_graphs() {
    for (capacity, bytes, graph) in [
        (0, 1, vec![vec![]]),
        (1, 0, vec![vec![]]),
        (1, 1, vec![]),
        (1, 1, vec![vec![0]]),
        (1, 1, vec![vec![], vec![0, 0]]),
    ] {
        assert!(bounded::<usize>(capacity, bytes, &graph).is_err());
    }
}

#[test]
fn out_of_order_producers_and_cancelled_claims_leave_no_sequence_hole() {
    let (producer, mut stages, control) = bounded(2, 8, &[vec![]]).unwrap();
    let first = producer.try_claim(4).unwrap();
    assert_eq!(first.sequence(), 1);
    assert_eq!(producer.try_claim(4).unwrap().publish(22), 2);
    assert!(matches!(
        stages[0].next_until(Instant::now() + Duration::from_millis(10)),
        Err(FlowError::Deadline)
    ));
    drop(first);
    let cancelled = stages[0].next_until(deadline()).unwrap().unwrap();
    assert_eq!(cancelled.sequence(), 1);
    assert_eq!(cancelled.value(), None);
    cancelled.finish();
    let second = stages[0].next_until(deadline()).unwrap().unwrap();
    assert_eq!(second.sequence(), 2);
    assert_eq!(second.value(), Some(&22));
    second.finish();
    control.close();
    assert!(stages[0].next_until(deadline()).unwrap().is_none());
    assert_eq!(control.stats().charged_bytes, 0);
    assert_eq!(control.stats().reclaimed, 2);
    drop(stages);
    assert!(!control.stats().fenced);
}

#[test]
fn slowest_consumer_gates_both_slot_reuse_and_payload_destruction() {
    struct Payload(Arc<AtomicUsize>);
    impl Drop for Payload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let destroyed = Arc::new(AtomicUsize::new(0));
    let (producer, mut stages, control) = bounded(1, 8, &[vec![], vec![]]).unwrap();
    producer
        .try_claim(8)
        .unwrap()
        .publish(Payload(Arc::clone(&destroyed)));
    stages[0].next_until(deadline()).unwrap().unwrap().finish();
    assert_eq!(control.stats().charged_bytes, 8);
    assert_eq!(control.stats().reclaimed, 0);
    assert_eq!(destroyed.load(Ordering::Relaxed), 0);
    assert!(matches!(producer.try_claim(0), Err(FlowError::Full)));
    stages[1].next_until(deadline()).unwrap().unwrap().finish();
    assert_eq!(destroyed.load(Ordering::Relaxed), 1);
    assert_eq!(control.stats().charged_bytes, 0);
    drop(producer.try_claim(8).unwrap());
    control.close();
    for stage in &mut stages {
        stage.next_until(deadline()).unwrap().unwrap().finish();
        assert!(stage.next_until(deadline()).unwrap().is_none());
    }
    drop(stages);
    assert!(!control.stats().fenced);
}

#[test]
fn independent_consumers_broadcast_and_join_stage_observes_both_dependencies() {
    struct Event {
        identity: usize,
        prepared: AtomicU8,
    }
    let (producer, stages, control) =
        bounded::<Event>(32, 32 * 16, &[vec![], vec![], vec![0, 1]]).unwrap();
    let workers: Vec<_> = stages
        .into_iter()
        .map(|mut stage| {
            thread::spawn(move || {
                let index = stage.index();
                let mut seen = Vec::new();
                while let Some(delivery) = stage.next_until(deadline()).unwrap() {
                    let event = delivery.value().unwrap();
                    if index < 2 {
                        event.prepared.fetch_or(1 << index, Ordering::AcqRel);
                    } else {
                        assert_eq!(event.prepared.load(Ordering::Acquire), 3);
                    }
                    seen.push((delivery.sequence(), event.identity));
                    delivery.finish();
                }
                seen
            })
        })
        .collect();
    let producers: Vec<_> = (0..4)
        .map(|worker| {
            let producer = producer.clone();
            thread::spawn(move || {
                for index in 0..500 {
                    producer
                        .claim_until(16, deadline())
                        .unwrap()
                        .publish(Event {
                            identity: worker * 500 + index,
                            prepared: AtomicU8::new(0),
                        });
                }
            })
        })
        .collect();
    for producer in producers {
        producer.join().unwrap();
    }
    control.close();
    let seen: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(seen[0], seen[1]);
    assert_eq!(seen[1], seen[2]);
    assert_eq!(seen[0].len(), 2000);
    let mut identities = seen[0]
        .iter()
        .map(|(_, identity)| *identity)
        .collect::<Vec<_>>();
    identities.sort_unstable();
    assert_eq!(identities, (0..2000).collect::<Vec<_>>());
    assert_eq!(
        seen[0]
            .iter()
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>(),
        (1..=2000).collect::<Vec<_>>()
    );
    let stats = control.stats();
    assert_eq!(stats.claimed, 2000);
    assert_eq!(stats.reclaimed, 2000);
    assert_eq!(stats.charged_bytes, 0);
    assert!(!stats.fenced);
}

#[test]
fn coalesced_group_does_not_publish_its_frontier_until_every_record_is_finished() {
    let (producer, mut stages, control) = bounded(4, 16, &[vec![], vec![0]]).unwrap();
    for value in [3, 4, 5] {
        producer.try_claim(4).unwrap().publish(value);
    }
    let (upstream, downstream) = stages.split_at_mut(1);
    let first = upstream[0].next_until(deadline()).unwrap().unwrap();
    let mut charged = 4;
    let batch = first
        .coalesce(4, |_| {
            charged += 4;
            charged <= 8
        })
        .unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.first_sequence(), 1);
    assert_eq!(batch.last_sequence(), 2);
    assert_eq!(
        batch
            .values()
            .map(|value| *value.unwrap())
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert!(matches!(
        downstream[0].next_until(Instant::now() + Duration::from_millis(10)),
        Err(FlowError::Deadline)
    ));
    assert_eq!(control.stats().stage_finished, vec![0, 0]);
    assert_eq!(control.stats().charged_bytes, 12);
    batch.finish();
    let downstream_batch = downstream[0]
        .next_until(deadline())
        .unwrap()
        .unwrap()
        .coalesce(4, |_| true)
        .unwrap();
    assert_eq!(downstream_batch.len(), 2);
    downstream_batch.finish();
    assert_eq!(control.stats().reclaimed, 2);
    assert_eq!(control.stats().charged_bytes, 4);
    control.close();
    for stage in &mut stages {
        stage.next_until(deadline()).unwrap().unwrap().finish();
        assert!(stage.next_until(deadline()).unwrap().is_none());
    }
    drop(stages);
    assert!(!control.stats().fenced);
}

#[test]
fn unfinished_delivery_and_abandoned_stage_fence_instead_of_shedding() {
    let (producer, mut stages, control) = bounded(2, 4, &[vec![]]).unwrap();
    producer.try_claim(1).unwrap().publish(42);
    drop(stages[0].next_until(deadline()).unwrap().unwrap());
    assert!(control.stats().fenced);
    assert_eq!(control.stats().reclaimed, 0);
    assert!(matches!(producer.try_claim(1), Err(FlowError::Failed(_))));
    assert!(matches!(
        stages[0].next_until(deadline()),
        Err(FlowError::Failed(_))
    ));
    let (producer, stages, control) = bounded::<usize>(2, 4, &[vec![]]).unwrap();
    drop(stages);
    assert!(control.stats().fenced);
    assert!(matches!(producer.try_claim(1), Err(FlowError::Failed(_))));
}

#[test]
fn bytes_and_slots_are_independent_and_full_slots_do_not_busy_spin() {
    let (producer, mut stages, control) = bounded::<usize>(1, 100, &[vec![]]).unwrap();
    producer.try_claim(1).unwrap().publish(1);
    assert!(matches!(producer.try_claim(101), Err(FlowError::TooLarge)));
    let attempts = producer.core.admission_attempts.load(Ordering::Relaxed);
    assert!(matches!(
        producer.claim_until(1, Instant::now() + Duration::from_millis(30)),
        Err(FlowError::Deadline)
    ));
    assert!(producer.core.admission_attempts.load(Ordering::Relaxed) - attempts < 8);
    assert_eq!(control.stats().charged_bytes, 1);
    assert_eq!(control.stats().claimed, 1);
    control.close();
    stages[0].next_until(deadline()).unwrap().unwrap().finish();
    assert!(stages[0].next_until(deadline()).unwrap().is_none());
}

#[test]
fn close_wakes_all_blocking_producers_and_drains_existing_reservations() {
    let (producer, mut stages, control) = bounded::<usize>(1, 1, &[vec![]]).unwrap();
    let held = producer.try_claim(1).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let waiters: Vec<_> = (0..4)
        .map(|_| {
            let producer = producer.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                matches!(producer.claim_until(1, deadline()), Err(FlowError::Closed))
            })
        })
        .collect();
    barrier.wait();
    control.close();
    for waiter in waiters {
        assert!(waiter.join().unwrap());
    }
    held.publish(7);
    let delivery = stages[0].next_until(deadline()).unwrap().unwrap();
    assert_eq!(delivery.value(), Some(&7));
    delivery.finish();
    assert!(stages[0].next_until(deadline()).unwrap().is_none());
    assert_eq!(control.stats().charged_bytes, 0);
    drop(stages);
    assert!(!control.stats().fenced);
}

#[tokio::test]
async fn cancelled_async_wait_has_no_slot_or_credit_and_completion_wakes_next() {
    let (producer, mut stages, control) = bounded::<usize>(1, 16, &[vec![]]).unwrap();
    producer.try_claim(1).unwrap().publish(1);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            producer.claim_async(1, deadline())
        )
        .await
        .is_err()
    );
    assert_eq!(control.stats().claimed, 1);
    assert_eq!(control.stats().charged_bytes, 1);
    assert!(producer.core.admission_attempts.load(Ordering::Relaxed) < 8);
    let waiting = producer.clone();
    let waiter =
        tokio::spawn(async move { waiting.claim_async(1, deadline()).await.unwrap().publish(2) });
    tokio::task::yield_now().await;
    stages[0].next_until(deadline()).unwrap().unwrap().finish();
    assert_eq!(waiter.await.unwrap(), 2);
    control.close();
    stages[0].next_until(deadline()).unwrap().unwrap().finish();
    assert!(stages[0].next_until(deadline()).unwrap().is_none());
    assert_eq!(control.stats().charged_bytes, 0);
}
