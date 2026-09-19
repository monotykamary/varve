//! Small memory-model witness for flow::Core's terminal reclamation protocol.
//! This models the exact completion/inspection ordering, not the full queue,
//! condition-variable wakeups, payload lifetime, or journal implementation.

#[cfg(test)]
mod tests {
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::{Arc, Mutex};

    fn completion_protocol(lock_before_inspection: bool) {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(2);
        model.check(move || {
            let shared = Arc::new((
                [AtomicUsize::new(0), AtomicUsize::new(0)],
                AtomicUsize::new(0),
                Mutex::new(()),
            ));
            let workers: Vec<_> = (0..2)
                .map(|index| {
                    let shared = Arc::clone(&shared);
                    loom::thread::spawn(move || {
                        shared.0[index].store(1, Ordering::Release);
                        let early_guard = lock_before_inspection.then(|| shared.2.lock().unwrap());
                        let minimum = shared
                            .0
                            .iter()
                            .map(|cursor| cursor.load(Ordering::Acquire))
                            .min()
                            .unwrap();
                        if minimum <= shared.1.load(Ordering::Acquire) {
                            return;
                        }
                        let late_guard =
                            (!lock_before_inspection).then(|| shared.2.lock().unwrap());
                        if shared.1.load(Ordering::Relaxed) < minimum {
                            shared.1.store(minimum, Ordering::Release);
                        }
                        drop(late_guard);
                        drop(early_guard);
                    })
                })
                .collect();
            for worker in workers {
                worker.join().unwrap();
            }
            assert_eq!(
                shared.1.load(Ordering::Acquire),
                1,
                "finished consumers stranded reclamation"
            );
        });
    }

    #[test]
    #[should_panic(expected = "finished consumers stranded reclamation")]
    fn old_prelock_snapshot_has_a_counterexample() {
        completion_protocol(false);
    }

    #[test]
    fn inspection_serialized_before_any_early_return_reclaims() {
        completion_protocol(true);
    }
}
