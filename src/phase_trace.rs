//! Opt-in, same-thread phase attribution; unrelated database and worker activity is excluded.
use super::{Metrics, Phase};
use serde::Serialize;
use std::cell::RefCell;

#[derive(Clone, Debug, Serialize)]
pub struct PhaseTrace {
    pub phase: &'static str,
    pub count: u64,
    pub total_ns: u64,
    pub max_ns: u64,
}

#[derive(Clone, Copy, Default)]
struct Cost {
    count: u64,
    total_ns: u64,
    max_ns: u64,
}

struct Capture {
    owner: usize,
    costs: [Cost; Phase::ALL.len()],
}

thread_local! {
    static ACTIVE: RefCell<Option<Capture>> = const { RefCell::new(None) };
}

pub(super) fn observe(metrics: &Metrics, phase: Phase, ns: u64) {
    ACTIVE.with_borrow_mut(|active| {
        if let Some(active) = active
            .as_mut()
            .filter(|capture| capture.owner == metrics as *const _ as usize)
        {
            let cost = &mut active.costs[phase as usize];
            cost.count = cost.count.saturating_add(1);
            cost.total_ns = cost.total_ns.saturating_add(ns);
            cost.max_ns = cost.max_ns.max(ns);
        }
    });
}

struct Restore(Option<Capture>);
impl Drop for Restore {
    fn drop(&mut self) {
        ACTIVE.set(self.0.take());
    }
}

pub(super) fn capture<R>(metrics: &Metrics, run: impl FnOnce() -> R) -> (R, Vec<PhaseTrace>) {
    let previous = ACTIVE.replace(Some(Capture {
        owner: metrics as *const _ as usize,
        costs: [Cost::default(); Phase::ALL.len()],
    }));
    let restore = Restore(previous);
    let result = run();
    let current = ACTIVE.take().expect("active phase capture");
    drop(restore);
    let phases = Phase::ALL
        .into_iter()
        .zip(current.costs)
        .filter_map(|(phase, cost)| {
            (cost.count > 0).then_some(PhaseTrace {
                phase: phase.as_str(),
                count: cost.count,
                total_ns: cost.total_ns,
                max_ns: cost.max_ns,
            })
        })
        .collect();
    (result, phases)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn capture_is_database_and_thread_local_and_nesting_restores() {
        let a = Metrics::default();
        let b = Metrics::default();
        let (_, outer) = capture(&a, || {
            a.observe(Phase::WalSync, Duration::from_nanos(7));
            b.observe(Phase::WalSync, Duration::from_nanos(100));
            std::thread::scope(|scope| {
                scope
                    .spawn(|| a.observe(Phase::WalSync, Duration::from_nanos(200)))
                    .join()
                    .unwrap();
            });
            let (_, inner) = capture(&a, || a.observe(Phase::WalSync, Duration::from_nanos(11)));
            assert_eq!(inner[0].total_ns, 11);
            a.observe(Phase::WalSync, Duration::from_nanos(13));
        });
        assert_eq!(outer.len(), 1);
        assert_eq!(
            (outer[0].count, outer[0].total_ns, outer[0].max_ns),
            (2, 20, 13)
        );
        assert!(ACTIVE.with_borrow(Option::is_none));
    }

    #[test]
    fn unwind_restores_capture_and_observations_saturate() {
        let metrics = Metrics::default();
        let (_, phases) = capture(&metrics, || {
            let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                capture(&metrics, || panic!("fixture"));
            }));
            assert!(failure.is_err());
            metrics.observe(Phase::WalSync, Duration::MAX);
            metrics.observe(Phase::WalSync, Duration::MAX);
        });
        assert_eq!(phases[0].total_ns, u64::MAX);
        assert_eq!(phases[0].count, 2);
        assert!(ACTIVE.with_borrow(Option::is_none));
    }
}
