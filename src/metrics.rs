//! Fixed-cardinality, database-lifetime phase observations.
//!
//! Durations are wall time, include failed attempts, and overlap for nested phases.
//! Concurrent snapshots are approximate; these counters are not transaction state.
#[path = "phase_trace.rs"]
mod capture;
pub use capture::PhaseTrace;

use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LockResult, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// Inclusive histogram bounds; the final bucket also contains saturated durations.
pub const DURATION_BOUNDS_NS: [u64; 10] = [
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    60_000_000_000,
    u64::MAX,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum Phase {
    StateLockWait,
    StateLockHold,
    WritePrepare,
    WalEncode,
    WalWrite,
    WalSync,
    Snapshot,
    QueryBuild,
    QueryWait,
    QueryRun,
    QuerySpawn,
    QueryReset,
    CheckpointPrepare,
    CheckpointPublish,
    CompactionPrepare,
    CompactionPublish,
    DiskAccount,
    RemoteIo,
    CheckpointCapture,
    CheckpointLocked,
    RootPrepare,
    ManifestCommit,
    DiskLockWait,
    DiskLockHold,
    WalDiskLockWait,
    GroupPrepare,
    DerivedVerify,
    DerivedPublish,
    RawVerify,
    RawPublish,
    AppendAccounting,
    CommitLockWait,
    CommitDetach,
    CommitInstall,
    AdmissionCheckpoint,
    CheckpointReclaim,
    WalFileSync,
    WalDirectorySync,
}

impl Phase {
    pub const ALL: [Self; 38] = [
        Self::StateLockWait,
        Self::StateLockHold,
        Self::WritePrepare,
        Self::WalEncode,
        Self::WalWrite,
        Self::WalSync,
        Self::Snapshot,
        Self::QueryBuild,
        Self::QueryWait,
        Self::QueryRun,
        Self::QuerySpawn,
        Self::QueryReset,
        Self::CheckpointPrepare,
        Self::CheckpointPublish,
        Self::CompactionPrepare,
        Self::CompactionPublish,
        Self::DiskAccount,
        Self::RemoteIo,
        Self::CheckpointCapture,
        Self::CheckpointLocked,
        Self::RootPrepare,
        Self::ManifestCommit,
        Self::DiskLockWait,
        Self::DiskLockHold,
        Self::WalDiskLockWait,
        Self::GroupPrepare,
        Self::DerivedVerify,
        Self::DerivedPublish,
        Self::RawVerify,
        Self::RawPublish,
        Self::AppendAccounting,
        Self::CommitLockWait,
        Self::CommitDetach,
        Self::CommitInstall,
        Self::AdmissionCheckpoint,
        Self::CheckpointReclaim,
        Self::WalFileSync,
        Self::WalDirectorySync,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StateLockWait => "state_lock_wait",
            Self::StateLockHold => "state_lock_hold",
            Self::WritePrepare => "write_prepare",
            Self::WalEncode => "wal_encode",
            Self::WalWrite => "wal_write",
            Self::WalSync => "wal_sync",
            Self::Snapshot => "snapshot",
            Self::QueryBuild => "query_build",
            Self::QueryWait => "query_wait",
            Self::QueryRun => "query_run",
            Self::QuerySpawn => "query_spawn",
            Self::QueryReset => "query_reset",
            Self::CheckpointPrepare => "checkpoint_prepare",
            Self::CheckpointPublish => "checkpoint_publish",
            Self::CompactionPrepare => "compaction_prepare",
            Self::CompactionPublish => "compaction_publish",
            Self::DiskAccount => "disk_account",
            Self::RemoteIo => "remote_io",
            Self::CheckpointCapture => "checkpoint_capture",
            Self::CheckpointLocked => "checkpoint_locked",
            Self::RootPrepare => "root_prepare",
            Self::ManifestCommit => "manifest_commit",
            Self::DiskLockWait => "disk_lock_wait",
            Self::DiskLockHold => "disk_lock_hold",
            Self::WalDiskLockWait => "wal_disk_lock_wait",
            Self::GroupPrepare => "group_prepare",
            Self::DerivedVerify => "derived_verify",
            Self::DerivedPublish => "derived_publish",
            Self::RawVerify => "raw_verify",
            Self::RawPublish => "raw_publish",
            Self::AppendAccounting => "append_accounting",
            Self::CommitLockWait => "commit_lock_wait",
            Self::CommitDetach => "commit_detach",
            Self::CommitInstall => "commit_install",
            Self::AdmissionCheckpoint => "admission_checkpoint",
            Self::CheckpointReclaim => "checkpoint_reclaim",
            Self::WalFileSync => "wal_file_sync",
            Self::WalDirectorySync => "wal_directory_sync",
        }
    }
}

struct Counter {
    total_ns: AtomicU64,
    max_ns: AtomicU64,
    buckets: [AtomicU64; DURATION_BOUNDS_NS.len()],
}

impl Default for Counter {
    fn default() -> Self {
        Self {
            total_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

fn saturating_add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(amount))
    });
}

/// No user-controlled labels, allocations on observation, or global singleton.
pub struct Metrics {
    counters: [Counter; Phase::ALL.len()],
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            counters: std::array::from_fn(|_| Counter::default()),
        }
    }
}

impl Metrics {
    pub fn observe(&self, phase: Phase, duration: Duration) {
        let ns = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        capture::observe(self, phase, ns);
        let counter = &self.counters[phase as usize];
        saturating_add(&counter.total_ns, ns);
        counter.max_ns.fetch_max(ns, Ordering::Relaxed);
        let bucket = DURATION_BOUNDS_NS.partition_point(|bound| *bound < ns);
        saturating_add(&counter.buckets[bucket], 1);
    }

    pub(crate) fn capture<R>(&self, run: impl FnOnce() -> R) -> (R, Vec<PhaseTrace>) {
        capture::capture(self, run)
    }

    pub fn timer(&self, phase: Phase) -> PhaseTimer<'_> {
        PhaseTimer {
            metrics: self,
            phase,
            start: Instant::now(),
        }
    }

    /// Measures the existing disk gate without changing its scope or poison policy.
    /// A poisoned lock still returns ownership in its error, just like Mutex::lock;
    /// that acquired guard is measured until the caller drops or recovers it.
    pub(crate) fn lock_disk<'a>(
        &'a self,
        mutex: &'a Mutex<()>,
    ) -> LockResult<MeasuredDiskGuard<'a>> {
        let wait = self.timer(Phase::DiskLockWait);
        let acquired = mutex.lock();
        drop(wait);
        let hold = self.timer(Phase::DiskLockHold);
        match acquired {
            Ok(guard) => Ok(MeasuredDiskGuard {
                _hold: hold,
                _guard: guard,
            }),
            Err(poison) => Err(PoisonError::new(MeasuredDiskGuard {
                _hold: hold,
                _guard: poison.into_inner(),
            })),
        }
    }

    pub fn snapshot(&self) -> PerformanceSnapshot {
        let phases = Phase::ALL
            .into_iter()
            .map(|phase| {
                let counter = &self.counters[phase as usize];
                let buckets =
                    std::array::from_fn(|index| counter.buckets[index].load(Ordering::Relaxed));
                let count = buckets
                    .iter()
                    .fold(0u64, |sum, value| sum.saturating_add(*value));
                (
                    phase.as_str().to_owned(),
                    PhaseSnapshot {
                        count,
                        total_ns: counter.total_ns.load(Ordering::Relaxed),
                        max_ns: counter.max_ns.load(Ordering::Relaxed),
                        buckets,
                    },
                )
            })
            .collect();
        PerformanceSnapshot { phases }
    }
}

#[must_use = "keep the timer alive for the operation being observed"]
pub struct PhaseTimer<'a> {
    metrics: &'a Metrics,
    phase: Phase,
    start: Instant,
}

impl Drop for PhaseTimer<'_> {
    fn drop(&mut self) {
        self.metrics.observe(self.phase, self.start.elapsed());
    }
}

/// Keeps the underlying mutex locked for exactly the caller's guard lifetime.
/// Field order records hold time before releasing the mutex, including on unwind.
#[must_use = "keep the guard alive for the existing disk admission scope"]
pub(crate) struct MeasuredDiskGuard<'a> {
    _hold: PhaseTimer<'a>,
    _guard: MutexGuard<'a, ()>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PhaseSnapshot {
    pub count: u64,
    pub total_ns: u64,
    pub max_ns: u64,
    /// Disjoint bucket counts, ordered by DURATION_BOUNDS_NS.
    pub buckets: [u64; DURATION_BOUNDS_NS.len()],
}

#[derive(Clone, Debug, Serialize)]
pub struct PerformanceSnapshot {
    pub phases: BTreeMap<String, PhaseSnapshot>,
}

impl PerformanceSnapshot {
    /// Emits cumulative Prometheus buckets; labels come exclusively from Phase.
    pub fn prometheus(&self) -> String {
        let mut output = String::from(
            "# HELP varve_phase_duration_seconds Wall time of attempted phases; nested phases overlap.\n\
             # TYPE varve_phase_duration_seconds histogram\n\
             # TYPE varve_phase_duration_seconds_max gauge\n",
        );
        for phase in Phase::ALL {
            let name = phase.as_str();
            let Some(sample) = self.phases.get(name) else {
                continue;
            };
            let mut cumulative = 0u64;
            for (bound, count) in DURATION_BOUNDS_NS.into_iter().zip(sample.buckets) {
                cumulative = cumulative.saturating_add(count);
                let limit = if bound == u64::MAX {
                    "+Inf".to_owned()
                } else {
                    (bound as f64 / 1_000_000_000.0).to_string()
                };
                writeln!(output, "varve_phase_duration_seconds_bucket{{phase=\"{name}\",le=\"{limit}\"}} {cumulative}").expect("format into String");
            }
            writeln!(
                output,
                "varve_phase_duration_seconds_count{{phase=\"{name}\"}} {}",
                sample.count
            )
            .expect("format into String");
            writeln!(
                output,
                "varve_phase_duration_seconds_sum{{phase=\"{name}\"}} {}",
                sample.total_ns as f64 / 1_000_000_000.0
            )
            .expect("format into String");
            writeln!(
                output,
                "varve_phase_duration_seconds_max{{phase=\"{name}\"}} {}",
                sample.max_ns as f64 / 1_000_000_000.0
            )
            .expect("format into String");
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_bounds_and_saturation_are_exact_without_timing_a_workload() {
        let metrics = Metrics::default();
        for ns in [0, 1_000, 1_001, 10_000, 10_001] {
            metrics.observe(Phase::WalSync, Duration::from_nanos(ns));
        }
        let snapshot = metrics.snapshot();
        let sample = &snapshot.phases["wal_sync"];
        assert_eq!(sample.count, 5);
        assert_eq!(sample.total_ns, 22_002);
        assert_eq!(sample.max_ns, 10_001);
        assert_eq!(&sample.buckets[..3], &[2, 2, 1]);
        let text = snapshot.prometheus();
        assert!(text.contains(
            "varve_phase_duration_seconds_bucket{phase=\"wal_sync\",le=\"0.00001\"} 4\n"
        ));
        assert!(
            text.contains(
                "varve_phase_duration_seconds_bucket{phase=\"wal_sync\",le=\"+Inf\"} 5\n"
            )
        );
        metrics.observe(Phase::WalSync, Duration::MAX);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.phases["wal_sync"].total_ns, u64::MAX);
        assert_eq!(snapshot.phases["wal_sync"].max_ns, u64::MAX);
        assert_eq!(snapshot.phases["wal_sync"].count, 6);
    }

    #[test]
    fn timer_records_unwinding_attempts_and_database_instances_do_not_mix() {
        let metrics = Metrics::default();
        let _ = std::panic::catch_unwind(|| {
            let _timer = metrics.timer(Phase::QueryRun);
            panic!("attempt failed");
        });
        assert_eq!(metrics.snapshot().phases["query_run"].count, 1);
        assert_eq!(Metrics::default().snapshot().phases["query_run"].count, 0);
        let names: std::collections::BTreeSet<_> =
            Phase::ALL.into_iter().map(Phase::as_str).collect();
        assert_eq!(names.len(), Phase::ALL.len());
    }

    #[test]
    fn every_phase_is_registered_observed_and_exported_exactly_once() {
        let metrics = Metrics::default();
        for (index, phase) in Phase::ALL.into_iter().enumerate() {
            assert_eq!(phase as usize, index);
            metrics.observe(phase, Duration::from_nanos(index as u64 + 1));
        }
        let snapshot = metrics.snapshot();
        let text = snapshot.prometheus();
        assert_eq!(snapshot.phases.len(), Phase::ALL.len());
        for (index, phase) in Phase::ALL.into_iter().enumerate() {
            let sample = &snapshot.phases[phase.as_str()];
            assert_eq!(sample.count, 1);
            assert_eq!(sample.total_ns, index as u64 + 1);
            assert_eq!(sample.max_ns, index as u64 + 1);
            let line = format!(
                "varve_phase_duration_seconds_count{{phase=\"{}\"}} 1\n",
                phase.as_str()
            );
            assert_eq!(text.matches(&line).count(), 1);
        }
    }

    #[test]
    fn phase_indices_and_names_preserve_the_original_twenty_two() {
        let expected = [
            "state_lock_wait",
            "state_lock_hold",
            "write_prepare",
            "wal_encode",
            "wal_write",
            "wal_sync",
            "snapshot",
            "query_build",
            "query_wait",
            "query_run",
            "query_spawn",
            "query_reset",
            "checkpoint_prepare",
            "checkpoint_publish",
            "compaction_prepare",
            "compaction_publish",
            "disk_account",
            "remote_io",
            "checkpoint_capture",
            "checkpoint_locked",
            "root_prepare",
            "manifest_commit",
            "disk_lock_wait",
            "disk_lock_hold",
            "wal_disk_lock_wait",
            "group_prepare",
            "derived_verify",
            "derived_publish",
            "raw_verify",
            "raw_publish",
            "append_accounting",
            "commit_lock_wait",
            "commit_detach",
            "commit_install",
            "admission_checkpoint",
            "checkpoint_reclaim",
            "wal_file_sync",
            "wal_directory_sync",
        ];
        assert_eq!(Phase::ALL.len(), expected.len());
        for (index, (phase, name)) in Phase::ALL.into_iter().zip(expected).enumerate() {
            assert_eq!(phase as usize, index);
            assert_eq!(phase.as_str(), name);
        }
    }

    #[test]
    fn appended_phase_timers_record_errors_and_unwinds_in_isolation() {
        fn fail(metrics: &Metrics, phase: Phase) -> Result<(), ()> {
            let _timer = metrics.timer(phase);
            Err(())?;
            Ok(())
        }
        let metrics = Metrics::default();
        let other = Metrics::default();
        for phase in Phase::ALL.into_iter().skip(22) {
            assert!(fail(&metrics, phase).is_err());
            assert!(
                std::panic::catch_unwind(|| {
                    let _timer = metrics.timer(phase);
                    panic!("failed measured attempt");
                })
                .is_err()
            );
        }
        for (index, phase) in Phase::ALL.into_iter().enumerate() {
            assert_eq!(
                metrics.snapshot().phases[phase.as_str()].count,
                if index < 22 { 0 } else { 2 }
            );
            assert_eq!(other.snapshot().phases[phase.as_str()].count, 0);
        }
    }

    #[test]
    fn disk_guard_records_only_completed_scopes_and_releases_on_error() {
        let metrics = Metrics::default();
        let other = Metrics::default();
        let mutex = Mutex::new(());
        let guard = metrics
            .lock_disk(&mutex)
            .unwrap_or_else(|_| panic!("unexpected poison"));
        assert!(matches!(
            mutex.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        assert_eq!(metrics.snapshot().phases["disk_lock_wait"].count, 1);
        assert_eq!(metrics.snapshot().phases["disk_lock_hold"].count, 0);
        drop(guard);
        assert!(mutex.try_lock().is_ok());
        assert_eq!(metrics.snapshot().phases["disk_lock_hold"].count, 1);

        let attempt = || -> Result<(), ()> {
            let _guard = metrics.lock_disk(&mutex).map_err(|_| ())?;
            Err(())
        };
        assert!(attempt().is_err());
        assert!(mutex.try_lock().is_ok());
        for phase in Phase::ALL {
            let expected =
                u64::from(matches!(phase, Phase::DiskLockWait | Phase::DiskLockHold)) * 2;
            assert_eq!(metrics.snapshot().phases[phase.as_str()].count, expected);
            assert_eq!(other.snapshot().phases[phase.as_str()].count, 0);
        }
    }

    #[test]
    fn disk_guard_unwinds_and_preserves_poison_ownership() {
        let metrics = Metrics::default();
        let mutex = Mutex::new(());
        assert!(
            std::panic::catch_unwind(|| {
                let _guard = metrics
                    .lock_disk(&mutex)
                    .unwrap_or_else(|_| panic!("unexpected poison"));
                panic!("poison while holding disk");
            })
            .is_err()
        );
        assert!(mutex.is_poisoned());
        assert_eq!(metrics.snapshot().phases["disk_lock_wait"].count, 1);
        assert_eq!(metrics.snapshot().phases["disk_lock_hold"].count, 1);
        let recovered = match metrics.lock_disk(&mutex) {
            Ok(_) => panic!("poison was swallowed"),
            Err(poison) => poison.into_inner(),
        };
        assert!(matches!(
            mutex.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        assert_eq!(metrics.snapshot().phases["disk_lock_wait"].count, 2);
        assert_eq!(metrics.snapshot().phases["disk_lock_hold"].count, 1);
        drop(recovered);
        assert_eq!(metrics.snapshot().phases["disk_lock_hold"].count, 2);
        assert!(mutex.is_poisoned());
        // Engine's unchanged map_err rejects poison, dropping its acquired guard.
        assert!(metrics.lock_disk(&mutex).map_err(|_| ()).is_err());
        assert_eq!(metrics.snapshot().phases["disk_lock_wait"].count, 3);
        assert_eq!(metrics.snapshot().phases["disk_lock_hold"].count, 3);
        assert!(matches!(
            mutex.try_lock(),
            Err(std::sync::TryLockError::Poisoned(_))
        ));
    }

    #[test]
    fn disk_guard_contention_uses_barriers_not_elapsed_thresholds() {
        use std::sync::{Barrier, TryLockError};
        let metrics = Metrics::default();
        let mutex = Mutex::new(());
        let ready = Barrier::new(2);
        let acquired = Barrier::new(2);
        let release = Barrier::new(2);
        let (blocked, before_release, second_held) = std::thread::scope(|scope| {
            let first = metrics
                .lock_disk(&mutex)
                .unwrap_or_else(|_| panic!("unexpected poison"));
            let worker = scope.spawn(|| {
                let blocked = matches!(mutex.try_lock(), Err(TryLockError::WouldBlock));
                ready.wait();
                let second = metrics
                    .lock_disk(&mutex)
                    .unwrap_or_else(|_| panic!("unexpected poison"));
                acquired.wait();
                release.wait();
                drop(second);
                blocked
            });
            ready.wait();
            // The second acquisition cannot finish until the first guard is dropped.
            let before_release = metrics.snapshot();
            drop(first);
            acquired.wait();
            let second_held = metrics.snapshot();
            // Release and join before asserting, so a count regression cannot strand a barrier.
            release.wait();
            (worker.join().unwrap(), before_release, second_held)
        });
        assert!(blocked);
        assert_eq!(before_release.phases["disk_lock_wait"].count, 1);
        assert_eq!(before_release.phases["disk_lock_hold"].count, 0);
        assert_eq!(second_held.phases["disk_lock_wait"].count, 2);
        assert_eq!(second_held.phases["disk_lock_hold"].count, 1);
        assert_eq!(metrics.snapshot().phases["disk_lock_wait"].count, 2);
        assert_eq!(metrics.snapshot().phases["disk_lock_hold"].count, 2);
        assert!(mutex.try_lock().is_ok());
    }

    #[test]
    fn concurrent_observers_are_exact_after_join() {
        let metrics = Metrics::default();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..32 {
                        metrics.observe(Phase::WritePrepare, Duration::from_nanos(7));
                    }
                });
            }
        });
        let sample = &metrics.snapshot().phases["write_prepare"];
        assert_eq!(sample.count, 128);
        assert_eq!(sample.total_ns, 896);
        assert_eq!(sample.max_ns, 7);
    }
}
