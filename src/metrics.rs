//! Fixed-cardinality, database-lifetime phase observations.
//!
//! Durations are wall time, include failed attempts, and overlap for nested phases.
//! Concurrent snapshots are approximate; these counters are not transaction state.
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
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
}

impl Phase {
    pub const ALL: [Self; 18] = [
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
        let counter = &self.counters[phase as usize];
        saturating_add(&counter.total_ns, ns);
        counter.max_ns.fetch_max(ns, Ordering::Relaxed);
        let bucket = DURATION_BOUNDS_NS.partition_point(|bound| *bound < ns);
        saturating_add(&counter.buckets[bucket], 1);
    }

    pub fn timer(&self, phase: Phase) -> PhaseTimer<'_> {
        PhaseTimer {
            metrics: self,
            phase,
            start: Instant::now(),
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
