//! Bounded diagnostic history. Not a receipt store or a durability frontier.
use crate::metrics::PhaseTrace;
use serde::Serialize;
use std::collections::VecDeque;

#[derive(Clone, Debug, Serialize)]
pub struct IngestTrace {
    pub group: u64,
    pub requests: usize,
    pub rows: usize,
    pub oldest_queue_ns: u64,
    pub newest_queue_ns: u64,
    pub service_ns: u64,
    /// Successful receipt sequences, including old sequences from duplicate retries.
    pub sequences: Vec<u64>,
    pub failed: usize,
    pub duplicate: usize,
    /// Nested wall-clock phases overlap; never sum them as exclusive CPU time.
    pub phases: Vec<PhaseTrace>,
}

#[derive(Clone, Debug, Serialize)]
pub struct IngestTraceSnapshot {
    pub capacity: usize,
    pub evicted: u64,
    pub groups: Vec<IngestTrace>,
}

pub(super) struct TraceBuffer {
    capacity: usize,
    evicted: u64,
    groups: VecDeque<IngestTrace>,
}

impl TraceBuffer {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            evicted: 0,
            groups: VecDeque::with_capacity(capacity),
        }
    }

    pub(super) fn push(&mut self, trace: IngestTrace) {
        if self.capacity == 0 {
            return;
        }
        if self.groups.len() == self.capacity {
            self.groups.pop_front();
            self.evicted = self.evicted.saturating_add(1);
        }
        self.groups.push_back(trace);
    }

    pub(super) fn snapshot(&self) -> IngestTraceSnapshot {
        IngestTraceSnapshot {
            capacity: self.capacity,
            evicted: self.evicted,
            groups: self.groups.iter().cloned().collect(),
        }
    }
}
