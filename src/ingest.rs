//! Bounded multi-producer ingestion. Admission is not a durability acknowledgement.
#[path = "ingest_trace.rs"]
mod trace;
use trace::TraceBuffer;
pub use trace::{IngestTrace, IngestTraceSnapshot};

use crate::engine::AdmittedWrite;
use crate::{Database, WriteReceipt, WriteRequest};
use anyhow::{Result, anyhow, ensure};
#[path = "ingest_flow.rs"]
mod flow;
#[path = "ingest_flow_workers.rs"]
mod workers;
use flow::Runtime;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, oneshot};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestConfig {
    pub queue_capacity: usize,
    pub max_pending_bytes: usize,
    pub max_group_requests: usize,
    pub max_group_rows: usize,
    pub max_group_bytes: usize,
    pub max_delay: Duration,
    /// Retain bounded same-thread group diagnostics; zero disables capture.
    pub trace_capacity: usize,
}
impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            max_pending_bytes: 16 * 1024 * 1024,
            max_group_requests: 128,
            max_group_rows: 10_000,
            max_group_bytes: 4 * 1024 * 1024,
            max_delay: Duration::from_millis(2),
            trace_capacity: 0,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct IngestStats {
    pub submitted: u64,
    pub rejected: u64,
    pub completed: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub dropped_receivers: u64,
    /// Requests which encountered bounded admission pressure and waited.
    pub admission_waits: u64,
    pub waiting_requests: usize,
    pub peak_waiting_requests: usize,
    pub admission_wait_ns: u64,
    /// Waiting futures cancelled or rejected before ownership reached the queue.
    pub waits_without_admission: u64,
    /// Worker flushes, not necessarily physical frames (all-retry groups write none).
    pub groups: u64,
    pub pending_requests: usize,
    pub pending_bytes: usize,
    pub peak_pending_requests: usize,
    pub peak_pending_bytes: usize,
    pub closed: bool,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

struct Pending {
    completion: Option<oneshot::Sender<Result<WriteReceipt>>>,
    bytes: usize,
    stats: Arc<Mutex<IngestStats>>,
    capacity: Arc<Notify>,
}
impl Pending {
    fn finish(&mut self, result: Result<WriteReceipt>) {
        let Some(completion) = self.completion.take() else {
            return;
        };
        let succeeded = result.is_ok();
        // Release the budget before waking the producer, so a completed producer
        // can immediately submit again even with a one-request byte budget.
        let mut stats = lock(&self.stats);
        stats.pending_requests -= 1;
        stats.pending_bytes -= self.bytes;
        stats.completed = stats.completed.saturating_add(1);
        if succeeded {
            stats.succeeded = stats.succeeded.saturating_add(1);
        } else {
            stats.failed = stats.failed.saturating_add(1);
        }
        if completion.send(result).is_err() {
            stats.dropped_receivers = stats.dropped_receivers.saturating_add(1);
        }
        drop(stats);
        self.capacity.notify_waiters();
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.finish(Err(anyhow!(
            "ingestion worker stopped before completion; retry the same request ID"
        )));
    }
}

/// A FIFO ingestion drain result. Counts include successes and failures since startup.
/// `sequence` covers preceding successful requests; failed requests are not committed.
#[derive(Clone, Debug, Serialize)]
pub struct IngestFlush {
    pub sequence: u64,
    pub completed: u64,
    pub succeeded: u64,
    pub failed: u64,
}

struct Owner {
    raw_memory: crate::raw_memory::RawMemoryBudget,
    flow: Arc<Runtime>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    config: IngestConfig,
    stats: Arc<Mutex<IngestStats>>,
    traces: Arc<Mutex<TraceBuffer>>,
    capacity: Arc<Notify>,
}
impl Owner {
    fn shutdown(&self) -> Result<()> {
        self.flow.close();
        // Keep this lock through every join: concurrent callers also await drain.
        let mut workers = lock(&self.workers);
        let mut panicked = false;
        for worker in workers.drain(..) {
            panicked |= worker.join().is_err();
        }
        ensure!(!panicked, "ingestion worker panicked");
        self.flow.check_failure()
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
thread_local! {
    static RAW_WAIT_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

enum Admission {
    Accepted(oneshot::Receiver<Result<WriteReceipt>>),
    Full(&'static str),
    RawPressure,
}

struct AdmissionWait {
    stats: Arc<Mutex<IngestStats>>,
    started: Instant,
    admitted: bool,
}
impl AdmissionWait {
    fn new(stats: Arc<Mutex<IngestStats>>) -> Self {
        {
            let mut current = lock(&stats);
            current.admission_waits = current.admission_waits.saturating_add(1);
            current.waiting_requests += 1;
            current.peak_waiting_requests =
                current.peak_waiting_requests.max(current.waiting_requests);
        }
        Self {
            stats,
            started: Instant::now(),
            admitted: false,
        }
    }
}
impl Drop for AdmissionWait {
    fn drop(&mut self) {
        let mut stats = lock(&self.stats);
        stats.waiting_requests -= 1;
        stats.admission_wait_ns = stats
            .admission_wait_ns
            .saturating_add(u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        if !self.admitted {
            stats.waits_without_admission = stats.waits_without_admission.saturating_add(1);
        }
    }
}

#[derive(Clone)]
pub struct Ingestor {
    inner: Arc<Owner>,
}
impl Ingestor {
    pub fn new(db: Database, mut config: IngestConfig) -> Result<Self> {
        ensure!(
            config.queue_capacity > 0 && config.queue_capacity <= 1_000_000,
            "ingestion queue_capacity must be 1..1000000"
        );
        ensure!(
            config.max_pending_bytes > 0
                && config.max_group_bytes > 128
                && config.max_group_rows > 0
                && config.max_group_requests > 0
                && config.max_group_requests <= crate::wal::MAX_GROUP_REQUESTS
                && config.max_delay <= Duration::from_secs(60),
            "invalid ingestion limits"
        );
        ensure!(
            config.trace_capacity <= 1024
                && config
                    .trace_capacity
                    .saturating_mul(config.max_group_requests)
                    <= 65_536,
            "ingestion trace capacity exceeds bounded history limits"
        );
        // Engine limits still apply; clamping keeps each flush in one frame's envelope.
        config.max_group_rows = config
            .max_group_rows
            .min(db.inner.config.max_batch_rows)
            .min(db.inner.config.hot_max_rows);
        config.max_group_bytes = config
            .max_group_bytes
            .min(db.inner.config.max_batch_bytes)
            .min(db.inner.config.hot_max_bytes)
            .min(db.inner.config.wal_max_bytes as usize)
            .min(crate::wal::MAX_FRAME_BYTES);
        ensure!(
            config.max_group_bytes > 128,
            "engine byte budget is too small for ingestion"
        );
        let stats = Arc::new(Mutex::new(IngestStats::default()));
        let traces = Arc::new(Mutex::new(TraceBuffer::new(config.trace_capacity)));
        let capacity = Arc::new(Notify::new());
        let (flow, stages) = Runtime::new(&config, stats.clone(), capacity.clone())?;
        let raw_memory = db.inner.raw_memory.clone();
        let workers = workers::start(
            db,
            stages,
            flow.clone(),
            config.clone(),
            stats.clone(),
            traces.clone(),
        )?;
        Ok(Self {
            inner: Arc::new(Owner {
                raw_memory,
                flow,
                workers: Mutex::new(workers),
                config,
                stats,
                traces,
                capacity,
            }),
        })
    }

    /// A returned receiver means admitted, not committed. Err guarantees not enqueued.
    /// This nonblocking API retains its explicit full-queue rejection semantics.
    pub fn submit(&self, request: WriteRequest) -> Result<oneshot::Receiver<Result<WriteReceipt>>> {
        let result = (|| {
            let mut request = Some(AdmittedWrite::new(request)?);
            match self.try_enqueue(&mut request)? {
                Admission::Accepted(receiver) => Ok(receiver),
                Admission::Full(message) => Err(anyhow!(message)),
                Admission::RawPressure => {
                    Err(crate::raw_memory::RawReservationError::Pressure.into())
                }
            }
        })();
        self.record_rejection(&result);
        result
    }

    /// Wait for queue/byte credit without discarding or revalidating the input.
    /// The caller's future retains its request until admission, outside pending
    /// queue bytes; callers must bound their concurrent futures. Transports use
    /// their existing request slots and deadlines. Cancelling while waiting does
    /// not enqueue the request. After receiving the receipt channel, cancellation
    /// does not cancel publication: retry only with the same request identity.
    pub async fn submit_wait(
        &self,
        request: WriteRequest,
    ) -> Result<oneshot::Receiver<Result<WriteReceipt>>> {
        let result = async {
            let mut request = Some(AdmittedWrite::new(request)?);
            let mut waiting: Option<AdmissionWait> = None;
            loop {
                // Register before inspecting credit, so completion or shutdown
                // between the check and await cannot be lost.
                let notified = self.inner.capacity.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let released = self.inner.raw_memory.released().notified();
                tokio::pin!(released);
                released.as_mut().enable();
                match self.try_enqueue(&mut request)? {
                    Admission::Accepted(receiver) => {
                        if let Some(waiting) = &mut waiting {
                            waiting.admitted = true;
                        }
                        return Ok(receiver);
                    }
                    Admission::RawPressure => {
                        #[cfg(test)]
                        RAW_WAIT_HOOK.with(|hook| {
                            if let Some(hook) = hook.borrow_mut().take() {
                                hook();
                            }
                        });
                        waiting.get_or_insert_with(|| AdmissionWait::new(self.inner.stats.clone()));
                        tokio::select! { _ = released => {}, _ = notified => {} }
                    }
                    Admission::Full(_) => {
                        waiting.get_or_insert_with(|| AdmissionWait::new(self.inner.stats.clone()));
                        notified.await;
                    }
                }
            }
        }
        .await;
        self.record_rejection(&result);
        result
    }

    fn record_rejection<T>(&self, result: &Result<T>) {
        if result.is_err() {
            let mut stats = lock(&self.inner.stats);
            stats.rejected = stats.rejected.saturating_add(1);
        }
    }

    fn try_enqueue(&self, request: &mut Option<AdmittedWrite>) -> Result<Admission> {
        let input = request
            .as_ref()
            .expect("unadmitted input is owned by caller");
        let bytes = input.bytes();
        ensure!(
            input.rows() <= self.inner.config.max_group_rows
                && bytes.saturating_add(128) <= self.inner.config.max_group_bytes,
            "request exceeds ingestion group capacity"
        );
        ensure!(
            bytes <= self.inner.config.max_pending_bytes,
            "request exceeds ingestion pending byte budget"
        );
        self.inner.flow.check_open()?;
        if let Err(error) = request
            .as_mut()
            .expect("caller-owned request")
            .reserve(&self.inner.raw_memory)
        {
            if error.downcast_ref::<crate::raw_memory::RawReservationError>()
                == Some(&crate::raw_memory::RawReservationError::Pressure)
            {
                return Ok(Admission::RawPressure);
            }
            return Err(error);
        }
        let result = self.inner.flow.enqueue(request);
        // Full/closed admission leaves the input caller-owned, outside raw credit.
        if let Some(input) = request {
            input.release_reservation();
        }
        result
    }

    /// Enqueue a FIFO barrier, forcing a pending partial group to publish without
    /// waiting for its batching deadline. The receiver completes after preceding
    /// writes have terminal results; inspect `failed` as well as individual receipts.
    /// This does not checkpoint/evict hot rows or acknowledge remote durability.
    /// Queue-full/closed errors mean the barrier was not enqueued.
    pub fn flush(&self) -> Result<oneshot::Receiver<Result<IngestFlush>>> {
        self.inner.flow.flush()
    }

    /// Observational diagnostics for the four-stage ingestion graph:
    /// static validation, owned private epoch, sync + atomic visibility, completion.
    /// Frontiers count contiguous ring slots (including rejected/duplicate/no-WAL
    /// terminals), NOT WAL sequences or maxima of observed receipt sequences.
    /// Stage 1 really owns prepared database state; exclusivity still serializes
    /// preparation. Stage 2 advances only after sync AND install; no separate
    /// public D frontier is exposed.
    /// Fenced slot reservations remain until graph destruction, even after
    /// workers release private inputs and send every terminal completion.
    pub fn flow_stats(&self) -> crate::flow::FlowStats {
        self.inner.flow.control.stats()
    }

    /// Inject a fail-closed panic on the selected worker's next delivery.
    /// Stages: 0 static validation, 1 private epoch, 2 sync/install, 3 completion.
    /// Scoped to this coordinator; an idle worker waits for an event.
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inject_worker_panic(&self, stage: usize) -> Result<()> {
        self.inner.flow.inject_worker_panic(stage)
    }

    /// Closes admission across all clones, drains accepted requests, then joins.
    /// This blocks; async callers should invoke it through spawn_blocking.
    pub fn shutdown(&self) -> Result<()> {
        self.inner.shutdown()
    }
    pub fn stats(&self) -> IngestStats {
        lock(&self.inner.stats).clone()
    }
    /// Snapshot of diagnostic history, not a durable log. Old records may be evicted.
    pub fn traces(&self) -> IngestTraceSnapshot {
        lock(&self.inner.traces).snapshot()
    }
}

#[cfg(test)]
#[path = "ingest_flow_tests.rs"]
mod tests;
