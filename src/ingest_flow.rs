//! Admission ownership and terminal completion bookkeeping for the real Flow.
use super::{Admission, IngestConfig, IngestFlush, IngestStats, Pending, lock};
use crate::WriteReceipt;
use crate::engine::{AdmittedWrite, PreparedEpoch, PreparedWrite};
use crate::flow::{self, Control, FlowError, Producer, Stage};
use anyhow::{Context, Result, anyhow, ensure};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;
use tokio::sync::{Notify, oneshot};

pub(super) enum Payload {
    Input(AdmittedWrite),
    Validated(Result<PreparedWrite>),
    Epoch(Box<PreparedGroup>),
    Member,
    Written(Result<WriteReceipt>),
    Barrier(Option<Result<u64>>),
    Empty,
}

pub(super) struct PreparedGroup {
    pub epoch: PreparedEpoch,
    pub count: usize,
    pub rows: usize,
    pub started: Instant,
    pub oldest: Instant,
    pub newest: Instant,
    pub phases: Vec<crate::metrics::PhaseTrace>,
}

pub(super) struct Event {
    pub bytes: usize,
    pub rows: usize,
    pub admitted: Instant,
    pub write: bool,
    pub payload: Mutex<Payload>,
}
impl Event {
    pub fn take(&self) -> Payload {
        std::mem::replace(&mut *lock(&self.payload), Payload::Empty)
    }
}

pub(super) enum Completion {
    Write(Pending),
    Flush(oneshot::Sender<Result<IngestFlush>>),
}
pub(super) struct Entry {
    pub event: Arc<Event>,
    pub completion: Completion,
}
struct Book {
    // The registry owns terminal senders independently of retained Flow slots.
    // Its size is bounded by the same slot limit, not a fallback event queue.
    entries: BTreeMap<u64, Entry>,
    validated: u64,
    published: u64,
    claimed: u64,
    closed: bool,
    failure: Option<String>,
}

type EventStages = Vec<Stage<Arc<Event>>>;

pub(super) struct Runtime {
    producer: Producer<Arc<Event>>,
    pub control: Control<Arc<Event>>,
    book: Mutex<Book>,
    changed: Condvar,
    stats: Arc<Mutex<IngestStats>>,
    capacity: Arc<Notify>,
    max_pending_bytes: usize,
    workers_left: AtomicUsize,
    #[cfg(any(test, feature = "fault-injection"))]
    panic_stage: AtomicUsize,
}
impl Runtime {
    pub fn new(
        config: &IngestConfig,
        stats: Arc<Mutex<IngestStats>>,
        capacity: Arc<Notify>,
    ) -> Result<(Arc<Self>, EventStages)> {
        // Static validation -> owned epoch -> durability + visibility -> completion.
        // These are contiguous RING SLOT frontiers, never physical WAL maxima.
        let (producer, stages, control) = flow::bounded(
            config.queue_capacity,
            config.max_pending_bytes,
            &[vec![], vec![0], vec![1], vec![2]],
        )?;
        Ok((
            Arc::new(Self {
                producer,
                control,
                book: Mutex::new(Book {
                    entries: BTreeMap::new(),
                    validated: 0,
                    published: 0,
                    claimed: 0,
                    closed: false,
                    failure: None,
                }),
                changed: Condvar::new(),
                stats,
                capacity,
                max_pending_bytes: config.max_pending_bytes,
                workers_left: AtomicUsize::new(4),
                #[cfg(any(test, feature = "fault-injection"))]
                panic_stage: AtomicUsize::new(usize::MAX),
            }),
            stages,
        ))
    }

    pub fn check_open(&self) -> Result<()> {
        ensure!(!lock(&self.book).closed, "ingestion is closed");
        Ok(())
    }

    pub fn enqueue(&self, input: &mut Option<AdmittedWrite>) -> Result<Admission> {
        let mut book = lock(&self.book);
        ensure!(!book.closed, "ingestion is closed");
        let request = input.as_ref().expect("caller owns unadmitted input");
        let bytes = request.bytes();
        let rows = request.rows();
        if lock(&self.stats).pending_bytes.saturating_add(bytes) > self.max_pending_bytes {
            return Ok(Admission::Full("ingestion pending byte budget exhausted"));
        }
        let claim = match self.producer.try_claim(bytes) {
            Ok(claim) => claim,
            Err(FlowError::Full) => return Ok(Admission::Full("ingestion queue is full")),
            Err(error) => return Err(error).context("ingestion worker is unavailable"),
        };
        let (completion, receiver) = oneshot::channel();
        let event = Arc::new(Event {
            bytes,
            rows,
            admitted: Instant::now(),
            write: true,
            payload: Mutex::new(Payload::Input(input.take().unwrap())),
        });
        {
            let mut stats = lock(&self.stats);
            stats.submitted = stats.submitted.saturating_add(1);
            stats.pending_requests += 1;
            stats.pending_bytes += bytes;
            stats.peak_pending_requests = stats.peak_pending_requests.max(stats.pending_requests);
            stats.peak_pending_bytes = stats.peak_pending_bytes.max(stats.pending_bytes);
        }
        book.claimed = claim.sequence();
        book.entries.insert(
            claim.sequence(),
            Entry {
                event: event.clone(),
                completion: Completion::Write(Pending {
                    completion: Some(completion),
                    bytes,
                    stats: self.stats.clone(),
                    capacity: self.capacity.clone(),
                }),
            },
        );
        // Claim, registry insertion and publication share the admission gate.
        // Closing admission cannot discard an outstanding unpublished claim.
        claim.publish(event);
        Ok(Admission::Accepted(receiver))
    }

    pub fn flush(&self) -> Result<oneshot::Receiver<Result<IngestFlush>>> {
        let mut book = lock(&self.book);
        ensure!(!book.closed, "ingestion is closed");
        let claim = self.producer.try_claim(0).map_err(|error| match error {
            FlowError::Full => anyhow!("ingestion queue is full"),
            other => anyhow!("ingestion worker is unavailable: {other}"),
        })?;
        let (completion, receiver) = oneshot::channel();
        let event = Arc::new(Event {
            bytes: 0,
            rows: 0,
            admitted: Instant::now(),
            write: false,
            payload: Mutex::new(Payload::Barrier(None)),
        });
        book.claimed = claim.sequence();
        book.entries.insert(
            claim.sequence(),
            Entry {
                event: event.clone(),
                completion: Completion::Flush(completion),
            },
        );
        claim.publish(event);
        Ok(receiver)
    }

    pub fn close(&self) {
        let mut book = lock(&self.book);
        book.closed = true;
        self.control.close();
        lock(&self.stats).closed = true;
        drop(book);
        self.changed.notify_all();
        self.capacity.notify_waiters();
    }

    pub fn fence(&self, message: &str) {
        let mut book = lock(&self.book);
        book.closed = true;
        if book.failure.is_none() {
            book.failure = Some(message.chars().take(256).collect());
        }
        self.control.fence(message);
        lock(&self.stats).closed = true;
        drop(book);
        self.changed.notify_all();
        self.capacity.notify_waiters();
    }

    pub fn check_failure(&self) -> Result<()> {
        if let Some(message) = &lock(&self.book).failure {
            return Err(anyhow!("ingestion worker failed: {message}"));
        }
        ensure!(!self.control.stats().fenced, "ingestion flow fenced");
        Ok(())
    }

    pub fn validated(&self, sequence: u64) {
        lock(&self.book).validated = sequence;
        self.changed.notify_all();
    }

    pub fn published(&self, sequence: u64) {
        lock(&self.book).published = sequence;
        self.changed.notify_all();
    }

    pub fn await_publication(&self, sequence: u64) -> Result<()> {
        let mut book = lock(&self.book);
        while book.published < sequence {
            if let Some(failure) = &book.failure {
                return Err(anyhow!("ingestion flow fenced: {failure}"));
            }
            book = self.changed.wait(book).unwrap_or_else(|p| p.into_inner());
        }
        Ok(())
    }

    pub fn group_len(&self, first: u64, config: &IngestConfig) -> Result<usize> {
        let mut book = lock(&self.book);
        let event = &book
            .entries
            .get(&first)
            .context("missing ingestion entry")?
            .event;
        if !event.write {
            return Ok(1);
        }
        let deadline = event.admitted + config.max_delay;
        loop {
            self.maybe_panic(1);
            if let Some(failure) = &book.failure {
                return Err(anyhow!("ingestion flow fenced: {failure}"));
            }
            let mut count = 0;
            let mut rows = 0usize;
            let mut bytes = 128usize;
            for (sequence, entry) in book.entries.range(first..) {
                if *sequence > book.validated {
                    break;
                }
                let event = &entry.event;
                if !event.write
                    || rows.saturating_add(event.rows) > config.max_group_rows
                    || bytes.saturating_add(event.bytes) > config.max_group_bytes
                {
                    return Ok(count.max(1));
                }
                count += 1;
                rows += event.rows;
                bytes += event.bytes;
                if count == config.max_group_requests
                    || rows == config.max_group_rows
                    || bytes == config.max_group_bytes
                {
                    return Ok(count);
                }
            }
            // An expired deadline forbids waiting, not coalescing ready backlog.
            // On graceful close, preparation still drains every admitted claim.
            if Instant::now() >= deadline || (book.closed && book.validated == book.claimed) {
                return Ok(count.max(1));
            }
            let (next, _) = self
                .changed
                .wait_timeout(book, deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|poison| poison.into_inner());
            book = next;
        }
    }

    pub fn complete(&self, sequence: u64, result: Payload, release: impl FnOnce()) -> Result<()> {
        let mut book = lock(&self.book);
        let entry = book
            .entries
            .get(&sequence)
            .context("missing ingestion completion")?;
        ensure!(
            matches!(
                (&entry.completion, &result),
                (Completion::Write(_), Payload::Written(_))
                    | (Completion::Flush(_), Payload::Barrier(Some(_)))
            ),
            "invalid ingestion completion state"
        );
        let entry = book.entries.remove(&sequence).unwrap();
        // No row owner remains in the cell or in workers after stage 2. Retain
        // the admission gate across slot release and stats credit transfer, so
        // racing producers cannot inflate peak pending counters in between.
        release();
        match (entry.completion, result) {
            (Completion::Write(mut pending), Payload::Written(result)) => pending.finish(result),
            (Completion::Flush(completion), Payload::Barrier(Some(result))) => {
                let result = result.map(|sequence| {
                    let stats = lock(&self.stats);
                    IngestFlush {
                        sequence,
                        completed: stats.completed,
                        succeeded: stats.succeeded,
                        failed: stats.failed,
                    }
                });
                let _ = completion.send(result);
            }
            _ => unreachable!("checked terminal ingestion state"),
        }
        drop(book);
        self.capacity.notify_waiters();
        Ok(())
    }

    pub fn worker_finished(&self, result: Result<()>) {
        if let Err(error) = result {
            self.fence(&format!("{error:#}"));
        } else if self.control.stats().fenced {
            self.fence("required ingestion stage stopped");
        }
        if self.workers_left.fetch_sub(1, Ordering::AcqRel) == 1 && self.control.stats().fenced {
            self.fail_pending();
        }
    }

    pub fn fail_pending(&self) {
        // Called only after all workers have stopped (also on spawn failure).
        // First destroy private row ownership; only then return logical credit
        // and notify senders. Committed rows are separately charged by engine.
        let entries = std::mem::take(&mut lock(&self.book).entries);
        for entry in entries.values() {
            drop(entry.event.take());
        }
        for (_, entry) in entries {
            let error =
                anyhow!("ingestion worker stopped before completion; retry the same request ID");
            match entry.completion {
                Completion::Write(mut pending) => pending.finish(Err(error)),
                Completion::Flush(completion) => {
                    let _ = completion.send(Err(error));
                }
            }
        }
        self.capacity.notify_waiters();
    }

    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_worker_panic(&self, stage: usize) -> Result<()> {
        ensure!(stage < 4, "ingestion stage must be 0, 1, 2 or 3");
        self.panic_stage.store(stage, Ordering::Release);
        self.changed.notify_all();
        Ok(())
    }

    pub fn maybe_panic(&self, stage: usize) {
        #[cfg(any(test, feature = "fault-injection"))]
        assert_ne!(
            self.panic_stage.load(Ordering::Acquire),
            stage,
            "injected ingestion worker panic"
        );
        #[cfg(not(any(test, feature = "fault-injection")))]
        let _ = stage;
    }
}
