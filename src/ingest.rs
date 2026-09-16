//! Bounded multi-producer ingestion. Admission is not a durability acknowledgement.
use crate::{Database, WriteReceipt, WriteRequest};
use anyhow::{Context, Result, anyhow, ensure};
use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestConfig {
    pub queue_capacity: usize,
    pub max_pending_bytes: usize,
    pub max_group_requests: usize,
    pub max_group_rows: usize,
    pub max_group_bytes: usize,
    pub max_delay: Duration,
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
    request: Option<WriteRequest>,
    completion: Option<oneshot::Sender<Result<WriteReceipt>>>,
    bytes: usize,
    admitted: Instant,
    stats: Arc<Mutex<IngestStats>>,
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
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.finish(Err(anyhow!(
            "ingestion worker stopped before completion; retry the same request ID"
        )));
    }
}

struct Owner {
    admission: Mutex<Option<Sender<Pending>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    config: IngestConfig,
    stats: Arc<Mutex<IngestStats>>,
}
impl Owner {
    fn shutdown(&self) -> Result<()> {
        lock(&self.admission).take();
        lock(&self.stats).closed = true;
        // Keep this lock through join: concurrent shutdown callers also await drain.
        if let Some(worker) = lock(&self.worker).take() {
            worker
                .join()
                .map_err(|_| anyhow!("ingestion worker panicked"))?;
        }
        Ok(())
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.shutdown();
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
        let (sender, receiver) = bounded(config.queue_capacity);
        let stats = Arc::new(Mutex::new(IngestStats::default()));
        let worker_config = config.clone();
        let worker_stats = stats.clone();
        let worker = thread::Builder::new()
            .name("varve-ingest".into())
            .spawn(move || consume(db, receiver, worker_config, worker_stats))
            .context("start ingestion worker")?;
        Ok(Self {
            inner: Arc::new(Owner {
                admission: Mutex::new(Some(sender)),
                worker: Mutex::new(Some(worker)),
                config,
                stats,
            }),
        })
    }

    /// A returned receiver means admitted, not committed. Err guarantees not enqueued.
    pub fn submit(&self, request: WriteRequest) -> Result<oneshot::Receiver<Result<WriteReceipt>>> {
        let admission = lock(&self.inner.admission);
        let attempt = (|| {
            let sender = admission.as_ref().context("ingestion is closed")?;
            let bytes = request.admission_bytes()?;
            ensure!(
                request.rows.len() <= self.inner.config.max_group_rows
                    && bytes.saturating_add(128) <= self.inner.config.max_group_bytes,
                "request exceeds ingestion group capacity"
            );
            let mut stats = lock(&self.inner.stats);
            ensure!(
                stats.pending_bytes.saturating_add(bytes) <= self.inner.config.max_pending_bytes,
                "ingestion pending byte budget exhausted"
            );
            let (completion, receiver) = oneshot::channel();
            let pending = Pending {
                request: Some(request),
                completion: Some(completion),
                bytes,
                admitted: Instant::now(),
                stats: self.inner.stats.clone(),
            };
            // Increment before handing ownership to the consumer, but never drop a
            // rejected Pending with its completion armed under the stats mutex.
            stats.pending_requests += 1;
            stats.pending_bytes += bytes;
            match sender.try_send(pending) {
                Ok(()) => {
                    stats.submitted = stats.submitted.saturating_add(1);
                    stats.peak_pending_requests =
                        stats.peak_pending_requests.max(stats.pending_requests);
                    stats.peak_pending_bytes = stats.peak_pending_bytes.max(stats.pending_bytes);
                    Ok(receiver)
                }
                Err(error) => {
                    stats.pending_requests -= 1;
                    stats.pending_bytes -= bytes;
                    let (mut pending, message) = match error {
                        TrySendError::Full(pending) => (pending, "ingestion queue is full"),
                        TrySendError::Disconnected(pending) => {
                            (pending, "ingestion worker is unavailable")
                        }
                    };
                    pending.completion.take();
                    Err(anyhow!(message))
                }
            }
        })();
        if attempt.is_err() {
            let mut stats = lock(&self.inner.stats);
            stats.rejected = stats.rejected.saturating_add(1);
        }
        attempt
    }

    /// Closes admission across all clones, drains accepted requests, then joins.
    /// This blocks; async callers should invoke it through spawn_blocking.
    pub fn shutdown(&self) -> Result<()> {
        self.inner.shutdown()
    }
    pub fn stats(&self) -> IngestStats {
        lock(&self.inner.stats).clone()
    }
}

fn consume(
    db: Database,
    receiver: Receiver<Pending>,
    config: IngestConfig,
    stats: Arc<Mutex<IngestStats>>,
) {
    let mut carry = None;
    loop {
        let first = match carry.take().or_else(|| receiver.recv().ok()) {
            Some(first) => first,
            None => break,
        };
        let deadline = first.admitted + config.max_delay;
        let mut bytes = 128 + first.bytes;
        let mut rows = first.request.as_ref().unwrap().rows.len();
        let mut group = vec![first];
        while group.len() < config.max_group_requests
            && rows < config.max_group_rows
            && bytes < config.max_group_bytes
        {
            // An expired deadline forbids waiting, not coalescing an already
            // queued backlog. Otherwise a slow fsync degenerates into one fsync
            // per stale queued request precisely when batching is most needed.
            let wait = deadline.saturating_duration_since(Instant::now());
            let Ok(next) = receiver.recv_timeout(wait) else {
                break;
            };
            let next_rows = next.request.as_ref().unwrap().rows.len();
            if bytes.saturating_add(next.bytes) > config.max_group_bytes
                || rows.saturating_add(next_rows) > config.max_group_rows
            {
                carry = Some(next);
                break;
            }
            rows += next_rows;
            bytes += next.bytes;
            group.push(next);
        }
        let requests = group
            .iter_mut()
            .map(|pending| pending.request.take().unwrap())
            .collect();
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.write_group(requests)));
        {
            let mut stats = lock(&stats);
            stats.groups = stats.groups.saturating_add(1);
        }
        match outcome {
            Ok(results) if results.len() == group.len() => {
                for (pending, result) in group.iter_mut().zip(results) {
                    pending.finish(result);
                }
            }
            _ => {
                for pending in &mut group {
                    pending.finish(Err(anyhow!(
                        "ingestion group failed unexpectedly; retry the same request ID"
                    )));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Row, TableConfig};

    #[test]
    fn expired_deadline_still_coalesces_ready_backlog() {
        let temp = tempfile::TempDir::new().unwrap();
        let db = Database::open(temp.path(), Config::default()).unwrap();
        db.create_table("metrics", TableConfig::default()).unwrap();
        let (sender, receiver) = bounded(4);
        let stats = Arc::new(Mutex::new(IngestStats::default()));
        let mut completions = Vec::new();
        for id in 0..4 {
            let request = WriteRequest {
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
            };
            let bytes = request.admission_bytes().unwrap();
            let (completion, receive) = oneshot::channel();
            completions.push(receive);
            lock(&stats).pending_requests += 1;
            lock(&stats).pending_bytes += bytes;
            sender
                .send(Pending {
                    request: Some(request),
                    completion: Some(completion),
                    bytes,
                    admitted: Instant::now(),
                    stats: stats.clone(),
                })
                .unwrap();
        }
        drop(sender);
        consume(
            db.clone(),
            receiver,
            IngestConfig {
                max_delay: Duration::ZERO,
                max_group_requests: 4,
                ..Default::default()
            },
            stats.clone(),
        );
        let receipts: Vec<_> = completions
            .into_iter()
            .map(|c| c.blocking_recv().unwrap().unwrap())
            .collect();
        assert!(receipts.iter().all(|r| r.sequence == receipts[0].sequence));
        assert_eq!(lock(&stats).groups, 1);
        assert_eq!(lock(&stats).pending_bytes, 0);
        assert_eq!(db.status().unwrap().hot_rows, 4);
    }
}
