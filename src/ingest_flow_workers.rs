//! Four exclusive consumers of the production ingestion Flow.
use super::flow::{Event, Payload, PreparedGroup, Runtime};
use super::{IngestConfig, IngestStats, IngestTrace, TraceBuffer, lock};
use crate::Database;
use crate::flow::{FlowError, Stage};
use anyhow::{Context, Result, anyhow, bail, ensure};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub(super) fn start(
    db: Database,
    stages: Vec<Stage<Arc<Event>>>,
    runtime: Arc<Runtime>,
    config: IngestConfig,
    stats: Arc<Mutex<IngestStats>>,
    traces: Arc<Mutex<TraceBuffer>>,
) -> Result<Vec<JoinHandle<()>>> {
    let mut workers = Vec::with_capacity(4);
    for (index, stage) in stages.into_iter().enumerate() {
        let db = db.clone();
        let worker_runtime = runtime.clone();
        let config = config.clone();
        let stats = stats.clone();
        let traces = traces.clone();
        let worker = thread::Builder::new()
            .name(format!("varve-ingest-{index}"))
            .spawn(move || {
                // The stage is dropped inside the unwind boundary, fencing any
                // abandoned delivery. No panic may strand retained-ring senders.
                let result = catch_unwind(AssertUnwindSafe(|| match index {
                    0 => validate(db, stage, &worker_runtime),
                    1 => prepare(db, stage, &worker_runtime, &config),
                    2 => commit(db, stage, &worker_runtime, &config, &stats, &traces),
                    3 => complete(stage, &worker_runtime),
                    _ => unreachable!("registered ingestion stage"),
                }))
                .unwrap_or_else(|_| Err(anyhow!("ingestion stage {index} panicked")));
                worker_runtime.worker_finished(result);
            });
        match worker {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                runtime.fence("could not start required ingestion worker");
                for worker in workers {
                    let _ = worker.join();
                }
                runtime.fail_pending();
                return Err(error).context("start ingestion worker");
            }
        }
    }
    Ok(workers)
}

pub(super) fn validate(
    db: Database,
    mut stage: Stage<Arc<Event>>,
    runtime: &Runtime,
) -> Result<()> {
    loop {
        let delivery = match stage.next_until(Instant::now() + Duration::from_secs(60)) {
            Ok(Some(delivery)) => delivery,
            Ok(None) => return Ok(()),
            Err(FlowError::Deadline) => continue,
            Err(error) => return Err(error.into()),
        };
        runtime.maybe_panic(0);
        let sequence = delivery.sequence();
        if let Some(event) = delivery.value() {
            // Move input out before doing canonical digest/preparation. This can
            // overlap the preceding group's fsync; partition ownership is not
            // claimed here. Neither rows nor prepared inputs are cloned.
            let prepared = match event.take() {
                Payload::Input(input) => Payload::Validated(input.prepare(&db.inner.config)),
                Payload::Barrier(None) => Payload::Barrier(None),
                _ => bail!("invalid ingestion preparation state"),
            };
            *lock(&event.payload) = prepared;
        }
        delivery.finish();
        runtime.validated(sequence);
    }
}

pub(super) fn prepare(
    db: Database,
    mut stage: Stage<Arc<Event>>,
    runtime: &Runtime,
    config: &IngestConfig,
) -> Result<()> {
    loop {
        let delivery = match stage.next_until(Instant::now() + Duration::from_secs(60)) {
            Ok(Some(delivery)) => delivery,
            Ok(None) => return Ok(()),
            Err(FlowError::Deadline) => continue,
            Err(error) => return Err(error.into()),
        };
        runtime.maybe_panic(1);
        let Some(event) = delivery.value() else {
            delivery.finish();
            continue;
        };
        if !event.write {
            ensure!(
                matches!(event.take(), Payload::Barrier(None)),
                "invalid ingestion barrier state"
            );
            *lock(&event.payload) = Payload::Barrier(None);
            delivery.finish();
            continue;
        }
        let first = delivery.sequence();
        let count = runtime.group_len(first, config)?;
        let group = delivery.coalesce(count, |_| true)?;
        ensure!(group.len() == count, "prepared ingestion prefix missing");
        let mut requests = Vec::with_capacity(count);
        let mut rows = 0;
        let mut oldest = None;
        let mut newest = None;
        for event in group.values() {
            let event = event.context("unexpected ingestion cancellation")?;
            rows += event.rows;
            oldest.get_or_insert(event.admitted);
            newest = Some(event.admitted);
            let Payload::Validated(prepared) = event.take() else {
                bail!("invalid ingestion commit state");
            };
            requests.push(prepared);
        }
        let started = Instant::now();
        let run = || db.prepare_epoch(requests);
        let (epoch, phases) = if config.trace_capacity > 0 {
            db.inner.metrics.capture(run)
        } else {
            (run(), Vec::new())
        };
        let mut events = group.values();
        let head = events.next().unwrap().unwrap();
        *lock(&head.payload) = Payload::Epoch(Box::new(PreparedGroup {
            epoch,
            count,
            rows,
            started,
            oldest: oldest.unwrap(),
            newest: newest.unwrap(),
            phases,
        }));
        for member in events {
            *lock(&member.unwrap().payload) = Payload::Member;
        }
        group.finish();
        // Exclusivity still serializes preparation. Wait through a fence-aware
        // condition, not on the lease held in an abandoned ring head: otherwise
        // a commit-worker panic could deadlock final sender/credit cleanup.
        runtime.await_publication(first + count as u64 - 1)?;
    }
}

pub(super) fn commit(
    db: Database,
    mut stage: Stage<Arc<Event>>,
    runtime: &Runtime,
    config: &IngestConfig,
    stats: &Mutex<IngestStats>,
    traces: &Mutex<TraceBuffer>,
) -> Result<()> {
    loop {
        let delivery = match stage.next_until(Instant::now() + Duration::from_secs(60)) {
            Ok(Some(delivery)) => delivery,
            Ok(None) => return Ok(()),
            Err(FlowError::Deadline) => continue,
            Err(error) => return Err(error.into()),
        };
        runtime.maybe_panic(2);
        let first = delivery.sequence();
        let Some(event) = delivery.value() else {
            delivery.finish();
            runtime.published(first);
            continue;
        };
        if !event.write {
            ensure!(
                matches!(event.take(), Payload::Barrier(None)),
                "invalid ingestion barrier state"
            );
            *lock(&event.payload) = Payload::Barrier(Some(db.committed_sequence()));
            delivery.finish();
            runtime.published(first);
            continue;
        }
        let Payload::Epoch(prepared) = event.take() else {
            bail!("missing owned ingestion epoch");
        };
        let PreparedGroup {
            epoch,
            count,
            rows,
            started,
            oldest,
            newest,
            mut phases,
        } = *prepared;
        let group = delivery.coalesce(count, |_| true)?;
        ensure!(group.len() == count, "owned ingestion prefix missing");
        for member in group.values().skip(1) {
            ensure!(
                matches!(
                    member.context("missing epoch member")?.take(),
                    Payload::Member
                ),
                "invalid epoch member"
            );
        }
        let run = || epoch.publish();
        let (results, publication_phases) = if config.trace_capacity > 0 {
            db.inner.metrics.capture(run)
        } else {
            (run(), Vec::new())
        };
        phases.extend(publication_phases);
        let group_id = {
            let mut stats = lock(stats);
            stats.groups = stats.groups.saturating_add(1);
            stats.groups
        };
        if config.trace_capacity > 0 {
            let ns = |duration: Duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
            let mut sequences = Vec::new();
            let failed = results.iter().filter(|result| result.is_err()).count();
            let mut duplicate = 0;
            for receipt in results.iter().filter_map(|result| result.as_ref().ok()) {
                sequences.push(receipt.sequence);
                duplicate += usize::from(receipt.duplicate);
            }
            sequences.sort_unstable();
            sequences.dedup();
            lock(traces).push(IngestTrace {
                group: group_id,
                requests: count,
                rows,
                oldest_queue_ns: ns(started.duration_since(oldest)),
                newest_queue_ns: ns(started.duration_since(newest)),
                service_ns: ns(started.elapsed()),
                sequences,
                failed,
                duplicate,
                phases,
            });
        }
        ensure!(
            results.len() == count,
            "ingestion commit returned an incomplete result group"
        );
        for (event, result) in group.values().zip(results) {
            *lock(&event.unwrap().payload) = Payload::Written(result);
        }
        // No maximum receipt sequence is a frontier. All slots, including
        // rejects and old duplicates, become terminal only after atomic install.
        group.finish();
        runtime.published(first + count as u64 - 1);
    }
}

fn complete(mut stage: Stage<Arc<Event>>, runtime: &Runtime) -> Result<()> {
    loop {
        let delivery = match stage.next_until(Instant::now() + Duration::from_secs(60)) {
            Ok(Some(delivery)) => delivery,
            Ok(None) => return Ok(()),
            Err(FlowError::Deadline) => continue,
            Err(error) => return Err(error.into()),
        };
        runtime.maybe_panic(3);
        let Some(event) = delivery.value() else {
            delivery.finish();
            continue;
        };
        let result = event.take();
        let sequence = delivery.sequence();
        runtime.complete(sequence, result, || delivery.finish())?;
    }
}
