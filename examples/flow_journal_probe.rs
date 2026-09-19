//! Executable replacement-component proof, not a Varve server or throughput benchmark.
//! Run only in the isolated Railway qualification workspace for this campaign.
use anyhow::{Context, Result, ensure};
use crossbeam_channel::{Receiver, Sender, bounded};
use std::collections::VecDeque;
use std::sync::{Arc, OnceLock, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use varve::flow;
use varve::journal::{Journal, JournalConfig};

const ROWS: usize = 4096;
const PRODUCERS: usize = 4;
const WINDOW: usize = 16;
const SLOTS: usize = PRODUCERS * WINDOW;
const CHARGE: usize = 512;

struct Event {
    identity: u64,
    encoded: OnceLock<[u8; 8]>,
    durable: OnceLock<u64>,
    receipt: Sender<(u64, u64)>,
}
#[derive(Default)]
struct Snapshot {
    sequence: u64,
    count: u64,
    sum: u64,
}

fn main() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let journal_path = directory.path().join("journal");
    let config = JournalConfig {
        max_total_bytes: 256 * 1024,
        segment_bytes: 16 * 1024,
        max_segments: 32,
        max_frame_bytes: 2048,
        max_record_bytes: 8,
        max_records_per_group: SLOTS,
    };
    let mut journal = Journal::open(&journal_path, config)?;
    let before = journal.stats();
    let (producer, mut stages, control) =
        flow::bounded::<Event>(SLOTS, SLOTS * CHARGE, &[vec![], vec![0], vec![1]])?;
    let mut publish_stage = stages.pop().unwrap();
    let mut durable_stage = stages.pop().unwrap();
    let mut prepare_stage = stages.pop().unwrap();
    let snapshot = Arc::new(RwLock::new(Arc::new(Snapshot::default())));
    let deadline = Instant::now() + Duration::from_secs(30);
    let (prepared, start_journal) = bounded(1);
    let prepare = thread::spawn(move || -> Result<()> {
        while let Some(delivery) = prepare_stage.next_until(deadline)? {
            if let Some(event) = delivery.value() {
                event
                    .encoded
                    .set(event.identity.to_le_bytes())
                    .map_err(|_| anyhow::anyhow!("prepared twice"))?;
            }
            let sequence = delivery.sequence();
            delivery.finish();
            // Deterministically put enough single inserts behind the durability
            // owner to prove actual coalescing, not timing-dependent batching.
            if sequence == SLOTS as u64 / 2 {
                prepared.send(())?;
            }
        }
        Ok(())
    });
    let durable = thread::spawn(move || -> Result<_> {
        start_journal.recv_deadline(deadline)?;
        while let Some(delivery) = durable_stage.next_until(deadline)? {
            let batch = delivery.coalesce(SLOTS, |_| true)?;
            let records = batch
                .values()
                .filter_map(|event| {
                    event.map(|event| event.encoded.get().expect("prepare dependency").as_slice())
                })
                .collect::<Vec<_>>();
            if !records.is_empty() {
                let receipt = journal.append_group(&records)?;
                let mut sequence = receipt.first_sequence();
                for event in batch.values().flatten() {
                    event
                        .durable
                        .set(sequence)
                        .map_err(|_| anyhow::anyhow!("durable twice"))?;
                    sequence += 1;
                }
                ensure!(
                    sequence - 1 == receipt.last_sequence(),
                    "journal group frontier mismatch"
                );
            }
            // No slot's durability dependency advances before the group fsync.
            batch.finish();
        }
        Ok(journal.stats())
    });
    let visible = Arc::clone(&snapshot);
    let publish = thread::spawn(move || -> Result<()> {
        while let Some(delivery) = publish_stage.next_until(deadline)? {
            if let Some(event) = delivery.value() {
                let sequence = *event
                    .durable
                    .get()
                    .context("visibility outran durability")?;
                let old = Arc::clone(&visible.read().unwrap());
                ensure!(sequence == old.sequence + 1, "noncontiguous visibility");
                *visible.write().unwrap() = Arc::new(Snapshot {
                    sequence,
                    count: old.count + 1,
                    sum: old.sum + event.identity,
                });
                // A slow or disconnected receiver cannot block or cancel commit.
                let _ = event.receipt.try_send((event.identity, sequence));
            }
            delivery.finish();
        }
        Ok(())
    });
    let clients = (0..PRODUCERS)
        .map(|client| {
            let producer = producer.clone();
            let snapshot = Arc::clone(&snapshot);
            thread::spawn(move || -> Result<usize> {
                let mut pending: VecDeque<(u64, Receiver<(u64, u64)>)> = VecDeque::new();
                let mut receipts = 0;
                for offset in 0..ROWS / PRODUCERS {
                    let identity = (client * (ROWS / PRODUCERS) + offset) as u64;
                    let claim = producer.claim_until(CHARGE, deadline)?;
                    let (receipt, wait) = bounded(1);
                    claim.publish(Event {
                        identity,
                        encoded: OnceLock::new(),
                        durable: OnceLock::new(),
                        receipt,
                    });
                    pending.push_back((identity, wait));
                    if pending.len() == WINDOW {
                        verify_receipt(pending.pop_front().unwrap(), &snapshot, deadline)?;
                        receipts += 1;
                    }
                }
                for pending in pending {
                    verify_receipt(pending, &snapshot, deadline)?;
                    receipts += 1;
                }
                Ok(receipts)
            })
        })
        .collect::<Vec<_>>();
    let mut receipts = 0;
    for client in clients {
        receipts += client
            .join()
            .map_err(|_| anyhow::anyhow!("producer panicked"))??;
    }
    control.close();
    prepare
        .join()
        .map_err(|_| anyhow::anyhow!("preparer panicked"))??;
    let stats = durable
        .join()
        .map_err(|_| anyhow::anyhow!("journal owner panicked"))??;
    publish
        .join()
        .map_err(|_| anyhow::anyhow!("publisher panicked"))??;
    let final_snapshot = Arc::clone(&snapshot.read().unwrap());
    let expected_sum = (ROWS as u64 - 1) * ROWS as u64 / 2;
    ensure!(
        receipts == ROWS
            && final_snapshot.count == ROWS as u64
            && final_snapshot.sum == expected_sum,
        "receipt/visibility oracle failed"
    );
    let flow = control.stats();
    ensure!(
        flow.claimed == ROWS as u64
            && flow.reclaimed == ROWS as u64
            && flow.charged_bytes == 0
            && !flow.fenced,
        "flow conservation failed"
    );
    ensure!(
        stats.groups_appended < ROWS as u64,
        "single inserts were not coalesced"
    );
    ensure!(
        stats.namespace_barriers - before.namespace_barriers < stats.groups_appended,
        "namespace barrier per group remains"
    );
    ensure!(stats.segments > 1, "rotation was not exercised");
    let reopened = Journal::open(&journal_path, config)?;
    let mut recovered = Vec::with_capacity(ROWS);
    reopened.scan(|group| {
        for (sequence, bytes) in group.records() {
            ensure!(
                recovered.len() < ROWS && bytes.len() == 8,
                "unexpected recovered record"
            );
            ensure!(
                sequence == recovered.len() as u64 + 1,
                "recovered sequence gap"
            );
            recovered.push(u64::from_le_bytes(bytes.try_into()?));
        }
        Ok(())
    })?;
    recovered.sort_unstable();
    ensure!(
        recovered == (0..ROWS as u64).collect::<Vec<_>>(),
        "independent reopen identity oracle failed"
    );
    println!(
        "{}",
        serde_json::json!({
            "kind": "replacement_component_proof_not_database_benchmark",
            "single_event_producers": PRODUCERS, "window_per_producer": WINDOW,
            "offered": ROWS, "acknowledged_after_durable_visibility": receipts,
            "recovered_exact_identities": recovered.len(), "visible_sum": final_snapshot.sum,
            "journal_groups": stats.groups_appended, "file_syncs": stats.file_syncs - before.file_syncs,
            "namespace_barriers": stats.namespace_barriers - before.namespace_barriers,
            "segments": stats.segments, "journal_encoded_bytes": stats.disk_bytes,
            "flow_charged_bytes_after_drain": flow.charged_bytes, "flow_reclaimed": flow.reclaimed,
            "not_claimed": ["HTTP/server integration", "SQL", "row schema/rollups", "idempotency", "S3", "checkpoint reclamation", "power-loss qualification", "Timescale performance"]
        })
    );
    Ok(())
}

fn verify_receipt(
    (identity, wait): (u64, Receiver<(u64, u64)>),
    snapshot: &RwLock<Arc<Snapshot>>,
    deadline: Instant,
) -> Result<()> {
    let (actual, sequence) = wait.recv_deadline(deadline)?;
    ensure!(actual == identity, "wrong client receipt");
    ensure!(
        snapshot.read().unwrap().sequence >= sequence,
        "acknowledged before publication"
    );
    Ok(())
}
