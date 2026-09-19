//! Matched before/after diagnostic, not a network database throughput claim.
//! Args: rows batch producers in_flight_per_producer [mixed|write].
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use varve::{Config, Database, IngestConfig, Ingestor, Row, TableConfig, WriteRequest};

const BASE: i64 = 1_700_000_000_000_000;

fn request(id: usize, batch: usize, total: usize) -> WriteRequest {
    let start = id * batch;
    WriteRequest {
        table: "metrics".into(),
        request_id: format!("batch-{id}"),
        now_us: BASE + total as i64,
        rows: (start..start + batch)
            .map(|row| Row {
                timestamp_us: BASE + row as i64,
                tenant: "bench".into(),
                series: format!("s{}", row % 64),
                value: 1.0,
                tags: Default::default(),
            })
            .collect(),
    }
}
fn stats(mut values: Vec<f64>) -> Value {
    values.sort_by(f64::total_cmp);
    let quantile = |fraction: f64| {
        values
            .get(((values.len() as f64 * fraction).ceil() as usize).saturating_sub(1))
            .copied()
    };
    json!({"samples":values.len(),"p50_ms":quantile(0.5),"p95_ms":quantile(0.95),"p99_ms":quantile(0.99),"raw_ms":values})
}
fn verify(db: &Database, total: usize, batch: usize) -> Result<()> {
    let mut identities = BTreeSet::new();
    let mut count = 0;
    for start in (0..total).step_by(10_000) {
        let end = (start + 10_000).min(total);
        let rows = db.scan(
            "metrics",
            Some(BASE + start as i64),
            Some(BASE + end as i64),
            None,
            None,
        )?;
        count += rows.len();
        ensure!(
            rows.iter().all(|row| row.row.value == 1.0),
            "value mismatch"
        );
        identities.extend(rows.iter().map(|row| row.row.timestamp_us));
    }
    ensure!(count == total, "raw row count mismatch");
    ensure!(
        identities.len() == total
            && identities.first() == Some(&BASE)
            && identities.last() == Some(&(BASE + total as i64 - 1)),
        "timestamp identity mismatch"
    );
    let rollups = db.rollups("metrics")?;
    ensure!(
        rollups.iter().map(|row| row.count).sum::<u64>() == total as u64,
        "rollup count mismatch"
    );
    ensure!(
        rollups.iter().map(|row| row.sum).sum::<f64>() == total as f64,
        "rollup sum mismatch"
    );
    ensure!(
        db.status()?.idempotency_keys == total / batch,
        "receipt count mismatch"
    );
    let retry = request(0, batch, total);
    ensure!(
        db.write(&retry.table, &retry.request_id, retry.rows, retry.now_us)?
            .duplicate,
        "duplicate replay failed"
    );
    Ok(())
}
fn query(db: &Database, sql: &str, expected: Option<usize>) -> Result<f64> {
    let start = Instant::now();
    let result = db.query(sql)?;
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    let n = result[0]["n"].as_u64().context("missing query count")?;
    let sum = result[0]["total"].as_f64().unwrap_or(0.0);
    ensure!(n as f64 == sum, "incoherent raw count/sum");
    if let Some(expected) = expected {
        ensure!(n == expected as u64, "query row oracle mismatch");
    }
    Ok(elapsed)
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let number = |index, fallback| -> Result<usize> {
        Ok(args
            .get(index)
            .map(|value: &String| value.parse())
            .transpose()?
            .unwrap_or(fallback))
    };
    let total = number(1, 100_000)?;
    let batch = number(2, 1000)?;
    let producers = number(3, 4)?;
    let inflight = number(4, 4)?;
    let mixed = args.get(5).is_none_or(|mode| mode == "mixed");
    ensure!(
        batch > 0
            && batch <= 10_000
            && producers > 0
            && producers <= 16
            && inflight > 0
            && inflight <= 16
            && total > 0
            && total <= 1_000_000
            && total % (batch * producers) == 0,
        "invalid bounded profile"
    );
    let root = tempfile::TempDir::new()?;
    let config = Config {
        query_executable: PathBuf::from(
            std::env::var_os("VARVE_DUCKDB").context("set VARVE_DUCKDB to real executable")?,
        ),
        query_retained_inputs: true,
        checkpoint_frozen_prefix: true,
        derived_pages: true,
        query_memory_mb: 256,
        decoded_cache_bytes: 32 * 1024 * 1024,
        hot_max_bytes: 32 * 1024 * 1024,
        max_batch_rows: 50_000,
        max_batch_bytes: 16 * 1024 * 1024,
        ..Config::default()
    };
    let db = Database::open(root.path(), config.clone())?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![1_000_000],
            ..TableConfig::default()
        },
    )?;
    let ingest_config = IngestConfig {
        max_pending_bytes: 64 * 1024 * 1024,
        max_group_rows: 50_000,
        max_group_bytes: 16 * 1024 * 1024,
        ..IngestConfig::default()
    };
    let ingest = Ingestor::new(db.clone(), ingest_config.clone())?;
    let done = Arc::new(AtomicBool::new(false));
    let reader = if mixed {
        let done = done.clone();
        let db = db.clone();
        Some(thread::spawn(move || -> Result<Vec<f64>> {
            let mut samples = Vec::new();
            while !done.load(Ordering::Acquire) {
                samples.push(query(
                    &db,
                    "SELECT count(*) AS n, sum(value) AS total FROM metrics",
                    None,
                )?);
                thread::sleep(Duration::from_millis(10));
            }
            Ok(samples)
        }))
    } else {
        None
    };
    let gate = Arc::new(Barrier::new(producers));
    let start = Instant::now();
    let workers: Vec<_> = (0..producers)
        .map(|producer| {
            let ingest = ingest.clone();
            let gate = gate.clone();
            thread::spawn(move || -> Result<Vec<f64>> {
                gate.wait();
                let requests = total / batch / producers;
                let mut timings = Vec::new();
                for offset in (0..requests).step_by(inflight) {
                    let mut pending = Vec::new();
                    for local in offset..(offset + inflight).min(requests) {
                        let start = Instant::now();
                        let receive =
                            ingest.submit(request(producer * requests + local, batch, total))?;
                        pending.push((start, receive));
                    }
                    for (start, receive) in pending {
                        let receipt = receive.blocking_recv()??;
                        ensure!(
                            receipt.durability == "local_fsync" && !receipt.duplicate,
                            "not a fresh durable receipt"
                        );
                        timings.push(start.elapsed().as_secs_f64() * 1000.0);
                    }
                }
                Ok(timings)
            })
        })
        .collect();
    let mut writes = Vec::new();
    let mut failure = None;
    for worker in workers {
        match worker
            .join()
            .map_err(|_| anyhow::anyhow!("writer panic"))
            .and_then(|result| result)
        {
            Ok(samples) => writes.extend(samples),
            Err(error) => failure = Some(error),
        }
    }
    ingest.shutdown()?;
    let elapsed = start.elapsed().as_secs_f64();
    done.store(true, Ordering::Release);
    let reads = reader
        .map(|reader| {
            reader
                .join()
                .map_err(|_| anyhow::anyhow!("reader panic"))
                .and_then(|result| result)
        })
        .transpose()?
        .unwrap_or_default();
    if let Some(error) = failure {
        return Err(error);
    }
    ensure!(
        ingest.stats().pending_requests == 0
            && ingest.stats().pending_bytes == 0
            && ingest.stats().failed == 0
            && ingest.stats().rejected == 0,
        "backlog/failure at drain"
    );
    verify(&db, total, batch)?;
    let performance = serde_json::to_value(db.performance())?;
    let workers_before = serde_json::to_value(db.query_worker_stats())?;
    let mut shapes = Vec::new();
    if mixed {
        for phase in ["before_checkpoint", "after_checkpoint"] {
            if phase == "after_checkpoint" {
                db.checkpoint()?;
            }
            for _ in 0..3 {
                for (name, sql, expected) in [
                    (
                        "full",
                        "SELECT count(*) AS n, sum(value) AS total FROM metrics",
                        total,
                    ),
                    (
                        "series",
                        "SELECT count(*) AS n, sum(value) AS total FROM metrics WHERE tenant='bench' AND series='s0'",
                        total.div_ceil(64),
                    ),
                ] {
                    shapes.push(
                        json!({"phase":phase,"query":name,"ms":query(&db,sql,Some(expected))?}),
                    );
                }
            }
        }
    }
    let workers_after = serde_json::to_value(db.query_worker_stats())?;
    let status = serde_json::to_value(db.status()?)?;
    let ingest_stats = ingest.stats();
    drop(ingest);
    drop(db);
    let reopened = Database::open(root.path(), config.clone())?;
    verify(&reopened, total, batch)?;
    println!(
        "{}",
        json!({"passed":true,"rows":total,"batch_rows":batch,"producers":producers,"inflight_per_producer":inflight,"mixed":mixed,"config":config,"ingest_config":ingest_config,"elapsed_seconds_including_drain":elapsed,"durable_rows_per_second":total as f64/elapsed,"write_latency":stats(writes),"read_latency":stats(reads),"ingest":ingest_stats,"performance":performance,"workers_before_query_matrix":workers_before,"workers_after_query_matrix":workers_after,"query_matrix":shapes,"status":status,"reopen_oracles_passed":true,"scope":"local library diagnostic; no transport or Timescale performance claim"})
    );
    Ok(())
}
