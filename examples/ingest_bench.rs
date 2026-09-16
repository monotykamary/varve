//! Self-checking bounded local probe: cargo run --example ingest_bench -- 512
use anyhow::{Result, ensure};
use std::{
    fs,
    path::Path,
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};
use varve::{Config, Database, IngestConfig, Ingestor, Row, TableConfig, WriteRequest};

fn request(id: usize) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: format!("r{id}"),
        now_us: 1000,
        rows: vec![Row {
            timestamp_us: id as i64,
            tenant: "bench".into(),
            series: "cpu".into(),
            value: 1.0,
            tags: Default::default(),
        }],
    }
}
fn open(path: &Path) -> Result<Database> {
    let db = Database::open(path, Config::default())?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![1_000_000],
            ..Default::default()
        },
    )?;
    Ok(db)
}
fn frames(path: &Path) -> Result<usize> {
    Ok(fs::read_dir(path.join("wal"))?
        .filter(|entry| {
            entry
                .as_ref()
                .is_ok_and(|e| e.path().extension().is_some_and(|s| s == "wal"))
        })
        .count())
}
fn verify(db: &Database, count: usize) -> Result<()> {
    ensure!(
        db.scan("metrics", None, None, None, None)?.len() == count,
        "missing raw rows"
    );
    let rollup = db.rollups("metrics")?.remove(0);
    ensure!(
        rollup.count == count as u64 && rollup.sum == count as f64,
        "incorrect aggregate"
    );
    ensure!(db.status()?.idempotency_keys == count, "missing receipts");
    let request = request(0);
    ensure!(
        db.write(
            &request.table,
            &request.request_id,
            request.rows,
            request.now_us
        )?
        .duplicate,
        "retry reinserted"
    );
    Ok(())
}
fn main() -> Result<()> {
    let count = std::env::args()
        .nth(1)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(512usize);
    ensure!(
        (32..=4096).contains(&count) && count % 4 == 0,
        "count must be 32..4096 and divisible by four"
    );
    let temp = tempfile::TempDir::new()?;
    let single_path = temp.path().join("single");
    let queued_path = temp.path().join("queued");
    let single = open(&single_path)?;
    let before = frames(&single_path)?;
    let started = Instant::now();
    for id in 0..count {
        let r = request(id);
        single.write(&r.table, &r.request_id, r.rows, r.now_us)?;
    }
    let single_elapsed = started.elapsed();
    let single_frames = frames(&single_path)? - before;
    verify(&single, count)?;
    drop(single);
    verify(&open(&single_path)?, count)?;

    let queued = open(&queued_path)?;
    let before = frames(&queued_path)?;
    let ingest = Ingestor::new(
        queued.clone(),
        IngestConfig {
            queue_capacity: count,
            max_delay: Duration::from_millis(5),
            ..Default::default()
        },
    )?;
    let barrier = Arc::new(Barrier::new(4));
    let started = Instant::now();
    let joins: Vec<_> = (0..4)
        .map(|producer| {
            let ingest = ingest.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || -> Result<()> {
                barrier.wait();
                // Bound outstanding acknowledgements per producer rather than accumulating
                // an arbitrarily large benchmark-owned receiver vector.
                for start in (0..count / 4).step_by(32) {
                    let mut receivers = Vec::new();
                    for index in start..(start + 32).min(count / 4) {
                        receivers.push(ingest.submit(request(producer * (count / 4) + index))?);
                    }
                    for receiver in receivers {
                        receiver.blocking_recv()??;
                    }
                }
                Ok(())
            })
        })
        .collect();
    for join in joins {
        join.join().expect("benchmark producer panicked")?;
    }
    ingest.shutdown()?;
    let queued_elapsed = started.elapsed();
    let queued_frames = frames(&queued_path)? - before;
    let stats = ingest.stats();
    ensure!(
        single_frames == count && queued_frames < count,
        "grouping did not reduce physical WAL publications"
    );
    ensure!(
        stats.completed == count as u64 && stats.failed == 0 && stats.pending_bytes == 0,
        "incomplete ingestion"
    );
    verify(&queued, count)?;
    drop(ingest);
    drop(queued);
    verify(&open(&queued_path)?, count)?;
    println!(
        "{}",
        serde_json::json!({"requests":count,"single_ms":single_elapsed.as_secs_f64()*1000.0,
        "queued_ms":queued_elapsed.as_secs_f64()*1000.0,"single_wal_frames":single_frames,
        "queued_wal_frames":queued_frames,"file_plus_directory_syncs_per_frame":2,
        "ingest":stats,"scope":"single-node local temp data; timings are not a throughput guarantee"})
    );
    Ok(())
}
