//! A small, self-checking mixed-workload probe, not a competitive performance benchmark.
use anyhow::{Result, ensure};
use std::{collections::BTreeMap, sync::Arc, time::Instant};
use varve::remote::FileStore;
use varve::{Config, Database, Row, TableConfig};

fn main() -> Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "10000".into())
        .parse()?;
    ensure!(
        (1..=20000).contains(&count),
        "probe is limited to 1..20000 rows"
    );
    let temp = tempfile::tempdir()?;
    let remote = Arc::new(FileStore::new(temp.path().join("objects"))?);
    let config = Config {
        flush_interval_us: 1,
        ship_interval_us: 1,
        compact_min_segments: 2,
        ..Default::default()
    };
    let db =
        Database::open_with_remote(temp.path().join("database"), config, Some(remote.clone()))?;
    db.create_table(
        "metrics",
        TableConfig {
            shards: 4,
            archive_after_us: Some(1),
            rollup_widths_us: vec![1_000_000],
            ..Default::default()
        },
    )?;
    let rows: Vec<_> = (0..count)
        .map(|n| Row {
            timestamp_us: n as i64 * 1000,
            tenant: "probe".into(),
            series: format!("sensor-{}", n % 8),
            value: 1.0,
            tags: BTreeMap::new(),
        })
        .collect();
    let started = Instant::now();
    for (batch, rows) in rows.chunks(256).enumerate() {
        db.write(
            "metrics",
            &format!("request-{batch}"),
            rows.to_vec(),
            count as i64 * 1000,
        )?;
    }
    let ingest_us = started.elapsed().as_micros();
    let started = Instant::now();
    let hot = db.query("SELECT count(*) AS n, sum(value) AS total FROM metrics")?;
    ensure!(
        hot[0]["n"].as_u64() == Some(count as u64),
        "hot SQL count mismatch"
    );
    let hot_query_us = started.elapsed().as_micros();
    let started = Instant::now();
    db.checkpoint()?;
    let checkpoint_us = started.elapsed().as_micros();
    let started = Instant::now();
    let report = db.maintain(100_000_000_000)?;
    let archive_us = started.elapsed().as_micros();
    ensure!(
        report.evicted_files > 0,
        "archive did not evict local segments"
    );
    let started = Instant::now();
    let cold = db.query("SELECT count(*) AS n, sum(value) AS total FROM metrics")?;
    let cold_query_us = started.elapsed().as_micros();
    ensure!(hot == cold, "hot/cold SQL mismatch");
    ensure!(
        db.rollups("metrics")?.iter().map(|r| r.count).sum::<u64>() == count as u64,
        "rollup mismatch"
    );
    let status = db.status()?;
    drop(db);
    let restored = Database::restore(temp.path().join("restored"), Config::default(), remote)?;
    ensure!(
        restored.query("SELECT count(*) AS n, sum(value) AS total FROM metrics")? == cold,
        "restore mismatch"
    );
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"rows":count,"batch_rows":256,"ingest_us":ingest_us,"hot_query_us":hot_query_us,"checkpoint_us":checkpoint_us,"archive_us":archive_us,"cold_query_us":cold_query_us,"local_disk_bytes":status.disk_bytes,"segments":status.segments,"verified":["local_fsync","hot_sql","checkpoint","archive","cold_sql","rollups","restore"],"note":"Unoptimized development build; includes DuckDB subprocess startup. Temp files cleaned on exit."})
        )?
    );
    Ok(())
}
