//! Bounded, local-only Varve query probe. Not a Timescale benchmark or production certification.
use anyhow::{Result, ensure};
use clap::Parser;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;
use tempfile::TempDir;
use varve::{Config, Database, Row, TableConfig};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 8192)]
    rows: usize,
    #[arg(long, default_value_t = 7)]
    iterations: usize,
    #[arg(long, default_value = ".tools/duckdb")]
    duckdb: PathBuf,
}

fn measure(db: &Database, sql: &str, expected: &Value, iterations: usize) -> Result<Value> {
    // Warmup is excluded. Check every result, not only the warmup.
    ensure!(db.query(sql)? == *expected, "warmup result mismatch: {sql}");
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let result = db.query(sql)?;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        ensure!(result == *expected, "measured result mismatch: {sql}");
    }
    let mut sorted = samples.clone();
    sorted.sort_by(f64::total_cmp);
    Ok(json!({"sql":sql,"median_ms":sorted[sorted.len()/2],"samples_ms":samples}))
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(!cfg!(debug_assertions), "run this probe with --release");
    ensure!((64..=32768).contains(&args.rows), "rows must be 64..32768");
    ensure!(
        (3..=100).contains(&args.iterations),
        "iterations must be 3..100"
    );
    let executable = args.duckdb.canonicalize()?;
    let config = Config {
        query_executable: executable.clone(),
        metadata_max_bytes: 64 * 1024 * 1024,
        hot_max_rows: 40000,
        segment_rows: 8192,
        ..Default::default()
    };
    let temp = TempDir::new()?;
    let db = Database::open(temp.path(), config)?;
    db.create_table(
        "metrics",
        TableConfig {
            shards: 16,
            rollup_widths_us: vec![1, 64, 256],
            ..Default::default()
        },
    )?;
    db.create_continuous_aggregate("metrics_one", "metrics", 1)?;
    let mut count = 0_u64;
    let mut sum = 0.0;
    let mut total = 0.0;
    for offset in (0..args.rows).step_by(1024) {
        let mut batch = Vec::new();
        for i in offset..(offset + 1024).min(args.rows) {
            let value = (i % 1024) as f64 / 4.0;
            total += value;
            if i % 64 == 1 {
                count += 1;
                sum += value;
            }
            batch.push(Row {
                timestamp_us: i as i64,
                tenant: format!("tenant-{}", i % 4),
                series: format!("series-{}", i % 64),
                value,
                tags: BTreeMap::from([("region".into(), "north".into())]),
            });
        }
        db.write(
            "metrics",
            &format!("batch-{offset}"),
            batch,
            args.rows as i64,
        )?;
    }
    let selective = json!([{"n":count,"total":sum}]);
    let full = json!([{"n":args.rows,"total":total}]);
    let cases = [
        (
            "raw_selective",
            "SELECT count(*)::BIGINT AS n, sum(value) AS total FROM metrics WHERE tenant = 'tenant-1' AND series = 'series-1'",
            &selective,
        ),
        (
            "aggregate_selective",
            "SELECT sum(count)::BIGINT AS n, sum(sum) AS total FROM metrics_one WHERE tenant = 'tenant-1' AND series = 'series-1'",
            &selective,
        ),
        (
            "raw_full",
            "SELECT count(*)::BIGINT AS n, sum(value) AS total FROM metrics",
            &full,
        ),
    ];
    let mut results = Vec::new();
    for phase in ["hot", "parquet"] {
        if phase == "parquet" {
            db.checkpoint()?;
        }
        for (name, sql, expected) in cases {
            results.push(json!({"phase":phase,"case":name,"measurement":measure(&db, sql, expected, args.iterations)?}));
        }
    }
    let mut fingerprint = blake3::Hasher::new();
    for source in [
        include_str!("../src/engine.rs"),
        include_str!("../src/plan.rs"),
        include_str!("../src/query.rs"),
        include_str!("../Cargo.lock"),
        include_str!("query_readiness_probe.rs"),
    ] {
        fingerprint.update(source.as_bytes());
    }
    let version = varve::query::version(&varve::query::QueryOptions {
        executable,
        ..Default::default()
    })?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "scope":"Varve-only local query probe; not a Timescale comparison",
            "source_blake3":fingerprint.finalize().to_hex().to_string(),
            "os":std::env::consts::OS,"arch":std::env::consts::ARCH,
            "profile":"release","duckdb":version,"rows":args.rows,"iterations":args.iterations,
            "shards":16,"query_threads":2,"query_memory_mb":128,
            "rollup_widths_us":[1,64,256],"warmup_per_case":1,
            "durability":"local_fsync; no remote backend; ingestion excluded from timing",
            "cold_definition":"local Parquet with warm OS cache, not cold disk or S3",
            "results":results
        }))?
    );
    Ok(())
}
