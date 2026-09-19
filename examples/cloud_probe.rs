use anyhow::{Context, Result, ensure};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;
use varve::remote::{FileStore, RemoteStore, S3Store};
use varve::{Config, Database, Row, TableConfig};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let local = args.iter().any(|arg| arg == "--local");
    let native_library = args
        .iter()
        .find_map(|arg| arg.strip_prefix("--rebuilt-library="))
        .map(std::path::PathBuf::from);
    let rebuilt = native_library.is_some();
    let rows: usize = args
        .iter()
        .find_map(|arg| arg.strip_prefix("--rows="))
        .unwrap_or("4000")
        .parse()?;
    ensure!((1..=50_000).contains(&rows), "rows must be 1..50000");
    ensure!(
        args.iter().all(|arg| arg == "--local"
            || arg.starts_with("--rows=")
            || arg.starts_with("--rebuilt-library=")),
        "usage: cloud_probe [--local] [--rows=N] [--rebuilt-library=/absolute/libduckdb.so]"
    );
    ensure!(
        local || std::env::var("VARVE_CLOUD_PROBE").as_deref() == Ok("true"),
        "explicit VARVE_CLOUD_PROBE=true is required for cloud requests"
    );
    let temporary = if std::path::Path::new("/data/probes").is_dir() {
        TempDir::new_in("/data/probes")?
    } else {
        TempDir::new()?
    };
    let namespace = format!("probe-{}", uuid::Uuid::new_v4());
    let (contract, store): (Arc<dyn RemoteStore>, Arc<dyn RemoteStore>) = if local {
        (
            Arc::new(FileStore::new(temporary.path().join("contract"))?),
            Arc::new(FileStore::new(temporary.path().join("objects"))?),
        )
    } else {
        let base = S3Store::from_env()?;
        (
            Arc::new(base.scoped(&format!("{namespace}/contract"))?),
            Arc::new(base.scoped(&format!("{namespace}/database"))?),
        )
    };
    ensure!(
        contract.head()?.is_none(),
        "contract namespace must be empty"
    );
    contract.put_immutable("objects/a", b"alpha")?;
    contract.put_immutable("objects/a", b"alpha")?;
    ensure!(
        contract.put_immutable("objects/a", b"conflict").is_err(),
        "immutable collision was overwritten"
    );
    ensure!(
        contract.get_bounded("objects/a", 4).is_err(),
        "read bound was ignored"
    );
    ensure!(
        contract.get_bounded("objects/a", 5)? == b"alpha",
        "object read mismatch"
    );
    let keys: Vec<String> = (0..7).map(|index| format!("pages/{index:04}")).collect();
    for key in &keys {
        contract.put_immutable(key, b"page")?;
    }
    let mut observed = Vec::new();
    let mut cursor = None;
    loop {
        let page = contract.list_page("pages", cursor.as_deref(), 3)?;
        ensure!(page.keys.len() <= 3, "page exceeded requested size");
        observed.extend(page.keys);
        match page.next {
            Some(next) => {
                ensure!(
                    cursor.as_ref().is_none_or(|prior| next > *prior),
                    "cursor did not advance"
                );
                cursor = Some(next);
            }
            None => break,
        }
        ensure!(observed.len() <= keys.len(), "pagination did not terminate");
    }
    ensure!(
        observed == keys,
        "paginated listing skipped or duplicated keys"
    );
    ensure!(
        contract.delete_batch(&keys)? == keys.len(),
        "bulk delete incomplete"
    );
    ensure!(
        contract.list_page("pages", None, 3)?.keys.is_empty(),
        "bulk delete left objects"
    );
    let first = contract.compare_and_swap_head(None, b"first")?;
    ensure!(
        contract.compare_and_swap_head(None, b"collision").is_err(),
        "conditional create overwritten"
    );
    let second = contract.compare_and_swap_head(Some(&first), b"second")?;
    ensure!(second != first, "changed head has stale token");
    ensure!(
        contract
            .compare_and_swap_head(Some(&first), b"stale")
            .is_err(),
        "stale update accepted"
    );
    let repeated = contract.compare_and_swap_head(Some(&second), b"second")?;
    ensure!(
        repeated != second,
        "identical head payload reused a token (ABA risk)"
    );
    ensure!(
        contract
            .compare_and_swap_head(Some(&second), b"stale-identical")
            .is_err(),
        "stale token accepted after identical-payload CAS"
    );
    ensure!(
        contract.head()?.context("missing contract head")?.bytes == b"second",
        "head mismatch"
    );
    contract.delete("objects/a")?;
    ensure!(contract.get("objects/a").is_err(), "delete failed");
    ensure!(
        contract.list("objects")?.is_empty(),
        "list includes deleted object"
    );
    let config = Config {
        hot_max_bytes: 16 * 1024 * 1024,
        metadata_max_bytes: 16 * 1024 * 1024,
        wal_max_bytes: 32 * 1024 * 1024,
        max_disk_bytes: 256 * 1024 * 1024,
        disk_cache_bytes: 16 * 1024 * 1024,
        query_memory_mb: 128,
        query_threads: 1,
        query_workers: 1,
        segment_rows: 1024,
        segmented_journal: rebuilt,
        checkpoint_frozen_prefix: rebuilt,
        derived_pages: rebuilt,
        duckdb_library: native_library,
        query_executable: if rebuilt {
            "/nonexistent/varve-cloud-probe-no-cli".into()
        } else {
            Config::default().query_executable
        },
        ..Default::default()
    };
    let local_root = temporary.path().join("source");
    let db = Database::open_with_remote(&local_root, config.clone(), Some(store.clone()))?;
    db.create_table(
        "metrics",
        TableConfig {
            shards: 2,
            window_us: 60_000_000,
            rollup_widths_us: vec![1_000_000],
            archive_after_us: Some(1),
            ..Default::default()
        },
    )?;
    db.query("CALL varve_create_continuous_aggregate('metrics_named', 'metrics', 1000000)")?;
    let start = Instant::now();
    let mut expected = 0.0;
    let mut first_request = None;
    for (batch_index, offset) in (0..rows).step_by(256).enumerate() {
        let batch: Vec<Row> = (offset..(offset + 256).min(rows))
            .map(|index| {
                let value = (index % 1000) as f64 / 8.0;
                expected += value;
                Row {
                    timestamp_us: ((rows - index) as i64) * 1000,
                    tenant: "probe".into(),
                    series: "cpu".into(),
                    value,
                    tags: BTreeMap::new(),
                }
            })
            .collect();
        let id = format!("batch-{batch_index}");
        let first = db.write("metrics", &id, batch.clone(), 1_000_000_000)?;
        if first_request.is_none() {
            first_request = Some((id.clone(), batch.clone(), first.sequence));
        }
        let retry = db.write("metrics", &id, batch, 1_000_000_000)?;
        ensure!(
            retry.duplicate && retry.sequence == first.sequence,
            "retry changed receipt"
        );
    }
    let ingest_us = start.elapsed().as_micros();
    let tail_sequence = db.ship()?;
    ensure!(
        db.status()?.checkpoint_sequence < tail_sequence,
        "tail proof was already checkpointed"
    );
    drop(db);
    let db = Database::restore(
        temporary.path().join("tail-restored"),
        config.clone(),
        store.clone(),
    )?;
    let tail = db.query("SELECT count(*) AS n, sum(value) AS total FROM metrics")?;
    ensure!(
        tail[0]["n"].as_u64() == Some(rows as u64) && tail[0]["total"].as_f64() == Some(expected),
        "uncheckpointed remote tail mismatch: {tail}"
    );
    let (id, batch, receipt_sequence) = first_request.context("missing retry fixture")?;
    let replay = db.write("metrics", &id, batch, 1_000_000_000)?;
    ensure!(
        replay.duplicate && replay.sequence == receipt_sequence,
        "post-restore retry changed receipt"
    );
    if rebuilt {
        let status = db.status()?;
        ensure!(
            status.segmented_journal && status.native_query.is_some(),
            "rebuilt backend was not selected"
        );
    }
    db.checkpoint()?;
    let sequence = db.ship()?;
    db.maintain(10_000_000_000)?;
    let raw = db.query("SELECT count(*) AS n, sum(value) AS total FROM metrics")?;
    ensure!(
        raw[0]["n"].as_u64() == Some(rows as u64) && raw[0]["total"].as_f64() == Some(expected),
        "raw mismatch: {raw}"
    );
    drop(db);
    // Restore transfers this isolated namespace's ownership only after its writer has stopped.
    let restored = Database::restore(temporary.path().join("restored"), config, store.clone())?;
    let after = restored.query("SELECT count(*) AS n, sum(value) AS total FROM metrics")?;
    ensure!(after == raw, "restore mismatch: {after}");
    let aggregate = restored
        .query("SELECT CAST(sum(count) AS BIGINT) AS n, sum(sum) AS total FROM metrics_named")?;
    ensure!(
        aggregate[0]["n"].as_u64() == Some(rows as u64)
            && aggregate[0]["total"].as_f64() == Some(expected),
        "aggregate mismatch: {aggregate}"
    );
    restored.query("CALL varve_set_policy('metrics', '{\"retention_us\":1}')")?;
    restored.maintain(20_000_000_000)?;
    let cleared = restored.query("SELECT count(*) AS n FROM metrics")?;
    ensure!(
        cleared[0]["n"].as_u64() == Some(0),
        "raw retention failed: {cleared}"
    );
    let retained = restored.query("SELECT CAST(sum(count) AS BIGINT) AS n FROM metrics_named")?;
    ensure!(
        retained[0]["n"].as_u64() == Some(rows as u64),
        "raw expiration destroyed independent rollup history"
    );
    restored.vacuum_remote()?;
    let status = restored.status()?;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"backend":if local{"filesystem"}else{"s3"},"rebuilt":rebuilt,"tail_sequence":tail_sequence,"isolated_namespace":namespace,"rows":rows,"sum":expected,"ingest_us":ingest_us,"published_sequence":sequence,"final_status":status,"verified":["immutable_collision","bounded_read","conditional_create","conditional_update","stale_cas_rejection","identical_payload_token_freshness","delete_list","paged_list","bulk_delete","local_fsync","idempotent_retry","uncheckpointed_remote_tail_restore","post_restore_retry","parquet","archive","sql","named_rollup","remote_restore","raw_retention","derived_history_retention","remote_vacuum"],"note":"Synthetic isolated prefixes only. Remote control/checkpoint objects are retained as evidence; local temporary data is removed on exit."})
        )?
    );
    Ok(())
}
