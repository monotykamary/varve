use anyhow::Result;
use std::collections::BTreeMap;
use tempfile::TempDir;
use varve::{Config, Database, Row, TableConfig, WriteRequest};

#[test]
fn empty_and_small_scan_work_at_16mib_output_and_default_raw_pool() -> Result<()> {
    let dir = TempDir::new()?;
    let db = Database::open(
        dir.path(),
        Config {
            query_max_output_bytes: 16 * 1024 * 1024,
            ..Default::default()
        },
    )?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![],
            ..Default::default()
        },
    )?;
    assert!(db.scan("metrics", None, None, None, None)?.is_empty());
    db.write_group(vec![request("one", 1)]).pop().unwrap()?;
    assert_eq!(db.scan("metrics", None, None, None, None)?.len(), 1);
    db.checkpoint()?;
    assert_eq!(db.scan("metrics", None, None, None, None)?.len(), 1);
    Ok(())
}

fn request(id: &str, count: usize) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: id.into(),
        now_us: 1,
        rows: (0..count)
            .map(|_| Row {
                timestamp_us: 1,
                tenant: "tenant".into(),
                series: "series".into(),
                value: -0.0,
                tags: BTreeMap::from([("tag".into(), "payload".repeat(128))]),
            })
            .collect(),
    }
}

#[test]
fn admitted_burst_converts_below_former_double_copy_peak() -> Result<()> {
    let dir = TempDir::new()?;
    let requests: Vec<_> = (0..3).map(|i| request(&format!("id{i}"), 256)).collect();
    // Independently compute the documented transferable envelope from public types.
    let mut envelope = 0;
    for r in &requests {
        let logical: usize = r.rows.iter().map(Row::estimated_bytes).sum();
        let excess: usize = r
            .rows
            .iter()
            .flat_map(|row| {
                std::iter::once(&row.tenant)
                    .chain(std::iter::once(&row.series))
                    .chain(row.tags.iter().flat_map(|(k, v)| [k, v]))
            })
            .map(|s| s.capacity() - s.len())
            .sum();
        let frame =
            serde_json::to_vec(&r.rows)?.len() + 6 * (r.table.len() + r.request_id.len()) + 512;
        envelope += 8 * logical
            + 256
            + excess
            + r.rows.capacity() * std::mem::size_of::<Row>()
            + r.table.capacity()
            + r.request_id.capacity()
            + (3 * frame).max(4096)
            + 8192
            + 512;
    }
    let db = Database::open(
        dir.path(),
        Config {
            raw_memory_max_bytes: envelope,
            ..Default::default()
        },
    )?;
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![],
            ..Default::default()
        },
    )?;
    let results = db.write_group(requests);
    assert!(
        results.iter().all(Result::is_ok),
        "admitted burst must not need second-copy credit: {results:?}"
    );
    assert_eq!(
        results
            .iter()
            .map(|r| r.as_ref().unwrap().rows)
            .sum::<usize>(),
        768
    );
    Ok(())
}
