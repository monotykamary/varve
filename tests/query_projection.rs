use anyhow::Result;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tempfile::TempDir;
use varve::model::shard_for;
use varve::remote::{FileStore, HeadObject, RemoteStore};
use varve::{Config, Database, Row, TableConfig};

fn row(timestamp_us: i64, tenant: &str, series: &str, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: tenant.into(),
        series: series.into(),
        value,
        tags: BTreeMap::new(),
    }
}

#[test]
fn projection_matches_unpruned_duckdb_for_hot_mixed_and_reopened_data() {
    let temp = TempDir::new().unwrap();
    let mut db = Database::open(temp.path(), Config::default()).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: vec![1, 10],
            ..Default::default()
        },
    )
    .unwrap();
    db.create_continuous_aggregate("metrics_ten", "metrics", 10)
        .unwrap();
    db.write(
        "metrics",
        "initial",
        vec![
            row(-10, "a", "cpu", 1.0),
            row(-1, "A", "cpu", 2.0),
            row(0, "O'Brien", "東京", 3.0),
            row(9, r"a\b", "disk", 4.0),
            row(10, "a", "cpu", 5.0),
            row(11, "a", "disk", 6.0),
        ],
        12,
    )
    .unwrap();
    for phase in ["hot", "mixed", "reopened"] {
        if phase == "mixed" {
            db.checkpoint().unwrap();
            db.write("metrics", "late", vec![row(1, "a", "cpu", 7.0)], 12)
                .unwrap();
        } else if phase == "reopened" {
            drop(db);
            db = Database::open(temp.path(), Config::default()).unwrap();
        }
        for predicate in [
            "tenant='a' AND series='cpu'",
            "'O''Brien'=m.tenant AND m.series='東京'",
            "\"m\".\"TENANT\"='A'",
            r"tenant='a\b'",
            "tenant='a' AND tenant='A'",
            "tenant='a' OR series='cpu'",
            "NOT (tenant='a')",
            "tenant IN ('a','A')",
            "tenant COLLATE NOCASE='A'",
            "tenant=('A' COLLATE NOCASE)",
            "lower(tenant)='a'",
            "tenant::VARCHAR='a'",
            "tenant LIKE 'a%'",
            "tenant='a' AND (series='cpu' OR series='disk')",
            "tenant='a' AND timestamp_us >= -1 AND timestamp_us < 10",
        ] {
            let projection = "timestamp_us,tenant,series,value";
            let sql =
                format!("SELECT {projection} FROM metrics m WHERE {predicate} ORDER BY 1,2,3,4");
            let oracle = format!(
                "WITH source AS (SELECT * FROM metrics) SELECT {projection} FROM source m WHERE {predicate} ORDER BY 1,2,3,4"
            );
            assert_eq!(
                db.query(&sql).unwrap(),
                db.query(&oracle).unwrap(),
                "{phase}: {predicate}"
            );
        }
        for predicate in [
            "tenant='a' AND series='cpu'",
            "tenant COLLATE NOCASE='A'",
            "tenant='a' OR series='cpu'",
        ] {
            let projection = "sum(count)::BIGINT AS n,sum(sum) AS total";
            let sql = format!("SELECT {projection} FROM metrics_ten WHERE {predicate}");
            let oracle = format!(
                "WITH source AS (SELECT * FROM metrics_ten) SELECT {projection} FROM source WHERE {predicate}"
            );
            assert_eq!(
                db.query(&sql).unwrap(),
                db.query(&oracle).unwrap(),
                "{phase}: {sql}"
            );
        }
        let native = db
            .scan("metrics", None, None, Some("a"), Some("cpu"))
            .unwrap();
        assert_eq!(native.len(), if phase == "hot" { 2 } else { 3 });
        assert_eq!(db.status().unwrap().active_snapshots, 0);
        assert_eq!(db.status().unwrap().active_queries, 0);
    }
}

struct CountedStore {
    inner: FileStore,
    segment_gets: AtomicUsize,
}
impl RemoteStore for CountedStore {
    fn local_root(&self) -> Option<&Path> {
        self.inner.local_root()
    }
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        if key.starts_with("segments/") {
            self.segment_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }
    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put_immutable(key, bytes)
    }
    fn head(&self) -> Result<Option<HeadObject>> {
        self.inner.head()
    }
    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        self.inner.compare_and_swap_head(expected, bytes)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }
}

#[test]
fn cold_exact_series_fetches_one_shard_and_named_rollups_fetch_no_raw_files() {
    let temp = TempDir::new().unwrap();
    let store = Arc::new(CountedStore {
        inner: FileStore::new(temp.path().join("remote")).unwrap(),
        segment_gets: AtomicUsize::new(0),
    });
    let config = Config {
        flush_interval_us: 1,
        ship_interval_us: 1,
        ..Default::default()
    };
    let db =
        Database::open_with_remote(temp.path().join("local"), config, Some(store.clone())).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            shards: 8,
            archive_after_us: Some(1),
            rollup_widths_us: vec![1, 10],
            ..Default::default()
        },
    )
    .unwrap();
    db.create_continuous_aggregate("metrics_ten", "metrics", 10)
        .unwrap();
    let mut shards = BTreeMap::new();
    for i in 0..1024 {
        let series = format!("series{i}");
        shards
            .entry(shard_for("tenant", &series, 8))
            .or_insert(series);
        if shards.len() == 8 {
            break;
        }
    }
    assert_eq!(shards.len(), 8);
    let selected = shards.values().next().unwrap().clone();
    let rows = shards
        .values()
        .map(|series| row(1, "tenant", series, 1.0))
        .collect();
    db.write("metrics", "initial", rows, 2).unwrap();
    assert_eq!(db.maintain(1000).unwrap().evicted_files, 8);
    db.write(
        "metrics",
        "hot",
        vec![row(2, "tenant", &selected, 17.0)],
        1000,
    )
    .unwrap();
    store.segment_gets.store(0, Ordering::SeqCst);
    let predicate = format!("tenant='tenant' AND series='{selected}'");
    let rollup = format!(
        "SELECT sum(count)::BIGINT AS n,sum(sum) AS total FROM metrics_ten WHERE {predicate}"
    );
    assert_eq!(db.query(&rollup).unwrap(), json!([{"n":2,"total":18.0}]));
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 0);
    assert_eq!(
        db.query("SELECT count(*)::BIGINT AS n FROM metrics WHERE tenant='a' AND tenant='b'")
            .unwrap(),
        json!([{"n":0}])
    );
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 0);
    let sql =
        format!("SELECT count(*)::BIGINT AS n,sum(value) AS total FROM metrics WHERE {predicate}");
    assert_eq!(db.query(&sql).unwrap(), json!([{"n":2,"total":18.0}]));
    assert_eq!(
        store.segment_gets.load(Ordering::SeqCst),
        1,
        "only the selected hash shard may be fetched"
    );
    assert_eq!(
        db.query("SELECT count(*)::BIGINT AS n FROM metrics")
            .unwrap(),
        json!([{"n":9}])
    );
    assert_eq!(
        store.segment_gets.load(Ordering::SeqCst),
        8,
        "unfiltered SQL still exposes every shard"
    );
    assert_eq!(db.status().unwrap().active_snapshots, 0);
}
