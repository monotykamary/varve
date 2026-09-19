use anyhow::Result;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use varve::model::shard_for;
use varve::remote::{FileStore, HeadObject, RemoteStore};
use varve::{Config, Database, Row, TableConfig, segment};

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
fn nested_queries_match_unpruned_duckdb_across_window_limit_and_aggregation() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(temp.path(), Config::default()).unwrap();
    db.create_table("metrics", TableConfig::default()).unwrap();
    db.write(
        "metrics",
        "rows",
        vec![
            row(1, "a", "cpu", 10.0),
            row(2, "a", "cpu", 20.0),
            row(3, "a", "disk", 30.0),
            row(4, "b", "cpu", 40.0),
        ],
        5,
    )
    .unwrap();

    let cases = [
        (
            r#"SELECT timestamp_us,value
                FROM (
                    SELECT timestamp_us,value,
                        row_number() OVER (
                            ORDER BY timestamp_us DESC,value DESC
                        ) AS rn
                    FROM metrics
                    WHERE tenant='a' AND series='cpu'
                ) ranked
                WHERE rn <= 1
                ORDER BY timestamp_us"#,
            r#"WITH source AS (SELECT * FROM metrics)
                SELECT timestamp_us,value
                FROM (
                    SELECT timestamp_us,value,
                        row_number() OVER (
                            ORDER BY timestamp_us DESC,value DESC
                        ) AS rn
                    FROM source
                    WHERE tenant='a' AND series='cpu'
                ) ranked
                WHERE rn <= 1
                ORDER BY timestamp_us"#,
        ),
        (
            r#"SELECT timestamp_us,series
                FROM (
                    SELECT timestamp_us,series
                    FROM metrics
                    WHERE tenant='a'
                    ORDER BY timestamp_us
                    LIMIT 2
                ) limited
                WHERE timestamp_us >= 2
                ORDER BY timestamp_us"#,
            r#"WITH source AS (SELECT * FROM metrics)
                SELECT timestamp_us,series
                FROM (
                    SELECT timestamp_us,series
                    FROM source
                    WHERE tenant='a'
                    ORDER BY timestamp_us
                    LIMIT 2
                ) limited
                WHERE timestamp_us >= 2
                ORDER BY timestamp_us"#,
        ),
        (
            r#"SELECT series,total
                FROM (
                    SELECT series,sum(value) AS total
                    FROM metrics
                    WHERE tenant='a'
                    GROUP BY series
                ) grouped
                WHERE total >= 20
                ORDER BY series"#,
            r#"WITH source AS (SELECT * FROM metrics)
                SELECT series,total
                FROM (
                    SELECT series,sum(value) AS total
                    FROM source
                    WHERE tenant='a'
                    GROUP BY series
                ) grouped
                WHERE total >= 20
                ORDER BY series"#,
        ),
    ];

    for (sql, oracle) in cases {
        assert_eq!(db.query(sql).unwrap(), db.query(oracle).unwrap(), "{sql}");
    }
}

struct TrackingStore {
    inner: FileStore,
    segment_gets: Mutex<Vec<String>>,
}

impl TrackingStore {
    fn take_segment_gets(&self) -> Vec<String> {
        std::mem::take(&mut *self.segment_gets.lock().unwrap())
    }
}

impl RemoteStore for TrackingStore {
    fn local_root(&self) -> Option<&Path> {
        self.inner.local_root()
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        if key.starts_with("segments/") {
            self.segment_gets.lock().unwrap().push(key.to_string());
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
fn eligible_nested_query_skips_missing_cold_files_but_side_query_falls_back() {
    let temp = TempDir::new().unwrap();
    let store = Arc::new(TrackingStore {
        inner: FileStore::new(temp.path().join("remote")).unwrap(),
        segment_gets: Mutex::new(Vec::new()),
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
            ..Default::default()
        },
    )
    .unwrap();
    db.create_table(
        "unrelated",
        TableConfig {
            archive_after_us: Some(1),
            ..Default::default()
        },
    )
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
    db.write(
        "metrics",
        "cold-rows",
        shards
            .values()
            .enumerate()
            .map(|(index, series)| row(1, "tenant", series, index as f64 + 1.0))
            .collect(),
        2,
    )
    .unwrap();
    db.write(
        "unrelated",
        "unrelated-cold-row",
        vec![row(1, "other-tenant", "other-series", 99.0)],
        2,
    )
    .unwrap();
    assert_eq!(db.maintain(1000).unwrap().evicted_files, 9);

    let segment_keys = store.list("segments").unwrap();
    assert_eq!(segment_keys.len(), 9);
    let remote_root = store.local_root().unwrap();
    let selected_key = segment_keys
        .iter()
        .find(|key| {
            segment::read(&remote_root.join(key))
                .unwrap()
                .iter()
                .any(|stored| stored.row.tenant == "tenant" && stored.row.series == selected)
        })
        .unwrap()
        .clone();
    let unrelated_key = segment_keys
        .iter()
        .find(|key| {
            segment::read(&remote_root.join(key))
                .unwrap()
                .iter()
                .any(|stored| {
                    stored.row.tenant == "other-tenant" && stored.row.series == "other-series"
                })
        })
        .unwrap()
        .clone();
    assert_ne!(selected_key, unrelated_key);
    store.delete(&unrelated_key).unwrap();

    let nested = format!(
        r#"SELECT timestamp_us,value
            FROM (
                SELECT timestamp_us,value,
                    row_number() OVER (ORDER BY timestamp_us DESC) AS rn
                FROM metrics
                WHERE tenant='tenant' AND series='{selected}'
            ) ranked
            WHERE rn <= 5"#
    );
    let result = db.query(&nested).unwrap();
    assert_eq!(result, json!([{"timestamp_us": 1, "value": 1.0}]));
    assert_eq!(store.take_segment_gets(), vec![selected_key.clone()]);

    let unsupported = format!(
        r#"SELECT timestamp_us,value
            FROM (
                SELECT timestamp_us,value,
                    row_number() OVER (ORDER BY timestamp_us DESC) AS rn
                FROM metrics
                WHERE tenant='tenant' AND series='{selected}'
            ) ranked
            WHERE rn <= (SELECT 5)"#
    );
    let error = db.query(&unsupported).unwrap_err();
    let attempted = store.take_segment_gets();
    assert!(
        attempted.contains(&unrelated_key),
        "fallback did not attempt the unrelated table's missing cold file: {attempted:?}; error: {error:#}"
    );
}
