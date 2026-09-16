use std::{collections::BTreeMap, fs, sync::Arc};
use tempfile::TempDir;
use varve::remote::FileStore;
use varve::{Config, Database, Row, TableConfig};

fn row(ts: i64) -> Row {
    Row {
        timestamp_us: ts,
        tenant: "t".into(),
        series: "s".into(),
        value: 1.0,
        tags: BTreeMap::new(),
    }
}
fn table() -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 100,
        rollup_widths_us: vec![10],
        archive_after_us: Some(1),
        ..Default::default()
    }
}
fn config() -> Config {
    Config {
        flush_interval_us: 1,
        ship_interval_us: 1,
        ..Default::default()
    }
}

#[test]
fn cold_queries_evict_unrelated_cache_and_prune_time_windows() {
    let tmp = TempDir::new().unwrap();
    let remote = Arc::new(FileStore::new(tmp.path().join("objects")).unwrap());
    let root = tmp.path().join("db");
    let db = Database::open_with_remote(&root, config(), Some(remote.clone())).unwrap();
    db.create_table("alpha", table()).unwrap();
    db.create_table("beta", table()).unwrap();
    db.write("alpha", "a", vec![row(1), row(101)], 101).unwrap();
    db.write("beta", "b", vec![row(2)], 101).unwrap();
    db.checkpoint().unwrap();
    let largest = fs::read_dir(root.join("segments"))
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .max()
        .unwrap();
    db.maintain(1000).unwrap();
    drop(db);
    let c = Config {
        disk_cache_bytes: largest,
        ..config()
    };
    let db = Database::open_with_remote(&root, c, Some(remote)).unwrap();
    for (sql, expected) in [
        (
            "SELECT count(*) AS n FROM alpha WHERE timestamp_us < 100",
            1,
        ),
        ("SELECT count(*) AS n FROM beta", 1),
        (
            "SELECT count(*) AS n FROM alpha WHERE timestamp_us >= 100",
            1,
        ),
        (
            "SELECT count(*) AS n FROM alpha WHERE timestamp_us < -100",
            0,
        ),
        (
            "SELECT CAST(sum(count) AS BIGINT) AS n FROM alpha__rollup",
            2,
        ),
    ] {
        assert_eq!(db.query(sql).unwrap()[0]["n"], expected, "{sql}");
        assert!(db.status().unwrap().disk_cache_bytes <= largest);
    }
    assert!(
        db.query("SELECT count(*) FROM alpha").is_err(),
        "two-file snapshot must respect the one-file cache budget"
    );
    assert_eq!(db.status().unwrap().active_snapshots, 0);
    assert_eq!(db.status().unwrap().active_queries, 0);
}

#[test]
fn amplified_rollup_metadata_is_rejected_before_ack_and_checkpoint_stays_live() {
    let tmp = TempDir::new().unwrap();
    let remote = Arc::new(FileStore::new(tmp.path().join("objects")).unwrap());
    let c = Config {
        metadata_max_bytes: 16 * 1024,
        ..config()
    };
    let db =
        Database::open_with_remote(tmp.path().join("db"), c.clone(), Some(remote.clone())).unwrap();
    db.create_table(
        "metrics",
        TableConfig {
            rollup_widths_us: (1..=16).collect(),
            retention_us: Some(100),
            ..table()
        },
    )
    .unwrap();
    db.write("metrics", "accepted", vec![row(1)], 1).unwrap();
    let before = db.status().unwrap().sequence;
    let mut huge = row(2);
    for n in 0..4 {
        huge.tags.insert(format!("key{n}"), "x".repeat(1024));
    }
    assert!(
        db.write("metrics", "rejected", vec![huge], 2)
            .unwrap_err()
            .to_string()
            .contains("metadata")
    );
    assert_eq!(db.status().unwrap().sequence, before);
    db.checkpoint().unwrap();
    db.maintain(1000).unwrap();
    assert_eq!(db.rollups("metrics").unwrap().len(), 16);
    assert_eq!(db.scan("metrics", None, None, None, None).unwrap().len(), 0);
    assert!(db.status().unwrap().metadata_bytes <= c.metadata_max_bytes);
    drop(db);
    let restored = Database::restore(tmp.path().join("restored"), c, remote).unwrap();
    assert_eq!(restored.rollups("metrics").unwrap().len(), 16);
    let root = tmp.path().join("restored");
    drop(restored);
    assert!(
        Database::open_with_remote(
            root,
            Config {
                metadata_max_bytes: 1024,
                ..config()
            },
            None
        )
        .is_err()
    );
}

#[test]
fn recognized_crash_temps_are_cleaned_before_recovery_disk_accounting() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().join("db");
    let c = Config {
        max_disk_bytes: 4096,
        wal_max_bytes: 2048,
        ..config()
    };
    let db = Database::open(&root, c.clone()).unwrap();
    db.create_table("metrics", table()).unwrap();
    drop(db);
    fs::write(root.join("wal/.unpublished.tmp"), vec![0; 8192]).unwrap();
    fs::write(root.join("staging/segment-crashed.tmp"), vec![0; 8192]).unwrap();
    let db = Database::open(&root, c).unwrap();
    assert!(!root.join("wal/.unpublished.tmp").exists());
    assert!(!root.join("staging/segment-crashed.tmp").exists());
    assert!(db.status().unwrap().disk_bytes < 4096);
}

#[test]
fn smaller_working_memory_rejects_segment_before_decompression() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path(), config()).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "a", vec![row(1); 1000], 1).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(
        tmp.path(),
        Config {
            hot_max_bytes: 1024,
            ..config()
        },
    )
    .unwrap();
    let error = db.scan("metrics", None, None, None, None).unwrap_err();
    assert!(
        error.to_string().contains("decoded working set"),
        "{error:#}"
    );
    assert_eq!(db.status().unwrap().decoded_cache_bytes, 0);
}
