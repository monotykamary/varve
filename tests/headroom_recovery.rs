use std::path::Path;
#[cfg(feature = "fault-injection")]
use std::process::Command;

use tempfile::TempDir;
use varve::{Config, Database, Row, TableConfig, WriteRequest};

fn config(metadata_max_bytes: usize) -> Config {
    Config {
        metadata_max_bytes,
        segment_rows: 64,
        ..Default::default()
    }
}

fn table() -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 100,
        rollup_widths_us: vec![10],
        ..Default::default()
    }
}

fn rows(count: usize) -> Vec<Row> {
    vec![
        Row {
            timestamp_us: 1,
            tenant: "tenant".into(),
            series: "series".into(),
            value: 1.0,
            tags: Default::default(),
        };
        count
    ]
}

fn request(id: &str) -> WriteRequest {
    WriteRequest {
        table: "metrics".into(),
        request_id: id.into(),
        now_us: 100,
        rows: rows(2),
    }
}

fn seed_with_padding(root: &Path, padding: usize) -> (usize, u64) {
    let db = Database::open(root, config(Config::default().metadata_max_bytes)).unwrap();
    for index in 0..padding {
        db.create_table(&format!("padding_{index}"), table())
            .unwrap();
    }
    db.create_table("metrics", table()).unwrap();
    db.create_continuous_aggregate("metrics_10", "metrics", 10)
        .unwrap();
    db.checkpoint().unwrap();
    let receipt = db.write("metrics", "seed", rows(16), 100).unwrap();
    let cap = db.status().unwrap().metadata_bytes + 20 * 512;
    (cap, receipt.sequence)
}

fn seed(root: &Path) -> (usize, u64) {
    seed_with_padding(root, 0)
}

fn open(root: &Path, cap: usize) -> Database {
    Database::open(root, config(cap)).unwrap()
}

fn assert_totals(db: &Database, expected: usize) {
    let raw = db.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(raw.len(), expected);
    assert_eq!(
        raw.iter().map(|row| row.row.value).sum::<f64>(),
        expected as f64
    );

    let rollups = db.rollups("metrics").unwrap();
    assert_eq!(rollups.len(), 1);
    assert_eq!(rollups[0].width_us, 10);
    assert_eq!(rollups[0].count, expected as u64);
    assert_eq!(rollups[0].sum, expected as f64);
}

fn write_new_group(db: &Database) -> u64 {
    let results = db.write_group(vec![request("a"), request("b")]);
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(Result::is_ok));
    assert!(
        results
            .iter()
            .all(|result| !result.as_ref().unwrap().duplicate)
    );
    let sequence = results[0].as_ref().unwrap().sequence;
    assert!(
        results
            .iter()
            .all(|result| result.as_ref().unwrap().sequence == sequence)
    );
    sequence
}

fn retry_group(db: &Database, committed: bool, point: &str) -> u64 {
    let results = db.write_group(vec![request("a"), request("b")]);
    assert_eq!(results.len(), 2, "{point}");
    let mut sequence = None;
    for result in results {
        let receipt = result.unwrap_or_else(|error| panic!("{point}: {error:#}"));
        assert_eq!(receipt.duplicate, committed, "{point}");
        assert_eq!(*sequence.get_or_insert(receipt.sequence), receipt.sequence);
    }
    sequence.unwrap()
}

#[test]
fn grouped_headroom_checkpoints_only_seed_before_rollup_commit() {
    let temp = TempDir::new().unwrap();
    let (cap, seed_sequence) = seed(temp.path());

    let db = open(temp.path(), cap);
    let before = db.status().unwrap();
    assert_eq!(before.sequence, seed_sequence);
    assert!(before.checkpoint_sequence < seed_sequence);
    assert_eq!(before.hot_rows, 16);

    let group_sequence = write_new_group(&db);
    assert_eq!(group_sequence, seed_sequence + 1);
    let status = db.status().unwrap();
    assert_eq!(status.sequence, seed_sequence + 1);
    assert_eq!(status.checkpoint_sequence, seed_sequence);
    assert_eq!(status.hot_rows, 4);
    assert_eq!(status.segments, 1);
    assert_totals(&db, 20);

    drop(db);
    let db = open(temp.path(), cap);
    assert_totals(&db, 20);
    retry_group(&db, true, "success reopen");
    assert_totals(&db, 20);
}

#[test]
fn grouped_headroom_recovers_across_checkpoint_sequence_decimal_width_boundary() {
    let temp = TempDir::new().unwrap();
    let (cap, seed_sequence) = seed_with_padding(temp.path(), 6);
    assert_eq!(seed_sequence, 9);

    let db = open(temp.path(), cap);
    let before = db.status().unwrap();
    assert_eq!(before.sequence, 9);
    assert_eq!(before.checkpoint_sequence, 8);
    assert_eq!(before.hot_rows, 16);

    let group_sequence = write_new_group(&db);
    assert_eq!(group_sequence, 10);
    let status = db.status().unwrap();
    assert_eq!(status.sequence, 10);
    assert_eq!(status.checkpoint_sequence, 9);
    assert_eq!(status.hot_rows, 4);
    assert_eq!(status.segments, 1);
    assert_totals(&db, 20);
    assert_eq!(retry_group(&db, true, "decimal boundary retry"), 10);
    assert_totals(&db, 20);

    drop(db);
    let db = open(temp.path(), cap);
    let status = db.status().unwrap();
    assert_eq!(status.sequence, 10);
    assert_eq!(status.checkpoint_sequence, 9);
    assert_eq!(status.hot_rows, 4);
    assert_eq!(status.segments, 1);
    assert_totals(&db, 20);
    assert_eq!(retry_group(&db, true, "decimal boundary reopen retry"), 10);
    assert_totals(&db, 20);
}

#[cfg(feature = "fault-injection")]
#[test]
fn headroom_fault_child() {
    let Ok(root) = std::env::var("VARVE_HEADROOM_CHILD_ROOT") else {
        return;
    };
    let cap = std::env::var("VARVE_HEADROOM_CHILD_CAP")
        .unwrap()
        .parse()
        .unwrap();
    let db = open(Path::new(&root), cap);
    let results = db.write_group(vec![request("a"), request("b")]);
    assert!(results.iter().all(Result::is_err));
    assert!(db.status().unwrap().fenced.is_some());
}

#[cfg(feature = "fault-injection")]
#[test]
fn grouped_headroom_checkpoint_and_wal_fault_matrix_recovers_exactly_once() {
    let cases = [
        ("segment_written", false, false),
        ("segments_published", false, false),
        ("manifest_published", false, false),
        ("atomic_manifest.bin_before_write", false, true),
        ("atomic_manifest.bin_before_rename", false, true),
        ("atomic_manifest.bin_before_dir_sync", false, true),
        ("wal_synced", false, false),
        ("wal_published", true, false),
        ("group_before_apply", true, false),
        ("group_applied", true, false),
        ("wal_before_write", false, true),
        ("wal_before_sync", false, true),
        ("wal_before_rename", false, true),
        ("wal_before_dir_sync", true, true),
    ];

    for (point, committed, io) in cases {
        let temp = TempDir::new().unwrap();
        let (cap, _) = seed(temp.path());
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "headroom_fault_child", "--nocapture"])
            .env("VARVE_HEADROOM_CHILD_ROOT", temp.path())
            .env("VARVE_HEADROOM_CHILD_CAP", cap.to_string())
            .env_remove(if io {
                "VARVE_FAILPOINT"
            } else {
                "VARVE_IO_FAILPOINT"
            })
            .env(
                if io {
                    "VARVE_IO_FAILPOINT"
                } else {
                    "VARVE_FAILPOINT"
                },
                point,
            )
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(if io { 0 } else { 86 }),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let db = open(temp.path(), cap);
        assert_totals(&db, if committed { 20 } else { 16 });
        retry_group(&db, committed, point);
        assert_totals(&db, 20);
        drop(db);

        let db = open(temp.path(), cap);
        assert_totals(&db, 20);
        retry_group(&db, true, point);
        assert_totals(&db, 20);
    }
}
