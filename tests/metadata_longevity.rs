use std::collections::BTreeMap;
use tempfile::TempDir;
use varve::{Config, Database, IDEMPOTENCY_MAX_FUTURE_SKEW_US, LifecyclePolicy, Row, TableConfig};

fn row(timestamp_us: i64, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value,
        tags: BTreeMap::new(),
    }
}

fn timed_table(window_us: i64) -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 100,
        rollup_widths_us: vec![10],
        idempotency_window_us: Some(window_us),
        ..Default::default()
    }
}

#[test]
fn timed_ids_retry_replay_expire_and_prune_at_checkpoint() {
    let temporary = TempDir::new().unwrap();
    let config = Config::default();
    let database = Database::open(temporary.path(), config.clone()).unwrap();
    database.create_table("metrics", timed_table(10)).unwrap();

    let first = database
        .write("metrics", "v1:100:first", vec![row(1, 1.0)], 100)
        .unwrap();
    let retry = database
        .write("metrics", "v1:100:first", vec![row(1, 1.0)], 105)
        .unwrap();
    assert!(retry.duplicate);
    assert_eq!(retry.sequence, first.sequence);
    drop(database);

    let database = Database::open(temporary.path(), config.clone()).unwrap();
    let replay_retry = database
        .write("metrics", "v1:100:first", vec![row(1, 1.0)], 106)
        .unwrap();
    assert!(replay_retry.duplicate);
    assert_eq!(replay_retry.sequence, first.sequence);
    assert!(
        database
            .write("metrics", "v1:100:first", vec![row(1, 1.0)], 111)
            .unwrap_err()
            .to_string()
            .contains("outside the idempotency window")
    );

    database
        .write("metrics", "v1:111:second", vec![row(2, 2.0)], 111)
        .unwrap();
    database.checkpoint().unwrap();
    assert_eq!(database.idempotency_floor_us("metrics").unwrap(), Some(101));
    assert_eq!(database.status().unwrap().idempotency_keys, 1);
    assert_eq!(
        database
            .scan("metrics", None, None, None, None)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(database.rollups("metrics").unwrap()[0].count, 2);
    drop(database);

    let reopened = Database::open(temporary.path(), config).unwrap();
    assert!(
        reopened
            .write("metrics", "v1:100:first", vec![row(1, 1.0)], 90)
            .is_err()
    );
    let second_retry = reopened
        .write("metrics", "v1:111:second", vec![row(2, 2.0)], 90)
        .unwrap();
    assert!(
        second_retry.duplicate,
        "clock rollback must not break an accepted retry"
    );
}

#[test]
fn cutover_is_explicit_future_skew_is_bounded_and_extension_never_resurrects() {
    let temporary = TempDir::new().unwrap();
    let database = Database::open(temporary.path(), Config::default()).unwrap();
    database
        .create_table(
            "metrics",
            TableConfig {
                shards: 1,
                rollup_widths_us: vec![],
                ..Default::default()
            },
        )
        .unwrap();
    database
        .write("metrics", "arbitrary-legacy-id", vec![row(1, 1.0)], 1_000)
        .unwrap();
    database
        .set_policy(
            "metrics",
            LifecyclePolicy {
                idempotency_window_us: Some(10),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        database
            .write("metrics", "arbitrary-legacy-id", vec![row(1, 1.0)], 1_000)
            .is_err()
    );
    assert!(
        database
            .write(
                "metrics",
                &format!("v1:{}:future", 1_000 + IDEMPOTENCY_MAX_FUTURE_SKEW_US + 1),
                vec![row(2, 2.0)],
                1_000,
            )
            .is_err()
    );
    database
        .write("metrics", "v1:1000:accepted", vec![row(2, 2.0)], 1_000)
        .unwrap();
    database.checkpoint().unwrap();
    assert_eq!(database.idempotency_floor_us("metrics").unwrap(), Some(990));

    database
        .set_policy(
            "metrics",
            LifecyclePolicy {
                idempotency_window_us: Some(1_000),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(database.idempotency_floor_us("metrics").unwrap(), Some(990));
    assert!(
        database
            .write("metrics", "v1:989:forgotten", vec![row(3, 3.0)], 900)
            .is_err()
    );
    let before = database.status().unwrap().sequence;
    assert!(
        database
            .set_policy("metrics", LifecyclePolicy::default())
            .is_err()
    );
    assert_eq!(database.status().unwrap().sequence, before);
}

#[test]
fn maintenance_clock_advances_and_persists_floor_without_new_data() {
    let temporary = TempDir::new().unwrap();
    let config = Config::default();
    let database = Database::open(temporary.path(), config.clone()).unwrap();
    database.create_table("metrics", timed_table(10)).unwrap();
    database
        .write("metrics", "v1:10:one", vec![row(1, 1.0)], 10)
        .unwrap();
    database.checkpoint().unwrap();
    database.maintain(100).unwrap();
    assert_eq!(database.idempotency_floor_us("metrics").unwrap(), Some(90));
    assert_eq!(database.status().unwrap().idempotency_keys, 0);
    drop(database);

    let reopened = Database::open(temporary.path(), config).unwrap();
    assert_eq!(reopened.idempotency_floor_us("metrics").unwrap(), Some(90));
    assert!(
        reopened
            .write("metrics", "v1:10:one", vec![row(1, 1.0)], 50)
            .is_err()
    );
}
