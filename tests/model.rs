use std::collections::BTreeMap;
use varve::model::*;

#[test]
fn row_json_roundtrip_and_validation() {
    let row = Row {
        timestamp_us: -1,
        tenant: "tenant".into(),
        series: "series".into(),
        value: 0.10000000000000002,
        tags: BTreeMap::new(),
    };
    assert_eq!(
        serde_json::from_slice::<Row>(&serde_json::to_vec(&row).unwrap()).unwrap(),
        row
    );
    let stored = StoredRow {
        row,
        sequence: 2,
        ordinal: 3,
    };
    assert_eq!(
        serde_json::from_slice::<StoredRow>(&serde_json::to_vec(&stored).unwrap()).unwrap(),
        stored
    );
}

#[test]
fn finite_float_bits_roundtrip_through_the_wal_json_representation() {
    let mut bits = 0x9e3779b97f4a7c15_u64;
    for _ in 0..10_000 {
        bits ^= bits << 13;
        bits ^= bits >> 7;
        bits ^= bits << 17;
        let value = f64::from_bits(bits);
        if value.is_finite() {
            let encoded = serde_json::to_vec(&value).unwrap();
            let decoded: f64 = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(
                decoded.to_bits(),
                bits,
                "{}",
                String::from_utf8(encoded).unwrap()
            );
        }
    }
    assert_eq!(
        serde_json::from_str::<f64>("-0.0").unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
}

#[test]
fn every_public_configuration_entry_is_documented() {
    let documentation = include_str!("../docs/CONFIGURATION.md");
    for config in [
        serde_json::to_value(Config::default()).unwrap(),
        serde_json::to_value(TableConfig::default()).unwrap(),
    ] {
        for key in config.as_object().unwrap().keys() {
            assert!(
                documentation.contains(&format!("`{key}`")),
                "undocumented configuration: {key}"
            );
        }
    }
}

#[test]
fn frozen_prefix_checkpoints_are_explicitly_opted_in() {
    assert!(!Config::default().checkpoint_frozen_prefix);
    let legacy: Config = serde_json::from_str("{}").unwrap();
    assert!(!legacy.checkpoint_frozen_prefix);
    let opted_in: Config = serde_json::from_value(serde_json::json!({
        "checkpoint_frozen_prefix": true
    }))
    .unwrap();
    opted_in.validate().unwrap();
    assert!(opted_in.checkpoint_frozen_prefix);
    assert_eq!(
        serde_json::to_value(opted_in).unwrap()["checkpoint_frozen_prefix"],
        serde_json::json!(true)
    );
}

#[test]
fn retained_query_inputs_are_explicitly_opted_in() {
    assert!(!Config::default().query_retained_inputs);
    let legacy: Config = serde_json::from_str("{}").unwrap();
    assert!(!legacy.query_retained_inputs);
    let opted_in: Config = serde_json::from_value(serde_json::json!({
        "query_retained_inputs": true
    }))
    .unwrap();
    opted_in.validate().unwrap();
    assert!(opted_in.query_retained_inputs);
    assert_eq!(
        serde_json::to_value(opted_in).unwrap()["query_retained_inputs"],
        serde_json::json!(true)
    );
}

#[test]
fn derived_layout_migration_is_explicit_and_writer_pages_are_bounded() {
    let legacy: Config = serde_json::from_str("{}").unwrap();
    assert!(!legacy.derived_pages);
    assert_eq!(legacy.derived_max_bytes, 64 * 1024 * 1024);
    assert_eq!(legacy.derived_page_bytes, 256 * 1024);
    legacy.validate().unwrap();
    let opted_in: Config = serde_json::from_value(serde_json::json!({
        "derived_pages": true, "derived_max_bytes": 8192, "derived_page_bytes": 4096
    }))
    .unwrap();
    opted_in.validate().unwrap();
    assert!(opted_in.derived_pages);
    for (budget, page) in [
        (4095, 4096),
        (8192, 4095),
        (4096, 8192),
        (512 * 1024 * 1024 + 1, 4096),
        (8 * 1024 * 1024, 1024 * 1024 + 1),
    ] {
        assert!(
            Config {
                derived_max_bytes: budget,
                derived_page_bytes: page,
                ..Config::default()
            }
            .validate()
            .is_err()
        );
    }
    assert!(
        serde_json::from_str::<Config>("{\"derived_page_bytes\":0}")
            .unwrap()
            .validate()
            .is_err()
    );
    assert!(serde_json::from_str::<Config>("{\"derived_pages\":\"true\"}").is_err());
}

#[test]
fn invalid_configuration_and_partition_extremes() {
    for name in ["", "x__rollup", "../x", "UPPER", "1x", "a-b"] {
        assert!(validate_name(name).is_err());
    }
    for width in [0, -1] {
        assert!(window_start(1, width).is_err());
    }
    assert_eq!(window_start(i64::MIN, 1).unwrap(), i64::MIN);
    assert_eq!(window_start(-1, 100).unwrap(), -100);
    assert_eq!(shard_for("tenant", "cpu", 8), 2);
    assert_eq!(shard_for("ab", "c", 1024), 726);
    assert_eq!(shard_for("a", "bc", 1024), 234);
    let config = TableConfig {
        rollup_widths_us: vec![10, 10],
        ..Default::default()
    };
    assert!(config.validate().is_err());
    let config = TableConfig {
        shards: 0,
        ..Default::default()
    };
    assert!(config.validate().is_err());
    let config = TableConfig {
        idempotency_window_us: Some(0),
        ..Default::default()
    };
    assert!(config.validate().is_err());
    let config = Config {
        hot_max_rows: 0,
        ..Default::default()
    };
    assert!(config.validate().is_err());
}
