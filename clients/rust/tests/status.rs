use serde_json::json;
use varve_client::Status;

#[test]
fn derived_counters_are_optional_for_old_servers_and_preserved_when_present() {
    let mut value = json!({
        "database_id":"legacy", "sequence":1, "checkpoint_sequence":0,
        "remote_sequence":0, "unshipped_batches":1, "hot_rows":1, "hot_bytes":128,
        "wal_bytes":256, "disk_bytes":512, "metadata_bytes":1024,
        "decoded_cache_bytes":0, "disk_cache_bytes":0, "tables":1, "segments":0,
        "rollup_groups":1, "idempotency_keys":1, "active_queries":0,
        "active_snapshots":0, "fenced":null, "last_maintenance_error":null
    });
    let old: Status = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(old.control_root_bytes, None);
    assert_eq!(old.derived_encoded_bytes, None);
    assert_eq!(old.derived_resident_bytes, None);
    assert_eq!(old.derived_working_bytes, None);
    value["control_root_bytes"] = json!(128);
    value["derived_encoded_bytes"] = json!(256);
    value["derived_resident_bytes"] = json!(512);
    value["derived_working_bytes"] = json!(0);
    value["future_additive_counter"] = json!(42);
    let new: Status = serde_json::from_value(value).unwrap();
    assert_eq!(new.control_root_bytes, Some(128));
    assert_eq!(new.derived_encoded_bytes, Some(256));
    assert_eq!(new.derived_resident_bytes, Some(512));
    assert_eq!(new.derived_working_bytes, Some(0));
    assert_eq!(new.sequence, old.sequence);
}
