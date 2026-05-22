use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeySchemaElement, KeyType,
    StoredTableInfo, TableName, TableStatus, TimeToLiveStatus, TimestampMillis,
};

use crate::ttl::{
    TtlConfigRecord, TtlIndexKey, TtlIndexKeyToken, parse_ttl_index_key, ttl_gsi_name,
    ttl_index_key, ttl_index_key_map_from_token, ttl_index_key_token_for_item, ttl_index_prefix,
};

fn make_config() -> TtlConfigRecord {
    let table = TableName::new("TestTable");
    let gsi = ttl_gsi_name(&table);
    TtlConfigRecord::new("ttl_attr".to_string(), &gsi, TimeToLiveStatus::Enabling)
}

fn make_table_info() -> StoredTableInfo {
    let table = TableName::new("TokenTable");
    StoredTableInfo {
        table_name: table,
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
    }
}

#[test]
fn ttl_index_token_round_trip() {
    let table_info = make_table_info();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("session".to_string()));

    let token = ttl_index_key_token_for_item(&table_info, &item).unwrap();
    let key_map = ttl_index_key_map_from_token(&token, &table_info).unwrap();
    let typed_key_map = TtlIndexKeyToken::from_item(&table_info, &item)
        .unwrap()
        .parse_key_map(&table_info)
        .unwrap();

    assert_eq!(
        key_map.get("pk"),
        Some(&AttributeValue::S("user".to_string()))
    );
    assert_eq!(
        key_map.get("sk"),
        Some(&AttributeValue::S("session".to_string()))
    );
    assert_eq!(typed_key_map, key_map);
}

#[test]
fn ttl_index_key_parses_timestamp_and_token() {
    let table_info = make_table_info();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("alpha".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("beta".to_string()));
    let token = ttl_index_key_token_for_item(&table_info, &item).unwrap();

    let table_name = &table_info.table_name;
    let ttl_seconds = 1_700_000_000_i64;
    let key = ttl_index_key(table_name, ttl_seconds, &token);
    let prefix = ttl_index_prefix(table_name);

    let parsed = parse_ttl_index_key(&key, &prefix).expect("parsed");
    let typed_parsed = TtlIndexKey::parse(&key, &prefix).expect("typed parsed");
    assert_eq!(parsed.0, ttl_seconds);
    assert_eq!(parsed.1, token);
    assert_eq!(typed_parsed.ttl_seconds(), ttl_seconds);
    assert_eq!(typed_parsed.token().as_str(), token);
    assert_eq!(typed_parsed.encode(table_name), key);
}

#[test]
fn ttl_index_key_builds_from_item() {
    let table_info = make_table_info();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("alpha".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("beta".to_string()));
    item.insert(
        "ttl".to_string(),
        AttributeValue::N("1700000000".to_string()),
    );

    let ttl_index_key = TtlIndexKey::for_item(&table_info, "ttl", &item)
        .expect("ttl key should build")
        .expect("ttl value should be present");
    let encoded = ttl_index_key.encode(&table_info.table_name);
    let parsed =
        TtlIndexKey::parse(&encoded, &ttl_index_prefix(&table_info.table_name)).expect("parse");

    assert_eq!(parsed.ttl_seconds(), 1_700_000_000);
    assert_eq!(parsed.token().as_str(), ttl_index_key.token().as_str());
}

#[test]
fn adaptive_batch_requires_consecutive_low_utilization() {
    let mut config = make_config();
    let interval_ms = 60_000;
    assert!(
        config
            .update_adaptive_batch(10_000, interval_ms, 2, 32, 8)
            .is_none()
    );
    assert!(
        config
            .update_adaptive_batch(10_000, interval_ms, 2, 32, 8)
            .is_some()
    );
    assert_eq!(config.adaptive_pk_batch_size, Some(9));
    assert!(
        config
            .update_adaptive_batch(50_000, interval_ms, 2, 32, 8)
            .is_none()
    );
    assert!(
        config
            .update_adaptive_batch(50_000, interval_ms, 2, 32, 8)
            .is_some()
    );
    assert_eq!(config.adaptive_pk_batch_size, Some(8));
}

#[test]
fn skip_progression_caps_and_resets() {
    let mut config = make_config();
    for _ in 0..15 {
        config.register_idle(10);
    }
    assert_eq!(config.skip_streak, 10);
    assert_eq!(config.skip_runs_remaining, 9);
    for _ in 0..9 {
        config.consume_skip();
    }
    assert_eq!(config.skip_runs_remaining, 0);
    config.register_progress();
    assert_eq!(config.skip_streak, 0);
    assert_eq!(config.skip_runs_remaining, 0);
}

#[test]
fn progress_resets_throttle_and_skip() {
    let mut config = make_config();
    config.register_idle(10);
    config.register_throttle();
    config.register_throttle();
    assert_eq!(config.throttled_runs, 2);

    config.register_progress();
    assert_eq!(config.throttled_runs, 0);
    assert_eq!(config.skip_streak, 0);
    assert_eq!(config.skip_runs_remaining, 0);
}

#[test]
fn throttle_tracking_resets() {
    let mut config = make_config();
    config.register_throttle();
    config.register_throttle();
    assert_eq!(config.throttled_runs, 2);

    config.reset_throttle();
    assert_eq!(config.throttled_runs, 0);
}
