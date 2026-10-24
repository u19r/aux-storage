use std::collections::HashMap;

use serde::Serialize;
use storage_types::{
    AttributeDefinition, AttributeValue, IndexName, KeyAttributeType, KeySchemaElement, KeyType,
    StoredTableInfo, TableName, TableStatus, TimeToLiveStatus, TimestampMillis,
};

use crate::ttl::{
    TtlConfigRecord, TtlIndexKey, TtlIndexKeyToken, TtlIndexWrite, TtlIndexWriteBatch,
    TtlMaintenanceCheckpoint, TtlMaintenanceCursor, ttl_tracking_enabled,
};

fn tracking_config(status: TimeToLiveStatus) -> TtlConfigRecord {
    TtlConfigRecord::try_new("ttl".to_string(), &TableName::new("ttl-table"), status)
        .expect("valid TTL config")
}

#[test]
fn given_ttl_config_when_tracking_predicate_runs_then_only_enabled_states_arm() {
    assert!(!ttl_tracking_enabled(None));
    assert!(ttl_tracking_enabled(Some(&tracking_config(
        TimeToLiveStatus::Enabled
    ))));
    assert!(ttl_tracking_enabled(Some(&tracking_config(
        TimeToLiveStatus::Enabling
    ))));
    assert!(!ttl_tracking_enabled(Some(&tracking_config(
        TimeToLiveStatus::Disabling
    ))));
}

fn make_config() -> TtlConfigRecord {
    let table = TableName::new("TestTable");
    TtlConfigRecord::try_new("ttl_attr".to_string(), &table, TimeToLiveStatus::Enabling)
        .expect("valid TTL config")
}

fn make_table_info() -> StoredTableInfo {
    let table = TableName::new("TokenTable");
    StoredTableInfo {
        max_indexers: storage_types::MaxIndexers::ZERO,
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
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

#[test]
fn ttl_index_token_round_trip() {
    let table_info = make_table_info();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("session".to_string()));

    let token = TtlIndexKeyToken::from_item(&table_info, &item).unwrap();
    let key_map = token.parse_key_map(&table_info).unwrap();

    assert_eq!(
        key_map.get("pk"),
        Some(&AttributeValue::S("user".to_string()))
    );
    assert_eq!(
        key_map.get("sk"),
        Some(&AttributeValue::S("session".to_string()))
    );
}

#[test]
fn ttl_index_key_parses_timestamp_and_token() {
    let table_info = make_table_info();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("alpha".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("beta".to_string()));
    let token = TtlIndexKeyToken::from_item(&table_info, &item).unwrap();

    let table_name = &table_info.table_name;
    let ttl_seconds = 1_700_000_000_i64;
    let key = TtlIndexKey::new(ttl_seconds, token.clone()).encode(table_name);
    let prefix = TtlIndexKey::prefix(table_name);

    let parsed = TtlIndexKey::parse(&key, &prefix).expect("parsed");
    let (parsed_ttl_seconds, parsed_token): (i64, String) = parsed.clone().into();
    assert_eq!(parsed_ttl_seconds, ttl_seconds);
    assert_eq!(parsed_token, token.as_ref());
    assert_eq!(parsed.encode(table_name), key);
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
        TtlIndexKey::parse(&encoded, &TtlIndexKey::prefix(&table_info.table_name)).expect("parse");
    let parsed_parts: (i64, String) = parsed.into();
    let expected_parts: (i64, String) = ttl_index_key.into();

    assert_eq!(parsed_parts.0, 1_700_000_000);
    assert_eq!(parsed_parts.1, expected_parts.1);
}

#[test]
fn ttl_config_round_trip_is_strict_and_old_shape_is_rejected() {
    let config = make_config();
    let encoded = storage_types::storage_serde::to_bytes(&config).expect("encode config");
    let decoded: TtlConfigRecord =
        storage_types::storage_serde::from_bytes(&encoded).expect("decode config");
    assert_eq!(decoded.attribute_name, config.attribute_name);
    assert_eq!(u64::from(decoded.policy_generation), 1);
    assert!(decoded.checkpoint.cursor.is_none());

    #[derive(Serialize)]
    struct LegacyTtlConfig {
        attribute_name: String,
        status: TimeToLiveStatus,
        gsi_name: String,
        created_at: TimestampMillis,
        updated_at: TimestampMillis,
    }
    let legacy = LegacyTtlConfig {
        attribute_name: "ttl_attr".to_string(),
        status: TimeToLiveStatus::Enabled,
        gsi_name: "legacy-ttl-index".to_string(),
        created_at: TimestampMillis::from_timestamp(1_700_000_000_000),
        updated_at: TimestampMillis::from_timestamp(1_700_000_000_001),
    };
    let legacy_bytes = storage_types::storage_serde::to_bytes(&legacy).expect("encode legacy");
    assert!(storage_types::storage_serde::from_bytes::<TtlConfigRecord>(&legacy_bytes).is_err());
}

#[test]
fn enabling_ttl_policy_activates_for_its_hidden_index() {
    let config = make_config();
    let index_name = config.gsi_name.clone();
    let activated_at =
        TimestampMillis::from_timestamp(config.updated_at.timestamp_millis().saturating_add(1));
    let activation = config
        .activate_for_index(TableName::new("TestTable"), &index_name, activated_at)
        .expect("matching enabling policy should activate");

    assert!(activation.expected.status == TimeToLiveStatus::Enabling);
    assert!(activation.activated.status == TimeToLiveStatus::Enabled);
    assert!(activation.activated.updated_at == activated_at);
    assert!(activation.expected.policy_generation == activation.activated.policy_generation);
}

#[test]
fn ttl_policy_activation_rejects_wrong_index_state_and_time() {
    let config = make_config();
    let index_name = config.gsi_name.clone();
    let earlier =
        TimestampMillis::from_timestamp(config.updated_at.timestamp_millis().saturating_sub(1));
    assert!(
        config
            .clone()
            .activate_for_index(
                TableName::new("TestTable"),
                &IndexName::new("other"),
                config.updated_at,
            )
            .is_err()
    );
    assert!(
        config
            .clone()
            .activate_for_index(TableName::new("TestTable"), &index_name, earlier)
            .is_err()
    );
    let enabled = TtlConfigRecord::try_new(
        "ttl_attr".to_string(),
        &TableName::new("TestTable"),
        TimeToLiveStatus::Enabled,
    )
    .expect("valid enabled policy");
    assert!(
        enabled
            .clone()
            .activate_for_index(
                TableName::new("TestTable"),
                &enabled.gsi_name,
                enabled.updated_at,
            )
            .is_err()
    );
}

#[test]
fn ttl_cursor_serialization_rejects_unknown_fields() {
    let cursor = TtlMaintenanceCursor::try_from("opaque-token").unwrap();
    let checkpoint = TtlMaintenanceCheckpoint {
        cursor: Some(cursor),
    };
    let encoded = storage_types::storage_serde::to_bytes(&checkpoint).expect("encode checkpoint");
    let decoded: TtlMaintenanceCheckpoint =
        storage_types::storage_serde::from_bytes(&encoded).expect("decode checkpoint");
    assert_eq!(decoded.cursor.as_ref().unwrap().token, "opaque-token");

    #[derive(Serialize)]
    struct UnknownCursor {
        token: String,
        extra: bool,
    }
    let unknown = UnknownCursor {
        token: "opaque-token".to_string(),
        extra: true,
    };
    let unknown_bytes = storage_types::storage_serde::to_bytes(&unknown).expect("encode unknown");
    assert!(
        storage_types::storage_serde::from_bytes::<TtlMaintenanceCursor>(&unknown_bytes).is_err()
    );
}

#[test]
fn given_ttl_tokens_when_encoding_then_persisted_bytes_and_normalization_are_exact() {
    let table = TableName::new("Token /表.Table-_");
    for timestamp in [i64::MIN, -1, 0, 1, 1_700_000_000, i64::MAX] {
        for token in ["a".to_string(), "token/é\0".repeat(128)] {
            let expected =
                format!("__ttl-index/Token.Table-_/{:020}/{token}", timestamp.max(0)).into_bytes();
            assert_eq!(
                TtlIndexKey::new(timestamp, token.clone().into()).encode(&table),
                expected
            );
        }
    }
}

#[test]
fn given_invalid_ttl_tokens_when_decoding_then_errors_are_domain_scoped() {
    let table = make_table_info();
    for token in ["", "not a token", "AA", "____", "é"] {
        let error = TtlIndexKeyToken::from(token.to_string())
            .parse_key_map(&table)
            .unwrap_err();
        let Some(storage_types::StorageEnum::InternalServerError { message }) = error.base_error()
        else {
            panic!("expected internal token decode error")
        };
        assert!(message.starts_with("ttl index token"));
    }
    let item = HashMap::from([("pk".to_string(), AttributeValue::S("hash".to_string()))]);
    let error = TtlIndexKeyToken::from_item(&table, &item)
        .err()
        .expect("invalid token must fail");
    assert!(
        matches!(error.base_error(), Some(storage_types::StorageEnum::InternalServerError { message }) if message.starts_with("ttl index token"))
    );
}

#[test]
fn given_absent_or_unparseable_ttl_when_building_keys_then_map_and_wire_skip_indexing() {
    for value in [
        None,
        Some(AttributeValue::S("text".to_string())),
        Some(AttributeValue::N("invalid".to_string())),
    ] {
        let table = make_table_info();
        let mut item = HashMap::from([
            ("pk".to_string(), AttributeValue::S("hash".to_string())),
            ("sk".to_string(), AttributeValue::S("range".to_string())),
        ]);
        if let Some(value) = value {
            item.insert("ttl".to_string(), value);
        }
        let wire = storage_types::WireItem::try_from(&item).unwrap();
        assert!(
            TtlIndexKey::for_item(&table, "ttl", &item)
                .unwrap()
                .is_none()
        );
        assert!(
            TtlIndexKey::for_wire_item(&table, "ttl", &wire)
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn given_changed_wire_ttl_when_planning_index_writes_then_delete_precedes_upsert() {
    let table = make_table_info();
    let config = TtlConfigRecord::try_new(
        "ttl".to_string(),
        &table.table_name,
        TimeToLiveStatus::Enabling,
    )
    .expect("valid TTL config");
    let old_item = storage_types::WireItem::try_from(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("hash".to_string())),
        ("sk".to_string(), AttributeValue::S("range".to_string())),
        ("ttl".to_string(), AttributeValue::N("10".to_string())),
    ]))
    .expect("old wire item");
    let new_item = storage_types::WireItem::try_from(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("hash".to_string())),
        ("sk".to_string(), AttributeValue::S("range".to_string())),
        ("ttl".to_string(), AttributeValue::N("20".to_string())),
    ]))
    .expect("new wire item");
    let mut writes =
        TtlIndexWriteBatch::for_wire_items(&table, Some(&config), Some(&old_item), Some(&new_item))
            .expect("TTL write plan")
            .into_iter();
    let Some(TtlIndexWrite::Delete(old_key)) = writes.next() else {
        panic!("first TTL write must delete the old key");
    };
    let Some(TtlIndexWrite::Upsert(new_key)) = writes.next() else {
        panic!("second TTL write must upsert the new key");
    };
    let (old_ttl, old_token): (i64, String) = old_key.into();
    let (new_ttl, new_token): (i64, String) = new_key.into();
    assert_eq!((old_ttl, new_ttl), (10, 20));
    assert_eq!(old_token, new_token);
    assert!(writes.next().is_none());
}

#[test]
fn given_changed_attribute_ttl_when_planning_index_writes_then_delete_precedes_upsert() {
    let table = make_table_info();
    let config = TtlConfigRecord::try_new(
        "ttl".to_string(),
        &table.table_name,
        TimeToLiveStatus::Enabling,
    )
    .expect("valid TTL config");
    let old_item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("hash".to_string())),
        ("sk".to_string(), AttributeValue::S("range".to_string())),
        ("ttl".to_string(), AttributeValue::N("10".to_string())),
    ]);
    let new_item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("hash".to_string())),
        ("sk".to_string(), AttributeValue::S("range".to_string())),
        ("ttl".to_string(), AttributeValue::N("20".to_string())),
    ]);
    let mut writes =
        TtlIndexWriteBatch::for_items(&table, Some(&config), Some(&old_item), Some(&new_item))
            .expect("TTL write plan")
            .into_iter();
    let Some(TtlIndexWrite::Delete(old_key)) = writes.next() else {
        panic!("first TTL write must delete the old key");
    };
    let Some(TtlIndexWrite::Upsert(new_key)) = writes.next() else {
        panic!("second TTL write must upsert the new key");
    };
    let (old_ttl, old_token): (i64, String) = old_key.into();
    let (new_ttl, new_token): (i64, String) = new_key.into();
    assert_eq!((old_ttl, new_ttl), (10, 20));
    assert_eq!(old_token, new_token);
    assert!(writes.next().is_none());
}

#[test]
fn given_same_attribute_ttl_when_planning_index_writes_then_plan_is_empty() {
    let table = make_table_info();
    let config = TtlConfigRecord::try_new(
        "ttl".to_string(),
        &table.table_name,
        TimeToLiveStatus::Enabled,
    )
    .expect("valid TTL config");
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("hash".to_string())),
        ("sk".to_string(), AttributeValue::S("range".to_string())),
        ("ttl".to_string(), AttributeValue::N("10".to_string())),
    ]);
    let batch = TtlIndexWriteBatch::for_items(&table, Some(&config), Some(&item), Some(&item))
        .expect("TTL write plan");
    assert!(batch.is_empty());
    let batch =
        TtlIndexWriteBatch::for_items(&table, None, Some(&item), None).expect("TTL write plan");
    assert!(batch.is_empty());
}

#[test]
fn given_ttl_token_workloads_when_measuring_then_all_owner_paths_complete() {
    use std::{hint::black_box, time::Instant};

    const ITERATIONS: usize = 256;
    let table = make_table_info();
    for key_size in [1, 100, 1023] {
        let mut item = HashMap::from([
            ("pk".to_string(), AttributeValue::S("h".repeat(key_size))),
            ("sk".to_string(), AttributeValue::S("r".repeat(key_size))),
            (
                "ttl".to_string(),
                AttributeValue::N("1700000000".to_string()),
            ),
        ]);
        for index in 0..7 {
            item.insert(
                format!("payload{index}"),
                AttributeValue::S("x".repeat(160)),
            );
        }
        let wire = storage_types::WireItem::try_from(&item).unwrap();
        let token = TtlIndexKeyToken::from_item(&table, &item).unwrap();
        let typed = TtlIndexKey::new(1_700_000_000, token.clone());
        let expected_key = typed.encode(&table.table_name);
        let key_map = token.parse_key_map(&table).unwrap();
        assert_eq!(key_map.get("pk"), item.get("pk"));
        assert_eq!(key_map.get("sk"), item.get("sk"));
        let wire_token = TtlIndexKeyToken::from_wire_item(&table, &wire).unwrap();
        assert!(wire_token == token);
        assert_eq!(
            TtlIndexKey::for_wire_item(&table, "ttl", &wire)
                .unwrap()
                .map(|key| key.encode(&table.table_name)),
            Some(expected_key)
        );
        for operation in [
            "encode",
            "token_item",
            "token_wire",
            "decode",
            "wire_ttl_key",
        ] {
            let guard = alloc_counter::AllocationGuard::start(
                module_path!(),
                "ttl_token_owner_path",
                file!(),
                line!(),
                Some(operation),
            );
            let started = Instant::now();
            for _ in 0..ITERATIONS {
                match operation {
                    "encode" => {
                        black_box(typed.encode(&table.table_name));
                    }
                    "token_item" => {
                        black_box(TtlIndexKeyToken::from_item(&table, &item).unwrap());
                    }
                    "token_wire" => {
                        black_box(TtlIndexKeyToken::from_wire_item(&table, &wire).unwrap());
                    }
                    "decode" => {
                        black_box(token.parse_key_map(&table).unwrap());
                    }
                    "wire_ttl_key" => {
                        black_box(
                            TtlIndexKey::for_wire_item(&table, "ttl", &wire)
                                .unwrap()
                                .map(|key| key.encode(&table.table_name)),
                        );
                    }
                    _ => unreachable!(),
                }
            }
            let elapsed_ns = started.elapsed().as_nanos();
            let report = guard.finish();
            println!(
                "ttl_token_owner_path operation={operation} key_size={key_size} \
                 iterations={ITERATIONS} allocations={} bytes={} elapsed_ns={elapsed_ns}",
                report.allocation_count, report.allocated_bytes
            );
            let _ = alloc_counter::emit_report(&report);
        }
    }
}

#[test]
fn given_prepared_new_ttl_when_old_image_is_observed_then_keys_match_and_policy_changes_conflict() {
    let table = make_table_info();
    let config = TtlConfigRecord::try_new(
        "ttl".to_owned(),
        &table.table_name,
        TimeToLiveStatus::Enabled,
    )
    .unwrap();
    let image = |ttl: Option<&str>| {
        let mut attributes = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("hash".to_owned())),
            ("sk".to_owned(), AttributeValue::S("range".to_owned())),
        ]);
        if let Some(ttl) = ttl {
            attributes.insert("ttl".to_owned(), AttributeValue::N(ttl.to_owned()));
        }
        storage_types::WireItem::try_from(&attributes).unwrap()
    };
    let keys = |writes: TtlIndexWriteBatch| {
        writes
            .into_iter()
            .map(|write| match write {
                TtlIndexWrite::Delete(key) => (false, <(i64, String)>::from(key)),
                TtlIndexWrite::Upsert(key) => (true, <(i64, String)>::from(key)),
            })
            .collect::<Vec<_>>()
    };
    for new in [None, Some("10"), Some("20"), Some("invalid")] {
        let new = image(new);
        let prepared = crate::PreparedTtlPut::try_new(&table, Some(&config), &new).unwrap();
        for old in [
            None,
            Some(image(None)),
            Some(image(Some("10"))),
            Some(image(Some("20"))),
        ] {
            let expected =
                TtlIndexWriteBatch::for_wire_items(&table, Some(&config), old.as_ref(), Some(&new))
                    .unwrap();
            assert_eq!(
                keys(prepared.finish(Some(&config), old.as_ref()).unwrap()),
                keys(expected)
            );
        }
        let mut changed = config.clone();
        changed.policy_generation = changed.policy_generation.next().unwrap();
        assert!(prepared.finish(Some(&changed), None).is_err());
        changed = config.clone();
        changed.attribute_name = "other_ttl".to_owned();
        assert!(prepared.finish(Some(&changed), None).is_err());
        assert!(prepared.finish(None, None).is_err());
    }
    let disabled = crate::PreparedTtlPut::try_new(&table, None, &image(Some("10"))).unwrap();
    assert!(
        disabled
            .finish(None, Some(&image(Some("20"))))
            .unwrap()
            .is_empty()
    );
    assert!(disabled.finish(Some(&config), None).is_err());
}
