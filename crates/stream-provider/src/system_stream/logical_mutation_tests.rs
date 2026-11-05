use std::collections::{BTreeMap, HashMap};

use serde::Serialize;
use storage_types::{
    AttributeMap, AttributeValue, ItemStreamVersion, KeyAttributes, StoredTableInfo,
    SystemStreamSourceTableName, TableName, TimestampMillis,
};

use super::logical_mutation::{
    LogicalSystemStreamMutation, LogicalSystemStreamMutationInput, canonical_route_key,
};

#[derive(Serialize)]
struct ExpectedRouteEvidence {
    source_table_name: SystemStreamSourceTableName,
    key: BTreeMap<String, AttributeValue>,
}

fn route_fixture() -> (
    SystemStreamSourceTableName,
    Vec<storage_types::KeySchemaElement>,
    HashMap<String, AttributeValue>,
) {
    let table_name = TableName::new("orders");
    let source_table_name =
        SystemStreamSourceTableName::try_from(&table_name).expect("source table name");
    let key_schema = vec![
        storage_types::KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: storage_types::KeyType::Hash,
        },
        storage_types::KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: storage_types::KeyType::Range,
        },
    ];
    let item = HashMap::from([
        (
            "sk".to_string(),
            AttributeValue::S("quote\"and\\slash".to_string()),
        ),
        ("payload".to_string(), AttributeValue::B("AAH/".to_string())),
        ("pk".to_string(), AttributeValue::S("tenant".to_string())),
    ]);
    (source_table_name, key_schema, item)
}

fn canonical_key_for_test(
    attributes: &HashMap<String, AttributeValue>,
    key_schema: &[storage_types::KeySchemaElement],
) -> BTreeMap<String, AttributeValue> {
    key_schema
        .iter()
        .filter_map(|key| {
            attributes
                .get(&key.attribute_name)
                .cloned()
                .map(|value| (key.attribute_name.clone(), value))
        })
        .collect()
}

#[test]
fn given_key_schema_when_canonical_route_then_borrowed_bytes_match_owned_encoding() {
    let (source_table_name, key_schema, item) = route_fixture();
    let actual_route =
        canonical_route_key(&source_table_name, &key_schema, &item).expect("canonical route");
    let expected_route = storage_types::storage_serde::to_bytes(&ExpectedRouteEvidence {
        source_table_name,
        key: canonical_key_for_test(&item, &key_schema),
    })
    .expect("owned canonical route");
    assert_eq!(actual_route, expected_route);
}

fn mutation_fixture() -> (StoredTableInfo, storage_types::WireItem, KeyAttributes) {
    let table_name = TableName::new("orders");
    let table_info = StoredTableInfo {
        table_name: table_name.clone(),
        table_status: storage_types::TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        attribute_definitions: vec![storage_types::AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: storage_types::KeyAttributeType::S,
        }],
        key_schema: vec![storage_types::KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: storage_types::KeyType::Hash,
        }],
        max_indexers: storage_types::MaxIndexers::ZERO,
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration:
            storage_types::StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        deletion_protection_enabled: false,
    };
    let item: AttributeMap =
        HashMap::from([("pk".to_string(), AttributeValue::S("tenant".to_string()))]).into();
    let key_attributes =
        KeyAttributes::from_iter([("pk".to_string(), AttributeValue::S("tenant".to_string()))]);
    (
        table_info,
        storage_types::WireItem::try_from(item).unwrap(),
        key_attributes,
    )
}

#[test]
fn given_protected_old_revision_when_mutation_is_prepared_then_exact_predecessor_is_required() {
    let (table, item, key) = mutation_fixture();
    let route = super::logical_mutation::DynamoMutationRoute::try_new(
        &table.table_name,
        &table.key_schema,
        &key,
    )
    .unwrap();
    let old = route.verify_item(item.clone()).unwrap();
    for (has_old, previous, accepted) in [
        (false, None, true),
        (false, Some(4), false),
        (true, None, false),
        (true, Some(0), false),
        (true, Some(5), false),
        (true, Some(6), false),
        (true, Some(4), true),
    ] {
        let result = LogicalSystemStreamMutation::try_from(LogicalSystemStreamMutationInput {
            history_id: 1_u32.into(),
            route: &route,
            item: Some(route.verify_item(item.clone()).unwrap()),
            old_item: has_old.then_some(&old),
            previous_version: previous.map(ItemStreamVersion::new),
            indexers: &[],
            item_stream_version: ItemStreamVersion::new(5),
            created_at: TimestampMillis::from_timestamp(0),
            user_identity: None,
        });
        assert_eq!(result.is_ok(), accepted);
        if let Ok(mutation) = result {
            assert_eq!(
                mutation.previous_version,
                previous.map(ItemStreamVersion::new)
            );
            assert_eq!(
                mutation.predecessor_payload_bytes,
                has_old.then(|| old.dynamo_json_bytes().unwrap().len())
            );
            assert_eq!(
                mutation.item.unwrap().dynamo_json_bytes().unwrap(),
                item.dynamo_json_bytes().unwrap()
            );
        }
    }
}

#[test]
fn given_delete_without_a_live_predecessor_when_prepared_then_no_history_is_invented() {
    let (table, _, key) = mutation_fixture();
    let route = super::logical_mutation::DynamoMutationRoute::try_new(
        &table.table_name,
        &table.key_schema,
        &key,
    )
    .unwrap();
    assert!(
        LogicalSystemStreamMutation::try_from(LogicalSystemStreamMutationInput {
            history_id: 1_u32.into(),
            route: &route,
            item: None,
            old_item: None,
            previous_version: None,
            indexers: &[],
            item_stream_version: ItemStreamVersion::new(1),
            created_at: TimestampMillis::from_timestamp(0),
            user_identity: None,
        })
        .is_err()
    );
}
