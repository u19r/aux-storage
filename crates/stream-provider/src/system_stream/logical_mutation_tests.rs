use std::collections::{BTreeMap, HashMap};

use serde::Serialize;
use storage_types::{
    AttributeMap, AttributeValue, ItemStreamVersion, KeyAttributes, StorageError, StoredTableInfo,
    StreamItemId, SystemStreamSourceTableName, TableName, TimestampMillis,
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
        default_item_stream_duration:
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
fn given_logical_inputs_when_try_new_then_effect_identity_is_stable() {
    let (table_info, item, key_attributes) = mutation_fixture();
    let first = LogicalSystemStreamMutation::from(LogicalSystemStreamMutationInput {
        history_id: 1u32.into(),
        route: &super::logical_mutation::DynamoMutationRoute::try_new(
            &table_info.table_name,
            &table_info.key_schema,
            &key_attributes,
        )
        .unwrap(),
        item: crate::PreparedStreamImage::try_new(
            &crate::DynamoMutationRoute::try_new(
                &table_info.table_name,
                &table_info.key_schema,
                &key_attributes,
            )
            .unwrap(),
            item.clone(),
        )
        .unwrap(),
        old_item: None,
        indexers: &[],
        old_indexers: None,
        stream_item_id: StreamItemId::from(ItemStreamVersion::new(7)),
        item_stream_version: ItemStreamVersion::new(7),
        is_deleted: false,

        created_at: TimestampMillis::from_timestamp(1_700_000_000_000),
    });
    let second = LogicalSystemStreamMutation::from(LogicalSystemStreamMutationInput {
        history_id: 1u32.into(),
        route: &super::logical_mutation::DynamoMutationRoute::try_new(
            &table_info.table_name,
            &table_info.key_schema,
            &key_attributes,
        )
        .unwrap(),
        item: crate::PreparedStreamImage::try_new(
            &crate::DynamoMutationRoute::try_new(
                &table_info.table_name,
                &table_info.key_schema,
                &key_attributes,
            )
            .unwrap(),
            item,
        )
        .unwrap(),
        old_item: None,
        indexers: &[],
        old_indexers: None,
        stream_item_id: StreamItemId::from(ItemStreamVersion::new(7)),
        item_stream_version: ItemStreamVersion::new(7),
        is_deleted: false,

        created_at: TimestampMillis::from_timestamp(1_700_000_000_000),
    });
    let position = storage_types::SystemStreamPosition::try_new("shard", "lineage", "1").unwrap();
    assert_eq!(
        crate::DynamoStreamEffectRow::for_write(first.clone(), position.clone())
            .unwrap()
            .1
            .effect_id,
        crate::DynamoStreamEffectRow::for_write(second.clone(), position)
            .unwrap()
            .1
            .effect_id,
    );
    assert_eq!(first.routing_key, second.routing_key);
}

#[test]
fn given_logical_mutation_when_expected_record_then_images_match_delete_flag() {
    let (table_info, item, key_attributes) = mutation_fixture();
    let mutation = LogicalSystemStreamMutation::from(LogicalSystemStreamMutationInput {
        history_id: 1u32.into(),
        route: &super::logical_mutation::DynamoMutationRoute::try_new(
            &table_info.table_name,
            &table_info.key_schema,
            &key_attributes,
        )
        .unwrap(),
        item: crate::PreparedStreamImage::try_new(
            &crate::DynamoMutationRoute::try_new(
                &table_info.table_name,
                &table_info.key_schema,
                &key_attributes,
            )
            .unwrap(),
            item,
        )
        .unwrap(),
        old_item: None,
        indexers: &[],
        old_indexers: None,
        stream_item_id: StreamItemId::from(ItemStreamVersion::new(9)),
        item_stream_version: ItemStreamVersion::new(9),
        is_deleted: false,

        created_at: TimestampMillis::from_timestamp(1_700_000_000_000),
    });
    let position = storage_types::SystemStreamPosition::try_new(
        "shard".to_string(),
        "lineage".to_string(),
        "00000000000000000001".to_string(),
    )
    .map_err(|error| StorageError::validation(error.to_string()))
    .expect("position");
    let (record, _) = crate::DynamoStreamEffectRow::for_write(mutation, position).unwrap();
    assert!(record.new_image.is_some());
    assert!(record.old_image.is_none());
}

#[test]
#[ignore = "manual allocation and elapsed-time probe of stream image preparation"]
fn given_wire_images_when_record_and_receipt_are_prepared_then_materialization_cost_is_measured() {
    use std::{hint::black_box, time::Instant};

    use alloc_counter::AllocationGuard;
    let (_, item, key) = mutation_fixture();
    let mut attributes = item.into_attribute_map().unwrap();
    for index in 0..14 {
        attributes.insert(format!("field{index}"), AttributeValue::S("x".repeat(64)));
    }
    let item = storage_types::WireItem::try_from(attributes).unwrap();
    let mutation = LogicalSystemStreamMutation {
        key,
        history_id: 1u32.into(),
        stream_item_id: StreamItemId::from(ItemStreamVersion::new(7)),
        source_table_name: SystemStreamSourceTableName::try_from("orders").unwrap(),
        item_json: item.dynamo_json_bytes().unwrap().into_owned(),
        old_source: None,
        item: item.clone(),
        old_item: Some(item),
        item_stream_version: ItemStreamVersion::new(7),
        indexers: Vec::new(),
        old_indexers: None,
        is_deleted: false,
        created_at: TimestampMillis::from_timestamp(123),
        routing_key: Vec::new(),
    };
    let position = storage_types::SystemStreamPosition::try_new("shard", "lineage", "1").unwrap();
    for label in ["map_image_materialization", "wire_record_and_trim_receipt"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "stream_image_preparation",
            file!(),
            line!(),
            Some(label),
        );
        let start = Instant::now();
        for _ in 0..1024 {
            if label == "map_image_materialization" {
                let item = mutation.item.to_attribute_map().unwrap();
                let old = mutation
                    .old_item
                    .as_ref()
                    .unwrap()
                    .to_attribute_map()
                    .unwrap();
                black_box(storage_types::storage_serde::to_bytes(&(&item, &old)).unwrap());
            } else {
                let (record, receipt) =
                    crate::DynamoStreamEffectRow::for_write(mutation.clone(), position.clone())
                        .unwrap();
                black_box(record);
                black_box(storage_types::storage_serde::to_bytes(&receipt).unwrap());
            }
        }
        let elapsed = start.elapsed().as_nanos();
        let report = guard.finish();
        println!(
            "{label}: allocations={} bytes={} elapsed_ns={elapsed} iterations=1024",
            report.allocation_count, report.allocated_bytes
        );
    }
}
