use std::sync::Arc;

use storage_provider::{RangeReadPlan, RangeReadiness};
use storage_types::{
    AttributeDefinition, AttributeMap, AttributeValue, BoundTableInfo, ItemKey, KeyAttributeType,
    KeySchemaElement, KeyType, ScanTableRequest, StoredTableInfo, TableName, TableStatus,
    TimestampMillis, WireItem,
};

use super::decode::range_evidence;
use crate::{catalog::KvTableIdentity, constants::PRIMARY_ITEM_KEY_PREFIX_BYTES};

pub(super) fn table() -> BoundTableInfo {
    BoundTableInfo {
        history_id: uuid::Uuid::from_bytes([1; 16]).into(),
        info: Arc::new(StoredTableInfo {
            table_name: TableName::new("items"),
            table_status: TableStatus::Active,
            created_at: TimestampMillis::now(),
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: KeyAttributeType::S,
            }],
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            }],
            max_indexers: storage_types::MaxIndexers::ZERO,
            global_secondary_indexes: None,
            table_size_bytes: 0,
            item_count: 0,
            stream_specification: None,
            table_stream_duration: Default::default(),
            default_item_stream_duration: Default::default(),
            deletion_protection_enabled: false,
        }),
    }
}

#[test]
fn given_a_physical_primary_key_when_range_image_disagrees_then_content_is_rejected() {
    let table = table();
    let plan = RangeReadPlan::try_from_scan(ScanTableRequest {
        table_name: table.table_name.clone(),
        index_name: None,
        limit: None,
        exclusive_start_key: None,
        consistent_read: true,
    })
    .unwrap();
    let bound = plan
        .bind(table.info.clone(), RangeReadiness::BaseTable)
        .unwrap();
    let key = ItemKey::from_key_schema(
        table.table_name.clone(),
        &table.key_schema,
        &AttributeMap::from_iter([("pk".into(), AttributeValue::S("owned".into()))]),
    )
    .unwrap();
    let identity = KvTableIdentity {
        table_id: 1,
        revision: std::num::NonZeroU64::new(1).unwrap(),
        table_name: table.table_name.clone(),
        indexes: Vec::new(),
        index_id_high_water: 0,
        deleted: false,
    };
    let physical = super::key_layout::physical_item_key(&identity, &table, &bound, &key).unwrap();
    let valid = WireItem::dynamo_json(br#"{"pk":{"S":"owned"}}"#.to_vec());
    assert!(range_evidence(&table, &bound, &physical, &valid).is_ok());
    for bytes in [
        br#"{"pk":{"S":"foreign"}}"#.as_slice(),
        br#"{"marker":{"S":"missing key"}}"#,
    ] {
        let image = WireItem::dynamo_json(bytes.to_vec());
        assert!(range_evidence(&table, &bound, &physical, &image).is_err());
    }
    assert!(
        range_evidence(
            &table,
            &bound,
            &physical[..PRIMARY_ITEM_KEY_PREFIX_BYTES - 1],
            &valid
        )
        .is_err()
    );
}
