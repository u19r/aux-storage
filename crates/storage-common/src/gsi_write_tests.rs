use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, StoredTableInfo, TableName, TableStatus,
    TimestampMillis,
};

use crate::{GsiWriteAction, plan_gsi_write_actions};

#[test]
fn keys_only_update_without_key_changes_skips_gsi_write() {
    let table = table_info(Projection {
        projection_type: Some(ProjectionType::KeysOnly),
        non_key_attributes: None,
    });
    let old = item("payload-a", "stable");
    let new = item("payload-b", "stable");

    let actions = plan_gsi_write_actions(&table, Some(&old), Some(&new)).unwrap();

    assert!(actions.is_empty());
}

#[test]
fn include_update_for_projected_attribute_writes_gsi() {
    let table = table_info(Projection {
        projection_type: Some(ProjectionType::Include),
        non_key_attributes: Some(vec!["included".to_string()]),
    });
    let old = item("payload-a", "old");
    let new = item("payload-b", "new");

    let actions = plan_gsi_write_actions(&table, Some(&old), Some(&new)).unwrap();

    assert_eq!(actions.len(), 1);
    assert!(matches!(actions[0], GsiWriteAction::Put { .. }));
}

fn table_info(projection: Projection) -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("table"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![attr("pk"), attr("sk"), attr("gsi_pk"), attr("gsi_sk")],
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("gsi"),
            key_schema: vec![key("gsi_pk", KeyType::Hash), key("gsi_sk", KeyType::Range)],
            projection,
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
    }
}

fn item(payload: &str, included: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S("pk".to_string())),
        ("sk".to_string(), AttributeValue::S("sk".to_string())),
        ("gsi_pk".to_string(), AttributeValue::S("gpk".to_string())),
        ("gsi_sk".to_string(), AttributeValue::S("gsk".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S(payload.to_string()),
        ),
        (
            "included".to_string(),
            AttributeValue::S(included.to_string()),
        ),
    ])
}

fn attr(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type: KeyAttributeType::S,
    }
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type,
    }
}
