use std::{collections::HashMap, sync::Arc};

use storage_common::ttl::{self, TtlConfigRecord};
use storage_provider::UpdateOperation;
use storage_types::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeySchemaElement, KeyType,
    StoredTableInfo, TableName, TableStatus, TimeToLiveStatus, TimestampMillis,
};

use crate::{
    backends::common::{KvMutation, plan_table_write},
    sorted_kv_store::TransactWriteTableOperation,
};

#[test]
fn plan_table_put_includes_ttl_index_mutation_in_same_plan() {
    let table_info = ttl_table_info("ttl_put_plan");
    let ttl_config = ttl_config(&table_info.table_name);
    let new_item = item_with_ttl("pk", "sk", "1700000000");
    let ttl_key =
        ttl::ttl_index_key_for_item(&table_info.table_name, &table_info, "ttl", &new_item)
            .expect("ttl key result")
            .expect("ttl key");

    let plan = plan_table_write(
        &[TransactWriteTableOperation::Put {
            table_info,
            item: new_item,
            condition: None,
            return_values_on_condition_check_failure: None,
            replication: None,
            ttl_config: Some(ttl_config),
        }],
        vec![None],
        &[None],
        false,
    )
    .expect("plan table put");

    assert_eq!(plan.mutations.len(), 2);
    assert!(matches!(plan.mutations[0], KvMutation::Put { .. }));
    assert!(matches!(
        &plan.mutations[1],
        KvMutation::Put { key, value } if key == &ttl_key && value.is_empty()
    ));
}

#[test]
fn plan_table_update_replaces_ttl_index_mutations_in_same_plan() {
    let table_info = ttl_table_info("ttl_update_plan");
    let ttl_config = ttl_config(&table_info.table_name);
    let old_item = item_with_ttl("pk", "sk", "1700000000");
    let new_ttl = AttributeValue::N("1800000000".to_string());
    let new_item = item_with_ttl("pk", "sk", "1800000000");
    let old_ttl_key =
        ttl::ttl_index_key_for_item(&table_info.table_name, &table_info, "ttl", &old_item)
            .expect("old ttl key result")
            .expect("old ttl key");
    let new_ttl_key =
        ttl::ttl_index_key_for_item(&table_info.table_name, &table_info, "ttl", &new_item)
            .expect("new ttl key result")
            .expect("new ttl key");
    let current_bytes =
        storage_types::storage_serde::to_bytes(&old_item).expect("serialize current item");

    let plan = plan_table_write(
        &[TransactWriteTableOperation::Update {
            table_info,
            key: key_attrs("pk", "sk").into(),
            operations: Arc::from([UpdateOperation::Set {
                field: "ttl".to_string().into(),
                value: new_ttl,
            }]),
            condition: None,
            replication: None,
            preserve_old_item: false,
            transaction_validation: false,
            ttl_config: Some(ttl_config),
        }],
        vec![Some(current_bytes)],
        &[None],
        false,
    )
    .expect("plan table update");

    assert_eq!(plan.mutations.len(), 3);
    assert!(matches!(plan.mutations[0], KvMutation::Put { .. }));
    assert!(matches!(
        &plan.mutations[1],
        KvMutation::Delete { key } if key == &old_ttl_key
    ));
    assert!(matches!(
        &plan.mutations[2],
        KvMutation::Put { key, value } if key == &new_ttl_key && value.is_empty()
    ));
}

fn ttl_config(table_name: &TableName) -> TtlConfigRecord {
    TtlConfigRecord::new(
        "ttl".to_string(),
        &ttl::ttl_gsi_name(table_name),
        TimeToLiveStatus::Enabled,
    )
}

fn ttl_table_info(name: &str) -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new(name),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![
            attr("pk", KeyAttributeType::S),
            attr("sk", KeyAttributeType::S),
        ],
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
    }
}

fn item_with_ttl(pk: &str, sk: &str, ttl_value: &str) -> HashMap<String, AttributeValue> {
    let mut item = key_attrs(pk, sk);
    item.insert("ttl".to_string(), AttributeValue::N(ttl_value.to_string()));
    item
}

fn key_attrs(pk: &str, sk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
    ])
}

fn attr(name: &str, attribute_type: KeyAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type,
    }
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type,
    }
}
