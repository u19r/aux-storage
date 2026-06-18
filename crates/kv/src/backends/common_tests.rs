use std::{collections::HashMap, sync::Arc};

use storage_common::ttl::TtlConfigRecord;
use storage_condition::parse_condition_expression;
use storage_provider::UpdateOperation;
use storage_types::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeySchemaElement, KeyType, StorageEnum,
    StoredTableInfo, TableName, TableStatus, TimeToLiveStatus, TimestampMillis,
    context::WrappedError as _,
};

use crate::{
    backends::common::{KvMutation, plan_table_write},
    keyspace::{compact::TableStorageId, table_identity::TableIdentity},
    sorted_kv_store::TransactWriteTableOperation,
    ttl,
};

fn table_identity(table_info: &StoredTableInfo) -> TableIdentity {
    TableIdentity::new(
        TableStorageId::new(1),
        table_info.table_name.clone(),
        Vec::new(),
    )
}

#[test]
fn plan_table_put_includes_ttl_index_mutation_in_same_plan() {
    let table_info = ttl_table_info("ttl_put_plan");
    let table_identity = table_identity(&table_info);
    let ttl_config = ttl_config(&table_info.table_name);
    let new_item = item_with_ttl("pk", "sk", "1700000000");
    let ttl_key =
        crate::ttl::compact_ttl_index_key_for_item(&table_identity, &table_info, "ttl", &new_item)
            .expect("ttl key result")
            .expect("ttl key");

    let plan = plan_table_write(
        &[TransactWriteTableOperation::Put {
            table_identity,
            table_info,
            item: new_item,
            item_stream_ttl_hours: None,
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
    let table_identity = table_identity(&table_info);
    let ttl_config = ttl_config(&table_info.table_name);
    let old_item = item_with_ttl("pk", "sk", "1700000000");
    let new_ttl = AttributeValue::N("1800000000".to_string());
    let new_item = item_with_ttl("pk", "sk", "1800000000");
    let old_ttl_key =
        crate::ttl::compact_ttl_index_key_for_item(&table_identity, &table_info, "ttl", &old_item)
            .expect("old ttl key result")
            .expect("old ttl key");
    let new_ttl_key =
        crate::ttl::compact_ttl_index_key_for_item(&table_identity, &table_info, "ttl", &new_item)
            .expect("new ttl key result")
            .expect("new ttl key");
    let current_bytes =
        storage_types::storage_serde::to_bytes(&old_item).expect("serialize current item");

    let plan = plan_table_write(
        &[TransactWriteTableOperation::Update {
            table_identity,
            table_info,
            key: key_attrs("pk", "sk").into(),
            operations: Arc::from([UpdateOperation::Set {
                field: "ttl".to_string().into(),
                value: new_ttl,
            }]),
            item_stream_ttl_hours: None,
            condition: None,
            return_values_on_condition_check_failure: None,
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

#[test]
fn plan_table_delete_accepts_equivalent_scientific_number_key() {
    let table_info = number_table_info("number_delete_plan");
    let current = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::N(
                "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
                    .to_string(),
            ),
        ),
        ("sk".to_string(), AttributeValue::N("1".to_string())),
    ]);
    let current_bytes =
        storage_types::storage_serde::to_bytes(&current).expect("serialize current item");

    let plan = plan_table_write(
        &[TransactWriteTableOperation::Delete {
            table_identity: table_identity(&table_info),
            table_info,
            key: HashMap::from([
                ("pk".to_string(), AttributeValue::N("1E-130".to_string())),
                ("sk".to_string(), AttributeValue::N("1".to_string())),
            ])
            .into(),
            item_stream_ttl_hours: None,
            use_key_attributes_for_missing_item_condition: false,
            condition: None,
            return_values_on_condition_check_failure: None,
            replication: None,
            ttl_config: None,
        }],
        vec![Some(current_bytes)],
        &[None],
        false,
    )
    .expect("equivalent number key should match stored item");

    assert_eq!(plan.mutations.len(), 1);
    assert!(matches!(plan.mutations[0], KvMutation::Delete { .. }));
}

#[test]
fn plan_table_write_collects_cancellation_reasons_after_first_failure() {
    let table_info = ttl_table_info("transaction_reason_plan");
    let values = HashMap::from([(
        ":closed".to_string(),
        AttributeValue::S("closed".to_string()),
    )]);
    let condition = parse_condition_expression("status = :closed", None, Some(&values))
        .expect("condition expression");
    let current_one = item_with_status("pk", "sk-1", "open");
    let current_two = item_with_status("pk", "sk-2", "open");

    let error = plan_table_write(
        &[
            TransactWriteTableOperation::Check {
                table_identity: table_identity(&table_info),
                table_info: table_info.clone(),
                key: key_attrs("pk", "sk-1").into(),
                condition: condition.clone(),
                return_values_on_condition_check_failure: None,
            },
            TransactWriteTableOperation::Check {
                table_identity: table_identity(&table_info),
                table_info,
                key: key_attrs("pk", "sk-2").into(),
                condition,
                return_values_on_condition_check_failure: None,
            },
        ],
        vec![
            Some(storage_types::storage_serde::to_bytes(&current_one).expect("item one")),
            Some(storage_types::storage_serde::to_bytes(&current_two).expect("item two")),
        ],
        &[None, None],
        false,
    );
    let Err(error) = error else {
        panic!("transaction should cancel");
    };

    let StorageEnum::TransactionCanceled { reasons } = error.to_enum() else {
        panic!("expected transaction cancellation, got {error:?}");
    };
    assert_eq!(
        reasons,
        &vec![
            "ConditionalCheckFailed".to_string(),
            "ConditionalCheckFailed".to_string()
        ]
    );
}

#[test]
fn plan_table_write_rejects_duplicate_transaction_item_targets() {
    let table_info = ttl_table_info("duplicate_transaction_targets_plan");
    let result = plan_table_write(
        &[
            TransactWriteTableOperation::Put {
                table_identity: table_identity(&table_info),
                table_info: table_info.clone(),
                item: item_with_status("pk", "sk", "open"),
                item_stream_ttl_hours: None,
                condition: None,
                return_values_on_condition_check_failure: None,
                replication: None,
                ttl_config: None,
            },
            TransactWriteTableOperation::Delete {
                table_identity: table_identity(&table_info),
                table_info,
                key: key_attrs("pk", "sk").into(),
                item_stream_ttl_hours: None,
                use_key_attributes_for_missing_item_condition: false,
                condition: None,
                return_values_on_condition_check_failure: None,
                replication: None,
                ttl_config: None,
            },
        ],
        vec![None, None],
        &[None, None],
        false,
    );
    let Err(error) = result else {
        panic!("duplicate transaction item targets should fail preflight");
    };

    let StorageEnum::Validation { message } = error.to_enum() else {
        panic!("expected validation error, got {error:?}");
    };
    assert_eq!(
        message,
        "Transaction request cannot include multiple operations on one item"
    );
}

#[test]
fn plan_table_write_rejects_invalid_number_key_before_backend_encoding() {
    let table_info = number_table_info("invalid_transaction_number_key_plan");
    let result = plan_table_write(
        &[TransactWriteTableOperation::Delete {
            table_identity: table_identity(&table_info),
            table_info,
            key: HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::N("not-a-number".to_string()),
                ),
                ("sk".to_string(), AttributeValue::N("1".to_string())),
            ])
            .into(),
            item_stream_ttl_hours: None,
            use_key_attributes_for_missing_item_condition: false,
            condition: None,
            return_values_on_condition_check_failure: None,
            replication: None,
            ttl_config: None,
        }],
        vec![None],
        &[None],
        false,
    );
    let Err(error) = result else {
        panic!("invalid number key should fail before backend key encoding");
    };

    assert!(
        !matches!(error.to_enum(), StorageEnum::TransactionCanceled { .. }),
        "invalid transaction key should not be wrapped as cancellation"
    );
    let message = error.to_string();
    assert!(
        message.contains("The parameter cannot be converted to a numeric value"),
        "{message}"
    );
}

#[test]
fn plan_table_delete_condition_on_missing_item_does_not_see_synthetic_key() {
    let table_info = ttl_table_info("delete_missing_condition_plan");
    let condition =
        parse_condition_expression("attribute_exists(pk)", None, None).expect("condition");
    let result = plan_table_write(
        &[TransactWriteTableOperation::Delete {
            table_identity: table_identity(&table_info),
            table_info,
            key: key_attrs("pk", "sk-missing").into(),
            item_stream_ttl_hours: None,
            use_key_attributes_for_missing_item_condition: false,
            condition: Some(condition),
            return_values_on_condition_check_failure: Some("ALL_OLD".to_string()),
            replication: None,
            ttl_config: None,
        }],
        vec![None],
        &[None],
        false,
    );
    let Err(error) = result else {
        panic!("delete condition on missing item should fail");
    };

    let StorageEnum::TransactionCanceled { reasons } = error.to_enum() else {
        panic!("expected transaction cancellation, got {error:?}");
    };
    assert_eq!(reasons, &vec!["ConditionalCheckFailed".to_string()]);
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
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

fn number_table_info(name: &str) -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new(name),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![
            attr("pk", KeyAttributeType::N),
            attr("sk", KeyAttributeType::N),
        ],
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

fn item_with_ttl(pk: &str, sk: &str, ttl_value: &str) -> HashMap<String, AttributeValue> {
    let mut item = key_attrs(pk, sk);
    item.insert("ttl".to_string(), AttributeValue::N(ttl_value.to_string()));
    item
}

fn item_with_status(pk: &str, sk: &str, status: &str) -> HashMap<String, AttributeValue> {
    let mut item = key_attrs(pk, sk);
    item.insert("status".to_string(), AttributeValue::S(status.to_string()));
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
