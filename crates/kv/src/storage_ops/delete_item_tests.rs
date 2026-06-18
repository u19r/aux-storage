use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemRequest, CreateTableRequest,
    DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, DeleteRequest, ItemKey, KeyAttributeType,
    KeySchemaElement, KeyType, StorageEnum, StreamRetentionDuration, TableName, TimestampMillis,
    TransactDeleteRequest, TransactWriteItem, TransactWriteItemsRequest, WriteRequest,
};
use tracing::info;

use crate::{keyspace::table_keys, sorted_kv_store::SortedKvStore};

#[tokio::test]
#[tracing_test::traced_test]
async fn kv_delete_condition_injects_key_attributes() {
    let store = crate::RocksDbKvStore::new(crate::kv_support_tests::rocksdb_test_path("kv-delete"))
        .unwrap();
    let provider = crate::SortedKvDbStorageProvider::new(store);
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("delete_condition_table");
    let create = CreateTableRequest::new(
        table.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    );
    provider.create_table(&create).await.unwrap();

    let key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("P1".to_string())),
        ("sk".to_string(), AttributeValue::S("S1".to_string())),
    ]);
    let item_key = ItemKey::from_key_schema(table.clone(), &create.key_schema, &key).unwrap();
    let table_metadata = provider
        .get_table_identity_from_name(&table)
        .await
        .unwrap()
        .expect("table metadata");
    let item_key = table_keys::item_key(&table_metadata.identity, &item_key).unwrap();

    let mut stored_item = HashMap::new();
    stored_item.insert("value".to_string(), AttributeValue::S("old".to_string()));
    let stored_bytes = storage_types::storage_serde::to_bytes(&stored_item).unwrap();
    provider
        .kv_store
        .put(&item_key, &stored_bytes, None)
        .await
        .unwrap();

    let expression_attribute_names = HashMap::from([
        ("#pk".to_string(), "pk".to_string()),
        ("#sk".to_string(), "sk".to_string()),
    ]);
    let expression_attribute_values = HashMap::from([
        (":pk".to_string(), AttributeValue::S("P1".to_string())),
        (":sk".to_string(), AttributeValue::S("S1".to_string())),
    ]);

    let deleted_missing_no_condition = provider
        .delete_item(
            table.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("P2".to_string())),
                ("sk".to_string(), AttributeValue::S("S2".to_string())),
            ])
            .into(),
            None,
            None,
            None,
        )
        .await
        .unwrap()
        .expect("empty hash");

    info!(
        "deleted_missing_no_condition: {:?}",
        deleted_missing_no_condition
    );

    assert!(deleted_missing_no_condition.keys().len() == 0);

    let deleted_missing = provider
        .delete_item(
            table.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("P2".to_string())),
                ("sk".to_string(), AttributeValue::S("S2".to_string())),
            ])
            .into(),
            Some("#pk = :pk AND #sk = :sk".to_string()),
            Some(expression_attribute_names.clone()),
            Some(HashMap::from([
                (":pk".to_string(), AttributeValue::S("P2".to_string())),
                (":sk".to_string(), AttributeValue::S("S2".to_string())),
            ])),
        )
        .await
        .unwrap()
        .expect("empty hash");

    assert!(deleted_missing.keys().len() == 0);

    let deleted = provider
        .delete_item(
            table.clone(),
            key.clone().into(),
            Some("#pk = :pk AND #sk = :sk".to_string()),
            Some(expression_attribute_names),
            Some(expression_attribute_values),
        )
        .await
        .unwrap()
        .expect("expected deleted item");

    assert_eq!(
        deleted.get("pk"),
        Some(&AttributeValue::S("P1".to_string()))
    );
    assert_eq!(
        deleted.get("sk"),
        Some(&AttributeValue::S("S1".to_string()))
    );

    let remaining = provider
        .get_item_map(table.clone(), key.into(), true)
        .await
        .unwrap();
    assert!(remaining.is_none());
}

#[tokio::test]
async fn kv_delete_missing_item_with_condition_returns_empty_item() {
    let store = crate::RocksDbKvStore::new(crate::kv_support_tests::rocksdb_test_path("kv-delete"))
        .unwrap();
    let provider = crate::SortedKvDbStorageProvider::new(store);
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("delete_missing_condition_table");
    let create = CreateTableRequest::new(
        table.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    );
    provider.create_table(&create).await.unwrap();

    let key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("P1".to_string())),
        ("sk".to_string(), AttributeValue::S("S1".to_string())),
    ]);
    let expression_attribute_names = HashMap::from([
        ("#pk".to_string(), "pk".to_string()),
        ("#sk".to_string(), "sk".to_string()),
    ]);
    let expression_attribute_values = HashMap::from([
        (":pk".to_string(), AttributeValue::S("P1".to_string())),
        (":sk".to_string(), AttributeValue::S("S1".to_string())),
    ]);

    let deleted = provider
        .delete_item(
            table.clone(),
            key.clone().into(),
            Some("#pk = :pk AND #sk = :sk".to_string()),
            Some(expression_attribute_names),
            Some(expression_attribute_values),
        )
        .await
        .unwrap()
        .expect("expected empty deleted item");

    assert!(deleted.is_empty());
    let remaining = provider
        .get_item_map(table.clone(), key.into(), true)
        .await
        .unwrap();
    assert!(remaining.is_none());
}

#[tokio::test]
async fn kv_delete_condition_failure_returns_conditional_check_failed() {
    let store = crate::RocksDbKvStore::new(crate::kv_support_tests::rocksdb_test_path("kv-delete"))
        .unwrap();
    let provider = crate::SortedKvDbStorageProvider::new(store);
    provider.initialize_storage().await.expect("initialize");

    let table = TableName::new("delete_condition_failure_table");
    let create = CreateTableRequest::new(
        table.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );
    provider.create_table(&create).await.expect("create");

    provider
        .put_item(
            table.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("A".to_string())),
                ("state".to_string(), AttributeValue::S("open".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("seed item");

    let err = provider
        .delete_item(
            table,
            HashMap::from([("pk".to_string(), AttributeValue::S("A".to_string()))]).into(),
            Some("#state = :expected".to_string()),
            Some(HashMap::from([("#state".to_string(), "state".to_string())])),
            Some(HashMap::from([(
                ":expected".to_string(),
                AttributeValue::S("closed".to_string()),
            )])),
        )
        .await
        .expect_err("delete should fail condition");

    assert!(matches!(err.as_ref(), StorageEnum::ConditionalCheckFailed));
    assert_eq!(err.to_string(), DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE);
}

#[tokio::test]
async fn kv_delete_item_with_stream_ttl_writes_item_duration_marker() {
    let provider = provider_for_custom_duration_case("kv-delete-custom-duration");
    let table = TableName::new("delete_custom_duration_table");
    provider
        .create_table(&hash_table_request(table.clone()))
        .await
        .expect("create");
    provider
        .put_item(table.clone(), item("A", "open"), None, None, None, None)
        .await
        .expect("seed");

    provider
        .delete_item_with_stream_ttl(
            table.clone(),
            key("A"),
            None,
            None,
            None,
            Some(StreamRetentionDuration::FiniteHours(1)),
        )
        .await
        .expect("delete");

    assert_eq!(item_duration_marker_count(&provider, &table).await, 1);
}

#[tokio::test]
async fn kv_failed_conditional_delete_does_not_write_item_duration_marker() {
    let provider = provider_for_custom_duration_case("kv-delete-custom-duration-condition-failure");
    let table = TableName::new("delete_custom_duration_failure_table");
    provider
        .create_table(&hash_table_request(table.clone()))
        .await
        .expect("create");
    provider
        .put_item(table.clone(), item("A", "open"), None, None, None, None)
        .await
        .expect("seed");

    provider
        .delete_item_with_stream_ttl(
            table.clone(),
            key("A"),
            Some("#state = :expected".to_string()),
            Some(HashMap::from([("#state".to_string(), "state".to_string())])),
            Some(HashMap::from([(
                ":expected".to_string(),
                AttributeValue::S("closed".to_string()),
            )])),
            Some(StreamRetentionDuration::FiniteHours(1)),
        )
        .await
        .expect_err("delete should fail condition");

    assert_eq!(item_duration_marker_count(&provider, &table).await, 0);
}

#[tokio::test]
async fn kv_batch_write_delete_applies_item_stream_duration_marker() {
    let provider = provider_for_custom_duration_case("kv-batch-delete-custom-duration");
    let table = TableName::new("batch_delete_custom_duration_table");
    provider
        .create_table(&hash_table_request(table.clone()))
        .await
        .expect("create");
    provider
        .put_item(table.clone(), item("A", "open"), None, None, None, None)
        .await
        .expect("seed");

    provider
        .batch_write_item(
            BatchWriteItemRequest {
                request_items: HashMap::from([(
                    table.clone(),
                    vec![WriteRequest {
                        put_request: None,
                        delete_request: Some(DeleteRequest {
                            key: key("A"),
                            aux_item_stream_ttl_hours: Some(StreamRetentionDuration::FiniteHours(
                                2,
                            )),
                        }),
                    }],
                )]),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            },
            true,
        )
        .await
        .expect("batch write");

    assert_eq!(item_duration_marker_count(&provider, &table).await, 1);
}

#[tokio::test]
async fn kv_transact_delete_applies_item_stream_duration_marker() {
    let provider = provider_for_custom_duration_case("kv-transact-delete-custom-duration");
    let table = TableName::new("transact_delete_custom_duration_table");
    provider
        .create_table(&hash_table_request(table.clone()))
        .await
        .expect("create");
    provider
        .put_item(table.clone(), item("A", "open"), None, None, None, None)
        .await
        .expect("seed");

    provider
        .transact_write_items(TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                put: None,
                update: None,
                delete: Some(TransactDeleteRequest {
                    table_name: table.clone(),
                    key: key("A"),
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: Some(StreamRetentionDuration::FiniteHours(3)),
                }),
                condition_check: None,
            }],
            client_request_token: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .await
        .expect("transaction");

    assert_eq!(item_duration_marker_count(&provider, &table).await, 1);
}

async fn item_duration_marker_count(
    provider: &crate::SortedKvDbStorageProvider<crate::RocksDbKvStore>,
    table: &TableName,
) -> usize {
    provider
        .list_due_stream_trim_markers(TimestampMillis::now() + 73 * 60 * 60 * 1000, 25)
        .await
        .expect("list markers")
        .into_iter()
        .filter(|marker| {
            marker.scope.table_name == *table
                && marker.scope.kind == storage_provider::StreamTrimScopeKind::Item
        })
        .count()
}

fn provider_for_custom_duration_case(
    label: &str,
) -> crate::SortedKvDbStorageProvider<crate::RocksDbKvStore> {
    let store = crate::RocksDbKvStore::new(crate::kv_support_tests::rocksdb_test_path(label))
        .expect("rocksdb store");
    crate::SortedKvDbStorageProvider::new(store)
}

fn hash_table_request(table: TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table,
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    )
}

fn item(pk: &str, state: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("state".to_string(), AttributeValue::S(state.to_string())),
    ])
}

fn key(pk: &str) -> storage_types::KeyAttributes {
    HashMap::from([("pk".to_string(), AttributeValue::S(pk.to_string()))]).into()
}
