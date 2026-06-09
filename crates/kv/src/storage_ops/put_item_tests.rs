use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemRequest, CreateTableRequest,
    DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, KeyAttributeType, KeySchemaElement, KeyType,
    PutRequest, StorageEnum, StreamRetentionDuration, TableName, TimestampMillis,
    TransactConditionCheckRequest, TransactPutRequest, TransactWriteItem,
    TransactWriteItemsRequest, WriteRequest,
};

#[tokio::test]
async fn kv_put_condition_failure_returns_conditional_check_failed() {
    let store =
        crate::RocksDbKvStore::new(crate::kv_support_tests::rocksdb_test_path("kv-put")).unwrap();
    let provider = crate::SortedKvDbStorageProvider::new(store);
    provider.initialize_storage().await.expect("initialize");

    let table = TableName::new("put_condition_table");
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
                ("v".to_string(), AttributeValue::N("1".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("seed item");

    let err = provider
        .put_item(
            table,
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("A".to_string())),
                ("v".to_string(), AttributeValue::N("2".to_string())),
            ]),
            Some("attribute_not_exists(pk)".to_string()),
            None,
            None,
            None,
        )
        .await
        .expect_err("conditional put should fail");

    assert!(matches!(err.as_ref(), StorageEnum::ConditionalCheckFailed));
    assert_eq!(err.to_string(), DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE);
}

#[tokio::test]
async fn kv_create_table_writes_custom_stream_duration_state() {
    let provider = test_provider("kv-create-custom-duration");
    let table = TableName::new("custom_duration_table");
    let mut create = hash_table_request(table.clone());
    create.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(2));
    create.aux_default_item_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(3));

    provider.create_table(&create).await.expect("create");

    let table_info = provider
        .get_table_metadata_from_name(&table)
        .await
        .expect("metadata read")
        .expect("table metadata");
    assert_eq!(
        table_info.table_stream_duration,
        StreamRetentionDuration::FiniteHours(2)
    );
    assert_eq!(
        table_info.default_item_stream_duration,
        StreamRetentionDuration::FiniteHours(3)
    );
    let markers = provider
        .list_due_stream_trim_markers(TimestampMillis::now() + 3 * 60 * 60 * 1000, 10)
        .await
        .expect("list markers");
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].scope.table_name, table);
}

#[tokio::test]
async fn kv_put_item_with_stream_ttl_writes_item_duration_marker() {
    let provider = test_provider("kv-put-custom-duration");
    let table = TableName::new("put_custom_duration_table");
    provider
        .create_table(&hash_table_request(table.clone()))
        .await
        .expect("create");

    provider
        .put_item_with_stream_ttl(
            table.clone(),
            item("A", "1"),
            None,
            None,
            None,
            None,
            Some(StreamRetentionDuration::FiniteHours(1)),
        )
        .await
        .expect("put");

    let markers = provider
        .list_due_stream_trim_markers(TimestampMillis::now() + 73 * 60 * 60 * 1000, 10)
        .await
        .expect("list markers");
    assert!(markers.iter().any(|marker| marker.scope.table_name == table
        && marker.scope.kind == storage_provider::StreamTrimScopeKind::Item));
}

#[tokio::test]
async fn kv_failed_conditional_put_does_not_write_item_duration_marker() {
    let provider = test_provider("kv-put-custom-duration-condition-failure");
    let table = TableName::new("put_custom_duration_failure_table");
    provider
        .create_table(&hash_table_request(table.clone()))
        .await
        .expect("create");
    provider
        .put_item(table.clone(), item("A", "1"), None, None, None, None)
        .await
        .expect("seed");

    provider
        .put_item_with_stream_ttl(
            table.clone(),
            item("A", "2"),
            Some("attribute_not_exists(pk)".to_string()),
            None,
            None,
            None,
            Some(StreamRetentionDuration::FiniteHours(1)),
        )
        .await
        .expect_err("conditional put should fail");

    let markers = provider
        .list_due_stream_trim_markers(TimestampMillis::now() + 73 * 60 * 60 * 1000, 10)
        .await
        .expect("list markers");
    assert!(
        !markers.iter().any(|marker| marker.scope.table_name == table
            && marker.scope.kind == storage_provider::StreamTrimScopeKind::Item)
    );
}

#[tokio::test]
async fn kv_batch_write_put_applies_item_stream_duration_markers() {
    let provider = test_provider("kv-batch-custom-duration");
    let table = TableName::new("batch_custom_duration_table");
    provider
        .create_table(&hash_table_request(table.clone()))
        .await
        .expect("create");

    provider
        .batch_write_item(
            BatchWriteItemRequest {
                request_items: HashMap::from([(
                    table.clone(),
                    vec![
                        WriteRequest {
                            put_request: Some(PutRequest {
                                item: item("A", "1"),
                                aux_item_stream_ttl_hours: Some(
                                    StreamRetentionDuration::FiniteHours(1),
                                ),
                            }),
                            delete_request: None,
                        },
                        WriteRequest {
                            put_request: Some(PutRequest {
                                item: item("B", "1"),
                                aux_item_stream_ttl_hours: Some(
                                    StreamRetentionDuration::FiniteHours(2),
                                ),
                            }),
                            delete_request: None,
                        },
                    ],
                )]),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            },
            true,
        )
        .await
        .expect("batch write");

    assert_eq!(item_duration_marker_count(&provider, &table).await, 2);
}

#[tokio::test]
async fn kv_cancelled_transaction_does_not_write_item_duration_marker() {
    let provider = test_provider("kv-transaction-custom-duration-rollback");
    let table = TableName::new("transaction_custom_duration_table");
    provider
        .create_table(&hash_table_request(table.clone()))
        .await
        .expect("create");

    provider
        .transact_write_items(TransactWriteItemsRequest {
            transact_items: vec![
                TransactWriteItem {
                    put: Some(TransactPutRequest {
                        table_name: table.clone(),
                        item: item("A", "1"),
                        condition_expression: None,
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                        aux_item_stream_ttl_hours: Some(StreamRetentionDuration::FiniteHours(1)),
                    }),
                    update: None,
                    delete: None,
                    condition_check: None,
                },
                TransactWriteItem {
                    put: None,
                    update: None,
                    delete: None,
                    condition_check: Some(TransactConditionCheckRequest {
                        table_name: table.clone(),
                        key: HashMap::from([(
                            "pk".to_string(),
                            AttributeValue::S("missing".to_string()),
                        )])
                        .into(),
                        condition_expression: "attribute_exists(pk)".to_string(),
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                    }),
                },
            ],
            client_request_token: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .await
        .expect_err("transaction should fail");

    assert_eq!(item_duration_marker_count(&provider, &table).await, 0);
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

fn test_provider(label: &str) -> crate::SortedKvDbStorageProvider<crate::RocksDbKvStore> {
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

fn item(pk: &str, value: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("v".to_string(), AttributeValue::N(value.to_string())),
    ])
}
