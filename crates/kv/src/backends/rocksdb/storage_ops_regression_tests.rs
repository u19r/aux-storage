use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, StreamSpecification, StreamViewType, TableName,
};

use crate::{RocksDbKvStore, SortedKvDbStorageProvider, kv_support_tests::rocksdb_test_path};

async fn create_test_provider() -> SortedKvDbStorageProvider<RocksDbKvStore> {
    SortedKvDbStorageProvider::new(
        RocksDbKvStore::new(rocksdb_test_path("storage-ops-regression")).unwrap(),
    )
}

fn create_table_request(table_name: &str) -> CreateTableRequest {
    CreateTableRequest::new(
        TableName::new(table_name),
        vec![AttributeDefinition {
            attribute_name: "id".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "id".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    )
}

fn create_stream_enabled_table_request(table_name: &str) -> CreateTableRequest {
    let mut request = create_table_request(table_name);
    request.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    });
    request
}

fn create_stream_enabled_hash_range_table_request(table_name: &str) -> CreateTableRequest {
    let mut request = CreateTableRequest::new(
        TableName::new(table_name),
        vec![
            AttributeDefinition {
                attribute_name: "customerId".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "orderId".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "customerId".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "orderId".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    );
    request.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    });
    request
}

async fn assert_table_listed(
    provider: &SortedKvDbStorageProvider<RocksDbKvStore>,
    table_name: &TableName,
) {
    let tables = provider.list_tables(100, None).await.unwrap();
    assert!(
        tables.iter().any(|table| table.table_name == *table_name),
        "created table should be visible to list_tables; got: {:?}",
        tables
            .iter()
            .map(|table| table.table_name.as_ref())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn created_table_is_immediately_listable_after_rocksdb_transaction_retries() {
    let provider = create_test_provider().await;
    provider.initialize_storage().await.unwrap();
    let request = create_table_request("RocksCreateListable");

    provider.create_table(&request).await.unwrap();

    assert_table_listed(&provider, &request.table_name).await;
}

#[tokio::test]
async fn recreated_table_is_immediately_listable_after_rocksdb_cleanup() {
    let provider = create_test_provider().await;
    provider.initialize_storage().await.unwrap();
    let request = create_table_request("RocksRecreateListable");

    provider.create_table(&request).await.unwrap();
    provider.delete_table(&request.table_name).await.unwrap();
    provider.create_table(&request).await.unwrap();

    assert_table_listed(&provider, &request.table_name).await;
}

#[tokio::test]
async fn stream_enabled_table_is_immediately_listable_after_rocksdb_create() {
    let provider = create_test_provider().await;
    provider.initialize_storage().await.unwrap();
    let request = create_stream_enabled_table_request("RocksStreamCreateListable");

    provider.create_table(&request).await.unwrap();

    assert_table_listed(&provider, &request.table_name).await;
}

#[tokio::test]
async fn stream_enabled_recreated_table_is_immediately_listable_after_rocksdb_delete() {
    let provider = create_test_provider().await;
    provider.initialize_storage().await.unwrap();
    let request = create_stream_enabled_table_request("RocksStreamRecreateListable");

    provider.create_table(&request).await.unwrap();
    provider.delete_table(&request.table_name).await.unwrap();
    provider.create_table(&request).await.unwrap();

    assert_table_listed(&provider, &request.table_name).await;
}

#[tokio::test]
async fn stream_enabled_hash_range_recreated_table_is_immediately_listable_after_rocksdb_delete() {
    let provider = create_test_provider().await;
    provider.initialize_storage().await.unwrap();
    let request = create_stream_enabled_hash_range_table_request("Orders");

    provider.create_table(&request).await.unwrap();
    provider.delete_table(&request.table_name).await.unwrap();
    provider.create_table(&request).await.unwrap();

    assert_table_listed(&provider, &request.table_name).await;
}

#[tokio::test]
async fn stream_enabled_hash_range_table_is_listable_after_prior_streamed_delete_item() {
    let provider = create_test_provider().await;
    provider.initialize_storage().await.unwrap();
    let users = create_stream_enabled_table_request("Users");
    let orders = create_stream_enabled_hash_range_table_request("Orders");

    provider.create_table(&users).await.unwrap();
    provider
        .put_item(
            users.table_name.clone(),
            HashMap::from([
                ("id".to_string(), AttributeValue::S("user123".to_string())),
                (
                    "name".to_string(),
                    AttributeValue::S("John Doe".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider
        .delete_item(
            users.table_name.clone(),
            HashMap::from([("id".to_string(), AttributeValue::S("user123".to_string()))]).into(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider.delete_table(&users.table_name).await.unwrap();

    provider.create_table(&orders).await.unwrap();

    assert_table_listed(&provider, &orders.table_name).await;
}

#[tokio::test]
async fn many_created_tables_are_immediately_listable() {
    let provider = create_test_provider().await;
    provider.initialize_storage().await.unwrap();

    for index in 1..=50 {
        let request = create_table_request(&format!("Table{index:03}"));
        provider.create_table(&request).await.unwrap();
    }

    let tables = provider.list_tables(100, None).await.unwrap();
    let table_names = tables
        .iter()
        .map(|table| table.table_name.as_ref())
        .collect::<Vec<_>>();
    for index in 1..=50 {
        let table_name = format!("Table{index:03}");
        assert!(
            table_names.iter().any(|name| **name == table_name),
            "created table should be visible to list_tables; missing {table_name}; got: \
             {table_names:?}"
        );
    }
}
