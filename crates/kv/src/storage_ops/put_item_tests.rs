use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest,
    DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, KeyAttributeType, KeySchemaElement, KeyType,
    StorageEnum, TableName,
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
