use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, StorageEnum, TableName, UpdateItemRequest,
};

use crate::backends::sqlite::SQLiteStorageProvider;

#[tokio::test]
async fn multi_set_update_with_condition_succeeds() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("test_table");
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

    // Insert initial item (_v 0)
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("AUTHN_TOTP".to_string()),
    );
    item.insert(
        "sk".to_string(),
        AttributeValue::S("U#u1#DEV#d1".to_string()),
    );
    item.insert("_v".to_string(), AttributeValue::N("0".to_string()));
    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    // Perform multi-field SET update with condition on _v
    let key = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("AUTHN_TOTP".to_string()),
        ),
        (
            "sk".to_string(),
            AttributeValue::S("U#u1#DEV#d1".to_string()),
        ),
    ]);
    let eav = HashMap::from([
        (":t".to_string(), AttributeValue::BOOL(true)),
        (
            ":at".to_string(),
            AttributeValue::S("2025-01-01T00:00:00Z".to_string()),
        ),
        (":bh".to_string(), AttributeValue::L(vec![])),
        (":v".to_string(), AttributeValue::N("1".to_string())),
        (
            ":expectedVersion".to_string(),
            AttributeValue::N("0".to_string()),
        ),
    ]);
    let ean = HashMap::from([("#v".to_string(), "_v".to_string())]);
    let _ = provider
        .update_item(
            UpdateItemRequest::builder()
                .table_name(table.clone())
                .key(key.clone())
                .update_expression(
                    "SET active = :t, activated_at = :at, backup_code_hashes = :bh, #v = :v",
                )
                .condition_expression(Some("#v = :expectedVersion".to_string()))
                .expression_attribute_names(Some(ean))
                .expression_attribute_values(Some(eav))
                .build(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn multi_set_update_with_condition_version_mismatch_fails() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("test_table");
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

    // Insert initial item (_v 0)
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("AUTHN_TOTP".to_string()),
    );
    item.insert(
        "sk".to_string(),
        AttributeValue::S("U#u1#DEV#d1".to_string()),
    );
    item.insert("_v".to_string(), AttributeValue::N("0".to_string()));
    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    // Attempt update expecting wrong _v (1 instead of stored 0) should fail
    let key = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("AUTHN_TOTP".to_string()),
        ),
        (
            "sk".to_string(),
            AttributeValue::S("U#u1#DEV#d1".to_string()),
        ),
    ]);
    let eav = HashMap::from([
        (":t".to_string(), AttributeValue::BOOL(true)),
        (
            ":at".to_string(),
            AttributeValue::S("2025-01-01T00:00:00Z".to_string()),
        ),
        (":bh".to_string(), AttributeValue::L(vec![])),
        (":v".to_string(), AttributeValue::N("2".to_string())),
        (
            ":expectedVersion".to_string(),
            AttributeValue::N("1".to_string()),
        ),
    ]);
    let ean = HashMap::from([("#v".to_string(), "_v".to_string())]);
    let err = provider
        .update_item(
            UpdateItemRequest::builder()
                .table_name(table.clone())
                .key(key.clone())
                .update_expression(
                    "SET active = :t, activated_at = :at, backup_code_hashes = :bh, #v = :v",
                )
                .condition_expression(Some("#v = :expectedVersion".to_string()))
                .expression_attribute_names(Some(ean))
                .expression_attribute_values(Some(eav))
                .build(),
        )
        .await
        .expect_err("expected version mismatch error");
    let msg = err.to_string();
    assert!(
        msg.contains("ConditionalCheckFailed")
            || msg == storage_types::DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE,
        "unexpected error message: {msg}"
    );
}

#[tokio::test]
async fn update_item_missing_table_returns_table_not_found() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();

    let key = HashMap::from([("id".to_string(), AttributeValue::S("item1".to_string()))]);
    let expression_attribute_names = HashMap::from([("#n".to_string(), "name".to_string())]);
    let expression_attribute_values =
        HashMap::from([(":new".to_string(), AttributeValue::S("new".to_string()))]);

    let err = provider
        .update_item(
            UpdateItemRequest::builder()
                .table_name(TableName::new("missing_table"))
                .key(key)
                .update_expression("SET #n = :new")
                .expression_attribute_names(Some(expression_attribute_names))
                .expression_attribute_values(Some(expression_attribute_values))
                .build(),
        )
        .await
        .expect_err("expected missing table to fail");

    assert!(
        matches!(err.as_ref(), StorageEnum::TableNotFound { .. }),
        "unexpected error: {err}"
    );
}
