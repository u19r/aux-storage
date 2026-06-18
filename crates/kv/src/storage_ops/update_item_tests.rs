use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, ItemKey, KeyAttributeType,
    KeySchemaElement, KeyType, TableName, UpdateItemRequest,
};

use crate::{keyspace::table_keys, sorted_kv_store::SortedKvStore};

#[tokio::test]
async fn kv_multi_set_update_with_condition_succeeds() {
    let store = crate::RocksDbKvStore::new(crate::kv_support_tests::rocksdb_test_path("kv-update"))
        .unwrap();
    let provider = crate::SortedKvDbStorageProvider::new(store);
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
async fn kv_multi_set_update_with_condition_version_mismatch_fails() {
    let store = crate::RocksDbKvStore::new(crate::kv_support_tests::rocksdb_test_path("kv-update"))
        .unwrap();
    let provider = crate::SortedKvDbStorageProvider::new(store);
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

    // Attempt update expecting wrong _v (1 instead of stored 0)
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
    // Accept either the internal variant name or the canonical display string.
    assert!(
        msg.contains("ConditionalCheckFailed")
            || msg == storage_types::DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE,
        "unexpected error message: {msg}"
    );
}

#[tokio::test]
async fn kv_update_condition_injects_missing_key_attributes() {
    let store = crate::RocksDbKvStore::new(crate::kv_support_tests::rocksdb_test_path("kv-update"))
        .unwrap();
    let provider = crate::SortedKvDbStorageProvider::new(store);
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
    stored_item.insert("expires".to_string(), AttributeValue::N("1".to_string()));

    let stored_bytes = storage_types::storage_serde::to_bytes(&stored_item).unwrap();
    provider
        .kv_store
        .put(&item_key, &stored_bytes, None)
        .await
        .unwrap();

    let eav = HashMap::from([(":v".to_string(), AttributeValue::S("new".to_string()))]);
    provider
        .update_item(
            UpdateItemRequest::builder()
                .table_name(table.clone())
                .key(key.clone())
                .update_expression("SET value = :v")
                .condition_expression(Some("attribute_exists(pk)".to_string()))
                .expression_attribute_values(Some(eav))
                .build(),
        )
        .await
        .unwrap();

    let item = provider
        .get_item_map(table.clone(), key.into(), true)
        .await
        .unwrap()
        .expect("item");
    assert!(item.contains_key("pk"));
    assert!(item.contains_key("sk"));
    assert_eq!(
        item.get("value"),
        Some(&AttributeValue::S("new".to_string()))
    );
}
