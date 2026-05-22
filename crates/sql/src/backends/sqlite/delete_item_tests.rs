use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, TableName,
};

use crate::backends::sqlite::SQLiteStorageProvider;

#[tokio::test]
async fn delete_item_condition_matches_key_attributes() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
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

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("P1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("S1".to_string()));
    item.insert(
        "payload".to_string(),
        AttributeValue::S("value".to_string()),
    );
    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

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
        .get_item_map(table.clone(), key, true)
        .await
        .unwrap();
    assert!(remaining.is_none());
}

#[tokio::test]
async fn delete_missing_item_with_condition_returns_empty_item() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
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
        .get_item_map(table.clone(), key, true)
        .await
        .unwrap();
    assert!(remaining.is_none());
}
