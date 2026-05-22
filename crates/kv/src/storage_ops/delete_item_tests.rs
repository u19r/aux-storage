use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest,
    DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, ItemKey, KeyAttributeType, KeySchemaElement,
    KeyType, SerializesToKey, StorageEnum, TableName,
};
use tracing::info;

use crate::sorted_kv_store::SortedKvStore;

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
    let item_key = ItemKey::from_key_schema(table.clone(), &create.key_schema, &key)
        .unwrap()
        .serialize_to_bytes()
        .unwrap();

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
