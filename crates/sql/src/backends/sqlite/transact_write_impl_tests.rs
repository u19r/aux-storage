use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType, StorageEnum, TableName, TransactConditionCheckRequest,
    TransactPutRequest, TransactWriteItem, TransactWriteItemsRequest, context::WrappedError as _,
};
use stream_provider::StreamProvider;

use crate::SQLiteStorageProvider;

async fn create_transaction_table() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    provider
        .create_table(&CreateTableRequest::new(
            table_name(),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            storage_types::BillingMode::PayPerRequest,
        ))
        .await
        .unwrap();

    for pk in ["existing-1", "existing-2"] {
        provider
            .put_item(table_name(), item(pk), None, None, None, None)
            .await
            .unwrap();
    }

    provider
}

#[tokio::test]
async fn transact_write_collects_later_condition_failures_in_cancellation_order() {
    let provider = create_transaction_table().await;

    let result = provider
        .transact_write_items(TransactWriteItemsRequest {
            transact_items: vec![
                condition_check_false("existing-1"),
                put_item("new-middle"),
                condition_check_false("existing-2"),
            ],
            client_request_token: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .await;

    let error = result.expect_err("transaction should be cancelled");
    let StorageEnum::TransactionCanceled { reasons } = error.to_enum() else {
        panic!("expected transaction cancellation, got {error:?}");
    };
    assert_eq!(
        reasons.as_slice(),
        vec![
            "ConditionalCheckFailed".to_string(),
            "None".to_string(),
            "ConditionalCheckFailed".to_string(),
        ]
        .as_slice()
    );
    assert!(
        provider
            .get_item(table_name(), key("new-middle"), true)
            .await
            .unwrap()
            .is_none(),
        "successful middle write must be rolled back when the transaction is cancelled"
    );
}

fn condition_check_false(pk: &str) -> TransactWriteItem {
    TransactWriteItem {
        condition_check: Some(TransactConditionCheckRequest {
            table_name: table_name(),
            key: key(pk),
            condition_expression: "attribute_not_exists(pk)".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        ..TransactWriteItem::default()
    }
}

fn put_item(pk: &str) -> TransactWriteItem {
    TransactWriteItem {
        put: Some(TransactPutRequest {
            table_name: table_name(),
            item: item(pk),
            indexers: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        ..TransactWriteItem::default()
    }
}

fn table_name() -> TableName {
    TableName::new("tx_reason_order")
}

fn key(pk: &str) -> KeyAttributes {
    KeyAttributes::from([("pk".to_string(), AttributeValue::S(pk.to_string()))])
}

fn item(pk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("note".to_string(), AttributeValue::S("fixture".to_string())),
    ])
}
