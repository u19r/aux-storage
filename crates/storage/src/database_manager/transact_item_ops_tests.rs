use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemRequest, CreateTableRequest,
    KeyAttributeType, KeySchemaElement, KeyType, PutRequest, TableName, TransactPutRequest,
    TransactWriteItem, TransactWriteItemsRequest, WireItem, WriteRequest,
};

async fn create_hash_table(db: &crate::DatabaseManager, table_name: &TableName) {
    let request = CreateTableRequest::new(
        table_name.clone(),
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
    db.create_table(&request).await.expect("create table");
}

fn table(name: &str) -> TableName {
    TableName::new(name)
}

#[tokio::test]
async fn manager_batch_write_persists_through_shared_owner() {
    let db = crate::DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = table("gate_batch_write");
    create_hash_table(&db, &table_name).await;
    db.batch_write_item(BatchWriteItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: WireItem::try_from(&HashMap::from([
                        ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                        (
                            "payload".to_string(),
                            AttributeValue::S("value".to_string()),
                        ),
                    ]))
                    .expect("valid item"),
                    indexers: None,
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            }],
        )])
        .into(),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("batch write succeeds after open gate");
    let stored = db
        .get_item_decode::<_, storage_types::AttributeMap>(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("read item")
        .expect("item exists");
    assert_eq!(
        stored.get("payload"),
        Some(&AttributeValue::S("value".to_string()))
    );
}

#[tokio::test]
async fn manager_transact_write_persists_through_shared_owner() {
    let db = crate::DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = table("gate_transact_write");
    create_hash_table(&db, &table_name).await;
    db.transact_write_items(TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            put: Some(TransactPutRequest {
                table_name: table_name.clone(),
                item: WireItem::try_from(&HashMap::from([
                    ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                    (
                        "payload".to_string(),
                        AttributeValue::S("value".to_string()),
                    ),
                ]))
                .expect("valid item"),
                indexers: None,
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            update: None,
            delete: None,
            condition_check: None,
        }],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("transact write succeeds after open gate");
    let stored = db
        .get_item_decode::<_, storage_types::AttributeMap>(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("read item")
        .expect("item exists");
    assert_eq!(
        stored.get("payload"),
        Some(&AttributeValue::S("value".to_string()))
    );
}
