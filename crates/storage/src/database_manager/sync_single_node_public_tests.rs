use std::collections::HashMap;

use storage_sync::SyncLogId;
use storage_types::{AllOld, AttributeValue, PutRequest, TableName, WriteRequest};

use crate::{
    PutItemInput,
    database_manager::sync_resolver_ops_support_tests::{
        create_hash_table, create_single_node_sync_db, item,
    },
};

#[tokio::test]
async fn public_put_item_routes_through_single_node_sync_log() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_public_put");
    create_hash_table(&db, &table_name).await;

    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("item#1", "open"))
            .build(),
    )
    .await
    .expect("put through sync mode");

    assert!(
        db.get_resolved_sync_log_entry(SyncLogId::new(1, 1))
            .await
            .expect("get persisted log entry")
            .is_some(),
        "public put must persist a sync log entry"
    );
    assert!(
        db.get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
        )
        .await
        .expect("read applied item")
        .is_some()
    );
}

#[tokio::test]
async fn public_put_item_return_values_are_resolved_in_single_node_sync_mode() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_public_put_return_values");
    create_hash_table(&db, &table_name).await;
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("item#1", "open"))
            .build(),
    )
    .await
    .expect("seed through sync mode");

    let response = db
        .put_item(
            PutItemInput::builder()
                .table_name(table_name)
                .item(item("item#1", "closed"))
                .return_values(AllOld::AllOld)
                .build(),
        )
        .await
        .expect("return values resolved by sync mode");

    assert_eq!(
        response.attributes.expect("old item").get("value"),
        Some(&AttributeValue::S("open".to_string()))
    );
}

#[tokio::test]
async fn public_delete_item_routes_through_single_node_sync_log() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_public_delete");
    create_hash_table(&db, &table_name).await;
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("item#1", "open"))
            .build(),
    )
    .await
    .expect("put through sync mode");

    db.delete_item(crate::DeleteItemInput {
        table_name: table_name.clone(),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]).into(),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
    })
    .await
    .expect("delete through sync mode");

    assert_eq!(
        db.last_resolved_sync_log_id()
            .await
            .expect("last sync log id"),
        Some(SyncLogId::new(1, 2))
    );
    assert!(
        db.get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
        )
        .await
        .expect("read deleted item")
        .is_none()
    );
}

#[tokio::test]
async fn public_update_item_routes_through_single_node_sync_log() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_public_update");
    create_hash_table(&db, &table_name).await;
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("item#1", "open"))
            .build(),
    )
    .await
    .expect("put through sync mode");

    db.update_item(crate::UpdateItemInput {
        table_name: table_name.clone(),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]).into(),
        update_expression: "SET #value = :value".to_string(),
        condition_expression: Some("#value = :old".to_string()),
        expression_attribute_names: Some(HashMap::from([(
            "#value".to_string(),
            "value".to_string(),
        )])),
        expression_attribute_values: Some(HashMap::from([
            (
                ":value".to_string(),
                AttributeValue::S("closed".to_string()),
            ),
            (":old".to_string(), AttributeValue::S("open".to_string())),
        ])),
        return_values: None,
    })
    .await
    .expect("update through sync mode");

    assert_eq!(
        db.last_resolved_sync_log_id()
            .await
            .expect("last sync log id"),
        Some(SyncLogId::new(1, 2))
    );
    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("read updated item")
        .expect("updated item");
    assert_eq!(
        stored.get("value"),
        Some(&AttributeValue::S("closed".to_string()))
    );
}

#[tokio::test]
async fn public_update_item_return_values_are_resolved_in_single_node_sync_mode() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_public_update_return_values");
    create_hash_table(&db, &table_name).await;
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("item#1", "open"))
            .build(),
    )
    .await
    .expect("seed through sync mode");

    let response = db
        .update_item(crate::UpdateItemInput {
            table_name,
            key: HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
                .into(),
            update_expression: "SET #value = :value".to_string(),
            condition_expression: None,
            expression_attribute_names: Some(HashMap::from([(
                "#value".to_string(),
                "value".to_string(),
            )])),
            expression_attribute_values: Some(HashMap::from([(
                ":value".to_string(),
                AttributeValue::S("closed".to_string()),
            )])),
            return_values: Some(storage_types::ReturnValuesOldNewUpdated::AllNew),
        })
        .await
        .expect("return values resolved by sync mode");

    assert_eq!(
        response.attributes.expect("new item").get("value"),
        Some(&AttributeValue::S("closed".to_string()))
    );
}

#[tokio::test]
async fn public_batch_write_item_routes_through_single_node_sync_log() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_public_batch");
    create_hash_table(&db, &table_name).await;

    let response = db
        .batch_write_item(storage_types::BatchWriteItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                vec![WriteRequest {
                    put_request: Some(PutRequest {
                        item: item("item#1", "open"),
                    }),
                    delete_request: None,
                }],
            )]),
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .await
        .expect("batch write through sync mode");

    assert!(response.unprocessed_items.is_none());
    let log_entry = db
        .get_resolved_sync_log_entry(SyncLogId::new(1, 1))
        .await
        .expect("get persisted log entry")
        .expect("sync log entry");
    assert_eq!(log_entry.batch.mutations.len(), 1);
    assert!(
        db.get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
        )
        .await
        .expect("read applied item")
        .is_some()
    );
}

#[tokio::test]
async fn public_transact_write_items_routes_through_single_node_sync_log() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_public_transact");
    create_hash_table(&db, &table_name).await;

    db.transact_write_items(storage_types::TransactWriteItemsRequest {
        transact_items: vec![
            storage_types::TransactWriteItem {
                put: Some(storage_types::TransactPutRequest {
                    table_name: table_name.clone(),
                    item: item("item#1", "open"),
                    condition_expression: Some("attribute_not_exists(pk)".to_string()),
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                }),
                update: None,
                delete: None,
                condition_check: None,
            },
            storage_types::TransactWriteItem {
                put: None,
                update: Some(storage_types::TransactUpdateRequest {
                    table_name: table_name.clone(),
                    key: HashMap::from([(
                        "pk".to_string(),
                        AttributeValue::S("item#1".to_string()),
                    )])
                    .into(),
                    update_expression: "SET #value = :value".to_string(),
                    condition_expression: Some("#value = :old".to_string()),
                    expression_attribute_names: Some(HashMap::from([(
                        "#value".to_string(),
                        "value".to_string(),
                    )])),
                    expression_attribute_values: Some(HashMap::from([
                        (
                            ":value".to_string(),
                            AttributeValue::S("closed".to_string()),
                        ),
                        (":old".to_string(), AttributeValue::S("open".to_string())),
                    ])),
                    return_values_on_condition_check_failure: None,
                }),
                delete: None,
                condition_check: None,
            },
        ],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("transact write through sync mode");

    let log_entry = db
        .get_resolved_sync_log_entry(SyncLogId::new(1, 1))
        .await
        .expect("get persisted log entry")
        .expect("sync log entry");
    assert_eq!(log_entry.batch.mutations.len(), 2);
    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("read transaction item")
        .expect("transaction item");
    assert_eq!(
        stored.get("value"),
        Some(&AttributeValue::S("closed".to_string()))
    );
}
