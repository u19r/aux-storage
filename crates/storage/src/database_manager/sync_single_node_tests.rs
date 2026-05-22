use std::collections::HashMap;

use storage_sync::{SyncLogId, SyncProposalId, SyncWriteProposalRequest, SyncWriteRequest};
use storage_types::{AttributeValue, PutItemRequest, PutRequest, TableName, WriteRequest};

use crate::{
    DatabaseManager, PutItemInput,
    database_manager::sync_resolver_ops_support_tests::{
        create_hash_table, create_single_node_sync_db, item,
    },
};

#[tokio::test]
async fn single_node_sync_write_persists_log_entry_and_applies_command() {
    let db = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_sync_put");
    create_hash_table(&db, &table_name).await;
    let proposal_id = SyncProposalId::new("proposal-single-node-put").unwrap();

    let response = db
        .run_single_node_sync_write(SyncWriteProposalRequest::new(
            proposal_id.clone(),
            SyncWriteRequest::PutItem(PutItemRequest {
                table_name: table_name.clone(),
                item: item("item#1", "open"),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                expected: None,
                conditional_operator: None,
                return_values: None,
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
                return_values_on_condition_check_failure: None,
            }),
        ))
        .await
        .expect("run single-node sync write");

    assert_eq!(response.proposal_id, proposal_id);
    assert_eq!(
        db.last_resolved_sync_log_id()
            .await
            .expect("last sync log id"),
        Some(SyncLogId::new(1, 1))
    );
    let log_entry = db
        .get_resolved_sync_log_entry(SyncLogId::new(1, 1))
        .await
        .expect("get persisted sync log entry")
        .expect("sync log entry");
    assert_eq!(log_entry.metadata.log_id, SyncLogId::new(1, 1));
    assert_eq!(log_entry.metadata.leader_node_id, "single-node");
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
async fn single_node_sync_write_increments_persistent_log_index() {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("single_node_sync_index");
    create_hash_table(&db, &table_name).await;

    db.run_single_node_sync_write(SyncWriteProposalRequest::new(
        SyncProposalId::new("proposal-single-node-first").unwrap(),
        SyncWriteRequest::PutItem(PutItemRequest {
            table_name: table_name.clone(),
            item: item("item#1", "first"),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            expected: None,
            conditional_operator: None,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: None,
        }),
    ))
    .await
    .expect("run first sync write");
    db.run_single_node_sync_write(SyncWriteProposalRequest::new(
        SyncProposalId::new("proposal-single-node-second").unwrap(),
        SyncWriteRequest::DeleteItem(storage_types::DeleteItemRequest {
            table_name: table_name.clone(),
            key: HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
                .into(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            expected: None,
            conditional_operator: None,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: None,
        }),
    ))
    .await
    .expect("run second sync write");

    assert_eq!(
        db.last_resolved_sync_log_id()
            .await
            .expect("last sync log id"),
        Some(SyncLogId::new(1, 2))
    );
    assert!(
        db.get_resolved_sync_log_entry(SyncLogId::new(1, 1))
            .await
            .expect("get first entry")
            .is_some()
    );
    assert!(
        db.get_resolved_sync_log_entry(SyncLogId::new(1, 2))
            .await
            .expect("get second entry")
            .is_some()
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
async fn single_node_sync_supported_writes_match_ordinary_storage_results() {
    let ordinary = DatabaseManager::new_for_test().await.expect("ordinary db");
    let sync = create_single_node_sync_db().await;
    let table_name = TableName::new("single_node_equivalence");
    create_hash_table(&ordinary, &table_name).await;
    create_hash_table(&sync, &table_name).await;

    for db in [&ordinary, &sync] {
        db.put_item(
            PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("item#1", "open"))
                .build(),
        )
        .await
        .expect("put item");
        db.update_item(crate::UpdateItemInput {
            table_name: table_name.clone(),
            key: HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
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
                    AttributeValue::S("updated".to_string()),
                ),
                (":old".to_string(), AttributeValue::S("open".to_string())),
            ])),
            return_values: None,
        })
        .await
        .expect("update item");
        db.batch_write_item(storage_types::BatchWriteItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                vec![WriteRequest {
                    put_request: Some(PutRequest {
                        item: item("item#2", "batch"),
                    }),
                    delete_request: None,
                }],
            )]),
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .await
        .expect("batch write");
        db.transact_write_items(storage_types::TransactWriteItemsRequest {
            transact_items: vec![storage_types::TransactWriteItem {
                put: None,
                update: None,
                delete: Some(storage_types::TransactDeleteRequest {
                    table_name: table_name.clone(),
                    key: HashMap::from([(
                        "pk".to_string(),
                        AttributeValue::S("item#2".to_string()),
                    )])
                    .into(),
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                }),
                condition_check: None,
            }],
            client_request_token: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .await
        .expect("transaction delete");
    }

    for key in ["item#1", "item#2"] {
        let key = HashMap::from([("pk".to_string(), AttributeValue::S(key.to_string()))]);
        assert_eq!(
            ordinary
                .get_item_map(table_name.clone(), key.clone())
                .await
                .expect("ordinary read"),
            sync.get_item_map(table_name.clone(), key)
                .await
                .expect("sync read")
        );
    }
}
