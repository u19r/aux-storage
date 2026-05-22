use std::collections::HashMap;

use storage_sync::{
    ResolvedSyncMutation, SyncMutationResolver, SyncProposalId, SyncWriteProposalRequest,
    SyncWriteRequest,
};
use storage_types::{
    AttributeValue, DeleteRequest, ItemStreamVersion, PutItemRequest, PutRequest, StorageEnum,
    TableName, WriteRequest, context::WrappedError as _,
};

use crate::{
    DatabaseManager, PutItemInput,
    database_manager::sync_resolver_ops_support_tests::{create_hash_table, item},
};

#[tokio::test]
async fn sync_resolver_resolves_put_item_without_writing() {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("sync_resolver_put");
    create_hash_table(&db, &table_name).await;

    let proposal = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-1").unwrap(),
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
        .expect("resolve put");

    assert_eq!(proposal.batch.mutations.len(), 1);
    assert_eq!(proposal.read_set.items.len(), 1);
    assert!(
        db.get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
        )
        .await
        .expect("read")
        .is_none(),
        "resolver must not write before committed apply"
    );
}

#[tokio::test]
async fn sync_resolver_uses_durable_item_revision_for_target_version() {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("sync_resolver_revision");
    create_hash_table(&db, &table_name).await;
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("item#1", "open"))
            .build(),
    )
    .await
    .expect("seed item v1");
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("item#1", "pending"))
            .build(),
    )
    .await
    .expect("seed item v2");

    let proposal = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-1").unwrap(),
            SyncWriteRequest::PutItem(PutItemRequest {
                table_name,
                item: item("item#1", "closed"),
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
        .expect("resolve put");

    assert_eq!(
        proposal.read_set.items[0].item_stream_version,
        Some(ItemStreamVersion::new(2))
    );
    let ResolvedSyncMutation::Put(put) = &proposal.batch.mutations[0] else {
        panic!("expected put mutation");
    };
    assert_eq!(put.target_item_stream_version, ItemStreamVersion::new(3));
}

#[tokio::test]
async fn sync_resolver_reuses_condition_expression_semantics() {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("sync_resolver_condition");
    create_hash_table(&db, &table_name).await;
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("item#1", "open"))
            .build(),
    )
    .await
    .expect("seed item");

    let error = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-1").unwrap(),
            SyncWriteRequest::PutItem(PutItemRequest {
                table_name,
                item: item("item#1", "closed"),
                condition_expression: Some("attribute_not_exists(pk)".to_string()),
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
        .expect_err("condition should fail");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
}

#[tokio::test]
async fn sync_resolver_batch_write_uses_overlay_for_later_operations() {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("sync_resolver_batch_overlay");
    create_hash_table(&db, &table_name).await;

    let proposal = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-1").unwrap(),
            SyncWriteRequest::BatchWriteItem(storage_types::BatchWriteItemRequest {
                request_items: HashMap::from([(
                    table_name,
                    vec![
                        WriteRequest {
                            put_request: Some(PutRequest {
                                item: item("item#1", "open"),
                            }),
                            delete_request: None,
                        },
                        WriteRequest {
                            put_request: None,
                            delete_request: Some(DeleteRequest {
                                key: HashMap::from([(
                                    "pk".to_string(),
                                    AttributeValue::S("item#1".to_string()),
                                )])
                                .into(),
                            }),
                        },
                    ],
                )]),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            }),
        ))
        .await
        .expect("resolve batch");

    assert_eq!(proposal.batch.mutations.len(), 2);
    assert!(matches!(
        proposal.batch.mutations[0],
        ResolvedSyncMutation::Put(_)
    ));
    let ResolvedSyncMutation::Delete(delete) = &proposal.batch.mutations[1] else {
        panic!("expected delete mutation");
    };
    assert!(
        delete.old_item_json.is_some(),
        "delete should see prior put through resolver overlay"
    );
}
