use std::collections::HashMap;

use storage_sync::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncApply, SyncLogId, SyncMutationResolver,
    SyncProposalId, SyncPutMutation, SyncWriteProposalRequest, SyncWriteRequest,
};
use storage_types::{AttributeValue, PutItemRequest, TableName};

use crate::{
    DatabaseManager,
    database_manager::sync_resolver_ops_support_tests::{commit_metadata, create_hash_table, item},
};

#[tokio::test]
async fn sync_apply_imports_resolved_put_and_delete() {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("sync_apply_put_delete");
    create_hash_table(&db, &table_name).await;

    let put_proposal = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-put").unwrap(),
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
    db.apply_resolved_sync_mutations(commit_metadata(1), put_proposal.batch)
        .await
        .expect("apply put");
    assert_eq!(
        db.last_resolved_sync_log_id()
            .await
            .expect("last sync log id"),
        Some(SyncLogId::new(1, 1))
    );
    assert!(
        db.get_item_map(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
        )
        .await
        .expect("read put")
        .is_some()
    );

    let delete_proposal = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-delete").unwrap(),
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
        .expect("resolve delete");
    db.apply_resolved_sync_mutations(commit_metadata(2), delete_proposal.batch)
        .await
        .expect("apply delete");
    assert!(
        db.get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
        )
        .await
        .expect("read delete")
        .is_none()
    );
}

#[tokio::test]
async fn sync_apply_ignores_duplicate_and_stale_resolved_versions() {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("sync_apply_replay_stability");
    create_hash_table(&db, &table_name).await;

    let first = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-first").unwrap(),
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
        .expect("resolve first");
    let first_batch = first.batch.clone();
    db.apply_resolved_sync_mutations(commit_metadata(1), first_batch.clone())
        .await
        .expect("apply first");
    db.apply_resolved_sync_mutations(commit_metadata(1), first_batch)
        .await
        .expect("duplicate apply is idempotent");

    let second = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-second").unwrap(),
            SyncWriteRequest::PutItem(PutItemRequest {
                table_name: table_name.clone(),
                item: item("item#1", "second"),
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
        .expect("resolve second");
    db.apply_resolved_sync_mutations(commit_metadata(2), second.batch)
        .await
        .expect("apply second");
    db.apply_resolved_sync_mutations(
        commit_metadata(3),
        ResolvedSyncMutationBatch::new(vec![ResolvedSyncMutation::Put(SyncPutMutation {
            mutation_id: storage_sync::SyncMutationId::new("stale").unwrap(),
            table_name: table_name.clone(),
            key_json: r#"{"pk":{"S":"item#1"}}"#.to_string(),
            item_json: serde_json::to_string(&item("item#1", "stale")).unwrap(),
            old_item_json: None,
            target_item_stream_version: storage_types::ItemStreamVersion::new(1),
            response: storage_sync::SyncMutationResponse::default(),
        })]),
    )
    .await
    .expect("stale apply is ignored");

    let stored = db
        .get_item_map(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("read stored")
        .expect("stored item");
    assert_eq!(
        stored.get("value"),
        Some(&AttributeValue::S("second".to_string()))
    );
}

#[tokio::test]
async fn sync_apply_rolls_back_all_mutations_when_batch_fails() {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("sync_apply_atomic_batch");
    create_hash_table(&db, &table_name).await;
    let valid = db
        .resolve_sync_mutation(SyncWriteProposalRequest::new(
            SyncProposalId::new("proposal-atomic").unwrap(),
            SyncWriteRequest::PutItem(PutItemRequest {
                table_name: table_name.clone(),
                item: item("item#1", "valid"),
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
        .expect("resolve valid put");
    let mut mutations = valid.batch.mutations;
    mutations.push(ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: storage_sync::SyncMutationId::new("missing-table").unwrap(),
        table_name: TableName::new("missing_table"),
        key_json: r#"{"pk":{"S":"item#2"}}"#.to_string(),
        item_json: serde_json::to_string(&item("item#2", "invalid")).unwrap(),
        old_item_json: None,
        target_item_stream_version: storage_types::ItemStreamVersion::new(1),
        response: storage_sync::SyncMutationResponse::default(),
    }));

    db.apply_resolved_sync_mutations(
        commit_metadata(1),
        ResolvedSyncMutationBatch::new(mutations),
    )
    .await
    .expect_err("missing table should fail batch");

    assert!(
        db.get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
        )
        .await
        .expect("read rolled back item")
        .is_none(),
        "valid mutation must roll back with failed batch"
    );
    assert_eq!(
        db.last_resolved_sync_log_id()
            .await
            .expect("last sync log id"),
        None
    );
}
