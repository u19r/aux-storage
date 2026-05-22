use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_sync::{
    SyncMutationResolver, SyncProposalId, SyncWriteProposalRequest, SyncWriteRequest,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemRequest, BillingMode, CreateTableRequest,
    KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, PutRequest, TableName,
    TransactPutRequest, TransactUpdateRequest, TransactWriteItem, TransactWriteItemsRequest,
    UpdateItemRequest, WriteRequest,
};

use crate::DatabaseManager;

const ITERATIONS: usize = 8;
const BATCH_SIZE: usize = 4;

#[tokio::test(flavor = "current_thread")]
async fn sync_batch_write_resolution_allocation_baseline_tests() {
    // Set AUX_ALLOC_COUNTER_REPORT_PATH to persist this allocation baseline as
    // JSONL.
    let report = measure_sync_batch_write_resolution().await;
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn sync_update_resolution_allocation_baseline_tests() {
    let report = measure_sync_update_resolution().await;
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn sync_transact_write_resolution_allocation_baseline_tests() {
    let report = measure_sync_transact_write_resolution().await;
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

async fn measure_sync_batch_write_resolution() -> alloc_counter::AllocationReport<'static> {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("sync_batch_write_alloc");
    create_hash_table(&db, &table_name).await;
    let requests = sync_batch_write_requests(&table_name);

    let guard = AllocationGuard::start(
        module_path!(),
        "sync_batch_write_resolution_allocation_baseline_tests",
        file!(),
        line!(),
        Some("batch_size_4"),
    );

    for request in requests {
        let proposal = db
            .resolve_sync_mutation(request)
            .await
            .expect("resolve sync batch write");
        assert_eq!(proposal.batch.mutations.len(), BATCH_SIZE);
    }

    guard.finish()
}

async fn measure_sync_update_resolution() -> alloc_counter::AllocationReport<'static> {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("sync_update_alloc");
    create_hash_table(&db, &table_name).await;
    seed_update_items(&db, &table_name).await;
    let requests = sync_update_requests(&table_name);

    let guard = AllocationGuard::start(
        module_path!(),
        "sync_update_resolution_allocation_baseline_tests",
        file!(),
        line!(),
        Some("update_existing_no_return_values"),
    );

    for request in requests {
        let proposal = db
            .resolve_sync_mutation(request)
            .await
            .expect("resolve sync update");
        assert_eq!(proposal.batch.mutations.len(), 1);
    }

    guard.finish()
}

async fn measure_sync_transact_write_resolution() -> alloc_counter::AllocationReport<'static> {
    let db = DatabaseManager::new_for_test().await.expect("db");
    let table_name = TableName::new("sync_transact_alloc");
    create_hash_table(&db, &table_name).await;
    seed_update_items(&db, &table_name).await;
    let requests = sync_transact_write_requests(&table_name);

    let guard = AllocationGuard::start(
        module_path!(),
        "sync_transact_write_resolution_allocation_baseline_tests",
        file!(),
        line!(),
        Some("put_then_update"),
    );

    for request in requests {
        let proposal = db
            .resolve_sync_mutation(request)
            .await
            .expect("resolve sync transact write");
        assert_eq!(proposal.batch.mutations.len(), 2);
    }

    guard.finish()
}

async fn create_hash_table(db: &DatabaseManager, table_name: &TableName) {
    db.create_table(&CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("create table");
}

fn sync_batch_write_requests(table_name: &TableName) -> Vec<SyncWriteProposalRequest> {
    (0..ITERATIONS)
        .map(|iteration| {
            SyncWriteProposalRequest::new(
                SyncProposalId::new(format!("proposal-{iteration}")).expect("proposal id"),
                SyncWriteRequest::BatchWriteItem(batch_write_request(table_name, iteration)),
            )
        })
        .collect()
}

async fn seed_update_items(db: &DatabaseManager, table_name: &TableName) {
    for iteration in 0..ITERATIONS {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(realistic_update_item(iteration, "open"))
                .build(),
        )
        .await
        .expect("seed update item");
    }
}

fn sync_update_requests(table_name: &TableName) -> Vec<SyncWriteProposalRequest> {
    (0..ITERATIONS)
        .map(|iteration| {
            SyncWriteProposalRequest::new(
                SyncProposalId::new(format!("proposal-update-{iteration}")).expect("proposal id"),
                SyncWriteRequest::UpdateItem(update_request(table_name, iteration)),
            )
        })
        .collect()
}

fn sync_transact_write_requests(table_name: &TableName) -> Vec<SyncWriteProposalRequest> {
    (0..ITERATIONS)
        .map(|iteration| {
            SyncWriteProposalRequest::new(
                SyncProposalId::new(format!("proposal-transact-{iteration}")).expect("proposal id"),
                SyncWriteRequest::TransactWriteItems(transact_write_request(table_name, iteration)),
            )
        })
        .collect()
}

fn update_request(table_name: &TableName, iteration: usize) -> UpdateItemRequest {
    UpdateItemRequest {
        table_name: table_name.clone(),
        key: KeyAttributes::from(HashMap::from([(
            "pk".to_string(),
            AttributeValue::S(format!("item#{iteration:04}")),
        )])),
        update_expression: "SET #status = :status, #payload = :payload".to_string(),
        attribute_updates: None,
        condition_expression: Some("#status = :old_status".to_string()),
        expression_attribute_names: Some(HashMap::from([
            ("#status".to_string(), "status".to_string()),
            ("#payload".to_string(), "payload".to_string()),
        ])),
        expression_attribute_values: Some(HashMap::from([
            (
                ":status".to_string(),
                AttributeValue::S("closed".to_string()),
            ),
            (
                ":old_status".to_string(),
                AttributeValue::S("open".to_string()),
            ),
            (":payload".to_string(), AttributeValue::S("y".repeat(1024))),
        ])),
        expected: None,
        conditional_operator: None,
        return_values: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
        return_values_on_condition_check_failure: None,
    }
}

fn transact_write_request(table_name: &TableName, iteration: usize) -> TransactWriteItemsRequest {
    let put_id = ITERATIONS.saturating_add(iteration);
    TransactWriteItemsRequest {
        transact_items: vec![
            TransactWriteItem {
                put: Some(TransactPutRequest {
                    table_name: table_name.clone(),
                    item: realistic_update_item(put_id, "open"),
                    condition_expression: Some("attribute_not_exists(pk)".to_string()),
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                }),
                update: None,
                delete: None,
                condition_check: None,
            },
            TransactWriteItem {
                put: None,
                update: Some(TransactUpdateRequest {
                    table_name: table_name.clone(),
                    key: KeyAttributes::from(HashMap::from([(
                        "pk".to_string(),
                        AttributeValue::S(format!("item#{iteration:04}")),
                    )])),
                    update_expression: "SET #status = :status, #payload = :payload".to_string(),
                    condition_expression: Some("#status = :old_status".to_string()),
                    expression_attribute_names: Some(HashMap::from([
                        ("#status".to_string(), "status".to_string()),
                        ("#payload".to_string(), "payload".to_string()),
                    ])),
                    expression_attribute_values: Some(HashMap::from([
                        (
                            ":status".to_string(),
                            AttributeValue::S("closed".to_string()),
                        ),
                        (
                            ":old_status".to_string(),
                            AttributeValue::S("open".to_string()),
                        ),
                        (":payload".to_string(), AttributeValue::S("z".repeat(1024))),
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
    }
}

fn batch_write_request(table_name: &TableName, iteration: usize) -> BatchWriteItemRequest {
    let mut writes = Vec::with_capacity(BATCH_SIZE);
    for offset in 0..BATCH_SIZE {
        let item_id = iteration.saturating_mul(BATCH_SIZE).saturating_add(offset);
        writes.push(WriteRequest {
            put_request: Some(PutRequest {
                item: HashMap::from([
                    (
                        "pk".to_string(),
                        AttributeValue::S(format!("item#{item_id:04}")),
                    ),
                    ("payload".to_string(), AttributeValue::S("x".repeat(1024))),
                ]),
            }),
            delete_request: None,
        });
    }

    BatchWriteItemRequest {
        request_items: HashMap::from([(table_name.clone(), writes)]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

fn realistic_update_item(iteration: usize, status: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("item#{iteration:04}")),
        ),
        ("status".to_string(), AttributeValue::S(status.to_string())),
        ("payload".to_string(), AttributeValue::S("x".repeat(1024))),
    ])
}
