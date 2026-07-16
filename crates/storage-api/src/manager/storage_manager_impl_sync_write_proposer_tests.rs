use std::{
    collections::HashMap,
    hint::black_box,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use alloc_counter::AllocationGuard;
use async_trait::async_trait;
use http_error::HttpApiError;
use serde_json::json;
use storage::DatabaseManager;
use storage_sync::{
    SyncMutationResponse, SyncProposalResponse, SyncWriteProposalRequest, SyncWriteRequest,
};
use storage_types::{AttributeMap, AttributeValue, PutItemResponse, TableName};

use crate::{
    manager::{
        StorageApiManager, StorageApiManagerImpl, StorageApiManagerOptions, SyncReadBarrier,
        SyncWriteProposer,
    },
    types::Response,
};

#[derive(Clone)]
struct RecordingSyncWriteProposer {
    requests: Arc<Mutex<Vec<SyncWriteProposalRequest>>>,
    fixed_response: Option<SyncProposalResponse>,
}

impl Default for RecordingSyncWriteProposer {
    fn default() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            fixed_response: None,
        }
    }
}

impl RecordingSyncWriteProposer {
    fn new(response: SyncProposalResponse) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            fixed_response: Some(response),
        }
    }

    fn operation_names(&self) -> Vec<&'static str> {
        self.requests
            .lock()
            .expect("requests")
            .iter()
            .map(|request| request.request.operation_name())
            .collect()
    }

    fn proposal_ids(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("requests")
            .iter()
            .map(|request| request.proposal_id.as_str().to_string())
            .collect()
    }
}

#[async_trait]
impl SyncWriteProposer for RecordingSyncWriteProposer {
    async fn propose_sync_write(
        &self,
        request: SyncWriteProposalRequest,
    ) -> Result<SyncProposalResponse, HttpApiError> {
        let response = self
            .fixed_response
            .clone()
            .unwrap_or_else(|| response_for_request(&request));
        self.requests.lock().expect("requests").push(request);
        Ok(response)
    }
}

#[derive(Default)]
struct CountingReadBarrier {
    calls: AtomicUsize,
}

impl CountingReadBarrier {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }
}

#[async_trait]
impl SyncReadBarrier for CountingReadBarrier {
    async fn ensure_linearizable_read(&self) -> Result<(), HttpApiError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

fn response_for_request(request: &SyncWriteProposalRequest) -> SyncProposalResponse {
    let response = match &request.request {
        SyncWriteRequest::CreateTable(_) => {
            json_response(json!({"TableDescription": table_description("ACTIVE")}))
        }
        SyncWriteRequest::UpdateTable(_) => {
            json_response(json!({"TableDescription": table_description("ACTIVE")}))
        }
        SyncWriteRequest::DeleteTable(_) => {
            json_response(json!({"TableDescription": table_description("DELETING")}))
        }
        SyncWriteRequest::UpdateTimeToLive(_) => json_response(json!({
            "TimeToLiveSpecification": {
                "AttributeName": "ttl",
                "Enabled": true
            }
        })),
        _ => SyncMutationResponse::default(),
    };
    SyncProposalResponse::new(request.proposal_id.clone(), vec![response])
}

fn json_response(value: serde_json::Value) -> SyncMutationResponse {
    SyncMutationResponse {
        response_json: Some(value.to_string()),
    }
}

fn table_description(status: &str) -> serde_json::Value {
    json!({
        "TableName": "SyncWrites",
        "TableStatus": status,
        "CreationDateTime": 1_700_000_000.0,
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ],
        "TableSizeBytes": 0,
        "ItemCount": 0,
        "TableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/SyncWrites"
    })
}

async fn create_db_with_table() -> Arc<DatabaseManager> {
    let db = Arc::new(DatabaseManager::new_for_test().await.expect("db"));
    let manager =
        StorageApiManagerImpl::new_with_options(db.clone(), StorageApiManagerOptions::default());
    manager
        .create_table(
            json!({
                "TableName": "SyncWrites",
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"}
                ],
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"}
                ]
            })
            .try_into()
            .expect("create table request"),
        )
        .await
        .expect("create table");
    db
}

fn realistic_put_request() -> storage_types::PutItemRequest {
    json!({
        "TableName": "SyncWrites",
        "Item": {
            "id": {"S": "allocation-put"},
            "payload": {"S": "x".repeat(64 * 1024)}
        }
    })
    .try_into()
    .expect("put request")
}

fn realistic_update_request() -> storage_types::UpdateItemRequest {
    json!({
        "TableName": "SyncWrites",
        "Key": {"id": {"S": "allocation-update"}},
        "UpdateExpression": "SET #payload = :payload",
        "ExpressionAttributeNames": {"#payload": "payload"},
        "ExpressionAttributeValues": {":payload": {"S": "x".repeat(64 * 1024)}}
    })
    .try_into()
    .expect("update request")
}

fn measure_eager_clone<T: Clone>(
    label: &'static str,
    request: &T,
    wrap: fn(T) -> SyncWriteRequest,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "disabled_sync_write_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..100 {
        black_box(wrap(request.clone()));
    }
    guard.finish()
}

async fn measure_lazy_disabled<T: Clone>(
    manager: &StorageApiManagerImpl,
    label: &'static str,
    request: &T,
    wrap: fn(T) -> SyncWriteRequest,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "disabled_sync_write_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..100 {
        let response = manager
            .propose_sync_write_if_configured(|| wrap(request.clone()))
            .await
            .expect("disabled proposer");
        black_box(response);
    }
    guard.finish()
}

#[tokio::test(flavor = "current_thread")]
async fn disabled_sync_proposer_does_not_construct_put_or_update_payloads() {
    let manager = StorageApiManagerImpl::new_with_options(
        create_db_with_table().await,
        StorageApiManagerOptions::default(),
    );
    let put = realistic_put_request();
    let update = realistic_update_request();

    let eager_put = measure_eager_clone("put_eager_clone", &put, SyncWriteRequest::PutItem);
    let lazy_put =
        measure_lazy_disabled(&manager, "put_lazy_disabled", &put, SyncWriteRequest::PutItem)
            .await;
    let eager_update =
        measure_eager_clone("update_eager_clone", &update, SyncWriteRequest::UpdateItem);
    let lazy_update = measure_lazy_disabled(
        &manager,
        "update_lazy_disabled",
        &update,
        SyncWriteRequest::UpdateItem,
    )
    .await;

    for report in [&eager_put, &lazy_put, &eager_update, &lazy_update] {
        alloc_counter::emit_report(report);
    }
    assert!(lazy_put.allocation_count < eager_put.allocation_count);
    assert!(lazy_put.allocated_bytes < eager_put.allocated_bytes);
    assert!(lazy_update.allocation_count < eager_update.allocation_count);
    assert!(lazy_update.allocated_bytes < eager_update.allocated_bytes);
}

#[tokio::test]
async fn configured_sync_write_proposer_receives_supported_public_writes() {
    let db = create_db_with_table().await;
    let proposer = Arc::new(RecordingSyncWriteProposer::default());
    let manager = StorageApiManagerImpl::new_with_options(
        db,
        StorageApiManagerOptions {
            sync_write_proposer: Some(proposer.clone()),
            ..StorageApiManagerOptions::default()
        },
    );

    manager
        .create_table(
            json!({
                "TableName": "SyncWrites",
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"}
                ],
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"}
                ]
            })
            .try_into()
            .expect("create table request"),
        )
        .await
        .expect("create table");
    manager
        .put_item(
            json!({
                "TableName": "SyncWrites",
                "Item": {"id": {"S": "item-1"}, "value": {"S": "put"}}
            })
            .try_into()
            .expect("put request"),
        )
        .await
        .expect("put");
    manager
        .delete_item(
            json!({
                "TableName": "SyncWrites",
                "Key": {"id": {"S": "item-1"}}
            })
            .try_into()
            .expect("delete request"),
        )
        .await
        .expect("delete");
    manager
        .update_item(
            json!({
                "TableName": "SyncWrites",
                "Key": {"id": {"S": "item-1"}},
                "UpdateExpression": "SET #value = :value",
                "ExpressionAttributeNames": {"#value": "value"},
                "ExpressionAttributeValues": {":value": {"S": "updated"}}
            })
            .try_into()
            .expect("update request"),
        )
        .await
        .expect("update");
    manager
        .batch_write_item(
            json!({
                "RequestItems": {
                    "SyncWrites": [{
                        "PutRequest": {
                            "Item": {"id": {"S": "item-2"}, "value": {"S": "batch"}}
                        }
                    }]
                }
            })
            .try_into()
            .expect("batch write request"),
        )
        .await
        .expect("batch write");
    manager
        .transact_write_items(
            json!({
                "TransactItems": [{
                    "Put": {
                        "TableName": "SyncWrites",
                        "Item": {"id": {"S": "item-3"}, "value": {"S": "txn"}}
                    }
                }]
            })
            .try_into()
            .expect("transact write request"),
        )
        .await
        .expect("transact write");
    manager
        .update_table(
            json!({
                "TableName": "SyncWrites",
                "StreamSpecification": {
                    "StreamEnabled": true,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                }
            })
            .try_into()
            .expect("update table request"),
        )
        .await
        .expect("update table");
    manager
        .update_time_to_live(
            serde_json::from_value(json!({
                "TableName": "SyncWrites",
                "TimeToLiveSpecification": {
                    "AttributeName": "ttl",
                    "Enabled": true
                }
            }))
            .expect("update ttl request"),
        )
        .await
        .expect("update ttl");
    manager
        .delete_table(
            json!({
                "TableName": "SyncWrites"
            })
            .try_into()
            .expect("delete table request"),
        )
        .await
        .expect("delete table");

    let operation_names = proposer.operation_names();
    assert_eq!(
        operation_names,
        vec![
            "CreateTable",
            "PutItem",
            "DeleteItem",
            "UpdateItem",
            "BatchWriteItem",
            "TransactWriteItems",
            "UpdateTable",
            "UpdateTimeToLive",
            "DeleteTable",
        ]
    );
}

#[tokio::test]
async fn configured_sync_write_proposer_maps_response_and_bypasses_direct_storage_write() {
    let db = create_db_with_table().await;
    let mut attributes = AttributeMap::new();
    attributes.insert("old", AttributeValue::S("from-sync-response".to_string()));
    let sync_response = SyncProposalResponse::new(
        storage_sync::SyncProposalId::new("proposal-response").expect("proposal id"),
        vec![SyncMutationResponse {
            response_json: Some(
                serde_json::to_string(&PutItemResponse {
                    attributes: Some(attributes.clone()),
                })
                .expect("response json"),
            ),
        }],
    );
    let proposer = Arc::new(RecordingSyncWriteProposer::new(sync_response));
    let manager = StorageApiManagerImpl::new_with_options(
        db.clone(),
        StorageApiManagerOptions {
            sync_write_proposer: Some(proposer),
            ..StorageApiManagerOptions::default()
        },
    );

    let response = manager
        .put_item(
            json!({
                "TableName": "SyncWrites",
                "Item": {"id": {"S": "item-1"}, "value": {"S": "put"}}
            })
            .try_into()
            .expect("put request"),
        )
        .await
        .expect("put");

    let Response::PutItem(response) = response else {
        panic!("expected PutItem response");
    };
    assert_eq!(response.attributes, Some(attributes));
    let stored = db
        .get_item_map(
            TableName::new("SyncWrites"),
            HashMap::from([("id".to_string(), AttributeValue::S("item-1".to_string()))]),
        )
        .await
        .expect("get item");
    assert!(stored.is_none());
}

#[tokio::test]
async fn transact_write_client_request_token_becomes_stable_proposal_id() {
    let db = create_db_with_table().await;
    let proposer = Arc::new(RecordingSyncWriteProposer::default());
    let manager = StorageApiManagerImpl::new_with_options(
        db,
        StorageApiManagerOptions {
            sync_write_proposer: Some(proposer.clone()),
            ..StorageApiManagerOptions::default()
        },
    );
    let request = || {
        json!({
            "ClientRequestToken": "token-123",
            "TransactItems": [{
                "Put": {
                    "TableName": "SyncWrites",
                    "Item": {"id": {"S": "item-token"}, "value": {"S": "txn"}}
                }
            }]
        })
        .try_into()
        .expect("transact write request")
    };

    manager
        .transact_write_items(request())
        .await
        .expect("first transact write");
    manager
        .transact_write_items(request())
        .await
        .expect("second transact write");

    assert_eq!(
        proposer.proposal_ids(),
        vec![
            "TransactWriteItems#client_request_token#token-123".to_string(),
            "TransactWriteItems#client_request_token#token-123".to_string(),
        ]
    );
}

#[tokio::test]
async fn generated_sync_proposal_ids_reuse_process_prefix_and_increment_sequence() {
    let db = create_db_with_table().await;
    let proposer = Arc::new(RecordingSyncWriteProposer::default());
    let manager = StorageApiManagerImpl::new_with_options(
        db,
        StorageApiManagerOptions {
            sync_write_proposer: Some(proposer.clone()),
            ..StorageApiManagerOptions::default()
        },
    );

    manager
        .put_item(
            json!({
                "TableName": "SyncWrites",
                "Item": {"id": {"S": "item-seq-1"}, "value": {"S": "put"}}
            })
            .try_into()
            .expect("put request"),
        )
        .await
        .expect("first put");
    manager
        .put_item(
            json!({
                "TableName": "SyncWrites",
                "Item": {"id": {"S": "item-seq-2"}, "value": {"S": "put"}}
            })
            .try_into()
            .expect("put request"),
        )
        .await
        .expect("second put");

    let proposal_ids = proposer.proposal_ids();
    assert_eq!(proposal_ids.len(), 2);
    let first_parts = proposal_ids[0].split('#').collect::<Vec<_>>();
    let second_parts = proposal_ids[1].split('#').collect::<Vec<_>>();
    assert_eq!(first_parts.len(), 4);
    assert_eq!(second_parts.len(), 4);
    assert_eq!(first_parts[0], "PutItem");
    assert_eq!(first_parts[1], "process");
    assert_eq!(first_parts[2], second_parts[2]);
    assert_ne!(first_parts[3], second_parts[3]);
}

#[tokio::test]
async fn sync_proposal_pipeline_limits_reject_before_proposer_call() {
    let db = create_db_with_table().await;
    let proposer = Arc::new(RecordingSyncWriteProposer::default());
    let manager = StorageApiManagerImpl::new_with_options(
        db,
        StorageApiManagerOptions {
            sync_write_proposer: Some(proposer.clone()),
            sync_proposal_pipeline_limits: storage_sync::SyncProposalPipelineLimits {
                max_batch_bytes: 1,
                ..storage_sync::SyncProposalPipelineLimits::default()
            },
            ..StorageApiManagerOptions::default()
        },
    );

    let error = manager
        .put_item(
            json!({
                "TableName": "SyncWrites",
                "Item": {"id": {"S": "item-1"}, "value": {"S": "put"}}
            })
            .try_into()
            .expect("put request"),
        )
        .await
        .expect_err("oversized proposal should fail");

    assert!(error.message.contains("sync proposal byte count"));
    assert!(proposer.operation_names().is_empty());
}

#[tokio::test]
async fn sync_proposal_pipeline_queue_depth_rejects_before_proposer_call() {
    let db = create_db_with_table().await;
    let proposer = Arc::new(RecordingSyncWriteProposer::default());
    let manager = StorageApiManagerImpl::new_with_options(
        db,
        StorageApiManagerOptions {
            sync_write_proposer: Some(proposer.clone()),
            sync_proposal_pipeline_limits: storage_sync::SyncProposalPipelineLimits {
                max_queue_depth: 0,
                ..storage_sync::SyncProposalPipelineLimits::default()
            },
            ..StorageApiManagerOptions::default()
        },
    );

    let error = manager
        .put_item(
            json!({
                "TableName": "SyncWrites",
                "Item": {"id": {"S": "item-1"}, "value": {"S": "put"}}
            })
            .try_into()
            .expect("put request"),
        )
        .await
        .expect_err("full queue should fail");

    assert_eq!(error.error_type, "ThrottlingException");
    assert!(error.message.contains("queue depth"));
    assert!(proposer.operation_names().is_empty());
}

#[tokio::test]
async fn sync_read_barrier_runs_for_strong_get_item_only() {
    let db = create_db_with_table().await;
    let writer =
        StorageApiManagerImpl::new_with_options(db.clone(), StorageApiManagerOptions::default());
    writer
        .put_item(
            json!({
                "TableName": "SyncWrites",
                "Item": {"id": {"S": "item-strong"}, "value": {"S": "read"}}
            })
            .try_into()
            .expect("put request"),
        )
        .await
        .expect("put");
    let barrier = Arc::new(CountingReadBarrier::default());
    let reader = StorageApiManagerImpl::new_with_options(
        db,
        StorageApiManagerOptions {
            sync_read_barrier: Some(barrier.clone()),
            ..StorageApiManagerOptions::default()
        },
    );

    reader
        .get_item(
            json!({
                "TableName": "SyncWrites",
                "Key": {"id": {"S": "item-strong"}},
                "ConsistentRead": false
            })
            .try_into()
            .expect("eventual get request"),
        )
        .await
        .expect("eventual get");
    assert_eq!(barrier.calls(), 0);

    reader
        .get_item(
            json!({
                "TableName": "SyncWrites",
                "Key": {"id": {"S": "item-strong"}},
                "ConsistentRead": true
            })
            .try_into()
            .expect("strong get request"),
        )
        .await
        .expect("strong get");
    assert_eq!(barrier.calls(), 1);
}
