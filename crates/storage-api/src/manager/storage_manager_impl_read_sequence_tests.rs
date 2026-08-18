use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use serde_json::json;
use storage_provider::{
    ReadSequenceExecutionBudget, ReadSequenceFlatResult, ReadSequenceFlatRow,
    StorageProviderReadContext,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse,
    BillingMode, CreateTableRequest, GetItemRequest, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType, QueryTableRequest, ReadSequenceConsistency, ReadSequenceNode,
    ReadSequenceNodeOperation, ReadSequenceRequest, StorageError, StorageResult, TableName,
    WireItem, plan_read_sequence,
};

use crate::manager::{
    StorageApiManager,
    storage_manager_impl_read_sequence::{
        execute_ordinary_read_sequence_for_test, execute_wave_for_test,
        read_sequence_consumed_capacity, read_sequence_shadow_sampled, validate_whole_plan_rows,
        whole_plan_execution_budget, whole_plan_static_budget_fits,
    },
};

fn plan(
    value: serde_json::Value,
) -> (
    storage_types::ReadSequenceRequest,
    storage_types::ReadSequencePlan,
) {
    let request = serde_json::from_value(value).expect("read sequence request");
    let plan = plan_read_sequence(&request).expect("read sequence plan");
    (request, plan)
}

#[test]
fn shadow_sampling_has_explicit_zero_and_full_bounds() {
    assert!(!read_sequence_shadow_sampled("request-a", 0));
    assert!(read_sequence_shadow_sampled("request-a", 100));
}

#[test]
fn shadow_sampling_is_deterministic_for_a_request_digest() {
    let digest = "request-digest";
    assert_eq!(
        read_sequence_shadow_sampled(digest, 37),
        read_sequence_shadow_sampled(digest, 37)
    );
    assert!(!read_sequence_shadow_sampled(digest, 0));
}

#[test]
fn read_sequence_consumed_capacity_is_a_provider_neutral_item_estimate() {
    assert_eq!(read_sequence_consumed_capacity(None, 4), None);
    assert_eq!(
        read_sequence_consumed_capacity(Some("TOTAL"), 0),
        Some(json!({
            "TableName": "ReadSequence",
            "CapacityUnits": 0.5,
            "ReadCapacityUnits": 0.5
        }))
    );
    assert_eq!(
        read_sequence_consumed_capacity(Some("INDEXES"), 2),
        Some(json!({
            "TableName": "ReadSequence",
            "CapacityUnits": 2.0,
            "ReadCapacityUnits": 2.0
        }))
    );
    assert_eq!(read_sequence_consumed_capacity(Some("NONE"), 2), None);
    assert_eq!(read_sequence_consumed_capacity(Some("invalid"), 2), None);
}

#[test]
fn static_whole_plan_budget_accepts_a_single_get() {
    let (request, plan) = plan(json!({
        "Nodes": [{
            "Name": "item",
            "Operation": {"Get": {
                "TableName": "items",
                "Key": {"id": {"S": "item"}}
            }}
        }]
    }));
    assert!(whole_plan_static_budget_fits(&request, &plan));
}

#[test]
fn static_whole_plan_budget_rejects_multiple_default_outputs() {
    let (request, plan) = plan(json!({
        "Nodes": [
            {"Name": "a", "Operation": {"Get": {
                "TableName": "items", "Key": {"id": {"S": "a"}}
            }}},
            {"Name": "b", "Operation": {"Get": {
                "TableName": "items", "Key": {"id": {"S": "b"}}
            }}}
        ],
        "Outputs": ["a", "b"]
    }));
    assert!(!whole_plan_static_budget_fits(&request, &plan));
}

#[test]
fn static_whole_plan_budget_rejects_an_overlarge_total_chain() {
    let keys = (0..100)
        .map(|index| json!({"id": {"S": format!("item-{index}")}}))
        .collect::<Vec<_>>();
    let operation = json!({
        "BatchGet": {"RequestItems": {"items": {"Keys": keys}}}
    });
    let nodes = (0..6)
        .map(|index| {
            let mut node = json!({
                "Name": format!("items_{index}"),
                "Operation": operation
            });
            if index > 0 {
                node["After"] = json!([format!("items_{}", index - 1)]);
            }
            node
        })
        .collect::<Vec<_>>();
    let (request, plan) = plan(json!({
        "Nodes": nodes,
        "Outputs": ["items_5"]
    }));
    assert!(!whole_plan_static_budget_fits(&request, &plan));
}

#[test]
fn bounded_whole_plan_budget_caps_a_single_query_to_the_smallest_frontier() {
    let (request, plan) = plan(json!({
        "MaxRootItems": 7,
        "MaxTotalReadItems": 5,
        "Nodes": [{
            "Name": "items",
            "Operation": {"Query": {
                "TableName": "items",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "p"}},
                "Limit": 10
            }}
        }]
    }));
    assert_eq!(
        whole_plan_execution_budget(&request, &plan),
        Some(ReadSequenceExecutionBudget::bounded_items(5))
    );
}

#[test]
fn bounded_whole_plan_budget_accounts_for_intermediate_item_limit() {
    let (request, plan) = plan(json!({
        "MaxIntermediateItems": 3,
        "Nodes": [{
            "Name": "items",
            "Operation": {"Query": {
                "TableName": "items",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "p"}},
                "Limit": 10
            }}
        }]
    }));
    assert_eq!(
        whole_plan_execution_budget(&request, &plan),
        Some(ReadSequenceExecutionBudget::bounded_items(3))
    );
}

#[test]
fn response_byte_budget_uses_one_item_provider_frontier() {
    let (request, plan) = plan(json!({
        "MaxResponseBytes": 4096,
        "Nodes": [{
            "Name": "items",
            "Operation": {"Query": {
                "TableName": "items",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "p"}},
                "Limit": 10
            }}
        }]
    }));
    assert_eq!(
        whole_plan_execution_budget(&request, &plan),
        Some(ReadSequenceExecutionBudget::bounded_items(1))
    );
}

#[test]
fn bounded_whole_plan_budget_rejects_dependent_query_shapes() {
    let (request, plan) = plan(json!({
        "MaxRootItems": 2,
        "Nodes": [
            {"Name": "parent", "Operation": {"Get": {
                "TableName": "items", "Key": {"id": {"S": "p"}}
            }}},
            {"Name": "child", "After": ["parent"], "Operation": {"Query": {
                "TableName": "items",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "p"}}
            }}}
        ],
        "Outputs": ["child"]
    }));
    assert_eq!(whole_plan_execution_budget(&request, &plan), None);
}

#[test]
fn zero_bounded_frontier_stays_on_ordinary_validation_path() {
    let (request, plan) = plan(json!({
        "MaxRootItems": 0,
        "Nodes": [{
            "Name": "items",
            "Operation": {"Query": {
                "TableName": "items",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "p"}}
            }}
        }]
    }));
    assert_eq!(whole_plan_execution_budget(&request, &plan), None);
}

#[test]
fn whole_plan_validation_rejects_an_omitted_error_dependent_invocation() {
    let request = serde_json::from_value(json!({
        "Nodes": [
            {
                "Name": "parent",
                "Operation": {"Get": {
                    "TableName": "items",
                    "Key": {"id": {"S": "missing"}}
                }}
            },
            {
                "Name": "child",
                "Operation": {"Get": {
                    "TableName": "items",
                    "Key": {"id": {"FromInput": "id"}}
                }},
                "Inputs": {"id": {
                    "From": {"Node": "parent", "Select": "$.Get.Item.id"},
                    "Cardinality": "ONE",
                    "OnMissing": "ERROR"
                }}
            }
        ],
        "Outputs": ["child"]
    }))
    .expect("request");
    let plan = storage_types::plan_read_sequence(&request).expect("plan");
    let rows = vec![ReadSequenceFlatRow {
        node: storage_types::ReadSequenceNodeId::from_index(0),
        invocation_ordinal: 0,
        input_refs: Default::default(),
        result: ReadSequenceFlatResult::Get { item: None },
    }];
    assert!(validate_whole_plan_rows(&plan, &rows).is_err());
}

struct DelayedReadContext {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

#[async_trait]
impl StorageProviderReadContext for DelayedReadContext {
    async fn get_item(
        &self,
        _table_name: TableName,
        _key: KeyAttributes,
        _consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(25)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(None)
    }

    async fn batch_get_item(
        &self,
        _request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        Err(StorageError::unsupported(
            "scheduler test does not batch read",
        ))
    }

    async fn query_table(
        &self,
        _request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        Err(StorageError::unsupported("scheduler test does not query"))
    }
}

fn independent_get_node(name: &str) -> ReadSequenceNode {
    ReadSequenceNode::new(
        name,
        ReadSequenceNodeOperation::Get(GetItemRequest::new(
            TableName::new("items"),
            KeyAttributes::from([(String::from("id"), AttributeValue::S(name.to_string()))]),
        )),
    )
}

fn independent_get_request() -> ReadSequenceRequest {
    ReadSequenceRequest {
        read_consistency: ReadSequenceConsistency::Transactional,
        nodes: vec![independent_get_node("left"), independent_get_node("right")],
        ..Default::default()
    }
}

#[tokio::test]
async fn independent_wave_nodes_overlap_within_scheduler_bound() {
    let request = independent_get_request();
    let manager = crate::manager::StorageApiManagerImpl::new_with_options(
        Arc::new(storage::DatabaseManager::new_for_test().await.expect("db")),
        crate::manager::StorageApiManagerOptions::default(),
    );
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let provider_context = Box::new(DelayedReadContext {
        active,
        peak: peak.clone(),
    });
    execute_wave_for_test(&manager, &request, provider_context)
        .await
        .expect("wave");
    assert_eq!(peak.load(Ordering::SeqCst), 2);
}

struct OrdinaryDynamoReadContext {
    get_calls: Arc<AtomicUsize>,
    batch_get_calls: Arc<AtomicUsize>,
    return_unrequested_child: bool,
}

#[async_trait]
impl StorageProviderReadContext for OrdinaryDynamoReadContext {
    async fn get_item(
        &self,
        _table_name: TableName,
        _key: KeyAttributes,
        _consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        self.batch_get_calls.fetch_add(1, Ordering::SeqCst);
        let (table_name, keys) = request
            .request_items
            .into_iter()
            .next()
            .expect("one BatchGet table");
        assert!(keys.consistent_read.unwrap_or(false));
        let items = match table_name.as_ref() {
            "parents" => {
                assert_eq!(keys.keys.len(), 2);
                vec![
                    wire_item(&[("id", "one"), ("child_id", "a")]),
                    wire_item(&[("id", "two"), ("child_id", "b")]),
                ]
            }
            "children" => {
                assert_eq!(keys.keys.len(), 2);
                if self.return_unrequested_child {
                    vec![wire_item(&[("id", "other"), ("value", "wrong")])]
                } else {
                    vec![
                        wire_item(&[("id", "b"), ("value", "B")]),
                        wire_item(&[("id", "a"), ("value", "A")]),
                    ]
                }
            }
            table => panic!("unexpected BatchGet table {table}"),
        };
        Ok(BatchGetWireItemResponse {
            responses: Some(std::collections::HashMap::from([(table_name, items)])),
            unprocessed_keys: None,
            consumed_capacity: None,
        })
    }

    async fn query_table(
        &self,
        _request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        Err(StorageError::unsupported(
            "ordinary DynamoDB batching test does not query",
        ))
    }
}

fn wire_item(values: &[(&str, &str)]) -> WireItem {
    WireItem::from_attribute_map(
        &values
            .iter()
            .map(|(name, value)| ((*name).to_string(), AttributeValue::S((*value).to_string())))
            .collect(),
    )
    .expect("wire item")
}

#[cfg(feature = "sqlite")]
async fn read_context_test_manager() -> crate::manager::StorageApiManagerImpl {
    let db = storage::DatabaseManager::new_for_test()
        .await
        .expect("test database");
    crate::manager::StorageApiManagerImpl::new_with_options(
        Arc::new(db),
        crate::manager::StorageApiManagerOptions::default(),
    )
}

#[cfg(feature = "sqlite")]
fn dependent_get_request() -> ReadSequenceRequest {
    serde_json::from_value(json!({
        "ReadConsistency": "TRANSACTIONAL",
        "Nodes": [
            {
                "Name": "parents",
                "Operation": {"BatchGet": {"RequestItems": {"parents": {"Keys": [
                    {"id": {"S": "one"}},
                    {"id": {"S": "two"}}
                ]}}}}
            },
            {
                "Name": "children",
                "Operation": {"Get": {
                    "TableName": "children",
                    "Key": {"id": {"FromInput": "child_id"}}
                }},
                "Inputs": {"child_id": {
                    "From": {"Node": "parents", "Select": "$.BatchGet.Items[*].child_id"},
                    "Cardinality": "MANY",
                    "OnMissing": "ERROR"
                }},
                "Iterate": "child_id"
            }
        ],
        "Outputs": ["children"]
    }))
    .expect("ReadSequence request")
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_many_dependent_gets_when_executed_then_one_batch_get_preserves_invocation_order() {
    let request = dependent_get_request();
    let manager = read_context_test_manager().await;
    let get_calls = Arc::new(AtomicUsize::new(0));
    let batch_get_calls = Arc::new(AtomicUsize::new(0));
    let response = execute_ordinary_read_sequence_for_test(
        &manager,
        &request,
        Box::new(OrdinaryDynamoReadContext {
            get_calls: get_calls.clone(),
            batch_get_calls: batch_get_calls.clone(),
            return_unrequested_child: false,
        }),
    )
    .await
    .expect("ordinary sequence");

    assert_eq!(get_calls.load(Ordering::SeqCst), 0);
    assert_eq!(batch_get_calls.load(Ordering::SeqCst), 2);
    let invocations = &response.nodes[0].invocations;
    assert_eq!(invocations.len(), 2);
    let values = invocations
        .iter()
        .map(|invocation| match &invocation.result {
            storage_types::ReadSequenceInvocationPayload::Get(response) => response
                .item
                .as_ref()
                .and_then(|item| item.get("value"))
                .cloned(),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            Some(AttributeValue::S("A".to_string())),
            Some(AttributeValue::S("B".to_string()))
        ]
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_batch_get_returns_an_unrequested_child_when_executed_then_sequence_fails_closed() {
    let manager = read_context_test_manager().await;
    let error = execute_ordinary_read_sequence_for_test(
        &manager,
        &dependent_get_request(),
        Box::new(OrdinaryDynamoReadContext {
            get_calls: Arc::new(AtomicUsize::new(0)),
            batch_get_calls: Arc::new(AtomicUsize::new(0)),
            return_unrequested_child: true,
        }),
    )
    .await
    .expect_err("unrequested item must fail");

    assert!(error.message.contains("outside the requested key set"));
}

struct ChunkedDynamoReadContext {
    batch_get_calls: Arc<AtomicUsize>,
    query_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl StorageProviderReadContext for ChunkedDynamoReadContext {
    async fn get_item(
        &self,
        _table_name: TableName,
        _key: KeyAttributes,
        _consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        Err(StorageError::unsupported("chunking test does not GetItem"))
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        self.batch_get_calls.fetch_add(1, Ordering::SeqCst);
        let (table_name, keys) = request
            .request_items
            .into_iter()
            .next()
            .expect("one BatchGet table");
        let items = if table_name.as_ref() == "parents" {
            vec![
                wire_item(&[("id", "one"), ("partition_id", "a")]),
                wire_item(&[("id", "two"), ("partition_id", "b")]),
            ]
        } else {
            assert!(keys.keys.len() <= 100);
            keys.keys
                .into_iter()
                .map(|key| {
                    let mut item = key.to_attribute_map();
                    item.insert("value".to_string(), AttributeValue::S("found".to_string()));
                    WireItem::from_attribute_map(&item).expect("wire item")
                })
                .collect()
        };
        Ok(BatchGetWireItemResponse {
            responses: Some(std::collections::HashMap::from([(table_name, items)])),
            unprocessed_keys: None,
            consumed_capacity: None,
        })
    }

    async fn query_table(
        &self,
        _request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let page = self.query_calls.fetch_add(1, Ordering::SeqCst);
        let start = page * 51;
        let items = (start..start + 51)
            .map(|index| wire_item(&[("child_id", &format!("child-{index}"))]))
            .collect();
        Ok((items, None))
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_dependent_get_fanout_over_one_hundred_when_executed_then_follow_on_batch_get_requests_are_used()
 {
    let request: ReadSequenceRequest = serde_json::from_value(json!({
        "ReadConsistency": "TRANSACTIONAL",
        "MaxFanoutPerStep": 102,
        "MaxIntermediateItems": 200,
        "MaxTotalReadItems": 300,
        "MaxChildQueryItemsPerParent": 60,
        "Nodes": [
            {
                "Name": "parents",
                "Operation": {"BatchGet": {"RequestItems": {"parents": {"Keys": [
                    {"id": {"S": "one"}},
                    {"id": {"S": "two"}}
                ]}}}}
            },
            {
                "Name": "groups",
                "Operation": {"Query": {
                    "TableName": "groups",
                    "KeyConditionExpression": "partition_id = :partition_id",
                    "ExpressionAttributeValues": {":partition_id": {"FromInput": "partition_id"}},
                    "Limit": 51
                }},
                "Inputs": {"partition_id": {
                    "From": {"Node": "parents", "Select": "$.BatchGet.Items[*].partition_id"},
                    "Cardinality": "MANY",
                    "OnMissing": "ERROR"
                }},
                "Iterate": "partition_id"
            },
            {
                "Name": "children",
                "Operation": {"Get": {
                    "TableName": "children",
                    "Key": {"id": {"FromInput": "child_id"}}
                }},
                "Inputs": {"child_id": {
                    "From": {"Node": "groups", "Select": "$.Query.Items[*].child_id"},
                    "Cardinality": "MANY",
                    "OnMissing": "ERROR"
                }},
                "Iterate": "child_id"
            }
        ],
        "Outputs": ["children"]
    }))
    .expect("ReadSequence request");
    let manager = read_context_test_manager().await;
    manager
        .create_table(CreateTableRequest::new(
            TableName::new("groups"),
            vec![AttributeDefinition {
                attribute_name: "partition_id".to_string(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "partition_id".to_string(),
                key_type: KeyType::Hash,
            }],
            BillingMode::PayPerRequest,
        ))
        .await
        .expect("create query table");
    let batch_get_calls = Arc::new(AtomicUsize::new(0));
    let query_calls = Arc::new(AtomicUsize::new(0));
    let response = execute_ordinary_read_sequence_for_test(
        &manager,
        &request,
        Box::new(ChunkedDynamoReadContext {
            batch_get_calls: batch_get_calls.clone(),
            query_calls: query_calls.clone(),
        }),
    )
    .await
    .expect("ordinary sequence");

    assert_eq!(query_calls.load(Ordering::SeqCst), 2);
    assert_eq!(batch_get_calls.load(Ordering::SeqCst), 3);
    assert_eq!(response.nodes[0].invocations.len(), 102);
}

struct UnprocessedBatchReadContext {
    calls: Arc<AtomicUsize>,
}

struct AlwaysUnprocessedBatchReadContext {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl StorageProviderReadContext for AlwaysUnprocessedBatchReadContext {
    async fn get_item(
        &self,
        _table_name: TableName,
        _key: KeyAttributes,
        _consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        Err(StorageError::unsupported(
            "retry exhaustion test does not GetItem",
        ))
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(BatchGetWireItemResponse {
            responses: None,
            unprocessed_keys: Some(request.request_items),
            consumed_capacity: None,
        })
    }

    async fn query_table(
        &self,
        _request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        Err(StorageError::unsupported(
            "retry exhaustion test does not Query",
        ))
    }
}

#[async_trait]
impl StorageProviderReadContext for UnprocessedBatchReadContext {
    async fn get_item(
        &self,
        _table_name: TableName,
        _key: KeyAttributes,
        _consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        Err(StorageError::unsupported("retry test does not GetItem"))
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let (table_name, mut keys) = request
            .request_items
            .into_iter()
            .next()
            .expect("one BatchGet table");
        if call == 0 {
            let unprocessed = keys.keys.pop().expect("second key");
            return Ok(BatchGetWireItemResponse {
                responses: Some(std::collections::HashMap::from([(
                    table_name.clone(),
                    vec![wire_item(&[("id", "one")])],
                )])),
                unprocessed_keys: Some(std::collections::HashMap::from([(
                    table_name,
                    storage_types::KeysAndAttributes {
                        keys: std::iter::once(unprocessed).collect(),
                        attributes_to_get: keys.attributes_to_get,
                        projection_expression: keys.projection_expression,
                        expression_attribute_names: keys.expression_attribute_names,
                        consistent_read: keys.consistent_read,
                    },
                )])),
                consumed_capacity: None,
            });
        }
        Ok(BatchGetWireItemResponse {
            responses: Some(std::collections::HashMap::from([(
                table_name,
                vec![wire_item(&[("id", "two")])],
            )])),
            unprocessed_keys: None,
            consumed_capacity: None,
        })
    }

    async fn query_table(
        &self,
        _request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        Err(StorageError::unsupported("retry test does not Query"))
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_unprocessed_batch_keys_when_executed_then_only_unprocessed_keys_are_retried() {
    let request: ReadSequenceRequest = serde_json::from_value(json!({
        "ReadConsistency": "TRANSACTIONAL",
        "Nodes": [{
            "Name": "items",
            "Operation": {"BatchGet": {"RequestItems": {"items": {"Keys": [
                {"id": {"S": "one"}},
                {"id": {"S": "two"}}
            ]}}}}
        }]
    }))
    .expect("ReadSequence request");
    let manager = read_context_test_manager().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let response = execute_ordinary_read_sequence_for_test(
        &manager,
        &request,
        Box::new(UnprocessedBatchReadContext {
            calls: calls.clone(),
        }),
    )
    .await
    .expect("ordinary sequence");

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let storage_types::ReadSequenceInvocationPayload::BatchGet(batch) =
        &response.nodes[0].invocations[0].result
    else {
        panic!("expected BatchGet result");
    };
    assert_eq!(
        batch
            .responses
            .as_ref()
            .and_then(|tables| tables.get(&TableName::new("items")))
            .map(Vec::len),
        Some(2)
    );
    assert!(batch.unprocessed_keys.is_none());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_batch_keys_remain_unprocessed_when_retry_budget_is_exhausted_then_sequence_fails() {
    let request: ReadSequenceRequest = serde_json::from_value(json!({
        "ReadConsistency": "TRANSACTIONAL",
        "Nodes": [{
            "Name": "items",
            "Operation": {"BatchGet": {"RequestItems": {"items": {"Keys": [
                {"id": {"S": "one"}}
            ]}}}}
        }]
    }))
    .expect("ReadSequence request");
    let manager = read_context_test_manager().await;
    let calls = Arc::new(AtomicUsize::new(0));

    let error = execute_ordinary_read_sequence_for_test(
        &manager,
        &request,
        Box::new(AlwaysUnprocessedBatchReadContext {
            calls: calls.clone(),
        }),
    )
    .await
    .expect_err("retry exhaustion must fail");

    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert!(error.message.contains("with 1 unprocessed key(s)"));
}
