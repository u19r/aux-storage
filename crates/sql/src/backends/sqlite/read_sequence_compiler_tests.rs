use std::collections::HashMap;

use storage_provider::{
    ReadSequenceExecution, ReadSequenceExecutionBudget, ReadSequenceFlatResult,
    StorageProvider as _,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, CreateTableRequest, GetItemRequest,
    KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, ProjectionType, QueryRequest,
    ReadSequenceConsistency, ReadSequenceNode, ReadSequenceNodeOperation, ReadSequenceRequest,
    TableName, context::WrappedError, plan_read_sequence,
};

use crate::{
    backends::sqlite::{
        SQLiteStorageProvider,
        storage_provider::{compile_sqlite_read_sequence_statement, sqlite_read_sequence_metadata},
    },
    sql_test_support::{
        mapped_gsi_parent_item, mapped_gsi_read_sequence_one_request,
        mapped_gsi_read_sequence_request, mapped_gsi_table_request_with_projection,
    },
};

async fn explain_query_plan(
    provider: &SQLiteStorageProvider,
    statement: &storage_provider::ReadSequenceSqlStatement,
) -> Vec<String> {
    let parameters = statement
        .parameters
        .iter()
        .map(|value| value.inner_string().expect("scalar EQP parameter"))
        .collect::<Vec<_>>();
    let sql = statement.sql.clone();
    crate::utils::call_sqlite(&provider.connection, move |connection| {
        let mut explain = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .map_err(crate::error_handler::map_sqlite_error)?;
        let rows = explain
            .query_map(rusqlite::params_from_iter(parameters.iter()), |row| {
                row.get::<_, String>(3)
            })
            .map_err(crate::error_handler::map_sqlite_error)?;
        rows.map(|row| row.map_err(crate::error_handler::map_sqlite_error))
            .collect()
    })
    .await
    .expect("EXPLAIN QUERY PLAN")
}

fn assert_single_statement(statement: &storage_provider::ReadSequenceSqlStatement) {
    assert!(!statement.sql.trim().is_empty(), "compiled SQL is empty");
    assert!(
        !statement.sql.contains(';'),
        "compiled read sequence must be one SQL statement, got a statement separator: {}",
        statement.sql
    );
}

fn sort_by_sort_key(items: &mut [storage_types::AttributeMap]) {
    items.sort_by(|left, right| {
        left.get("sk")
            .map(|value| value.inner_string().unwrap_or_default())
            .cmp(
                &right
                    .get("sk")
                    .map(|value| value.inner_string().unwrap_or_default()),
            )
    });
}

fn request(nodes: Vec<ReadSequenceNode>) -> ReadSequenceRequest {
    ReadSequenceRequest::new(nodes)
}

fn key(id: &str) -> KeyAttributes {
    KeyAttributes::from([(String::from("pk"), AttributeValue::S(id.to_string()))])
}

async fn provider_with_items() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("provider");
    provider.initialize_storage().await.expect("initialize");
    provider
        .create_table(&CreateTableRequest::new(
            TableName::new("read_sequence_items"),
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
        .expect("create table");
    for id in ["a", "b"] {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(id.to_string()));
        item.insert(
            "value".to_string(),
            AttributeValue::S(format!("value-{id}")),
        );
        provider
            .put_item(
                TableName::new("read_sequence_items"),
                item,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("put item");
    }
    provider
}

#[tokio::test]
async fn compiled_get_matches_the_typed_item_contract() {
    let provider = provider_with_items().await;
    let request = request(vec![ReadSequenceNode {
        name: "root".to_string(),
        operation: ReadSequenceNodeOperation::Get(GetItemRequest::new(
            TableName::new("read_sequence_items"),
            key("a"),
        )),
        inputs: None,
        iterate: None,
        after: None,
    }]);
    let plan = plan_read_sequence(&request).expect("plan");
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .unwrap_or_else(|error| panic!("execute: {:?}", error.to_enum()));
    let storage_provider::ReadSequenceExecution::Executed(execution) = execution else {
        panic!("sqlite root get should compile");
    };
    let storage_provider::ReadSequenceFlatResult::Get { item } = &execution.rows[0].result else {
        panic!("expected get result");
    };
    let item = item.as_ref().expect("item");
    assert_eq!(item.get("pk"), Some(&AttributeValue::S("a".to_string())));
    assert_eq!(
        item.get("value"),
        Some(&AttributeValue::S("value-a".to_string()))
    );
}

#[tokio::test]
async fn compiled_get_reconstructs_present_and_missing_indexer_slots() {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("provider");
    provider.initialize_storage().await.expect("initialize");
    let table_name = TableName::new("read_sequence_indexed");
    let mut create = CreateTableRequest::new(
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
    create.max_indexers = storage_types::MaxIndexers::try_new(2).expect("capacity");
    provider.create_table(&create).await.expect("create table");
    let mut put = storage_types::PutItemRequest::new(
        table_name.clone(),
        HashMap::from([
            ("pk".to_string(), AttributeValue::S("a".to_string())),
            (
                "customer_id".to_string(),
                AttributeValue::S("customer-1".to_string()),
            ),
            (
                "payload".to_string(),
                AttributeValue::S("value".to_string()),
            ),
        ]),
    );
    put.indexers = Some(vec!["customer_id".to_string(), "optional_id".to_string()]);
    provider
        .put_item_request(put)
        .await
        .expect("put indexed item");
    let plan = plan_read_sequence(&request(vec![ReadSequenceNode {
        name: "root".to_string(),
        operation: ReadSequenceNodeOperation::Get(GetItemRequest::new(table_name, key("a"))),
        inputs: None,
        iterate: None,
        after: None,
    }]))
    .expect("plan");

    let ReadSequenceExecution::Executed(executed) = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("compiled indexed read")
    else {
        panic!("indexed root get should compile");
    };
    let ReadSequenceFlatResult::Get { item: Some(item) } = &executed.rows[0].result else {
        panic!("indexed item");
    };
    assert_eq!(
        item.get("customer_id"),
        Some(&AttributeValue::S("customer-1".to_string()))
    );
    assert!(item.get("optional_id").is_none());
    assert_eq!(
        item.get("payload"),
        Some(&AttributeValue::S("value".to_string()))
    );
}

#[tokio::test]
async fn compiled_mapped_indexer_join_returns_present_and_nil_children_in_one_statement() {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("provider");
    provider.initialize_storage().await.expect("initialize");
    let parents = TableName::new("mapped_parents");
    let children = TableName::new("mapped_children");
    let mut parent_table = CreateTableRequest::new(
        parents.clone(),
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
    parent_table.max_indexers = storage_types::MaxIndexers::try_new(1).expect("capacity");
    provider
        .create_table(&parent_table)
        .await
        .expect("create parent table");
    provider
        .create_table(&CreateTableRequest::new(
            children.clone(),
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
        ))
        .await
        .expect("create child table");
    for (sort, customer) in [("a", Some("customer-1")), ("b", None)] {
        let mut item = HashMap::from([
            ("pk".to_string(), AttributeValue::S("group".to_string())),
            ("sk".to_string(), AttributeValue::S(sort.to_string())),
        ]);
        if let Some(customer) = customer {
            item.insert(
                "customer_id".to_string(),
                AttributeValue::S(customer.to_string()),
            );
        }
        let mut put = storage_types::PutItemRequest::new(parents.clone(), item);
        put.indexers = Some(vec!["customer_id".to_string()]);
        provider.put_item_request(put).await.expect("put parent");
    }
    provider
        .put_item(
            children.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("group".to_string())),
                (
                    "sk".to_string(),
                    AttributeValue::S("customer-1".to_string()),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("related".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put child");
    let request: ReadSequenceRequest = serde_json::from_value(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": parents, "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "group"}}
            }}},
            {"Name": "children", "Operation": {"Get": {
                "TableName": children, "Key": {
                    "pk": {"S": "group"},
                    "sk": {"FromInput": "customer"}
                }
            }}, "Inputs": {
                "customer": {
                    "From": {"Node": "parents", "Select": "$.Query.Items[*].customer_id"},
                    "MappedKeySource": {"AttributeName": "customer_id", "Indexer": 0},
                    "Cardinality": "MANY", "OnMissing": "SKIP"
                }
            }, "Iterate": "customer"}
        ]
    }))
    .expect("mapped request");
    let plan = plan_read_sequence(&request).expect("mapped plan");
    storage_provider::read_sequence_sql_mapped_source(&plan)
        .expect("hash+range mapped SQL source should be recognized");

    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("mapped SQL execution");
    let ReadSequenceExecution::Executed(executed) = execution else {
        panic!("mapped SQL shape should compile: {execution:?}");
    };
    assert_eq!(executed.rows.len(), 3);
    assert!(matches!(
        &executed.rows[0].result,
        ReadSequenceFlatResult::Query { items, count: 2, .. } if items.len() == 2
    ));
    assert!(matches!(
        &executed.rows[1].result,
        ReadSequenceFlatResult::Get { item: Some(item) }
            if item.get("payload") == Some(&AttributeValue::S("related".to_string()))
    ));
    assert!(matches!(
        &executed.rows[2].result,
        ReadSequenceFlatResult::Get { item: None }
    ));
}

#[tokio::test]
async fn given_keys_only_gsi_when_mapping_indexer_then_sqlite_uses_projected_row() {
    let provider = SQLiteStorageProvider::new_with_settings(
        ":memory:",
        storage_provider::SqliteSettings {
            immediate_gsi_consistency: true,
            ..Default::default()
        },
    )
    .await
    .expect("provider");
    provider.initialize_storage().await.expect("initialize");
    let parents = TableName::new("mapped_gsi_parents");
    let children = TableName::new("mapped_gsi_children");
    provider
        .create_table(&mapped_gsi_table_request_with_projection(
            parents.clone(),
            ProjectionType::KeysOnly,
        ))
        .await
        .expect("create GSI parent table");
    provider
        .create_table(&CreateTableRequest::new(
            children.clone(),
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
        ))
        .await
        .expect("create child table");
    for (sort, customer) in [("a", Some("customer-1")), ("b", None)] {
        let mut put = storage_types::PutItemRequest::new(
            parents.clone(),
            mapped_gsi_parent_item(sort, customer),
        );
        put.indexers = Some(vec!["customer_id".to_string()]);
        provider.put_item_request(put).await.expect("put parent");
    }
    provider
        .put_item(
            children.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("group".to_string())),
                (
                    "sk".to_string(),
                    AttributeValue::S("customer-1".to_string()),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("related".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put child");

    let plan = plan_read_sequence(&mapped_gsi_read_sequence_request(&parents, &children))
        .expect("mapped GSI plan");
    let ReadSequenceExecution::Executed(executed) = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("mapped GSI execution")
    else {
        panic!("mapped GSI shape should compile");
    };
    assert_eq!(executed.rows.len(), 3);
    assert!(matches!(
        &executed.rows[0].result,
        ReadSequenceFlatResult::Query { items, .. }
            if items.iter().all(|item| !item.contains_key("customer_id"))
    ));
    assert!(matches!(
        &executed.rows[1].result,
        ReadSequenceFlatResult::Get { item: Some(item) }
            if item.get("payload") == Some(&AttributeValue::S("related".to_string()))
    ));
    assert!(matches!(
        &executed.rows[2].result,
        ReadSequenceFlatResult::Get { item: None }
    ));

    let one = plan_read_sequence(&mapped_gsi_read_sequence_one_request(&parents, &children))
        .expect("mapped GSI ONE plan");
    let ReadSequenceExecution::Executed(one) = provider
        .execute_read_sequence_plan(&one, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("mapped GSI ONE execution")
    else {
        panic!("mapped GSI ONE shape should compile");
    };
    assert_eq!(one.rows.len(), 2);
    assert!(matches!(
        &one.rows[1].result,
        ReadSequenceFlatResult::Get { item: Some(item) }
            if item.get("payload") == Some(&AttributeValue::S("related".to_string()))
    ));
}

#[tokio::test]
async fn strong_consistency_stays_on_the_ordinary_path() {
    let provider = provider_with_items().await;
    let request = request(vec![ReadSequenceNode {
        name: "root".to_string(),
        operation: ReadSequenceNodeOperation::Get(GetItemRequest::new(
            TableName::new("read_sequence_items"),
            key("a"),
        )),
        inputs: None,
        iterate: None,
        after: None,
    }]);
    let plan = plan_read_sequence(&request).expect("plan");
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Strong, None)
        .await
        .expect("strong consistency should not fail compilation");
    assert!(matches!(
        execution,
        ReadSequenceExecution::Unsupported(
            storage_provider::ReadSequenceUnsupportedReason::OperationShape
        )
    ));
}

#[tokio::test]
async fn compiled_independent_roots_preserve_node_order_and_missing_reads() {
    let provider = provider_with_items().await;
    let table_name = TableName::new("read_sequence_items");
    let batch = BatchGetItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            storage_types::KeysAndAttributes {
                keys: vec![key("b"), key("missing")].into(),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
                consistent_read: None,
            },
        )]),
        return_consumed_capacity: None,
    };
    let request = request(vec![
        ReadSequenceNode {
            name: "first".to_string(),
            operation: ReadSequenceNodeOperation::Get(GetItemRequest::new(
                table_name.clone(),
                key("a"),
            )),
            inputs: None,
            iterate: None,
            after: None,
        },
        ReadSequenceNode {
            name: "missing".to_string(),
            operation: ReadSequenceNodeOperation::Get(GetItemRequest::new(
                table_name.clone(),
                key("not-present"),
            )),
            inputs: None,
            iterate: None,
            after: None,
        },
        ReadSequenceNode {
            name: "batch".to_string(),
            operation: ReadSequenceNodeOperation::BatchGet(batch.clone()),
            inputs: None,
            iterate: None,
            after: None,
        },
    ]);
    let plan = plan_read_sequence(&request).expect("independent-root plan");
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .unwrap_or_else(|error| panic!("execute: {:?}", error.to_enum()));
    let ReadSequenceExecution::Executed(execution) = execution else {
        panic!("independent roots should compile");
    };
    assert_eq!(execution.next_continuation, None);
    assert_eq!(
        execution
            .rows
            .iter()
            .map(|row| row.node.index())
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    let ReadSequenceFlatResult::Get { item: Some(first) } = &execution.rows[0].result else {
        panic!("first root should return an item");
    };
    assert_eq!(first.get("pk"), Some(&AttributeValue::S("a".to_string())));
    let ReadSequenceFlatResult::Get { item: None } = &execution.rows[1].result else {
        panic!("missing root must remain an explicit empty invocation");
    };
    let ReadSequenceFlatResult::BatchGet { responses } = &execution.rows[2].result else {
        panic!("third root should return a batch response");
    };
    let batch_items = responses.get(&table_name).expect("batch table response");
    assert_eq!(batch_items.len(), 1);
    assert_eq!(
        batch_items[0].get("pk"),
        Some(&AttributeValue::S("b".to_string()))
    );

    let ordinary_first = provider
        .get_item(table_name.clone(), key("a"), false)
        .await
        .expect("ordinary first")
        .expect("ordinary first item")
        .to_attribute_map()
        .expect("ordinary first decode");
    assert_eq!(first.to_hashmap(), ordinary_first);
    assert!(
        provider
            .get_item(table_name.clone(), key("not-present"), false)
            .await
            .expect("ordinary missing")
            .is_none()
    );
    let ordinary_batch = provider
        .batch_get_item(batch)
        .await
        .expect("ordinary batch")
        .responses
        .expect("ordinary batch response")
        .remove(&table_name)
        .expect("ordinary batch table");
    assert_eq!(
        batch_items
            .iter()
            .map(storage_types::AttributeMap::to_hashmap)
            .collect::<Vec<_>>(),
        ordinary_batch
            .iter()
            .map(storage_types::AttributeMap::to_hashmap)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn generated_get_corpus_matches_ordinary_reads() {
    // Keep this corpus deterministic and large enough to exercise request
    // planning, SQL lowering, decoding, and the ordinary fallback boundary
    // repeatedly.  The case formula is intentionally fixed so a failure is
    // replayable without a random seed or an external fixture.
    const CASES: usize = 16_384;
    let provider = provider_with_items().await;

    for case in 0..CASES {
        let id = match (case.wrapping_mul(17) ^ (case >> 3)) % 5 {
            0 | 3 => "a",
            1 => "b",
            _ => "missing",
        };
        let get_request = GetItemRequest::new(TableName::new("read_sequence_items"), key(id));
        let read_request = request(vec![ReadSequenceNode {
            name: "get".to_string(),
            operation: ReadSequenceNodeOperation::Get(get_request.clone()),
            inputs: None,
            iterate: None,
            after: None,
        }]);
        let plan = plan_read_sequence(&read_request).expect("generated plan");
        let compiled = provider
            .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
            .await
            .unwrap_or_else(|error| panic!("compiled case {case}: {:?}", error.to_enum()));
        let ReadSequenceExecution::Executed(compiled) = compiled else {
            panic!("generated get case {case} unexpectedly fell back");
        };
        let ReadSequenceFlatResult::Get {
            item: compiled_item,
        } = &compiled.rows[0].result
        else {
            panic!("generated case {case} returned the wrong result shape");
        };
        let compiled_item = compiled_item.as_ref().map(|item| item.to_hashmap());

        let ordinary_item = provider
            .get_item(
                get_request.table_name.clone(),
                get_request.key.clone(),
                false,
            )
            .await
            .unwrap_or_else(|error| panic!("ordinary case {case}: {:?}", error.to_enum()));
        let ordinary_item = ordinary_item
            .as_ref()
            .map(|item| item.to_attribute_map())
            .transpose()
            .unwrap_or_else(|error| panic!("ordinary decode case {case}: {:?}", error.to_enum()));

        assert_eq!(compiled_item, ordinary_item, "case {case} key={id}");
    }
}

#[tokio::test]
async fn generated_mixed_root_corpus_matches_ordinary_reads() {
    // Exercise the provider-owned independent-root lowering across a stable,
    // replayable corpus instead of only proving one hand-written mixed plan.
    // Every case contains both a point read and a batch read; the permutations
    // cover node order, key order, missing items, and duplicate table entries
    // without relying on an external generator or a non-deterministic seed.
    const CASES: usize = 4_096;
    let provider = provider_with_items().await;
    let table_name = TableName::new("read_sequence_items");

    for case in 0..CASES {
        let point_id = match (case.wrapping_mul(11) ^ (case >> 2)) % 4 {
            0 => "a",
            1 => "b",
            _ => "missing",
        };
        let batch_ids = match case % 4 {
            0 => ["a", "b", "missing"],
            1 => ["missing", "b", "a"],
            2 => ["b", "missing", "a"],
            _ => ["a", "missing", "b"],
        };
        let batch_request = BatchGetItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                storage_types::KeysAndAttributes {
                    keys: batch_ids.iter().map(|id| key(id)).collect(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: None,
                },
            )]),
            return_consumed_capacity: None,
        };
        let point_node = ReadSequenceNode {
            name: "point".to_string(),
            operation: ReadSequenceNodeOperation::Get(GetItemRequest::new(
                table_name.clone(),
                key(point_id),
            )),
            inputs: None,
            iterate: None,
            after: None,
        };
        let batch_node = ReadSequenceNode {
            name: "batch".to_string(),
            operation: ReadSequenceNodeOperation::BatchGet(batch_request.clone()),
            inputs: None,
            iterate: None,
            after: None,
        };
        let nodes = if case & 1 == 0 {
            vec![point_node, batch_node]
        } else {
            vec![batch_node, point_node]
        };
        let plan = plan_read_sequence(&request(nodes.clone())).unwrap_or_else(|error| {
            panic!("generated mixed case {case} failed to plan: {error:?}")
        });
        let execution = provider
            .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
            .await
            .unwrap_or_else(|error| panic!("compiled mixed case {case}: {:?}", error.to_enum()));
        let ReadSequenceExecution::Executed(execution) = execution else {
            panic!("generated mixed case {case} unexpectedly fell back")
        };
        assert_eq!(execution.rows.len(), 2, "case {case}");
        assert_eq!(execution.next_continuation, None, "case {case}");

        for (index, node) in nodes.iter().enumerate() {
            let row = &execution.rows[index];
            assert_eq!(row.node.index(), index, "case {case} row order");
            match &node.operation {
                ReadSequenceNodeOperation::Get(get_request) => {
                    let expected = provider
                        .get_item(
                            get_request.table_name.clone(),
                            get_request.key.clone(),
                            false,
                        )
                        .await
                        .unwrap_or_else(|error| {
                            panic!("ordinary mixed get case {case}: {:?}", error.to_enum())
                        });
                    let expected = expected
                        .map(|item| item.to_attribute_map().map(Into::into))
                        .transpose()
                        .unwrap_or_else(|error| {
                            panic!(
                                "ordinary mixed get decode case {case}: {:?}",
                                error.to_enum()
                            )
                        });
                    let ReadSequenceFlatResult::Get { item } = &row.result else {
                        panic!("case {case} point shape")
                    };
                    assert_eq!(
                        item.as_ref().map(storage_types::AttributeMap::to_hashmap),
                        expected
                            .as_ref()
                            .map(storage_types::AttributeMap::to_hashmap),
                        "case {case} point result"
                    );
                }
                ReadSequenceNodeOperation::BatchGet(batch_request) => {
                    let expected = provider
                        .batch_get_item(batch_request.clone())
                        .await
                        .unwrap_or_else(|error| {
                            panic!("ordinary mixed batch case {case}: {:?}", error.to_enum())
                        })
                        .responses
                        .expect("ordinary batch responses");
                    let ReadSequenceFlatResult::BatchGet { responses } = &row.result else {
                        panic!("case {case} batch shape")
                    };
                    let normalize =
                        |responses: &HashMap<TableName, Vec<storage_types::AttributeMap>>| {
                            responses
                                .iter()
                                .map(|(table, items)| {
                                    let mut normalized = items
                                        .iter()
                                        .map(storage_types::AttributeMap::to_hashmap)
                                        .collect::<Vec<_>>();
                                    normalized.sort_by(|left, right| {
                                        left.get("pk")
                                            .map(|value| value.inner_string().unwrap_or_default())
                                            .cmp(&right.get("pk").map(|value| {
                                                value.inner_string().unwrap_or_default()
                                            }))
                                    });
                                    (table.clone(), normalized)
                                })
                                .collect::<HashMap<_, _>>()
                        };
                    assert_eq!(
                        normalize(responses),
                        normalize(&expected),
                        "case {case} batch result"
                    );
                }
                ReadSequenceNodeOperation::Query(_) => {
                    unreachable!("generated mixed corpus only contains point roots")
                }
            }
        }
    }
}

#[tokio::test]
async fn compiled_batch_get_preserves_input_order_and_missing_items() {
    let provider = provider_with_items().await;
    let mut request_items = HashMap::new();
    request_items.insert(
        TableName::new("read_sequence_items"),
        storage_types::KeysAndAttributes {
            keys: vec![key("b"), key("missing"), key("a")].into(),
            attributes_to_get: None,
            projection_expression: None,
            expression_attribute_names: None,
            consistent_read: None,
        },
    );
    let request = request(vec![ReadSequenceNode {
        name: "root".to_string(),
        operation: ReadSequenceNodeOperation::BatchGet(BatchGetItemRequest {
            request_items,
            return_consumed_capacity: None,
        }),
        inputs: None,
        iterate: None,
        after: None,
    }]);
    let plan = plan_read_sequence(&request).expect("plan");
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .unwrap_or_else(|error| panic!("execute: {:?}", error.to_enum()));
    let storage_provider::ReadSequenceExecution::Executed(execution) = execution else {
        panic!("sqlite batch get should compile");
    };
    let storage_provider::ReadSequenceFlatResult::BatchGet { responses } =
        &execution.rows[0].result
    else {
        panic!("expected batch result");
    };
    let items = responses
        .get(&TableName::new("read_sequence_items"))
        .expect("table response");
    assert_eq!(items.len(), 2);
    assert_eq!(
        items[0].get("pk"),
        Some(&AttributeValue::S("b".to_string()))
    );
    assert_eq!(
        items[1].get("pk"),
        Some(&AttributeValue::S("a".to_string()))
    );
}

#[tokio::test]
async fn hash_only_query_compiles_without_emitting_a_cursor() {
    let provider = provider_with_items().await;
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("a".to_string()));
    let mut query = QueryRequest::new(
        TableName::new("read_sequence_items"),
        "pk = :pk".to_string(),
    );
    query.expression_attribute_values = Some(values);
    query.limit = Some(1);
    let request = request(vec![ReadSequenceNode {
        name: "root".to_string(),
        operation: ReadSequenceNodeOperation::Query(query),
        inputs: None,
        iterate: None,
        after: None,
    }]);
    let plan = plan_read_sequence(&request).expect("plan");
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .unwrap_or_else(|error| panic!("execute: {:?}", error.to_enum()));
    let ReadSequenceExecution::Executed(execution) = execution else {
        panic!("hash-only query should compile");
    };
    assert!(matches!(
        &execution.rows[0].result,
        ReadSequenceFlatResult::Query {
            items,
            last_evaluated_key: None,
            ..
        } if items.len() == 1
    ));
}

#[tokio::test]
async fn compiled_bounded_query_uses_a_stable_cursor_for_the_next_page() {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("provider");
    provider.initialize_storage().await.expect("initialize");
    provider
        .create_table(&CreateTableRequest::new(
            TableName::new("read_sequence_query_items"),
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
        ))
        .await
        .expect("create table");
    for sort_key in ["a", "b"] {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("group".to_string()));
        item.insert("sk".to_string(), AttributeValue::S(sort_key.to_string()));
        item.insert("value".to_string(), AttributeValue::S(sort_key.to_string()));
        provider
            .put_item(
                TableName::new("read_sequence_query_items"),
                item,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("put item");
    }
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("group".to_string()));
    let mut query = QueryRequest::new(
        TableName::new("read_sequence_query_items"),
        "pk = :pk".to_string(),
    );
    query.expression_attribute_values = Some(values);
    query.limit = Some(2);
    query.scan_index_forward = Some(true);
    let request = request(vec![ReadSequenceNode {
        name: "root".to_string(),
        operation: ReadSequenceNodeOperation::Query(query),
        inputs: None,
        iterate: None,
        after: None,
    }]);
    let plan = plan_read_sequence(&request).expect("plan");
    let zero_budget = provider
        .execute_read_sequence_plan_with_budget(
            &plan,
            ReadSequenceConsistency::Eventual,
            None,
            ReadSequenceExecutionBudget::bounded_items(0),
        )
        .await
        .expect("zero frontier is a provider result");
    assert!(matches!(
        zero_budget,
        ReadSequenceExecution::Unsupported(
            storage_provider::ReadSequenceUnsupportedReason::ParameterLimit
        )
    ));
    let first = provider
        .execute_read_sequence_plan_with_budget(
            &plan,
            ReadSequenceConsistency::Eventual,
            None,
            ReadSequenceExecutionBudget::bounded_items(1),
        )
        .await
        .unwrap_or_else(|error| panic!("execute first: {:?}", error.to_enum()));
    let storage_provider::ReadSequenceExecution::Executed(first) = first else {
        panic!("query should compile");
    };
    let storage_provider::ReadSequenceFlatResult::Query {
        items,
        last_evaluated_key,
        ..
    } = &first.rows[0].result
    else {
        panic!("expected query result");
    };
    assert_eq!(items.len(), 1);
    assert!(last_evaluated_key.is_some());
    let continuation = first.next_continuation.expect("query continuation");

    let second = provider
        .execute_read_sequence_plan_with_budget(
            &plan,
            ReadSequenceConsistency::Eventual,
            Some(&continuation),
            ReadSequenceExecutionBudget::bounded_items(1),
        )
        .await
        .unwrap_or_else(|error| panic!("execute second: {:?}", error.to_enum()));
    let storage_provider::ReadSequenceExecution::Executed(second) = second else {
        panic!("query continuation should compile");
    };
    let storage_provider::ReadSequenceFlatResult::Query { items, .. } = &second.rows[0].result
    else {
        panic!("expected query continuation result");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("sk"),
        Some(&AttributeValue::S("b".to_string()))
    );
    assert!(second.next_continuation.is_none());
}

#[tokio::test]
async fn file_backed_compiled_reads_match_ordinary_reads_and_have_eqp_scan_plans() {
    let tempdir = crate::sql_test_support::temp_dir("read-sequence");
    let database_path = tempdir.path().join("read_sequence_compiled.db");
    let provider = SQLiteStorageProvider::new_with_settings(
        database_path.to_str().expect("sqlite path"),
        storage_provider::SqliteSettings {
            force_file_backed_database: true,
            ..Default::default()
        },
    )
    .await
    .expect("file-backed provider");
    provider.initialize_storage().await.expect("initialize");
    let table_name = TableName::new("read_sequence_file_items");
    provider
        .create_table(&CreateTableRequest::new(
            table_name.clone(),
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
        ))
        .await
        .expect("create table");
    for (sort, value) in [("a", "Ada"), ("b", "Bob"), ("c", "Cid")] {
        provider
            .put_item(
                table_name.clone(),
                HashMap::from([
                    ("pk".to_string(), AttributeValue::S("group".to_string())),
                    ("sk".to_string(), AttributeValue::S(sort.to_string())),
                    ("value".to_string(), AttributeValue::S(value.to_string())),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("put item");
    }

    let get_request = GetItemRequest::new(table_name.clone(), key("group"));
    let get_request = GetItemRequest {
        key: KeyAttributes::from([
            ("pk".to_string(), AttributeValue::S("group".to_string())),
            ("sk".to_string(), AttributeValue::S("b".to_string())),
        ]),
        ..get_request
    };
    let get_node = ReadSequenceNode {
        name: "get".to_string(),
        operation: ReadSequenceNodeOperation::Get(get_request.clone()),
        inputs: None,
        iterate: None,
        after: None,
    };
    let get_plan = plan_read_sequence(&request(vec![get_node.clone()])).expect("get plan");
    let get_execution = provider
        .execute_read_sequence_plan(&get_plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("compiled get");
    let ReadSequenceExecution::Executed(get_execution) = get_execution else {
        panic!("file-backed get should compile");
    };
    let ReadSequenceFlatResult::Get {
        item: Some(compiled_get),
    } = &get_execution.rows[0].result
    else {
        panic!("compiled file-backed get item");
    };
    let ordinary_get: storage_types::AttributeMap = provider
        .get_item(table_name.clone(), get_request.key.clone(), false)
        .await
        .expect("ordinary get")
        .expect("ordinary get item")
        .to_attribute_map()
        .expect("ordinary get decode")
        .into();
    assert_eq!(compiled_get.to_hashmap(), ordinary_get.to_hashmap());
    let (get_metadata, _) = sqlite_read_sequence_metadata(&provider, &get_node, None)
        .await
        .expect("get metadata")
        .expect("get metadata shape");
    let get_statement = compile_sqlite_read_sequence_statement(&get_plan, &get_metadata)
        .expect("compile get")
        .expect("get statement");
    assert_single_statement(&get_statement);
    let get_eqp = explain_query_plan(&provider, &get_statement).await;
    assert!(get_eqp.iter().any(|detail| detail.contains("SEARCH")));
    let batch_request = BatchGetItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            storage_types::KeysAndAttributes {
                keys: vec![
                    KeyAttributes::from([
                        ("pk".to_string(), AttributeValue::S("group".to_string())),
                        ("sk".to_string(), AttributeValue::S("c".to_string())),
                    ]),
                    KeyAttributes::from([
                        ("pk".to_string(), AttributeValue::S("group".to_string())),
                        ("sk".to_string(), AttributeValue::S("missing".to_string())),
                    ]),
                    KeyAttributes::from([
                        ("pk".to_string(), AttributeValue::S("group".to_string())),
                        ("sk".to_string(), AttributeValue::S("a".to_string())),
                    ]),
                ]
                .into(),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
                consistent_read: None,
            },
        )]),
        return_consumed_capacity: None,
    };
    let batch_node = ReadSequenceNode {
        name: "batch".to_string(),
        operation: ReadSequenceNodeOperation::BatchGet(batch_request.clone()),
        inputs: None,
        iterate: None,
        after: None,
    };
    let batch_plan = plan_read_sequence(&request(vec![batch_node.clone()])).expect("batch plan");
    let batch_execution = provider
        .execute_read_sequence_plan(&batch_plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("compiled batch");
    let ReadSequenceExecution::Executed(batch_execution) = batch_execution else {
        panic!("file-backed batch should compile");
    };
    let ReadSequenceFlatResult::BatchGet { responses } = &batch_execution.rows[0].result else {
        panic!("compiled file-backed batch response");
    };
    let compiled_batch = responses.get(&table_name).expect("compiled batch table");
    let ordinary_batch: Vec<storage_types::AttributeMap> = provider
        .batch_get_item(batch_request.clone())
        .await
        .expect("ordinary batch")
        .responses
        .expect("ordinary batch response")
        .remove(&table_name)
        .expect("ordinary batch table")
        .into_iter()
        .collect::<Vec<_>>();
    let mut compiled_batch_normalized = compiled_batch.clone();
    sort_by_sort_key(&mut compiled_batch_normalized);
    let mut ordinary_batch_normalized = ordinary_batch.clone();
    sort_by_sort_key(&mut ordinary_batch_normalized);
    assert_eq!(
        compiled_batch_normalized
            .iter()
            .map(storage_types::AttributeMap::to_hashmap)
            .collect::<Vec<_>>(),
        ordinary_batch_normalized
            .iter()
            .map(storage_types::AttributeMap::to_hashmap)
            .collect::<Vec<_>>()
    );
    let (batch_metadata, _) = sqlite_read_sequence_metadata(&provider, &batch_node, None)
        .await
        .expect("batch metadata")
        .expect("batch metadata shape");
    let batch_statement = compile_sqlite_read_sequence_statement(&batch_plan, &batch_metadata)
        .expect("compile batch")
        .expect("batch statement");
    assert_single_statement(&batch_statement);
    let batch_eqp = explain_query_plan(&provider, &batch_statement).await;
    assert!(batch_eqp.iter().any(|detail| detail.contains("SEARCH")));

    let mut query = QueryRequest::new(table_name.clone(), "pk = :pk".to_string());
    query.expression_attribute_values = Some(HashMap::from([(
        ":pk".to_string(),
        AttributeValue::S("group".to_string()),
    )]));
    query.limit = Some(2);
    query.scan_index_forward = Some(true);
    let query_node = ReadSequenceNode {
        name: "query".to_string(),
        operation: ReadSequenceNodeOperation::Query(query.clone()),
        inputs: None,
        iterate: None,
        after: None,
    };
    let query_plan = plan_read_sequence(&request(vec![query_node.clone()])).expect("query plan");
    let query_execution = provider
        .execute_read_sequence_plan(&query_plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("compiled query");
    let ReadSequenceExecution::Executed(query_execution) = query_execution else {
        panic!("file-backed query should compile");
    };
    let ReadSequenceFlatResult::Query { items, .. } = &query_execution.rows[0].result else {
        panic!("compiled file-backed query response");
    };
    let ordinary_query: Vec<storage_types::AttributeMap> = provider
        .query_table(&storage_types::QueryTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: query.expression_attribute_values.clone(),
            projection_expression: None,
            limit: Some(2),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: false,
        })
        .await
        .expect("ordinary query")
        .0
        .into_iter()
        .map(storage_types::AttributeMap::from)
        .collect();
    assert_eq!(
        items
            .iter()
            .map(storage_types::AttributeMap::to_hashmap)
            .collect::<Vec<_>>(),
        ordinary_query
            .iter()
            .map(storage_types::AttributeMap::to_hashmap)
            .collect::<Vec<_>>()
    );
    let (query_metadata, _) = sqlite_read_sequence_metadata(&provider, &query_node, None)
        .await
        .expect("query metadata")
        .expect("query metadata shape");
    let query_statement = compile_sqlite_read_sequence_statement(&query_plan, &query_metadata)
        .expect("compile query")
        .expect("query statement");
    assert_single_statement(&query_statement);
    let query_eqp = explain_query_plan(&provider, &query_statement).await;
    assert!(query_eqp.iter().any(|detail| detail.contains("SEARCH")));

    const QUERY_CASES: usize = 1_024;
    for case in 0..QUERY_CASES {
        let partition = if case % 5 == 0 { "missing" } else { "group" };
        let limit = Some((case % 4 + 1) as u32);
        let mut generated_query = QueryRequest::new(table_name.clone(), "pk = :pk".to_string());
        generated_query.expression_attribute_values = Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S(partition.to_string()),
        )]));
        generated_query.limit = limit;
        generated_query.scan_index_forward = Some(true);
        let generated_node = ReadSequenceNode {
            name: "query".to_string(),
            operation: ReadSequenceNodeOperation::Query(generated_query.clone()),
            inputs: None,
            iterate: None,
            after: None,
        };
        let generated_plan = plan_read_sequence(&request(vec![generated_node]))
            .unwrap_or_else(|error| panic!("generated sqlite query case {case}: {error:?}"));
        let generated = provider
            .execute_read_sequence_plan(&generated_plan, ReadSequenceConsistency::Eventual, None)
            .await
            .unwrap_or_else(|error| {
                panic!("compiled sqlite query case {case}: {:?}", error.to_enum())
            });
        let ReadSequenceExecution::Executed(generated) = generated else {
            panic!("generated sqlite query case {case} unexpectedly fell back");
        };
        let ReadSequenceFlatResult::Query { items, .. } = &generated.rows[0].result else {
            panic!("generated sqlite query case {case} returned the wrong result shape");
        };
        let ordinary = provider
            .query_table(&storage_types::QueryTableRequest {
                table_name: table_name.clone(),
                index_name: None,
                key_condition_expression: "pk = :pk".to_string(),
                expression_attribute_names: None,
                expression_attribute_values: generated_query.expression_attribute_values.clone(),
                projection_expression: None,
                limit,
                exclusive_start_key: None,
                scan_index_forward: Some(true),
                consistent_read: false,
            })
            .await
            .unwrap_or_else(|error| {
                panic!("ordinary sqlite query case {case}: {:?}", error.to_enum())
            })
            .0
            .into_iter()
            .map(storage_types::AttributeMap::from)
            .collect::<Vec<_>>();
        assert_eq!(
            items
                .iter()
                .map(storage_types::AttributeMap::to_hashmap)
                .collect::<Vec<_>>(),
            ordinary
                .iter()
                .map(storage_types::AttributeMap::to_hashmap)
                .collect::<Vec<_>>(),
            "generated sqlite query case {case} partition={partition} limit={limit:?}"
        );
    }

    provider
        .delete_table(&table_name)
        .await
        .expect("delete table");
}
