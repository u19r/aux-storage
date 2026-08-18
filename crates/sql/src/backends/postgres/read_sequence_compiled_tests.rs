use std::collections::HashMap;

use storage_provider::{ReadSequenceExecution, ReadSequenceFlatResult, StorageProvider as _};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, BillingMode, CreateTableRequest,
    GetItemRequest, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, KeysAndAttributes,
    QueryRequest, QueryTableRequest, ReadSequenceConsistency, ReadSequenceNode,
    ReadSequenceNodeOperation, ReadSequenceRequest, TableName, context::WrappedError as _,
    plan_read_sequence,
};

use crate::{
    backends::postgres::{
        PostgresStorageProvider,
        storage_provider_impl::{
            compile_postgres_read_sequence_statement, postgres_batch_read_sequence_metadata,
            postgres_query_read_sequence_metadata, postgres_read_sequence_metadata,
        },
    },
    sql_test_support::{mapped_gsi_parent_item, mapped_gsi_read_sequence_request},
};

fn postgres_test_dsn() -> Option<String> {
    std::env::var("TEST_POSTGRES_DSN")
        .ok()
        .or_else(|| std::env::var("CUCUMBER_POSTGRES_DSN").ok())
}

fn read_sequence_request(nodes: Vec<ReadSequenceNode>) -> ReadSequenceRequest {
    ReadSequenceRequest::new(nodes)
}

fn node(name: &str, operation: ReadSequenceNodeOperation) -> ReadSequenceNode {
    ReadSequenceNode {
        name: name.to_string(),
        operation,
        inputs: None,
        iterate: None,
        after: None,
    }
}

fn key(partition: &str, sort: &str) -> KeyAttributes {
    KeyAttributes::from([
        ("pk".to_string(), AttributeValue::S(partition.to_string())),
        ("sk".to_string(), AttributeValue::S(sort.to_string())),
    ])
}

fn table_request(table_name: &TableName) -> CreateTableRequest {
    let mut request = CreateTableRequest::new(
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
        BillingMode::PayPerRequest,
    );
    request.max_indexers = storage_types::MaxIndexers::try_new(2).expect("capacity");
    request
}

async fn provider_with_items() -> Option<(PostgresStorageProvider, TableName)> {
    let dsn = postgres_test_dsn()?;
    // The local CI fixture uses a Unix socket without TLS.  Operators can
    // provide a TLS DSN through the existing lifecycle tests instead.
    let provider = PostgresStorageProvider::new_with_tls(&dsn, 4, 1, false)
        .await
        .unwrap_or_else(|error| panic!("postgres provider: {error:?}"));
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");

    let table_name = TableName::new(&format!(
        "pg_read_sequence_{}",
        uuid::Uuid::now_v7().simple()
    ));
    provider
        .create_table(&table_request(&table_name))
        .await
        .unwrap_or_else(|error| panic!("create table: {:?}", error.to_enum()));
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
    Some((provider, table_name))
}

#[tokio::test]
async fn compiled_postgres_get_reconstructs_indexer_slots() {
    let Some((provider, table_name)) = provider_with_items().await else {
        return;
    };
    let mut put = storage_types::PutItemRequest::new(
        table_name.clone(),
        HashMap::from([
            ("pk".to_string(), AttributeValue::S("group".to_string())),
            ("sk".to_string(), AttributeValue::S("indexed".to_string())),
            (
                "customer_id".to_string(),
                AttributeValue::S("customer-1".to_string()),
            ),
        ]),
    );
    put.indexers = Some(vec!["customer_id".to_string(), "optional_id".to_string()]);
    provider
        .put_item_request(put)
        .await
        .expect("put indexed item");
    let plan = plan_read_sequence(&read_sequence_request(vec![node(
        "indexed",
        ReadSequenceNodeOperation::Get(GetItemRequest::new(
            table_name.clone(),
            key("group", "indexed"),
        )),
    )]))
    .expect("plan");

    let ReadSequenceExecution::Executed(executed) = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("compiled indexed get")
    else {
        panic!("indexed postgres get should compile");
    };
    let ReadSequenceFlatResult::Get { item: Some(item) } = &executed.rows[0].result else {
        panic!("indexed postgres item");
    };
    assert_eq!(
        item.get("customer_id"),
        Some(&AttributeValue::S("customer-1".to_string()))
    );
    assert!(item.get("optional_id").is_none());
    provider
        .delete_table(&table_name)
        .await
        .expect("delete table");
}

#[tokio::test]
async fn compiled_postgres_mapped_indexer_join_handles_present_and_nil_slots() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new_with_tls(&dsn, 4, 1, false)
        .await
        .expect("postgres provider");
    provider.initialize_storage().await.expect("initialize");
    let suffix = uuid::Uuid::now_v7().simple();
    let parents = TableName::new(&format!("pg_mapped_parents_{suffix}"));
    let children = TableName::new(&format!("pg_mapped_children_{suffix}"));
    provider
        .create_table(&table_request(&parents))
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
            BillingMode::PayPerRequest,
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

    let ReadSequenceExecution::Executed(executed) = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("mapped PostgreSQL execution")
    else {
        panic!("mapped PostgreSQL shape should compile");
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
    provider
        .delete_table(&parents)
        .await
        .expect("delete parents");
    provider
        .delete_table(&children)
        .await
        .expect("delete children");
}

#[tokio::test]
async fn given_keys_only_gsi_when_mapping_indexer_then_postgres_uses_projected_row() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new_with_tls(&dsn, 4, 1, false)
        .await
        .expect("postgres provider")
        .with_immediate_gsi_consistency(true);
    provider.initialize_storage().await.expect("initialize");
    let suffix = uuid::Uuid::now_v7().simple();
    let parents = TableName::new(&format!("pg_mapped_gsi_parents_{suffix}"));
    let children = TableName::new(&format!("pg_mapped_gsi_children_{suffix}"));
    provider
        .create_table(
            &crate::sql_test_support::mapped_gsi_table_request_with_projection(
                parents.clone(),
                storage_types::ProjectionType::KeysOnly,
            ),
        )
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
            BillingMode::PayPerRequest,
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
        .unwrap_or_else(|error| panic!("mapped GSI execution: {:?}", error.to_enum()))
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
    provider
        .delete_table(&parents)
        .await
        .expect("delete parents");
    provider
        .delete_table(&children)
        .await
        .expect("delete children");
}

async fn explain(
    provider: &PostgresStorageProvider,
    statement: &storage_provider::ReadSequenceSqlStatement,
    has_limit: bool,
) -> String {
    let client = provider
        .acquire_client("read_sequence_explain")
        .await
        .expect("postgres explain client");
    let values = statement
        .parameters
        .iter()
        .enumerate()
        .map(|(index, value)| {
            if has_limit && index + 1 == statement.parameters.len() {
                PostgresStorageProvider::scalar_key_value(value, "read_sequence_explain")
                    .expect("scalar explain limit")
                    .parse::<i32>()
                    .map(|value| {
                        Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Send + Sync>
                    })
                    .expect("integer explain limit")
            } else {
                Box::new(
                    PostgresStorageProvider::scalar_key_value(value, "read_sequence_explain")
                        .expect("scalar explain parameter"),
                ) as Box<dyn tokio_postgres::types::ToSql + Send + Sync>
            }
        })
        .collect::<Vec<_>>();
    let params = values
        .iter()
        .map(|value| value.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect::<Vec<_>>();
    let rows = client
        .query(&format!("EXPLAIN (COSTS OFF) {}", statement.sql), &params)
        .await
        .expect("EXPLAIN compiled read sequence statement");
    rows.iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_single_statement(statement: &storage_provider::ReadSequenceSqlStatement) {
    assert!(!statement.sql.trim().is_empty(), "compiled SQL is empty");
    assert!(
        !statement.sql.contains(';'),
        "compiled read sequence must be one SQL statement, got a statement separator: {}",
        statement.sql
    );
}

#[tokio::test]
async fn compiled_get_batch_and_query_match_ordinary_reads_and_explain() {
    let Some((provider, table_name)) = provider_with_items().await else {
        return;
    };

    let get_request = GetItemRequest::new(table_name.clone(), key("group", "b"));
    let get_plan = plan_read_sequence(&read_sequence_request(vec![node(
        "get",
        ReadSequenceNodeOperation::Get(get_request.clone()),
    )]))
    .expect("get plan");
    let get_execution = provider
        .execute_read_sequence_plan(&get_plan, ReadSequenceConsistency::Eventual, None)
        .await
        .unwrap_or_else(|error| panic!("compiled get execution: {:?}", error.to_enum()));
    let ReadSequenceExecution::Executed(get_execution) = get_execution else {
        panic!("postgres get should compile");
    };
    let ReadSequenceFlatResult::Get {
        item: Some(compiled_get),
    } = &get_execution.rows[0].result
    else {
        panic!("compiled get did not return an item");
    };
    let ordinary_get: storage_types::AttributeMap = provider
        .get_item(table_name.clone(), key("group", "b"), false)
        .await
        .expect("ordinary get")
        .expect("ordinary get item")
        .to_attribute_map()
        .expect("ordinary get decode")
        .into();
    assert_eq!(compiled_get.to_hashmap(), ordinary_get.to_hashmap());

    let strong_execution = provider
        .execute_read_sequence_plan(&get_plan, ReadSequenceConsistency::Strong, None)
        .await
        .expect("strong reads should remain on the ordinary path");
    assert!(matches!(
        strong_execution,
        ReadSequenceExecution::Unsupported(
            storage_provider::ReadSequenceUnsupportedReason::OperationShape
        )
    ));

    let get_info = provider
        .get_table_info(&table_name)
        .await
        .expect("get table metadata");
    let get_metadata = postgres_read_sequence_metadata(
        &get_request,
        &get_info,
        storage_provider::ReadSequenceSqlShape::Get,
    )
    .expect("get metadata")
    .expect("get metadata shape");
    let get_statement = compile_postgres_read_sequence_statement(&get_plan, &get_metadata)
        .expect("compile get")
        .expect("get statement");
    assert_single_statement(&get_statement);
    let get_explain = explain(&provider, &get_statement, false).await;
    assert!(
        get_explain.contains("Scan"),
        "unexpected get plan: {get_explain}"
    );

    let batch_keys = vec![
        key("group", "c"),
        key("group", "missing"),
        key("group", "a"),
    ];
    let batch_request = BatchGetItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            KeysAndAttributes {
                keys: batch_keys.clone().into(),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
                consistent_read: None,
            },
        )]),
        return_consumed_capacity: None,
    };
    let batch_plan = plan_read_sequence(&read_sequence_request(vec![node(
        "batch",
        ReadSequenceNodeOperation::BatchGet(batch_request.clone()),
    )]))
    .expect("batch plan");
    let batch_execution = provider
        .execute_read_sequence_plan(&batch_plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("compiled batch execution");
    let ReadSequenceExecution::Executed(batch_execution) = batch_execution else {
        panic!("postgres batch should compile");
    };
    let ReadSequenceFlatResult::BatchGet { responses } = &batch_execution.rows[0].result else {
        panic!("compiled batch did not return responses");
    };
    let compiled_batch = responses.get(&table_name).expect("compiled batch table");
    let ordinary_batch: Vec<storage_types::AttributeMap> = provider
        .batch_get_item(batch_request.clone())
        .await
        .expect("ordinary batch")
        .responses
        .expect("ordinary batch responses")
        .remove(&table_name)
        .expect("ordinary batch table")
        .into_iter()
        .map(|item| {
            item.to_attribute_map()
                .expect("ordinary batch decode")
                .into()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        normalized_items(compiled_batch),
        normalized_items(&ordinary_batch)
    );

    let batch_metadata = postgres_batch_read_sequence_metadata(
        &table_name,
        batch_request
            .request_items
            .get(&table_name)
            .expect("batch metadata request"),
        &get_info,
    )
    .expect("batch metadata")
    .expect("batch metadata shape");
    let batch_statement = compile_postgres_read_sequence_statement(&batch_plan, &batch_metadata)
        .expect("compile batch")
        .expect("batch statement");
    assert_single_statement(&batch_statement);
    let batch_explain = explain(&provider, &batch_statement, false).await;
    assert!(
        batch_explain.contains("Scan"),
        "unexpected batch plan: {batch_explain}"
    );

    let query_request = {
        let mut request = QueryRequest::new(table_name.clone(), "pk = :pk".to_string());
        request.expression_attribute_values = Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("group".to_string()),
        )]));
        request.limit = Some(2);
        request.scan_index_forward = Some(true);
        request
    };
    let query_plan = plan_read_sequence(&read_sequence_request(vec![node(
        "query",
        ReadSequenceNodeOperation::Query(query_request.clone()),
    )]))
    .expect("query plan");
    let query_execution = provider
        .execute_read_sequence_plan(&query_plan, ReadSequenceConsistency::Eventual, None)
        .await
        .unwrap_or_else(|error| panic!("compiled query execution: {:?}", error.to_enum()));
    let ReadSequenceExecution::Executed(query_execution) = query_execution else {
        panic!("postgres query should compile");
    };
    let ReadSequenceFlatResult::Query { items, .. } = &query_execution.rows[0].result else {
        panic!("compiled query did not return items");
    };
    let ordinary_query: Vec<storage_types::AttributeMap> = provider
        .query_table(&QueryTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: query_request.expression_attribute_values.clone(),
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
        .map(|item| {
            item.to_attribute_map()
                .expect("ordinary query decode")
                .into()
        })
        .collect::<Vec<_>>();
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
    assert_eq!(items.len(), 2);

    let query_metadata = postgres_query_read_sequence_metadata(&query_request, &get_info, None)
        .expect("query metadata")
        .expect("query metadata shape");
    let query_statement = compile_postgres_read_sequence_statement(&query_plan, &query_metadata)
        .expect("compile query")
        .expect("query statement");
    assert_single_statement(&query_statement);
    let query_explain = explain(&provider, &query_statement, true).await;
    assert!(
        query_explain.contains("Scan"),
        "unexpected query plan: {query_explain}"
    );

    provider
        .delete_table(&table_name)
        .await
        .expect("delete test table");
}

#[tokio::test]
async fn compiled_independent_roots_preserve_missing_gets_and_batch_rows() {
    let Some((provider, table_name)) = provider_with_items().await else {
        return;
    };
    let batch_request = BatchGetItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            KeysAndAttributes {
                keys: vec![key("group", "c"), key("group", "missing")].into(),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
                consistent_read: None,
            },
        )]),
        return_consumed_capacity: None,
    };
    let request = read_sequence_request(vec![
        node(
            "first",
            ReadSequenceNodeOperation::Get(GetItemRequest::new(
                table_name.clone(),
                key("group", "a"),
            )),
        ),
        node(
            "missing",
            ReadSequenceNodeOperation::Get(GetItemRequest::new(
                table_name.clone(),
                key("group", "missing"),
            )),
        ),
        node(
            "batch",
            ReadSequenceNodeOperation::BatchGet(batch_request.clone()),
        ),
    ]);
    let plan = plan_read_sequence(&request).expect("independent-root plan");
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .unwrap_or_else(|error| panic!("compiled roots: {:?}", error.to_enum()));
    let ReadSequenceExecution::Executed(execution) = execution else {
        panic!("independent roots should compile");
    };
    assert_eq!(execution.rows.len(), 3);
    let ReadSequenceFlatResult::Get { item: Some(first) } = &execution.rows[0].result else {
        panic!("first root should return an item");
    };
    assert_eq!(first.get("sk"), Some(&AttributeValue::S("a".to_string())));
    assert!(matches!(
        execution.rows[1].result,
        ReadSequenceFlatResult::Get { item: None }
    ));
    let ReadSequenceFlatResult::BatchGet { responses } = &execution.rows[2].result else {
        panic!("third root should return a batch response");
    };
    let items = responses.get(&table_name).expect("batch table response");
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("sk"),
        Some(&AttributeValue::S("c".to_string()))
    );
    provider
        .delete_table(&table_name)
        .await
        .expect("delete test table");
}

#[tokio::test]
async fn generated_get_corpus_matches_ordinary_reads() {
    const CASES: usize = 1_024;
    let Some((provider, table_name)) = provider_with_items().await else {
        return;
    };

    for case in 0..CASES {
        let sort = match (case.wrapping_mul(17) ^ (case >> 3)) % 5 {
            0 => "a",
            1 => "b",
            2 => "c",
            _ => "missing",
        };
        let get_request = GetItemRequest::new(table_name.clone(), key("group", sort));
        let request = read_sequence_request(vec![node(
            "get",
            ReadSequenceNodeOperation::Get(get_request.clone()),
        )]);
        let plan = plan_read_sequence(&request).unwrap_or_else(|error| {
            panic!("generated postgres case {case} failed to plan: {error:?}")
        });
        let execution = provider
            .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
            .await
            .unwrap_or_else(|error| {
                panic!("generated postgres case {case}: {:?}", error.to_enum())
            });
        let ReadSequenceExecution::Executed(execution) = execution else {
            panic!("generated postgres case {case} unexpectedly fell back")
        };
        let ReadSequenceFlatResult::Get { item } = &execution.rows[0].result else {
            panic!("generated postgres case {case} returned the wrong result shape")
        };
        let ordinary = provider
            .get_item(table_name.clone(), get_request.key.clone(), false)
            .await
            .unwrap_or_else(|error| panic!("ordinary postgres case {case}: {:?}", error.to_enum()))
            .map(|item| {
                item.to_attribute_map()
                    .unwrap_or_else(|error| panic!("ordinary postgres decode {case}: {error:?}"))
                    .into()
            });
        assert_eq!(
            item.as_ref().map(storage_types::AttributeMap::to_hashmap),
            ordinary
                .as_ref()
                .map(storage_types::AttributeMap::to_hashmap),
            "generated postgres case {case} sort={sort}"
        );
    }

    provider
        .delete_table(&table_name)
        .await
        .expect("delete generated postgres table");
}

fn normalized_items(items: &[storage_types::AttributeMap]) -> Vec<HashMap<String, AttributeValue>> {
    let mut normalized = items
        .iter()
        .map(storage_types::AttributeMap::to_hashmap)
        .collect::<Vec<_>>();
    normalized.sort_by(|left, right| {
        left.get("sk")
            .and_then(|value| value.inner_string().ok())
            .cmp(&right.get("sk").and_then(|value| value.inner_string().ok()))
    });
    normalized
}

#[tokio::test]
async fn generated_batch_get_corpus_matches_ordinary_reads() {
    const CASES: usize = 256;
    let Some((provider, table_name)) = provider_with_items().await else {
        return;
    };

    for case in 0..CASES {
        let batch_ids = match case % 4 {
            0 => ["a", "b", "missing"],
            1 => ["missing", "c", "a"],
            2 => ["c", "missing", "b"],
            _ => ["b", "a", "missing"],
        };
        let batch_request = BatchGetItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: batch_ids.iter().map(|sort| key("group", sort)).collect(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: None,
                },
            )]),
            return_consumed_capacity: None,
        };
        let plan = plan_read_sequence(&read_sequence_request(vec![node(
            "batch",
            ReadSequenceNodeOperation::BatchGet(batch_request.clone()),
        )]))
        .unwrap_or_else(|error| panic!("generated postgres batch case {case}: {error:?}"));
        let execution = provider
            .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
            .await
            .unwrap_or_else(|error| {
                panic!("compiled postgres batch case {case}: {:?}", error.to_enum())
            });
        let ReadSequenceExecution::Executed(execution) = execution else {
            panic!("generated postgres batch case {case} unexpectedly fell back");
        };
        let ReadSequenceFlatResult::BatchGet { responses } = &execution.rows[0].result else {
            panic!("generated postgres batch case {case} returned the wrong result shape");
        };
        let compiled = responses
            .get(&table_name)
            .unwrap_or_else(|| panic!("generated postgres batch case {case} missing table"));
        let ordinary = provider
            .batch_get_item(batch_request)
            .await
            .unwrap_or_else(|error| {
                panic!("ordinary postgres batch case {case}: {:?}", error.to_enum())
            })
            .responses
            .expect("ordinary batch responses")
            .remove(&table_name)
            .unwrap_or_else(|| panic!("ordinary postgres batch case {case} missing table"))
            .into_iter()
            .map(|item| {
                item.to_attribute_map()
                    .unwrap_or_else(|error| {
                        panic!("ordinary postgres batch decode {case}: {error:?}")
                    })
                    .into()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            normalized_items(compiled),
            normalized_items(&ordinary),
            "generated postgres batch case {case}"
        );
    }

    provider
        .delete_table(&table_name)
        .await
        .expect("delete generated postgres batch table");
}

#[tokio::test]
async fn generated_query_corpus_matches_ordinary_reads() {
    const CASES: usize = 256;
    let Some((provider, table_name)) = provider_with_items().await else {
        return;
    };

    for case in 0..CASES {
        let partition = if case % 5 == 0 { "missing" } else { "group" };
        let limit = Some((case % 4 + 1) as u32);
        let mut query_request = QueryRequest::new(table_name.clone(), "pk = :pk".to_string());
        query_request.expression_attribute_values = Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S(partition.to_string()),
        )]));
        query_request.limit = limit;
        query_request.scan_index_forward = Some(true);
        let plan = plan_read_sequence(&read_sequence_request(vec![node(
            "query",
            ReadSequenceNodeOperation::Query(query_request.clone()),
        )]))
        .unwrap_or_else(|error| panic!("generated postgres query case {case}: {error:?}"));
        let execution = provider
            .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
            .await
            .unwrap_or_else(|error| {
                panic!("compiled postgres query case {case}: {:?}", error.to_enum())
            });
        let ReadSequenceExecution::Executed(execution) = execution else {
            panic!("generated postgres query case {case} unexpectedly fell back");
        };
        let ReadSequenceFlatResult::Query { items, .. } = &execution.rows[0].result else {
            panic!("generated postgres query case {case} returned the wrong result shape");
        };
        let ordinary = provider
            .query_table(&QueryTableRequest {
                table_name: table_name.clone(),
                index_name: None,
                key_condition_expression: "pk = :pk".to_string(),
                expression_attribute_names: None,
                expression_attribute_values: query_request.expression_attribute_values.clone(),
                projection_expression: None,
                limit,
                exclusive_start_key: None,
                scan_index_forward: Some(true),
                consistent_read: false,
            })
            .await
            .unwrap_or_else(|error| {
                panic!("ordinary postgres query case {case}: {:?}", error.to_enum())
            })
            .0
            .into_iter()
            .map(|item| {
                item.to_attribute_map()
                    .unwrap_or_else(|error| {
                        panic!("ordinary postgres query decode {case}: {error:?}")
                    })
                    .into()
            })
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
            "generated postgres query case {case} partition={partition} limit={limit:?}"
        );
    }

    provider
        .delete_table(&table_name)
        .await
        .expect("delete generated postgres query table");
}
