use std::collections::HashMap;

use serde_json::json;
use storage::{DatabaseManager, PutItemInput};
use storage_types::{
    AttributeDefinition, AttributeMap, AttributeValue, CreateGlobalSecondaryIndex,
    CreateTableRequest, IndexName, KeyAttributeType, KeySchemaElement, KeyType, Projection,
    ProjectionType, TableName,
};

use crate::{
    routes::routes_tests::{
        create_string_key_table, default_conformance_backends, handle_query, seed_attributes,
        seed_item, seed_nested_projection_item,
    },
    types::Response,
};

struct DecodedQueryResponse {
    items: Option<Vec<storage_types::AttributeMap>>,
    count: u32,
    scanned_count: u32,
    last_evaluated_key: Option<storage_types::KeyAttributes>,
    consumed_capacity: Option<storage_types::ConsumedCapacity>,
}

fn expect_query_response(response: Response) -> DecodedQueryResponse {
    let Response::Query(response) = response else {
        panic!("Expected Query response");
    };
    DecodedQueryResponse {
        items: response.items.map(|items| {
            items
                .into_iter()
                .map(|item| item.into_attribute_map().expect("query item should decode"))
                .collect()
        }),
        count: response.count,
        scanned_count: response.scanned_count,
        last_evaluated_key: response.last_evaluated_key,
        consumed_capacity: response.consumed_capacity,
    }
}

async fn create_test_table(db: &DatabaseManager, table_name: &str) {
    create_string_key_table(db, table_name, "pk", None).await;
}

async fn create_test_table_hash_range(db: &DatabaseManager, table_name: &str) {
    create_string_key_table(db, table_name, "pk", Some("sk")).await;
}

async fn create_test_table_binary_hash_range(db: &DatabaseManager, table_name: &str) {
    let request = CreateTableRequest::new(
        TableName::new(table_name),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::B,
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

    db.create_table(&request).await.unwrap();
}

async fn create_test_table_hash_range_with_gsi(db: &DatabaseManager, table_name: &str) {
    let mut request = CreateTableRequest::new(
        TableName::new(table_name),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gpk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsk".to_string(),
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
    request.global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("ByGsi"),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gpk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]);

    db.create_table(&request).await.unwrap();
}

async fn create_test_table_with_keys_only_gsi(db: &DatabaseManager, table_name: &str) {
    let mut request = CreateTableRequest::new(
        TableName::new(table_name),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gpk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );
    request.global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("ByGsi"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gpk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::KeysOnly),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]);

    db.create_table(&request).await.unwrap();
}

async fn put_test_item(db: &DatabaseManager, table_name: &str, pk: &str, name: &str, age: u32) {
    seed_attributes(
        db,
        table_name,
        vec![
            ("pk", AttributeValue::S(pk.to_string())),
            ("name", AttributeValue::S(name.to_string())),
            ("age", AttributeValue::N(age.to_string())),
        ],
    )
    .await;
}

async fn put_test_item_hash_range(
    db: &DatabaseManager,
    table_name: &str,
    pk: &str,
    sk: &str,
    data: &str,
) {
    seed_attributes(
        db,
        table_name,
        vec![
            ("pk", AttributeValue::S(pk.to_string())),
            ("sk", AttributeValue::S(sk.to_string())),
            ("data", AttributeValue::S(data.to_string())),
        ],
    )
    .await;
}

async fn put_test_item_binary_hash_range(
    db: &DatabaseManager,
    table_name: &str,
    pk: &str,
    sk: &str,
) {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    item.insert("sk".to_string(), AttributeValue::B(sk.to_string()));
    item.insert("data".to_string(), AttributeValue::S("binary".to_string()));

    seed_item(
        db,
        PutItemInput::builder()
            .table_name(TableName::new(table_name))
            .item(item)
            .build(),
    )
    .await
    .unwrap();
}

async fn put_test_item_hash_range_with_gsi(
    db: &DatabaseManager,
    table_name: &str,
    pk: &str,
    sk: &str,
    gpk: &str,
    gsk: &str,
) {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    item.insert("sk".to_string(), AttributeValue::S(sk.to_string()));
    item.insert("gpk".to_string(), AttributeValue::S(gpk.to_string()));
    item.insert("gsk".to_string(), AttributeValue::S(gsk.to_string()));

    seed_item(
        db,
        PutItemInput::builder()
            .table_name(TableName::new(table_name))
            .item(item)
            .build(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn query_caps_source_page_by_logical_bytes_and_resume_token_continues() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "QueryBytePageTable").await;
        let data = "\\".repeat(300 * 1024);
        for sk in ["001", "002", "003", "004"] {
            put_test_item_hash_range(db.as_ref(), "QueryBytePageTable", "tenant#1", sk, &data)
                .await;
        }

        let first_payload = json!({
            "TableName": "QueryBytePageTable",
            "KeyConditionExpression": "pk = :pk_val",
            "ExpressionAttributeValues": {":pk_val": {"S": "tenant#1"}}
        });
        let first = handle_query(db.clone(), first_payload.try_into().unwrap())
            .await
            .unwrap_or_else(|err| panic!("{} first query page: {err:?}", backend.name));
        let first = expect_query_response(first);
        let first_items = first.items.expect("query should return items");
        assert!(
            serde_json::to_vec(&first_items).unwrap().len() > 1_024 * 1_024,
            "escaped response should exceed the removed encoded cutoff: {}",
            backend.name
        );
        assert_eq!(first_items.len(), 3, "{}", backend.name);
        assert_eq!(first.count, 3, "{}", backend.name);
        assert_eq!(first.scanned_count, 3, "{}", backend.name);
        assert_eq!(
            first_items
                .iter()
                .map(|item| item.get("sk").cloned())
                .collect::<Vec<_>>(),
            ["001", "002", "003"]
                .map(|sk| Some(AttributeValue::S(sk.into())))
                .to_vec(),
            "{}",
            backend.name
        );
        let resume = first
            .last_evaluated_key
            .expect("logical-budgeted query should return resume token");

        let second_payload = json!({
            "TableName": "QueryBytePageTable",
            "KeyConditionExpression": "pk = :pk_val",
            "ExpressionAttributeValues": {":pk_val": {"S": "tenant#1"}},
            "ExclusiveStartKey": resume
        });
        let second = handle_query(db, second_payload.try_into().unwrap())
            .await
            .unwrap_or_else(|err| panic!("{} second query page: {err:?}", backend.name));
        let second = expect_query_response(second);
        let second_items = second.items.expect("query should return remaining item");
        assert_eq!(second_items.len(), 1, "{}", backend.name);
        assert_eq!(second.count, 1, "{}", backend.name);
        assert_eq!(second.scanned_count, 1, "{}", backend.name);
        assert_eq!(
            second_items[0].get("sk"),
            Some(&AttributeValue::S("004".to_string())),
            "{}",
            backend.name
        );
        assert!(second.last_evaluated_key.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn query_projected_logical_page_uses_unprojected_source_keys() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "ProjectedQueryBytePageTable").await;
        let data = "x".repeat(300 * 1024);
        for sk in ["001", "002", "003", "004"] {
            put_test_item_hash_range(
                db.as_ref(),
                "ProjectedQueryBytePageTable",
                "tenant#1",
                sk,
                &data,
            )
            .await;
        }

        let first = expect_query_response(
            handle_query(
                db.clone(),
                json!({
                    "TableName": "ProjectedQueryBytePageTable",
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "tenant#1"}},
                    "ProjectionExpression": "#data",
                    "ExpressionAttributeNames": {"#data": "data"}
                })
                .try_into()
                .expect("projected query request"),
            )
            .await
            .unwrap_or_else(|error| panic!("{} projected query: {error:?}", backend.name)),
        );
        let first_items = first.items.expect("projected query items");
        assert_eq!(first_items.len(), 3, "{}", backend.name);
        assert_eq!(first.count, 3, "{}", backend.name);
        assert_eq!(first.scanned_count, 3, "{}", backend.name);
        assert!(
            first_items
                .iter()
                .all(|item| item.len() == 1 && item.contains_key("data"))
        );
        let resume = first.last_evaluated_key.expect("source frontier");
        assert_eq!(
            resume.get("sk"),
            Some(&AttributeValue::S("003".to_string())),
            "{}",
            backend.name
        );

        let second = expect_query_response(
            handle_query(
                db,
                json!({
                    "TableName": "ProjectedQueryBytePageTable",
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "tenant#1"}},
                    "ProjectionExpression": "#data",
                    "ExpressionAttributeNames": {"#data": "data"},
                    "ExclusiveStartKey": resume
                })
                .try_into()
                .expect("continued projected query"),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("{} continued projected query: {error:?}", backend.name)
            }),
        );
        assert_eq!(second.items.expect("last projected item").len(), 1);
        assert_eq!(second.count, 1, "{}", backend.name);
        assert_eq!(second.scanned_count, 1, "{}", backend.name);
        assert!(second.last_evaluated_key.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn query_filtered_projected_logical_page_resumes_from_source_frontier() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "FilteredProjectedBytePage").await;
        for sk in ["001", "002", "003", "004", "005"] {
            put_test_item_hash_range(
                db.as_ref(),
                "FilteredProjectedBytePage",
                "tenant#1",
                sk,
                &format!(
                    "{}{}",
                    if sk == "002" { "d" } else { "x" },
                    "x".repeat(300 * 1024 - 1)
                ),
            )
            .await;
        }
        let request = |start: Option<storage_types::KeyAttributes>| {
            let mut payload = json!({
                "TableName": "FilteredProjectedBytePage",
                "KeyConditionExpression": "pk = :pk",
                "FilterExpression": "NOT begins_with(#data, :drop)",
                "ProjectionExpression": "#data",
                "ExpressionAttributeNames": {"#data": "data"},
                "ExpressionAttributeValues": {
                    ":pk": {"S": "tenant#1"},
                    ":drop": {"S": "d"}
                }
            });
            if let Some(start) = start {
                payload["ExclusiveStartKey"] = serde_json::to_value(start).expect("start key");
            }
            payload
        };
        let first = expect_query_response(
            handle_query(db.clone(), request(None).try_into().expect("first request"))
                .await
                .unwrap_or_else(|error| panic!("{} first query: {error:?}", backend.name)),
        );
        let first_items = first.items.expect("first projected page");
        assert_eq!(first_items.len(), 2, "{}", backend.name);
        assert!(
            first_items
                .iter()
                .all(|item| item.len() == 1 && item.contains_key("data"))
        );
        assert_eq!(first.count, 2, "{}", backend.name);
        assert_eq!(first.scanned_count, 3, "{}", backend.name);
        let frontier = first.last_evaluated_key.expect("source frontier");
        assert_eq!(
            frontier.get("sk"),
            Some(&AttributeValue::S("003".into())),
            "{}",
            backend.name
        );
        let second = expect_query_response(
            handle_query(
                db,
                request(Some(frontier))
                    .try_into()
                    .expect("continued request"),
            )
            .await
            .unwrap_or_else(|error| panic!("{} continued query: {error:?}", backend.name)),
        );
        assert_eq!(second.items.expect("second projected page").len(), 2);
        assert_eq!(second.count, 2, "{}", backend.name);
        assert_eq!(second.scanned_count, 2, "{}", backend.name);
        assert!(second.last_evaluated_key.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn query_without_filter_projection_or_count_uses_wire_response_fast_path() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "QueryWireFastPathTable").await;
        put_test_item_hash_range(
            db.as_ref(),
            "QueryWireFastPathTable",
            "tenant#1",
            "001",
            "payload",
        )
        .await;

        let payload = json!({
            "TableName": "QueryWireFastPathTable",
            "KeyConditionExpression": "pk = :pk_val",
            "ExpressionAttributeValues": {":pk_val": {"S": "tenant#1"}}
        });

        let result = handle_query(db, payload.try_into().unwrap())
            .await
            .expect("query should use wire fast path");
        let Response::Query(response) = result else {
            panic!("Expected Query response");
        };
        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
        assert!(response.last_evaluated_key.is_none());
        assert_eq!(response.items.expect("query should return items").len(), 1);
    }
}

#[tokio::test]
async fn query_gsi_with_limit_returns_full_dynamodb_last_evaluated_key() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range_with_gsi(db.as_ref(), "LimitedGsiTable").await;

        for i in 1..=3 {
            put_test_item_hash_range_with_gsi(
                &db,
                "LimitedGsiTable",
                "U#1",
                &format!("item#{i:03}"),
                "G#1",
                &format!("gitem#{i:03}"),
            )
            .await;
        }

        let payload = json!({
            "TableName": "LimitedGsiTable",
            "IndexName": "ByGsi",
            "KeyConditionExpression": "gpk = :gpk",
            "ExpressionAttributeValues": {":gpk": {"S": "G#1"}},
            "ProjectionExpression": "gsk",
            "Limit": 2
        });

        let result = handle_query(db.clone(), payload.try_into().unwrap()).await;
        assert!(result.is_ok(), "{}: {result:?}", backend.name);
        let response = expect_query_response(result.unwrap());
        assert_eq!(response.count, 2, "{}", backend.name);
        let items = response.items.as_ref().expect("projected GSI items");
        assert!(
            items
                .iter()
                .all(|item| item.len() == 1 && item.contains_key("gsk")),
            "{}",
            backend.name
        );
        let last_evaluated_key = response
            .last_evaluated_key
            .expect("limited GSI query should return a DynamoDB key map");
        assert_eq!(
            serde_json::to_value(&last_evaluated_key).unwrap(),
            json!({
                "pk": {"S": "U#1"},
                "sk": {"S": "item#002"},
                "gpk": {"S": "G#1"},
                "gsk": {"S": "gitem#002"}
            }),
            "{}",
            backend.name
        );

        let resume_payload = json!({
            "TableName": "LimitedGsiTable",
            "IndexName": "ByGsi",
            "KeyConditionExpression": "gpk = :gpk",
            "ExpressionAttributeValues": {":gpk": {"S": "G#1"}},
            "ProjectionExpression": "gsk",
            "ExclusiveStartKey": last_evaluated_key
        });

        let result = handle_query(db, resume_payload.try_into().unwrap()).await;
        assert!(result.is_ok(), "{}: {result:?}", backend.name);
        let response = expect_query_response(result.unwrap());
        let items = response.items.unwrap();
        assert_eq!(items.len(), 1, "{}", backend.name);
        assert_eq!(
            items[0].get("gsk"),
            Some(&AttributeValue::S("gitem#003".to_string())),
            "{}",
            backend.name
        );
        assert!(response.last_evaluated_key.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn query_with_malformed_exclusive_start_key_returns_dynamodb_error() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "MalformedStartKeyTable").await;
        put_test_item_hash_range(&db, "MalformedStartKeyTable", "U#1", "item#001", "Data 1").await;

        let payload = json!({
            "TableName": "MalformedStartKeyTable",
            "KeyConditionExpression": "pk = :pk_val",
            "ExpressionAttributeValues": {":pk_val": {"S": "U#1"}},
            "ExclusiveStartKey": {
                "pk": {"S": "U#1"}
            }
        });

        let result = handle_query(db, payload.try_into().unwrap()).await;
        let error = result.expect_err("malformed start key should fail");
        assert_eq!(error.status_code, 400, "{}", backend.name);
        assert_eq!(
            error.error_type, "com.amazon.coral.validate#ValidationException",
            "{}",
            backend.name
        );
        assert_eq!(
            error.message, "The provided starting key is invalid",
            "{}",
            backend.name
        );
    }
}

#[tokio::test]
async fn query_rejects_exclusive_start_key_with_wrong_key_type() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "WrongTypeStartKeyTable").await;

        let payload = json!({
            "TableName": "WrongTypeStartKeyTable",
            "KeyConditionExpression": "pk = :pk_val",
            "ExpressionAttributeValues": {":pk_val": {"S": "U#1"}},
            "ExclusiveStartKey": {
                "pk": {"N": "1"},
                "sk": {"S": "item#001"}
            }
        });

        let error = handle_query(db, payload.try_into().unwrap())
            .await
            .expect_err("wrong type start key should fail");
        assert_eq!(error.status_code, 400, "{}", backend.name);
        assert_eq!(
            error.error_type, "com.amazon.coral.validate#ValidationException",
            "{}",
            backend.name
        );
        assert_eq!(
            error.message, "The provided starting key is invalid",
            "{}",
            backend.name
        );
    }
}

#[tokio::test]
async fn query_gsi_with_malformed_exclusive_start_key_returns_dynamodb_error() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range_with_gsi(db.as_ref(), "MalformedGsiStartKeyTable").await;
        put_test_item_hash_range_with_gsi(
            &db,
            "MalformedGsiStartKeyTable",
            "U#1",
            "item#001",
            "G#1",
            "gitem#001",
        )
        .await;

        let payload = json!({
            "TableName": "MalformedGsiStartKeyTable",
            "IndexName": "ByGsi",
            "KeyConditionExpression": "gpk = :gpk",
            "ExpressionAttributeValues": {":gpk": {"S": "G#1"}},
            "ExclusiveStartKey": {
                "gpk": {"S": "G#1"},
                "gsk": {"S": "gitem#001"}
            }
        });

        let result = handle_query(db, payload.try_into().unwrap()).await;
        let error = result.expect_err("malformed GSI start key should fail");
        assert_eq!(error.status_code, 400, "{}", backend.name);
        assert_eq!(
            error.error_type, "com.amazon.coral.validate#ValidationException",
            "{}",
            backend.name
        );
        assert_eq!(
            error.message, "The provided starting key is invalid",
            "{}",
            backend.name
        );
    }
}

#[tokio::test]
async fn query_reverse_filter_and_projection_keep_native_page_semantics() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "ReverseFilterProjectionTable").await;
        for (sk, data) in [("001", "keep"), ("002", "drop"), ("003", "keep")] {
            put_test_item_hash_range(
                db.as_ref(),
                "ReverseFilterProjectionTable",
                "tenant#1",
                sk,
                data,
            )
            .await;
        }

        let response = expect_query_response(
            handle_query(
                db,
                json!({
                    "TableName": "ReverseFilterProjectionTable",
                    "KeyConditionExpression": "pk = :pk",
                    "FilterExpression": "#data = :keep",
                    "ProjectionExpression": "sk",
                    "ExpressionAttributeNames": {"#data": "data"},
                    "ExpressionAttributeValues": {
                        ":pk": {"S": "tenant#1"},
                        ":keep": {"S": "keep"}
                    },
                    "ScanIndexForward": false
                })
                .try_into()
                .expect("reverse query request"),
            )
            .await
            .unwrap_or_else(|error| panic!("{} reverse query: {error:?}", backend.name)),
        );
        assert_eq!(response.scanned_count, 3, "{}", backend.name);
        assert_eq!(response.count, 2, "{}", backend.name);
        let items = response.items.expect("reverse query items");
        assert_eq!(items.len(), 2, "{}", backend.name);
        assert_eq!(
            items[0].get("sk"),
            Some(&AttributeValue::S("003".to_string()))
        );
        assert_eq!(
            items[1].get("sk"),
            Some(&AttributeValue::S("001".to_string()))
        );
        assert!(items.iter().all(|item| item.len() == 1));
    }
}

#[tokio::test]
async fn query_count_filter_and_limit_keep_evaluated_frontier() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "CountFilteredQueryTable").await;
        for (sk, data) in [("001", "drop"), ("002", "keep"), ("003", "keep")] {
            put_test_item_hash_range(db.as_ref(), "CountFilteredQueryTable", "tenant", sk, data)
                .await;
        }

        let first = expect_query_response(
            handle_query(
                db.clone(),
                json!({
                    "TableName": "CountFilteredQueryTable",
                    "KeyConditionExpression": "pk = :pk",
                    "FilterExpression": "#data = :keep",
                    "ExpressionAttributeNames": {"#data": "data"},
                    "ExpressionAttributeValues": {
                        ":pk": {"S": "tenant"},
                        ":keep": {"S": "keep"}
                    },
                    "Select": "COUNT",
                    "Limit": 1
                })
                .try_into()
                .expect("first count query"),
            )
            .await
            .unwrap_or_else(|error| panic!("{} count query: {error:?}", backend.name)),
        );
        assert_eq!(first.count, 0, "{}", backend.name);
        assert_eq!(first.scanned_count, 1, "{}", backend.name);
        assert!(first.items.is_none(), "{}", backend.name);
        let resume = first.last_evaluated_key.expect("native evaluated frontier");

        let second = expect_query_response(
            handle_query(
                db,
                json!({
                    "TableName": "CountFilteredQueryTable",
                    "KeyConditionExpression": "pk = :pk",
                    "FilterExpression": "#data = :keep",
                    "ExpressionAttributeNames": {"#data": "data"},
                    "ExpressionAttributeValues": {
                        ":pk": {"S": "tenant"},
                        ":keep": {"S": "keep"}
                    },
                    "Select": "COUNT",
                    "Limit": 1,
                    "ExclusiveStartKey": resume
                })
                .try_into()
                .expect("continued count query"),
            )
            .await
            .unwrap_or_else(|error| panic!("{} continued count query: {error:?}", backend.name)),
        );
        assert_eq!(second.count, 1, "{}", backend.name);
        assert_eq!(second.scanned_count, 1, "{}", backend.name);
        assert!(second.items.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn query_gsi_rejects_unprojected_projection_expression_attribute() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_with_keys_only_gsi(db.as_ref(), "ProjectionGsiTable").await;

        for (field, expression, expected) in [
            (
                "ProjectionExpression",
                "#private",
                "One or more parameter values were invalid: Global secondary index ByGsi does not \
                 project [private_note]",
            ),
            (
                "FilterExpression",
                "attribute_exists(#private.child[0])",
                "One or more parameter values were invalid: Secondary index ByGsi does not \
                 project one or more filter attributes: [private_note]",
            ),
        ] {
            let mut payload = json!({
                "TableName": "ProjectionGsiTable", "IndexName": "ByGsi",
                "KeyConditionExpression": "#gpk = :gpk",
                "ExpressionAttributeNames": {"#gpk": "gpk", "#private": "private_note"},
                "ExpressionAttributeValues": {":gpk": {"S": "tenant#1"}}
            });
            payload[field] = json!(expression);
            let error = handle_query(db.clone(), payload.try_into().unwrap())
                .await
                .expect_err("GSI query must reject a non-projected attribute");
            assert_eq!(error.status_code, 400);
            assert_eq!(error.message, expected);
        }

        let payload = json!({
            "TableName": "ProjectionGsiTable", "IndexName": "ByGsi",
            "KeyConditionExpression": "#gpk = :gpk",
            "ExpressionAttributeNames": {"#gpk": "gpk"},
            "ExpressionAttributeValues": {":gpk": {"S": "tenant#1"}},
            "Select": "ALL_ATTRIBUTES"
        });
        let error = handle_query(db.clone(), payload.try_into().unwrap())
            .await
            .expect_err("Select ALL_ATTRIBUTES needs an ALL projection");
        assert_eq!(error.status_code, 400);
        assert_eq!(
            error.message,
            "One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not \
             supported for global secondary index ByGsi because its projection type is not ALL"
        );
    }
}

#[tokio::test]
async fn query_filter_expression_uses_full_condition_expression_engine() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "FullFilterTable").await;
        seed_nested_projection_item(db.as_ref(), "FullFilterTable", "pk").await;

        let payload = json!({
            "TableName": "FullFilterTable",
            "KeyConditionExpression": "pk = :pk",
            "FilterExpression": "contains(tags, :tag) AND profile.age >= :minAge",
            "ExpressionAttributeValues": {
                ":pk": {"S": "item-1"},
                ":tag": {"S": "red"},
                ":minAge": {"N": "30"}
            }
        });

        let response =
            expect_query_response(handle_query(db, payload.try_into().unwrap()).await.unwrap());

        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
    }
}

#[tokio::test]
async fn query_filter_expression_supports_or_and_attribute_not_exists() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "FullFilterOrTable").await;
        seed_nested_projection_item(db.as_ref(), "FullFilterOrTable", "pk").await;

        let payload = json!({
            "TableName": "FullFilterOrTable",
            "KeyConditionExpression": "pk = :pk",
            "FilterExpression": "attribute_not_exists(#missing) OR #s = :status",
            "ExpressionAttributeNames": {
                "#missing": "missing",
                "#s": "status"
            },
            "ExpressionAttributeValues": {
                ":pk": {"S": "item-1"},
                ":status": {"S": "active"}
            }
        });

        let response =
            expect_query_response(handle_query(db, payload.try_into().unwrap()).await.unwrap());

        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
    }
}

#[tokio::test]
async fn query_key_condition_accepts_value_on_left_like_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "ReverseKeyConditionTable").await;
        seed_nested_projection_item(db.as_ref(), "ReverseKeyConditionTable", "pk").await;

        let payload = json!({
            "TableName": "ReverseKeyConditionTable",
            "KeyConditionExpression": ":pk = pk",
            "ExpressionAttributeValues": {
                ":pk": {"S": "item-1"}
            }
        });

        let response =
            expect_query_response(handle_query(db, payload.try_into().unwrap()).await.unwrap());

        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
    }
}

#[tokio::test]
async fn query_key_condition_accepts_no_space_hash_equality_like_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "NoSpaceKeyConditionTable").await;
        seed_nested_projection_item(db.as_ref(), "NoSpaceKeyConditionTable", "pk").await;

        let payload = json!({
            "TableName": "NoSpaceKeyConditionTable",
            "KeyConditionExpression": "pk=:pk",
            "ExpressionAttributeValues": {
                ":pk": {"S": "item-1"}
            }
        });

        let response =
            expect_query_response(handle_query(db, payload.try_into().unwrap()).await.unwrap());

        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
    }
}

#[tokio::test]
async fn query_key_condition_accepts_parenthesized_hash_equality_like_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "ParenthesizedKeyConditionTable").await;
        seed_nested_projection_item(db.as_ref(), "ParenthesizedKeyConditionTable", "pk").await;

        let payload = json!({
            "TableName": "ParenthesizedKeyConditionTable",
            "KeyConditionExpression": "(pk = :pk)",
            "ExpressionAttributeValues": {
                ":pk": {"S": "item-1"}
            }
        });

        let response =
            expect_query_response(handle_query(db, payload.try_into().unwrap()).await.unwrap());

        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
    }
}

#[tokio::test]
async fn query_key_condition_accepts_value_on_left_sort_comparison_like_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "ValueLeftSortConditionTable").await;
        put_test_item_hash_range(
            db.as_ref(),
            "ValueLeftSortConditionTable",
            "tenant#1",
            "sort#001",
            "payload",
        )
        .await;

        let payload = json!({
            "TableName": "ValueLeftSortConditionTable",
            "KeyConditionExpression": "pk = :pk AND :lo <= sk",
            "ExpressionAttributeValues": {
                ":pk": {"S": "tenant#1"},
                ":lo": {"S": "sort#000"}
            }
        });

        let response =
            expect_query_response(handle_query(db, payload.try_into().unwrap()).await.unwrap());

        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
    }
}

#[tokio::test]
async fn query_key_condition_accepts_binary_sort_begins_with_like_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_binary_hash_range(db.as_ref(), "BinaryBeginsWithTable").await;
        put_test_item_binary_hash_range(
            db.as_ref(),
            "BinaryBeginsWithTable",
            "tenant#1",
            "AQIDBA==",
        )
        .await;

        let payload = json!({
            "TableName": "BinaryBeginsWithTable",
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :prefix)",
            "ExpressionAttributeValues": {
                ":pk": {"S": "tenant#1"},
                ":prefix": {"B": "AQI="}
            }
        });

        let response =
            expect_query_response(handle_query(db, payload.try_into().unwrap()).await.unwrap());

        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
    }
}

#[tokio::test]
async fn query_key_condition_accepts_aliases_between_and_begins_with_like_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "AliasBetweenBeginsWithTable").await;
        put_test_item_hash_range(
            db.as_ref(),
            "AliasBetweenBeginsWithTable",
            "tenant#1",
            "sort#001",
            "payload",
        )
        .await;
        put_test_item_hash_range(
            db.as_ref(),
            "AliasBetweenBeginsWithTable",
            "tenant#1",
            "other#001",
            "payload",
        )
        .await;

        let between_payload = json!({
            "TableName": "AliasBetweenBeginsWithTable",
            "KeyConditionExpression": "#pk = :pk AND #sk BETWEEN :lo AND :hi",
            "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
            "ExpressionAttributeValues": {
                ":pk": {"S": "tenant#1"},
                ":lo": {"S": "sort#000"},
                ":hi": {"S": "sort#009"}
            }
        });
        let between_response = expect_query_response(
            handle_query(db.clone(), between_payload.try_into().unwrap())
                .await
                .unwrap(),
        );
        assert_eq!(between_response.count, 1);

        let begins_with_payload = json!({
            "TableName": "AliasBetweenBeginsWithTable",
            "KeyConditionExpression": "#pk=:pk AND begins_with(#sk,:prefix)",
            "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
            "ExpressionAttributeValues": {
                ":pk": {"S": "tenant#1"},
                ":prefix": {"S": "sort#"}
            }
        });
        let begins_with_response = expect_query_response(
            handle_query(db, begins_with_payload.try_into().unwrap())
                .await
                .unwrap(),
        );
        assert_eq!(begins_with_response.count, 1);
    }
}

#[tokio::test]
async fn query_key_condition_rejects_invalid_operators_like_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "InvalidKeyConditionTable").await;
        put_test_item_hash_range(
            db.as_ref(),
            "InvalidKeyConditionTable",
            "tenant#1",
            "sort#001",
            "payload",
        )
        .await;

        for (expression, values) in [
            (
                "pk = :pk AND BEGINS_WITH(sk, :prefix)",
                json!({":pk": {"S": "tenant#1"}, ":prefix": {"S": "sort#"}}),
            ),
            (
                "pk = :pk OR sk = :sk",
                json!({":pk": {"S": "tenant#1"}, ":sk": {"S": "sort#001"}}),
            ),
            (
                "pk = :pk AND sk <> :sk",
                json!({":pk": {"S": "tenant#1"}, ":sk": {"S": "sort#001"}}),
            ),
            (
                "pk = :pk AND contains(sk, :prefix)",
                json!({":pk": {"S": "tenant#1"}, ":prefix": {"S": "sort#"}}),
            ),
            (
                "pk = :pk AND sk > :lo AND sk <= :hi",
                json!({
                    ":pk": {"S": "tenant#1"},
                    ":lo": {"S": "sort#000"},
                    ":hi": {"S": "sort#999"}
                }),
            ),
            ("sk = :sk", json!({":sk": {"S": "sort#001"}})),
            (
                "begins_with(pk, :prefix)",
                json!({":prefix": {"S": "tenant#"}}),
            ),
        ] {
            let payload = json!({
                "TableName": "InvalidKeyConditionTable",
                "KeyConditionExpression": expression,
                "ExpressionAttributeValues": values
            });
            let request = match payload.try_into() {
                Ok(request) => request,
                Err(_) => continue,
            };
            let error = match handle_query(db.clone(), request).await {
                Ok(_) => panic!("invalid key condition should fail for {expression}"),
                Err(error) => error,
            };
            assert_eq!(error.status_code, 400, "{expression}");
            assert_eq!(
                error.error_type, "com.amazon.coral.validate#ValidationException",
                "{expression}"
            );
        }
    }
}

#[tokio::test]
async fn query_projection_reconstructs_nested_maps_and_lists_like_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "NestedProjectionTable").await;
        seed_nested_projection_item(db.as_ref(), "NestedProjectionTable", "pk").await;

        let payload = json!({
            "TableName": "NestedProjectionTable",
            "KeyConditionExpression": "pk = :pk",
            "ProjectionExpression": "pk, profile.#n, tags[0], nested.#list[0].v",
            "ExpressionAttributeNames": {
                "#n": "name",
                "#list": "list"
            },
            "ExpressionAttributeValues": {
                ":pk": {"S": "item-1"}
            }
        });

        let response =
            expect_query_response(handle_query(db, payload.try_into().unwrap()).await.unwrap());
        let items = response.items.expect("projected items");
        assert_eq!(items.len(), 1);
        let item = &items[0];

        assert_eq!(
            item.get("profile"),
            Some(&AttributeValue::M(AttributeMap::from(HashMap::from([(
                "name".to_string(),
                AttributeValue::S("Ada".to_string()),
            )]))))
        );
        assert_eq!(
            item.get("tags"),
            Some(&AttributeValue::L(vec![AttributeValue::S(
                "red".to_string()
            )]))
        );
        assert_eq!(
            item.get("nested"),
            Some(&AttributeValue::M(AttributeMap::from(HashMap::from([(
                "list".to_string(),
                AttributeValue::L(vec![AttributeValue::M(AttributeMap::from(HashMap::from(
                    [("v".to_string(), AttributeValue::S("first".to_string()),)]
                )))]),
            )]))))
        );
    }
}

#[tokio::test]
async fn query_attributes_to_get_with_key_condition_expression_is_rejected() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "AttributesToGetTable").await;
        put_test_item(db.as_ref(), "AttributesToGetTable", "user1", "Alice", 25).await;
        let payload = json!({
            "TableName": "AttributesToGetTable",
            "KeyConditionExpression": "pk = :pk",
            "AttributesToGet": ["pk", "name"],
            "ExpressionAttributeValues": {":pk": {"S": "user1"}}
        });

        let error = match payload.try_into() {
            Ok(request) => {
                handle_query(db, request)
                    .await
                    .expect_err("legacy and expression parameters cannot be mixed")
                    .message
            }
            Err(message) => message,
        };
        assert_eq!(
            error,
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {AttributesToGet} Expression parameters: \
             {KeyConditionExpression}"
        );
    }
}

#[tokio::test]
async fn query_consumed_capacity_total() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "TestTable").await;

        // Add test items
        put_test_item(db.as_ref(), "TestTable", "test1", "Alice", 25).await;
        put_test_item(db.as_ref(), "TestTable", "test2", "Bob", 30).await;

        let payload = json!({
            "TableName": "TestTable",
            "KeyConditionExpression": "pk = :pk_val",
            "ExpressionAttributeValues": {
                ":pk_val": {"S": "test1"}
            },
            "ReturnConsumedCapacity": "TOTAL"
        });

        let request: storage_types::QueryRequest = payload.try_into().unwrap();
        let result = handle_query(db.clone(), request).await.unwrap();

        let response = expect_query_response(result);
        assert!(response.consumed_capacity.is_some());
        let consumed_capacity = response.consumed_capacity.unwrap();

        assert_eq!(consumed_capacity.table_name.to_string(), "TestTable");
        assert_eq!(consumed_capacity.capacity_units, 0.0);
    }
}

#[tokio::test]
async fn given_query_payload_without_key_condition_when_routed_then_aws_missing_parameter_text() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "NoKceTable").await;

        let request: storage_types::QueryRequest =
            serde_json::from_value(json!({"TableName": "NoKceTable"})).unwrap();
        let error = handle_query(db, request)
            .await
            .expect_err("absent key condition should fail");
        assert_eq!(error.status_code, 400, "{}", backend.name);
        assert_eq!(
            error.error_type, "com.amazon.coral.validate#ValidationException",
            "{}",
            backend.name
        );
        assert_eq!(
            error.message,
            "Either the KeyConditions or KeyConditionExpression parameter must be specified in \
             the request.",
            "{}",
            backend.name
        );
    }
}

#[tokio::test]
async fn given_query_payload_with_empty_key_condition_when_routed_then_empty_expression_text() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "EmptyKceTable").await;

        let request: storage_types::QueryRequest = serde_json::from_value(
            json!({"TableName": "EmptyKceTable", "KeyConditionExpression": ""}),
        )
        .unwrap();
        let error = handle_query(db, request)
            .await
            .expect_err("empty key condition should fail");
        assert_eq!(
            error.message, "Invalid KeyConditionExpression: The expression can not be empty;",
            "{}",
            backend.name
        );
    }
}
