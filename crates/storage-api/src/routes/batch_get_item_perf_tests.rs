use std::{collections::HashMap, sync::Arc, time::Instant};

use alloc_counter::AllocationGuard;
use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use serde_json::{Value, json};
use storage::{DatabaseManager, DatabaseManagerRuntimeOptions, PutItemInput};
use storage_types::{AttributeValue, CreateTableRequest, TableName};

use crate::{
    routes::{dynamodb::dynamodb_endpoint, routes_tests::seed_item},
    types::AppState,
};

const ITERATIONS: u32 = 20;

#[tokio::test]
#[ignore = "manual caller-thread allocation probe; SQLite worker allocations are excluded"]
async fn given_realistic_batches_when_http_batch_get_then_measure_caller_allocations() {
    for width in [800, 1150, 1500] {
        let state = create_batch_fixture(width).await;
        for (label, count, mode) in [
            ("one_projected", 1, "expression"),
            ("ten_projected", 10, "expression"),
            ("quarter_projected", 25, "expression"),
            ("full_projected", 100, "expression"),
            ("full_wire", 100, "wire"),
            ("legacy_projected", 25, "legacy"),
            ("mixed_tables", 25, "mixed"),
            ("missing_items", 25, "missing"),
            ("wire_holdout", 25, "wire"),
        ] {
            let request = make_batch_request(count, mode);
            for _ in 0..3 {
                execute_batch_request(&state, &request).await;
            }
            let guard = AllocationGuard::start(module_path!(), label, file!(), line!(), None);
            let started = Instant::now();
            let mut last_body = Bytes::new();
            for _ in 0..ITERATIONS {
                last_body = execute_batch_request(&state, &request).await;
                std::hint::black_box(&last_body);
            }
            let elapsed = started.elapsed();
            let report = guard.finish();
            assert_batch_response(&last_body, count, mode, width);
            println!(
                "batch_get_http case={label} width={width} iterations={ITERATIONS} \
                 allocations_per_op={:.2} bytes_per_op={:.2} us_per_op={:.2}",
                report.allocation_count as f64 / f64::from(ITERATIONS),
                report.allocated_bytes as f64 / f64::from(ITERATIONS),
                elapsed.as_secs_f64() * 1_000_000.0 / f64::from(ITERATIONS),
            );
        }
    }
}

#[tokio::test]
async fn given_mixed_batch_get_shapes_when_projecting_then_preserve_items_and_empty_tables() {
    let state = create_batch_fixture(800).await;
    for mode in ["mixed", "legacy", "missing", "wire"] {
        let response = execute_batch_request(&state, &make_batch_request(2, mode)).await;
        assert_batch_response(&response, 2, mode, 800);
    }
}

async fn create_batch_fixture(width: usize) -> Arc<AppState> {
    let db = Arc::new(
        DatabaseManager::new_for_test_with_runtime_options(
            DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(false)
                .build(),
        )
        .await
        .expect("SQLite database"),
    );
    for name in ["batch_a", "batch_b"] {
        let request: CreateTableRequest = serde_json::from_value(json!({
            "TableName": name,
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
                {"AttributeName": "gsk", "AttributeType": "S"}
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [
                {"IndexName": "by_group", "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}},
                {"IndexName": "by_sort", "KeySchema": [{"AttributeName": "gsk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}
            ]
        }))
        .expect("table definition");
        db.create_table(&request).await.expect("create table");
        for index in 0..100 {
            seed_item(
                &db,
                PutItemInput::builder()
                    .table_name(TableName::new(name))
                    .item(make_batch_item(index, width))
                    .build(),
            )
            .await
            .expect("seed item");
        }
    }
    Arc::new(AppState::new(db))
}

fn make_batch_item(index: usize, width: usize) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::from([
        ("pk".to_string(), AttributeValue::S(format!("{index:0100}"))),
        ("payload".to_string(), AttributeValue::S("x".repeat(width))),
        ("gpk".to_string(), AttributeValue::S("group".to_string())),
        (
            "gsk".to_string(),
            AttributeValue::S(format!("sort#{index}")),
        ),
        (
            "ttl".to_string(),
            AttributeValue::N("4102444800".to_string()),
        ),
    ]);
    for attribute in 0..5 {
        item.insert(
            format!("attribute_{attribute}"),
            AttributeValue::S("value".repeat(8)),
        );
    }
    item
}

fn make_batch_request(count: usize, mode: &str) -> Bytes {
    let start = if mode == "missing" { 100 } else { 0 };
    let keys = (start..start + count)
        .map(|index| json!({"pk": {"S": format!("{index:0100}")}}))
        .collect::<Vec<_>>();
    let mut table = json!({"Keys": keys});
    if mode == "legacy" {
        table["AttributesToGet"] = json!(["pk"]);
    } else if mode != "wire" {
        table["ProjectionExpression"] = json!("#key");
        table["ExpressionAttributeNames"] = json!({"#key": "pk"});
    }
    let mut tables = json!({"batch_a": table});
    if mode == "mixed" {
        tables["batch_b"] = json!({"Keys": keys});
    }
    Bytes::from(serde_json::to_vec(&json!({"RequestItems": tables})).expect("request bytes"))
}

async fn execute_batch_request(state: &Arc<AppState>, request: &Bytes) -> Bytes {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.BatchGetItem"),
    );
    let response = dynamodb_endpoint(State(state.clone()), headers, request.clone())
        .await
        .expect("BatchGet HTTP response");
    assert_eq!(response.status(), StatusCode::OK);
    body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response bytes")
}

fn assert_batch_response(body: &Bytes, count: usize, mode: &str, width: usize) {
    let response: Value = serde_json::from_slice(body).expect("response JSON");
    assert_eq!(response["UnprocessedKeys"], json!({}));
    let tables = response["Responses"].as_object().expect("response tables");
    assert_eq!(tables.len(), if mode == "mixed" { 2 } else { 1 });
    for (table, items) in tables {
        let items = items.as_array().expect("table items");
        assert_eq!(items.len(), if mode == "missing" { 0 } else { count });
        let mut actual_keys = Vec::new();
        for item in items {
            actual_keys.push(item["pk"]["S"].as_str().expect("pk").to_string());
            let full = mode == "wire" || table == "batch_b";
            assert_eq!(
                item.as_object().expect("item").len(),
                if full { 10 } else { 1 }
            );
            if full {
                let index = item["pk"]["S"]
                    .as_str()
                    .expect("pk")
                    .parse()
                    .expect("key index");
                assert_eq!(
                    *item,
                    serde_json::to_value(make_batch_item(index, width)).expect("expected item")
                );
            }
        }
        actual_keys.sort();
        if mode != "missing" {
            assert_eq!(
                actual_keys,
                (0..count)
                    .map(|index| format!("{index:0100}"))
                    .collect::<Vec<_>>()
            );
        }
    }
}
