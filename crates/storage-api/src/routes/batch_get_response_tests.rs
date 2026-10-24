use std::{hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use axum::body;
use serde::Serialize;
use serde_json::json;
use storage_types::{AttributeMap, BatchGetItemResponse, TableName, VecMap, WireItem};

use crate::{raw_dynamodb_response::json_response_bytes, types::Response};

#[derive(Clone, Serialize)]
struct MapResponse {
    #[serde(rename = "Responses")]
    responses: VecMap<TableName, Vec<AttributeMap>>,
}

fn responses(items_per_table: usize) -> (MapResponse, BatchGetItemResponse) {
    let mut map_tables = VecMap::new();
    let mut wire_tables = VecMap::new();
    for table in ["batch-a", "batch-b"] {
        let mut maps = Vec::with_capacity(items_per_table);
        let mut wires = Vec::with_capacity(items_per_table);
        for index in 0..items_per_table {
            let item = json!({
                "pk": {"S": format!("item#{index}")},
                "payload": {"S": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},
                "counter": {"N": "42"}
            });
            let bytes = serde_json::to_vec(&item).unwrap();
            maps.push(serde_json::from_slice(&bytes).unwrap());
            wires.push(WireItem::dynamo_json(bytes));
        }
        map_tables.insert(TableName::new(table), maps);
        wire_tables.insert(TableName::new(table), wires);
    }
    (
        MapResponse {
            responses: map_tables,
        },
        BatchGetItemResponse {
            responses: Some(wire_tables),
            unprocessed_keys: None,
            consumed_capacity: None,
        },
    )
}

#[test]
fn batch_get_wire_serde_preserves_response_shape() {
    let (map, wire) = responses(2);
    let expected: serde_json::Value = serde_json::to_value(map).unwrap();
    let actual: serde_json::Value = serde_json::to_value(&wire).unwrap();
    assert_eq!(actual, expected);
    let decoded: BatchGetItemResponse = serde_json::from_value(actual).unwrap();
    assert_eq!(
        decoded
            .responses
            .unwrap()
            .get(&TableName::new("batch-a"))
            .unwrap()
            .len(),
        2
    );
    assert!(
        serde_json::from_str::<BatchGetItemResponse>(r#"{"Responses":{"batch-a":[42]}}"#).is_err()
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn batch_get_wire_clone_and_serde_report() {
    const ITERATIONS: usize = 10_000;
    let (map, wire) = responses(1);
    for label in ["map_clone", "wire_clone", "map_serde", "wire_serde"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "batch_get_wire_clone_and_serde_report",
            file!(),
            line!(),
            Some(label),
        );
        let started = Instant::now();
        let mut total = 0;
        for _ in 0..ITERATIONS {
            total += match label {
                "map_clone" => black_box(map.clone()).responses.len(),
                "wire_clone" => black_box(wire.clone()).responses.unwrap().len(),
                "map_serde" => serde_json::to_vec(black_box(&map)).unwrap().len(),
                _ => serde_json::to_vec(black_box(&wire)).unwrap().len(),
            };
        }
        let elapsed = started.elapsed();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert!(total > 0);
        println!(
            "batch_get_narrow label={label} iterations={ITERATIONS} elapsed_ns={}",
            elapsed.as_nanos()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
async fn batch_get_multi_item_http_response_report() {
    const ITERATIONS: usize = 2_000;
    let (map, wire) = responses(25);
    for label in ["map_http", "wire_http"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "batch_get_multi_item_http_response_report",
            file!(),
            line!(),
            Some(label),
        );
        let started = Instant::now();
        let mut total = 0;
        for _ in 0..ITERATIONS {
            let response = if label == "map_http" {
                json_response_bytes(serde_json::to_vec(black_box(&map.clone())).unwrap())
            } else {
                super::dynamodb::response_to_http(Response::BatchGetItem(black_box(wire.clone())))
            };
            total += body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .len();
        }
        let elapsed = started.elapsed();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert!(total > 0);
        println!(
            "batch_get_http label={label} iterations={ITERATIONS} elapsed_ns={}",
            elapsed.as_nanos()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
async fn given_batch_get_with_ten_items_when_repeated_then_report_endpoint_cost() {
    use std::{collections::HashMap, sync::Arc};

    use axum::{
        body::Bytes,
        extract::State,
        http::{HeaderMap, HeaderValue, StatusCode},
    };
    use storage::PutItemInput;
    use storage_types::{AttributeValue, CreateTableRequest};

    use crate::types::AppState;

    const ITERATIONS: usize = 5_000;
    let db = super::routes_tests::create_sqlite_test_db().await;
    let create: CreateTableRequest = serde_json::from_value(json!({
        "TableName": "batch_wire_measurement",
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
    }))
    .unwrap();
    db.create_table(&create).await.unwrap();
    for index in 0..10 {
        let item = HashMap::from([
            (
                "pk".to_owned(),
                AttributeValue::S(format!("item#{index:02}")),
            ),
            (
                "payload".to_owned(),
                AttributeValue::S(
                    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz".to_owned(),
                ),
            ),
        ]);
        super::routes_tests::seed_item(
            db.as_ref(),
            PutItemInput::builder()
                .table_name(TableName::new("batch_wire_measurement"))
                .item(item)
                .build(),
        )
        .await
        .unwrap();
    }
    let state = Arc::new(AppState::new(db));
    let keys = (0..10)
        .map(|index| json!({"pk": {"S": format!("item#{index:02}")}}))
        .collect::<Vec<_>>();
    let request = Bytes::from(
        serde_json::to_vec(&json!({
            "RequestItems": {"batch_wire_measurement": {"Keys": keys}}
        }))
        .unwrap(),
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.BatchGetItem"),
    );
    for _ in 0..100 {
        let response = super::dynamodb::dynamodb_endpoint(
            State(state.clone()),
            headers.clone(),
            request.clone(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let guard = AllocationGuard::start(
        module_path!(),
        "given_batch_get_with_ten_items_when_repeated_then_report_endpoint_cost",
        file!(),
        line!(),
        None,
    );
    let started = Instant::now();
    let mut total = 0;
    for _ in 0..ITERATIONS {
        let response = super::dynamodb::dynamodb_endpoint(
            State(state.clone()),
            headers.clone(),
            black_box(request.clone()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        total += body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .len();
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).unwrap();
    assert!(total > 0);
    println!(
        "batch_get_endpoint iterations={ITERATIONS} elapsed_ns={}",
        elapsed.as_nanos()
    );
}
