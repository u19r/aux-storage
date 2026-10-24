use std::{hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use axum::body;
use serde::Serialize;
use serde_json::json;
use storage_types::{
    AttributeMap, AttributeProjection, AttributeValue, QueryResponse, ScanResponse, WireItem,
};

use crate::{raw_dynamodb_response::json_response_bytes, types::Response};

#[test]
fn given_attribute_list_with_duplicates_when_projecting_then_values_match() {
    let item = AttributeMap::from(std::collections::HashMap::from([
        ("pk".to_owned(), AttributeValue::S("tenant".to_owned())),
        ("payload".to_owned(), AttributeValue::S("data".to_owned())),
    ]));
    let selected = ["pk".to_owned(), "missing".to_owned(), "pk".to_owned()];
    let result = AttributeProjection::from_attributes(&selected).project(&item);
    assert_eq!(result.len(), 1);
    assert_eq!(result.get("pk"), item.get("pk"));
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_attribute_list_when_projected_repeatedly_then_report_cost() {
    const ITERATIONS: usize = 100_000;
    let item = AttributeMap::from(std::collections::HashMap::from([
        ("pk".to_owned(), AttributeValue::S("tenant".to_owned())),
        (
            "payload".to_owned(),
            AttributeValue::S("abcdefghijklmnopqrstuvwxyz".to_owned()),
        ),
        ("counter".to_owned(), AttributeValue::N("42".to_owned())),
    ]));
    let selected = ["pk".to_owned(), "counter".to_owned()];
    for label in ["hash_map", "attribute_map"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "given_attribute_list_when_projected_repeatedly_then_report_cost",
            file!(),
            line!(),
            Some(label),
        );
        let started = Instant::now();
        let mut total = 0;
        for _ in 0..ITERATIONS {
            let projected = if label == "hash_map" {
                let mut projected = std::collections::HashMap::with_capacity(selected.len());
                for name in &selected {
                    if let Some(value) = black_box(&item).get(name) {
                        projected.insert(name.clone(), value.clone());
                    }
                }
                AttributeMap::from(projected)
            } else {
                AttributeProjection::from_attributes(&selected).project(black_box(&item))
            };
            total += black_box(projected).len();
        }
        let elapsed = started.elapsed();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert!(total > 0);
        println!(
            "projection_attribute_list label={label} iterations={ITERATIONS} elapsed_ns={}",
            elapsed.as_nanos()
        );
    }
}

#[derive(Clone, Serialize)]
struct MapRangeResponse {
    #[serde(rename = "Items")]
    items: Vec<AttributeMap>,
    #[serde(rename = "Count")]
    count: u32,
    #[serde(rename = "ScannedCount")]
    scanned_count: u32,
}

fn responses(count: usize) -> (MapRangeResponse, QueryResponse, ScanResponse) {
    let mut maps = Vec::with_capacity(count);
    let mut wires = Vec::with_capacity(count);
    for index in 0..count {
        let item = json!({
            "pk": {"S": format!("item#{index}")},
            "payload": {"S": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},
            "counter": {"N": "42"}
        });
        let bytes = serde_json::to_vec(&item).unwrap();
        maps.push(serde_json::from_slice(&bytes).unwrap());
        wires.push(WireItem::dynamo_json(bytes));
    }
    let map = MapRangeResponse {
        items: maps,
        count: count as u32,
        scanned_count: count as u32,
    };
    let query = QueryResponse {
        items: Some(wires.clone()),
        count: count as u32,
        scanned_count: count as u32,
        last_evaluated_key: None,
        consumed_capacity: None,
    };
    let scan = ScanResponse {
        items: Some(wires),
        count: count as u32,
        scanned_count: count as u32,
        last_evaluated_key: None,
        consumed_capacity: None,
    };
    (map, query, scan)
}

#[test]
fn query_and_scan_wire_serde_preserve_response_shape() {
    let (map, query, scan) = responses(2);
    let expected = serde_json::to_value(map).unwrap();
    assert_eq!(serde_json::to_value(&query).unwrap(), expected);
    assert_eq!(serde_json::to_value(&scan).unwrap(), expected);
    let query: QueryResponse = serde_json::from_value(expected.clone()).unwrap();
    let scan: ScanResponse = serde_json::from_value(expected).unwrap();
    assert_eq!(query.items.unwrap().len(), 2);
    assert_eq!(scan.items.unwrap().len(), 2);
    assert!(
        serde_json::from_str::<QueryResponse>(r#"{"Items":[42],"Count":1,"ScannedCount":1}"#)
            .is_err()
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn query_wire_clone_and_serde_report() {
    const ITERATIONS: usize = 10_000;
    let (map, query, _) = responses(1);
    for label in ["map_clone", "wire_clone", "map_serde", "wire_serde"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "query_wire_clone_and_serde_report",
            file!(),
            line!(),
            Some(label),
        );
        let started = Instant::now();
        let mut total = 0;
        for _ in 0..ITERATIONS {
            total += match label {
                "map_clone" => black_box(map.clone()).items.len(),
                "wire_clone" => black_box(query.clone()).items.unwrap().len(),
                "map_serde" => serde_json::to_vec(black_box(&map)).unwrap().len(),
                _ => serde_json::to_vec(black_box(&query)).unwrap().len(),
            };
        }
        let elapsed = started.elapsed();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert!(total > 0);
        println!(
            "query_narrow label={label} iterations={ITERATIONS} elapsed_ns={}",
            elapsed.as_nanos()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
async fn query_and_scan_multi_item_http_response_report() {
    const ITERATIONS: usize = 2_000;
    let (map, query, scan) = responses(50);
    for label in ["map_query", "wire_query", "map_scan", "wire_scan"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "query_and_scan_multi_item_http_response_report",
            file!(),
            line!(),
            Some(label),
        );
        let started = Instant::now();
        let mut total = 0;
        for _ in 0..ITERATIONS {
            let response = match label {
                "map_query" | "map_scan" => {
                    json_response_bytes(serde_json::to_vec(black_box(&map.clone())).unwrap())
                }
                "wire_query" => {
                    super::dynamodb::response_to_http(Response::Query(black_box(query.clone())))
                }
                _ => super::dynamodb::response_to_http(Response::Scan(black_box(scan.clone()))),
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
            "range_http label={label} iterations={ITERATIONS} elapsed_ns={}",
            elapsed.as_nanos()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
async fn query_and_scan_repeated_endpoint_report() {
    use std::{collections::HashMap, sync::Arc};

    use axum::{
        body::Bytes,
        extract::State,
        http::{HeaderMap, HeaderValue, StatusCode},
    };
    use storage::PutItemInput;
    use storage_types::{AttributeValue, CreateTableRequest, TableName};

    use crate::types::AppState;

    const ITERATIONS: usize = 5_000;
    let db = super::routes_tests::create_sqlite_test_db().await;
    let create: CreateTableRequest = serde_json::from_value(json!({
        "TableName": "range_wire_perf",
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ]
    }))
    .unwrap();
    db.create_table(&create).await.unwrap();
    for index in 0..10 {
        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("tenant".to_owned())),
            (
                "sk".to_owned(),
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
                .table_name(TableName::new("range_wire_perf"))
                .item(item)
                .build(),
        )
        .await
        .unwrap();
    }
    let app_state = Arc::new(AppState::new(db));
    for (label, target, request) in [
        (
            "query_endpoint",
            "DynamoDB_20120810.Query",
            Bytes::from_static(br#"{"TableName":"range_wire_perf","KeyConditionExpression":"pk = :pk","ExpressionAttributeValues":{":pk":{"S":"tenant"}}}"#),
        ),
        (
            "scan_endpoint",
            "DynamoDB_20120810.Scan",
            Bytes::from_static(br#"{"TableName":"range_wire_perf"}"#),
        ),
        (
            "query_attributes_endpoint",
            "DynamoDB_20120810.Query",
            Bytes::from_static(br#"{"TableName":"range_wire_perf","KeyConditionExpression":"pk = :pk","ExpressionAttributeValues":{":pk":{"S":"tenant"}},"AttributesToGet":["pk","payload"]}"#),
        ),
        (
            "scan_attributes_endpoint",
            "DynamoDB_20120810.Scan",
            Bytes::from_static(br#"{"TableName":"range_wire_perf","AttributesToGet":["pk","payload"]}"#),
        ),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-target", HeaderValue::from_static(target));
        for _ in 0..100 {
            let response = super::dynamodb::dynamodb_endpoint(
                State(app_state.clone()), headers.clone(), request.clone(),
            ).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        let guard = AllocationGuard::start(
            module_path!(),
            "query_and_scan_repeated_endpoint_report",
            file!(),
            line!(),
            Some(label),
        );
        let started = Instant::now();
        let mut total = 0;
        for _ in 0..ITERATIONS {
            let response = super::dynamodb::dynamodb_endpoint(
                State(app_state.clone()), headers.clone(), black_box(request.clone()),
            ).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            total += body::to_bytes(response.into_body(), usize::MAX).await.unwrap().len();
        }
        let elapsed = started.elapsed();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert!(total > 0);
        println!("range_endpoint label={label} iterations={ITERATIONS} elapsed_ns={}", elapsed.as_nanos());
    }
}
