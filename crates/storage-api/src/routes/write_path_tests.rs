use std::{hint::black_box, sync::Arc, time::Instant};

use alloc_counter::AllocationGuard;
use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use serde_json::{Value, json};
use storage_condition::{evaluate_wire_condition, parse_condition_expression_opt};
use storage_types::{CreateTableRequest, PutItemRequest, WireItem};

use crate::types::AppState;

fn build_write_item(extra: usize) -> Value {
    let mut item =
        json!({"pk":{"S":"tenant"},"sk":{"S":"item"},"ttl":{"N":"4102444800"},"version":{"N":"1"}});
    for index in 1..=5 {
        item[format!("gsi{index}pk")] = json!({"S":format!("group{index}")});
        item[format!("gsi{index}sk")] = json!({"S":"sort"});
        item[format!("indexer{index}")] = json!({"S":"indexed"});
    }
    for index in 0..extra {
        item[format!("filler{index:03}")] = json!({"S":"x".repeat(128)});
    }
    item
}

fn report_write_cost(
    label: &str,
    iterations: usize,
    elapsed: std::time::Duration,
    cpu: std::time::Duration,
) {
    println!(
        "write_path label={label} iterations={iterations} elapsed_ns={} cpu_ns={}",
        elapsed.as_nanos(),
        cpu.as_nanos()
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_write_requests_when_preparing_repeatedly_then_report_cost() {
    const ITERATIONS: usize = 10_000;
    for extra in [0, 64] {
        let item = build_write_item(extra);
        let request =
            serde_json::to_vec(&json!({"TableName":"write_measurement","Item":item})).unwrap();
        let wire = WireItem::dynamo_json(serde_json::to_vec(&item).unwrap());
        let condition = parse_condition_expression_opt(
            Some("pk = :pk AND sk = :sk AND #version = :version".to_owned()),
            Some(
                std::collections::HashMap::from([("#version".to_owned(), "version".to_owned())])
                    .into(),
            ),
            Some(
                serde_json::from_value(
                    json!({":pk":{"S":"tenant"},":sk":{"S":"item"},":version":{"N":"1"}}),
                )
                .unwrap(),
            ),
        )
        .unwrap()
        .unwrap();
        for mode in ["prepare", "condition"] {
            let label = format!("{mode}_{extra}");
            let guard = AllocationGuard::start(
                module_path!(),
                "write_narrow",
                file!(),
                line!(),
                Some(&label),
            );
            let started = Instant::now();
            let cpu_started = cpu_time::ProcessTime::try_now().unwrap();
            for _ in 0..ITERATIONS {
                if mode == "prepare" {
                    let request: PutItemRequest =
                        serde_json::from_slice(black_box(&request)).unwrap();
                    black_box(storage_provider::PutItemShape::try_from_request(request).unwrap());
                } else {
                    assert!(black_box(
                        evaluate_wire_condition(Some(&wire), &condition).unwrap()
                    ));
                }
            }
            report_write_cost(
                &label,
                ITERATIONS,
                started.elapsed(),
                cpu_started.try_elapsed().unwrap(),
            );
            alloc_counter::emit_report(&guard.finish()).unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
async fn given_write_endpoints_when_executing_repeatedly_then_report_cost() {
    const ITERATIONS: usize = 1_000;
    for extra in [0, 64] {
        let db = super::routes_tests::create_sqlite_test_db().await;
        let mut create_json = json!({
            "TableName":"write_measurement", "BillingMode":"PAY_PER_REQUEST", "MaxIndexers":5,
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]
        });
        if extra != 0 {
            let mut indexes = Vec::new();
            for index in 1..=5 {
                for suffix in ["pk", "sk"] {
                    create_json["AttributeDefinitions"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({
                            "AttributeName":format!("gsi{index}{suffix}"),"AttributeType":"S"
                        }));
                }
                indexes.push(json!({
                    "IndexName":format!("gsi{index}"),
                    "KeySchema":[{"AttributeName":format!("gsi{index}pk"),"KeyType":"HASH"},{"AttributeName":format!("gsi{index}sk"),"KeyType":"RANGE"}],
                    "Projection":{"ProjectionType":"ALL"}
                }));
            }
            create_json["GlobalSecondaryIndexes"] = json!(indexes);
        }
        let create: CreateTableRequest = serde_json::from_value(create_json).unwrap();
        db.create_table(&create).await.unwrap();
        let state = Arc::new(AppState::new(db));
        if extra != 0 {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-amz-target",
                HeaderValue::from_static("DynamoDB_20120810.UpdateTimeToLive"),
            );
            let request = Bytes::from_static(br#"{"TableName":"write_measurement","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"ttl"}}"#);
            let response =
                super::dynamodb::dynamodb_endpoint(State(state.clone()), headers, request)
                    .await
                    .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
        }
        let item = build_write_item(extra);
        let indexers = ["indexer1", "indexer2", "indexer3", "indexer4", "indexer5"];
        let plain = json!({"TableName":"write_measurement","Item":item,"Indexers":indexers});
        let mut conditional = plain.clone();
        conditional["ConditionExpression"] = json!("pk = :pk AND sk = :sk AND #version = :version");
        conditional["ExpressionAttributeNames"] = json!({"#version":"version"});
        conditional["ExpressionAttributeValues"] =
            json!({":pk":{"S":"tenant"},":sk":{"S":"item"},":version":{"N":"1"}});
        let requests = [
            ("put", "DynamoDB_20120810.PutItem", plain.clone()),
            ("conditional_put", "DynamoDB_20120810.PutItem", conditional),
            (
                "batch_put",
                "DynamoDB_20120810.BatchWriteItem",
                json!({"RequestItems":{"write_measurement":[{"PutRequest":{"Item":item,"Indexers":indexers}}]}}),
            ),
            (
                "transact_put",
                "DynamoDB_20120810.TransactWriteItems",
                json!({"TransactItems":[{"Put":plain}]}),
            ),
        ];
        for (mode, target, request) in requests {
            let request = Bytes::from(serde_json::to_vec(&request).unwrap());
            let mut headers = HeaderMap::new();
            headers.insert("x-amz-target", HeaderValue::from_static(target));
            for _ in 0..20 {
                let response = super::dynamodb::dynamodb_endpoint(
                    State(state.clone()),
                    headers.clone(),
                    request.clone(),
                )
                .await
                .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
            }
            let label = format!("{mode}_{extra}");
            let guard = AllocationGuard::start(
                module_path!(),
                "write_broad",
                file!(),
                line!(),
                Some(&label),
            );
            let started = Instant::now();
            let cpu_started = cpu_time::ProcessTime::try_now().unwrap();
            for _ in 0..ITERATIONS {
                let response = super::dynamodb::dynamodb_endpoint(
                    State(state.clone()),
                    headers.clone(),
                    black_box(request.clone()),
                )
                .await
                .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                black_box(
                    body::to_bytes(response.into_body(), usize::MAX)
                        .await
                        .unwrap(),
                );
            }
            report_write_cost(
                &label,
                ITERATIONS,
                started.elapsed(),
                cpu_started.try_elapsed().unwrap(),
            );
            alloc_counter::emit_report(&guard.finish()).unwrap();
        }
    }
}

#[test]
fn given_raw_attributes_when_validating_and_normalizing_then_match_typed_policy() {
    use storage_types::{AttributeMap, LogicalItemSize};
    let item = json!({
        "pk":{"S":"tenant"}, "escaped\"name":{"S":"line\nbreak"},
        "number":{"N":"1e2"}, "binary":{"B":"YWJj"},
        "strings":{"SS":["one","two"]}, "numbers":{"NS":["1e1","2e1"]},
        "binaries":{"BS":["YQ==","Yg=="]}, "bool":{"BOOL":true}, "null":{"NULL":true},
        "nested":{"M":{"child":{"L":[{"N":"3e2"},{"N":"4e2"},{"S":"hello"}]}}}
    });
    let bytes = serde_json::to_vec(&item).unwrap();
    let mut typed: AttributeMap = serde_json::from_slice(&bytes).unwrap();
    let expected_size = LogicalItemSize::try_from_entries(typed.iter()).unwrap();
    let wire = WireItem::dynamo_json(bytes);
    assert_eq!(wire.validate_for_write("Item").unwrap(), expected_size);
    typed.normalize_numbers_for_write();
    let (normalized, _) = wire.normalized_for_write().unwrap();
    assert_eq!(AttributeMap::try_from(&normalized).unwrap(), typed);
    let value =
        serde_json::from_str::<Value>(&normalized.to_dynamo_json_string().unwrap()).unwrap();
    assert_eq!(value["nested"]["M"]["child"]["L"][1]["N"], "400");
    assert_eq!(
        normalized.attribute_value("escaped\"name").unwrap(),
        Some(storage_types::AttributeValue::S("line\nbreak".to_owned()))
    );
}

#[test]
fn given_invalid_unselected_attributes_when_validating_write_then_reject_them() {
    let invalid = [
        json!({"N":"not-a-number"}),
        json!({"NS":["1","1.0"]}),
        json!({"SS":[]}),
        json!({"SS":["duplicate","duplicate"]}),
        json!({"B":"not base64!"}),
        json!({"BS":["YQ==","YQ=="]}),
        json!({"L":[{"M":{"nested":{"N":"invalid"}}}]}),
        json!({"S":"text","N":"1"}),
        json!({"UNKNOWN":"text"}),
        json!({}),
    ];
    for attribute in invalid {
        let wire = WireItem::dynamo_json(
            serde_json::to_vec(&json!({"pk":{"S":"valid"},"unused":attribute})).unwrap(),
        );
        assert!(wire.validate_for_write("Item").is_err(), "{attribute}");
        assert!(wire.normalized_for_write().is_err(), "{attribute}");
    }
    for bytes in [
        br#"{}"#.as_slice(),
        br#"{"pk":{"S":"a"},"pk":{"S":"b"}}"#,
        br#"{"pk":{"S":"a"}} true"#,
    ] {
        assert!(
            WireItem::dynamo_json(bytes.to_vec())
                .validate_for_write("Item")
                .is_err()
        );
    }
    let oversized = json!({"pk":{"S":"x".repeat(400*1024)}});
    assert!(
        WireItem::dynamo_json(serde_json::to_vec(&oversized).unwrap())
            .validate_for_write("Item")
            .is_err()
    );
}

#[test]
fn given_raw_item_when_replacing_timestamp_then_preserve_other_attributes() {
    use storage_types::{AttributeMap, AttributeValue};
    let wire = WireItem::dynamo_json(br#"{"pk":{"S":"tenant"},"updated_at":{"N":"1"},"u_at":{"N":"2"},"escaped\"name": {"L":[{"N":"3"},{"S":"large"}]}}"#.to_vec());
    let replacements =
        AttributeMap::from_iter([("u_at".to_owned(), AttributeValue::N("4".to_owned()))]);
    let updated = wire
        .with_attribute_updates(&["updated_at"], &replacements)
        .unwrap();
    assert_eq!(
        updated.attribute_value("u_at").unwrap(),
        Some(AttributeValue::N("4".to_owned()))
    );
    let raw: Value = serde_json::from_str(&updated.to_dynamo_json_string().unwrap()).unwrap();
    assert!(raw.get("updated_at").is_none());
    assert_eq!(
        updated.attribute_value("escaped\"name").unwrap(),
        wire.attribute_value("escaped\"name").unwrap()
    );
    assert_eq!(
        updated
            .to_dynamo_json_string()
            .unwrap()
            .matches("\"u_at\"")
            .count(),
        1
    );
}

#[test]
fn given_nested_condition_roots_when_selecting_once_then_match_full_evaluation() {
    use storage_types::AttributeMap;
    let item = json!({"pk":{"S":"tenant"},"nested":{"M":{"children":{"L":[{"N":"7"},{"S":"hello"}]}}},"unused":{"S":"x".repeat(4096)}});
    let bytes = serde_json::to_vec(&item).unwrap();
    let full: AttributeMap = serde_json::from_slice(&bytes).unwrap();
    let wire = WireItem::dynamo_json(bytes);
    let expressions = [
        "#root.children[0] = :seven AND #root.children[1] = :hello",
        "#root.children[0] > :seven OR attribute_not_exists(missing)",
        "NOT (#root.children[0] = :seven)",
        "attribute_exists(#root.children[1]) AND begins_with(#root.children[1], :hello)",
        "contains(#root.children, :seven)",
        "size(#root.children) = :two",
    ];
    for expression in expressions {
        let condition = parse_condition_expression_opt(
            Some(expression.to_owned()),
            Some(
                std::collections::HashMap::from([("#root".to_owned(), "nested".to_owned())]).into(),
            ),
            Some(
                serde_json::from_value(
                    json!({":seven":{"N":"7"},":hello":{"S":"hello"},":two":{"N":"2"}}),
                )
                .unwrap(),
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            evaluate_wire_condition(Some(&wire), &condition).unwrap(),
            storage_condition::evaluate_condition(&full, &condition),
            "{expression}"
        );
    }
}

#[test]
fn given_indexers_when_encoding_raw_item_then_reconstruct_complete_item() {
    use storage_types::{AttributeMap, IndexedWireItem, IndexerDeclaration, MaxIndexers};
    let item = build_write_item(64);
    let bytes = serde_json::to_vec(&item).unwrap();
    let expected: AttributeMap = serde_json::from_slice(&bytes).unwrap();
    let wire = WireItem::dynamo_json(bytes);
    let names = (1..=5).map(|index| format!("indexer{index}")).collect();
    let declaration =
        IndexerDeclaration::try_new(names, MaxIndexers::try_from(5_u8).unwrap()).unwrap();
    let indexed = IndexedWireItem::extract_wire_items(&wire, &wire, &declaration).unwrap();
    assert_eq!(indexed.to_attribute_map().unwrap(), expected);
    let (restored, restored_declaration) = indexed.into_wire_item_with_declaration().unwrap();
    assert_eq!(restored_declaration, declaration);
    assert_eq!(AttributeMap::try_from(&restored).unwrap(), expected);
}

#[test]
fn given_missing_indexer_and_escaped_names_when_restoring_wire_then_preserve_declaration() {
    use storage_types::{AttributeMap, IndexedWireItem, IndexerDeclaration, MaxIndexers};
    let wire = WireItem::dynamo_json(br#"{"pk":{"S":"tenant"},"escaped\"name":{"S":"indexed"},"nested":{"M":{"escaped\"child":{"N":"7"}}}}"#.to_vec());
    let declaration = IndexerDeclaration::try_new(
        vec!["escaped\"name".to_owned(), "missing".to_owned()],
        MaxIndexers::try_from(2_u8).unwrap(),
    )
    .unwrap();
    let indexed = IndexedWireItem::extract_wire_items(&wire, &wire, &declaration).unwrap();
    let (restored, actual) = indexed.into_wire_item_with_declaration().unwrap();
    assert_eq!(actual, declaration);
    assert_eq!(
        AttributeMap::try_from(&restored).unwrap(),
        AttributeMap::try_from(&wire).unwrap()
    );
}

#[test]
fn given_corrupt_indexed_residual_when_restoring_wire_then_reject_invalid_markers() {
    use storage_types::{INDEXED_VALUE_RAW_HEADER, IndexedWireItem, indexer_tuple_index};
    let index = indexer_tuple_index(0);
    let invalid = [
        json!({"pk":{"S":"tenant"},"slot":{"I":index,"S":"extra"}}),
        json!({"pk":{"S":"tenant"},"slot":{"I":index+1}}),
        json!({"pk":{"S":"tenant"},"one":{"I":index},"two":{"I":index}}),
        json!({"pk":{"S":"tenant"},"slot":{"I":-1}}),
        json!({"pk":{"S":"tenant"}}),
        json!({"pk":{"S":"tenant"},"slot":{"I":index},"unused":{"UNKNOWN":"invalid"}}),
    ];
    for item in invalid {
        let bytes = serde_json::to_vec(&item).unwrap();
        let indexed = IndexedWireItem::from_encoded_parts_for_read(
            INDEXED_VALUE_RAW_HEADER,
            &bytes,
            vec![Some("value".to_owned())],
        )
        .unwrap();
        assert!(indexed.into_wire_item_with_declaration().is_err(), "{item}");
    }
}

#[test]
fn given_wide_item_with_duplicate_name_when_validating_then_reject_after_hash_transition() {
    let mut bytes = serde_json::to_vec(&build_write_item(64)).unwrap();
    bytes.pop();
    bytes.extend_from_slice(br#", "pk":{"S":"duplicate"}}"#);
    assert!(
        WireItem::dynamo_json(bytes)
            .validate_for_write("Item")
            .is_err()
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_write_item_shapes_when_normalizing_then_report_cost() {
    use storage_types::{AttributeMap, LogicalItemSize};
    const ITERATIONS: usize = 10_000;
    let mut nested = build_write_item(0);
    nested["payload"] = json!({"M":(0..64).map(|index| (format!("child{index}"), json!({"S":"x".repeat(128)}))).collect::<serde_json::Map<_,_>>()});
    let mut numbers = build_write_item(0);
    numbers["payload"] =
        json!({"L":(0..64).map(|index| json!({"N":format!("{index}e2")})).collect::<Vec<_>>()});
    for (case, item) in [
        ("flat_small", build_write_item(0)),
        ("flat_wide", build_write_item(64)),
        ("nested_strings", nested),
        ("nested_numbers", numbers),
    ] {
        let bytes = serde_json::to_vec(&item).unwrap();
        let mut expected: AttributeMap = serde_json::from_slice(&bytes).unwrap();
        expected.normalize_numbers_for_write();
        let (normalized, _) = WireItem::dynamo_json(bytes.clone())
            .normalized_for_write()
            .unwrap();
        assert_eq!(AttributeMap::try_from(&normalized).unwrap(), expected);
        for mode in ["map", "wire"] {
            let label = format!("normalize_{case}_{mode}");
            let guard = AllocationGuard::start(
                module_path!(),
                "write_normalize",
                file!(),
                line!(),
                Some(&label),
            );
            let started = Instant::now();
            let cpu_started = cpu_time::ProcessTime::try_now().unwrap();
            for _ in 0..ITERATIONS {
                if mode == "map" {
                    let mut item: AttributeMap = serde_json::from_slice(black_box(&bytes)).unwrap();
                    let size = LogicalItemSize::try_from_entries(item.iter()).unwrap();
                    item.normalize_numbers_for_write();
                    black_box((serde_json::to_vec(&item).unwrap(), size));
                } else {
                    black_box(
                        WireItem::dynamo_json(black_box(bytes.clone()))
                            .normalized_for_write()
                            .unwrap(),
                    );
                }
            }
            report_write_cost(
                &label,
                ITERATIONS,
                started.elapsed(),
                cpu_started.try_elapsed().unwrap(),
            );
            alloc_counter::emit_report(&guard.finish()).unwrap();
        }
    }
}
