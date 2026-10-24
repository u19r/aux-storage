use serde_json::json;
use storage::DatabaseManager;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, PutItemRequest, TableName, TransactGetItemsRequest,
};

use crate::{
    routes::routes_tests::{
        default_conformance_backends, handle_create_table, handle_put_item,
        handle_transact_get_items,
    },
    types::Response,
};

async fn setup_table(db: &DatabaseManager) {
    setup_named_table(db, "TransactGetTable").await;
}

async fn setup_named_table(db: &DatabaseManager, table_name: &str) {
    let request = CreateTableRequest::new(
        TableName::new(table_name),
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
    db.create_table(&request).await.expect("create table");
}

#[tokio::test]
async fn transact_get_items_returns_ordered_item_responses() {
    for backend in default_conformance_backends() {
        let db = backend.create_transactional_db().await;
        setup_table(&db).await;
        let put = json!({
            "TableName": "TransactGetTable",
            "Item": {
                "pk": { "S": "item#1" },
                "status": { "S": "active" }
            }
        });
        handle_put_item(
            db.clone(),
            PutItemRequest::try_from(put).expect("valid put item"),
        )
        .await
        .expect("put item");

        let request = json!({
            "TransactItems": [
                {
                    "Get": {
                        "TableName": "TransactGetTable",
                        "Key": { "pk": { "S": "item#1" } },
                        "ProjectionExpression": "#s",
                        "ExpressionAttributeNames": { "#s": "status" }
                    }
                },
                {
                    "Get": {
                        "TableName": "TransactGetTable",
                        "Key": { "pk": { "S": "missing" } }
                    }
                }
            ]
        });

        let response = handle_transact_get_items(
            db,
            TransactGetItemsRequest::try_from(request).expect("valid transact get"),
        )
        .await
        .expect("transact get succeeds");

        let Response::TransactGetItems(response) = response else {
            panic!("expected TransactGetItems response");
        };
        assert_eq!(response.responses.len(), 2, "{}", backend.name);
        let first =
            storage_types::AttributeMap::try_from(response.responses[0].item.clone().unwrap())
                .unwrap();
        assert_eq!(
            first.get("status"),
            Some(&AttributeValue::S("active".to_string())),
            "{}",
            backend.name
        );
        assert!(!first.contains_key("pk"), "{}", backend.name);
        assert!(response.responses[1].item.is_none(), "{}", backend.name);
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
async fn transact_get_endpoint_repeated_response_report() {
    use std::{hint::black_box, sync::Arc, time::Instant};

    use alloc_counter::AllocationGuard;
    use axum::{
        body::{self, Bytes},
        extract::State,
        http::{HeaderMap, HeaderValue, StatusCode},
    };

    use crate::{routes::dynamodb::dynamodb_endpoint, types::AppState};

    const ITERATIONS: usize = 10_000;
    let db = super::routes_tests::create_sqlite_test_db().await;
    setup_table(db.as_ref()).await;
    handle_put_item(
        db.clone(),
        json!({"TableName":"TransactGetTable","Item":{
            "pk":{"S":"item#1"},"status":{"S":"active"}
        }})
        .try_into()
        .unwrap(),
    )
    .await
    .unwrap();
    let app_state = Arc::new(AppState::new(db));
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.TransactGetItems"),
    );
    let request = Bytes::from_static(br#"{"TransactItems":[{"Get":{"TableName":"TransactGetTable","Key":{"pk":{"S":"item#1"}}}},{"Get":{"TableName":"TransactGetTable","Key":{"pk":{"S":"missing"}}}}]}"#);
    for _ in 0..100 {
        let response =
            dynamodb_endpoint(State(app_state.clone()), headers.clone(), request.clone())
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let guard = AllocationGuard::start(
        module_path!(),
        "transact_get_endpoint_repeated_response_report",
        file!(),
        line!(),
        Some("transact_get_endpoint"),
    );
    let started = Instant::now();
    let mut total_bytes = 0;
    for _ in 0..ITERATIONS {
        let response = dynamodb_endpoint(
            State(app_state.clone()),
            headers.clone(),
            black_box(request.clone()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        total_bytes += body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .len();
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).unwrap();
    assert!(total_bytes > 0);
    println!(
        "transact_get_endpoint iterations={ITERATIONS} elapsed_ns={}",
        elapsed.as_nanos()
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn transact_get_response_wire_clone_and_serde_report() {
    use std::{hint::black_box, time::Instant};

    use alloc_counter::AllocationGuard;
    use serde::Serialize;
    use storage_types::{ItemResponse, TransactGetItemsResponse, WireItem};

    #[derive(Clone, Serialize)]
    struct MapSlot {
        #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
        item: Option<storage_types::AttributeMap>,
    }

    #[derive(Clone, Serialize)]
    struct MapResponse {
        #[serde(rename = "Responses")]
        responses: Vec<MapSlot>,
    }

    const ITERATIONS: usize = 10_000;
    const ITEM: &[u8] =
        br#"{"pk":{"S":"item#1"},"status":{"S":"active"},"nested":{"M":{"child":{"S":"value"}}}}"#;
    let map = MapResponse {
        responses: vec![MapSlot {
            item: Some(serde_json::from_slice(ITEM).unwrap()),
        }],
    };
    let wire = TransactGetItemsResponse {
        responses: vec![ItemResponse {
            item: Some(WireItem::dynamo_json(ITEM.to_vec())),
        }],
        consumed_capacity: None,
    };
    for label in ["map_clone", "wire_clone", "map_serde", "wire_serde"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "transact_get_response_wire_clone_and_serde_report",
            file!(),
            line!(),
            Some(label),
        );
        let started = Instant::now();
        let mut total = 0;
        for _ in 0..ITERATIONS {
            total += match label {
                "map_clone" => black_box(map.clone()).responses.len(),
                "wire_clone" => black_box(wire.clone()).responses.len(),
                "map_serde" => serde_json::to_vec(black_box(&map)).unwrap().len(),
                _ => serde_json::to_vec(black_box(&wire)).unwrap().len(),
            };
        }
        let elapsed = started.elapsed();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert!(total > 0);
        println!(
            "transact_get_response label={label} iterations={ITERATIONS} elapsed_ns={}",
            elapsed.as_nanos()
        );
    }
}

#[tokio::test]
async fn transact_get_items_response_shape_matches_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_transactional_db().await;
        setup_table(&db).await;
        handle_put_item(
            db.clone(),
            json!({
                "TableName": "TransactGetTable",
                "Item": {
                    "pk": { "S": "item#1" },
                    "status": { "S": "active" },
                    "nested": { "M": { "child": { "S": "value" } } }
                }
            })
            .try_into()
            .expect("valid put item"),
        )
        .await
        .expect("put item");

        let response = transact_get_response(
            db.clone(),
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "TableName": "TransactGetTable",
                            "Key": { "pk": { "S": "item#1" } }
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TransactGetTable",
                            "Key": { "pk": { "S": "missing" } }
                        }
                    }
                ],
                "ReturnConsumedCapacity": "TOTAL"
            }),
        )
        .await;

        assert_eq!(response.responses.len(), 2, "{}", backend.name);
        assert!(response.responses[0].item.is_some(), "{}", backend.name);
        assert!(response.responses[1].item.is_none(), "{}", backend.name);
        assert_eq!(
            response.consumed_capacity,
            Some(json!([{
                "TableName": "TransactGetTable",
                "CapacityUnits": 4.0,
                "ReadCapacityUnits": 4.0
            }])),
            "{}",
            backend.name
        );

        let indexes_response = transact_get_response(
            db.clone(),
            json!({
                "TransactItems": [{
                    "Get": {
                        "TableName": "TransactGetTable",
                        "Key": { "pk": { "S": "item#1" } }
                    }
                }],
                "ReturnConsumedCapacity": "INDEXES"
            }),
        )
        .await;
        assert_eq!(
            indexes_response.consumed_capacity,
            Some(json!([{
                "TableName": "TransactGetTable",
                "CapacityUnits": 2.0,
                "ReadCapacityUnits": 2.0,
                "Table": {
                    "ReadCapacityUnits": 2.0,
                    "CapacityUnits": 2.0
                }
            }])),
            "{}",
            backend.name
        );

        for projection_expression in ["absent", "nested.absent"] {
            let response = transact_get_response(
                db.clone(),
                json!({
                    "TransactItems": [{
                        "Get": {
                            "TableName": "TransactGetTable",
                            "Key": { "pk": { "S": "item#1" } },
                            "ProjectionExpression": projection_expression
                        }
                    }]
                }),
            )
            .await;
            assert_eq!(response.responses.len(), 1, "{}", backend.name);
            assert!(response.responses[0].item.is_none(), "{}", backend.name);
        }
    }
}

#[tokio::test]
async fn transact_get_duplicate_detection_treats_table_name_and_arn_as_same_item() {
    for backend in default_conformance_backends() {
        let db = backend.create_transactional_db().await;
        setup_table(&db).await;

        let err = transact_get_error(
            db,
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "TableName": "TransactGetTable",
                            "Key": { "pk": { "S": "item#1" } }
                        }
                    },
                    {
                        "Get": {
                            "TableName": "arn:aws:dynamodb:eu-central-1:123456789012:table/TransactGetTable",
                            "Key": { "pk": { "S": "item#1" } }
                        }
                    }
                ]
            }),
        )
        .await;

        assert_eq!(
            err.error_type, "com.amazon.coral.validate#ValidationException",
            "{}",
            backend.name
        );
        assert_eq!(
            err.message, "Transaction request cannot include multiple operations on one item",
            "{}",
            backend.name
        );
    }
}

#[tokio::test]
async fn transact_get_consumed_capacity_groups_and_orders_like_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_transactional_db().await;
        setup_named_table(&db, "TransactGetCapacityA").await;
        setup_named_table(&db, "TransactGetCapacityB").await;
        for (table_name, keys) in [
            ("TransactGetCapacityA", ["a1", "a2"]),
            ("TransactGetCapacityB", ["b1", "b2"]),
        ] {
            for key in keys {
                handle_put_item(
                    db.clone(),
                    json!({
                        "TableName": table_name,
                        "Item": {
                            "pk": { "S": key },
                            "payload": { "S": key }
                        }
                    })
                    .try_into()
                    .expect("valid put item"),
                )
                .await
                .expect("put item");
            }
        }

        let response = transact_get_response(
            db.clone(),
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "TableName": "TransactGetCapacityB",
                            "Key": { "pk": { "S": "b1" } }
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TransactGetCapacityA",
                            "Key": { "pk": { "S": "a1" } }
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TransactGetCapacityB",
                            "Key": { "pk": { "S": "b2" } }
                        }
                    }
                ],
                "ReturnConsumedCapacity": "TOTAL"
            }),
        )
        .await;
        assert_eq!(
            response.consumed_capacity,
            Some(json!([
                {
                    "TableName": "TransactGetCapacityB",
                    "CapacityUnits": 4.0,
                    "ReadCapacityUnits": 4.0
                },
                {
                    "TableName": "TransactGetCapacityA",
                    "CapacityUnits": 2.0,
                    "ReadCapacityUnits": 2.0
                }
            ])),
            "{}",
            backend.name
        );

        let table_a_arn = "arn:aws:dynamodb:eu-central-1:123456789012:table/TransactGetCapacityA";
        let response = transact_get_response(
            db.clone(),
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "TableName": table_a_arn,
                            "Key": { "pk": { "S": "a1" } }
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TransactGetCapacityB",
                            "Key": { "pk": { "S": "b1" } }
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TransactGetCapacityA",
                            "Key": { "pk": { "S": "a2" } }
                        }
                    }
                ],
                "ReturnConsumedCapacity": "INDEXES"
            }),
        )
        .await;
        assert_eq!(
            response.consumed_capacity,
            Some(json!([
                {
                    "TableName": "TransactGetCapacityB",
                    "CapacityUnits": 2.0,
                    "ReadCapacityUnits": 2.0,
                    "Table": {
                        "ReadCapacityUnits": 2.0,
                        "CapacityUnits": 2.0
                    }
                },
                {
                    "TableName": table_a_arn,
                    "CapacityUnits": 4.0,
                    "ReadCapacityUnits": 4.0,
                    "Table": {
                        "ReadCapacityUnits": 4.0,
                        "CapacityUnits": 4.0
                    }
                }
            ])),
            "{}",
            backend.name
        );
    }
}

#[test]
fn transact_get_items_rejects_limit_and_expression_violations() {
    let too_many = json!({
        "TransactItems": (0..101)
            .map(|idx| json!({
                "Get": {
                    "TableName": "TransactGetTable",
                    "Key": { "pk": { "S": format!("item#{idx}") } }
                }
            }))
            .collect::<Vec<_>>()
    });
    let too_long_projection = json!({
        "TransactItems": [{
            "Get": {
                "TableName": "TransactGetTable",
                "Key": { "pk": { "S": "item#1" } },
                "ProjectionExpression": "a".repeat(storage_types::MAX_EXPRESSION_BYTES + 1)
            }
        }]
    });

    assert_eq!(
        TransactGetItemsRequest::try_from(too_many).expect_err("too many items should fail"),
        "TransactItems cannot contain more than 100 operations"
    );
    assert!(
        TransactGetItemsRequest::try_from(too_long_projection)
            .expect_err("long projection should fail")
            .contains("ProjectionExpression")
    );
}

#[test]
fn transact_get_projection_validation_takes_priority_over_duplicate_keys() {
    let request = json!({
        "TransactItems": [
            {
                "Get": {
                    "TableName": "TxnReadPriority",
                    "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}},
                    "ProjectionExpression": "COMMENT"
                }
            },
            {
                "Get": {
                    "TableName": "TxnReadPriority",
                    "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                }
            },
            {
                "Get": {
                    "TableName": "TxnReadPriority",
                    "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                }
            }
        ]
    });

    assert_eq!(
        TransactGetItemsRequest::try_from(request).expect_err("projection validation should fail"),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
}

#[test]
fn transact_get_alias_validation_takes_priority_over_transaction_preflight() {
    let cases = [
        (
            json!({
                "ProjectionExpression": "#ok",
                "ExpressionAttributeNames": {"ok": "ok"}
            }),
            "ExpressionAttributeNames contains invalid key: Syntax error; key: \"ok\"",
        ),
        (
            json!({
                "ProjectionExpression": "#ok",
                "ExpressionAttributeNames": {"#ok": "ok", "#unused": "COMMENT"}
            }),
            "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}",
        ),
        (
            json!({
                "ProjectionExpression": "#missing"
            }),
            "Invalid ProjectionExpression: An expression attribute name used in the document path \
             is not defined; attribute name: #missing",
        ),
    ];

    for (projection, expected_message) in cases {
        for other_get in [
            json!({
                "TableName": "TxnReadPriority",
                "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
            }),
            json!({
                "TableName": "MissingTxnReadPriority",
                "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
            }),
            json!({
                "TableName": "TxnReadPriority",
                "Key": {"pk": {"N": "1"}, "sk": {"S": "s"}}
            }),
        ] {
            let mut projected_get = json!({
                "TableName": "TxnReadPriority",
                "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
            });
            projected_get
                .as_object_mut()
                .expect("get object")
                .extend(projection.as_object().expect("projection object").clone());

            let request = json!({
                "TransactItems": [
                    {"Get": projected_get},
                    {"Get": other_get}
                ]
            });

            assert_eq!(
                TransactGetItemsRequest::try_from(request)
                    .expect_err("alias validation should fail before preflight"),
                expected_message
            );
        }
    }
}

#[tokio::test]
async fn transact_get_key_validation_takes_priority_over_duplicate_keys() {
    for backend in default_conformance_backends() {
        let db = backend.create_transactional_db().await;
        create_composite_test_table(db.clone()).await;

        let duplicate_err = transact_get_error(
            db.clone(),
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    }
                ]
            }),
        )
        .await;

        assert_eq!(
            duplicate_err.error_type, "com.amazon.coral.validate#ValidationException",
            "{}",
            backend.name
        );
        assert_eq!(
            duplicate_err.message,
            "Transaction request cannot include multiple operations on one item",
            "{}",
            backend.name
        );
        assert!(
            duplicate_err.cancellation_reasons.is_none(),
            "{}",
            backend.name
        );

        let key_validation_err = transact_get_error(
            db.clone(),
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"N": "1"}, "sk": {"S": "s"}}
                        }
                    }
                ]
            }),
        )
        .await;

        assert_eq!(
            key_validation_err.error_type,
            "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
            "{}",
            backend.name
        );
        assert_eq!(
            key_validation_err.message,
            "Transaction cancelled, please refer cancellation reasons for specific reasons [None, \
             None, ValidationError]",
            "{}",
            backend.name
        );
        let reasons = key_validation_err
            .cancellation_reasons
            .expect("cancellation reasons");
        assert_eq!(reasons[0].code, "None", "{}", backend.name);
        assert_eq!(reasons[1].code, "None", "{}", backend.name);
        assert_eq!(reasons[2].code, "ValidationError", "{}", backend.name);
        assert_eq!(
            reasons[2].message.as_deref(),
            Some("The provided key element does not match the schema"),
            "{}",
            backend.name
        );

        let leading_key_validation_err = transact_get_error(
            db,
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"N": "1"}, "sk": {"S": "s"}}
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    }
                ]
            }),
        )
        .await;

        assert_eq!(
            leading_key_validation_err.message,
            "Transaction cancelled, please refer cancellation reasons for specific reasons \
             [ValidationError, None, None]",
            "{}",
            backend.name
        );
        let reasons = leading_key_validation_err
            .cancellation_reasons
            .expect("cancellation reasons");
        assert_eq!(reasons[0].code, "ValidationError", "{}", backend.name);
        assert_eq!(
            reasons[0].message.as_deref(),
            Some("The provided key element does not match the schema"),
            "{}",
            backend.name
        );
        assert_eq!(reasons[1].code, "None", "{}", backend.name);
        assert_eq!(reasons[2].code, "None", "{}", backend.name);
    }
}

#[tokio::test]
async fn transact_get_missing_table_takes_priority_over_duplicate_and_key_validation() {
    for backend in default_conformance_backends() {
        let db = backend.create_transactional_db().await;
        create_composite_test_table(db.clone()).await;

        for request in [
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "TableName": "MissingTxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    }
                ]
            }),
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    },
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    },
                    {
                        "Get": {
                            "TableName": "MissingTxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    }
                ]
            }),
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "TableName": "TxnReadPriority",
                            "Key": {"pk": {"N": "1"}, "sk": {"S": "s"}}
                        }
                    },
                    {
                        "Get": {
                            "TableName": "MissingTxnReadPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}
                        }
                    }
                ]
            }),
        ] {
            let err = transact_get_error(db.clone(), request).await;
            assert_eq!(err.status_code, 400, "{}", backend.name);
            assert!(
                err.error_type.contains("ResourceNotFoundException"),
                "{}: {}",
                backend.name,
                err.error_type
            );
            assert_eq!(
                err.message, "Requested resource not found",
                "{}",
                backend.name
            );
            assert!(err.cancellation_reasons.is_none(), "{}", backend.name);
        }
    }
}

async fn create_composite_test_table(db: std::sync::Arc<DatabaseManager>) {
    handle_create_table(
        db,
        json!({
            "TableName": "TxnReadPriority",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create table");
}

async fn transact_get_error(
    db: std::sync::Arc<DatabaseManager>,
    request: serde_json::Value,
) -> http_error::HttpApiError {
    let request: TransactGetItemsRequest = request.try_into().expect("transact get request");
    handle_transact_get_items(db, request)
        .await
        .expect_err("transaction should fail")
}

async fn transact_get_response(
    db: std::sync::Arc<DatabaseManager>,
    request: serde_json::Value,
) -> storage_types::TransactGetItemsResponse {
    let request: TransactGetItemsRequest = request.try_into().expect("transact get request");
    let response = handle_transact_get_items(db, request)
        .await
        .expect("transaction should succeed");
    let Response::TransactGetItems(response) = response else {
        panic!("expected transact get response");
    };
    response
}
