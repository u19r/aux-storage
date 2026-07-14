use serde_json::json;
use storage::DatabaseManager;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, PutItemRequest, TableName, TransactGetItemsRequest,
};

use crate::{
    routes::routes_test_support::{
        create_transactional_test_db, default_conformance_backends, handle_create_table,
        handle_put_item, handle_transact_get_items,
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
    let db = create_transactional_test_db().await;
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
    assert_eq!(response.responses.len(), 2);
    let first = response.responses[0].item.as_ref().expect("first item");
    assert_eq!(
        first.get("status"),
        Some(&AttributeValue::S("active".to_string()))
    );
    assert!(!first.contains_key("pk"));
    assert!(response.responses[1].item.is_none());
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
