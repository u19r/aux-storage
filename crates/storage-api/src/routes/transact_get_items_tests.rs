use serde_json::json;
use storage::DatabaseManager;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, PutItemRequest, TableName, TransactGetItemsRequest,
};

use crate::{
    routes::routes_tests::{
        default_conformance_backends, handle_put_item, handle_transact_get_items,
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
            serde_json::to_value(&response.consumed_capacity).expect("capacity json"),
            json!([
                {
                    "TableName": "TransactGetCapacityB",
                    "CapacityUnits": 0.0,
                    "ReadCapacityUnits": 0.0
                },
                {
                    "TableName": "TransactGetCapacityA",
                    "CapacityUnits": 0.0,
                    "ReadCapacityUnits": 0.0
                }
            ]),
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
            serde_json::to_value(&response.consumed_capacity).expect("capacity json"),
            json!([
                {
                    "TableName": "TransactGetCapacityB",
                    "CapacityUnits": 0.0,
                    "ReadCapacityUnits": 0.0,
                    "Table": {
                        "ReadCapacityUnits": 0.0,
                        "CapacityUnits": 0.0
                    }
                },
                {
                    "TableName": table_a_arn,
                    "CapacityUnits": 0.0,
                    "ReadCapacityUnits": 0.0,
                    "Table": {
                        "ReadCapacityUnits": 0.0,
                        "CapacityUnits": 0.0
                    }
                }
            ]),
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
        "1 validation error detected: Value at 'TransactItems' failed to satisfy constraint: \
         Member must have length less than or equal to 100"
    );
    assert!(
        TransactGetItemsRequest::try_from(too_long_projection)
            .expect_err("long projection should fail")
            .contains("ProjectionExpression")
    );
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
