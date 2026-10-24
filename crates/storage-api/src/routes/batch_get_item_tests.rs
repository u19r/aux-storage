use std::sync::Arc;

use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use serde_json::json;
use storage::PutItemInput;
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, TableName,
};

use crate::{
    routes::{
        dynamodb::dynamodb_endpoint,
        routes_tests::{for_each_conformance_backend, handle_batch_get_item, seed_item},
    },
    types::{AppState, Response},
};

#[tokio::test]
async fn batch_get_item_returns_raw_wire_items_over_http() {
    for_each_conformance_backend(|_backend, db| async move {
        let table_name = TableName::new("BatchGetRawTable");
        db.create_table(&CreateTableRequest::new(
            table_name.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            BillingMode::PayPerRequest,
        ))
        .await
        .expect("create table");

        let mut item = std::collections::HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("item-1".to_string()));
        item.insert(
            "payload".to_string(),
            AttributeValue::S("value-1".to_string()),
        );
        seed_item(
            &db,
            PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item)
                .build(),
        )
        .await
        .expect("put item");

        let manager_response = crate::routes::routes_tests::handle_batch_get_item(
            db.clone(),
            serde_json::from_value(json!({
                "RequestItems": {
                    "BatchGetRawTable": {
                        "Keys": [{"pk": {"S": "item-1"}}]
                    }
                }
            }))
            .expect("batch get request"),
        )
        .await
        .expect("batch get manager response");
        assert!(matches!(manager_response, Response::BatchGetItem(_)));

        let app_state = Arc::new(AppState::new(db));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-target",
            HeaderValue::from_static("DynamoDB_20120810.BatchGetItem"),
        );

        let body = Bytes::from(
            serde_json::to_vec(&json!({
                "RequestItems": {
                    "BatchGetRawTable": {
                        "Keys": [{"pk": {"S": "item-1"}}]
                    }
                }
            }))
            .expect("serialize request"),
        );

        let response = dynamodb_endpoint(State(app_state), headers, body)
            .await
            .expect("batch get http response");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("response json");
        assert_eq!(
            payload["Responses"]["BatchGetRawTable"][0]["payload"]["S"],
            "value-1"
        );
    })
    .await;
}

#[tokio::test]
async fn batch_get_item_projection_returns_only_requested_nested_attributes() {
    for_each_conformance_backend(|_backend, db| async move {
        create_batch_get_pk_sk_table(db.as_ref(), "BatchGetProjectionTable").await;
        put_projection_item(db.as_ref(), "BatchGetProjectionTable").await;

        let manager_response = handle_batch_get_item(
            db,
            serde_json::from_value(json!({
                "RequestItems": {
                    "BatchGetProjectionTable": {
                        "Keys": [{"pk": {"S": "p1"}, "sk": {"S": "s1"}}],
                        "ProjectionExpression": "#m.#comment, l[0].child",
                        "ExpressionAttributeNames": {
                            "#m": "m",
                            "#comment": "COMMENT"
                        }
                    }
                }
            }))
            .expect("batch get request"),
        )
        .await
        .expect("batch get manager response");

        let Response::BatchGetItem(response) = manager_response else {
            panic!("projection should use decoded BatchGetItem response");
        };
        let responses = response.responses.expect("responses");
        let items = responses
            .get(&TableName::new("BatchGetProjectionTable"))
            .expect("table response");
        assert_eq!(items.len(), 1);
        let item = storage_types::AttributeMap::try_from(items.first().unwrap())
            .unwrap()
            .to_hashmap();
        assert_eq!(
            item.get("m"),
            Some(&AttributeValue::M(
                [(
                    "COMMENT".to_string(),
                    AttributeValue::S("visible".to_string())
                )]
                .into()
            ))
        );
        assert_eq!(
            item.get("l"),
            Some(&AttributeValue::L(vec![AttributeValue::M(
                [("child".to_string(), AttributeValue::S("first".to_string()))].into()
            )]))
        );
        assert!(!item.contains_key("hidden"));
    })
    .await;
}

#[tokio::test]
async fn batch_get_item_includes_empty_response_for_requested_table_with_no_matches() {
    for_each_conformance_backend(|_backend, db| async move {
        create_batch_get_pk_sk_table(db.as_ref(), "BatchGetMissingTable").await;

        let manager_response = handle_batch_get_item(
            db,
            serde_json::from_value(json!({
                "RequestItems": {
                    "BatchGetMissingTable": {
                        "Keys": [{"pk": {"S": "missing"}, "sk": {"S": "missing"}}]
                    }
                }
            }))
            .expect("batch get request"),
        )
        .await
        .expect("batch get manager response");

        let Response::BatchGetItem(response) = manager_response else {
            panic!("expected batch get response");
        };
        let responses = response.responses.expect("responses");
        assert_eq!(
            responses
                .get(&TableName::new("BatchGetMissingTable"))
                .expect("requested table should be present")
                .len(),
            0
        );
        assert_eq!(
            response
                .unprocessed_keys
                .expect("empty unprocessed keys should be present")
                .len(),
            0
        );
    })
    .await;
}

#[tokio::test]
async fn batch_get_item_rejects_invalid_key_schema_and_duplicate_keys() {
    for_each_conformance_backend(|_backend, db| async move {
        create_batch_get_pk_sk_table(db.as_ref(), "BatchGetKeyValidationTable").await;

        let missing_sort_key = handle_batch_get_item(
            db.clone(),
            serde_json::from_value(json!({
                "RequestItems": {
                    "BatchGetKeyValidationTable": {
                        "Keys": [{"pk": {"S": "p1"}}]
                    }
                }
            }))
            .expect("batch get request"),
        )
        .await
        .expect_err("missing sort key should fail");
        assert_eq!(
            missing_sort_key.message,
            "The provided key element does not match the schema"
        );

        let duplicate_keys = handle_batch_get_item(
            db.clone(),
            serde_json::from_value(json!({
                "RequestItems": {
                    "BatchGetKeyValidationTable": {
                        "Keys": [
                            {"pk": {"S": "p1"}, "sk": {"S": "s1"}},
                            {"sk": {"S": "s1"}, "pk": {"S": "p1"}}
                        ]
                    }
                }
            }))
            .expect("batch get request"),
        )
        .await
        .expect_err("duplicate keys should fail");
        assert_eq!(
            duplicate_keys.message,
            "Provided list of item keys contains duplicates"
        );

        let numeric_table = TableName::new("BatchGetNumericValidationTable");
        db.create_table(&CreateTableRequest::new(
            numeric_table.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::N,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            BillingMode::PayPerRequest,
        ))
        .await
        .expect("create numeric validation table");

        let invalid_numeric_key = handle_batch_get_item(
            db,
            serde_json::from_value(json!({
                "RequestItems": {
                    "BatchGetNumericValidationTable": {
                        "Keys": [{"pk": {"N": "1E-131"}}]
                    }
                }
            }))
            .expect("batch get invalid numeric request"),
        )
        .await
        .expect_err("out-of-range numeric key should fail before provider execution");
        assert!(
            invalid_numeric_key.message.contains("underflow"),
            "unexpected invalid numeric key message: {}",
            invalid_numeric_key.message
        );
    })
    .await;
}

async fn create_batch_get_pk_sk_table(db: &storage::DatabaseManager, table_name: &str) {
    db.create_table(&CreateTableRequest::new(
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
    .expect("create table");
}

async fn put_projection_item(db: &storage::DatabaseManager, table_name: &str) {
    let mut item = std::collections::HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("p1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("s1".to_string()));
    item.insert(
        "m".to_string(),
        AttributeValue::M(
            [
                (
                    "COMMENT".to_string(),
                    AttributeValue::S("visible".to_string()),
                ),
                ("other".to_string(), AttributeValue::S("hidden".to_string())),
            ]
            .into(),
        ),
    );
    item.insert(
        "l".to_string(),
        AttributeValue::L(vec![AttributeValue::M(
            [
                ("child".to_string(), AttributeValue::S("first".to_string())),
                ("other".to_string(), AttributeValue::S("hidden".to_string())),
            ]
            .into(),
        )]),
    );
    item.insert("hidden".to_string(), AttributeValue::S("no".to_string()));

    seed_item(
        db,
        PutItemInput::builder()
            .table_name(TableName::new(table_name))
            .item(item)
            .build(),
    )
    .await
    .expect("put item");
}
