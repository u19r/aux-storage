use std::{collections::HashMap, sync::Arc};

use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use serde_json::json;
use storage_types::{AttributeMap, AttributeValue};

use crate::{
    routes::{
        dynamodb::dynamodb_endpoint,
        routes_tests::{
            for_each_conformance_backend, handle_create_table, handle_get_item, handle_put_item,
        },
    },
    types::{AppState, Response},
};

#[tokio::test]
async fn get_item_empty_number_key_matches_dynamodb_message() {
    for_each_conformance_backend(|backend, db| async move {
        let create_table_payload = json!({
            "TableName": "number_key_table",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "N"},
                {"AttributeName": "sk", "AttributeType": "N"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ]
        });

        handle_create_table(db.clone(), create_table_payload.try_into().unwrap())
            .await
            .expect("create number key table");

        let get_payload = json!({
            "TableName": "number_key_table",
            "Key": {
                "pk": {"N": ""},
                "sk": {"N": "1"}
            }
        });

        let err = handle_get_item(db, get_payload.try_into().unwrap())
            .await
            .expect_err("empty number key should fail");

        assert_eq!(err.status_code, 400, "{backend:?}: {err:?}");
        assert_eq!(
            err.error_type,
            "com.amazon.coral.validate#ValidationException"
        );
        assert_eq!(
            err.message,
            "The parameter cannot be converted to a numeric value: "
        );
    })
    .await;
}

#[tokio::test]
async fn get_item_projection_reconstructs_nested_maps_and_lists_like_dynamodb() {
    for_each_conformance_backend(|_backend, db| async move {
        let create_table_payload = json!({
            "TableName": "NestedProjectionTable",
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"}
            ]
        });

        handle_create_table(db.clone(), create_table_payload.try_into().unwrap())
            .await
            .expect("create table");

        let put_payload = json!({
            "TableName": "NestedProjectionTable",
            "Item": {
                "id": {"S": "item-1"},
                "profile": {"M": {
                    "name": {"S": "Ada"},
                    "age": {"N": "37"},
                    "city": {"S": "London"}
                }},
                "tags": {"L": [{"S": "red"}, {"S": "blue"}]},
                "nested": {"M": {
                    "list": {"L": [
                        {"M": {"v": {"S": "first"}, "x": {"S": "drop"}}},
                        {"M": {"v": {"S": "second"}}}
                    ]}
                }}
            }
        });
        handle_put_item(db.clone(), put_payload.try_into().unwrap())
            .await
            .expect("put item");

        let get_payload = json!({
            "TableName": "NestedProjectionTable",
            "Key": {"id": {"S": "item-1"}},
            "ProjectionExpression": "id, profile.#n, tags[0], nested.#list[0].v",
            "ExpressionAttributeNames": {
                "#n": "name",
                "#list": "list"
            }
        });

        let response = expect_get_item_response(
            handle_get_item(db, get_payload.try_into().unwrap())
                .await
                .unwrap(),
        );
        let item = storage_types::AttributeMap::try_from(response.item.expect("projected item"))
            .expect("decode projected item");

        assert_eq!(
            item.get("id"),
            Some(&AttributeValue::S("item-1".to_string()))
        );
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
    })
    .await;
}

#[tokio::test]
async fn get_item_returns_raw_wire_item_over_http() {
    for_each_conformance_backend(|_backend, db| async move {
        let create_table_payload = json!({
            "TableName": "test_table",
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"}
            ]
        });
        handle_create_table(db.clone(), create_table_payload.try_into().unwrap())
            .await
            .expect("create table");

        let put_payload = json!({
            "TableName": "test_table",
            "Item": {
                "id": {"S": "test123"},
                "name": {"S": "Test Item"}
            }
        });
        handle_put_item(db.clone(), put_payload.try_into().unwrap())
            .await
            .expect("put item");

        let manager_response = handle_get_item(
            db.clone(),
            json!({
                "TableName": "test_table",
                "Key": {
                    "id": {"S": "test123"}
                }
            })
            .try_into()
            .unwrap(),
        )
        .await
        .expect("get item manager response");
        assert!(matches!(manager_response, Response::GetItem(_)));

        let app_state = Arc::new(AppState::new(db));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-target",
            HeaderValue::from_static("DynamoDB_20120810.GetItem"),
        );

        let body = Bytes::from(
            serde_json::to_vec(&json!({
                "TableName": "test_table",
                "Key": {
                    "id": {"S": "test123"}
                }
            }))
            .expect("serialize request"),
        );
        let response = dynamodb_endpoint(State(app_state), headers, body)
            .await
            .expect("get item http response");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("response json");
        assert_eq!(payload["Item"]["name"]["S"], "Test Item");
    })
    .await;
}

fn expect_get_item_response(response: Response) -> storage_types::GetItemResponse {
    match response {
        Response::GetItem(response) => response,
        other => panic!("Expected GetItem response, got: {other:?}"),
    }
}
