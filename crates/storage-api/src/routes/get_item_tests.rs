use std::sync::Arc;

use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use serde_json::json;
use storage::DatabaseManager;
use storage_types::AttributeValue;

use crate::{
    manager::StorageApiManagerOptions,
    routes::{
        dynamodb::dynamodb_endpoint,
        routes_support_tests::{
            create_test_db, handle_create_table, handle_get_item, handle_put_item,
        },
    },
    types::{AppState, Response},
};

async fn create_test_db_manager() -> std::sync::Arc<DatabaseManager> {
    create_test_db().await
}

#[tokio::test]
async fn put_and_get_item_handler() {
    let db = create_test_db_manager().await;
    let create_table_payload = json!({
        "TableName": "test_table",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ]
    });

    let create_result =
        handle_create_table(db.clone(), create_table_payload.try_into().unwrap()).await;
    assert!(
        create_result.is_ok(),
        "CreateTable should succeed: {create_result:?}",
    );
    let put_payload = json!({
        "TableName": "test_table",
        "Item": {
            "id": {"S": "test123"},
            "name": {"S": "Test Item"},
            "count": {"N": "42"}
        }
    });

    let put_result = handle_put_item(db.clone(), put_payload.try_into().unwrap()).await;
    assert!(put_result.is_ok(), "PutItem should succeed: {put_result:?}",);
    let get_payload = json!({
        "TableName": "test_table",
        "Key": {
            "id": {"S": "test123"}
        }
    });

    let get_result = handle_get_item(db.clone(), get_payload.try_into().unwrap()).await;
    assert!(get_result.is_ok(), "GetItem should succeed: {get_result:?}",);
    let response = expect_get_item_response(get_result.unwrap());
    assert!(response.item.is_some(), "GetItem should return an item");
    let json_response = serde_json::to_value(&response).expect("Should serialize to JSON");

    let item = response.item.unwrap();

    assert!(item.len() >= 3, "Should have at least 3 attributes");
    assert_eq!(
        item.get("id"),
        Some(&AttributeValue::S("test123".to_string()))
    );
    assert_eq!(
        item.get("name"),
        Some(&AttributeValue::S("Test Item".to_string()))
    );
    assert_eq!(
        item.get("count"),
        Some(&AttributeValue::N("42".to_string()))
    );
    assert!(
        json_response.get("Item").is_some(),
        "JSON should have Item field"
    );
    let json_item = json_response.get("Item").unwrap();
    assert!(json_item.is_object(), "Item should be JSON object");

    let json_item_obj = json_item.as_object().unwrap();
    assert!(
        json_item_obj.len() >= 3,
        "JSON Item should have at least 3 attributes"
    );
    if let Some(id_attr) = json_item_obj.get("id") {
        assert!(id_attr.is_object(), "ID attribute should be object");
        assert!(id_attr.get("S").is_some(), "ID should have S field");
        assert_eq!(id_attr.get("S").unwrap().as_str(), Some("test123"));
    }
}

#[tokio::test]
async fn get_item_nonexistent() {
    let db = create_test_db_manager().await;
    let create_table_payload = json!({
        "TableName": "test_table",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ]
    });

    let create_result =
        handle_create_table(db.clone(), create_table_payload.try_into().unwrap()).await;
    assert!(create_result.is_ok(), "CreateTable should succeed");

    // Try to get a nonexistent item
    let get_payload = json!({
        "TableName": "test_table",
        "Key": {
            "id": {"S": "nonexistent"}
        }
    });

    let get_result = handle_get_item(db.clone(), get_payload.try_into().unwrap()).await;
    assert!(
        get_result.is_ok(),
        "GetItem should succeed even for nonexistent items: {get_result:?}"
    );

    let response = expect_get_item_response(get_result.unwrap());
    assert!(
        response.item.is_none(),
        "GetItem should return None for nonexistent items"
    );
    let json_response = serde_json::to_value(&response).expect("Should serialize to JSON");

    // Should not have Item field or it should be null
    assert!(
        json_response.get("Item").is_none() || json_response.get("Item") == Some(&json!(null)),
        "JSON should not have Item field for nonexistent items"
    );
}

#[tokio::test]
async fn get_item_returns_raw_wire_item_over_http() {
    let db = create_test_db_manager().await;
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
    assert!(matches!(manager_response, Response::GetWire(_)));

    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));
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
}

fn expect_get_item_response(response: Response) -> storage_types::GetItemResponse {
    match response {
        Response::GetItem(response) => response,
        Response::GetWire(response) => response.into_get_item_response().unwrap(),
        other => panic!("Expected GetItem response, got: {other:?}"),
    }
}
