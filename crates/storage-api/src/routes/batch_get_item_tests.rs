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
    manager::StorageApiManagerOptions,
    routes::{dynamodb::dynamodb_endpoint, routes_support_tests::create_test_db},
    types::{AppState, Response},
};

#[tokio::test]
async fn batch_get_item_returns_raw_wire_items_over_http() {
    let db = create_test_db().await;
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
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item)
            .build(),
    )
    .await
    .expect("put item");

    let manager_response = crate::routes::routes_support_tests::handle_batch_get_item(
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
    assert!(matches!(manager_response, Response::BatchGetWire(_)));

    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));
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
}
