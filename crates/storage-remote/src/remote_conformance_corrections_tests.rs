use std::collections::HashMap;

use httpmock::prelude::*;
use storage_provider::{RemoteCredentialStrategy, StorageProvider};
use storage_types::{
    AttributeValue, BatchGetItemRequest, KeyAttributes, KeysAndAttributes, TableName,
    TransactGetItem, TransactGetItemsRequest, TransactGetRequest,
};

use crate::RemoteStorageProvider;

async fn provider_for(server: &MockServer) -> RemoteStorageProvider {
    RemoteStorageProvider::new(storage_provider::RemoteStorageSettings {
        endpoint_urls: vec![server.url("/")],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    })
    .await
    .expect("remote provider")
}

#[tokio::test]
async fn transact_get_items_forwards_the_native_action() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactGetItems")
                .path("/");
            then.status(200).json_body(serde_json::json!({
                "Responses": [{"Item": {"pk": {"S": "item-1"}}}]
            }));
        })
        .await;

    let response = provider
        .transact_get_items(TransactGetItemsRequest {
            transact_items: vec![TransactGetItem {
                get: TransactGetRequest {
                    table_name: TableName::new("items"),
                    key: HashMap::from([(
                        "pk".to_string(),
                        AttributeValue::S("item-1".to_string()),
                    )])
                    .into(),
                    projection_expression: None,
                    expression_attribute_names: None,
                },
            }],
            return_consumed_capacity: None,
        })
        .await
        .expect("native transaction get");

    assert_eq!(response.responses.len(), 1);
    assert_eq!(
        response.responses[0]
            .item
            .as_ref()
            .map(|item| item.required_string_attribute("pk").expect("valid key")),
        Some("item-1".to_string())
    );
    assert!(provider.supports_native_transact_get_items());
    mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn batch_get_forwards_unprojected_items_for_manager_projection() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .path("/")
                .body_excludes("ProjectionExpression")
                .body_excludes("AttributesToGet")
                .body_excludes("ExpressionAttributeNames");
            then.status(200).json_body(serde_json::json!({
                "Responses": {
                    "items": [{
                        "pk": {"S": "item-1"},
                        "list": {"L": [{"S": "first"}, {"S": "second"}]}
                    }]
                }
            }));
        })
        .await;

    let key: KeyAttributes =
        HashMap::from([("pk".to_string(), AttributeValue::S("item-1".to_string()))]).into();
    let response = provider
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([(
                TableName::new("items"),
                KeysAndAttributes {
                    keys: vec![key],
                    attributes_to_get: None,
                    projection_expression: Some("list[1]".to_string()),
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )])
            .into(),
            return_consumed_capacity: None,
        })
        .await
        .expect("remote batch get");

    let item = response
        .responses
        .expect("responses")
        .remove(&TableName::new("items"))
        .expect("table response")
        .pop()
        .expect("item")
        .into_attribute_map()
        .expect("decoded item");
    let AttributeValue::L(values) = item.get("list").expect("list") else {
        panic!("expected list attribute");
    };
    assert_eq!(values.len(), 2);
    mock.assert_calls_async(1).await;
}
