use std::collections::HashMap;

use httpmock::prelude::*;
use storage_provider::{DynamoOperations as _, RemoteCredentialStrategy};
use storage_types::{
    AttributeValue, TableName, TransactGetItem, TransactGetItemsRequest, TransactGetRequest,
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
    mock.assert_calls_async(1).await;
}
