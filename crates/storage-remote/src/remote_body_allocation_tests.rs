use std::collections::HashMap;

use aws_sigv4_signing::{AwsRequestSigner, AwsStaticCredentials, CredentialSource};
use storage_provider::{DynamoOperations as _, RemoteCredentialStrategy, RemoteStorageSettings};
use storage_types::{AttributeValue, GetItemRequest, TableName};

use crate::RemoteStorageProvider;

async fn create_provider(url: &str, signed: bool) -> RemoteStorageProvider {
    let mut provider = RemoteStorageProvider::new(RemoteStorageSettings {
        endpoint_urls: vec![url.to_owned()],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    })
    .await
    .expect("remote provider");
    if signed {
        // This seam exercises signing bytes over local HTTP; it does not prove
        // TLS.
        provider.endpoints[0].requires_signature = true;
        provider.endpoints[0].uri = "https://dynamodb.eu-west-2.amazonaws.com/"
            .parse()
            .expect("signing URI");
        provider.signer = Some(
            AwsRequestSigner::new(
                "eu-west-2",
                CredentialSource::Static(AwsStaticCredentials {
                    access_key_id: "AKIDEXAMPLE".to_owned(),
                    secret_access_key: "fixture-only-secret".to_owned(),
                    session_token: None,
                }),
                "dynamodb",
            )
            .expect("fixture signer"),
        );
    }
    provider
}

fn proof_request() -> GetItemRequest {
    let mut request = GetItemRequest::new(
        TableName::new("remote-body-proof"),
        HashMap::from([("pk".to_owned(), AttributeValue::S("p".to_owned()))])
            .into_iter()
            .collect::<storage_types::KeyAttributes>(),
    );
    request.consistent_read = Some(true);
    request
}

#[tokio::test]
async fn given_wire_response_when_provider_is_dropped_then_all_item_bytes_remain_owned() {
    let server = httpmock::MockServer::start_async().await;
    let item = serde_json::json!({"pk": {"S": "p"}, "data": {"S": "é\"\\\n".repeat(256)}});
    let expected = item.clone();
    let response = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .json_body(serde_json::json!({
                    "TableName": "remote-body-proof", "Key": {"pk": {"S": "p"}},
                    "ConsistentRead": true,
                }));
            then.status(200)
                .json_body(serde_json::json!({"Item": item}));
        })
        .await;
    let provider = create_provider(&server.url("/"), false).await;
    let result = provider
        .get_item(proof_request())
        .await
        .expect("get success")
        .item
        .expect("present item");
    drop(provider);
    let attributes = result.into_attribute_map().expect("owned item decode");
    assert_eq!(
        serde_json::to_value(attributes).expect("item JSON"),
        expected
    );
    response.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_invalid_success_body_when_provider_decodes_then_it_returns_serialization_error() {
    use storage_types::{StorageEnum, context::WrappedError as _};

    let server = httpmock::MockServer::start_async().await;
    let response = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::POST);
            then.status(200).body("{invalid-json");
        })
        .await;
    let provider = create_provider(&server.url("/"), false).await;
    let error = provider
        .get_item(proof_request())
        .await
        .expect_err("malformed success body is not an item");
    assert!(matches!(error.to_enum(), StorageEnum::Serialization(_)));
    response.assert_calls_async(1).await;
}
