use httpmock::prelude::*;
use storage_provider::{RemoteCredentialStrategy, RemoteStorageSettings, RemoteTimeoutOverrides};
use storage_types::{DescribeTableRequest, TableName};

use super::{constants::MAX_TRANSACTION_DURATION, provider::apply_default_timeout};

fn settings(timeouts: Option<RemoteTimeoutOverrides>) -> RemoteStorageSettings {
    RemoteStorageSettings {
        endpoint_urls: vec!["http://private-storage".to_string()],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts,
    }
}

#[test]
fn given_missing_connector_timeouts_when_defaults_are_applied_then_requests_are_bounded() {
    let mut settings = settings(None);

    apply_default_timeout(&mut settings);

    let timeouts = settings.timeouts.expect("default timeouts");
    let default_timeout_ms = u64::try_from(MAX_TRANSACTION_DURATION.as_millis()).unwrap();
    assert_eq!(timeouts.connect_timeout_ms, Some(default_timeout_ms));
    assert_eq!(timeouts.request_timeout_ms, Some(default_timeout_ms));
}

#[test]
fn given_a_connect_timeout_when_request_timeout_is_missing_then_connect_setting_is_preserved() {
    let mut settings = settings(Some(RemoteTimeoutOverrides {
        connect_timeout_ms: Some(17),
        request_timeout_ms: None,
    }));

    apply_default_timeout(&mut settings);

    let timeouts = settings.timeouts.expect("default request timeout");
    let default_timeout_ms = u64::try_from(MAX_TRANSACTION_DURATION.as_millis()).unwrap();
    assert_eq!(timeouts.connect_timeout_ms, Some(17));
    assert_eq!(timeouts.request_timeout_ms, Some(default_timeout_ms));
}

#[tokio::test]
async fn given_a_job_store_307_redirect_when_invoking_dynamodb_then_post_is_preserved() {
    assert_redirect_preserves_post(307).await;
}

#[tokio::test]
async fn given_a_job_store_308_redirect_when_invoking_dynamodb_then_post_is_preserved() {
    assert_redirect_preserves_post(308).await;
}

async fn assert_redirect_preserves_post(status: u16) {
    let alias = MockServer::start_async().await;
    let target = alias
        .mock_async(|when, then| {
            when.method(POST)
                .path("/target")
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .body_includes("remote-test-table");
            then.status(200).json_body(serde_json::json!({
                "Table": {
                    "TableName": "remote-test-table",
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": "arn:aws:dynamodb:local:table/remote-test-table",
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;
    let redirect = alias
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable");
            then.status(status).header("location", "/target");
        })
        .await;

    let mut provider =
        crate::provider::implementation::RemoteStorageProvider::new(settings_for(alias.url("/")))
            .await
            .expect("remote provider");
    provider.enable_scheduled_job_http_redirects();
    let response: storage_types::DescribeTableResponse = provider
        .invoke(
            "DynamoDB_20120810.DescribeTable",
            &DescribeTableRequest {
                table_name: TableName::new("remote-test-table"),
            },
        )
        .await
        .expect("redirected DynamoDB request");

    assert_eq!(
        response.table.table_name,
        TableName::new("remote-test-table")
    );
    redirect.assert_calls_async(1).await;
    target.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_a_cross_host_307_redirect_when_invoking_dynamodb_then_post_identity_is_preserved() {
    assert_cross_host_redirect_preserves_post(307).await;
}

#[tokio::test]
async fn given_a_cross_host_308_redirect_when_invoking_dynamodb_then_post_identity_is_preserved() {
    assert_cross_host_redirect_preserves_post(308).await;
}

async fn assert_cross_host_redirect_preserves_post(status: u16) {
    let alias = MockServer::start_async().await;
    let target = MockServer::start_async().await;
    let expected_request = serde_json::json!({"TableName": "remote-test-table"});

    let target_request = target
        .mock_async(|when, then| {
            when.method(POST)
                .path("/target")
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .header_missing("authorization")
                .json_body(expected_request.clone());
            then.status(200).json_body(serde_json::json!({
                "Table": {
                    "TableName": "remote-test-table",
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [],
                    "KeySchema": [],
                    "TableArn": "arn:aws:dynamodb:local:table/remote-test-table",
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await;
    let alias_request = alias
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .header_missing("authorization")
                .json_body(expected_request);
            then.status(status)
                .header("location", target.url("/target"));
        })
        .await;

    let mut provider =
        crate::provider::implementation::RemoteStorageProvider::new(settings_for(alias.url("/")))
            .await
            .expect("remote provider");
    provider.enable_scheduled_job_http_redirects();
    let response: storage_types::DescribeTableResponse = provider
        .invoke(
            "DynamoDB_20120810.DescribeTable",
            &DescribeTableRequest {
                table_name: TableName::new("remote-test-table"),
            },
        )
        .await
        .expect("cross-host redirected DynamoDB request");

    assert_eq!(
        response.table.table_name,
        TableName::new("remote-test-table")
    );
    alias_request.assert_calls_async(1).await;
    target_request.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_a_job_store_302_redirect_when_invoking_dynamodb_then_post_is_not_rewritten() {
    let server = MockServer::start_async().await;
    let target = server
        .mock_async(|when, then| {
            when.method(POST).path("/target");
            then.status(200).json_body(serde_json::json!({}));
        })
        .await;
    let redirect = server
        .mock_async(|when, then| {
            when.method(POST).path("/");
            then.status(302).header("location", "/target");
        })
        .await;
    let mut provider =
        crate::provider::implementation::RemoteStorageProvider::new(settings_for(server.url("/")))
            .await
            .expect("remote provider");
    provider.enable_scheduled_job_http_redirects();

    let error = provider
        .invoke::<_, storage_types::DescribeTableResponse>(
            "DynamoDB_20120810.DescribeTable",
            &DescribeTableRequest {
                table_name: TableName::new("remote-test-table"),
            },
        )
        .await
        .expect_err("302 redirects must be rejected");
    assert!(error.to_string().contains("307 or 308"));
    redirect.assert_calls_async(1).await;
    target.assert_calls_async(0).await;
}

#[tokio::test]
async fn given_a_job_store_redirect_loop_when_invoking_dynamodb_then_loop_is_rejected() {
    let server = MockServer::start_async().await;
    let first = server
        .mock_async(|when, then| {
            when.method(POST).path("/");
            then.status(307).header("location", "/second");
        })
        .await;
    let second = server
        .mock_async(|when, then| {
            when.method(POST).path("/second");
            then.status(307).header("location", "/");
        })
        .await;
    let mut provider =
        crate::provider::implementation::RemoteStorageProvider::new(settings_for(server.url("/")))
            .await
            .expect("remote provider");
    provider.enable_scheduled_job_http_redirects();

    let error = provider
        .invoke::<_, storage_types::DescribeTableResponse>(
            "DynamoDB_20120810.DescribeTable",
            &DescribeTableRequest {
                table_name: TableName::new("remote-test-table"),
            },
        )
        .await
        .expect_err("redirect loop must be rejected");
    assert!(error.to_string().contains("redirect loop"));
    first.assert_calls_async(1).await;
    second.assert_calls_async(1).await;
}

fn settings_for(endpoint: String) -> RemoteStorageSettings {
    RemoteStorageSettings {
        endpoint_urls: vec![endpoint],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    }
}
