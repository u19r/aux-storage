use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use httpmock::prelude::*;
use storage_provider::{
    RawOrderedJobStoreError, RemoteCredentialStrategy, RemoteStorageSettings,
    RemoteTimeoutOverrides,
};
use storage_types::{DescribeTableRequest, ErrorClass, TableName};

use super::{
    constants::MAX_TRANSACTION_DURATION,
    provider::{apply_default_timeout, bootstrap_job_table, connect_timeout_for, ensure_job_table},
};

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
    let connect_timeout_ms =
        u64::try_from(connect_timeout_for(MAX_TRANSACTION_DURATION).as_millis()).unwrap();
    assert_eq!(timeouts.connect_timeout_ms, Some(connect_timeout_ms));
    assert_eq!(timeouts.request_timeout_ms, Some(default_timeout_ms));
}

#[test]
fn given_request_timeouts_when_connect_timeout_is_computed_then_it_is_a_quarter_capped_at_two_seconds()
 {
    assert_eq!(
        connect_timeout_for(Duration::from_millis(400)),
        Duration::from_millis(100)
    );
    assert_eq!(
        connect_timeout_for(Duration::from_secs(8)),
        Duration::from_secs(2)
    );
    assert_eq!(
        connect_timeout_for(Duration::from_secs(30)),
        Duration::from_secs(2)
    );
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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, ("remote-test-table").as_bytes()).to_string(),
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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, ("remote-test-table").as_bytes()).to_string(),
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

const JOB_TABLE: &str = "remote-job-table";

fn job_table_not_found_body() -> String {
    serde_json::json!({
        "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
        "message": "Requested resource not found"
    })
    .to_string()
}

fn job_table_description_body() -> String {
    serde_json::json!({
        "Table": {
            "TableName": JOB_TABLE,
            "TableStatus": "ACTIVE",
            "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, JOB_TABLE.as_bytes()).to_string(),
            "CreationDateTime": 0,
            "AttributeDefinitions": [],
            "KeySchema": [],
            "TableArn": format!("arn:aws:dynamodb:local:table/{JOB_TABLE}"),
            "ItemCount": 0,
            "TableSizeBytes": 0
        }
    })
    .to_string()
}

async fn provider_on(
    server: &MockServer,
) -> crate::provider::implementation::RemoteStorageProvider {
    let mut settings = settings(None);
    settings.endpoint_urls = vec![server.url("/")];
    crate::provider::implementation::RemoteStorageProvider::new(settings)
        .await
        .expect("remote provider")
}

fn create_table_unknown_mock(server: &MockServer) -> httpmock::Mock<'_> {
    server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "DynamoDB_20120810.CreateTable");
        then.status(500).body(
            serde_json::json!({
                "__type": "com.amazonaws.dynamodb.v20120810#InternalServerError",
                "message": "internal"
            })
            .to_string(),
        );
    })
}

#[tokio::test]
async fn given_job_table_create_with_unknown_outcome_when_table_then_exists_then_bootstrap_accepts_it()
 {
    let server = MockServer::start_async().await;
    let describe_calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&describe_calls);
    let describe = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable");
            then.respond_with(move |_| {
                if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                    HttpMockResponse::builder()
                        .status(400)
                        .body(job_table_not_found_body())
                        .build()
                } else {
                    HttpMockResponse::builder()
                        .status(200)
                        .body(job_table_description_body())
                        .build()
                }
            });
        })
        .await;
    let create = create_table_unknown_mock(&server);
    let provider = provider_on(&server).await;

    ensure_job_table(&provider, &TableName::new(JOB_TABLE))
        .await
        .expect("an unknown create followed by an existing table is accepted");

    create.assert_calls_async(1).await;
    describe.assert_calls_async(2).await;
}

fn create_table_ok_body() -> String {
    let description: serde_json::Value =
        serde_json::from_str(&job_table_description_body()).expect("describe body");
    serde_json::json!({"TableDescription": description["Table"]}).to_string()
}

#[tokio::test]
async fn given_job_table_create_with_unknown_outcome_when_table_is_absent_then_create_is_sent_again()
 {
    let server = MockServer::start_async().await;
    let describe = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable");
            then.status(400).body(job_table_not_found_body());
        })
        .await;
    let create_calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&create_calls);
    let create = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.CreateTable");
            then.respond_with(move |_| {
                if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                    HttpMockResponse::builder()
                        .status(500)
                        .body(
                            serde_json::json!({
                                "__type": "com.amazonaws.dynamodb.v20120810#InternalServerError",
                                "message": "internal"
                            })
                            .to_string(),
                        )
                        .build()
                } else {
                    HttpMockResponse::builder()
                        .status(200)
                        .body(create_table_ok_body())
                        .build()
                }
            });
        })
        .await;
    let provider = provider_on(&server).await;

    ensure_job_table(&provider, &TableName::new(JOB_TABLE))
        .await
        .expect("an unknown create on an absent table is sent again and succeeds");

    create.assert_calls_async(2).await;
    // One observation before the first send and one between the sends.
    assert!(describe.calls_async().await >= 2);
}

#[tokio::test]
async fn given_job_table_create_that_always_answers_unknown_when_table_stays_absent_then_three_sends_then_unknown_is_returned()
 {
    let server = MockServer::start_async().await;
    let describe = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable");
            then.status(400).body(job_table_not_found_body());
        })
        .await;
    let create = create_table_unknown_mock(&server);
    let provider = provider_on(&server).await;

    let result = ensure_job_table(&provider, &TableName::new(JOB_TABLE)).await;

    match result {
        Err(RawOrderedJobStoreError::Backend(error)) => {
            assert_eq!(error.class(), ErrorClass::CommitUnknown);
        }
        Err(_) => panic!("expected a backend error"),
        Ok(()) => panic!("an absent table must not be accepted"),
    }
    create.assert_calls_async(3).await;
    assert!(describe.calls_async().await >= 3);
}

fn describe_table_mock<'a>(
    server: &'a MockServer,
    status: u16,
    error_type: &str,
) -> httpmock::Mock<'a> {
    let body = serde_json::json!({
        "__type": format!("com.amazonaws.dynamodb.v20120810#{error_type}"),
        "message": "describe table"
    })
    .to_string();
    server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "DynamoDB_20120810.DescribeTable");
        then.status(status).body(body);
    })
}

#[tokio::test]
async fn given_storage_that_stays_unavailable_when_bootstrapping_then_it_retries_until_the_window_ends()
 {
    let server = MockServer::start_async().await;
    let describe = describe_table_mock(&server, 503, "ServiceUnavailable");
    let provider = provider_on(&server).await;

    let result = bootstrap_job_table(&provider, Duration::from_millis(1_500)).await;

    let Err(RawOrderedJobStoreError::Backend(error)) = result else {
        panic!("unavailable storage must fail once the window ends");
    };
    assert!(error.class().is_retryable(), "{error}");
    assert!(describe.calls() >= 2, "bootstrap was not repeated");
}

#[tokio::test]
async fn given_a_non_retryable_failure_when_bootstrapping_then_it_returns_without_waiting() {
    let server = MockServer::start_async().await;
    let describe = describe_table_mock(&server, 400, "ValidationException");
    let provider = provider_on(&server).await;
    let started = std::time::Instant::now();

    let result = bootstrap_job_table(&provider, Duration::from_secs(60)).await;

    assert!(matches!(result, Err(RawOrderedJobStoreError::Backend(_))));
    assert_eq!(describe.calls(), 1);
    assert!(started.elapsed() < Duration::from_secs(10));
}
