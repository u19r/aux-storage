use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use http_request::HttpRequestDispatchStatus::{self, NotDispatched, PossiblyDispatched};
use httpmock::prelude::*;
use storage_provider::{
    DynamoOperations as _, RemoteCredentialStrategy, RemoteStorageSettings, RemoteTimeoutOverrides,
};
use storage_types::{
    AttemptFailure, AttemptKind, AttemptPhase, ConflictCause, ErrorClass,
    STORAGE_NOT_LEADER_ERROR_TYPE, StorageEnum, StorageError,
};

use super::{
    implementation::{RemoteInvocationError, RemoteStorageProvider},
    replay_policy::{AfterFailure, ReplayPolicy},
};

/// Upper bound for one HTTP test call; a replay loop that hangs fails here.
const TEST_CALL_BOUND: Duration = Duration::from_secs(10);

const INTERNAL_ERROR_TYPE: &str = "com.amazonaws.dynamodb.v20120810#InternalServerError";

async fn provider_with(
    endpoints: Vec<String>,
    request_timeout_ms: Option<u64>,
) -> RemoteStorageProvider {
    RemoteStorageProvider::new(RemoteStorageSettings {
        endpoint_urls: endpoints,
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: request_timeout_ms.map(|ms| RemoteTimeoutOverrides {
            connect_timeout_ms: None,
            request_timeout_ms: Some(ms),
        }),
    })
    .await
    .expect("remote provider")
}

/// An endpoint URL with nothing listening (connect refused).
fn refused_endpoint() -> String {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("reserve port");
    let port = listener.local_addr().expect("address").port();
    drop(listener);
    format!("http://127.0.0.1:{port}/")
}

/// A server that reads each request until `needle` (a body value) arrives,
/// then closes the connection without answering. Returns the URL and the
/// number of accepted connections.
async fn closing_endpoint(needle: &'static [u8]) -> (String, Arc<AtomicUsize>) {
    use tokio::io::AsyncReadExt as _;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let url = format!("http://{}/", listener.local_addr().expect("address"));
    let connections = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&connections);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            counted.fetch_add(1, Ordering::SeqCst);
            let mut received = Vec::new();
            let mut chunk = [0_u8; 4096];
            while !received
                .windows(needle.len())
                .any(|window| window == needle)
            {
                match socket.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => received.extend_from_slice(&chunk[..read]),
                }
            }
            drop(socket);
        }
    });
    (url, connections)
}

fn put_request(table: &str) -> storage_types::PutItemRequest {
    let item: std::collections::HashMap<String, storage_types::AttributeValue> = [
        (
            "pk".to_string(),
            storage_types::AttributeValue::S("T#1".to_string()),
        ),
        (
            "sk".to_string(),
            storage_types::AttributeValue::S("U#1".to_string()),
        ),
    ]
    .into_iter()
    .collect();
    storage_types::PutItemRequest::new(
        storage_types::TableName::new(table),
        storage_types::WireItem::try_from(&item).expect("wire item"),
    )
}

fn assert_commit_unknown(error: &StorageError) {
    assert_eq!(error.class(), ErrorClass::CommitUnknown);
    assert_eq!(error.attempt_kind(), Some(AttemptKind::CommitUnknown));
}

fn internal_error_body() -> String {
    serde_json::json!({"__type": INTERNAL_ERROR_TYPE, "message": "internal"}).to_string()
}

/// A responder that answers `first` once, then `second` for every later call.
fn first_then(
    first: (u16, String),
    second: (u16, String),
) -> impl Fn(&HttpMockRequest) -> HttpMockResponse + Send + Sync + 'static {
    let calls = AtomicUsize::new(0);
    move |_| {
        let (status, body) = if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            &first
        } else {
            &second
        };
        HttpMockResponse::builder()
            .status(*status)
            .body(body.clone())
            .build()
    }
}

fn invocation(error: StorageError, status: HttpRequestDispatchStatus) -> RemoteInvocationError {
    RemoteInvocationError::new(error, status)
}

#[test]
fn given_each_remote_target_when_classified_then_reads_repeat_and_writes_send_once() {
    let repeatable = [
        "DynamoDB_20120810.BatchGetItem",
        "DynamoDB_20120810.DescribeTable",
        "DynamoDB_20120810.DescribeTimeToLive",
        "DynamoDB_20120810.GetItem",
        "DynamoDB_20120810.GetStreamRecords",
        "DynamoDB_20120810.ListTables",
        "DynamoDB_20120810.Query",
        "DynamoDB_20120810.QuerySourcePage",
        "DynamoDB_20120810.ReadSequence",
        "DynamoDB_20120810.Scan",
        "DynamoDB_20120810.TransactGetItems",
        "DynamoDB_20120810.UpdateContinuousBackups",
        "DynamoDBStreams_20120810.DescribeStream",
        "DynamoDBStreams_20120810.GetRecords",
        "DynamoDBStreams_20120810.GetShardIterator",
        "DynamoDBStreams_20120810.ListStreams",
    ];
    let send_once = [
        "DynamoDB_20120810.PutItem",
        "DynamoDB_20120810.UpdateItem",
        "DynamoDB_20120810.DeleteItem",
        "DynamoDB_20120810.BatchWriteItem",
        "DynamoDB_20120810.TransactWriteItems",
        "DynamoDB_20120810.CreateTable",
        "DynamoDB_20120810.DeleteTable",
        "DynamoDB_20120810.UpdateTable",
        "DynamoDB_20120810.UpdateTimeToLive",
        "DynamoDB_20120810.NewOperation",
    ];

    for target in repeatable {
        assert_eq!(
            ReplayPolicy::for_target(target),
            ReplayPolicy::Repeatable,
            "{target}"
        );
    }
    for target in send_once {
        assert_eq!(
            ReplayPolicy::for_target(target),
            ReplayPolicy::SendOnce,
            "{target}"
        );
    }
}

#[test]
fn given_typed_refusals_when_possibly_dispatched_then_not_ambiguous() {
    let message = || "refused".to_string();
    let refusals = [
        StorageError::service_unavailable(1),
        StorageError::from(StorageEnum::Throttled { message: message() }),
        StorageError::from(StorageEnum::ProvisionedThroughputExceeded { message: message() }),
        StorageError::from(StorageEnum::RequestLimitExceeded),
        StorageError::from(StorageEnum::LimitExceeded { message: message() }),
        StorageError::from(StorageEnum::TransactionInProgress { message: message() }),
        StorageError::from(StorageEnum::AwsService {
            code: Some("ExpiredTokenException".to_string()),
            message: message(),
        }),
        StorageError::from(StorageEnum::AwsService {
            code: Some(STORAGE_NOT_LEADER_ERROR_TYPE.to_string()),
            message: message(),
        }),
        StorageError::from(StorageEnum::AwsService {
            code: Some("TransactionCanceledException".to_string()),
            message: message(),
        }),
        StorageError::from(StorageEnum::MissingAuthenticationToken),
        StorageError::from(AttemptFailure::Conflict(ConflictCause::Remote)),
        StorageError::validation("x"),
    ];

    for error in refusals {
        let label = error.label();
        assert!(
            !invocation(error, PossiblyDispatched).is_ambiguous(),
            "{label}"
        );
    }
}

#[test]
fn given_unanswered_or_internal_failures_when_dispatched_then_ambiguous_only_if_possibly_dispatched()
 {
    let failures = [
        || StorageError::internal("remote transport error"),
        || storage_types::expired_in(AttemptPhase::Statement),
        || {
            StorageError::from(StorageEnum::AwsService {
                code: None,
                message: "unknown".to_string(),
            })
        },
        || {
            StorageError::from(StorageEnum::AwsService {
                code: Some("SomeNewException".to_string()),
                message: "unknown".to_string(),
            })
        },
        || StorageError::commit_unknown("x"),
    ];

    for make in failures {
        let label = make().label();
        assert!(
            invocation(make(), PossiblyDispatched).is_ambiguous(),
            "{label} possibly dispatched"
        );
        assert!(
            !invocation(make(), NotDispatched).is_ambiguous(),
            "{label} not dispatched"
        );
    }
}

#[test]
fn given_each_policy_and_failure_when_classified_then_retry_return_or_commit_unknown() {
    let ambiguous = invocation(StorageError::internal("x"), PossiblyDispatched);
    let refused = invocation(StorageError::service_unavailable(1), PossiblyDispatched);

    let cases = [
        (
            ReplayPolicy::SendOnce,
            &ambiguous,
            true,
            AfterFailure::CommitUnknown,
        ),
        (
            ReplayPolicy::SendOnce,
            &ambiguous,
            false,
            AfterFailure::CommitUnknown,
        ),
        (ReplayPolicy::SendOnce, &refused, true, AfterFailure::Retry),
        (
            ReplayPolicy::SendOnce,
            &refused,
            false,
            AfterFailure::Return,
        ),
        (
            ReplayPolicy::Repeatable,
            &ambiguous,
            true,
            AfterFailure::Retry,
        ),
        (
            ReplayPolicy::Repeatable,
            &ambiguous,
            false,
            AfterFailure::Return,
        ),
    ];

    for (policy, failure, retryable, expected) in cases {
        assert_eq!(
            policy.classify_failure(failure, retryable),
            expected,
            "{policy:?} retryable={retryable}"
        );
    }
}

#[tokio::test]
async fn given_put_item_when_connection_closes_after_send_then_commit_unknown_after_one_send() {
    let (url, connections) = closing_endpoint(b"replay-table").await;
    let provider = provider_with(vec![url], None).await;

    let result = tokio::time::timeout(
        TEST_CALL_BOUND,
        provider.put_item(put_request("replay-table")),
    )
    .await
    .expect("put_item finishes within the bound");

    match result {
        Ok(_) => panic!("a closed connection must not succeed"),
        Err(error) => assert_commit_unknown(&error),
    }
    assert_eq!(connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn given_put_item_when_response_exceeds_request_timeout_then_commit_unknown_after_one_send() {
    let server = MockServer::start_async().await;
    let put = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.PutItem");
            then.status(200).body("{}").delay(Duration::from_secs(2));
        })
        .await;
    let provider = provider_with(vec![server.url("/")], Some(200)).await;

    let result = tokio::time::timeout(
        TEST_CALL_BOUND,
        provider.put_item(put_request("replay-table")),
    )
    .await
    .expect("put_item finishes within the bound");

    match result {
        Ok(_) => panic!("a timed-out write must not succeed"),
        Err(error) => assert_commit_unknown(&error),
    }
    put.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_tokenized_transact_write_when_server_answers_500_then_commit_unknown_after_one_send()
{
    let server = MockServer::start_async().await;
    let transact = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
            then.status(500).body(internal_error_body());
        })
        .await;
    let provider = provider_with(vec![server.url("/")], None).await;

    let result = provider
        .transact_write_items(storage_types::TransactWriteItemsRequest {
            client_request_token: Some("replay-token".into()),
            ..storage_types::TransactWriteItemsRequest::default()
        })
        .await;

    match result {
        Ok(_) => panic!("a 500 answer must not succeed"),
        Err(error) => assert_commit_unknown(&error),
    }
    transact.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_put_item_when_first_endpoint_refuses_connect_then_second_endpoint_gets_one_send() {
    let server = MockServer::start_async().await;
    let put = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.PutItem");
            then.status(200).body("{}");
        })
        .await;
    let provider = provider_with(vec![refused_endpoint(), server.url("/")], None).await;

    tokio::time::timeout(
        TEST_CALL_BOUND,
        provider.put_item(put_request("replay-table")),
    )
    .await
    .expect("put_item finishes within the bound")
    .map(|_| ())
    .expect("the live endpoint answers after the refused one");

    put.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_put_item_when_server_answers_typed_503_then_it_is_retried() {
    let server = MockServer::start_async().await;
    let unavailable = serde_json::json!({
        "__type": "com.amazonaws.dynamodb.v20120810#ServiceUnavailable"
    })
    .to_string();
    let put = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.PutItem");
            then.respond_with(first_then((503, unavailable), (200, "{}".to_string())));
        })
        .await;
    let provider = provider_with(vec![server.url("/")], None).await;

    provider
        .put_item(put_request("replay-table"))
        .await
        .map(|_| ())
        .expect("a typed 503 is retried and the second send succeeds");

    put.assert_calls_async(2).await;
}

#[tokio::test]
async fn given_get_item_when_server_answers_500_then_the_read_is_replayed() {
    let server = MockServer::start_async().await;
    let found = serde_json::json!({"Item": {"pk": {"S": "T#1"}, "sk": {"S": "U#1"}}}).to_string();
    let get = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem");
            then.respond_with(first_then((500, internal_error_body()), (200, found)));
        })
        .await;
    let provider = provider_with(vec![server.url("/")], None).await;
    let key: storage_types::KeyAttributes = [
        (
            "pk".to_string(),
            storage_types::AttributeValue::S("T#1".to_string()),
        ),
        (
            "sk".to_string(),
            storage_types::AttributeValue::S("U#1".to_string()),
        ),
    ]
    .into_iter()
    .collect();

    let response = provider
        .get_item(storage_types::GetItemRequest::new(
            storage_types::TableName::new("replay-table"),
            key,
        ))
        .await
        .expect("the replayed read succeeds");

    assert!(response.item.is_some());
    get.assert_calls_async(2).await;
}

/// Sends one PutItem to a server that answers `status` with `body`; returns the
/// result and the number of sends the server saw.
async fn put_item_answered_with(
    status: u16,
    body: serde_json::Value,
) -> (Result<(), StorageError>, usize) {
    let server = MockServer::start_async().await;
    let put = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.PutItem");
            then.status(status).body(body.to_string());
        })
        .await;
    let provider = provider_with(vec![server.url("/")], None).await;

    let result = tokio::time::timeout(
        TEST_CALL_BOUND,
        provider.put_item(put_request("replay-table")),
    )
    .await
    .expect("put_item finishes within the bound")
    .map(|_| ());
    (result, put.calls_async().await)
}

#[tokio::test]
async fn given_put_item_when_server_answers_undecodable_transaction_cancel_then_it_is_not_commit_unknown()
 {
    let (result, sends) = put_item_answered_with(
        400,
        serde_json::json!({
            "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
            "message": "cancelled",
            "CancellationReasons": [{"Code": "NotACancellationCode"}]
        }),
    )
    .await;

    let error = result.expect_err("a cancelled write fails");
    assert_ne!(error.class(), ErrorClass::CommitUnknown);
    assert_eq!(sends, 1);
}

#[tokio::test]
async fn given_put_item_when_server_answers_missing_authentication_token_then_it_is_not_commit_unknown()
 {
    let (result, sends) = put_item_answered_with(
        400,
        serde_json::json!({
            "__type": "com.amazonaws.dynamodb.v20120810#MissingAuthenticationToken",
            "message": "Request is missing Authentication Token"
        }),
    )
    .await;

    let error = result.expect_err("an unauthenticated write fails");
    assert_ne!(error.class(), ErrorClass::CommitUnknown);
    assert_eq!(sends, 1);
}

#[tokio::test]
async fn given_put_item_when_first_endpoint_answers_500_then_second_endpoint_gets_no_send() {
    let first = MockServer::start_async().await;
    let second = MockServer::start_async().await;
    let first_put = first
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.PutItem");
            then.status(500).body(internal_error_body());
        })
        .await;
    let second_put = second
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.PutItem");
            then.status(200).body("{}");
        })
        .await;
    let provider = provider_with(vec![first.url("/"), second.url("/")], None).await;

    let result = tokio::time::timeout(
        TEST_CALL_BOUND,
        provider.put_item(put_request("replay-table")),
    )
    .await
    .expect("put_item finishes within the bound");

    match result {
        Ok(_) => panic!("a 500 answer must not succeed"),
        Err(error) => assert_commit_unknown(&error),
    }
    first_put.assert_calls_async(1).await;
    second_put.assert_calls_async(0).await;
}
