use std::{
    net::TcpListener,
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use http_request::{HttpRequestDispatchStatus, HttpRequestError, HttpRequestErrorKind};
use httpmock::{Mock, prelude::*};
use scheduled_bg_jobs::store::{
    Check, CommitOperationId, Mutation, StoreKey, StoreTransaction, StoreTransactionInput,
    TransactionLimits,
};
use storage_provider::{RawOrderedJobStoreError, RemoteCredentialStrategy, RemoteStorageSettings};
use storage_types::{StorageError, context::WrappedError as _};

use super::{
    commit::{SendTransactionError, classify_send_transaction_error, run_attempt},
    encoding,
};
use crate::provider::implementation::RemoteInvocationError;

fn absent_transaction() -> StoreTransaction {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id: CommitOperationId::from([9; 16]),
        checks: vec![Check::Absent(
            StoreKey::try_from(b"conflict".as_slice()).expect("key"),
        )],
        mutations: vec![Mutation::Put(
            StoreKey::try_from(b"conflict".as_slice()).expect("key"),
            Bytes::from_static(b"new"),
        )],
        limits: TransactionLimits {
            max_checks: std::num::NonZeroU32::new(4).expect("non-zero check limit"),
            max_mutations: std::num::NonZeroU32::new(4).expect("non-zero mutation limit"),
            max_bytes: std::num::NonZeroUsize::new(1024).expect("non-zero transaction limit"),
        },
    })
    .expect("valid transaction")
}

async fn provider_for(
    server: &MockServer,
) -> Arc<crate::provider::implementation::RemoteStorageProvider> {
    Arc::new(
        crate::provider::implementation::RemoteStorageProvider::new(RemoteStorageSettings {
            endpoint_urls: vec![server.url("/")],
            region: None,
            tls: false,
            credentials: RemoteCredentialStrategy::DefaultChain,
            timeouts: None,
        })
        .await
        .expect("remote provider"),
    )
}

fn unconditional_transaction() -> StoreTransaction {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id: CommitOperationId::from([9; 16]),
        checks: Vec::new(),
        mutations: vec![Mutation::Put(
            StoreKey::try_from(b"ambiguous-dispatch".as_slice()).expect("key"),
            Bytes::from_static(b"new"),
        )],
        limits: TransactionLimits {
            max_checks: std::num::NonZeroU32::new(4).expect("non-zero check limit"),
            max_mutations: std::num::NonZeroU32::new(4).expect("non-zero mutation limit"),
            max_bytes: std::num::NonZeroUsize::new(1024).expect("non-zero transaction limit"),
        },
    })
    .expect("valid transaction")
}

fn receipt_key() -> String {
    format!("R#{}", "09".repeat(16))
}

fn data_key() -> String {
    "K#636f6e666c696374".to_string()
}

fn existing_data_item() -> serde_json::Value {
    serde_json::json!({
        "pk": {"S": "SCHEDULED_JOB_STORE#1"},
        "sk": {"S": data_key()},
        "v": {"B": STANDARD.encode(b"existing")},
    })
}

fn operation_token() -> String {
    format!("{}-f", "09".repeat(16))
}

fn committed_operation_token() -> String {
    format!("{}-c", "09".repeat(16))
}

async fn delayed_attempt_mocks(
    server: &MockServer,
    check_delay: Duration,
    transaction_delay: Duration,
) -> (Mock<'_>, Mock<'_>, Mock<'_>) {
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(receipt_key())
                .body_includes("\"ConsistentRead\":true");
            then.status(200).body("{}").delay(Duration::from_millis(20));
        })
        .await;
    let check = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .body_includes(data_key())
                .body_includes("\"ConsistentRead\":true");
            then.status(200)
                .json_body(serde_json::json!({
                    "Responses": {"job": [existing_data_item()]}
                }))
                .delay(check_delay);
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(operation_token());
            then.status(200).body("{}").delay(transaction_delay);
        })
        .await;
    (receipt, check, transaction)
}

#[test]
fn given_connect_setup_timeout_when_dispatch_status_is_read_then_commit_stays_backend() {
    let dispatch_status = HttpRequestError::Transport {
        kind: HttpRequestErrorKind::Connect,
    }
    .dispatch_status();
    assert_eq!(dispatch_status, HttpRequestDispatchStatus::NotDispatched);

    let failure = RemoteInvocationError::new(
        StorageError::internal("connect/setup timeout"),
        dispatch_status,
    );

    assert!(matches!(
        classify_send_transaction_error(failure),
        SendTransactionError::Backend(_)
    ));
}

#[test]
fn given_response_timeout_or_loss_when_dispatch_status_is_read_then_commit_reconciles() {
    for http_error in [
        HttpRequestError::Transport {
            kind: HttpRequestErrorKind::Timeout,
        },
        HttpRequestError::ResponseBody,
    ] {
        assert_eq!(
            http_error.dispatch_status(),
            HttpRequestDispatchStatus::PossiblyDispatched
        );
        let failure = RemoteInvocationError::new(
            StorageError::internal("response timeout or loss"),
            http_error.dispatch_status(),
        );

        assert!(matches!(
            classify_send_transaction_error(failure),
            SendTransactionError::Reconcile
        ));
    }
}

#[test]
fn given_response_timeout_when_storage_validation_is_definitive_then_commit_stays_backend() {
    let failure = RemoteInvocationError::new(
        StorageError::validation("invalid transaction"),
        HttpRequestDispatchStatus::PossiblyDispatched,
    );

    assert!(matches!(
        classify_send_transaction_error(failure),
        SendTransactionError::Backend(_)
    ));
}

#[test]
fn given_idempotent_parameter_mismatch_when_dispatch_may_have_started_then_commit_stays_typed() {
    let failure = RemoteInvocationError::new(
        StorageError::idempotent_parameter_mismatch(),
        HttpRequestDispatchStatus::PossiblyDispatched,
    );

    assert!(matches!(
        classify_send_transaction_error(failure),
        SendTransactionError::Backend(error)
            if matches!(
                error.to_enum(),
                storage_types::StorageEnum::IdempotentParameterMismatch
            )
    ));
}

#[tokio::test]
async fn given_a_stalled_check_read_when_deadline_expires_then_no_transaction_is_sent() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let (receipt, check, transaction) =
        delayed_attempt_mocks(&server, Duration::from_millis(500), Duration::ZERO).await;
    let transaction_input = absent_transaction();
    let fingerprint = encoding::transaction_fingerprint(&transaction_input);
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        run_attempt(
            provider,
            &transaction_input,
            &fingerprint,
            Instant::now() + Duration::from_millis(100),
        ),
    )
    .await
    .expect("stalled pre-dispatch read must be bounded");

    assert!(matches!(result, Err(RawOrderedJobStoreError::Timeout)));
    receipt.assert_calls_async(1).await;
    check.assert_calls_async(1).await;
    assert_eq!(transaction.calls_async().await, 0);
}

#[tokio::test]
async fn given_slow_pre_dispatch_reads_when_send_outlives_deadline_then_outcome_is_unknown_without_retry()
 {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let (receipt, check, transaction) = delayed_attempt_mocks(
        &server,
        Duration::from_millis(20),
        Duration::from_millis(500),
    )
    .await;
    let transaction_input = absent_transaction();
    let fingerprint = encoding::transaction_fingerprint(&transaction_input);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        run_attempt(
            provider,
            &transaction_input,
            &fingerprint,
            Instant::now() + Duration::from_millis(100),
        ),
    )
    .await
    .expect("send deadline must be bounded")
    .expect("possibly dispatched send returns an outcome");

    assert!(matches!(
        result,
        crate::provider::scheduled_job_store::commit::AttemptDecision::Outcome(
            scheduled_bg_jobs::store::CommitOutcome::Unknown,
        )
    ));
    receipt.assert_calls_async(1).await;
    check.assert_calls_async(1).await;
    assert_eq!(transaction.calls_async().await, 1);
}

#[tokio::test]
async fn given_redirect_then_unreachable_target_when_committing_then_outcome_is_unknown() {
    let server = MockServer::start_async().await;
    let receipt_redirect = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(receipt_key())
                .body_includes("\"ConsistentRead\":true");
            then.status(307).header("location", "/receipt");
        })
        .await;
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/receipt")
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(receipt_key())
                .body_includes("\"ConsistentRead\":true");
            then.status(200).body("{}");
        })
        .await;
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve unreachable port");
    let unreachable_port = listener.local_addr().expect("unreachable address").port();
    drop(listener);
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(committed_operation_token());
            then.status(307).header(
                "location",
                format!("http://127.0.0.1:{unreachable_port}/unreachable"),
            );
        })
        .await;

    let mut provider =
        crate::provider::implementation::RemoteStorageProvider::new(RemoteStorageSettings {
            endpoint_urls: vec![server.url("/")],
            region: None,
            tls: false,
            credentials: RemoteCredentialStrategy::DefaultChain,
            timeouts: None,
        })
        .await
        .expect("remote provider");
    provider.enable_scheduled_job_http_redirects();

    let outcome = crate::provider::scheduled_job_store::commit::run(
        Arc::new(provider),
        &unconditional_transaction(),
    )
    .await
    .expect("ambiguous commit is reconciled to an outcome");

    assert_eq!(outcome, scheduled_bg_jobs::store::CommitOutcome::Unknown);
    assert!(transaction.calls_async().await > 0);
    assert!(receipt_redirect.calls_async().await >= 2);
    assert!(receipt.calls_async().await >= 2);
}
