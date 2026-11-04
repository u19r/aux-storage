use std::{
    net::TcpListener,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use http_request::{HttpRequestDispatchStatus, HttpRequestError, HttpRequestErrorKind};
use httpmock::prelude::*;
use scheduled_bg_jobs::store::{
    Check, CommitOperationId, Mutation, StoreKey, StoreTransaction, StoreTransactionInput,
    TransactionLimits,
};
use storage_provider::{RawOrderedJobStoreError, RemoteCredentialStrategy, RemoteStorageSettings};
use storage_types::{StorageError, context::WrappedError as _};

use super::commit::{SendTransactionError, classify_send_transaction_error, run_attempt};
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
async fn given_an_expired_dispatch_deadline_when_attempted_then_no_native_transaction_is_sent() {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
            then.status(200).body("{}");
        })
        .await;
    let input = absent_transaction();
    let result = run_attempt(provider, &input, Instant::now() - Duration::from_millis(1)).await;
    assert!(matches!(result, Err(RawOrderedJobStoreError::Timeout)));
    transaction.assert_calls_async(0).await;
}

#[tokio::test]
async fn given_a_conditional_request_when_native_dispatch_succeeds_then_only_the_checked_data_write_is_sent()
 {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes("attribute_not_exists(#pk)")
                .body_includes("K#636f6e666c696374")
                .body_excludes("ClientRequestToken")
                .body_excludes("R#");
            then.status(200).body("{}");
        })
        .await;
    let outcome = super::commit::run(provider, &absent_transaction())
        .await
        .unwrap();
    assert_eq!(outcome, scheduled_bg_jobs::store::CommitOutcome::Committed);
    transaction.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_a_definitive_native_condition_failure_when_dispatched_then_the_attempt_returns_conflict()
 {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
            then.status(400).json_body(serde_json::json!({
                "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
                "CancellationReasons": [{"Code": "ConditionalCheckFailed"}]
            }));
        })
        .await;
    let outcome = super::commit::run(provider, &absent_transaction())
        .await
        .unwrap();
    assert_eq!(outcome, scheduled_bg_jobs::store::CommitOutcome::Conflict);
    transaction.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_a_dispatched_write_when_its_response_exceeds_the_deadline_then_outcome_is_unknown_without_retry()
 {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
            then.status(200)
                .body("{}")
                .delay(Duration::from_millis(500));
        })
        .await;
    let input = absent_transaction();
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        run_attempt(
            provider,
            &input,
            Instant::now() + Duration::from_millis(100),
        ),
    )
    .await
    .expect("bounded native attempt")
    .expect("ambiguous dispatch is an outcome");
    assert!(matches!(
        outcome,
        super::commit::AttemptDecision::Outcome(scheduled_bg_jobs::store::CommitOutcome::Unknown)
    ));
    transaction.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_redirect_then_unreachable_target_when_committing_then_outcome_is_unknown() {
    let server = MockServer::start_async().await;
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve unreachable port");
    let unreachable_port = listener.local_addr().unwrap().port();
    drop(listener);
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
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
        .unwrap();
    provider.enable_scheduled_job_http_redirects();
    let outcome = super::commit::run(Arc::new(provider), &unconditional_transaction())
        .await
        .unwrap();
    assert_eq!(outcome, scheduled_bg_jobs::store::CommitOutcome::Unknown);
    transaction.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_an_ambiguous_native_failure_before_a_later_abort_when_invoked_then_transport_does_not_retry()
 {
    let server = MockServer::start_async().await;
    let provider = provider_for(&server).await;
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let recorded = Arc::clone(&requests);
    let transaction = server.mock_async(|when, then| {
        when.method(POST).header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
        then.respond_with(move |_| {
            let first = recorded.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
            let body = if first {
                serde_json::json!({"__type": "InternalServerError"})
            } else {
                serde_json::json!({"__type": "TransactionCanceledException", "CancellationReasons": [{"Code": "ConditionalCheckFailed"}]})
            };
            HttpMockResponse::builder().status(if first {500} else {400}).body(body.to_string()).build()
        });
    }).await;
    let outcome = super::commit::run(provider, &absent_transaction())
        .await
        .unwrap();
    assert_eq!(outcome, scheduled_bg_jobs::store::CommitOutcome::Unknown);
    transaction.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_a_definitive_not_leader_abort_when_invoked_then_the_native_attempt_follows_the_leader_hint()
 {
    let follower = MockServer::start_async().await;
    let leader = MockServer::start_async().await;
    let leader_endpoint = leader.url("/");
    let provider =
        crate::provider::implementation::RemoteStorageProvider::new(RemoteStorageSettings {
            endpoint_urls: vec![follower.url("/"), leader_endpoint.clone()],
            region: None,
            tls: false,
            credentials: RemoteCredentialStrategy::DefaultChain,
            timeouts: None,
        })
        .await
        .unwrap();
    let aborted = follower
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
            then.status(500)
                .header(
                    storage_types::STORAGE_LEADER_HINT_HEADER,
                    leader_endpoint.trim_end_matches('/'),
                )
                .json_body(
                    serde_json::json!({"__type": storage_types::STORAGE_NOT_LEADER_ERROR_TYPE}),
                );
        })
        .await;
    let committed = leader
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
            then.status(200).body("{}");
        })
        .await;
    assert_eq!(
        super::commit::run(Arc::new(provider), &absent_transaction())
            .await
            .unwrap(),
        scheduled_bg_jobs::store::CommitOutcome::Committed
    );
    aborted.assert_calls_async(1).await;
    committed.assert_calls_async(1).await;
}
