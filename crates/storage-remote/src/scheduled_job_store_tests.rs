use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bg_job_store::{ScheduledJobStore, ScheduledJobStoreError};
use bytes::Bytes;
use httpmock::prelude::*;
use scheduled_bg_jobs::store::{
    Check, CommitOperationId, CommitOutcome, Mutation, RangeDirection, RangeRequest,
    RangeRequestInput, StoreKey, StoreLevel, StoreTransaction, StoreTransactionInput,
    TransactionLimits,
};
use sha2::{Digest, Sha256};
use storage_provider::{RemoteCredentialStrategy, RemoteStorageSettings, RemoteTimeoutOverrides};

const PARTITION_KEY: &str = "SCHEDULED_JOB_STORE#1";
const RESPONSE_TIMEOUT: Duration = Duration::from_millis(100);
const COMMIT_WATCHDOG: Duration = Duration::from_secs(5);
// The server must not answer before the real-time watchdog, even if the test
// process is descheduled while its client clock is paused.
const STALLED_RESPONSE_DELAY: Duration = Duration::from_secs(6);

async fn store_for(server: &MockServer) -> ScheduledJobStore {
    crate::build_scheduled_job_store(RemoteStorageSettings {
        endpoint_urls: vec![server.url("/")],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    })
    .await
    .expect("construct remote scheduled-job store")
}

async fn store_for_with_request_timeout(
    server: &MockServer,
    request_timeout: Duration,
) -> ScheduledJobStore {
    let timeout_ms = u64::try_from(request_timeout.as_millis()).expect("timeout fits in u64");
    crate::build_scheduled_job_store(RemoteStorageSettings {
        endpoint_urls: vec![server.url("/")],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: Some(RemoteTimeoutOverrides {
            connect_timeout_ms: Some(timeout_ms),
            request_timeout_ms: Some(timeout_ms),
        }),
    })
    .await
    .expect("construct remote scheduled-job store")
}

async fn commit_after_observed_timeout(
    store: &ScheduledJobStore,
    request: &httpmock::Mock<'_>,
) -> Result<CommitOutcome, ScheduledJobStoreError> {
    tokio::time::pause();
    let input = put_transaction(b"v");
    let commit = store.commit(&input);
    let observed = async {
        while request.calls_async().await == 0 {
            tokio::task::yield_now().await;
        }
    };
    tokio::pin!(commit);
    tokio::pin!(observed);
    let started = std::time::Instant::now();
    let mut has_advanced_timeout = false;
    let outcome = loop {
        assert!(
            started.elapsed() < COMMIT_WATCHDOG,
            "commit exceeded its fixture watchdog"
        );
        tokio::select! {
            result = &mut commit => break result,
            () = &mut observed, if !has_advanced_timeout => {
                tokio::time::advance(RESPONSE_TIMEOUT + Duration::from_millis(1)).await;
                has_advanced_timeout = true;
            }
            // Keep I/O progressing without Tokio automatically advancing the
            // paused clock before the mock has received the selected request.
            () = tokio::task::yield_now() => {}
        }
    };
    tokio::time::resume();
    assert!(
        has_advanced_timeout,
        "commit completed before the selected timeout: {outcome:?}"
    );
    outcome
}

fn key(value: &[u8]) -> StoreKey {
    StoreKey::try_from(value).expect("fixture key is non-empty")
}

fn data_item(hex_key: &str, encoded_value: &str) -> serde_json::Value {
    serde_json::json!({
        "pk": {"S": PARTITION_KEY},
        "sk": {"S": format!("K#{hex_key}")},
        "v": {"B": encoded_value},
    })
}

fn key_item(hex_key: &str) -> serde_json::Value {
    serde_json::json!({
        "pk": {"S": PARTITION_KEY},
        "sk": {"S": format!("K#{hex_key}")},
    })
}

fn range_request(direction: RangeDirection, continuation: Option<StoreKey>) -> RangeRequest {
    range_request_with_bounds(direction, continuation, Some(b"z"), 2)
}

fn range_request_with_bounds(
    direction: RangeDirection,
    continuation: Option<StoreKey>,
    end: Option<&[u8]>,
    item_limit: u32,
) -> RangeRequest {
    RangeRequest::try_from(RangeRequestInput {
        start: Some(key(b"a")),
        end: end.map(key),
        direction,
        item_limit: NonZeroU32::new(item_limit).expect("non-zero item limit"),
        byte_limit: NonZeroUsize::new(1024).expect("non-zero byte limit"),
        continuation,
    })
    .expect("valid range request")
}

fn put_transaction(value: &'static [u8]) -> StoreTransaction {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id: CommitOperationId::from([7; 16]),
        checks: Vec::new(),
        mutations: vec![Mutation::Put(key(b"k"), Bytes::from_static(value))],
        limits: TransactionLimits {
            max_checks: NonZeroU32::new(4).expect("non-zero check limit"),
            max_mutations: NonZeroU32::new(4).expect("non-zero mutation limit"),
            max_bytes: NonZeroUsize::new(1024).expect("non-zero transaction limit"),
        },
    })
    .expect("valid transaction")
}

fn absent_transaction() -> StoreTransaction {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id: CommitOperationId::from([9; 16]),
        checks: vec![Check::Absent(key(b"conflict"))],
        mutations: vec![Mutation::Put(key(b"conflict"), Bytes::from_static(b"new"))],
        limits: TransactionLimits {
            max_checks: NonZeroU32::new(4).expect("non-zero check limit"),
            max_mutations: NonZeroU32::new(4).expect("non-zero mutation limit"),
            max_bytes: NonZeroUsize::new(1024).expect("non-zero transaction limit"),
        },
    })
    .expect("valid transaction")
}

fn receipt_item(sort_key: &str, fingerprint: &[u8], outcome: &str) -> serde_json::Value {
    serde_json::json!({
        "pk": {"S": PARTITION_KEY},
        "sk": {"S": sort_key},
        "content": {"B": STANDARD.encode(fingerprint)},
        "outcome": {"S": outcome},
        "kind": {"S": "commit-receipt-v2"},
    })
}

fn fingerprint_put(value: &[u8]) -> Vec<u8> {
    let mut bytes = vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 1, b'k'];
    bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
    bytes.extend_from_slice(value);
    Sha256::digest(&bytes).to_vec()
}

fn operation_hex(byte: u8) -> String {
    format!("{byte:02x}").repeat(16)
}

fn receipt_sort_key(byte: u8) -> String {
    format!("R#{}", operation_hex(byte))
}

#[tokio::test]
async fn get_uses_a_strong_read_and_decodes_the_reserved_data_row() {
    let server = MockServer::start_async().await;
    let mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes("\"TableName\":\"job\"")
                .body_includes("\"ConsistentRead\":true")
                .header_missing("authorization")
                .header_missing("x-amz-date")
                .header_missing("x-amz-security-token")
                .header_missing("x-api-key");
            then.status(200).json_body(serde_json::json!({
                "Item": data_item("616c706861", "b25l")
            }));
        })
        .await;

    let store = store_for(&server).await;
    let value = store.get(key(b"alpha")).await.expect("strong get");

    assert_eq!(value, Some(Bytes::from_static(b"one")));
    mock.assert_calls_async(1).await;
}

#[tokio::test]
async fn scheduled_store_connector_rejects_tls_settings_before_network_access() {
    let result = crate::connect_scheduled_job_store(RemoteStorageSettings {
        endpoint_urls: vec!["https://private.example".to_string()],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    })
    .await;
    let error = match result {
        Ok(_) => panic!("scheduled store must remain HTTP-only"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("HTTP endpoint"));
}

#[tokio::test]
async fn scheduled_store_connector_bootstraps_and_verifies_the_reserved_job_table() {
    let server = MockServer::start_async().await;
    let describe = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .body_includes("\"TableName\":\"job\"");
            then.status(200).json_body(serde_json::json!({
                "Table": {
                    "TableName": "job",
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"},
                        {"AttributeName": "sk", "AttributeType": "S"}
                    ],
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "sk", "KeyType": "RANGE"}
                    ],
                    "TableArn": "arn:aws:dynamodb:local:table/job",
                    "ItemCount": 0,
                    "TableSizeBytes": 0,
                    "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"}
                }
            }));
        })
        .await;

    let store = crate::connect_scheduled_job_store(RemoteStorageSettings {
        endpoint_urls: vec![server.url("/")],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    })
    .await
    .expect("reserved job table is bootstrapped and verified");

    assert_eq!(store.capabilities().level(), StoreLevel::Distributed);
    describe.assert_calls_async(3).await;
}

#[tokio::test]
async fn scheduled_store_connector_rejects_an_invalid_reserved_job_schema() {
    let server = MockServer::start_async().await;
    let describe = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .body_includes("\"TableName\":\"job\"");
            then.status(200).json_body(serde_json::json!({
                "Table": {
                    "TableName": "job",
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"},
                        {"AttributeName": "sk", "AttributeType": "S"}
                    ],
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"}
                    ],
                    "TableArn": "arn:aws:dynamodb:local:table/job",
                    "ItemCount": 0,
                    "TableSizeBytes": 0,
                    "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"}
                }
            }));
        })
        .await;

    let result = crate::connect_scheduled_job_store(RemoteStorageSettings {
        endpoint_urls: vec![server.url("/")],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    })
    .await;

    assert!(
        result.is_err(),
        "invalid reserved schema must not expose a store"
    );
    describe.assert_calls_async(3).await;
}

#[tokio::test]
async fn same_operation_race_reconciles_one_durable_commit_for_both_callers() {
    let server = MockServer::start_async().await;
    let receipt_key = receipt_sort_key(7);
    let fingerprint = fingerprint_put(b"v");
    let receipt_calls = Arc::new(AtomicUsize::new(0));
    let receipt_calls_for_mock = Arc::clone(&receipt_calls);
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(&receipt_key)
                .body_includes("\"ConsistentRead\":true");
            then.respond_with(move |_request| {
                let call = receipt_calls_for_mock.fetch_add(1, Ordering::SeqCst);
                if call < 2 {
                    return HttpMockResponse::builder().status(200).body("{}").build();
                }
                HttpMockResponse::builder()
                    .status(200)
                    .body(
                        serde_json::json!({
                            "Item": receipt_item(&receipt_key, &fingerprint, "committed")
                        })
                        .to_string(),
                    )
                    .build()
            })
            .delay(Duration::from_millis(100));
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(format!("{}-c", operation_hex(7)));
            then.respond_with({
                let calls = Arc::new(AtomicUsize::new(0));
                move |_request| {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        HttpMockResponse::builder()
                            .status(200)
                            .body("{}")
                            .build()
                    } else {
                        HttpMockResponse::builder()
                            .status(400)
                            .body(
                                serde_json::json!({
                                    "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
                                    "message": "Transaction cancelled",
                                    "CancellationReasons": [{"Code": "ConditionalCheckFailed"}]
                                })
                                .to_string(),
                            )
                            .build()
                    }
                }
            });
        })
        .await;

    let store = store_for(&server).await;
    let left = put_transaction(b"v");
    let right = put_transaction(b"v");
    let joined = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(store.commit(&left), store.commit(&right))
    })
    .await
    .expect("same-operation race completed");
    assert_eq!(
        joined.0.expect("first race result"),
        CommitOutcome::Committed
    );
    assert_eq!(
        joined.1.expect("second race result"),
        CommitOutcome::Committed
    );
    receipt.assert_calls_async(3).await;
    transaction.assert_calls_async(2).await;
}

#[tokio::test]
async fn independently_constructed_clients_share_atomic_operation_receipt() {
    let server = MockServer::start_async().await;
    let receipt_key = receipt_sort_key(7);
    let left_fingerprint = fingerprint_put(b"left");
    let right_fingerprint = fingerprint_put(b"right");
    let winner = Arc::new(Mutex::new(None::<Vec<u8>>));
    let receipt_calls = Arc::new(AtomicUsize::new(0));
    let winner_for_receipt = Arc::clone(&winner);
    let receipt_calls_for_mock = Arc::clone(&receipt_calls);
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(&receipt_key)
                .body_includes("\"ConsistentRead\":true");
            then.respond_with(move |_request| {
                let call = receipt_calls_for_mock.fetch_add(1, Ordering::SeqCst);
                if call < 2 {
                    return HttpMockResponse::builder().status(200).body("{}").build();
                }
                let fingerprint = winner_for_receipt
                    .lock()
                    .expect("winner lock")
                    .clone()
                    .expect("transaction winner recorded");
                HttpMockResponse::builder()
                    .status(200)
                    .body(
                        serde_json::json!({
                            "Item": receipt_item(&receipt_key, &fingerprint, "committed")
                        })
                        .to_string(),
                    )
                    .build()
            })
            .delay(Duration::from_millis(100));
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(format!("{}-c", operation_hex(7)));
            let calls = Arc::new(AtomicUsize::new(0));
            let winner_for_transaction = Arc::clone(&winner);
            then.respond_with(move |request| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    let fingerprint = if request.body_string().contains("bGVmdA==") {
                        left_fingerprint.clone()
                    } else {
                        right_fingerprint.clone()
                    };
                    *winner_for_transaction.lock().expect("winner lock") = Some(fingerprint);
                    HttpMockResponse::builder()
                        .status(200)
                        .body("{}")
                        .build()
                } else {
                    HttpMockResponse::builder()
                        .status(400)
                        .body(
                            serde_json::json!({
                                "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
                                "message": "Transaction cancelled",
                                "CancellationReasons": [{"Code": "ConditionalCheckFailed"}]
                            })
                            .to_string(),
                        )
                        .build()
                }
            });
        })
        .await;

    let left_store = store_for(&server).await;
    let right_store = store_for(&server).await;
    let left = put_transaction(b"left");
    let right = put_transaction(b"right");
    let joined = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(left_store.commit(&left), right_store.commit(&right))
    })
    .await
    .expect("different-content race completed");
    let results = [joined.0, joined.1];
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Ok(CommitOutcome::Committed))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(ScheduledJobStoreError::OperationReuseMismatch))
            .count(),
        1
    );
    receipt.assert_calls_async(3).await;
    transaction.assert_calls_async(2).await;
}

#[tokio::test]
async fn post_dispatch_request_timeout_reconciles_the_durable_receipt() {
    let server = MockServer::start_async().await;
    let store = store_for_with_request_timeout(&server, RESPONSE_TIMEOUT).await;
    let receipt_key = receipt_sort_key(7);
    let fingerprint = fingerprint_put(b"v");
    let receipt_calls = Arc::new(AtomicUsize::new(0));
    let receipt_calls_for_mock = Arc::clone(&receipt_calls);
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(&receipt_key)
                .body_includes("\"ConsistentRead\":true");
            then.respond_with(move |_request| {
                let call = receipt_calls_for_mock.fetch_add(1, Ordering::SeqCst);
                let body = if call == 0 {
                    serde_json::json!({})
                } else {
                    serde_json::json!({
                        "Item": receipt_item(&receipt_key, &fingerprint, "committed")
                    })
                };
                HttpMockResponse::builder()
                    .status(200)
                    .body(body.to_string())
                    .build()
            });
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(format!("{}-c", operation_hex(7)));
            then.status(200).body("{}").delay(STALLED_RESPONSE_DELAY);
        })
        .await;

    let result = commit_after_observed_timeout(&store, &transaction)
        .await
        .expect("durable receipt reconciled");

    assert_eq!(result, CommitOutcome::Committed);
    receipt.assert_calls_async(2).await;
    assert!(transaction.calls_async().await >= 1);
}

#[tokio::test]
async fn post_dispatch_timeout_without_a_receipt_returns_unknown_without_retrying() {
    let server = MockServer::start_async().await;
    let store = store_for_with_request_timeout(&server, RESPONSE_TIMEOUT).await;
    let receipt_key = receipt_sort_key(7);
    let receipt_calls = Arc::new(AtomicUsize::new(0));
    let receipt_calls_for_mock = Arc::clone(&receipt_calls);
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(&receipt_key)
                .body_includes("\"ConsistentRead\":true");
            then.respond_with(move |_request| {
                receipt_calls_for_mock.fetch_add(1, Ordering::SeqCst);
                HttpMockResponse::builder().status(200).body("{}").build()
            });
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(format!("{}-c", operation_hex(7)));
            then.status(200).body("{}").delay(STALLED_RESPONSE_DELAY);
        })
        .await;

    let outcome = commit_after_observed_timeout(&store, &transaction).await;

    let transaction_calls = transaction.calls_async().await;
    assert!(
        matches!(outcome, Ok(CommitOutcome::Unknown)),
        "commit outcome: {outcome:?}; completed receipt responses: {}; dispatched transaction \
         requests: {transaction_calls}",
        receipt_calls.load(Ordering::SeqCst),
    );
    receipt.assert_calls_async(2).await;
    assert_eq!(transaction_calls, 1);
}

#[tokio::test]
async fn given_receipt_read_times_out_when_committing_then_no_transaction_is_dispatched() {
    let server = MockServer::start_async().await;
    let store = store_for_with_request_timeout(&server, RESPONSE_TIMEOUT).await;
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(receipt_sort_key(7))
                .body_includes("\"ConsistentRead\":true");
            then.status(200).body("{}").delay(STALLED_RESPONSE_DELAY);
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
            then.status(200).body("{}");
        })
        .await;

    let outcome = commit_after_observed_timeout(&store, &receipt).await;

    assert!(
        matches!(outcome, Err(ScheduledJobStoreError::Backend(_))),
        "{outcome:?}"
    );
    receipt.assert_calls_async(1).await;
    assert_eq!(transaction.calls_async().await, 0);
}

#[tokio::test]
async fn idempotent_parameter_mismatch_is_operation_reuse_mismatch() {
    let server = MockServer::start_async().await;
    let receipt_key = receipt_sort_key(7);
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(&receipt_key)
                .body_includes("\"ConsistentRead\":true");
            then.status(200)
                .body("{}")
                .delay(Duration::from_millis(100));
        })
        .await;
    let transaction_calls = Arc::new(AtomicUsize::new(0));
    let transaction_calls_for_mock = Arc::clone(&transaction_calls);
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(format!("{}-c", operation_hex(7)));
            then.respond_with(move |_request| {
                let call = transaction_calls_for_mock.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    HttpMockResponse::builder().status(200).body("{}").build()
                } else {
                    HttpMockResponse::builder()
                        .status(400)
                        .body(
                            serde_json::json!({
                                "__type": "com.amazonaws.dynamodb.v20120810#IdempotentParameterMismatchException",
                                "message": "request token has already been used with different parameters"
                            })
                            .to_string(),
                        )
                        .build()
                }
            });
        })
        .await;

    let store = store_for(&server).await;
    let left = put_transaction(b"left");
    let right = put_transaction(b"right");
    let joined = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(store.commit(&left), store.commit(&right))
    })
    .await
    .expect("idempotency race completed");
    let results = [joined.0, joined.1];
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Ok(CommitOutcome::Committed))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(ScheduledJobStoreError::OperationReuseMismatch))
            .count(),
        1
    );
    receipt.assert_calls_async(2).await;
    assert_eq!(transaction.calls_async().await, 2);
}

#[tokio::test]
async fn idempotent_parameter_mismatch_without_a_receipt_is_operation_reuse_mismatch() {
    let server = MockServer::start_async().await;
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(receipt_sort_key(7))
                .body_includes("\"ConsistentRead\":true");
            then.status(200).body("{}");
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(format!("{}-c", operation_hex(7)));
            then.status(400).json_body(serde_json::json!({
                "__type": "com.amazonaws.dynamodb.v20120810#IdempotentParameterMismatchException",
                "message": "request token has already been used with different parameters"
            }));
        })
        .await;

    let store = store_for(&server).await;
    let result = store.commit(&put_transaction(b"different")).await;

    assert_eq!(result, Err(ScheduledJobStoreError::OperationReuseMismatch));
    receipt.assert_calls_async(1).await;
    transaction.assert_calls_async(1).await;
}

#[tokio::test]
async fn cross_host_307_redirect_preserves_scheduled_commit_identity() {
    assert_cross_host_scheduled_commit_redirect(307).await;
}

#[tokio::test]
async fn cross_host_308_redirect_preserves_scheduled_commit_identity() {
    assert_cross_host_scheduled_commit_redirect(308).await;
}

async fn assert_cross_host_scheduled_commit_redirect(status: u16) {
    let alias = MockServer::start_async().await;
    let target = MockServer::start_async().await;
    let token = format!("{}-c", operation_hex(7));
    let receipt_key = receipt_sort_key(7);
    let receipt = alias
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(&receipt_key)
                .body_includes("\"ConsistentRead\":true")
                .header_missing("authorization");
            then.status(200).body("{}");
        })
        .await;
    let alias_transaction = alias
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(&token)
                .header_missing("authorization");
            then.status(status)
                .header("location", target.url("/target"));
        })
        .await;
    let target_transaction = target
        .mock_async(|when, then| {
            when.method(POST)
                .path("/target")
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(&token)
                .body_includes(&receipt_key)
                .body_includes("attribute_not_exists")
                .header_missing("authorization");
            then.status(200).body("{}");
        })
        .await;

    let store = store_for(&alias).await;
    assert_eq!(
        store
            .commit(&put_transaction(b"redirected"))
            .await
            .expect("cross-host redirected commit"),
        CommitOutcome::Committed
    );
    receipt.assert_calls_async(1).await;
    alias_transaction.assert_calls_async(1).await;
    target_transaction.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_coherent_remote_store_when_capabilities_are_read_then_level_is_distributed() {
    let server = MockServer::start_async().await;
    let capabilities = store_for(&server).await.capabilities();

    assert_eq!(capabilities.level(), StoreLevel::Distributed);
    assert_eq!(capabilities.max_key_bytes().get(), 511);
    assert_eq!(capabilities.max_value_bytes().get(), 131_072);
    assert_eq!(capabilities.max_get_many_keys().get(), 100);
    assert_eq!(capabilities.max_get_many_key_bytes().get(), 51_100);
    assert_eq!(capabilities.max_transaction_bytes().get(), 1_048_576);
    assert_eq!(capabilities.max_transaction_keys().get(), 99);
    assert_eq!(capabilities.max_range_items().get(), 100);
    assert_eq!(capabilities.max_range_bytes().get(), 262_144);
    assert_eq!(capabilities.max_transaction_duration().as_secs(), 30);
}

#[tokio::test]
async fn range_rejects_a_response_that_exceeds_the_requested_item_limit() {
    let server = MockServer::start_async().await;
    let query = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"Limit\":2");
            then.status(200).json_body(serde_json::json!({
                "Items": [
                    data_item("61", "YQ=="),
                    data_item("62", "Yg=="),
                    data_item("63", "Yw=="),
                    data_item("64", "ZA==")
                ],
                "Count": 4,
                "ScannedCount": 4
            }));
        })
        .await;

    let error = store_for(&server)
        .await
        .read_range(range_request(RangeDirection::Forward, None))
        .await
        .expect_err("an over-limit response must fail closed");
    assert_eq!(format!("{error:?}"), "Backend(Corrupt(<redacted>))");
    assert!(!error.to_string().contains("6162"));
    query.assert_calls_async(1).await;
}

#[tokio::test]
async fn get_many_retries_unprocessed_keys_and_restores_input_order_and_duplicates() {
    let server = MockServer::start_async().await;
    let first = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .body_includes("\"K#61\"")
                .body_includes("\"K#62\"")
                .body_includes("\"K#63\"")
                .body_includes("\"ConsistentRead\":true");
            then.status(200).json_body(serde_json::json!({
                "Responses": {
                    "job": [
                        data_item("61", "YQ=="),
                        data_item("62", "Yg==")
                    ]
                },
                "UnprocessedKeys": {
                    "job": {
                        "Keys": [key_item("63")],
                        "ConsistentRead": true
                    }
                }
            }));
        })
        .await;
    let second = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .body_includes("\"K#63\"")
                .body_excludes("\"K#61\"")
                .body_excludes("\"K#62\"");
            then.status(200).json_body(serde_json::json!({
                "Responses": {"job": [data_item("63", "Yw==")]}
            }));
        })
        .await;

    let store = store_for(&server).await;
    let values = store
        .get_many(&[key(b"a"), key(b"c"), key(b"a"), key(b"b"), key(b"missing")])
        .await
        .expect("ordered batch get");

    assert_eq!(
        values,
        vec![
            Some(Bytes::from_static(b"a")),
            Some(Bytes::from_static(b"c")),
            Some(Bytes::from_static(b"a")),
            Some(Bytes::from_static(b"b")),
            None,
        ]
    );
    first.assert_calls_async(1).await;
    second.assert_calls_async(1).await;
}

#[tokio::test]
async fn range_uses_exclusive_forward_continuation_and_strong_reverse_query() {
    let server = MockServer::start_async().await;
    let first = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":true")
                .body_includes("\"Limit\":2")
                .body_includes("BETWEEN")
                .body_excludes("\"ExclusiveStartKey\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("61", "YQ=="), data_item("62", "Yg==")],
                "Count": 2,
                "ScannedCount": 2,
                "LastEvaluatedKey": {
                    "pk": {"S": PARTITION_KEY},
                    "sk": {"S": "K#62"}
                }
            }));
        })
        .await;
    let second = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":true")
                .body_includes("\"ExclusiveStartKey\"")
                .body_includes("\"K#62\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("63", "Yw=="), data_item("7a", "eg==")],
                "Count": 2,
                "ScannedCount": 2
            }));
        })
        .await;

    let store = store_for(&server).await;
    let page = store
        .read_range(range_request(RangeDirection::Forward, None))
        .await
        .expect("first forward page");
    assert_eq!(page.entries.len(), 2);
    let next = page.continuation.clone().expect("forward continuation");
    let final_page = store
        .read_range(range_request(RangeDirection::Forward, Some(next)))
        .await
        .expect("second forward page");
    assert_eq!(final_page.entries.len(), 1);
    assert!(final_page.continuation.is_none());
    first.assert_calls_async(1).await;
    second.assert_calls_async(1).await;

    let reverse_server = MockServer::start_async().await;
    let reverse = reverse_server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":false")
                .body_includes("BETWEEN")
                .body_includes("\"ConsistentRead\":true");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("62", "Yg=="), data_item("61", "YQ==")],
                "Count": 2,
                "ScannedCount": 2
            }));
        })
        .await;
    let reverse_page = store_for(&reverse_server)
        .await
        .read_range(range_request(RangeDirection::Reverse, None))
        .await
        .expect("reverse page");
    assert_eq!(reverse_page.entries.len(), 2);
    assert!(reverse_page.continuation.is_none());
    reverse.assert_calls_async(1).await;
}

#[tokio::test]
async fn range_forward_end_preserves_rows_after_full_pages() {
    let server = MockServer::start_async().await;
    let first = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":true")
                .body_includes("\"Limit\":1")
                .body_includes("BETWEEN")
                .body_excludes("\"ExclusiveStartKey\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("61", "YQ==")],
                "Count": 1,
                "ScannedCount": 1,
                "LastEvaluatedKey": {
                    "pk": {"S": PARTITION_KEY},
                    "sk": {"S": "K#61"}
                }
            }));
        })
        .await;
    let second = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":true")
                .body_includes("\"Limit\":1")
                .body_includes("\"ExclusiveStartKey\"")
                .body_includes("\"K#61\"")
                .body_excludes("\"K#62\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("62", "Yg==")],
                "Count": 1,
                "ScannedCount": 1,
                "LastEvaluatedKey": {
                    "pk": {"S": PARTITION_KEY},
                    "sk": {"S": "K#62"}
                }
            }));
        })
        .await;
    let third = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":true")
                .body_includes("\"Limit\":1")
                .body_includes("\"ExclusiveStartKey\"")
                .body_includes("\"K#62\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("7a", "eg==")],
                "Count": 1,
                "ScannedCount": 1
            }));
        })
        .await;
    let store = store_for(&server).await;
    let page = store
        .read_range(range_request_with_bounds(
            RangeDirection::Forward,
            None,
            Some(b"z"),
            1,
        ))
        .await
        .expect("first forward page");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].key, key(b"a"));
    let next = page.continuation.clone().expect("next valid row");
    let middle_page = store
        .read_range(range_request_with_bounds(
            RangeDirection::Forward,
            Some(next),
            Some(b"z"),
            1,
        ))
        .await
        .expect("middle forward page");
    assert_eq!(middle_page.entries.len(), 1);
    assert_eq!(middle_page.entries[0].key, key(b"b"));
    let final_page = store
        .read_range(range_request_with_bounds(
            RangeDirection::Forward,
            middle_page.continuation.clone(),
            Some(b"z"),
            1,
        ))
        .await
        .expect("terminal forward end page");
    assert!(final_page.entries.is_empty());
    assert!(final_page.continuation.is_none());
    first.assert_calls_async(1).await;
    second.assert_calls_async(1).await;
    third.assert_calls_async(1).await;
}

#[tokio::test]
async fn range_reverse_end_excludes_the_inclusive_sentinel() {
    let server = MockServer::start_async().await;
    let first = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":false")
                .body_includes("\"Limit\":1")
                .body_includes("\"ExclusiveStartKey\"")
                .body_includes("\"K#7a\"")
                .body_excludes("\"K#64\"")
                .body_excludes("\"K#63\"")
                .body_includes("BETWEEN");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("64", "ZA==")],
                "Count": 1,
                "ScannedCount": 1,
                "LastEvaluatedKey": {
                    "pk": {"S": PARTITION_KEY},
                    "sk": {"S": "K#64"}
                }
            }));
        })
        .await;
    let second = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":false")
                .body_includes("\"Limit\":1")
                .body_includes("\"ExclusiveStartKey\"")
                .body_includes("\"K#64\"")
                .body_excludes("\"K#63\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("63", "Yw==")],
                "Count": 1,
                "ScannedCount": 1,
                "LastEvaluatedKey": {
                    "pk": {"S": PARTITION_KEY},
                    "sk": {"S": "K#63"}
                }
            }));
        })
        .await;
    let third = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":false")
                .body_includes("\"Limit\":1")
                .body_includes("\"ExclusiveStartKey\"")
                .body_includes("\"K#63\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("62", "Yg==")],
                "Count": 1,
                "ScannedCount": 1
            }));
        })
        .await;

    let store = store_for(&server).await;
    let page = store
        .read_range(range_request_with_bounds(
            RangeDirection::Reverse,
            None,
            Some(b"z"),
            1,
        ))
        .await
        .expect("first reverse page");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].key, key(b"d"));
    let next = page.continuation.clone().expect("reverse continuation");
    let middle_page = store
        .read_range(range_request_with_bounds(
            RangeDirection::Reverse,
            Some(next),
            Some(b"z"),
            1,
        ))
        .await
        .expect("middle reverse page");
    assert_eq!(middle_page.entries.len(), 1);
    assert_eq!(middle_page.entries[0].key, key(b"c"));
    let final_page = store
        .read_range(range_request_with_bounds(
            RangeDirection::Reverse,
            middle_page.continuation.clone(),
            Some(b"z"),
            1,
        ))
        .await
        .expect("final reverse page");
    assert_eq!(final_page.entries.len(), 1);
    assert_eq!(final_page.entries[0].key, key(b"b"));
    assert!(final_page.continuation.is_none());
    first.assert_calls_async(1).await;
    second.assert_calls_async(1).await;
    third.assert_calls_async(1).await;
}

#[tokio::test]
async fn range_without_end_preserves_forward_and_reverse_cursors() {
    let forward_server = MockServer::start_async().await;
    let forward_first = forward_server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":true")
                .body_includes("\"Limit\":1")
                .body_excludes("\"ExclusiveStartKey\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("61", "YQ==")],
                "Count": 1,
                "ScannedCount": 1,
                "LastEvaluatedKey": {
                    "pk": {"S": PARTITION_KEY},
                    "sk": {"S": "K#61"}
                }
            }));
        })
        .await;
    let forward_second = forward_server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":true")
                .body_includes("\"Limit\":1")
                .body_includes("\"ExclusiveStartKey\"")
                .body_includes("\"K#61\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("62", "Yg==")],
                "Count": 1,
                "ScannedCount": 1
            }));
        })
        .await;
    let forward_store = store_for(&forward_server).await;
    let forward_page = forward_store
        .read_range(range_request_with_bounds(
            RangeDirection::Forward,
            None,
            None,
            1,
        ))
        .await
        .expect("forward page without end");
    let forward_next = forward_page.continuation.clone().expect("forward cursor");
    let forward_final = forward_store
        .read_range(range_request_with_bounds(
            RangeDirection::Forward,
            Some(forward_next),
            None,
            1,
        ))
        .await
        .expect("forward final page without end");
    assert_eq!(forward_page.entries[0].key, key(b"a"));
    assert_eq!(forward_final.entries[0].key, key(b"b"));
    assert!(forward_final.continuation.is_none());
    forward_first.assert_calls_async(1).await;
    forward_second.assert_calls_async(1).await;

    let reverse_server = MockServer::start_async().await;
    let reverse_first = reverse_server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":false")
                .body_includes("\"Limit\":1")
                .body_excludes("\"ExclusiveStartKey\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("64", "ZA==")],
                "Count": 1,
                "ScannedCount": 1,
                "LastEvaluatedKey": {
                    "pk": {"S": PARTITION_KEY},
                    "sk": {"S": "K#64"}
                }
            }));
        })
        .await;
    let reverse_second = reverse_server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"ScanIndexForward\":false")
                .body_includes("\"Limit\":1")
                .body_includes("\"ExclusiveStartKey\"")
                .body_includes("\"K#64\"");
            then.status(200).json_body(serde_json::json!({
                "Items": [data_item("63", "Yw==")],
                "Count": 1,
                "ScannedCount": 1
            }));
        })
        .await;
    let reverse_store = store_for(&reverse_server).await;
    let reverse_page = reverse_store
        .read_range(range_request_with_bounds(
            RangeDirection::Reverse,
            None,
            None,
            1,
        ))
        .await
        .expect("reverse page without end");
    let reverse_next = reverse_page.continuation.clone().expect("reverse cursor");
    let reverse_final = reverse_store
        .read_range(range_request_with_bounds(
            RangeDirection::Reverse,
            Some(reverse_next),
            None,
            1,
        ))
        .await
        .expect("reverse final page without end");
    assert_eq!(reverse_page.entries[0].key, key(b"d"));
    assert_eq!(reverse_final.entries[0].key, key(b"c"));
    assert!(reverse_final.continuation.is_none());
    reverse_first.assert_calls_async(1).await;
    reverse_second.assert_calls_async(1).await;
}

#[tokio::test]
async fn commit_replays_exact_receipt_and_rejects_changed_content_without_reapplying() {
    let server = MockServer::start_async().await;
    let fingerprint = fingerprint_put(b"v");
    let receipt_key = receipt_sort_key(7);
    let receipt_calls = Arc::new(Mutex::new(0usize));
    let receipt_calls_for_mock = Arc::clone(&receipt_calls);
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(&receipt_key)
                .body_includes("\"ConsistentRead\":true");
            then.respond_with(move |_request| {
                let mut calls = receipt_calls_for_mock.lock().expect("receipt call lock");
                let body = if *calls == 0 {
                    serde_json::json!({})
                } else {
                    serde_json::json!({
                        "Item": receipt_item(&receipt_key, &fingerprint, "committed")
                    })
                };
                *calls += 1;
                HttpMockResponse::builder()
                    .status(200)
                    .body(body.to_string())
                    .build()
            });
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes(format!("{}-c", operation_hex(7)))
                .body_includes("SCHEDULED_JOB_STORE#1")
                .body_excludes("#v");
            then.status(200).json_body(serde_json::json!({}));
        })
        .await;

    let store = store_for(&server).await;
    assert_eq!(
        store.commit(&put_transaction(b"v")).await.expect("commit"),
        CommitOutcome::Committed
    );
    assert_eq!(
        store.commit(&put_transaction(b"v")).await.expect("replay"),
        CommitOutcome::Committed
    );
    let error = store
        .commit(&put_transaction(b"different"))
        .await
        .expect_err("changed content must be rejected");
    assert_eq!(format!("{error:?}"), "OperationReuseMismatch");
    receipt.assert_calls_async(3).await;
    transaction.assert_calls_async(1).await;
}

#[tokio::test]
async fn failed_check_persists_conflict_guard_and_replays_without_mutation() {
    let server = MockServer::start_async().await;
    let receipt_key = receipt_sort_key(9);
    let fingerprint = {
        let transaction = absent_transaction();
        let mut bytes = vec![2, 0, 0, 0, 1, 0];
        let key_bytes = b"conflict";
        bytes.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(key_bytes);
        bytes.extend_from_slice(&(1u32).to_be_bytes());
        bytes.push(2);
        bytes.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(key_bytes);
        bytes.extend_from_slice(&(3u32).to_be_bytes());
        bytes.extend_from_slice(b"new");
        assert_eq!(transaction.checks().len(), 1);
        Sha256::digest(&bytes).to_vec()
    };
    let receipt_calls = Arc::new(Mutex::new(0usize));
    let receipt_calls_for_mock = Arc::clone(&receipt_calls);
    let receipt_key_for_response = receipt_key.clone();
    let fingerprint_for_response = fingerprint.clone();
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes(&receipt_key)
                .body_includes("\"ConsistentRead\":true");
            then.respond_with(move |_request| {
                let mut calls = receipt_calls_for_mock.lock().expect("receipt call lock");
                let body = if *calls == 0 {
                    serde_json::json!({})
                } else {
                    serde_json::json!({
                        "Item": receipt_item(
                            &receipt_key_for_response,
                            &fingerprint_for_response,
                            "conflict"
                        )
                    })
                };
                *calls += 1;
                HttpMockResponse::builder()
                    .status(200)
                    .body(body.to_string())
                    .build()
            });
        })
        .await;
    let batch = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .body_includes("\"ConsistentRead\":true")
                .body_includes("K#636f6e666c696374");
            then.status(200).json_body(serde_json::json!({
                "Responses": {
                    "job": [data_item("636f6e666c696374", "ZXhpc3Rpbmc=")]
                }
            }));
        })
        .await;
    let conflict_write = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes("attribute_exists(#pk)")
                .body_includes(&receipt_key)
                .body_includes("\"ConditionCheck\"");
            then.status(200).json_body(serde_json::json!({}));
        })
        .await;

    let store = store_for(&server).await;
    assert_eq!(
        store
            .commit(&absent_transaction())
            .await
            .expect("durable conflict"),
        CommitOutcome::Conflict
    );
    assert_eq!(
        store
            .commit(&absent_transaction())
            .await
            .expect("conflict replay"),
        CommitOutcome::Conflict
    );
    receipt.assert_calls_async(2).await;
    batch.assert_calls_async(1).await;
    conflict_write.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_950_kib_payload_when_remote_chunks_commit_then_receipt_contains_only_a_digest() {
    let server = MockServer::start_async().await;
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem");
            then.status(200).json_body(serde_json::json!({}));
        })
        .await;
    let payload = vec![0x41; scheduled_bg_jobs::MAX_INLINE_PAYLOAD_BYTES];
    let mutations = payload
        .chunks(131_066)
        .enumerate()
        .map(|(index, chunk)| {
            Mutation::Put(
                key(format!("chunk-{index}").as_bytes()),
                Bytes::copy_from_slice(chunk),
            )
        })
        .collect();
    let transaction = StoreTransaction::try_from(StoreTransactionInput {
        operation_id: CommitOperationId::from([17; 16]),
        checks: Vec::new(),
        mutations,
        limits: TransactionLimits {
            max_checks: NonZeroU32::new(99).unwrap(),
            max_mutations: NonZeroU32::new(99).unwrap(),
            max_bytes: NonZeroUsize::new(1_048_576).unwrap(),
        },
    })
    .expect("large transaction");
    let captured = Arc::new(Mutex::new(None));
    let capture = Arc::clone(&captured);
    let commit = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
            then.respond_with(move |request| {
                *capture.lock().unwrap() = Some(request.body_string().to_owned());
                HttpMockResponse::builder().status(200).body("{}").build()
            });
        })
        .await;
    let store = store_for(&server).await;
    assert_eq!(
        store.commit(&transaction).await.expect("commit"),
        CommitOutcome::Committed
    );
    let request: serde_json::Value =
        serde_json::from_str(captured.lock().unwrap().as_ref().unwrap()).unwrap();
    let items = request["TransactItems"]
        .as_array()
        .expect("transaction items");
    let mut restored = Vec::new();
    let mut receipts = 0;
    for item in items {
        let row = &item["Put"]["Item"];
        if row["kind"]["S"] == "commit-receipt-v2" {
            receipts += 1;
            assert_eq!(
                STANDARD
                    .decode(row["content"]["B"].as_str().unwrap())
                    .unwrap()
                    .len(),
                32
            );
        } else {
            restored.extend(STANDARD.decode(row["v"]["B"].as_str().unwrap()).unwrap());
        }
    }
    assert_eq!(receipts, 1);
    assert_eq!(restored, payload);
    receipt.assert_calls_async(1).await;
    commit.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_malformed_receipt_fingerprints_when_replaying_then_no_mutation_is_dispatched() {
    for length in [0, 31, 33] {
        let server = MockServer::start_async().await;
        let row = receipt_item(&receipt_sort_key(7), &vec![0; length], "committed");
        let receipt = server
            .mock_async(|when, then| {
                when.method(POST)
                    .header("x-amz-target", "DynamoDB_20120810.GetItem");
                then.status(200).json_body(serde_json::json!({"Item": row}));
            })
            .await;
        let mutation = server
            .mock_async(|when, then| {
                when.method(POST)
                    .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
                then.status(200).body("{}");
            })
            .await;
        let store = store_for(&server).await;
        assert!(matches!(
            store.commit(&put_transaction(b"v")).await,
            Err(ScheduledJobStoreError::Backend(
                scheduled_bg_jobs::store::StoreFailure::Corrupt(_)
            ))
        ));
        receipt.assert_calls_async(1).await;
        mutation.assert_calls_async(0).await;
    }
}

#[tokio::test]
async fn given_obsolete_full_content_receipt_when_replaying_then_format_is_rejected() {
    let server = MockServer::start_async().await;
    let mut row = receipt_item(&receipt_sort_key(7), &fingerprint_put(b"v"), "committed");
    row["kind"]["S"] = serde_json::json!("commit-receipt-v1");
    let receipt = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem");
            then.status(200).json_body(serde_json::json!({"Item": row}));
        })
        .await;
    let mutation = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems");
            then.status(200).body("{}");
        })
        .await;
    let store = store_for(&server).await;
    assert!(matches!(
        store.commit(&put_transaction(b"v")).await,
        Err(ScheduledJobStoreError::Backend(
            scheduled_bg_jobs::store::StoreFailure::Corrupt(_)
        ))
    ));
    receipt.assert_calls_async(1).await;
    mutation.assert_calls_async(0).await;
}
