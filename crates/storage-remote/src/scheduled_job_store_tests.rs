use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bg_job_store::{ScheduledJobStore, ScheduledJobStoreError};
use bytes::Bytes;
use httpmock::prelude::*;
use scheduled_bg_jobs::store::{
    CommitOperationId, CommitOutcome, Mutation, RangeDirection, RangeRequest, RangeRequestInput,
    StoreKey, StoreLevel, StoreTransaction, StoreTransactionInput, TransactionLimits,
};
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
    let commit = store.transact(&input);
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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, ("job").as_bytes()).to_string(),
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
                    "TableId": uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, ("job").as_bytes()).to_string(),
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
    let alias_transaction = alias
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_includes("K#6b")
                .body_excludes("ClientRequestToken")
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
                .body_includes("K#6b")
                .body_excludes("ClientRequestToken")
                .body_excludes("R#")
                .header_missing("authorization");
            then.status(200).body("{}");
        })
        .await;

    let store = store_for(&alias).await;
    assert_eq!(
        store
            .transact(&put_transaction(b"redirected"))
            .await
            .expect("cross-host redirected commit"),
        CommitOutcome::Committed
    );
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
    assert_eq!(capabilities.max_transaction_keys().get(), 100);
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
async fn given_950_kib_payload_when_native_chunks_commit_then_only_the_requested_chunks_are_written()
 {
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
        store.transact(&transaction).await.expect("commit"),
        CommitOutcome::Committed
    );
    let request: serde_json::Value =
        serde_json::from_str(captured.lock().unwrap().as_ref().unwrap()).unwrap();
    let items = request["TransactItems"]
        .as_array()
        .expect("transaction items");
    let mut restored = Vec::new();
    for item in items {
        let row = &item["Put"]["Item"];
        assert!(row["sk"]["S"].as_str().unwrap().starts_with("K#"));
        assert!(row.get("content").is_none());
        restored.extend(STANDARD.decode(row["v"]["B"].as_str().unwrap()).unwrap());
    }
    assert_eq!(items.len(), transaction.mutations().len());
    assert!(request.get("ClientRequestToken").is_none());
    assert_eq!(restored, payload);
    receipt.assert_calls_async(0).await;
    commit.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_a_native_response_timeout_when_dispatched_then_unknown_is_preserved_without_history_reads()
 {
    let server = MockServer::start_async().await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.TransactWriteItems")
                .body_excludes("ClientRequestToken")
                .body_excludes("R#");
            then.status(200).body("{}").delay(STALLED_RESPONSE_DELAY);
        })
        .await;
    let store = store_for_with_request_timeout(&server, RESPONSE_TIMEOUT).await;
    let outcome = commit_after_observed_timeout(&store, &transaction)
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Unknown);
    transaction.assert_calls_async(1).await;
}
