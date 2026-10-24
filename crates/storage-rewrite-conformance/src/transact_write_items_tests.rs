//! Transaction token behavior retained from the legacy provider suites.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{
    CreateTableRequest, GetItemRequest, KeyAttributes, PutItemRequest, StreamName, TableName,
    TransactWriteItemsRequest,
};
use stream_provider::StreamRowKind;
use tracing::Level;

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

// Negative and business rules from legacy SQL and Cucumber: a middle
// ConditionCheck failure rolls back both surrounding writes and their CDC.
#[tokio::test]
async fn given_failing_middle_check_when_transaction_runs_then_items_and_cdc_remain_unchanged() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        })).unwrap();
        store.operations.create_table(create).await.unwrap();

        for pk in ["checked", "deleted"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": pk}, "value": {"S": "old"}}
            })).unwrap();
            store.operations.put_item(put).await.unwrap();
        }
        let system_stream = StreamRowKind::System(StreamName::system_table_stream());
        let before = store.stream_operations.read_forward(system_stream.clone(), None, 10)
            .await.unwrap();
        assert_eq!(before.items.len(), 2);

        let request = TransactWriteItemsRequest::try_from(json!({
            "TransactItems": [
                {"Put": {
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "created"}, "value": {"S": "new"}}
                }},
                {"ConditionCheck": {
                    "TableName": table.as_ref(),
                    "Key": {"pk": {"S": "checked"}},
                    "ConditionExpression": "attribute_not_exists(pk)"
                }},
                {"Delete": {
                    "TableName": table.as_ref(),
                    "Key": {"pk": {"S": "deleted"}}
                }}
            ]
        })).unwrap();
        let rejected = observe(store.operations.transact_write_items(request)).await;
        rejected.assert_no_warning_or_error();
        let error = rejected.result.unwrap_err();
        let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
            http_error::HttpApiError::from(error).into();
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(serde_json::to_value(body).unwrap(), json!({
            "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
            "CancellationReasons": [
                {"Code": "None"},
                {"Code": "ConditionalCheckFailed", "Message": "The conditional request failed"},
                {"Code": "None"}
            ],
            "Message": "Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed, None]"
        }));

        for (pk, expected) in [
            ("created", json!({})),
            ("checked", json!({"Item": {"pk": {"S": "checked"}, "value": {"S": "old"}}})),
            ("deleted", json!({"Item": {"pk": {"S": "deleted"}, "value": {"S": "old"}}})),
        ] {
            let get = GetItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": pk}}
            })).unwrap();
            let actual = store.operations.get_item(get).await.unwrap();
            assert_eq!(serde_json::to_value(actual).unwrap(), expected);
        }
        let after = store.stream_operations.read_forward(system_stream, None, 10)
            .await.unwrap();
        assert_eq!(after.items.len(), before.items.len());
        assert_eq!(after.items.iter().map(|row| &row.id).collect::<Vec<_>>(),
            before.items.iter().map(|row| &row.id).collect::<Vec<_>>());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

fn put_request(table: &TableName, token: &str, key: &str) -> TransactWriteItemsRequest {
    TransactWriteItemsRequest::try_from(json!({
        "ClientRequestToken": token,
        "TransactItems": [{"Put": {
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": key}},
            "ConditionExpression": "attribute_not_exists(pk)"
        }}]
    }))
    .unwrap()
}

async fn read_item(store: &TestStore, table: TableName, key: &str) -> Value {
    let key: KeyAttributes = serde_json::from_value(json!({"pk": {"S": key}})).unwrap();
    let response = store
        .operations
        .get_item(GetItemRequest::new(table, key))
        .await
        .unwrap();
    serde_json::to_value(response).unwrap()["Item"].clone()
}

// Business rule: a token replay returns the original success without rerunning
// its condition.
#[tokio::test]
async fn given_committed_token_when_same_transaction_replays_then_original_success_and_item_remain()
{
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let request = put_request(&table, "replay-token", "first");

        let first = observe(store.operations.transact_write_items(request.clone())).await;
        first.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(first.result.unwrap()).unwrap(),
            json!({})
        );

        let replay = observe(store.operations.transact_write_items(request)).await;
        replay.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(replay.result.unwrap()).unwrap(),
            json!({})
        );
        assert_eq!(
            read_item(&store, table, "first").await,
            json!({"pk": {"S": "first"}})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business rule from the legacy KV provider: concurrent uses of one token
// acknowledge one conditional write, and the replay cannot run its condition.
#[tokio::test]
async fn given_concurrent_matching_tokens_when_transactions_run_then_one_conditional_write_commits()
{
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let request = put_request(&table, "concurrent-token", "private-item-id");
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
        let mut requests = Vec::new();
        for _ in 0..2 {
            let operations = std::sync::Arc::clone(&store.operations);
            let request = request.clone();
            let barrier = std::sync::Arc::clone(&barrier);
            requests.push(tokio::spawn(async move {
                barrier.wait().await;
                observe(operations.transact_write_items(request)).await
            }));
        }
        barrier.wait().await;

        for request in requests {
            let observed = request.await.unwrap();
            observed.assert_no_warning_or_error();
            let span =
                observed.one_span_with("dynamo_operation", &[("operation", "TransactWriteItems")]);
            assert_eq!(span.fields.len(), 1);
            assert_eq!(
                serde_json::to_value(observed.result.unwrap()).unwrap(),
                json!({})
            );
            assert!(
                !format!("{:?} {:?}", observed.events, observed.spans).contains("private-item-id")
            );
        }
        assert_eq!(
            read_item(&store, table, "private-item-id").await,
            json!({"pk": {"S": "private-item-id"}})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Negative rule: changing request parameters under a committed token rejects
// the second write.
#[tokio::test]
async fn given_committed_token_when_transaction_changes_then_mismatch_response_and_state_remain() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let first = put_request(&table, "mismatch-token", "first");
        store.operations
            .transact_write_items(first)
            .await
            .unwrap();

        let changed = put_request(&table, "mismatch-token", "second");
        let observed = observe(store.operations.transact_write_items(changed)).await;
        observed.assert_no_warning_or_error();
        observed.one_event_with(
            Level::INFO,
            &[
                ("operation", "TransactWriteItems"),
                ("error_code", "IdempotentParameterMismatchException"),
                ("reason", "token_reused_with_different_request"),
            ],
        );
        let error = observed.result.unwrap_err();
        let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
            http_error::HttpApiError::from(error).into();
        assert_eq!(status.as_u16(), 400);
        assert_eq!(
            serde_json::to_value(body).unwrap(),
            json!({
                "__type": "com.amazonaws.dynamodb.v20120810#IdempotentParameterMismatchException",
                "message": "The request was rejected because the same client request token was used with different parameters."
            })
        );
        assert_eq!(
            read_item(&store, table.clone(), "first").await,
            json!({"pk": {"S": "first"}})
        );
        assert!(
            read_item(&store, table, "second").await.is_null(),
            "mismatched request must not write"
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary and negative rules from legacy Turso token tests: a completed
// receipt rejects changed parameters until the ten-minute window expires.
#[tokio::test]
async fn given_completed_token_when_window_expires_then_changed_transaction_can_commit() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let original = put_request(&table, "expiry-token", "first");
        let first = observe(store.operations.transact_write_items(original)).await;
        first.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(first.result.unwrap())
                .unwrap(),
            json!({})
        );

        let changed = put_request(&table, "expiry-token", "second");
        store.advance_time(Duration::from_secs(599));
        let before_expiry = observe(store.operations.transact_write_items(changed.clone())).await;
        before_expiry.assert_no_warning_or_error();
        before_expiry.one_event_with(
            Level::INFO,
            &[
                ("operation", "TransactWriteItems"),
                ("error_code", "IdempotentParameterMismatchException"),
                ("reason", "token_reused_with_different_request"),
            ],
        );
        assert_dynamo_error_response_exact(
            before_expiry.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#IdempotentParameterMismatchException","message":"The request was rejected because the same client request token was used with different parameters."}"#,
        );
        assert!(read_item(&store, table.clone(), "second").await.is_null());

        store.advance_time(Duration::from_secs(1));
        let after_expiry = observe(store.operations.transact_write_items(changed)).await;
        after_expiry.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(after_expiry.result.unwrap())
                .unwrap(),
            json!({})
        );
        assert_eq!(
            read_item(&store, table.clone(), "first").await,
            json!({"pk": {"S": "first"}})
        );
        assert_eq!(
            read_item(&store, table, "second").await,
            json!({"pk": {"S": "second"}})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Negative and business rules from legacy Turso token tests: a rejected
// transaction has neither partial writes nor a completed token receipt.
#[tokio::test]
async fn given_failed_tokenized_transaction_when_retried_then_token_and_items_can_be_reused() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let failing = TransactWriteItemsRequest::try_from(json!({
            "ClientRequestToken": "failed-token",
            "TransactItems": [
                {"Put": {
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "first"}},
                    "ConditionExpression": "attribute_not_exists(pk)"
                }},
                {"Put": {
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "second"}},
                    "ConditionExpression": "attribute_exists(pk)"
                }}
            ]
        }))
        .unwrap();
        let rejected = observe(store.operations.transact_write_items(failing)).await;
        rejected.assert_no_warning_or_error();
        assert_dynamo_error_response_exact(
            rejected.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","CancellationReasons":[{"Code":"None"},{"Code":"ConditionalCheckFailed","Message":"The conditional request failed"}],"Message":"Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed]"}"#,
        );
        assert!(read_item(&store, table.clone(), "first").await.is_null());
        assert!(read_item(&store, table.clone(), "second").await.is_null());

        let retry = put_request(&table, "failed-token", "first");
        let observed = observe(store.operations.transact_write_items(retry)).await;
        observed.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(observed.result.unwrap())
                .unwrap(),
            json!({})
        );
        assert_eq!(
            read_item(&store, table.clone(), "first").await,
            json!({"pk": {"S": "first"}})
        );
        assert!(read_item(&store, table, "second").await.is_null());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
