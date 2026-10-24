//! PutItem retention scheduling through the replacement operation and real
//! worker.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_provider::{StreamDurationTrimConfig, StreamDurationTrimWorker, StreamTrimScopeKind};
use storage_types::{CreateTableRequest, GetItemRequest, PutItemRequest};

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

// Business rule from legacy KV PutItem: a committed custom duration schedules
// retention work.
#[tokio::test]
async fn given_custom_item_stream_duration_when_put_commits_then_retention_worker_finds_due_work() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "AuxStreamDurationHours": 1,
            "AuxDefaultItemStreamDurationHours": 2
        }))
        .expect("valid table retention request");
        store
            .operations
            .create_table(create)
            .await
            .expect("create table with stream retention");
        let item = json!({"pk": {"S": "one"}, "value": {"S": "committed"}});
        let request = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": item,
            "AuxItemStreamTtlHours": 1
        }))
        .expect("valid custom-duration PutItem");

        let written = observe(store.operations.put_item(request)).await;
        written.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(written.result.expect("commit PutItem")).unwrap(),
            json!({})
        );

        let now = store.advance_time(Duration::from_secs(3_601));
        let scheduled = observe(
            store
                .stream_retention_repository
                .list_due_stream_trim_markers(now, 10),
        )
        .await;
        scheduled.assert_no_warning_or_error();
        let scheduled = scheduled.result.expect("read due retention work");
        assert_eq!(
            scheduled
                .iter()
                .filter(|marker| marker.scope.kind == StreamTrimScopeKind::Item
                    && marker.scope.table_name == table)
                .count(),
            1
        );
        let worker = StreamDurationTrimWorker::new(
            store.stream_retention_repository.as_ref(),
            StreamDurationTrimConfig {
                marker_page_size: storage_provider::StreamTrimPageLimit::try_from(2usize).unwrap(),
                stream_page_size: storage_provider::StreamTrimPageLimit::try_from(25usize).unwrap(),
            },
        );
        let trimmed = observe(worker.run_due_page(now, now)).await;
        trimmed.assert_no_warning_or_error();
        assert_eq!(
            trimmed
                .result
                .expect("run due retention work")
                .due_markers_scanned,
            2
        );

        let replayed = observe(worker.run_due_page(now, now)).await;
        replayed.assert_no_warning_or_error();
        assert!(
            replayed
                .result
                .expect("replay bounded trim work")
                .due_markers_scanned
                <= 2
        );
        let remaining = store
            .stream_retention_repository
            .list_due_stream_trim_markers(now, 10)
            .await
            .expect("read remaining due work");
        assert!(
            remaining.iter().all(|marker| scheduled.contains(marker)),
            "completed trim work must not create or resurrect due markers"
        );

        let read = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Key": {"pk": {"S": "one"}}
        }))
        .expect("valid GetItem");
        let found = observe(store.operations.get_item(read)).await;
        found.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(found.result.expect("read committed item")).unwrap(),
            json!({"Item": item})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Negative rule from legacy KV PutItem: a failed condition must not schedule
// custom retention.
#[tokio::test]
async fn given_failed_conditional_put_with_custom_duration_when_worker_runs_then_no_retention_work_is_due()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "AuxStreamDurationHours": 1,
            "AuxDefaultItemStreamDurationHours": 2
        })).expect("valid table retention request");
        store.operations.create_table(create).await.expect("create table with stream retention");
        let original = json!({"pk": {"S": "one"}, "value": {"S": "original"}});
        let seed = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Item": original
        })).expect("valid seed PutItem");
        store.operations.put_item(seed).await.expect("commit seed item");

        let rejected = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "one"}, "value": {"S": "replacement"}},
            "ConditionExpression": "attribute_not_exists(pk)",
            "AuxItemStreamTtlHours": 1
        })).expect("valid conditional PutItem");
        let failure = observe(store.operations.put_item(rejected)).await;
        failure.assert_no_warning_or_error();
        // ../dynamo-conformance: put_item/field-condition_expression/400/attribute_not_exists_failure.json
        assert_dynamo_error_response_exact(
            failure.result.expect_err("existing key rejects conditional PutItem"),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
        );

        let now = store.advance_time(Duration::from_secs(3_601));
        let scheduled = observe(store.stream_retention_repository
            .list_due_stream_trim_markers(now, 10)).await;
        scheduled.assert_no_warning_or_error();
        let scheduled = scheduled.result.expect("read due retention work");
        assert_eq!(scheduled.iter().filter(|marker| marker.scope.kind == StreamTrimScopeKind::Item
            && marker.scope.table_name == table).count(), 0);
        let worker = StreamDurationTrimWorker::new(
            store.stream_retention_repository.as_ref(),
            StreamDurationTrimConfig::default(),
        );
        let trimmed = observe(worker.run_due_page(now, now)).await;
        trimmed.assert_no_warning_or_error();
        assert_eq!(trimmed.result.expect("run due retention work").due_markers_scanned, 1);

        let read = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Key": {"pk": {"S": "one"}}
        })).expect("valid GetItem");
        let found = observe(store.operations.get_item(read)).await;
        found.assert_no_warning_or_error();
        assert_eq!(serde_json::to_value(found.result.expect("read original item")).unwrap(),
            json!({"Item": original}));
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
