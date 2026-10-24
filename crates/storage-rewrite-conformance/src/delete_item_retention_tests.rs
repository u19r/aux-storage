//! DeleteItem retention scheduling through the replacement operation and
//! worker.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_provider::{StreamDurationTrimConfig, StreamDurationTrimWorker, StreamTrimScopeKind};
use storage_types::{CreateTableRequest, DeleteItemRequest, GetItemRequest, PutItemRequest};

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

/// Business and negative rules from legacy KV: only a committed custom-duration
/// DeleteItem schedules item retention work.
#[tokio::test]
async fn given_custom_duration_deletes_when_one_condition_fails_then_only_committed_delete_is_due()
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
        })).unwrap();
        store.operations.create_table(create).await.unwrap();

        for pk in ["committed", "rejected"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": pk}, "state": {"S": "open"}}
            })).unwrap();
            store.operations.put_item(put).await.unwrap();
        }

        let committed = DeleteItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "committed"}},
            "AuxItemStreamTtlHours": 1
        })).unwrap();
        let accepted = observe(store.operations.delete_item(committed)).await;
        accepted.assert_no_warning_or_error();
        assert_eq!(serde_json::to_value(accepted.result.unwrap()).unwrap(), json!({}));

        let rejected = DeleteItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "rejected"}},
            "ConditionExpression": "#state = :closed",
            "ExpressionAttributeNames": {"#state": "state"},
            "ExpressionAttributeValues": {":closed": {"S": "closed"}},
            "AuxItemStreamTtlHours": 1
        })).unwrap();
        let denied = observe(store.operations.delete_item(rejected)).await;
        denied.assert_no_warning_or_error();
        assert_dynamo_error_response_exact(
            denied.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
        );

        let now = store.advance_time(Duration::from_secs(3_601));
        let due = observe(store.stream_retention_repository.list_due_stream_trim_markers(now, 10))
            .await;
        due.assert_no_warning_or_error();
        let due = due.result.unwrap();
        assert_eq!(due.iter().filter(|marker| marker.scope.kind == StreamTrimScopeKind::Item
            && marker.scope.table_name == table).count(), 1);
        let worker = StreamDurationTrimWorker::new(
            store.stream_retention_repository.as_ref(),
            StreamDurationTrimConfig::default(),
        );
        let trimmed = observe(worker.run_due_page(now, now)).await;
        trimmed.assert_no_warning_or_error();
        assert_eq!(trimmed.result.unwrap().due_markers_scanned, 2);

        for (pk, expected) in [
            ("committed", json!({})),
            ("rejected", json!({"Item": {
                "pk": {"S": "rejected"}, "state": {"S": "open"}
            }})),
        ] {
            let get = GetItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Key": {"pk": {"S": pk}}
            })).unwrap();
            let read = observe(store.operations.get_item(get)).await;
            read.assert_no_warning_or_error();
            assert_eq!(serde_json::to_value(read.result.unwrap()).unwrap(), expected);
        }
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
