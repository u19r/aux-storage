//! Transactional stream retention through the replacement store and worker.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_provider::{StreamDurationTrimConfig, StreamDurationTrimWorker, StreamTrimScopeKind};
use storage_types::{CreateTableRequest, GetItemRequest, TransactWriteItemsRequest};

use super::test_support::{TestStore, unique_table_name};

// Business rule from custom_stream_duration.feature: transaction member TTLs
// are metadata, and only a finite item duration schedules item trim work.
#[tokio::test]
async fn given_transaction_members_with_finite_and_forever_stream_ttls_when_committed_then_only_finite_item_trim_is_due()
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
        }))
        .expect("valid table retention request");
        store
            .operations
            .create_table(create)
            .await
            .expect("create retention table");

        let request = TransactWriteItemsRequest::try_from(json!({
            "TransactItems": [
                {"Put": {
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "finite"}, "value": {"S": "created"}},
                    "AuxItemStreamTtlHours": 1
                }},
                {"Update": {
                    "TableName": table.as_ref(),
                    "Key": {"pk": {"S": "forever"}},
                    "UpdateExpression": "SET #value = :value",
                    "ExpressionAttributeNames": {"#value": "value"},
                    "ExpressionAttributeValues": {":value": {"S": "updated"}},
                    "AuxItemStreamTtlHours": -1
                }}
            ]
        }))
        .expect("valid custom-duration transaction");
        let committed = observe(store.operations.transact_write_items(request)).await;
        committed.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(committed.result.expect("commit transaction"))
                .expect("serialize transaction response"),
            json!({})
        );

        for (key, value) in [("finite", "created"), ("forever", "updated")] {
            let get = GetItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": key}}
            }))
            .expect("valid GetItem");
            let found = observe(store.operations.get_item(get)).await;
            found.assert_no_warning_or_error();
            assert_eq!(
                serde_json::to_value(found.result.expect("read transaction item"))
                    .expect("serialize GetItem response"),
                json!({"Item": {"pk": {"S": key}, "value": {"S": value}}})
            );
        }

        let due_at = store.advance_time(Duration::from_secs(3_601));
        let due = observe(
            store
                .stream_retention_repository
                .list_due_stream_trim_markers(due_at, 10),
        )
        .await;
        due.assert_no_warning_or_error();
        let due = due.result.expect("list due retention work");
        assert_eq!(
            due.iter()
                .filter(|marker| marker.scope.kind == StreamTrimScopeKind::Item
                    && marker.scope.table_name == table)
                .count(),
            1
        );

        let worker = StreamDurationTrimWorker::new(
            store.stream_retention_repository.as_ref(),
            StreamDurationTrimConfig::default(),
        );
        let trimmed = observe(worker.run_due_page(due_at, due_at)).await;
        trimmed.assert_no_warning_or_error();
        assert_eq!(
            trimmed
                .result
                .expect("run due retention work")
                .due_markers_scanned,
            2
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
