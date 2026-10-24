//! BatchWriteItem stream retention through the replacement operation and
//! worker.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::FutureExt as _;
use serde_json::{Map, json};
use storage_observability_test_support::observation::observe;
use storage_provider::{StreamDurationTrimConfig, StreamDurationTrimWorker, StreamTrimScopeKind};
use storage_types::{BatchWriteItemRequest, CreateTableRequest, GetItemRequest};

use super::test_support::{TestStore, unique_table_name};

// Business rule from legacy KV PutItem: each accepted batch member schedules
// its own custom item-stream duration.
#[tokio::test]
async fn given_two_batch_puts_with_distinct_stream_durations_when_time_advances_then_each_trim_is_due_and_items_remain()
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
        .unwrap();
        store.operations.create_table(create).await.unwrap();

        let mut request_items = Map::new();
        request_items.insert(
            table.to_string(),
            json!([
                {"PutRequest": {
                    "Item": {"pk": {"S": "one"}}, "AuxItemStreamTtlHours": 1
                }},
                {"PutRequest": {
                    "Item": {"pk": {"S": "two"}}, "AuxItemStreamTtlHours": 2
                }}
            ]),
        );
        let request = BatchWriteItemRequest::try_from(json!({
            "RequestItems": request_items
        }))
        .unwrap();
        let written = observe(store.operations.batch_write_item(request)).await;
        written.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(written.result.unwrap()).unwrap(),
            json!({"UnprocessedItems": {}})
        );

        let after_one_hour = store.advance_time(Duration::from_secs(3_601));
        let first_due = observe(
            store
                .stream_retention_repository
                .list_due_stream_trim_markers(after_one_hour, 10),
        )
        .await;
        first_due.assert_no_warning_or_error();
        assert_eq!(
            first_due
                .result
                .unwrap()
                .iter()
                .filter(|marker| marker.scope.table_name == table
                    && marker.scope.kind == StreamTrimScopeKind::Item)
                .count(),
            1
        );

        let after_two_hours = store.advance_time(Duration::from_secs(3_600));
        let second_due = observe(
            store
                .stream_retention_repository
                .list_due_stream_trim_markers(after_two_hours, 10),
        )
        .await;
        second_due.assert_no_warning_or_error();
        assert_eq!(
            second_due
                .result
                .unwrap()
                .iter()
                .filter(|marker| marker.scope.table_name == table
                    && marker.scope.kind == StreamTrimScopeKind::Item)
                .count(),
            2
        );

        let worker = StreamDurationTrimWorker::new(
            store.stream_retention_repository.as_ref(),
            StreamDurationTrimConfig::default(),
        );
        let trimmed = observe(worker.run_due_page(after_two_hours, after_two_hours)).await;
        trimmed.assert_no_warning_or_error();
        assert_eq!(trimmed.result.unwrap().due_markers_scanned, 3);

        for key in ["one", "two"] {
            let request = GetItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Key": {"pk": {"S": key}}
            }))
            .unwrap();
            let read = observe(store.operations.get_item(request)).await;
            read.assert_no_warning_or_error();
            assert_eq!(
                serde_json::to_value(read.result.unwrap()).unwrap(),
                json!({"Item": {"pk": {"S": key}}})
            );
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
