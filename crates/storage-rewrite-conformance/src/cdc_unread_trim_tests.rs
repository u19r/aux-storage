//! Unread logical changes remain available while retention advances.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_provider::SystemStreamTrimExecutor;
use storage_types::{CreateTableRequest, PutItemRequest, StreamName};
use stream_provider::{StreamRowKind, SystemStreamPageRequest};

use super::test_support::{TestStore, unique_table_name};

// Boundary rule from the legacy SystemStream trim tests: an issued checkpoint
// protects changes that its reader has not consumed.
#[tokio::test]
async fn given_unread_cdc_after_a_checkpoint_when_retention_trims_then_unread_changes_remain_available()
 {
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
        }))
        .unwrap();
        let created = observe(store.operations.create_table(create)).await;
        created.assert_no_warning_or_error();
        created.result.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();

        for key in ["first", "second", "third"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}}
            }))
            .unwrap();
            let written = observe(store.operations.put_item(put)).await;
            written.assert_no_warning_or_error();
            written.one_span_with("dynamo_operation", &[("operation", "PutItem")]);
            written.result.unwrap();
        }

        let first = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 1).unwrap(),
        ))
        .await;
        first.assert_no_warning_or_error();
        let first = first.result.unwrap();
        assert_eq!(first.records().len(), 1);
        assert!(!first.terminal());
        assert_eq!(
            serde_json::to_value(&first.records()[0].keys).unwrap(),
            json!({"pk": {"S": "first"}}),
        );
        let checkpoint = first.issued_checkpoint().token().clone();

        let worker =
            SystemStreamTrimExecutor::new(store.system_stream_trim_repository.as_ref(), 10);
        let trimmed = observe(worker.run_page(scope.clone(), store.now() + 60_000, None)).await;
        trimmed.assert_no_warning_or_error();
        let trimmed = trimmed.result.unwrap();
        assert_eq!(trimmed.reclaimed_records(), 0);
        assert!(trimmed.next_continuation().is_none());

        let resumed = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, Some(checkpoint), 10).unwrap(),
        ))
        .await;
        resumed.assert_no_warning_or_error();
        let resumed = resumed.result.unwrap();
        assert!(resumed.terminal());
        assert_eq!(resumed.records().len(), 2);
        for (record, key) in resumed.records().iter().zip(["second", "third"]) {
            assert_eq!(record.source_table_name.as_ref(), Some(&table));
            assert_eq!(
                serde_json::to_value(&record.keys).unwrap(),
                json!({"pk": {"S": key}}),
            );
            assert_eq!(
                serde_json::to_value(&record.new_image).unwrap(),
                json!({"pk": {"S": key}}),
            );
        }

        let physical = observe(store.stream_operations.read_forward(
            StreamRowKind::System(StreamName::system_table_stream()),
            None,
            10,
        ))
        .await;
        physical.assert_no_warning_or_error();
        assert_eq!(physical.result.unwrap().items.len(), 3);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
