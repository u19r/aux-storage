//! Unread logical changes remain available while retention advances.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_provider::SystemStreamTrimExecutor;
use storage_types::{CreateTableRequest, PutItemRequest};
use stream_provider::SystemStreamPageRequest;

use super::test_support::{TestStore, unique_table_name};

// Inside the fixed 24-hour window, trimming cannot remove unread changes.
#[tokio::test]
async fn given_unread_cdc_inside_the_system_window_when_retention_trims_then_changes_remain_available()
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

        store.wait_until_changes_are_readable().await;
        let lanes = store
            .stream_operations
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap()
            .len();
        let mut previous = None;
        let mut first = None;
        // Idle native lanes can consume a one-record page before the written
        // lane.
        for _ in 0..=lanes {
            let read = observe(store.stream_operations.get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), previous.clone(), 1).unwrap(),
            ))
            .await;
            read.assert_no_warning_or_error();
            let page = read.result.unwrap();
            assert_eq!(page.source_scope(), &scope);
            assert!(page.records().len() <= 1);
            if !page.records().is_empty() {
                first = Some(page);
                break;
            }
            assert!(!page.terminal(), "three committed records remain available");
            assert_ne!(Some(page.issued_checkpoint().token()), previous.as_ref());
            previous = Some(page.issued_checkpoint().token().clone());
        }
        let first = first.expect("the catalog traversal reaches the first committed record");
        let first_key = first.records()[0]
            .keys
            .get("pk")
            .unwrap()
            .inner_str()
            .unwrap()
            .to_owned();
        let checkpoint = first.issued_checkpoint().token().clone();

        let worker = SystemStreamTrimExecutor::new(store.maintenance.as_ref(), 10);
        let trimmed =
            observe(worker.run_page(scope.clone(), store.now() - 24 * 3600 * 1000 + 60_000, None))
                .await;
        trimmed.assert_no_warning_or_error();
        let trimmed = trimmed.result.unwrap();
        assert_eq!(trimmed.reclaimed_records(), 0);
        assert!(trimmed.next_continuation().is_none());

        let resumed = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(checkpoint.clone()), 10).unwrap(),
        ))
        .await;
        resumed.assert_no_warning_or_error();
        let resumed = resumed.result.unwrap();
        assert!(resumed.terminal());
        assert_eq!(resumed.records().len(), 2);
        let mut delivered = vec![first_key];
        for record in resumed.records() {
            let key = record.keys.get("pk").unwrap().inner_str().unwrap();
            delivered.push(key.to_owned());
            assert_eq!(record.source_table_name.as_ref(), Some(&table));
            assert_eq!(
                serde_json::to_value(&record.keys).unwrap(),
                json!({"pk": {"S": key}}),
            );
            assert_eq!(
                serde_json::to_value(&record.item).unwrap(),
                json!({"pk": {"S": key}}),
            );
        }
        delivered.sort_unstable();
        assert_eq!(delivered, ["first", "second", "third"]);

        let current = super::cdc_stream_tests::system_records(&store, 10)
            .await
            .unwrap();
        assert_eq!(current.len(), 3);
        store.advance_time(std::time::Duration::from_secs(25 * 3600));
        let expired = worker
            .run_page(scope.clone(), store.now() - 24 * 3600 * 1000, None)
            .await
            .unwrap();
        assert_eq!(expired.reclaimed_records(), 3);
        assert!(
            store
                .stream_operations
                .get_system_stream_page(
                    SystemStreamPageRequest::try_new(scope, Some(checkpoint), 10).unwrap(),
                )
                .await
                .is_err(),
            "an unread checkpoint cannot extend system retention"
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
