//! Native snapshot visibility and response-byte pagination.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::FutureExt as _;
use serde_json::json;
use storage_provider::SystemStreamTrimExecutor;
use storage_types::{CreateTableRequest, PutItemRequest, TableName, context::WrappedError as _};
use stream_provider::SystemStreamPageRequest;

use super::test_support::{TestStore, unique_table_name};

async fn create_stream_table(store: &TestStore) -> TableName {
    let table = unique_table_name();
    store.operations.create_table(CreateTableRequest::try_from(json!({
        "TableName": table.as_ref(),
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    })).unwrap()).await.unwrap();
    table
}

async fn put_snapshot(store: &TestStore, table: &TableName, key: &str, value: &str) {
    store
        .operations
        .put_item(
            PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": {"pk": {"S": key}, "value": {"S": value}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn given_untrimmed_expired_snapshot_when_read_then_fresh_reader_skips_it_and_old_checkpoint_reports_gap()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = create_stream_table(&store).await;
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let origin = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), None, 10).unwrap(),
            )
            .await
            .unwrap()
            .issued_checkpoint()
            .token()
            .clone();
        put_snapshot(&store, &table, "expired", "old").await;
        store.advance_time(Duration::from_secs(23 * 3600));
        put_snapshot(&store, &table, "retained", "new").await;
        store.advance_time(Duration::from_secs(3601));
        let page = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), None, 10).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(page.records().len(), 1);
        assert_eq!(
            serde_json::to_value(&page.records()[0].keys).unwrap(),
            json!({"pk": {"S": "retained"}})
        );
        assert!(page.terminal());
        let gap = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), Some(origin), 10).unwrap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            gap.into_storage_error().to_enum(),
            storage_types::StorageEnum::HistoryUnavailable
        ));
        let trim = SystemStreamTrimExecutor::new(store.system_stream_trim_repository.as_ref(), 10);
        let result = trim
            .run_page(scope.clone(), store.now() - 24 * 3600 * 1000, None)
            .await
            .unwrap();
        assert_eq!(result.reclaimed_records(), 1);
        let fresh = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), None, 10).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(fresh.records()).unwrap(),
            serde_json::to_value(page.records()).unwrap()
        );
        let resumed = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(
                    scope,
                    Some(page.issued_checkpoint().token().clone()),
                    10,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(resumed.records().is_empty());
        assert!(resumed.terminal());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_large_snapshots_when_response_byte_limit_is_reached_then_checkpoint_resumes_omitted_suffix()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = create_stream_table(&store).await;
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let item_count = storage_types::STREAM_RECORDS_MAX_ENCODED_BYTES as usize / 380_000 + 2;
        for key in 0..item_count {
            put_snapshot(
                &store,
                &table,
                &format!("key-{key:02}"),
                &"x".repeat(380_000),
            )
            .await;
        }
        let first = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), None, item_count as u32).unwrap(),
            )
            .await
            .unwrap();
        assert!(!first.records().is_empty() && first.records().len() < item_count);
        assert!(!first.terminal());
        assert!(
            serde_json::to_vec(&first.clone().into_response().unwrap())
                .unwrap()
                .len()
                <= storage_types::STREAM_RECORDS_MAX_ENCODED_BYTES as usize
        );
        let second = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(
                    scope.clone(),
                    Some(first.issued_checkpoint().token().clone()),
                    item_count as u32,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.records().len(), item_count - first.records().len());
        assert!(second.terminal());
        let keys = first
            .records()
            .iter()
            .chain(second.records())
            .map(|record| {
                serde_json::to_value(&record.keys).unwrap()["pk"]["S"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            (0..item_count)
                .map(|key| format!("key-{key:02}"))
                .collect::<Vec<_>>()
        );
        let tail = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(
                    scope,
                    Some(second.issued_checkpoint().token().clone()),
                    item_count as u32,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(tail.records().is_empty());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_expired_prefix_larger_than_page_when_fresh_system_scan_resumes_then_initial_expiry_is_skipped_and_new_gaps_fail()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = create_stream_table(&store).await;
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        for key in 0..7 {
            put_snapshot(&store, &table, &format!("expired-{key}"), "old").await;
        }
        store.advance_time(Duration::from_secs(25 * 3600));
        put_snapshot(&store, &table, "retained", "new").await;
        let mut checkpoint = None;
        let mut seen = 0;
        for _ in 0..5 {
            let page = store
                .stream_operations
                .get_system_stream_page(
                    SystemStreamPageRequest::try_new(scope.clone(), checkpoint, 2).unwrap(),
                )
                .await
                .unwrap();
            checkpoint = Some(page.issued_checkpoint().token().clone());
            for record in page.records() {
                assert_eq!(
                    serde_json::to_value(&record.keys).unwrap(),
                    json!({"pk": {"S": "retained"}})
                );
                seen += 1;
            }
        }
        assert_eq!(seen, 1);
        put_snapshot(&store, &table, "missed", "must-report-gap").await;
        store.advance_time(Duration::from_secs(25 * 3600));
        let error = store
            .stream_operations
            .get_system_stream_page(SystemStreamPageRequest::try_new(scope, checkpoint, 2).unwrap())
            .await
            .unwrap_err();
        assert!(matches!(
            error.into_storage_error().as_ref(),
            storage_types::StorageEnum::HistoryUnavailable
        ));
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
