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
        store.wait_until_changes_are_readable().await;
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
        let trim = SystemStreamTrimExecutor::new(store.maintenance.as_ref(), 10);
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
        store.wait_until_changes_are_readable().await;
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
        let mut keys = first
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
        keys.sort();
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
        store.wait_until_changes_are_readable().await;
        let mut checkpoint = None;
        let mut seen = 0;
        let lineages = store
            .stream_operations
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap();
        // Each lane can return an empty expired-prefix page before the retained
        // event.
        for _ in 0..(8 + lineages.len()) {
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
        store.wait_until_changes_are_readable().await;
        store.advance_time(Duration::from_secs(25 * 3600));
        let mut gap = None;
        for _ in 0..lineages.len() + 1 {
            match store
                .stream_operations
                .get_system_stream_page(
                    SystemStreamPageRequest::try_new(scope.clone(), checkpoint.clone(), 2).unwrap(),
                )
                .await
            {
                Ok(page) => {
                    assert!(page.records().is_empty());
                    assert!(!page.terminal());
                    let next = page.issued_checkpoint().token().clone();
                    assert_ne!(checkpoint.as_ref(), Some(&next));
                    checkpoint = Some(next);
                }
                Err(error) => {
                    gap = Some(error);
                    break;
                }
            }
        }
        let error =
            gap.expect("issued checkpoint reaches the expired gap within the lineage bound");
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

#[tokio::test]
async fn given_oversized_canonical_modify_when_system_snapshot_pages_then_complete_images_resume_once()
 {
    use base64::Engine as _;

    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = create_stream_table(&store).await;
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let mut state = 0x1234_5678_u32;
        let payload = (0..396_000).map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        }).collect::<Vec<_>>();
        let binary = base64::engine::general_purpose::STANDARD.encode(payload);
        let mut expected = Vec::new();
        let mut lengths = Vec::new();
        for version in 0..3 {
            let item = json!({"pk": {"S": "same-key"}, "value": {"B": binary}, "version": {"N": version.to_string()}});
            store.operations.put_item(PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": item
            })).unwrap()).await.unwrap();
            expected.push(item);
            store.wait_until_changes_are_readable().await;
            let mut reads = super::image_retention_tests::canonical_history_reads(&store, &table).await.unwrap();
            for read in &mut reads {
                read.direction = stream_provider::ReadDirection::Backward;
                read.limit = std::num::NonZeroU32::MIN;
            }
            let canonical = (store.canonical_history_read)(reads).await.unwrap();
            lengths.extend(canonical.iter().flat_map(|page| &page.entries)
                .map(|entry| entry.encode_value().unwrap().len()));
        }
        assert_eq!(lengths.len(), 3);
        assert_eq!(lengths.iter().filter(|length| **length > 1024 * 1024).count(), 2);
        eprintln!("canonical entry lengths: {lengths:?}");
        store.wait_until_changes_are_readable().await;
        let mut checkpoint = None;
        let mut actual = Vec::new();
        let mut page_counts = Vec::new();
        for _ in 0..12 {
            let page = store.stream_operations.get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), checkpoint.clone(), 100).unwrap()
            ).await.unwrap();
            for record in page.records() {
                assert_eq!(serde_json::to_value(&record.keys).unwrap(), json!({"pk": {"S": "same-key"}}));
                actual.push(serde_json::to_value(record.item.as_ref().unwrap()).unwrap());
            }
            page_counts.push(page.records().len());
            let next = page.issued_checkpoint().token().clone();
            if !page.terminal() { assert_ne!(checkpoint.as_ref(), Some(&next)); }
            checkpoint = Some(next);
            if page.terminal() { break; }
        }
        assert_eq!(actual, expected);
        let tail = store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, checkpoint, 100).unwrap()
        ).await.unwrap();
        assert!(tail.terminal() && tail.records().is_empty());
        eprintln!("system page record counts: {page_counts:?}; tail=0");
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_unrelated_writes_when_system_pages_have_one_record_limit_then_later_lineage_progresses()
 {
    use stream_provider::{DynamoMutationRoute, SystemStreamLineageCatalog};

    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = create_stream_table(&store).await;
        let other = unique_table_name();
        store
            .operations
            .create_table(super::test_support::hash_table_request(other.clone()))
            .await
            .unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let mut entries = store
            .stream_operations
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap();
        while entries.iter().filter(|lineage| !lineage.sealed()).count() < 4 {
            use stream_provider::{
                SystemStreamLineage, SystemStreamSplitChild, SystemStreamSplitOperationId,
                SystemStreamSplitRequest,
            };
            let parent = entries.iter().find(|lineage| !lineage.sealed()).unwrap();
            let prefix = format!("page-{}", entries.len());
            let children = ["left", "right"].map(|side| {
                let id = format!("{prefix}-{side}");
                SystemStreamSplitChild::try_new(
                    SystemStreamLineage::try_new(
                        &id,
                        &id,
                        Some(parent.lineage_id().as_str().to_owned()),
                        false,
                    )
                    .unwrap(),
                    1,
                )
                .unwrap()
            });
            store
                .stream_operations
                .split_system_stream(
                    SystemStreamSplitRequest::try_new(
                        scope.clone(),
                        parent.clone(),
                        children,
                        SystemStreamSplitOperationId::try_new(prefix).unwrap(),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            entries = store
                .stream_operations
                .system_stream_lineage_catalog(scope.clone())
                .await
                .unwrap();
        }
        let catalog = SystemStreamLineageCatalog::try_new(scope.clone(), entries).unwrap();
        let active = catalog
            .entries()
            .iter()
            .filter(|lineage| !lineage.sealed())
            .collect::<Vec<_>>();
        assert_eq!(active.len(), 4);
        let last = active.last().unwrap();
        let schema = super::test_support::hash_table_request(table.clone());
        let key = (0..1000)
            .map(|index| format!("later-{index}"))
            .find(|key| {
                let attributes = serde_json::from_value(json!({"pk": {"S": key}})).unwrap();
                let route =
                    DynamoMutationRoute::try_new(&table, &schema.key_schema, &attributes).unwrap();
                catalog
                    .writer_lineage(route.routing_key())
                    .unwrap()
                    .lineage_id()
                    == last.lineage_id()
            })
            .expect("a key routes to the last lineage");
        put_snapshot(&store, &table, &key, "target").await;
        let mut checkpoint = None;
        let mut records = Vec::new();
        for index in 0..catalog.entries().len() + 1 {
            put_snapshot(&store, &other, &format!("unrelated-{index}"), "clock").await;
            store.wait_until_changes_are_readable().await;
            let page = store
                .stream_operations
                .get_system_stream_page(
                    SystemStreamPageRequest::try_new(scope.clone(), checkpoint.clone(), 1).unwrap(),
                )
                .await
                .unwrap();
            let next = page.issued_checkpoint().token().clone();
            if !page.terminal() {
                assert_ne!(
                    checkpoint.as_ref(),
                    Some(&next),
                    "a bounded nonterminal page advances its checkpoint"
                );
            }
            checkpoint = Some(next);
            records.extend(page.records().iter().cloned());
        }
        assert_eq!(
            records.len(),
            1,
            "later history must progress while unrelated writes continue"
        );
        assert_eq!(
            serde_json::to_value(&records[0].keys).unwrap(),
            json!({"pk": {"S": key}})
        );
        assert_eq!(
            serde_json::to_value(records[0].item.as_ref().unwrap()).unwrap(),
            json!({"pk": {"S": key}, "value": {"S": "target"}})
        );
        let tail = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope, checkpoint, 100).unwrap(),
            )
            .await
            .unwrap();
        assert!(tail.records().is_empty());
        assert!(tail.terminal());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
