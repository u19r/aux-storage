//! Standard DynamoDB views share exact references with snapshot table reads.

use std::{
    collections::BTreeSet,
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::FutureExt as _;
use serde_json::json;
use storage_types::{
    CreateTableRequest, DeleteItemRequest, DescribeStreamRequest, GetRecordsRequest,
    GetShardIteratorRequest, GetStreamRecordsRequest, PutItemRequest, TableName,
};
use stream_provider::{
    SystemStreamLineage, SystemStreamPageRequest, SystemStreamSplitChild,
    SystemStreamSplitOperationId, SystemStreamSplitRequest,
};

use super::test_support::{TestStore, unique_table_name};

pub(super) async fn create_stream_table(store: &TestStore, view: &str) -> (TableName, String) {
    let table = unique_table_name();
    let created = store
        .operations
        .create_table(
            CreateTableRequest::try_from(json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST", "AuxStreamDurationHours": 48,
                "StreamSpecification": {"StreamEnabled": true, "StreamViewType": view}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    (table, created.table_description.latest_stream_arn.unwrap())
}

pub(super) async fn put_value(store: &TestStore, table: &TableName, key: &str, value: &str) {
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

pub(super) async fn describe_shards(store: &TestStore, arn: &str) -> Vec<storage_types::Shard> {
    store
        .operations
        .describe_stream(DescribeStreamRequest {
            stream_arn: arn.into(),
            exclusive_start_shard_id: None,
            limit: None,
            shard_filter: None,
        })
        .await
        .unwrap()
        .stream_description
        .shards
}

pub(super) async fn iterator(
    store: &TestStore,
    arn: &str,
    shard: &str,
    kind: &str,
    sequence: Option<String>,
) -> String {
    store
        .operations
        .get_shard_iterator(GetShardIteratorRequest {
            stream_arn: arn.into(),
            shard_id: shard.into(),
            shard_iterator_type: kind.into(),
            sequence_number: sequence,
        })
        .await
        .unwrap()
        .shard_iterator
}

async fn read_published_records(
    store: &TestStore,
    request: GetRecordsRequest,
    expected: usize,
) -> storage_types::GetRecordsResponse {
    // The safe PostgreSQL publication frontier can lag an acknowledged write
    // while an earlier cluster transaction remains open. Retain the iterator
    // until the expected records are visible; never advance past an empty page.
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let page = store.operations.get_records(request.clone()).await.unwrap();
            if page.records.len() >= expected {
                return page;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("acknowledged stream records did not become published")
}

#[tokio::test]
async fn given_all_dynamodb_views_when_history_is_paged_then_exact_predecessors_and_insert_after_delete_are_preserved()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        for view in ["KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES"] {
            let (table, arn) = create_stream_table(&store, view).await;
            put_value(&store, &table, "item", "v1").await;
            put_value(&store, &table, "item", "v2").await;
            store
                .operations
                .delete_item(
                    DeleteItemRequest::try_from(json!({
                        "TableName": table.as_ref(), "Key": {"pk": {"S": "item"}}
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
            put_value(&store, &table, "item", "v3").await;
            let shards = describe_shards(&store, &arn).await;
            assert_eq!(shards.len(), 1);
            let shard = &shards[0].shard_id;
            assert_eq!(shard.len(), 64);
            let origin = iterator(&store, &arn, shard, "TRIM_HORIZON", None).await;
            let first = read_published_records(
                &store,
                GetRecordsRequest {
                    shard_iterator: origin.clone(),
                    limit: Some(2),
                },
                2,
            )
            .await;
            let second = read_published_records(
                &store,
                GetRecordsRequest {
                    shard_iterator: first.next_shard_iterator.unwrap(),
                    limit: Some(2),
                },
                2,
            )
            .await;
            let records = first
                .records
                .iter()
                .chain(second.records.iter())
                .collect::<Vec<_>>();
            assert_eq!(records.len(), 4);
            for (record, (event, old, new)) in records.iter().zip([
                ("INSERT", None, Some("v1")),
                ("MODIFY", Some("v1"), Some("v2")),
                ("REMOVE", Some("v2"), None),
                ("INSERT", None, Some("v3")),
            ]) {
                assert_eq!(record.event_name, event);
                assert_eq!(record.dynamodb.sequence_number.len(), 40);
                assert!(
                    record
                        .dynamodb
                        .sequence_number
                        .bytes()
                        .all(|byte| byte.is_ascii_digit())
                );
                let encoded = serde_json::to_value(record).unwrap();
                assert_eq!(encoded["dynamodb"]["Keys"], json!({"pk": {"S": "item"}}));
                let expected_old =
                    old.filter(|_| matches!(view, "OLD_IMAGE" | "NEW_AND_OLD_IMAGES"));
                let expected_new =
                    new.filter(|_| matches!(view, "NEW_IMAGE" | "NEW_AND_OLD_IMAGES"));
                assert_eq!(
                    encoded["dynamodb"]["OldImage"]["value"]["S"].as_str(),
                    expected_old
                );
                assert_eq!(
                    encoded["dynamodb"]["NewImage"]["value"]["S"].as_str(),
                    expected_new
                );
            }
            let at = iterator(
                &store,
                &arn,
                shard,
                "AT_SEQUENCE_NUMBER",
                Some(records[1].dynamodb.sequence_number.clone()),
            )
            .await;
            let at = read_published_records(
                &store,
                GetRecordsRequest {
                    shard_iterator: at,
                    limit: Some(1),
                },
                1,
            )
            .await;
            assert_eq!(at.records[0].event_name, "MODIFY");
            let after = iterator(
                &store,
                &arn,
                shard,
                "AFTER_SEQUENCE_NUMBER",
                Some(records[1].dynamodb.sequence_number.clone()),
            )
            .await;
            let after = read_published_records(
                &store,
                GetRecordsRequest {
                    shard_iterator: after,
                    limit: Some(1),
                },
                1,
            )
            .await;
            assert_eq!(after.records[0].event_name, "REMOVE");
            let latest = iterator(&store, &arn, shard, "LATEST", None).await;
            let idle = store
                .operations
                .get_records(GetRecordsRequest {
                    shard_iterator: latest,
                    limit: Some(10),
                })
                .await
                .unwrap();
            assert!(idle.records.is_empty());
            put_value(&store, &table, "item", "v4").await;
            let tail = read_published_records(
                &store,
                GetRecordsRequest {
                    shard_iterator: idle.next_shard_iterator.unwrap(),
                    limit: Some(10),
                },
                1,
            )
            .await;
            assert_eq!(tail.records.len(), 1);
            assert_eq!(tail.records[0].event_name, "MODIFY");
            let snapshots = store
                .operations
                .get_stream_records(GetStreamRecordsRequest::table(table, None, Some(10)))
                .await
                .unwrap();
            assert_eq!(snapshots.records().len(), 5);
            assert!(snapshots.records()[2].item.is_none());
            assert_eq!(
                serde_json::to_value(&snapshots.records()[3].item).unwrap()["value"]["S"],
                "v3"
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

#[tokio::test]
async fn given_source_split_when_table_pages_resume_then_each_lane_is_read_once_and_standard_shards_expose_parentage()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let (table, arn) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        put_value(&store, &table, "parent", "before").await;
        let before = store
            .operations
            .get_stream_records(GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await
            .unwrap();
        let mut checkpoint = before.last_evaluated_key().unwrap().to_owned();
        let old_shard = describe_shards(&store, &arn).await.remove(0).shard_id;
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let parent = store
            .stream_operations
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap()
            .into_iter()
            .find(|lane| !lane.sealed())
            .unwrap();
        let children = ["table-left", "table-right"].map(|id| {
            SystemStreamSplitChild::try_new(
                SystemStreamLineage::try_new(
                    id,
                    id,
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
                    parent,
                    children,
                    SystemStreamSplitOperationId::try_new("table-history-split").unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        for key in 0..32 {
            put_value(&store, &table, &format!("child-{key:02}"), "after").await;
        }
        let mut seen = BTreeSet::new();
        for page in 0..33 {
            let next = store
                .operations
                .get_stream_records(GetStreamRecordsRequest::table(
                    table.clone(),
                    Some(checkpoint),
                    Some(3),
                ))
                .await
                .unwrap();
            checkpoint = next.last_evaluated_key().unwrap().to_owned();
            if next.records().is_empty() {
                break;
            }
            assert!(page < 32, "table cursor made no progress");
            for record in next.records() {
                let key = serde_json::to_value(&record.keys).unwrap()["pk"]["S"]
                    .as_str()
                    .unwrap()
                    .to_owned();
                assert!(seen.insert(key));
            }
        }
        assert_eq!(seen.len(), 32);
        let shards = describe_shards(&store, &arn).await;
        assert_eq!(shards.len(), 3);
        assert_eq!(
            shards
                .iter()
                .filter(|shard| shard.parent_shard_id.as_deref() == Some(&old_shard))
                .count(),
            2
        );
        let mut standard_count = 0;
        for shard in &shards {
            let start = iterator(&store, &arn, &shard.shard_id, "TRIM_HORIZON", None).await;
            let page = store
                .operations
                .get_records(GetRecordsRequest {
                    shard_iterator: start,
                    limit: Some(100),
                })
                .await
                .unwrap();
            if shard.shard_id == old_shard {
                assert!(page.next_shard_iterator.is_none());
                assert_eq!(page.records.len(), 1);
            } else {
                assert!(!page.records.is_empty());
                assert!(page.next_shard_iterator.is_some());
            }
            standard_count += page.records.len();
        }
        assert_eq!(standard_count, 33);
        store.advance_time(Duration::from_secs(24 * 3600 + 1));
        let system = store
            .stream_operations
            .get_system_stream_page(SystemStreamPageRequest::try_new(scope, None, 100).unwrap())
            .await
            .unwrap();
        assert!(system.records().is_empty());
        let table_page = store
            .operations
            .get_stream_records(GetStreamRecordsRequest::table(table, None, Some(100)))
            .await
            .unwrap();
        assert_eq!(table_page.records().len(), 33);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_untrimmed_expired_prefix_when_fresh_table_reader_pages_then_it_skips_initial_expiry_and_detects_later_gaps()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        for key in 0..7 {
            put_value(&store, &table, &format!("expired-{key}"), "old").await;
        }
        let before = store
            .operations
            .get_stream_records(GetStreamRecordsRequest::table(table.clone(), None, Some(1)))
            .await
            .unwrap();
        let old_checkpoint = before.last_evaluated_key().unwrap().to_owned();
        store.advance_time(Duration::from_secs(49 * 3600));
        put_value(&store, &table, "retained", "new").await;
        let mut checkpoint = None;
        let mut seen = 0;
        for _ in 0..5 {
            let page = store
                .operations
                .get_stream_records(GetStreamRecordsRequest::table(
                    table.clone(),
                    checkpoint,
                    Some(2),
                ))
                .await
                .unwrap();
            checkpoint = page.last_evaluated_key().map(str::to_owned);
            for record in page.records() {
                assert_eq!(
                    serde_json::to_value(&record.keys).unwrap(),
                    json!({"pk": {"S": "retained"}})
                );
                seen += 1;
            }
        }
        assert_eq!(seen, 1);
        let error = store
            .operations
            .get_stream_records(GetStreamRecordsRequest::table(
                table.clone(),
                Some(old_checkpoint),
                Some(2),
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            error.as_ref(),
            storage_types::StorageEnum::HistoryUnavailable
        ));
        put_value(&store, &table, "missed", "must-report-gap").await;
        store.advance_time(Duration::from_secs(49 * 3600));
        let error = store
            .operations
            .get_stream_records(GetStreamRecordsRequest::table(table, checkpoint, Some(2)))
            .await
            .unwrap_err();
        assert!(matches!(
            error.as_ref(),
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
async fn given_large_dynamodb_events_when_page_reaches_one_mib_then_omitted_events_resume_without_duplicates()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let (table, arn) = create_stream_table(&store, "NEW_IMAGE").await;
        for key in 0..4 {
            put_value(&store, &table, &format!("key-{key}"), &"x".repeat(380_000)).await;
        }
        let shard = describe_shards(&store, &arn).await.remove(0).shard_id;
        let start = iterator(&store, &arn, &shard, "TRIM_HORIZON", None).await;
        let first = store
            .operations
            .get_records(GetRecordsRequest {
                shard_iterator: start,
                limit: Some(10),
            })
            .await
            .unwrap();
        assert_eq!(first.records.len(), 2);
        assert!(serde_json::to_vec(&first).unwrap().len() <= 1024 * 1024);
        let second = store
            .operations
            .get_records(GetRecordsRequest {
                shard_iterator: first.next_shard_iterator.unwrap(),
                limit: Some(10),
            })
            .await
            .unwrap();
        assert_eq!(second.records.len(), 2);
        let values = first
            .records
            .iter()
            .chain(&second.records)
            .map(|record| {
                serde_json::to_value(&record.dynamodb.keys).unwrap()["pk"]["S"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, ["key-0", "key-1", "key-2", "key-3"]);
        let idle = store
            .operations
            .get_records(GetRecordsRequest {
                shard_iterator: second.next_shard_iterator.unwrap(),
                limit: Some(10),
            })
            .await
            .unwrap();
        assert!(idle.records.is_empty());
        let origin = iterator(
            &store,
            &arn,
            &shard,
            "AT_SEQUENCE_NUMBER",
            Some(first.records[0].dynamodb.sequence_number.clone()),
        )
        .await;
        store.advance_time(Duration::from_secs(49 * 3600));
        let error = store
            .operations
            .get_records(GetRecordsRequest {
                shard_iterator: origin,
                limit: Some(10),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error.as_ref(),
            storage_types::StorageEnum::TrimmedDataAccess
        ));
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
