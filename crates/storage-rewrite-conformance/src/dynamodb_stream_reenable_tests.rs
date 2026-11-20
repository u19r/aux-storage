//! Each stream enable is a new stream: a new label and ARN, no earlier
//! records, and no reads through the earlier ARN.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::{FutureExt as _, future::BoxFuture};
use serde_json::json;
use storage_types::{
    DeleteTableRequest, DescribeStreamRequest, DescribeTableRequest, GetRecordsRequest,
    GetShardIteratorRequest, ListStreamsRequest, StorageEnum, StorageError, StreamViewType,
    TableName, context::WrappedError as _,
};

use super::{
    dynamodb_stream_history_tests::{create_stream_table, describe_shards, iterator, put_value},
    table_history_retention_tests::read_standard,
    test_support::TestStore,
};

async fn set_stream(store: &TestStore, table: &TableName, specification: serde_json::Value) {
    store
        .operations
        .update_table(
            serde_json::from_value(
                json!({"TableName": table, "StreamSpecification": specification}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn latest_stream(store: &TestStore, table: &TableName) -> (String, String) {
    let table = store
        .operations
        .describe_table(DescribeTableRequest {
            table_name: table.clone(),
        })
        .await
        .unwrap()
        .table;
    (
        table.latest_stream_arn.expect("enabled stream has an ARN"),
        table
            .latest_stream_label
            .expect("enabled stream has a label"),
    )
}

fn assert_stream_not_found(error: StorageError, request: &str) {
    assert!(
        matches!(
            error.to_enum(),
            StorageEnum::ResourceNotFound {
                resource_type: "stream",
                ..
            }
        ),
        "{request} through an earlier stream ARN must find no stream"
    );
}

/// The first stream of a table, observed before its disable.
struct FirstStream {
    table: TableName,
    arn: String,
    label: String,
    shard: String,
    iterator: String,
}

/// Write a MODIFY under `NEW_IMAGE`, then disable and re-enable with
/// `NEW_AND_OLD_IMAGES`.
async fn reenable_with_new_view(store: &TestStore) -> FirstStream {
    let (table, first_arn) = create_stream_table(store, "NEW_IMAGE").await;
    let (_, first_label) = latest_stream(store, &table).await;
    put_value(store, &table, "item", "one").await;
    put_value(store, &table, "item", "two").await;
    store.wait_until_changes_are_readable().await;
    let shard = describe_shards(store, &first_arn).await[0].shard_id.clone();
    let first_iterator = iterator(store, &first_arn, &shard, "TRIM_HORIZON", None).await;
    set_stream(store, &table, json!({"StreamEnabled": false})).await;
    set_stream(
        store,
        &table,
        json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}),
    )
    .await;
    FirstStream {
        table,
        arn: first_arn,
        label: first_label,
        shard,
        iterator: first_iterator,
    }
}

/// Run `body` and release the store even when an assertion fails.
async fn with_store(body: impl for<'a> FnOnce(&'a TestStore) -> BoxFuture<'a, ()>) {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(body(&store)).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

async fn describe_stream(
    store: &TestStore,
    arn: &str,
) -> Result<storage_types::StreamDescription, StorageError> {
    store
        .operations
        .describe_stream(DescribeStreamRequest {
            stream_arn: arn.into(),
            exclusive_start_shard_id: None,
            limit: None,
            shard_filter: None,
        })
        .await
        .map(|response| response.stream_description)
}

#[tokio::test]
async fn given_disabled_stream_when_reenabled_then_stream_label_and_arn_are_new() {
    with_store(|store| {
        Box::pin(async move {
            let first = reenable_with_new_view(store).await;
            let (arn, label) = latest_stream(store, &first.table).await;
            super::describe_table_conformance_tests::assert_iso_stream_label(
                &label,
                "re-enabled stream",
            );
            // The test clock does not move: the store issues the next
            // millisecond.
            assert!(label > first.label, "{label} must follow {}", first.label);
            assert_ne!(arn, first.arn);
            assert_eq!(
                arn,
                format!(
                    "arn:aws:dynamodb:us-east-1:123456789012:table/{}/stream/{label}",
                    first.table
                )
            );
            let described = describe_stream(store, &arn).await.unwrap();
            assert_eq!(described.stream_arn, arn);
            assert_eq!(described.stream_label, label);
            assert_eq!(described.stream_view_type, StreamViewType::NewAndOldImages);
            let listed = store
                .operations
                .list_streams(ListStreamsRequest {
                    table_name: Some(first.table.clone()),
                    exclusive_start_stream_arn: None,
                    limit: None,
                })
                .await
                .unwrap()
                .streams;
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].stream_arn, arn);
            assert_eq!(listed[0].stream_label, label);
        })
    })
    .await;
}

#[tokio::test]
async fn given_reenabled_stream_when_earlier_arn_is_used_then_stream_is_not_found() {
    with_store(|store| {
        Box::pin(async move {
            let first = reenable_with_new_view(store).await;
            assert_stream_not_found(
                describe_stream(store, &first.arn).await.unwrap_err(),
                "DescribeStream",
            );
            let error = store
                .operations
                .get_shard_iterator(GetShardIteratorRequest {
                    stream_arn: first.arn.clone(),
                    shard_id: first.shard.clone(),
                    shard_iterator_type: "TRIM_HORIZON".into(),
                    sequence_number: None,
                })
                .await
                .unwrap_err();
            assert_stream_not_found(error, "GetShardIterator");
            let error = store
                .operations
                .get_records(GetRecordsRequest {
                    shard_iterator: first.iterator.clone(),
                    limit: Some(100),
                })
                .await
                .unwrap_err();
            assert_stream_not_found(error, "GetRecords");
        })
    })
    .await;
}

#[tokio::test]
async fn given_records_of_an_earlier_enable_when_reenabled_with_another_view_then_new_stream_has_only_later_records()
 {
    with_store(|store| {
        Box::pin(async move {
            let first = reenable_with_new_view(store).await;
            let (arn, _) = latest_stream(store, &first.table).await;
            // The new stream starts empty, as on AWS.
            assert!(read_standard(store, &arn).await.unwrap().is_empty());
            put_value(store, &first.table, "item", "three").await;
            store.wait_until_changes_are_readable().await;
            let records = read_standard(store, &arn).await.unwrap();
            assert_eq!(
                records.len(),
                1,
                "only the write after the enable is served"
            );
            let record = &records[0];
            assert_eq!(record.event_name, "MODIFY");
            assert_eq!(record.event_source_arn, arn);
            assert_eq!(
                record.dynamodb.stream_view_type,
                StreamViewType::NewAndOldImages
            );
            assert_eq!(
                record.dynamodb.old_image,
                Some(
                    serde_json::from_value(json!({"pk": {"S": "item"}, "value": {"S": "two"}}))
                        .unwrap()
                )
            );
            assert_eq!(
                record.dynamodb.new_image,
                Some(
                    serde_json::from_value(json!({"pk": {"S": "item"}, "value": {"S": "three"}}))
                        .unwrap()
                )
            );
        })
    })
    .await;
}

#[tokio::test]
async fn given_table_recreated_in_an_earlier_millisecond_when_stream_enabled_then_arn_is_new() {
    with_store(|store| {
        Box::pin(async move {
            let (table, first_arn) = create_stream_table(store, "NEW_IMAGE").await;
            let (_, first_label) = latest_stream(store, &table).await;
            store
                .operations
                .delete_table(DeleteTableRequest {
                    table_name: table.clone(),
                })
                .await
                .unwrap();
            // Another node's clock lags this one's: the recreate sees an
            // earlier time.
            store.set_time(store.now() - 1_000);
            let mut request = super::test_support::hash_table_request(table.clone());
            request.stream_specification = Some(storage_types::StreamSpecification {
                stream_enabled: true,
                stream_view_type: Some(StreamViewType::NewImage),
            });
            let created = store
                .operations
                .create_table(request)
                .await
                .unwrap()
                .table_description;
            let label = created.latest_stream_label.unwrap();
            assert_ne!(created.latest_stream_arn.unwrap(), first_arn);
            assert!(label > first_label, "{label} must follow {first_label}");
            assert_stream_not_found(
                describe_stream(store, &first_arn).await.unwrap_err(),
                "DescribeStream",
            );
        })
    })
    .await;
}
