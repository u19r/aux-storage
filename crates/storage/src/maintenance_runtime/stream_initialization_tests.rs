//! GSI maintenance does not synthesize stream entries for existing items.

use std::sync::Arc;

use bg_jobs_tokio::test_support::{TestClock, runtime_builder};
use scheduled_bg_jobs::{
    DriveBudget, Namespace, OwnerId, QueueSelection, Timestamp, runtime::MonotonicDeadline,
};
use storage_common::{DatabaseJobIntervals, StorageJob};
use storage_provider::DynamoOperations;
use storage_types::{TableName, TimestampMillis};

use super::catalog::MaintenanceJobCatalog;

#[tokio::test]
async fn given_immediate_enable_when_gsi_job_runs_then_no_synthetic_images_are_published() {
    let store = Arc::new(
        sql_backend::SqlStore::initialize_schema(
            sql_backend::sqlite::SqliteBackend::initialize(
                ":memory:",
                storage_types::AttemptDeadlines::default(),
            )
            .await
            .unwrap(),
            storage_types::InitialStreamLineages::ONE,
            true,
        )
        .await
        .unwrap(),
    );
    let table = TableName::new("runtime-stream-capture");
    store
        .create_table(
            serde_json::from_value(serde_json::json!({
                "TableName": table,
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    for index in 0..40 {
        store
            .put_item(
                serde_json::from_value(serde_json::json!({
                    "TableName": table, "Item": {"pk": {"S": index.to_string()}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let updated = store.update_table(serde_json::from_value(serde_json::json!({
        "TableName": table,
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    })).unwrap()).await.unwrap();
    assert!(
        updated
            .table_description
            .stream_specification
            .unwrap()
            .stream_enabled
    );
    let now = Timestamp::from_millis(TimestampMillis::now().timestamp_millis());
    let clock = Arc::new(TestClock::new(now, MonotonicDeadline::ZERO));
    let bundle = crate::InitializedStore::native(store.clone()).caller_maintenance();
    let catalog = MaintenanceJobCatalog::try_new(
        bundle.maintenance.unwrap(),
        DatabaseJobIntervals::default(),
        now,
        None,
    )
    .unwrap()
    .0;
    let runtime = runtime_builder(
        bundle.scheduled_job_store,
        catalog,
        DriveBudget::try_resident(
            QueueSelection::only(vec![StorageJob::GsiBackfill.queue()]).unwrap(),
        )
        .unwrap(),
    )
    .namespace(Namespace::try_new("runtime-stream-capture").unwrap())
    .owner(OwnerId::from([1; 16]))
    .clock(clock.clone())
    .build()
    .unwrap();
    runtime.ensure_seeds().await.unwrap();
    for _ in 0..64 {
        runtime.drive_once().await.unwrap();
        if store
            .get_table_info(table.clone())
            .await
            .unwrap()
            .has_enabled_stream()
        {
            break;
        }
        clock.advance(StorageJob::GsiBackfill.interval(DatabaseJobIntervals::default()));
    }
    let info = store.get_table_info(table.clone()).await.unwrap();
    assert!(
        info.has_enabled_stream(),
        "GSI maintenance changed immediate stream enablement"
    );
    assert!(
        store
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(100)
            ))
            .await
            .unwrap()
            .records()
            .is_empty()
    );
    store
        .update_table(
            serde_json::from_value(serde_json::json!({
                "TableName": table, "StreamSpecification": {"StreamEnabled": false}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .update_table(
            serde_json::from_value(serde_json::json!({
                "TableName": table,
                "GlobalSecondaryIndexUpdates": [{"Create": {
                    "IndexName": "by_pk", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }}]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        !store
            .get_table_info(table.clone())
            .await
            .unwrap()
            .requires_stream_entry()
    );
    for _ in 0..64 {
        runtime.drive_once().await.unwrap();
        let info = store.get_table_info(table.clone()).await.unwrap();
        assert!(
            !info.has_enabled_stream(),
            "internal GSI build exposed a public stream"
        );
        clock.advance(StorageJob::GsiBackfill.interval(DatabaseJobIntervals::default()));
    }
    assert!(
        !store
            .get_table_info(table.clone())
            .await
            .unwrap()
            .requires_stream_entry(),
        "GSI maintenance unexpectedly enabled stream capture"
    );
    store
        .put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": table, "Item": {"pk": {"S": "after-build"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        store
            .list_streams(serde_json::from_value(serde_json::json!({"TableName": table})).unwrap())
            .await
            .unwrap()
            .streams
            .is_empty()
    );
    let query = store
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": table, "IndexName": "by_pk", "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "after-build"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(query.items.unwrap_or_default().len(), 1);
}
