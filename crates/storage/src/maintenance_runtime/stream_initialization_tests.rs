//! The assembled scheduler publishes streams only after all initial images
//! exist.

use std::{sync::Arc, time::Duration};

use bg_jobs_tokio::test_support::{TestClock, runtime_builder};
use scheduled_bg_jobs::{
    DriveBudget, Namespace, OwnerId, QueueSelection, Timestamp, runtime::MonotonicDeadline,
};
use storage_common::{DatabaseJobIntervals, StorageJob};
use storage_provider::DynamoOperations;
use storage_types::{BoundTableInfo, TableHistoryId, TableName, TimestampMillis};
use stream_provider::StreamOperations;

use super::catalog::MaintenanceJobCatalog;
use crate::StorageProviderBundle;

#[tokio::test]
async fn given_pending_stream_when_existing_gsi_job_runs_then_every_current_image_precedes_publication()
 {
    let store = Arc::new(
        sql_backend::SqlStore::initialize_schema(
            sql_backend::sqlite::SqliteBackend::initialize(":memory:")
                .await
                .unwrap(),
        )
        .await
        .unwrap(),
    );
    let table = TableName::new("runtime-stream-capture");
    let created = store
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
        !updated
            .table_description
            .stream_specification
            .unwrap()
            .stream_enabled
    );
    let now = Timestamp::from_millis(TimestampMillis::now().timestamp_millis());
    let clock = Arc::new(TestClock::new(now, MonotonicDeadline::ZERO));
    let bundle =
        StorageProviderBundle::try_from_initialized_store(store.clone(), Default::default())
            .unwrap();
    let catalog = MaintenanceJobCatalog::try_new(
        bundle.maintenance.unwrap(),
        bundle.table_reclaim_repository,
        DatabaseJobIntervals::default(),
        now,
    )
    .unwrap();
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
        clock.advance(Duration::from_millis(1));
    }
    let info = store.get_table_info(table.clone()).await.unwrap();
    assert!(
        info.has_enabled_stream(),
        "GSI maintenance did not complete initialization"
    );
    let bound = BoundTableInfo {
        history_id: TableHistoryId::from(
            *uuid::Uuid::parse_str(&created.table_description.table_id)
                .unwrap()
                .as_bytes(),
        ),
        info,
    };
    for index in 0..40 {
        let key =
            serde_json::from_value(serde_json::json!({"pk": {"S": index.to_string()}})).unwrap();
        let address = stream_provider::dynamo_item_history_address(&bound, &key).unwrap();
        let page = store
            .read_forward(
                stream_provider::StreamRowKind::Item(address.physical_stream_name().unwrap()),
                None,
                2,
            )
            .await
            .unwrap();
        assert_eq!(
            page.items.len(),
            1,
            "public stream is missing an initial current image"
        );
    }
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
        store
            .get_table_info(table.clone())
            .await
            .unwrap()
            .has_internal_stream_capture()
    );
    for _ in 0..64 {
        runtime.drive_once().await.unwrap();
        let info = store.get_table_info(table.clone()).await.unwrap();
        assert!(
            !info.has_enabled_stream(),
            "internal GSI build exposed a public stream"
        );
        if !info.has_internal_stream_capture() {
            break;
        }
        clock.advance(Duration::from_millis(1));
    }
    assert!(
        !store
            .get_table_info(table.clone())
            .await
            .unwrap()
            .requires_stream_entry(),
        "assembled GSI maintenance left temporary capture enabled"
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
    let key = serde_json::from_value(serde_json::json!({"pk": {"S": "after-build"}})).unwrap();
    let address = stream_provider::dynamo_item_history_address(&bound, &key).unwrap();
    assert!(
        store
            .read_forward(
                stream_provider::StreamRowKind::Item(address.physical_stream_name().unwrap()),
                None,
                2,
            )
            .await
            .unwrap()
            .items
            .is_empty(),
        "write after GSI completion created history"
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
