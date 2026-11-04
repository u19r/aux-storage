use std::{sync::Arc, time::Duration};

use bg_jobs_tokio::test_support::{TestClock, runtime_builder};
use scheduled_bg_jobs::{
    DriveBudget, DriveBudgetInput, Namespace, OwnerId, QueueSelection, Timestamp,
    runtime::MonotonicDeadline,
};
use storage_common::{DatabaseJobIntervals, StorageJob};
use storage_provider::{DynamoOperations, StreamRetentionRepository};
use storage_types::TimestampMillis;

use super::catalog::MaintenanceJobCatalog;
use crate::StorageProviderBundle;

#[tokio::test]
async fn given_pending_history_when_the_job_yields_then_cleanup_resumes_at_its_next_deadline() {
    let store = Arc::new(
        sql_backend::SqlStore::initialize_schema(
            sql_backend::sqlite::SqliteBackend::initialize(":memory:")
                .await
                .unwrap(),
        )
        .await
        .unwrap(),
    );
    store
        .create_table(
            serde_json::from_value(serde_json::json!({
                "TableName": "retention-cadence",
                "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
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
                    "TableName": "retention-cadence", "Item": {"pk": {"S": index.to_string()}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let scope = store
        .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 2)
        .await
        .unwrap()
        .remove(0)
        .scope;
    let now = TimestampMillis::now() + 4 * 86_400_000;
    let clock = Arc::new(TestClock::new(
        Timestamp::from_millis(now.timestamp_millis()),
        MonotonicDeadline::ZERO,
    ));
    let bundle =
        StorageProviderBundle::try_from_initialized_store(store.clone(), Default::default())
            .unwrap();
    let catalog = MaintenanceJobCatalog::try_new(
        bundle.maintenance.unwrap(),
        bundle.table_reclaim_repository,
        DatabaseJobIntervals::default(),
        Timestamp::from_millis(now.timestamp_millis()),
    )
    .unwrap();
    let runtime = runtime_builder(
        bundle.scheduled_job_store,
        catalog,
        DriveBudget::try_from(DriveBudgetInput {
            queues: QueueSelection::only(vec![StorageJob::StreamTrim.queue()]).unwrap(),
            max_claims: crate::constants::MAINTENANCE_DRIVER_CLAIMS,
            max_concurrency: crate::constants::MAINTENANCE_DRIVER_CONCURRENCY,
            max_store_operations: crate::constants::MAINTENANCE_DRIVER_MAX_STORE_OPERATIONS,
            stop_claiming_after: Duration::from_secs(1),
            finish_after: Duration::from_secs(2),
        })
        .unwrap(),
    )
    .namespace(Namespace::try_new("retention-cadence").unwrap())
    .owner(OwnerId::from([1; 16]))
    .clock(clock.clone())
    .build()
    .unwrap();
    runtime.ensure_seeds().await.unwrap();
    let first = runtime.drive_once().await.unwrap();
    let states = store
        .load_stream_trim_states(std::slice::from_ref(&scope))
        .await
        .unwrap();
    assert_eq!(
        states[0].as_ref().unwrap().next_due_at,
        Some(now + 1),
        "{first:?}"
    );

    // A second drive at the same instant must not consume the continuation
    // early and replace it with the normal one-hour idle cadence.
    runtime.drive_once().await.unwrap();
    clock.advance(Duration::from_millis(1));
    runtime.drive_once().await.unwrap();
    let states = store.load_stream_trim_states(&[scope]).await.unwrap();
    let state = states[0].as_ref().unwrap();
    assert_eq!(
        state.updated_at,
        now + 1,
        "pending cleanup slept past its deadline"
    );
    assert!(state.next_due_at.is_some_and(|deadline| deadline > now + 1));
    for index in 0..40 {
        let response = store
            .get_item(
                serde_json::from_value(serde_json::json!({
                    "TableName": "retention-cadence", "Key": {"pk": {"S": index.to_string()}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            response.item.is_some(),
            "history cleanup removed a current item"
        );
    }
}
