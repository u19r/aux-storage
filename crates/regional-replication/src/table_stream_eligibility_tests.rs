//! Replication admission requires completed table-stream initialization.

use std::sync::Arc;

use storage_provider::{CatalogAttempt, DynamoOperations, DynamoPort};
use storage_types::{AttemptMode, TableName};

use crate::test_support as fixture;

#[tokio::test]
async fn given_disabled_table_stream_when_replication_is_requested_then_no_capture_or_enable_work_is_committed()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination = fixture::open(&directory.path().join("destination.sqlite")).await;
    let pair = fixture::NativeCluster::start(&[&source, &destination]).await;
    let name = TableName::new(fixture::NAME);
    fixture::create_table(&source, &name).await;
    pair.operations[0]
        .update_table(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "StreamSpecification": {"StreamEnabled": false}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    pair.operations[0].update_table(serde_json::from_value(serde_json::json!({
        "TableName": name, "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "internal_build", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }}]
    })).unwrap()).await.unwrap();
    let info = pair.operations[0]
        .get_table_info(name.clone())
        .await
        .unwrap();
    assert!(info.has_internal_stream_capture() && !info.has_enabled_stream());
    let rejected = pair.operations[0]
        .update_table(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "ReplicaUpdates": [{"Create": {"RegionName": "region-2"}}]
            }))
            .unwrap(),
        )
        .await;
    assert!(matches!(
        rejected.unwrap_err().as_ref(),
        storage_types::StorageEnum::Validation { .. }
            | storage_types::StorageEnum::RawValidation { .. }
    ));
    let journal = Arc::clone(&pair.journals[0]);
    source
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let stamp = attempt.read_catalog_stamp(&name).await?.unwrap();
                let table = attempt.read_table_at_stamp(&name, stamp).await?;
                assert!(journal.load_capture_state(attempt, &table).await?.is_none());
                assert!(journal.select_pending_enable(attempt).await?.is_none());
                Ok(())
            })
        })
        .await
        .unwrap();
    pair.shutdown().await;
}

#[tokio::test]
async fn given_active_replication_when_stream_disable_is_requested_then_configuration_stays_enabled()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination = fixture::open(&directory.path().join("destination.sqlite")).await;
    let pair = fixture::NativeCluster::start(&[&source, &destination]).await;
    pair.install(&source, fixture::NAME, &[2]).await;
    let name = TableName::new(fixture::NAME);
    let rejected = pair.operations[0]
        .update_table(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "StreamSpecification": {"StreamEnabled": false}
            }))
            .unwrap(),
        )
        .await;
    assert!(matches!(
        rejected.unwrap_err().as_ref(),
        storage_types::StorageEnum::Validation { .. }
    ));
    assert!(
        pair.operations[0]
            .get_table_info(name)
            .await
            .unwrap()
            .has_enabled_stream()
    );
    pair.shutdown().await;
}
