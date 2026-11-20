//! Native aborts retry; unknown acknowledgements preserve committed
//! publication.

use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};

use futures::future::BoxFuture;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
use sql_backend::{SqlCommitBoundary, SqlFaultProbe};
use storage_types::{StorageError, StorageResult};

use super::{
    table_stream_eligibility_tests::set_stream,
    test_support::{TestStore, hash_table_request, unique_table_name},
};

#[derive(Default)]
struct PublicationFailure {
    point: AtomicU8,
    fired: AtomicUsize,
}

impl PublicationFailure {
    fn fire(&self, point: u8) -> bool {
        if self
            .point
            .compare_exchange(point, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.fired.fetch_add(1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
impl SqlFaultProbe for PublicationFailure {
    fn fail(&self, _: sql_backend::SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        let point = match boundary {
            SqlCommitBoundary::BeforeCommit => 1,
            SqlCommitBoundary::AfterDurableAcknowledgement => 2,
        };
        let failed = self.fire(point);
        Box::pin(async move {
            if failed {
                Err(StorageError::internal(
                    "injected publication acknowledgement failure",
                ))
            } else {
                Ok(())
            }
        })
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for PublicationFailure {
    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        let point = match boundary {
            kv_backend::KvCommitBoundary::BeforeCommit => 1,
            kv_backend::KvCommitBoundary::AfterDurableAcknowledgement => 2,
        };
        let failed = self.fire(point);
        Box::pin(async move {
            if failed {
                Err(StorageError::internal(
                    "injected publication acknowledgement failure",
                ))
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn given_native_abort_or_lost_ack_when_enabling_then_only_real_mutations_are_published()
-> anyhow::Result<()> {
    for point in [1, 2] {
        let probe = Arc::new(PublicationFailure::default());
        let store = TestStore::new_with_native_probes(
            #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
            probe.clone(),
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            probe.clone(),
        )
        .await;
        let table = unique_table_name();
        store
            .operations
            .create_table(hash_table_request(table.clone()))
            .await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "initial").await;
        probe.point.store(point, Ordering::SeqCst);
        let enabled = set_stream(&store, &table, true).await;
        if point == 2 {
            anyhow::ensure!(
                enabled
                    .unwrap_err()
                    .downcast_ref::<StorageError>()
                    .is_some_and(StorageError::is_commit_unknown)
            );
        } else {
            enabled?;
        }
        anyhow::ensure!(probe.fired.load(Ordering::SeqCst) == 1);
        anyhow::ensure!(
            store
                .operations
                .get_table_info(table.clone())
                .await?
                .has_enabled_stream()
        );
        anyhow::ensure!(
            super::image_retention_tests::table_records(&store, &table)
                .await?
                .is_empty()
        );
        anyhow::ensure!(
            super::cdc_stream_tests::system_records(&store, 100)
                .await?
                .is_empty()
        );
        probe.point.store(point, Ordering::SeqCst);
        let written = store
            .operations
            .put_item(
                storage_types::PutItemRequest::try_from(serde_json::json!({
                    "TableName": table, "Item": {"pk": {"S": "one"}, "value": {"S": "after"}}
                }))
                .map_err(anyhow::Error::msg)?,
            )
            .await;
        if point == 2 {
            anyhow::ensure!(written.unwrap_err().is_commit_unknown());
        } else {
            written?;
        }
        anyhow::ensure!(probe.fired.load(Ordering::SeqCst) == 2);
        store.wait_until_changes_are_readable().await;
        let arn = store
            .operations
            .describe_table(storage_types::DescribeTableRequest {
                table_name: table.clone(),
            })
            .await?
            .table
            .latest_stream_arn
            .unwrap();
        let records = super::table_history_retention_tests::read_standard(&store, &arn).await?;
        anyhow::ensure!(records.len() == 1 && records[0].event_name == "MODIFY");
        anyhow::ensure!(
            records[0].dynamodb.old_image
                == Some(serde_json::from_value(
                    serde_json::json!({"pk": {"S": "one"}, "value": {"S": "initial"}})
                )?)
        );
        store.finish().await;
    }
    Ok(())
}
