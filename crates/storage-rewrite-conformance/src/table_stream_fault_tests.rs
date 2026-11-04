//! Native aborts and unknown enable acknowledgements preserve resumable
//! capture.

use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};

use futures::future::BoxFuture;
use sql_backend::{SqlCommitBoundary, SqlFaultCall, SqlFaultOperation, SqlFaultProbe};
use storage_types::{BoundTableInfo, StorageError, StorageResult, TableHistoryId};
use stream_provider::StreamRowKind;

use super::{
    table_stream_eligibility_tests::{finish_initialization, set_stream},
    test_support::{TestStore, hash_table_request, unique_table_name},
};

#[derive(Default)]
struct InitializationFailure {
    point: AtomicU8,
    fired: AtomicUsize,
}

impl InitializationFailure {
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

impl SqlFaultProbe for InitializationFailure {
    fn fail(&self, call: SqlFaultCall<'_>) -> Option<StorageError> {
        (call.operation == SqlFaultOperation::Execute
            && call.sql.starts_with("INSERT INTO sys_dynamo_stream_items")
            && self.fire(3))
        .then(|| StorageError::internal("injected initialization anchor failure"))
    }

    fn at_commit_boundary(&self, boundary: SqlCommitBoundary) -> BoxFuture<'_, StorageResult<()>> {
        let point = match boundary {
            SqlCommitBoundary::BeforeCommit => 1,
            SqlCommitBoundary::AfterDurableAcknowledgement => 2,
        };
        let failed = self.fire(point);
        Box::pin(async move {
            if failed {
                Err(StorageError::internal(
                    "injected initialization acknowledgement failure",
                ))
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn given_native_abort_or_lost_ack_when_enable_resumes_then_initial_values_are_captured_once()
-> anyhow::Result<()> {
    for point in [1, 2, 3] {
        let probe = Arc::new(InitializationFailure::default());
        let store = TestStore::new_sql_with_fault_probe(probe.clone()).await;
        let table = unique_table_name();
        let created = store
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
        if point == 3 {
            anyhow::ensure!(
                store
                    .stream_initialization_repository
                    .advance_table_stream_initialization()
                    .await
                    .is_err()
            );
            anyhow::ensure!(
                !store
                    .operations
                    .describe_table(storage_types::DescribeTableRequest {
                        table_name: table.clone()
                    })
                    .await?
                    .table
                    .stream_specification
                    .unwrap()
                    .stream_enabled
            );
        }
        finish_initialization(&store, &table).await?;
        anyhow::ensure!(
            probe.fired.load(Ordering::SeqCst) == 1,
            "native fault did not fire once"
        );
        let bound = BoundTableInfo {
            history_id: TableHistoryId::from(
                *uuid::Uuid::parse_str(&created.table_description.table_id)?.as_bytes(),
            ),
            info: store.operations.get_table_info(table.clone()).await?,
        };
        let key = serde_json::from_value(serde_json::json!({"pk": {"S": "one"}}))?;
        let address = stream_provider::dynamo_item_history_address(&bound, &key)?;
        let rows = store
            .stream_operations
            .read_forward(
                StreamRowKind::Item(address.physical_stream_name()?),
                None,
                100,
            )
            .await?;
        anyhow::ensure!(
            rows.items.len() == 1,
            "resumption duplicated or omitted the current image"
        );
        anyhow::ensure!(
            super::cdc_stream_tests::system_records(&store, 100)
                .await?
                .is_empty()
        );
        store.finish().await;
    }
    Ok(())
}
