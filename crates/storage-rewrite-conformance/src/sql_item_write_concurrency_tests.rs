//! PostgreSQL item locks fence batched observations across competing attempts.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures::future::BoxFuture;
use serde_json::json;
use sql_backend::{SqlCommitBoundary, SqlFaultCall, SqlFaultProbe};
use storage_types::{BatchWriteItemRequest, StorageError, StorageResult};
use tokio::sync::Notify;

use super::{
    gsi_workflow_tests::{group_keys, put_group, table_request},
    test_support::TestStore,
};

#[derive(Default)]
struct HeldCommit {
    hold_next: AtomicBool,
    observe_lock: AtomicBool,
    prepared: Notify,
    lock_requested: Notify,
    release: Notify,
}

impl SqlFaultProbe for HeldCommit {
    fn fail(&self, call: SqlFaultCall<'_>) -> Option<StorageError> {
        if self.observe_lock.load(Ordering::SeqCst) && call.sql.contains("pg_advisory_xact_lock") {
            self.lock_requested.notify_one();
        }
        None
    }

    fn at_commit_boundary(&self, boundary: SqlCommitBoundary) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == SqlCommitBoundary::BeforeCommit
                && self.hold_next.swap(false, Ordering::SeqCst)
            {
                self.prepared.notify_one();
                tokio::time::timeout(std::time::Duration::from_secs(20), self.release.notified())
                    .await
                    .expect("competing observation must reach the item lock");
            }
            Ok(())
        })
    }
}

fn batch(group: &str) -> BatchWriteItemRequest {
    BatchWriteItemRequest::try_from(json!({
        "RequestItems": {"sql_batch_conflict": [{"PutRequest": {"Item": {
            "pk": {"S": "hot"}, "gsi_pk": {"S": group}, "gsi_sk": {"S": "hot"}
        }}}]}
    }))
    .unwrap()
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_held_batch_commit_when_second_writer_waits_then_observations_refresh_after_the_item_lock()
-> anyhow::Result<()> {
    assert_eq!(
        std::env::var("STORAGE_TEST_BACKEND").as_deref(),
        Ok("postgres")
    );
    let probe = Arc::new(HeldCommit::default());
    let fixture = TestStore::new_sql_with_fault_probe(probe.clone()).await;
    let outcome = async {
        fixture
            .operations
            .create_table(table_request("sql_batch_conflict"))
            .await?;
        put_group(&fixture, "sql_batch_conflict", "hot", "seed").await?;
        probe.hold_next.store(true, Ordering::SeqCst);
        let operations = fixture.operations.clone();
        let first = tokio::spawn(async move { operations.batch_write_item(batch("first")).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            probe.prepared.notified(),
        )
        .await?;
        probe.observe_lock.store(true, Ordering::SeqCst);
        let operations = fixture.operations.clone();
        let second =
            tokio::spawn(async move { operations.batch_write_item(batch("second")).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            probe.lock_requested.notified(),
        )
        .await?;
        probe.release.notify_one();
        for response in [first.await??, second.await??] {
            anyhow::ensure!(
                response
                    .unprocessed_items
                    .as_ref()
                    .is_none_or(|items| items.values().all(Vec::is_empty))
            );
        }
        anyhow::ensure!(
            group_keys(&fixture, "sql_batch_conflict", "seed")
                .await?
                .is_empty()
        );
        anyhow::ensure!(
            group_keys(&fixture, "sql_batch_conflict", "first")
                .await?
                .is_empty()
        );
        anyhow::ensure!(group_keys(&fixture, "sql_batch_conflict", "second").await? == ["hot"]);
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
