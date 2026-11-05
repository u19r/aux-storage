//! Native failures cannot commit history deletion without table-policy
//! progress.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use storage_types::{StorageError, TimestampMillis};

use super::{
    dynamodb_stream_history_tests::{create_stream_table, put_value},
    image_retention_tests::table_stream,
    table_history_retention_tests::{change_duration, cleanup_page, history_count, item_history},
    test_support::TestStore,
};

#[derive(Default)]
struct CleanupAbortProbe {
    armed: AtomicBool,
    staged: AtomicBool,
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
impl sql_backend::SqlFaultProbe for CleanupAbortProbe {
    fn fail(&self, call: sql_backend::SqlFaultCall<'_>) -> Option<StorageError> {
        if call.operation == sql_backend::SqlFaultOperation::Execute
            && call.sql.starts_with("DELETE FROM")
            && call.sql.contains("sys_stream_trim_due_markers")
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.staged.store(true, Ordering::SeqCst);
            return Some(StorageError::internal(
                "injected cleanup completion failure",
            ));
        }
        None
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for CleanupAbortProbe {
    fn fail(&self, call: kv_backend::KvFaultCall) -> Option<StorageError> {
        if call.operation == kv_backend::KvFaultOperation::Stage
            && self.armed.load(Ordering::SeqCst)
            && self.staged.swap(true, Ordering::SeqCst)
            && self.armed.swap(false, Ordering::SeqCst)
        {
            return Some(StorageError::internal(
                "injected cleanup staged-write failure",
            ));
        }
        None
    }
}

async fn verify_cleanup_abort(store: TestStore, probe: &CleanupAbortProbe) -> anyhow::Result<()> {
    let outcome = async {
        let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        change_duration(&store, &table, 24).await?;
        for value in ["v1", "v2", "v3"] {
            put_value(&store, &table, "item", value).await;
        }
        let history = item_history(&store, &table).await?;
        let pointers = table_stream(&store, &table).await?;
        store.advance_time(Duration::from_secs(25 * 3600));
        let repository = &store.stream_retention_repository;
        let horizon = TimestampMillis::from_timestamp(i64::MAX);
        let markers = repository
            .list_due_stream_trim_markers(horizon, 100)
            .await?;
        let scopes = markers
            .iter()
            .map(|marker| marker.scope.clone())
            .collect::<Vec<_>>();
        let states = repository.load_stream_trim_states(&scopes).await?;
        probe.armed.store(true, Ordering::SeqCst);
        anyhow::ensure!(
            cleanup_page(&store, 100).await.is_err(),
            "injected native failure was hidden"
        );
        anyhow::ensure!(probe.staged.load(Ordering::SeqCst) && !probe.armed.load(Ordering::SeqCst));
        anyhow::ensure!(history_count(&store, &history).await? == 3);
        anyhow::ensure!(history_count(&store, &pointers).await? == 3);
        anyhow::ensure!(repository.load_stream_trim_states(&scopes).await? == states);
        anyhow::ensure!(
            repository
                .list_due_stream_trim_markers(horizon, 100)
                .await?
                == markers
        );
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 1);
        store.advance_time(Duration::from_millis(1));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 4);
        anyhow::ensure!(history_count(&store, &history).await? == 1);
        anyhow::ensure!(history_count(&store, &pointers).await? == 0);
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
#[tokio::test]
async fn given_sql_history_deletes_when_marker_completion_fails_then_native_attempt_rolls_back_and_retry_finishes()
-> anyhow::Result<()> {
    let probe = Arc::new(CleanupAbortProbe::default());
    let store = TestStore::new_sql_with_fault_probe(probe.clone()).await;
    verify_cleanup_abort(store, &probe).await
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
#[tokio::test]
async fn given_kv_history_deletes_when_native_write_fails_then_rows_and_progress_roll_back_and_retry_finishes()
-> anyhow::Result<()> {
    let probe = Arc::new(CleanupAbortProbe::default());
    let store = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    verify_cleanup_abort(store, &probe).await
}
