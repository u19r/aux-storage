use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use storage_types::{Clock, StorageError, StorageResult, TimestampMillis};

use crate::{KvCommitBoundary, KvFaultCall, KvFaultProbe, KvStore, rocksdb::RocksDbBackend};

struct InitializationFault(AtomicUsize);

impl KvFaultProbe for InitializationFault {
    fn fail(&self, _call: KvFaultCall) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: KvCommitBoundary,
    ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == KvCommitBoundary::BeforeCommit
                && self
                    .0
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                        left.checked_sub(1)
                    })
                    .is_ok()
            {
                return Err(StorageError::internal("injected initialization abort"));
            }
            Ok(())
        })
    }
}

#[derive(Default)]
struct SelectedClock(AtomicUsize);

impl Clock for SelectedClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::from_timestamp(1234)
    }

    fn sleep(
        &self,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        assert!(!duration.is_zero());
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn given_selected_clock_when_format_commits_abort_then_initialization_and_published_store_keep_that_clock()
 {
    let root = tempfile::tempdir().unwrap();
    let backend = RocksDbBackend::initialize(root.path().join("store"))
        .unwrap()
        .with_fault_probe(Arc::new(InitializationFault(AtomicUsize::new(2))));
    let clock = Arc::new(SelectedClock::default());
    let store = KvStore::initialize_format_with_clock(backend, clock.clone())
        .await
        .unwrap();
    assert_eq!(
        clock.0.load(Ordering::SeqCst),
        2,
        "initialization bypassed the selected clock"
    );
    assert_eq!(store.now(), clock.now());
    store.sleep(Duration::from_millis(1)).await.unwrap();
    assert_eq!(
        clock.0.load(Ordering::SeqCst),
        3,
        "published operations replaced the selected clock"
    );
}
