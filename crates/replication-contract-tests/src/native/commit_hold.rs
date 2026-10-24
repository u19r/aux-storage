//! Native commit suspension shared by scheduler settlement fixtures.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use futures::future::BoxFuture;
#[cfg(any(feature = "native-rocksdb", feature = "native-foundationdb"))]
use kv_backend::{KvCommitBoundary, KvFaultCall, KvFaultProbe};
#[cfg(any(
    feature = "native-sqlite",
    feature = "native-postgres",
    feature = "native-turso"
))]
use sql_backend::{SqlCommitBoundary, SqlFaultCall, SqlFaultProbe};
use storage_types::{StorageError, StorageResult};
use tokio::sync::{Notify, oneshot};

pub(super) struct BeforeCommitHold {
    is_armed: AtomicBool,
    is_ack_ordered: AtomicBool,
    entered: Notify,
    release: Mutex<Option<oneshot::Receiver<()>>>,
    confirmed: Notify,
}

impl BeforeCommitHold {
    pub(super) fn new() -> (Arc<Self>, oneshot::Sender<()>) {
        let (release, receiver) = oneshot::channel();
        (
            Arc::new(Self {
                is_armed: AtomicBool::new(false),
                is_ack_ordered: AtomicBool::new(true),
                entered: Notify::new(),
                release: Mutex::new(Some(receiver)),
                confirmed: Notify::new(),
            }),
            release,
        )
    }

    pub(super) fn arm(&self) {
        self.is_armed.store(true, Ordering::SeqCst);
    }

    #[cfg(any(feature = "native-postgres", feature = "native-foundationdb"))]
    pub(super) fn allow_independent_commits(&self) {
        self.is_ack_ordered.store(false, Ordering::SeqCst);
    }

    pub(super) async fn wait_ready(&self) {
        self.entered.notified().await;
    }

    async fn hold_commit(&self) {
        if !self.is_armed.load(Ordering::SeqCst) {
            return;
        }
        let release = self.release.lock().expect("native hold lock").take();
        if let Some(release) = release {
            self.entered.notify_one();
            let _ = release.await;
        } else if self.is_ack_ordered.load(Ordering::SeqCst) {
            // The test orders the original commit first; optimistic retries
            // can stage effects without having to win the native commit race.
            self.wait_for_ack().await;
        }
    }

    async fn wait_for_ack(&self) {
        let confirmed = self.confirmed.notified();
        tokio::pin!(confirmed);
        confirmed.as_mut().enable();
        if self.is_armed.load(Ordering::SeqCst) {
            confirmed.await;
        }
    }

    fn confirm_commit(&self) {
        if self.is_armed.swap(false, Ordering::SeqCst) {
            self.confirmed.notify_waiters();
        }
    }
}

#[cfg(any(
    feature = "native-sqlite",
    feature = "native-postgres",
    feature = "native-turso"
))]
impl SqlFaultProbe for BeforeCommitHold {
    fn fail(&self, _call: SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(&self, boundary: SqlCommitBoundary) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            match boundary {
                SqlCommitBoundary::BeforeCommit => self.hold_commit().await,
                SqlCommitBoundary::AfterDurableAcknowledgement => self.confirm_commit(),
            }
            Ok(())
        })
    }
}

#[cfg(any(feature = "native-rocksdb", feature = "native-foundationdb"))]
impl KvFaultProbe for BeforeCommitHold {
    fn fail(&self, _call: KvFaultCall) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(&self, boundary: KvCommitBoundary) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            match boundary {
                KvCommitBoundary::BeforeCommit => self.hold_commit().await,
                KvCommitBoundary::AfterDurableAcknowledgement => self.confirm_commit(),
            }
            Ok(())
        })
    }
}
