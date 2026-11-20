//! The test seam of a native attempt's commit. Production attempts carry no
//! observer; a backend's `fault-injection` feature installs one.

use futures::future::BoxFuture;
use storage_types::{AttemptMode, StorageError, StorageResult};

use crate::native_outcome::{NativeCommitError, classify_commit};

/// The two points of an attempt's commit that an observer can hold or fail.
/// Read and write attempts both reach them; the observer receives the
/// attempt's `AttemptMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitBoundary {
    /// After the attempt's last native call and before
    /// `AttemptLifecycle::submit_commit`.
    BeforeCommit,
    /// After the COMMIT answered success and the driver's durability wait
    /// ended.
    AfterDurableAcknowledgement,
}

/// Watches the commits of every attempt of one store. `Send + Sync`: worker
/// threads and the supervisor task share it.
pub trait NativeObserver: Send + Sync {
    /// Holds the commit of an attempt of `mode` at `boundary` until the
    /// future ends; an `Err` fails the attempt there.
    fn at_commit_boundary(
        &self,
        boundary: CommitBoundary,
        mode: AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>>;
}

/// Waits at `boundary`. Call it only inside
/// `if let Some(observer) = lifecycle.observer()`, so an attempt without an
/// observer pays one `Option` test.
///
/// A failure before the commit is a known conflict: no COMMIT was issued, so
/// the retry owner tries again. A failure after the acknowledgement withholds
/// a submitted commit's answer: `classify_commit` makes a write's an unknown
/// outcome, which is never retried, and a read's a known failure, since a read
/// commits nothing.
pub async fn observe_commit_boundary(
    observer: &dyn NativeObserver,
    boundary: CommitBoundary,
    mode: AttemptMode,
) -> StorageResult<()> {
    crate::writer_occupancy::exempt_wait(observer.at_commit_boundary(boundary, mode))
        .await
        .map_err(|_| match boundary {
            CommitBoundary::BeforeCommit => StorageError::logical_conflict(
                "native commit was rejected by its observer before submission",
            ),
            CommitBoundary::AfterDurableAcknowledgement => classify_commit(
                mode != AttemptMode::ReadOnly,
                NativeCommitError::Unproven {
                    reason: "durable commit answer was withheld by its observer",
                    cause: None,
                },
            ),
        })
}
