//! Publishes staged queue wakes after the native outcome of one attempt.

use std::{
    future::Future,
    sync::{Arc, Mutex, PoisonError},
};

use queue_provider::{QueueWakeBoard, QueueWakeSlot};
use storage_types::StorageResult;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
use storage_types::{StorageEnum, StorageError};

/// Slots the attempt body staged; the body stores them before it returns Ok.
pub(crate) type StagedWakes = Arc<Mutex<Vec<QueueWakeSlot>>>;

pub(crate) fn store_staged(staged: &StagedWakes, slots: Vec<QueueWakeSlot>) {
    *staged.lock().unwrap_or_else(PoisonError::into_inner) = slots;
}

/// Runs `attempt` and publishes the staged slots on the board after the
/// native outcome: on Ok and on commit-unknown (an extra advance is only a
/// probe). Every SQL board records commits, so with a runtime the attempt and
/// the publication run in one spawned task: a dropped caller still publishes.
pub(crate) async fn publish_after<R: Send + 'static>(
    board: Option<Arc<QueueWakeBoard>>,
    staged: StagedWakes,
    attempt: impl Future<Output = StorageResult<R>> + Send + 'static,
) -> StorageResult<R> {
    let Some(board) = board else {
        return attempt.await;
    };
    let run = async move {
        let result = attempt.await;
        let publish = match &result {
            Ok(_) => true,
            Err(error) => error.is_commit_unknown(),
        };
        if publish {
            let slots = std::mem::take(&mut *staged.lock().unwrap_or_else(PoisonError::into_inner));
            board.committed(&slots);
        }
        result
    };
    run_detached(run).await
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
async fn run_detached<R: Send + 'static>(
    run: impl Future<Output = StorageResult<R>> + Send + 'static,
) -> StorageResult<R> {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return run.await;
    };
    match handle.spawn(run).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(_) => Err(StorageError::from(StorageEnum::InternalServerError {
            message: "queue attempt task was cancelled".to_owned(),
        })),
    }
}

#[cfg(not(any(feature = "sqlite", feature = "turso", feature = "postgres")))]
async fn run_detached<R: Send + 'static>(
    run: impl Future<Output = StorageResult<R>> + Send + 'static,
) -> StorageResult<R> {
    run.await
}
