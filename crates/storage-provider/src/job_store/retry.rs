use std::{
    future::Future,
    time::{Duration, Instant},
};

use scheduled_bg_jobs::store::CommitOutcome;
use storage_types::{StorageEnum, StorageError, context::WrappedError as _};

use crate::RawOrderedJobStoreError;

/// Retry only native attempts known to have aborted. An ambiguous settlement
/// stays unknown so the caller can resolve it through the durable receipt.
pub async fn settle_native_commit<F, Fut>(
    duration: Duration,
    mut attempt: F,
) -> Result<CommitOutcome, RawOrderedJobStoreError>
where
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = Result<CommitOutcome, RawOrderedJobStoreError>>,
{
    let deadline = Instant::now()
        .checked_add(duration)
        .ok_or_else(|| StorageError::validation("scheduled-job duration cannot form a deadline"))?;
    for _ in 0..8 {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(RawOrderedJobStoreError::Timeout);
        };
        match attempt(remaining).await {
            Err(RawOrderedJobStoreError::Backend(error))
                if matches!(error.to_enum(), StorageEnum::TransactionConflict { .. }) => {}
            Err(RawOrderedJobStoreError::Backend(error))
                if matches!(error.to_enum(), StorageEnum::SyncSubmissionAmbiguous { .. }) =>
            {
                return Ok(CommitOutcome::Unknown);
            }
            result => return result,
        }
    }
    Err(StorageError::service_unavailable(1).into())
}
