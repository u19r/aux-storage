use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use scheduled_bg_jobs::store::CommitOutcome;
use storage_types::{StorageError, StorageResult};

use super::{
    CommitExecution, JobCommitError,
    settlement::{NativeJobWait, wait_native_job},
};
use crate::RawOrderedJobStoreError;

/// Carries contract failures out of a native attempt while still forcing its
/// rollback through the native `StorageResult` boundary.
#[derive(Clone)]
pub struct ContractErrorSlot(Arc<Mutex<Option<RawOrderedJobStoreError>>>);

impl ContractErrorSlot {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    pub fn preserve(
        &self,
        result: Result<CommitExecution, JobCommitError>,
    ) -> StorageResult<CommitExecution> {
        match result {
            Ok(execution) => Ok(execution),
            Err(JobCommitError::Storage(error)) => Err(error),
            Err(JobCommitError::Contract(error)) => {
                *self
                    .0
                    .lock()
                    .map_err(|_| StorageError::internal("job commit error slot is poisoned"))? =
                    Some(error);
                Err(StorageError::internal("scheduled-job commit rejected"))
            }
        }
    }

    fn take(&self) -> StorageResult<Option<RawOrderedJobStoreError>> {
        Ok(self
            .0
            .lock()
            .map_err(|_| StorageError::internal("job commit error slot is poisoned"))?
            .take())
    }
}

/// Wait for one native attempt; cancellation after dispatch stays unknown.
pub async fn run_native_job_attempt<F, Fut>(
    remaining: Duration,
    admission: Option<Arc<dyn crate::native_admission::NativeAdmission>>,
    operation: F,
) -> Result<CommitOutcome, RawOrderedJobStoreError>
where
    F: FnOnce(ContractErrorSlot) -> Fut + Send + 'static,
    Fut: Future<Output = StorageResult<CommitExecution>> + Send + 'static,
{
    let slot = ContractErrorSlot::new();
    let native_slot = slot.clone();
    let result = match wait_native_job(remaining, admission, move || operation(native_slot)).await?
    {
        NativeJobWait::Settled(result) => result,
        NativeJobWait::Pending => return Ok(CommitOutcome::Unknown),
    };
    // A remembered preparation/contract failure cannot acknowledge native
    // abort. Preserve stronger native settlement before returning that
    // remembered cause.
    let result = match result {
        Err(error)
            if error.is_commit_unknown()
                || error.native_attempt_rejection()
                    == Some(storage_types::NativeAttemptRejection::Capacity) =>
        {
            return Err(error.into());
        }
        result => result,
    };
    if let Some(error) = slot.take().map_err(RawOrderedJobStoreError::Backend)? {
        return Err(error);
    }
    match result {
        Ok(CommitExecution::Replay(outcome) | CommitExecution::Write(outcome)) => Ok(outcome),
        Err(error) => Err(error.into()),
    }
}
