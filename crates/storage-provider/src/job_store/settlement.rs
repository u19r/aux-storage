use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use storage_types::{StorageError, StorageResult};

use crate::{
    RawOrderedJobStoreError,
    native_admission::{NativeAdmission, NativeAdmissionOutcome, NativeWork},
};

pub(super) enum NativeJobWait<R> {
    Settled(StorageResult<R>),
    Pending,
}

/// Admission and native settlement stay owned after the requesting caller
/// leaves.
pub(super) async fn wait_native_job<R, F, Fut>(
    remaining: Duration,
    admission: Option<Arc<dyn NativeAdmission>>,
    operation: F,
) -> Result<NativeJobWait<R>, RawOrderedJobStoreError>
where
    R: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = StorageResult<R>> + Send + 'static,
{
    use tracing::{Instrument as _, instrument::WithSubscriber as _};
    let deadline = tokio::time::Instant::now()
        .checked_add(remaining)
        .ok_or_else(|| StorageError::internal("job admission deadline overflowed"))?;
    let permit = match admission {
        Some(admission) => Some(
            tokio::time::timeout_at(deadline, admission.acquire(NativeWork::Recovery))
                .await
                .map_err(|_| RawOrderedJobStoreError::Timeout)??,
        ),
        None => None,
    };
    if tokio::time::Instant::now() >= deadline {
        return Err(RawOrderedJobStoreError::Timeout);
    }
    let settlement = async move {
        let started = Instant::now();
        let result = operation().await;
        if let Some(permit) = permit {
            permit.complete(
                NativeAdmissionOutcome {
                    error: result.as_ref().err(),
                    timed_out: tokio::time::Instant::now() >= deadline,
                },
                started.elapsed(),
            );
        }
        result
    };
    let settlement = tokio::spawn(
        metrics_facade::inherit_request_cost(settlement)
            .instrument(tracing::Span::current())
            .with_current_subscriber(),
    );
    Ok(match tokio::time::timeout_at(deadline, settlement).await {
        Ok(Ok(result)) => NativeJobWait::Settled(result),
        Ok(Err(_)) | Err(_) => NativeJobWait::Pending,
    })
}

/// Reads have no commit identity; a detached settlement is an explicit timeout.
pub async fn run_native_job_read<R, F, Fut>(
    remaining: Duration,
    admission: Option<Arc<dyn NativeAdmission>>,
    operation: F,
) -> Result<R, RawOrderedJobStoreError>
where
    R: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = StorageResult<R>> + Send + 'static,
{
    match wait_native_job(remaining, admission, operation).await? {
        NativeJobWait::Settled(result) => result.map_err(Into::into),
        NativeJobWait::Pending => Err(RawOrderedJobStoreError::Timeout),
    }
}
