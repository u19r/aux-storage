use std::{future::Future, num::NonZeroUsize, pin::Pin, time::Duration};

use storage_types::{StorageError, StorageResult};

/// Physical scheduling reported by the exact initialized store used by admitted
/// work.
pub trait NativeAttemptSource: Send + Sync {
    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity>;
    /// Bind enforced work admission before publishing owners on this exact
    /// store.
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()>;
}

/// Completion evidence from the owner of one admitted native attempt.
pub struct NativeAdmissionOutcome<'a> {
    pub error: Option<&'a StorageError>,
    pub timed_out: bool,
}

pub trait NativeAdmissionPermit: Send {
    fn complete(self: Box<Self>, outcome: NativeAdmissionOutcome<'_>, elapsed: Duration);
}

pub type NativeAdmissionFuture<'a> =
    Pin<Box<dyn Future<Output = StorageResult<Box<dyn NativeAdmissionPermit>>> + Send + 'a>>;

#[derive(Clone, Copy)]
pub enum NativeWork {
    Recovery,
    RangeRead,
    Write,
    Background,
}

/// Native work uses foreground lanes, discretionary free slots, or the recovery
/// reserve.
pub trait NativeAdmission: Send + Sync {
    /// Host connection identity used to select this controller and its job
    /// store.
    fn connection_id(&self) -> &str;

    /// Qualify required recovery slots on this exact host connection. Unbounded
    /// ordinary scheduling does not establish reserved regional capacity.
    fn require_reserved_attempts(&self, required: NonZeroUsize) -> StorageResult<()>;

    /// Background work requires a free foreground slot and never joins its
    /// queue. The host's queue wait bounds other admission before a native
    /// attempt starts. Reserve exhaustion is retryable unavailability, not
    /// a commit outcome.
    fn acquire(&self, work: NativeWork) -> NativeAdmissionFuture<'_>;

    /// The host stops intake and its job driver before awaiting settlement in
    /// both foreground lanes and the reserved recovery lane.
    fn drain_native(&self) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>>;
}

/// Shared by assembled owners before the host publishes its qualified
/// controller.
#[derive(Default)]
pub struct NativeAdmissionBinding {
    admission: std::sync::OnceLock<std::sync::Arc<dyn NativeAdmission>>,
}

impl NativeAdmissionBinding {
    pub fn bind(&self, admission: std::sync::Arc<dyn NativeAdmission>) -> StorageResult<()> {
        self.admission
            .set(admission)
            .map_err(|_| StorageError::internal("native admission is already bound"))
    }

    pub fn require(&self) -> StorageResult<std::sync::Arc<dyn NativeAdmission>> {
        self.admission
            .get()
            .cloned()
            .ok_or_else(|| StorageError::internal("native admission is not bound"))
    }

    pub async fn settle<R, F, Fut>(&self, work: NativeWork, operation: F) -> StorageResult<R>
    where
        R: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = StorageResult<R>> + Send + 'static,
    {
        let permit = self.require()?.acquire(work).await?;
        settle_native_attempt(permit, async move { operation().await }).await
    }
}

/// Retain native admission through provider settlement even if the response
/// waiter is dropped.
pub async fn settle_native_attempt<R, Fut>(
    permit: Box<dyn NativeAdmissionPermit>,
    operation: Fut,
) -> StorageResult<R>
where
    R: Send + 'static,
    Fut: Future<Output = StorageResult<R>> + Send + 'static,
{
    use tracing::{Instrument as _, instrument::WithSubscriber as _};
    let settlement = async move {
        let started = std::time::Instant::now();
        let result = operation.await;
        permit.complete(
            NativeAdmissionOutcome {
                error: result.as_ref().err(),
                timed_out: false,
            },
            started.elapsed(),
        );
        result
    };
    match tokio::spawn(
        metrics_facade::inherit_request_cost(settlement)
            .instrument(tracing::Span::current())
            .with_current_subscriber(),
    )
    .await
    {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(_) => Err(StorageError::internal("admitted native attempt stopped")),
    }
}
