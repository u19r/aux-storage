use std::{future::Future, time::Duration};

use storage_provider::native_admission::{
    NativeAdmissionOutcome, NativeAdmissionPermit, NativeWork, settle_native_attempt,
};
use storage_types::{StorageError, StorageResult};

use super::{
    AdmissionClass, AdmissionController, AdmissionOutcome, AdmissionPermit, ControlPermit,
    is_admission_pressure,
};

struct ControlSettlementPermit {
    controller: AdmissionController,
    _permit: ControlPermit,
}

impl NativeAdmissionPermit for AdmissionPermit {
    fn complete(self: Box<Self>, outcome: NativeAdmissionOutcome<'_>, elapsed: Duration) {
        (*self).complete(AdmissionOutcome::from_storage_error(
            outcome.error,
            elapsed,
            outcome.timed_out,
        ));
    }
}

impl NativeAdmissionPermit for ControlSettlementPermit {
    fn complete(self: Box<Self>, outcome: NativeAdmissionOutcome<'_>, _elapsed: Duration) {
        self.controller.record_control_pressure(
            outcome.timed_out,
            outcome.error.is_some_and(is_admission_pressure),
        );
    }
}

impl AdmissionController {
    pub(super) async fn acquire_native_permit(
        &self,
        work: NativeWork,
    ) -> StorageResult<Box<dyn NativeAdmissionPermit>> {
        match work {
            NativeWork::Recovery if self.config().control_reserve_concurrency > 0 => {
                let permit = self.acquire_recovery().await.map_err(|error| {
                    StorageError::service_unavailable(error.retry_after_seconds)
                })?;
                Ok(Box::new(ControlSettlementPermit {
                    controller: self.clone(),
                    _permit: permit,
                }))
            }
            NativeWork::Background => {
                let permit = self.try_acquire(AdmissionClass::Write).map_err(|error| {
                    StorageError::service_unavailable(error.retry_after_seconds)
                })?;
                Ok(Box::new(permit))
            }
            NativeWork::Recovery | NativeWork::RangeRead | NativeWork::Write => {
                let class = match work {
                    NativeWork::RangeRead => AdmissionClass::RangeRead,
                    _ => AdmissionClass::Write,
                };
                let permit = self.acquire(class).await.map_err(|error| {
                    StorageError::service_unavailable(error.retry_after_seconds)
                })?;
                Ok(Box::new(permit))
            }
        }
    }

    pub(crate) async fn settle_foreground<F, Fut, T>(
        &self,
        class: AdmissionClass,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = StorageResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let permit = self
            .acquire(class)
            .await
            .map_err(|error| StorageError::service_unavailable(error.retry_after_seconds))?;
        settle_native_attempt(Box::new(permit), operation()).await
    }

    pub(crate) async fn settle_native<F, Fut, T>(
        &self,
        work: NativeWork,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = StorageResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let permit = self.acquire_native_permit(work).await?;
        settle_native_attempt(permit, operation()).await
    }
}
