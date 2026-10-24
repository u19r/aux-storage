//! Isolated native fixtures own finite slots; assembled host admission is
//! proved elsewhere.
use std::{future::Future, num::NonZeroUsize, pin::Pin, sync::Arc, time::Duration};

use storage_provider::native_admission::{
    NativeAdmission, NativeAdmissionOutcome, NativeAdmissionPermit, NativeWork,
};
use storage_types::{StorageError, StorageResult};

pub(super) struct FixtureAdmission {
    foreground: Arc<tokio::sync::Semaphore>,
    recovery: Arc<tokio::sync::Semaphore>,
}

impl Default for FixtureAdmission {
    fn default() -> Self {
        Self {
            foreground: Arc::new(tokio::sync::Semaphore::new(2)),
            recovery: Arc::new(tokio::sync::Semaphore::new(2)),
        }
    }
}

struct FixturePermit(tokio::sync::OwnedSemaphorePermit);

impl NativeAdmissionPermit for FixturePermit {
    fn complete(self: Box<Self>, _: NativeAdmissionOutcome<'_>, _: Duration) {
        drop(self.0);
    }
}

impl NativeAdmission for FixtureAdmission {
    fn connection_id(&self) -> &str {
        "isolated-native-fixture"
    }

    fn require_reserved_attempts(&self, required: NonZeroUsize) -> StorageResult<()> {
        if required.get() > 2 {
            return Err(StorageError::unsupported(
                "fixture recovery reserve exceeded",
            ));
        }
        Ok(())
    }

    fn acquire(
        &self,
        work: NativeWork,
    ) -> Pin<Box<dyn Future<Output = StorageResult<Box<dyn NativeAdmissionPermit>>> + Send + '_>>
    {
        Box::pin(async move {
            let slots = if matches!(work, NativeWork::Recovery) {
                &self.recovery
            } else {
                &self.foreground
            };
            let permit = if matches!(work, NativeWork::Background) {
                Arc::clone(slots)
                    .try_acquire_owned()
                    .map_err(|_| StorageError::service_unavailable(1))?
            } else {
                Arc::clone(slots)
                    .acquire_owned()
                    .await
                    .map_err(|_| StorageError::internal("fixture admission stopped"))?
            };
            Ok(Box::new(FixturePermit(permit)) as Box<dyn NativeAdmissionPermit>)
        })
    }

    fn drain_native(&self) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async {
            let _foreground = self
                .foreground
                .acquire_many(2)
                .await
                .map_err(|_| StorageError::internal("fixture admission stopped"))?;
            let _recovery = self
                .recovery
                .acquire_many(2)
                .await
                .map_err(|_| StorageError::internal("fixture admission stopped"))?;
            Ok(())
        })
    }
}
