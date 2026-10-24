//! One host connection owns foreground and scheduled native admission.
use std::{future::Future, num::NonZeroUsize, pin::Pin, sync::Arc};

use storage_provider::native_admission::{NativeAdmission, NativeAdmissionPermit, NativeWork};
use storage_types::{StorageError, StorageResult};

use super::{AdmissionConfig, AdmissionController};

struct ConnectionNativeAdmission {
    controller: AdmissionController,
}

impl AdmissionConfig {
    pub(crate) fn require_reserved_attempts(self, required: NonZeroUsize) -> StorageResult<()> {
        Self::try_new(self).map_err(|error| StorageError::validation(error.to_string()))?;
        if !self.enabled {
            return Err(StorageError::validation(
                "regional recovery requires enabled admission with reserved native concurrency",
            ));
        }
        if self.control_reserve_concurrency < required.get() {
            return Err(StorageError::validation(
                "regional native capacity exceeds this connection's recovery reserve",
            ));
        }
        Ok(())
    }
}

impl AdmissionController {
    pub(crate) fn native_admission(&self) -> Arc<dyn NativeAdmission> {
        Arc::new(ConnectionNativeAdmission {
            controller: self.clone(),
        })
    }
}

impl NativeAdmission for ConnectionNativeAdmission {
    fn connection_id(&self) -> &str {
        self.controller.inner.connection_id.as_ref()
    }

    fn require_reserved_attempts(&self, required: NonZeroUsize) -> StorageResult<()> {
        self.controller.config().require_reserved_attempts(required)
    }

    fn drain_native(&self) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async {
            self.controller.wait_for_native_idle().await;
            Ok(())
        })
    }

    fn acquire(
        &self,
        work: NativeWork,
    ) -> Pin<Box<dyn Future<Output = StorageResult<Box<dyn NativeAdmissionPermit>>> + Send + '_>>
    {
        Box::pin(self.controller.acquire_native_permit(work))
    }
}
