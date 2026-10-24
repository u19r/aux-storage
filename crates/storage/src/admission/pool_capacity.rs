//! Qualify admission against the connections available to all work classes.

use storage_types::{StorageError, StorageResult};

use super::AdmissionConfig;

impl AdmissionConfig {
    pub(crate) fn qualify_native_capacity(
        self,
        capacity: storage_types::NativeAttemptCapacity,
    ) -> StorageResult<Self> {
        match capacity {
            storage_types::NativeAttemptCapacity::SharedPool { connections } => {
                self.qualify_pool_capacity(connections.get())
            }
            storage_types::NativeAttemptCapacity::SerializedWriter
            | storage_types::NativeAttemptCapacity::Independent => {
                Self::try_new(self).map_err(|error| StorageError::validation(error.to_string()))
            }
        }
    }

    pub(crate) fn qualify_pool_capacity(self, pool_size: usize) -> StorageResult<Self> {
        Self::try_new(self).map_err(|error| StorageError::validation(error.to_string()))?;
        let connections = std::num::NonZeroUsize::new(pool_size).ok_or_else(|| {
            StorageError::validation("native pool capacity must be greater than zero")
        })?;
        storage_types::NativeAttemptCapacity::SharedPool { connections }
            .require_foreground_attempts(
                self.control_reserve_concurrency,
                self.minimum_concurrency,
            )?;
        let foreground_capacity = pool_size - self.control_reserve_concurrency;
        let effective_maximum = self.effective_maximum().min(foreground_capacity);
        let maximum_concurrency = effective_maximum
            .checked_add(self.control_reserve_concurrency)
            .ok_or_else(|| StorageError::validation("native admission maximum overflow"))?;
        Self::try_new(Self {
            maximum_concurrency,
            ..self
        })
        .map_err(|error| StorageError::validation(error.to_string()))
    }
}
