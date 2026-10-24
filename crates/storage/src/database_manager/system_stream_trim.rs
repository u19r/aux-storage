use storage_provider::SystemStreamTrimExecutor;
use storage_types::{StorageError, StorageResult, SystemStreamScope, TimestampMillis};
use stream_provider::{SystemStreamTrimContinuation, SystemStreamTrimPageResult};

use super::DatabaseManager;
use crate::admission::AdmissionClass;

impl DatabaseManager {
    /// Run one logical SystemStream reclaim page for the internal conformance
    /// harness.
    #[doc(hidden)]
    pub async fn run_system_stream_trim_page(
        &self,
        scope: SystemStreamScope,
        cutoff_timestamp: TimestampMillis,
        page_limit: usize,
        continuation: Option<SystemStreamTrimContinuation>,
    ) -> StorageResult<SystemStreamTrimPageResult> {
        let repository = self.system_stream_trim_repository.clone().ok_or_else(|| {
            StorageError::unsupported(
                "logical SystemStream trim is not supported by this storage provider",
            )
        })?;
        self.admission_controller_for_connection("default")?
            .settle_foreground(AdmissionClass::Write, || async move {
                SystemStreamTrimExecutor::new(repository.as_ref(), page_limit)
                    .run_page(scope, cutoff_timestamp, continuation)
                    .await
            })
            .await
    }
}
