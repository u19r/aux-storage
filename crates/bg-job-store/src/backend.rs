use std::sync::Arc;

use storage_provider::RawOrderedJobStore;

use crate::ScheduledJobStore;

/// Constructs the adapter for workspace-owned provider crates.
#[doc(hidden)]
pub fn create_scheduled_job_store(raw: Arc<dyn RawOrderedJobStore>) -> ScheduledJobStore {
    ScheduledJobStore::new(raw)
}

/// Derives scheduled-job access from the initialized owner used by domain
/// operations.
pub trait ScheduledJobStoreSource: Send + Sync {
    fn scheduled_job_store(
        &self,
        config: crate::ScheduledJobStoreConfig,
    ) -> storage_types::StorageResult<ScheduledJobStore>;
}
