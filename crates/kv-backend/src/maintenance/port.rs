//! Physical task admission and progress inside one Kv catalog attempt.

use serde::de::DeserializeOwned;
use storage_provider::maintenance::{MaintenancePort, ProgressWrite};
use storage_types::{
    StorageResult,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
};

use super::{read_admission, store_progress};
use crate::{KvAttempt, catalog::KvCatalogAttempt};

impl<A: KvAttempt> MaintenancePort for KvCatalogAttempt<'_, A> {
    async fn load_task_record<P: DeserializeOwned + Send>(
        &mut self,
        kind: MaintenanceKind,
    ) -> StorageResult<(TargetIncarnation, Option<TaskProgress<P>>)> {
        read_admission(self.attempt, kind).await
    }

    async fn store_task_progress_write(&mut self, write: ProgressWrite) -> StorageResult<()> {
        store_progress(self.attempt, write)
    }
}
