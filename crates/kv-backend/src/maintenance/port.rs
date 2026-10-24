//! Physical task admission and progress inside one Kv catalog attempt.

use serde::{Serialize, de::DeserializeOwned};
use storage_provider::maintenance::MaintenancePort;
use storage_types::{
    Revision, StorageResult,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
};

use super::{ProgressFence, read_admission, write_progress};
use crate::{KvAttempt, catalog::KvCatalogAttempt};

impl<A: KvAttempt> MaintenancePort for KvCatalogAttempt<'_, A> {
    type ProgressFence = ProgressFence;

    async fn load_task_admission<P: DeserializeOwned + Send>(
        &mut self,
        kind: MaintenanceKind,
    ) -> StorageResult<(
        TargetIncarnation,
        Option<TaskProgress<P>>,
        Self::ProgressFence,
    )> {
        read_admission(self.attempt, kind).await
    }

    async fn store_task_progress<P: Serialize + Send>(
        &mut self,
        kind: MaintenanceKind,
        fence: &Self::ProgressFence,
        expected: Option<Revision>,
        next: TaskProgress<P>,
    ) -> StorageResult<()> {
        write_progress(self.attempt, kind, fence, expected, next)
    }
}
