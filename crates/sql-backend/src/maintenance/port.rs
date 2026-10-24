//! Physical task admission and progress inside one Sql catalog attempt.

use serde::{Serialize, de::DeserializeOwned};
use storage_provider::maintenance::MaintenancePort;
use storage_types::{
    Revision, StorageResult,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
};

use super::{read_admission, write_progress};
use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

impl<A: SqlAttempt, L: SqlLayout> MaintenancePort for SqlCatalogAttempt<'_, A, L> {
    type ProgressFence = ();

    async fn load_task_admission<P: DeserializeOwned + Send>(
        &mut self,
        kind: MaintenanceKind,
    ) -> StorageResult<(
        TargetIncarnation,
        Option<TaskProgress<P>>,
        Self::ProgressFence,
    )> {
        let (identity, progress) = read_admission(self.attempt, self.engine, kind).await?;
        Ok((identity, progress, ()))
    }

    async fn store_task_progress<P: Serialize + Send>(
        &mut self,
        kind: MaintenanceKind,
        _: &Self::ProgressFence,
        expected: Option<Revision>,
        next: TaskProgress<P>,
    ) -> StorageResult<()> {
        write_progress(self.attempt, self.engine, kind, expected, next).await
    }
}
