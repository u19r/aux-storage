//! Protected task admission and progress storage shared by background
//! executors.

use std::future::Future;

use serde::{Serialize, de::DeserializeOwned};
use storage_types::{
    Revision, StorageResult,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
};

pub trait MaintenancePort: Send {
    type ProgressFence: Send + Sync;

    fn load_task_admission<P: DeserializeOwned + Send>(
        &mut self,
        kind: MaintenanceKind,
    ) -> impl Future<
        Output = StorageResult<(
            TargetIncarnation,
            Option<TaskProgress<P>>,
            Self::ProgressFence,
        )>,
    > + Send;

    fn store_task_progress<P: Serialize + Send>(
        &mut self,
        kind: MaintenanceKind,
        fence: &Self::ProgressFence,
        expected: Option<Revision>,
        next: TaskProgress<P>,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}
