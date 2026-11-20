//! Protected task admission and progress storage shared by background
//! executors.

use std::future::Future;

use serde::{Serialize, de::DeserializeOwned};
use storage_types::{
    Revision, StorageResult,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
};

use super::{ProgressFence, ProgressWrite};

pub trait MaintenancePort: Send {
    /// Physical: the maintenance identity and the stored progress record of
    /// `kind`, decoded with `decode_task_progress`. KV reads both with
    /// conflict protection; SQL reads them as today.
    fn load_task_record<P: DeserializeOwned + Send>(
        &mut self,
        kind: MaintenanceKind,
    ) -> impl Future<Output = StorageResult<(TargetIncarnation, Option<TaskProgress<P>>)>> + Send;

    /// Physical compare-and-set of one checked progress write. A write that
    /// finds another revision returns
    /// `StorageError::write_miss("maintenance_progress",
    /// MissExpectation::Revision)`.
    fn store_task_progress_write(
        &mut self,
        write: ProgressWrite,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn load_task_admission<P: DeserializeOwned + Send>(
        &mut self,
        kind: MaintenanceKind,
    ) -> impl Future<
        Output = StorageResult<(TargetIncarnation, Option<TaskProgress<P>>, ProgressFence)>,
    > + Send {
        async move {
            let (identity, progress) = self.load_task_record::<P>(kind).await?;
            let fence = ProgressFence::observed(progress.as_ref());
            Ok((identity, progress, fence))
        }
    }

    fn store_task_progress<P: Serialize + Send>(
        &mut self,
        kind: MaintenanceKind,
        fence: &ProgressFence,
        expected: Option<Revision>,
        next: TaskProgress<P>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        async move {
            let write = ProgressWrite::new(kind, fence, expected, &next)?;
            self.store_task_progress_write(write).await
        }
    }
}
