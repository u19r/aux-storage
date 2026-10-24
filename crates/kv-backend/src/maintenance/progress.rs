//! Protected task progress shared by KV maintenance workflows.

use serde::{Serialize, de::DeserializeOwned};
use storage_types::{
    Revision, StorageEnum, StorageError, StorageResult,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
    storage_serde,
};

use super::key_layout::progress_key;
use crate::{KvAttempt, KvMutation};

/// Evidence that this native attempt read the progress key with conflict
/// protection. Only maintenance admission can construct the fence.
pub struct ProgressFence(Option<(Revision, TargetIncarnation)>);

impl ProgressFence {
    pub(super) fn from_read<P>(progress: Option<&TaskProgress<P>>) -> Self {
        Self(progress.map(|progress| (progress.revision, progress.authority.scope.target.clone())))
    }
}

pub(super) fn decode_progress<P: DeserializeOwned>(
    value: Option<Vec<u8>>,
    kind: MaintenanceKind,
) -> StorageResult<Option<TaskProgress<P>>> {
    let progress = value
        .map(|bytes| storage_serde::from_bytes::<TaskProgress<P>>(&bytes))
        .transpose()?;
    if progress
        .as_ref()
        .is_some_and(|progress| progress.authority.scope.work != kind)
    {
        return Err(StorageError::internal(
            "KV maintenance task progress has the wrong kind",
        ));
    }
    Ok(progress)
}

pub(crate) fn write_progress<A: KvAttempt, P: Serialize>(
    attempt: &mut A,
    kind: MaintenanceKind,
    observed: &ProgressFence,
    expected: Option<Revision>,
    next: TaskProgress<P>,
) -> StorageResult<()> {
    if next.authority.scope.work != kind {
        return Err(StorageError::internal(
            "maintenance task progress has the wrong kind",
        ));
    }
    // Admission read this key with conflict protection. Both native drivers
    // reject a concurrent change at commit, so rereading cannot strengthen
    // the fence and would add a dependent network round on FoundationDB.
    if observed.0.as_ref().map(|(revision, _)| *revision) != expected
        || observed
            .0
            .as_ref()
            .is_some_and(|(_, target)| target != &next.authority.scope.target)
    {
        return Err(StorageEnum::TransactionConflict {
            message: "maintenance task progress changed during this attempt".to_owned(),
        }
        .into());
    }
    attempt.stage(vec![KvMutation::Put {
        key: progress_key(kind),
        value: storage_serde::to_bytes(&next)?,
    }])
}
