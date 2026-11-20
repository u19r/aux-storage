//! The shared progress fence and checked progress replacement for every
//! maintenance kind.

use serde::{Serialize, de::DeserializeOwned};
use storage_types::{
    MissExpectation, Revision, StorageError, StorageResult,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
    storage_serde,
};

/// What this attempt's admission read from the progress record. Built only by
/// `MaintenancePort::load_task_admission`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgressFence(Option<(Revision, TargetIncarnation)>);

impl ProgressFence {
    pub(crate) fn observed<P>(progress: Option<&TaskProgress<P>>) -> Self {
        Self(progress.map(|progress| (progress.revision, progress.authority.scope.target.clone())))
    }
}

/// One progress replacement, checked against the fence and encoded once.
/// `expected: None` inserts; `Some` replaces that revision of `target`.
pub struct ProgressWrite {
    pub kind: MaintenanceKind,
    pub expected: Option<Revision>,
    pub target: TargetIncarnation,
    pub revision: Revision,
    pub bytes: Vec<u8>,
}

impl ProgressWrite {
    pub fn new<P: Serialize>(
        kind: MaintenanceKind,
        fence: &ProgressFence,
        expected: Option<Revision>,
        next: &TaskProgress<P>,
    ) -> StorageResult<Self> {
        if next.authority.scope.work != kind {
            return Err(StorageError::internal(
                "maintenance task progress has the wrong kind",
            ));
        }
        // Admission read the progress record in this attempt (KV: with
        // conflict protection; SQL: the statement repeats this compare).
        if fence.0.as_ref().map(|(revision, _)| *revision) != expected
            || fence
                .0
                .as_ref()
                .is_some_and(|(_, target)| target != &next.authority.scope.target)
        {
            return Err(StorageError::write_miss(
                "maintenance_progress",
                MissExpectation::Revision,
            ));
        }
        Ok(Self {
            kind,
            expected,
            target: next.authority.scope.target.clone(),
            revision: next.revision,
            bytes: storage_serde::to_bytes(next)?,
        })
    }
}

/// Decode one stored progress record of `kind`.
pub fn decode_task_progress<P: DeserializeOwned>(
    bytes: &[u8],
    kind: MaintenanceKind,
) -> StorageResult<TaskProgress<P>> {
    let progress: TaskProgress<P> = storage_serde::from_bytes(bytes)?;
    if progress.authority.scope.work != kind {
        return Err(StorageError::internal(
            "stored maintenance task progress has another kind",
        ));
    }
    Ok(progress)
}
