use storage_types::{
    Revision, StorageError, StorageResult, TimestampMillis,
    maintenance::{
        MaintenanceKind, TargetIncarnation, TaskAdmission, TaskAuthority, TaskProgress, TaskScope,
    },
};

use super::{
    ExpirationEvaluation, ExpirationPosition, ExpirationProgress, ExpirationRequest,
    ExpirationStart,
};

impl ExpirationStart {
    pub(super) fn try_new(
        current_target: &TargetIncarnation,
        durable: Option<TaskProgress<ExpirationProgress>>,
        request: &ExpirationRequest,
    ) -> StorageResult<Self> {
        let authority = TaskAuthority {
            scope: TaskScope {
                destination: request.destination.clone(),
                target: current_target.clone(),
                work: MaintenanceKind::TtlSweep,
            },
            job: request.job,
            fence: request.fence,
        };
        Self::admit(
            current_target,
            durable,
            &authority,
            request.requested_cutoff,
        )
    }

    fn admit(
        current_target: &TargetIncarnation,
        durable: Option<TaskProgress<ExpirationProgress>>,
        authority: &TaskAuthority,
        cutoff: TimestampMillis,
    ) -> StorageResult<Self> {
        match authority.admit(Some(current_target), durable.as_ref()) {
            TaskAdmission::Initialize { authority } => Ok(Self::initialize(authority, cutoff)),
            TaskAdmission::Evaluate {
                authority,
                expected_revision,
            } => Self::resume(authority, expected_revision, durable, cutoff),
            TaskAdmission::Superseded => Ok(Self::Superseded),
            TaskAdmission::BindingMismatch => Ok(Self::BindingMismatch),
        }
    }

    fn initialize(authority: TaskAuthority, cutoff: TimestampMillis) -> Self {
        Self::Evaluate(ExpirationEvaluation {
            authority,
            expected_revision: None,
            progress: ExpirationProgress {
                cutoff,
                position: ExpirationPosition::Start,
            },
        })
    }

    fn resume(
        authority: TaskAuthority,
        expected_revision: Revision,
        durable: Option<TaskProgress<ExpirationProgress>>,
        cutoff: TimestampMillis,
    ) -> StorageResult<Self> {
        let mut progress = durable
            .ok_or_else(|| StorageError::internal("expiration task progress disappeared"))?
            .state;
        if progress.position == ExpirationPosition::Start {
            progress.cutoff = cutoff;
        }
        Ok(Self::Evaluate(ExpirationEvaluation {
            authority,
            expected_revision: Some(expected_revision),
            progress,
        }))
    }
}
