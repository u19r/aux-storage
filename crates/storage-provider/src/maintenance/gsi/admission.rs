use storage_types::maintenance::{
    MaintenanceKind, TargetIncarnation, TaskAdmission, TaskAuthority, TaskProgress, TaskScope,
};

use super::{
    GsiBuildRequest,
    models::{GsiBuildEvaluation, GsiBuildStart},
};

impl GsiBuildStart {
    pub(super) fn new(
        current_target: TargetIncarnation,
        durable: Option<TaskProgress<Option<storage_backfill::GsiBuildTarget>>>,
        request: &GsiBuildRequest,
    ) -> Self {
        let requested = TaskAuthority {
            scope: TaskScope {
                destination: request.destination.clone(),
                target: current_target,
                work: MaintenanceKind::GsiBackfill,
            },
            job: request.job,
            fence: request.fence,
        };
        match requested.admit(Some(&requested.scope.target), durable.as_ref()) {
            TaskAdmission::Initialize { authority } => Self::Evaluate(GsiBuildEvaluation {
                authority,
                after: None,
                expected_revision: None,
                page_limit: request.page_limit,
                captured_at: request.captured_at,
            }),
            TaskAdmission::Evaluate {
                authority,
                expected_revision,
            } => Self::Evaluate(GsiBuildEvaluation {
                authority,
                after: durable.as_ref().and_then(|progress| progress.state.clone()),
                expected_revision: Some(expected_revision),
                page_limit: request.page_limit,
                captured_at: request.captured_at,
            }),
            TaskAdmission::Superseded => Self::Superseded,
            TaskAdmission::BindingMismatch => Self::BindingMismatch,
        }
    }
}

// TODO: add FoundationDB simulation coverage for GSI admission state
// transitions.
impl GsiBuildStart {
    pub(super) async fn advance(
        self,
        transaction: &mut impl super::GsiBuildTransaction,
    ) -> storage_types::StorageResult<super::GsiBuildAdvance> {
        use storage_types::{StorageError, maintenance::StepResult};
        match self {
            Self::Evaluate(evaluation) => evaluation.advance(transaction).await,
            Self::Superseded => Ok(super::GsiBuildAdvance {
                result: StepResult::Superseded,
                ttl_activation: None,
            }),
            Self::BindingMismatch => Err(StorageError::validation(
                "GSI build task is bound to another catalog job",
            )),
        }
    }
}
