use std::num::NonZeroU64;

use scheduled_bg_jobs::{JobId, Namespace};
use storage_backfill::{GsiBuild, GsiPageLimit, GsiProjectionWriteBatch, GsiState};
use storage_common::TtlConfigActivation;
use storage_types::{
    Revision, TimestampMillis,
    maintenance::{StepResult, TaskAuthority, TaskProgress},
};

#[derive(Clone, PartialEq, Eq)]
pub struct GsiBuildRequest {
    pub destination: Namespace,
    pub job: JobId,
    pub fence: NonZeroU64,
    pub page_limit: GsiPageLimit,
    pub captured_at: TimestampMillis,
}

pub(super) enum GsiBuildStart {
    Evaluate(GsiBuildEvaluation),
    Superseded,
    BindingMismatch,
}

pub(super) struct GsiBuildEvaluation {
    pub after: Option<storage_backfill::GsiBuildTarget>,
    pub authority: TaskAuthority,
    pub expected_revision: Option<Revision>,
    pub page_limit: GsiPageLimit,
    pub captured_at: TimestampMillis,
}

pub struct GsiBuildAdvance {
    pub result: StepResult<()>,
    pub ttl_activation: Option<TtlConfigActivation>,
}

pub struct GsiTransition<'a, Cursor> {
    pub expected_revision: Option<Revision>,
    pub next_progress: TaskProgress<Option<storage_backfill::GsiBuildTarget>>,
    pub expected: GsiBuild<Cursor>,
    pub projections: GsiProjectionWriteBatch,
    pub next_state: GsiState<Cursor>,
    pub ttl_activation: Option<&'a TtlConfigActivation>,
}

impl<Cursor> GsiTransition<'_, Cursor> {
    pub fn validate_table(
        &self,
        table: &storage_types::BoundTableInfo,
    ) -> storage_types::StorageResult<()> {
        self.expected.target.validate_table(table)?;
        self.projections.validate_target(&self.expected.target)?;
        if self.ttl_activation.is_some_and(|activation| {
            activation.table_name != self.expected.target.table_name
                || activation.activated.gsi_name != self.expected.target.index_name
        }) {
            return Err(storage_types::StorageError::internal(
                "GSI TTL activation has another target",
            ));
        }
        Ok(())
    }
}
