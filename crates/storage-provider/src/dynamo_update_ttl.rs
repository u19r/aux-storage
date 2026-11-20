//! One TTL policy transition under the caller's protected table attempt.

use storage_common::ttl::{TtlConfigRecord, TtlPolicyChange};
use storage_types::{
    BoundTableInfo, StorageResult, TimeToLiveSpecification, TimestampMillis,
    UpdateTimeToLiveResponse,
};

use crate::{SemanticDecision, TtlPolicyAttempt};

/// Previous protected checkpoint and its replacement; the policy stays
/// unchanged.
pub struct TtlCheckpointChange {
    pub previous: storage_common::ttl::TtlMaintenanceCheckpoint,
    pub next: TtlConfigRecord,
}

impl TtlCheckpointChange {
    pub fn try_new(
        mut observed: TtlConfigRecord,
        next_cursor: Option<storage_common::TtlMaintenanceCursor>,
    ) -> StorageResult<Self> {
        let previous = observed.checkpoint.clone();
        observed.checkpoint.revision = previous.revision.checked_next()?;
        observed.checkpoint.cursor = next_cursor;
        Ok(Self {
            previous,
            next: observed,
        })
    }
}

/// Keep the response tied to the request; the persisted policy may remain
/// `Enabling` until the bounded maintenance job finishes its existing items.
pub async fn update_time_to_live_in_attempt<A: TtlPolicyAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    specification: &TimeToLiveSpecification,
    now: TimestampMillis,
) -> StorageResult<SemanticDecision<UpdateTimeToLiveResponse>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let existing = attempt.read_ttl_config(table).await?;
        let change = match TtlPolicyChange::try_new(
            &table.info.table_name,
            specification,
            existing.as_ref(),
            now,
        )? {
            SemanticDecision::Accepted(change) => change,
            SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
        };
        attempt.install_ttl_policy(table, change).await?;
        Ok(SemanticDecision::Accepted(UpdateTimeToLiveResponse {
            time_to_live_specification: specification.clone(),
        }))
    }
    .await;
    settlement.complete(result)
}
