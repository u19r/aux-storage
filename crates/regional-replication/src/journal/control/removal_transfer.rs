//! Primary-native progress for bounded removal seal propagation.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, RecordChange},
};
use replication_types::{configuration::proposal::ControlProposal, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::{CONTROL_REMOVAL_TRANSFER_SUFFIX, CONTROL_SLOT_SUFFIX},
    control::{
        ControlDecision,
        removal_transfer::{RemovalTransferPlan, RemovalTransferPosition},
    },
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemovalTransferProgress {
    content: CanonicalDigest,
    position: RemovalTransferPosition,
}

impl JournalStore {
    pub(crate) async fn load_removal_transfer_position<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<RemovalTransferPosition> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_removal_transfer_primary(attempt, proposal)
                .await?;
            let key =
                self.identity_key(CONTROL_REMOVAL_TRANSFER_SUFFIX, &proposal.operation().id)?;
            let row = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?
                .pop()
                .flatten();
            self.decode_removal_transfer(row.as_ref(), proposal)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn advance_removal_transfer<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        completed: RemovalTransferPosition,
    ) -> StorageResult<RemovalTransferPosition> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_removal_transfer_primary(attempt, proposal)
                .await?;
            let plan = RemovalTransferPlan::try_new(proposal)?;
            if plan.select_step(completed)?.is_none() {
                return Err(corrupt_record());
            }
            let next = completed.checked_next()?;
            let key =
                self.identity_key(CONTROL_REMOVAL_TRANSFER_SUFFIX, &proposal.operation().id)?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            let current = self.decode_removal_transfer(row.as_ref(), proposal)?;
            if current >= next {
                return Ok(current);
            }
            if current != completed {
                return Err(corrupt_record());
            }
            let progress = RemovalTransferProgress {
                content: proposal.operation().content,
                position: next,
            };
            self.records
                .store(
                    attempt,
                    &[self
                        .records
                        .prepare(key, row.map(|row| row.revision), &progress)?],
                )
                .await?;
            Ok(next)
        }
        .await;
        settlement.complete(result)
    }

    async fn require_removal_transfer_primary<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<()> {
        RemovalTransferPlan::try_new(proposal)?;
        if proposal.operation().home_region != self.region {
            return Err(corrupt_record());
        }
        let row = self
            .load_control(attempt, proposal, ReadProtection::Conflict)
            .await?
            .ok_or_else(corrupt_record)?;
        if !matches!(
            self.decode_control(&row, proposal)?.decision,
            ControlDecision::Prepared | ControlDecision::Committed | ControlDecision::Installed
        ) {
            return Err(corrupt_record());
        }
        let table = proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?
            .proposed
            .configuration
            .table;
        let key = self.identity_key(CONTROL_SLOT_SUFFIX, &table)?;
        let row = self
            .records
            .load(attempt, &[key], ReadProtection::Conflict)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        if self.decode_control_slot(&row, table)?
            != ControlSlotOwner::Configuration(proposal.operation().id)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn decode_removal_transfer(
        &self,
        row: Option<&DurableRecord>,
        proposal: &ControlProposal,
    ) -> StorageResult<RemovalTransferPosition> {
        let plan = RemovalTransferPlan::try_new(proposal)?;
        let Some(row) = row else {
            return Ok(RemovalTransferPosition::first());
        };
        if row.key
            != self.identity_key(CONTROL_REMOVAL_TRANSFER_SUFFIX, &proposal.operation().id)?
        {
            return Err(corrupt_record());
        }
        let progress: RemovalTransferProgress = self.records.decode(row)?;
        if progress.content != proposal.operation().content {
            return Err(corrupt_record());
        }
        plan.require_position(progress.position)?;
        Ok(progress.position)
    }

    pub(super) fn prepare_removal_transfer_retirement(
        &self,
        row: Option<DurableRecord>,
        proposal: &ControlProposal,
    ) -> StorageResult<Option<RecordChange>> {
        let Some(row) = row else {
            return Ok(None);
        };
        self.decode_removal_transfer(Some(&row), proposal)?;
        Ok(Some(RecordChange::Delete {
            key: row.key,
            expected: row.revision,
        }))
    }
}
