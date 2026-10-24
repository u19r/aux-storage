use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::installed::RegionalTablePhase;
use storage_provider::DynamoAttempt;
use storage_types::{ReadProtection, StorageResult, TimestampMillis};

use crate::{
    bootstrap::HiddenTargetRequest,
    constants::{CONTROL_OPERATION_SUFFIX, CONTROL_SLOT_SUFFIX, TARGET_BOOTSTRAP_SUFFIX},
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn load_cancelled_target_request<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &replication_types::configuration::proposal::ControlProposal,
    ) -> StorageResult<Option<HiddenTargetRequest>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let control = self
                .load_control(attempt, proposal, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            if self.decode_control(&control, proposal)?.decision
                != crate::control::ControlDecision::Cancelled
            {
                return Err(corrupt_record());
            }
            let key = self.identity_key(TARGET_BOOTSTRAP_SUFFIX, &proposal.operation().id)?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Snapshot)
                .await?;
            let Some(row) = &rows[0] else {
                return Ok(None);
            };
            let root: super::record::TargetRecord = self.records.decode(row)?;
            let mut operation = proposal.operation().clone();
            operation.content = [0; 32].into();
            if operation != root.request.operation || root.content != root.request.content()? {
                return Err(corrupt_record());
            }
            Ok(Some(root.request))
        }
        .await;
        settlement.complete(result)
    }

    /// Before any participant grant, this hidden incarnation remains
    /// disposable. A participant grant requires a durably recorded primary
    /// cancellation.
    pub(crate) async fn cancel_hidden_target<A: PhysicalRecordAttempt + DynamoAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        now: TimestampMillis,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some((row, mut record)) = self
                .load_hidden_target(attempt, request, ReadProtection::Conflict)
                .await?
            else {
                return self.persist_target_tombstone(attempt, request).await;
            };
            if record.cancelled {
                return Ok(());
            }
            let key = self.identity_key(CONTROL_OPERATION_SUFFIX, &request.operation.id)?;
            let has_cancellation_receipt = if let Some(control) = &self
                .records
                .load(attempt, &[key], ReadProtection::Conflict)
                .await?[0]
            {
                let control = self.decode_control_record(control)?;
                let mut operation = control.proposal.operation().clone();
                operation.content = [0; 32].into();
                let mut active = record.state.clone();
                active.phase = RegionalTablePhase::Active;
                if control.decision != crate::control::ControlDecision::Cancelled
                    || operation != request.operation
                    || control
                        .proposal
                        .participant(&self.region)
                        .map_err(|_| corrupt_record())?
                        .proposed
                        != active
                {
                    return Err(corrupt_record());
                }
                true
            } else {
                false
            };
            let table = self
                .require_hidden_table(attempt, &record, ReadProtection::Conflict)
                .await?;
            let slot_key =
                self.identity_key(CONTROL_SLOT_SUFFIX, &record.state.configuration.table)?;
            let slots = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&slot_key),
                    ReadProtection::Conflict,
                )
                .await?;
            let slot = slots[0].as_ref().ok_or_else(corrupt_record)?;
            let owner = self.decode_control_slot(slot, record.state.configuration.table)?;
            if owner != ControlSlotOwner::Idle
                && owner != ControlSlotOwner::Configuration(request.operation.id)
            {
                return Err(corrupt_record());
            }
            if owner == ControlSlotOwner::Idle && !has_cancellation_receipt {
                return Err(corrupt_record());
            }
            let observed = self
                .observe_table_state(
                    attempt,
                    record.state.configuration.table,
                    ReadProtection::Conflict,
                )
                .await?;
            let mut removed = record.state.clone();
            removed.phase = RegionalTablePhase::Removed;
            self.persist_table_state(attempt, (observed, removed))
                .await?;
            attempt.delete_table_layout(&table, now).await?;
            record.cancelled = true;
            let root_key = self.identity_key(TARGET_BOOTSTRAP_SUFFIX, &request.operation.id)?;
            let changes = [
                self.records
                    .prepare(root_key, Some(row.revision), &record)?,
                self.prepare_control_slot(
                    record.state.configuration.table,
                    Some(slot.revision),
                    ControlSlotOwner::Idle,
                )?,
            ];
            self.records.store(attempt, &changes).await
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn observe_hidden_cancellation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let target = self
                .load_hidden_target(attempt, request, ReadProtection::Snapshot)
                .await?;
            Ok(target.is_some_and(|(_, record)| record.cancelled)
                || self
                    .load_target_tombstone(attempt, request, ReadProtection::Snapshot)
                    .await?)
        }
        .await;
        settlement.complete(result)
    }
}
