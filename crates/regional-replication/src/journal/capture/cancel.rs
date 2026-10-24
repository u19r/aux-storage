use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::{
    record::{CapturePhase, CaptureRecord},
    status,
};
use crate::{
    bootstrap::SourceCaptureStatus,
    constants::CONTROL_SLOT_SUFFIX,
    control::ControlDecision,
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn cancel_source_capture<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<SourceCaptureStatus> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let Some((row, mut record)) = self
                .load_source_capture(attempt, proposal, ReadProtection::Conflict)
                .await?
            else {
                let control = self
                    .load_control(attempt, proposal, ReadProtection::Conflict)
                    .await?
                    .ok_or_else(corrupt_record)?;
                if self.decode_control(&control, proposal)?.decision != ControlDecision::Cancelled {
                    return Err(corrupt_record());
                }
                return Ok(SourceCaptureStatus::Cancelled);
            };
            if matches!(record.phase, CapturePhase::Cancelled) {
                return Ok(status(&record.phase));
            }
            if matches!(record.phase, CapturePhase::Cancelling(_)) {
                return self
                    .advance_capture_cancellation(attempt, (row.revision, record))
                    .await;
            }
            if let Some(control) = self
                .load_control(attempt, proposal, ReadProtection::Conflict)
                .await?
            {
                let decision = self.decode_control(&control, proposal)?.decision;
                if decision != ControlDecision::Cancelled {
                    return Err(corrupt_record());
                }
            }
            let capture = self
                .require_capture_table(attempt, proposal, storage_types::ReadProtection::Shared)
                .await?;
            let observed = self
                .observe_table_state(
                    attempt,
                    capture.configuration.table,
                    ReadProtection::Conflict,
                )
                .await?;
            let before = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .clone()
                .ok_or_else(corrupt_record)?;
            self.persist_table_state(attempt, (observed, before))
                .await?;
            record.phase = CapturePhase::Cancelling(0);
            self.store_source_capture(attempt, &record, Some(row.revision))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn advance_capture_cancellation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (storage_types::Revision, CaptureRecord),
    ) -> StorageResult<SourceCaptureStatus> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (revision, mut record) = input;
            self.advance_source_cuts(attempt, &mut record).await?;
            if matches!(record.phase, CapturePhase::Cancelled) {
                self.settle_reenable_inventory(
                    attempt,
                    &record.proposal,
                    super::super::reenable_history::InventorySettlement::Cancel,
                )
                .await?;
                let key =
                    self.identity_key(CONTROL_SLOT_SUFFIX, &record.proposal.operation().table)?;
                let rows = self
                    .records
                    .load(
                        attempt,
                        std::slice::from_ref(&key),
                        ReadProtection::Conflict,
                    )
                    .await?;
                let row = rows[0].as_ref().ok_or_else(corrupt_record)?;
                let active = self.decode_control_slot(row, record.proposal.operation().table)?;
                if active == ControlSlotOwner::Configuration(record.proposal.operation().id) {
                    let change = self.prepare_control_slot(
                        record.proposal.operation().table,
                        Some(row.revision),
                        ControlSlotOwner::Idle,
                    )?;
                    self.records.store(attempt, &[change]).await?;
                } else {
                    let control = self
                        .load_control(attempt, &record.proposal, ReadProtection::Shared)
                        .await?
                        .ok_or_else(corrupt_record)?;
                    if self.decode_control(&control, &record.proposal)?.decision
                        != ControlDecision::Cancelled
                    {
                        return Err(corrupt_record());
                    }
                }
            }
            self.store_source_capture(attempt, &record, Some(revision))
                .await
        }
        .await;
        settlement.complete(result)
    }
}
