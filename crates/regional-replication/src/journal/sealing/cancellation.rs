use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::SealPhase;
use crate::{
    constants::CONTROL_SLOT_SUFFIX,
    control::ControlDecision,
    journal::{JournalStore, control_slot::ControlSlotOwner},
    lifecycle::{SealRequest, SealStatus},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn cancel_table_seal<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
    ) -> StorageResult<SealStatus> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (row, mut record) = self
                .load_table_seal(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            if matches!(record.phase, SealPhase::Cancelled) {
                return record.status();
            }
            if matches!(record.phase, SealPhase::Removed | SealPhase::Retired) {
                return Err(corrupt_record());
            }
            let has_cancellation_receipt = if let Some(control) = self
                .load_control(attempt, &request.proposal, ReadProtection::Conflict)
                .await?
            {
                if self.decode_control(&control, &request.proposal)?.decision
                    != ControlDecision::Cancelled
                {
                    return Err(corrupt_record());
                }
                true
            } else {
                false
            };
            self.require_sealing_table(attempt, request).await?;
            let before = request.before()?;
            let observed = self
                .observe_table_state(
                    attempt,
                    before.configuration.table,
                    ReadProtection::Conflict,
                )
                .await?;
            self.persist_table_state(attempt, (observed, before.clone()))
                .await?;
            let slot = self.identity_key(CONTROL_SLOT_SUFFIX, &before.configuration.table)?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&slot),
                    ReadProtection::Conflict,
                )
                .await?;
            let slot = rows[0].as_ref().ok_or_else(corrupt_record)?;
            let owner = self.decode_control_slot(slot, before.configuration.table)?;
            if owner != ControlSlotOwner::Idle
                && owner != ControlSlotOwner::Configuration(request.proposal.operation().id)
            {
                return Err(corrupt_record());
            }
            if owner != ControlSlotOwner::Idle {
                self.records
                    .store(
                        attempt,
                        &[self.prepare_control_slot(
                            before.configuration.table,
                            Some(slot.revision),
                            ControlSlotOwner::Idle,
                        )?],
                    )
                    .await?;
            } else if !has_cancellation_receipt {
                return Err(corrupt_record());
            }
            record.phase = SealPhase::Cancelled;
            self.store_table_seal(attempt, &record, Some(row.revision))
                .await?;
            record.status()
        }
        .await;
        settlement.complete(result)
    }
}
