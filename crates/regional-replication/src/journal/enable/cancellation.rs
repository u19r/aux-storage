use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult};

use crate::{
    bootstrap::SourceCaptureStatus,
    control::ControlDecision,
    enable::{EnablePhase, EnableRecord},
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn cancel_enable_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &mut EnableRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if matches!(record.phase, EnablePhase::Cancelling(_)) {
                return Ok(());
            }
            if let Some(proposal) = &record.proposal {
                match self
                    .load_control(attempt, proposal, ReadProtection::Conflict)
                    .await?
                {
                    None => {
                        self.cancel_participant(attempt, proposal).await?;
                    }
                    Some(row) => match self.decode_control(&row, proposal)?.decision {
                        ControlDecision::Prepared => {
                            self.decide_control(attempt, (proposal, None)).await?;
                        }
                        ControlDecision::Cancelled => {}
                        _ => {
                            return Err(StorageError::unsupported(
                                "Committed global enable cannot be cancelled",
                            ));
                        }
                    },
                }
            } else if record.phase != EnablePhase::Reserving {
                return Err(corrupt_record());
            }
            record.phase = EnablePhase::Cancelling(0);
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn finish_enable_cancellation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &EnableRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if let Some(proposal) = &record.proposal {
                let row = self
                    .load_control(attempt, proposal, ReadProtection::Shared)
                    .await?
                    .ok_or_else(corrupt_record)?;
                if self.decode_control(&row, proposal)?.decision != ControlDecision::Cancelled
                    || !matches!(
                        self.observe_source_capture(attempt, proposal).await?,
                        None | Some(SourceCaptureStatus::Cancelled)
                    )
                {
                    return Err(corrupt_record());
                }
            } else {
                let keys = [
                    self.identity_key(
                        crate::constants::CONTROL_OPERATION_SUFFIX,
                        &record.plan.operation.id,
                    )?,
                    self.identity_key(
                        crate::constants::SOURCE_CAPTURE_SUFFIX,
                        &record.plan.operation.id,
                    )?,
                ];
                if self
                    .records
                    .load(attempt, &keys, ReadProtection::Conflict)
                    .await?
                    .iter()
                    .any(Option::is_some)
                {
                    return Err(corrupt_record());
                }
            }
            let table = record.plan.operation.table;
            let key = self.identity_key(crate::constants::CONTROL_SLOT_SUFFIX, &table)?;
            let row = self
                .records
                .load(attempt, &[key], ReadProtection::Conflict)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let owner = self.decode_control_slot(&row, table)?;
            if owner == ControlSlotOwner::Configuration(record.plan.operation.id) {
                let change =
                    self.prepare_control_slot(table, Some(row.revision), ControlSlotOwner::Idle)?;
                self.records.store(attempt, &[change]).await?;
            } else if record.proposal.is_none() {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
