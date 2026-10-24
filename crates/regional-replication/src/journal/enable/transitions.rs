use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::installed::InstalledTableState;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    bootstrap::{BaselineCopyScope, SourceCaptureStatus},
    control::ControlDecision,
    enable::{EnablePhase, EnablePlan, EnableRecord},
    journal::{JournalStore, TransferPhase},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) fn retain_enable_reservation(
        &self,
        plan: &EnablePlan,
        record: &mut EnableRecord,
        state: &InstalledTableState,
    ) -> StorageResult<()> {
        if record.reservations.contains(state) {
            return Ok(());
        }
        if record.phase != EnablePhase::Reserving {
            return Err(corrupt_record());
        }
        plan.require_reservation(record.reservations.len(), state)?;
        record.reservations.push(state.clone());
        if record.reservations.len() == plan.targets.len() {
            record.proposal = Some(plan.bind_proposal(&record.reservations)?);
            record.phase = EnablePhase::Bootstrapping(0);
        }
        Ok(())
    }

    pub(super) async fn advance_enable_phase<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        plan: &EnablePlan,
        record: &mut EnableRecord,
        expected: EnablePhase,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if record.phase != expected
                || matches!(expected, EnablePhase::Completed | EnablePhase::Cancelled)
            {
                return Ok(());
            }
            if let EnablePhase::Cancelling(index) = expected {
                if (index as usize) < plan.targets.len() {
                    record.phase =
                        EnablePhase::Cancelling(index.checked_add(1).ok_or_else(corrupt_record)?);
                } else {
                    self.finish_enable_cancellation(attempt, record).await?;
                    record.phase = EnablePhase::Cancelled;
                }
                return Ok(());
            }
            let proposal = record.proposal.as_ref().ok_or_else(corrupt_record)?;
            record.phase = match expected {
                EnablePhase::Bootstrapping(index) => {
                    let target = plan
                        .targets
                        .get(index as usize)
                        .ok_or_else(corrupt_record)?;
                    let scope = BaselineCopyScope::try_new(
                        proposal.clone(),
                        target.clone(),
                        plan.destination(target)?.clone(),
                    )?;
                    if self.load_transfer_phase(attempt, &scope).await? != TransferPhase::Verified {
                        return Err(corrupt_record());
                    }
                    let next = index.checked_add(1).ok_or_else(corrupt_record)?;
                    if next as usize == plan.targets.len() {
                        EnablePhase::Controlling
                    } else {
                        EnablePhase::Bootstrapping(next)
                    }
                }
                EnablePhase::Controlling => {
                    let row = self
                        .load_control(attempt, proposal, ReadProtection::Shared)
                        .await?
                        .ok_or_else(corrupt_record)?;
                    match (
                        self.decode_control(&row, proposal)?.decision,
                        self.observe_source_capture(attempt, proposal).await?,
                    ) {
                        (ControlDecision::Completed, Some(SourceCaptureStatus::Released)) => {
                            EnablePhase::Completed
                        }
                        (ControlDecision::Cancelled, Some(SourceCaptureStatus::Cancelled)) => {
                            EnablePhase::Cancelled
                        }
                        _ => return Err(corrupt_record()),
                    }
                }
                _ => return Err(corrupt_record()),
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
