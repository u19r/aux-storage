use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::installed::InstalledTableState;
use storage_provider::DescribeTimeToLiveAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    addition::{AddRegionPlan, AdditionPhase, AdditionRecord},
    bootstrap::BaselineCopyScope,
    control::ControlDecision,
    history_capture::{HistoryCaptureRequest, HistoryCaptureStatus},
    journal::{JournalStore, OriginalTraversalWork},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) fn retain_addition_reservation(
        &self,
        plan: &AddRegionPlan,
        record: &mut AdditionRecord,
        state: &InstalledTableState,
    ) -> StorageResult<()> {
        if record.reservation.as_ref() == Some(state) {
            return Ok(());
        }
        if record.phase != AdditionPhase::Reserving {
            return Err(corrupt_record());
        }
        plan.require_reservation(plan.operation.id, plan.target.content()?, state)?;
        record.proposal = Some(plan.bind_reserved_state(state.clone())?);
        record.reservation = Some(state.clone());
        record.phase = AdditionPhase::Bootstrapping;
        Ok(())
    }

    pub(super) async fn advance_addition_phase<
        A: PhysicalRecordAttempt + DescribeTimeToLiveAttempt,
    >(
        &self,
        attempt: &mut A,
        record: &mut AdditionRecord,
        expected: AdditionPhase,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if record.phase != expected {
                return Ok(());
            }
            if expected == AdditionPhase::Cancelling && record.proposal.is_none() {
                self.finish_unreserved_addition_cancellation(attempt, &record.plan)
                    .await?;
                record.phase = AdditionPhase::Cancelled;
                return Ok(());
            }
            let proposal = record.proposal.as_ref().ok_or_else(corrupt_record)?;
            record.phase = match expected {
                AdditionPhase::Bootstrapping => {
                    let scope = BaselineCopyScope::try_new(
                        proposal.clone(),
                        record.plan.target.clone(),
                        record.plan.destination()?.clone(),
                    )?;
                    if !matches!(
                        self.plan_original_traversal(attempt, &scope).await?,
                        OriginalTraversalWork::Complete
                    ) {
                        return Err(corrupt_record());
                    }
                    AdditionPhase::Controlling
                }
                AdditionPhase::Controlling | AdditionPhase::Cancelling => {
                    let row = self
                        .load_control(attempt, proposal, ReadProtection::Shared)
                        .await?
                        .ok_or_else(corrupt_record)?;
                    let control = self.decode_control(&row, proposal)?;
                    let request = HistoryCaptureRequest {
                        proposal: proposal.clone(),
                        region: self.region.clone(),
                    };
                    let capture = self.observe_history_capture(attempt, &request).await?;
                    match (control.decision, capture) {
                        (ControlDecision::Completed, Some(HistoryCaptureStatus::Released)) => {
                            AdditionPhase::Completed
                        }
                        (
                            ControlDecision::Cancelled,
                            None | Some(HistoryCaptureStatus::Cancelled),
                        ) => {
                            self.clear_history_slot(attempt, &request).await?;
                            AdditionPhase::Cancelled
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

    pub(super) async fn cancel_addition_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &mut AdditionRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(proposal) = record.proposal.as_ref() else {
                if record.phase != AdditionPhase::Reserving
                    && record.phase != AdditionPhase::Cancelling
                {
                    return Err(corrupt_record());
                }
                record.phase = AdditionPhase::Cancelling;
                return Ok(());
            };
            if self
                .load_control(attempt, proposal, ReadProtection::Conflict)
                .await?
                .is_none()
            {
                self.cancel_participant(attempt, proposal).await?;
            } else {
                replication_provider::control::validate_preparation(proposal)
                    .map_err(|_| corrupt_record())?;
                self.decide_control(attempt, (proposal, None)).await?;
            }
            record.phase = AdditionPhase::Cancelling;
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
