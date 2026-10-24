use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::{HistoryCapturePhase, HistoryCaptureRecord, capture_status};
use crate::{
    constants::CONTROL_SLOT_SUFFIX,
    control::ControlDecision,
    history_capture::{HistoryCaptureRequest, HistoryCaptureStatus},
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn require_history_slot<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_history_owner(attempt, request, protection)
                .await?;
            if self
                .load_control(attempt, &request.proposal, protection)
                .await?
                .is_some()
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_history_owner<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let source = replication_provider::control::borrow_addition_source(
                &request.proposal,
                &self.region,
            )
            .map_err(|_| corrupt_record())?;
            let key = self.identity_key(CONTROL_SLOT_SUFFIX, &source.configuration.table)?;
            let rows = self.records.load(attempt, &[key], protection).await?;
            let active = self.decode_control_slot(
                rows[0].as_ref().ok_or_else(corrupt_record)?,
                source.configuration.table,
            )?;
            if active != ControlSlotOwner::Configuration(request.proposal.operation().id) {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn cancel_history_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
    ) -> StorageResult<HistoryCaptureStatus> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (row, mut record) = self
                .load_history_capture(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            if matches!(record.phase, HistoryCapturePhase::Cancelled) {
                return Ok(capture_status(record.phase));
            }
            if matches!(record.phase, HistoryCapturePhase::Cancelling(_)) {
                return self
                    .advance_history_cleanup(attempt, (row.revision, record))
                    .await;
            }
            if let Some(control) = self
                .load_control(attempt, &request.proposal, ReadProtection::Conflict)
                .await?
            {
                if self.decode_control(&control, &request.proposal)?.decision
                    != ControlDecision::Cancelled
                {
                    return Err(corrupt_record());
                }
            } else {
                self.require_history_slot(
                    attempt,
                    request,
                    storage_types::ReadProtection::Conflict,
                )
                .await?;
            }
            if matches!(
                record.phase,
                HistoryCapturePhase::Released | HistoryCapturePhase::Releasing(_)
            ) {
                return Err(corrupt_record());
            }
            if matches!(record.phase, HistoryCapturePhase::Admitting { .. }) {
                record.phase = HistoryCapturePhase::Cancelled;
                self.clear_history_slot(attempt, request).await?;
                return self
                    .store_history_capture(attempt, &record, Some(row.revision))
                    .await;
            }
            record.phase = HistoryCapturePhase::Cancelling(0);
            self.store_history_capture(attempt, &record, Some(row.revision))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn release_history_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
    ) -> StorageResult<HistoryCaptureStatus> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (row, mut record) = self
                .load_history_capture(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            if matches!(record.phase, HistoryCapturePhase::Released) {
                return Ok(capture_status(record.phase));
            }
            let control = self
                .load_control(attempt, &request.proposal, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            let control = self.decode_control(&control, &request.proposal)?;
            if control.decision != ControlDecision::Completed || control.bootstrap.is_none() {
                return Err(corrupt_record());
            }
            match record.phase {
                HistoryCapturePhase::Ready => record.phase = HistoryCapturePhase::Releasing(0),
                HistoryCapturePhase::Releasing(_) => {}
                _ => return Err(corrupt_record()),
            }
            self.advance_history_cleanup(attempt, (row.revision, record))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn advance_history_cleanup<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (storage_types::Revision, HistoryCaptureRecord),
    ) -> StorageResult<HistoryCaptureStatus> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (revision, mut record) = input;
            self.advance_history_cuts(attempt, &mut record).await?;
            self.settle_adoption_inventory(attempt, &record).await?;
            if matches!(record.phase, HistoryCapturePhase::Cancelled) {
                self.clear_history_slot(attempt, &record.request).await?;
            }
            self.store_history_capture(attempt, &record, Some(revision))
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn clear_history_slot<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let table = request
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed
                .configuration
                .table;
            let key = self.identity_key(CONTROL_SLOT_SUFFIX, &table)?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            let row = rows[0].as_ref().ok_or_else(corrupt_record)?;
            let active = self.decode_control_slot(row, table)?;
            if active == ControlSlotOwner::Configuration(request.proposal.operation().id) {
                return self
                    .records
                    .store(
                        attempt,
                        &[self.prepare_control_slot(
                            table,
                            Some(row.revision),
                            ControlSlotOwner::Idle,
                        )?],
                    )
                    .await;
            }
            let control = self
                .load_control(attempt, &request.proposal, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            if self.decode_control(&control, &request.proposal)?.decision
                != ControlDecision::Cancelled
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
