use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::CONTROL_SLOT_SUFFIX,
    control::{ControlDecision, ControlRecord, ParticipantEvidence},
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn decide_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&ControlProposal, Option<&[ParticipantEvidence]>),
    ) -> StorageResult<ControlRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (operation, grants) = input;
            self.require_control_primary(operation, grants)?;
            if grants.is_some() {
                self.require_received_removal_seals(attempt, operation)
                    .await?;
            }
            let row = self
                .load_control(attempt, operation, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            let mut record = self.decode_control(&row, operation)?;
            if grants.is_some_and(|grants| {
                grants
                    .iter()
                    .any(|grant| grant.bootstrap() != record.bootstrap)
            }) {
                return Err(corrupt_record());
            }
            let decision = if grants.is_some() {
                ControlDecision::Committed
            } else {
                ControlDecision::Cancelled
            };
            if record.decision == decision
                || (decision == ControlDecision::Committed
                    && matches!(
                        record.decision,
                        ControlDecision::Installed | ControlDecision::Completed
                    ))
            {
                return Ok(record);
            }
            if record.decision != ControlDecision::Prepared {
                return Err(corrupt_record());
            }
            if grants.is_some() {
                record.retirement = self.load_retirement_evidence(attempt, operation).await?;
            }
            record.decision = decision;
            self.store_control_transition(attempt, (row.revision, &record))
                .await?;
            if decision == ControlDecision::Cancelled {
                self.release_control(attempt, operation).await?;
            }
            Ok(record)
        }
        .await;
        settlement.complete(result)
    }

    fn require_control_primary(
        &self,
        operation: &ControlProposal,
        grants: Option<&[ParticipantEvidence]>,
    ) -> StorageResult<()> {
        if operation.operation().home_region != self.region {
            return Err(corrupt_record());
        }
        if let Some(grants) = grants
            && (grants.len() != operation.operation().participants.len()
                || grants
                    .iter()
                    .zip(&operation.operation().participants)
                    .any(|(grant, region)| {
                        !grant.matches(operation, ControlDecision::Prepared, region)
                    }))
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) async fn release_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: &ControlProposal,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let table = operation
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
            if active != ControlSlotOwner::Configuration(operation.operation().id) {
                return Err(corrupt_record());
            }
            let change =
                self.prepare_control_slot(table, Some(row.revision), ControlSlotOwner::Idle)?;
            self.records.store(attempt, &[change]).await
        }
        .await;
        settlement.complete(result)
    }
}
