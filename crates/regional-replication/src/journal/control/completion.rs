use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use replication_types::configuration::proposal::ControlProposal;
use storage_types::{ReadProtection, Revision, StorageResult};

use crate::{
    constants::CONTROL_OPERATION_SUFFIX,
    control::{ControlDecision, ControlRecord, ParticipantEvidence},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn finish_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&ControlProposal, &[ParticipantEvidence]),
    ) -> StorageResult<ControlRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (proposal, installations) = input;
            let operation = proposal.operation();
            if operation.home_region != self.region
                || installations.len() != operation.participants.len()
                || installations
                    .iter()
                    .zip(&operation.participants)
                    .any(|(receipt, region)| {
                        !receipt.matches(proposal, ControlDecision::Installed, region)
                    })
            {
                return Err(corrupt_record());
            }
            let row = self
                .load_control(attempt, proposal, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            let record = self.decode_control(&row, proposal)?;
            if installations.iter().any(|receipt| {
                receipt.bootstrap() != record.bootstrap || receipt.retirement() != record.retirement
            }) {
                return Err(corrupt_record());
            }
            self.finish_participant(
                attempt,
                (row, record),
                (ControlDecision::Installed, ControlDecision::Completed),
            )
            .await
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn complete_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        certificate: &crate::control::CompletedControl,
    ) -> StorageResult<ControlRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let row = self
                .load_control(attempt, &certificate.proposal, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            let record = self.decode_control(&row, &certificate.proposal)?;
            if record.bootstrap != certificate.bootstrap
                || record.retirement != certificate.retirement
            {
                return Err(corrupt_record());
            }
            self.finish_participant(
                attempt,
                (row, record),
                (ControlDecision::Installed, ControlDecision::Completed),
            )
            .await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn cancel_participant<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<ControlRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let row = self
                .load_control(attempt, proposal, ReadProtection::Conflict)
                .await?;
            let observed = row
                .map(|row| {
                    self.decode_control(&row, proposal)
                        .map(|record| (row, record))
                })
                .transpose()?;
            self.cancel_observed_participant(attempt, proposal, observed)
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn release_cancelled_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        certificate: &crate::control::CancelledControl,
    ) -> StorageResult<ControlRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let proposal = &certificate.proposal;
            let row = self
                .load_control(attempt, proposal, ReadProtection::Conflict)
                .await?;
            let observed = row
                .map(|row| {
                    self.decode_control(&row, proposal)
                        .map(|record| (row, record))
                })
                .transpose()?;
            if let Some((_, record)) = &observed
                && (record.decision == ControlDecision::Prepared || record.bootstrap.is_some())
                && record.bootstrap != certificate.bootstrap
            {
                return Err(corrupt_record());
            }
            self.cancel_observed_participant(attempt, proposal, observed)
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn cancel_observed_participant<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        observed: Option<(DurableRecord, ControlRecord)>,
    ) -> StorageResult<ControlRecord> {
        let Some(observed) = observed else {
            let record = ControlRecord {
                proposal: proposal.clone(),
                decision: ControlDecision::Cancelled,
                bootstrap: None,
                retirement: Vec::new(),
            };
            let key = self.identity_key(CONTROL_OPERATION_SUFFIX, &proposal.operation().id)?;
            let change = self.records.prepare(key, None, &record)?;
            self.records.store(attempt, &[change]).await?;
            return Ok(record);
        };
        self.finish_participant(
            attempt,
            observed,
            (ControlDecision::Prepared, ControlDecision::Cancelled),
        )
        .await
    }

    async fn finish_participant<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        observed: (DurableRecord, ControlRecord),
        transition: (ControlDecision, ControlDecision),
    ) -> StorageResult<ControlRecord> {
        let (row, mut record) = observed;
        let (expected, next) = transition;
        if record.decision == next {
            return Ok(record);
        }
        if record.decision != expected {
            return Err(corrupt_record());
        }
        record.decision = next;
        self.store_control_transition(attempt, (row.revision, &record))
            .await?;
        self.release_control(attempt, &record.proposal).await?;
        Ok(record)
    }

    pub(super) async fn store_control_transition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (Revision, &ControlRecord),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (revision, record) = input;
            let key =
                self.identity_key(CONTROL_OPERATION_SUFFIX, &record.proposal.operation().id)?;
            let change = self.records.prepare(key, Some(revision), record)?;
            self.records.store(attempt, &[change]).await
        }
        .await;
        settlement.complete(result)
    }
}
