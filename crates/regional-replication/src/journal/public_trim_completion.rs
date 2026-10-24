//! Terminal evidence precedes atomic counted release; primary recovery outlives
//! Finish delivery.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, RecordChange},
};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::{JournalStore, control_slot::ControlSlotOwner, public_trim::TrimOwnership};
use crate::{
    constants::{
        CONTROL_PENDING_SUFFIX, PUBLIC_TRIM_COMPLETION_SUFFIX, PUBLIC_TRIM_PREPARATION_SUFFIX,
        PUBLIC_TRIM_SOURCE_SCAN_SUFFIX,
    },
    control::pending::PendingControlReference,
    native_record::corrupt_record,
    transport::RemoteTrimReceipt,
    trim::{
        completion::TrimCompletionCertificate,
        executor::TrimRequest,
        record::{TrimDecision, TrimObservation, TrimOutcome},
    },
};

impl JournalStore {
    pub(super) async fn execute_terminal_trim<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &TrimRequest,
        observed: super::public_trim::TrimOperationObservation,
    ) -> StorageResult<TrimOutcome> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut record = observed.record.ok_or_else(corrupt_record)?;
            let mut completion_released = observed.cancellation_released;
            match request {
                TrimRequest::Complete { .. } | TrimRequest::Finish(_) => {
                    self.complete_public_trim(
                        attempt,
                        request,
                        (&mut record, observed.revision.ok_or_else(corrupt_record)?),
                        observed.ownership,
                    )
                    .await?
                }
                TrimRequest::ReleaseCompletion { receipts, .. } => {
                    self.release_public_trim_completion(attempt, &record, receipts)
                        .await?;
                    completion_released = true;
                }
                _ => return Err(corrupt_record()),
            }
            Ok(TrimOutcome {
                grant_turn: None,
                observation: Some(record),
                certificate: None,
                installation: None,
                completion: None,
                work: crate::trim::record::TrimWorkState::Waiting,
                participants_prepared: false,
                cancellation_released: false,
                completion_released,
                prefix: None,
            })
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn qualify_public_trim_outcome<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        outcome: &mut TrimOutcome,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(record) = &outcome.observation else {
                return Ok(());
            };
            outcome.grant_turn = self
                .load_public_trim_grant_turn(attempt, record, ReadProtection::Snapshot)
                .await?;
            outcome.certificate = self
                .load_public_trim_commit(attempt, record, ReadProtection::Snapshot)
                .await?
                .map(super::public_trim_commit::NativeTrimCommit::into_certificate);
            if let (Some(certificate), Some(turn)) = (&outcome.certificate, &outcome.grant_turn) {
                certificate.require_grant_turn(turn, &self.region)?;
            }
            if matches!(
                record.decision,
                TrimDecision::Installed(_) | TrimDecision::Completed(_)
            ) {
                outcome.installation = Some(
                    self.load_trim_installation_acknowledgement(
                        attempt,
                        outcome.certificate.as_ref().ok_or_else(corrupt_record)?,
                        ReadProtection::Snapshot,
                    )
                    .await?,
                );
            }
            if matches!(record.decision, TrimDecision::Completed(_)) {
                outcome.completion_released |= outcome.cancellation_released;
                outcome.cancellation_released = false;
                outcome.completion = Some(
                    self.load_public_trim_completion(attempt, record, ReadProtection::Snapshot)
                        .await?,
                );
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_public_trim_completion<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &TrimObservation,
        protection: ReadProtection,
    ) -> StorageResult<TrimCompletionCertificate> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key =
                self.identity_key(PUBLIC_TRIM_COMPLETION_SUFFIX, &record.proposal.operation)?;
            let row = self
                .records
                .load(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let completion: TrimCompletionCertificate = self.records.decode(&row)?;
            completion.certificate.require_proposal(&record.proposal)?;
            completion.require_integrity(self.records.content_digest(&completion.certificate)?)?;
            if row.key != key
                || row.revision != Revision::INITIAL
                || record.decision
                    != TrimDecision::Completed(self.records.content_digest(&completion)?)
            {
                return Err(corrupt_record());
            }
            Ok(completion)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_absent_trim_completion<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &TrimObservation,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key =
                self.identity_key(PUBLIC_TRIM_COMPLETION_SUFFIX, &record.proposal.operation)?;
            if self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .is_some()
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn qualify_terminal_trim_custody<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &TrimObservation,
        rows: &[Option<DurableRecord>],
        protection: ReadProtection,
    ) -> StorageResult<Option<bool>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if rows.len() != 5 {
                return Err(corrupt_record());
            }
            if matches!(record.decision, TrimDecision::Completed(_)) {
                return self
                    .require_completed_trim_ownership(attempt, record, rows, protection)
                    .await
                    .map(Some);
            }
            self.require_absent_trim_completion(attempt, record, protection)
                .await?;
            if matches!(record.decision, TrimDecision::Installed(_)) {
                return Ok(None);
            }
            let proposal = &record.proposal;
            self.require_absent_trim_installation(attempt, proposal, protection)
                .await?;
            if record.decision != TrimDecision::Cancelled {
                return Ok(None);
            }
            self.require_absent_trim_reservation(attempt, proposal, protection)
                .await?;
            let table = proposal
                .participant(&self.region)?
                .state
                .configuration
                .table;
            let pending = rows[1]
                .as_ref()
                .map(|row| self.decode_pending_control_reference(row))
                .transpose()?;
            if rows[0]
                .as_ref()
                .map(|row| self.decode_control_slot(row, table))
                .transpose()?
                == Some(ControlSlotOwner::Trim(proposal.operation))
                || rows[2].is_some()
                || rows[3].is_some()
                || rows[4].is_some()
                || pending.is_some_and(|pending| {
                    self.region != proposal.primary
                        || pending != PendingControlReference::Trim(proposal.operation)
                })
            {
                return Err(corrupt_record());
            }
            Ok(Some(self.region == proposal.primary && pending.is_none()))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_completed_trim_ownership<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &TrimObservation,
        rows: &[Option<DurableRecord>],
        protection: ReadProtection,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let proposal = &record.proposal;
            let completion = self
                .load_public_trim_completion(attempt, record, protection)
                .await?;
            let installed = self
                .load_trim_installation_acknowledgement(
                    attempt,
                    &completion.certificate,
                    protection,
                )
                .await?;
            completion.require_local(&installed)?;
            let (_, count) = self.load_journal_barrier(attempt, protection).await?;
            self.require_trim_reservation_count(attempt, count, protection)
                .await?;
            self.require_absent_trim_reservation(attempt, proposal, protection)
                .await?;
            let table = proposal
                .participant(&self.region)?
                .state
                .configuration
                .table;
            if rows[0]
                .as_ref()
                .map(|row| self.decode_control_slot(row, table))
                .transpose()?
                == Some(ControlSlotOwner::Trim(proposal.operation))
                || rows[2].is_some()
                || rows[3].is_some()
                || rows[4].is_some()
            {
                return Err(corrupt_record());
            }
            let pending = rows[1]
                .as_ref()
                .map(|row| self.decode_pending_control_reference(row))
                .transpose()?;
            if pending.is_some_and(|pending| {
                self.region != proposal.primary
                    || pending != PendingControlReference::Trim(proposal.operation)
            }) {
                return Err(corrupt_record());
            }
            Ok(self.region == proposal.primary && pending.is_none())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn complete_public_trim<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &TrimRequest,
        input: (&mut TrimObservation, Revision),
        ownership: Option<TrimOwnership>,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (record, revision) = input;
            let completion = self
                .qualify_trim_completion(attempt, request, record)
                .await?;
            if matches!(record.decision, TrimDecision::Completed(_)) {
                return Ok(());
            }
            let ownership = ownership.ok_or_else(corrupt_record)?;
            let proposal = &record.proposal;
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            let mut changes = self
                .prepare_trim_reservation_release(
                    attempt,
                    proposal,
                    completion.certificate.grant_for(&self.region)?,
                )
                .await?;
            changes.extend(self.prepare_completed_trim_custody(proposal, ownership)?);
            changes.push(self.records.prepare(
                self.identity_key(PUBLIC_TRIM_COMPLETION_SUFFIX, &proposal.operation)?,
                None,
                &completion,
            )?);
            record.decision = TrimDecision::Completed(self.records.content_digest(&completion)?);
            changes.push(self.prepare_public_trim_record(record, Some(revision))?);
            self.records.store(attempt, &changes).await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn qualify_trim_completion<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &TrimRequest,
        record: &TrimObservation,
    ) -> StorageResult<TrimCompletionCertificate> {
        let completion = match request {
            TrimRequest::Finish(finish) => finish.completion.clone(),
            TrimRequest::Complete { .. }
                if matches!(record.decision, TrimDecision::Completed(_)) =>
            {
                return self
                    .load_public_trim_completion(attempt, record, ReadProtection::Conflict)
                    .await;
            }
            TrimRequest::Complete { receipts, .. } => {
                self.build_trim_completion(attempt, record, receipts)
                    .await?
            }
            _ => return Err(corrupt_record()),
        };
        completion.certificate.require_proposal(&record.proposal)?;
        completion.require_integrity(self.records.content_digest(&completion.certificate)?)?;
        let installed = self
            .load_trim_installation_acknowledgement(
                attempt,
                &completion.certificate,
                ReadProtection::Conflict,
            )
            .await?;
        completion.require_local(&installed)?;
        match record.decision {
            TrimDecision::Installed(digest) if digest == installed.certificate => {}
            TrimDecision::Completed(digest)
                if digest == self.records.content_digest(&completion)? =>
            {
                if self
                    .load_public_trim_completion(attempt, record, ReadProtection::Conflict)
                    .await?
                    != completion
                {
                    return Err(corrupt_record());
                }
            }
            _ => return Err(corrupt_record()),
        }
        Ok(completion)
    }

    async fn build_trim_completion<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &TrimObservation,
        receipts: &[RemoteTrimReceipt],
    ) -> StorageResult<TrimCompletionCertificate> {
        let certificate = self
            .load_public_trim_commit(attempt, record, ReadProtection::Conflict)
            .await?
            .ok_or_else(corrupt_record)?
            .into_certificate();
        let local = self
            .load_trim_installation_acknowledgement(attempt, &certificate, ReadProtection::Conflict)
            .await?;
        let proposal = &record.proposal;
        if self.region != proposal.primary || receipts.len() != proposal.participants.len() - 1 {
            return Err(corrupt_record());
        }
        let mut remote = receipts.iter();
        let mut installations = Vec::with_capacity(proposal.participants.len());
        for participant in &proposal.participants {
            installations.push(if participant.region == self.region {
                local.clone()
            } else {
                remote
                    .next()
                    .ok_or_else(corrupt_record)?
                    .require_installation(proposal, &participant.region, local.certificate)?
                    .clone()
            });
        }
        let completion = TrimCompletionCertificate {
            certificate,
            installations,
        };
        completion.require_integrity(local.certificate)?;
        Ok(completion)
    }

    fn prepare_completed_trim_custody(
        &self,
        proposal: &crate::trim::proposal::PublicTrimProposal,
        ownership: TrimOwnership,
    ) -> StorageResult<Vec<RecordChange>> {
        let table = proposal
            .participant(&self.region)?
            .state
            .configuration
            .table;
        let scan = ownership.scan.ok_or_else(corrupt_record)?;
        let mut changes = vec![
            self.prepare_control_slot(table, Some(ownership.slot), ControlSlotOwner::Idle)?,
            RecordChange::Delete {
                key: self.identity_key(PUBLIC_TRIM_SOURCE_SCAN_SUFFIX, &proposal.operation)?,
                expected: scan.revision,
            },
            RecordChange::Delete {
                key: self.public_trim_grant_turn_key(proposal)?,
                expected: ownership.authorization.ok_or_else(corrupt_record)?,
            },
        ];
        if self.region == proposal.primary {
            changes.push(RecordChange::Delete {
                key: self.identity_key(PUBLIC_TRIM_PREPARATION_SUFFIX, &proposal.operation)?,
                expected: ownership.preparation.ok_or_else(corrupt_record)?,
            });
        } else {
            if ownership.preparation.is_some() {
                return Err(corrupt_record());
            }
            changes.push(RecordChange::Delete {
                key: self.identity_key(CONTROL_PENDING_SUFFIX, &proposal.operation)?,
                expected: ownership.pending,
            });
        }
        Ok(changes)
    }

    pub(super) async fn release_public_trim_completion<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &TrimObservation,
        receipts: &[RemoteTrimReceipt],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let TrimDecision::Completed(digest) = record.decision else {
                return Err(corrupt_record());
            };
            let proposal = &record.proposal;
            if self.region != proposal.primary || receipts.len() != proposal.participants.len() - 1
            {
                return Err(corrupt_record());
            }
            let participants = proposal
                .participants
                .iter()
                .filter(|participant| participant.region != self.region);
            for (participant, receipt) in participants.zip(receipts) {
                receipt.require_decision(
                    proposal,
                    &participant.region,
                    TrimDecision::Completed(digest),
                )?;
            }
            let key = self.identity_key(CONTROL_PENDING_SUFFIX, &proposal.operation)?;
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
            let Some(row) = row else {
                return Ok(());
            };
            if self.decode_pending_control_reference(&row)?
                != PendingControlReference::Trim(proposal.operation)
            {
                return Err(corrupt_record());
            }
            self.records
                .store(
                    attempt,
                    &[RecordChange::Delete {
                        key,
                        expected: row.revision,
                    }],
                )
                .await
        }
        .await;
        settlement.complete(result)
    }
}
