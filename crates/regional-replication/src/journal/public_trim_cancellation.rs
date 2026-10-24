//! Primary cancellation stays discoverable until all participant tombstones
//! settle.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::RecordChange};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::{JournalStore, control_slot::ControlSlotOwner, public_trim::TrimOwnership};
use crate::{
    constants::{CONTROL_PENDING_SUFFIX, PUBLIC_TRIM_SOURCE_SCAN_SUFFIX},
    control::pending::PendingControlReference,
    native_record::corrupt_record,
    trim::{
        proposal::PublicTrimProposal,
        record::{TrimDecision, TrimObservation},
    },
};

impl JournalStore {
    pub(super) async fn cancel_public_trim_operation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        observed: (
            Option<TrimObservation>,
            Option<Revision>,
            Option<TrimOwnership>,
        ),
    ) -> StorageResult<Option<TrimObservation>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (record, revision, ownership) = observed;
            let Some(mut record) = record else {
                return self
                    .record_absent_trim_cancellation(attempt, proposal)
                    .await;
            };
            if record.decision == TrimDecision::Cancelled {
                return Ok(Some(record));
            }
            self.cancel_public_trim(
                attempt,
                (&mut record, revision.ok_or_else(corrupt_record)?),
                ownership.ok_or_else(corrupt_record)?,
            )
            .await?;
            Ok(Some(record))
        }
        .await;
        settlement.complete(result)
    }

    async fn cancel_public_trim<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&mut TrimObservation, Revision),
        ownership: TrimOwnership,
    ) -> StorageResult<()> {
        let (record, revision) = input;
        let grant = match record.decision {
            TrimDecision::Preparing => None,
            TrimDecision::Granted(grant) => Some(grant),
            TrimDecision::Committed(_)
            | TrimDecision::Installed(_)
            | TrimDecision::Completed(_) => {
                return Err(storage_types::StorageError::unsupported(
                    "Committed trim must roll forward",
                ));
            }
            TrimDecision::Cancelled => return Err(corrupt_record()),
        };
        let proposal = &record.proposal;
        let table = proposal
            .participant(&self.region)?
            .state
            .configuration
            .table;
        record.released_grant = grant;
        record.decision = TrimDecision::Cancelled;
        let mut changes = vec![
            self.prepare_public_trim_record(record, Some(revision))?,
            self.prepare_control_slot(table, Some(ownership.slot), ControlSlotOwner::Idle)?,
        ];
        if self.region != record.proposal.primary {
            changes.push(RecordChange::Delete {
                key: self.identity_key(CONTROL_PENDING_SUFFIX, &record.proposal.operation)?,
                expected: ownership.pending,
            });
        }
        if let Some(scan) = ownership.scan {
            changes.push(RecordChange::Delete {
                key: self
                    .identity_key(PUBLIC_TRIM_SOURCE_SCAN_SUFFIX, &record.proposal.operation)?,
                expected: scan.revision,
            });
        }
        if let Some(revision) = ownership.preparation {
            changes.push(RecordChange::Delete {
                key: self.identity_key(
                    crate::constants::PUBLIC_TRIM_PREPARATION_SUFFIX,
                    &record.proposal.operation,
                )?,
                expected: revision,
            });
        }
        if let Some(revision) = ownership.authorization {
            changes.push(RecordChange::Delete {
                key: self.public_trim_grant_turn_key(proposal)?,
                expected: revision,
            });
        }
        if let Some(grant) = grant {
            changes.extend(
                self.prepare_trim_reservation_release(attempt, proposal, grant)
                    .await?,
            );
        }
        self.records.store(attempt, &changes).await
    }

    async fn record_absent_trim_cancellation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
    ) -> StorageResult<Option<TrimObservation>> {
        self.require_public_trim_configuration(attempt, proposal)
            .await?;
        let capacity = self.records.observe_discretionary_capacity(attempt).await?;
        let record = TrimObservation {
            proposal: proposal.clone(),
            decision: TrimDecision::Cancelled,
            released_grant: None,
        };
        let mut changes = Vec::from(self.prepare_new_public_trim_record(&record)?);
        if self.region == proposal.primary {
            changes.push(self.records.prepare(
                self.identity_key(CONTROL_PENDING_SUFFIX, &proposal.operation)?,
                None,
                &PendingControlReference::Trim(proposal.operation),
            )?);
        }
        self.records.store(attempt, &changes).await?;
        self.records
            .require_discretionary_capacity(attempt, capacity)
            .await?;
        Ok(Some(record))
    }

    pub(super) async fn require_absent_trim_ownership<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_absent_trim_reservation(attempt, proposal, protection)
                .await?;
            self.require_absent_trim_installation(attempt, proposal, protection)
                .await?;
            let table = proposal
                .participant(&self.region)?
                .state
                .configuration
                .table;
            let keys = [
                self.identity_key(crate::constants::CONTROL_SLOT_SUFFIX, &table)?,
                self.identity_key(CONTROL_PENDING_SUFFIX, &proposal.operation)?,
                self.identity_key(PUBLIC_TRIM_SOURCE_SCAN_SUFFIX, &proposal.operation)?,
                self.identity_key(
                    crate::constants::PUBLIC_TRIM_PREPARATION_SUFFIX,
                    &proposal.operation,
                )?,
                self.identity_key(
                    crate::constants::PUBLIC_TRIM_COMMIT_SUFFIX,
                    &proposal.operation,
                )?,
                self.identity_key(
                    crate::constants::PUBLIC_TRIM_COMPLETION_SUFFIX,
                    &proposal.operation,
                )?,
                self.public_trim_grant_turn_key(proposal)?,
            ];
            let rows = self.records.load(attempt, &keys, protection).await?;
            if rows[1..].iter().any(Option::is_some) {
                return Err(corrupt_record());
            }
            if rows[0]
                .as_ref()
                .map(|row| self.decode_control_slot(row, table))
                .transpose()?
                == Some(ControlSlotOwner::Trim(proposal.operation))
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn release_public_trim_cancellation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &TrimObservation,
        receipts: &[crate::transport::RemoteTrimReceipt],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let proposal = &record.proposal;
            if record.decision != TrimDecision::Cancelled {
                return Err(corrupt_record());
            }
            let regions = proposal
                .participants
                .iter()
                .filter(|participant| participant.region != self.region);
            if receipts.len() != proposal.participants.len() - 1 {
                return Err(corrupt_record());
            }
            for (participant, receipt) in regions.zip(receipts) {
                receipt.require_decision(proposal, &participant.region, TrimDecision::Cancelled)?;
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
