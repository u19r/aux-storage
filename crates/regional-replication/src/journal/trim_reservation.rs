//! Native grants and their exact immutable reservation inventory share one
//! barrier.
use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_types::{
    identity::resource::{ControlOperationId, TableLineage},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageError, StorageResult};

use super::JournalStore;
use crate::{
    history::source_retention::SourceRetentionRange,
    native_record::corrupt_record,
    trim::{proposal::PublicTrimProposal, record::TrimDecision},
};

pub(in crate::journal) struct TrimReservation {
    pub(in crate::journal) proposal: PublicTrimProposal,
    prefix: CanonicalDigest,
    grant: CanonicalDigest,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeTrimReservation {
    lineage: TableLineage,
    operation: ControlOperationId,
    content: CanonicalDigest,
    prefix: CanonicalDigest,
    qualification: CanonicalDigest,
    grant: CanonicalDigest,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrimReservationReference {
    lineage: TableLineage,
    operation: ControlOperationId,
    grant: CanonicalDigest,
}

struct TrimReservationObservation {
    reservation: TrimReservation,
    decision: TrimDecision,
    commit: Option<super::public_trim_commit::NativeTrimCommit>,
}

pub(super) enum TrimReservationGrant<'a> {
    Granted(CanonicalDigest),
    Committed(&'a super::public_trim_commit::NativeTrimCommit),
}

impl TrimReservationGrant<'_> {
    fn require_reservation(&self, reservation: &TrimReservation) -> StorageResult<()> {
        match self {
            Self::Granted(grant) if *grant == reservation.grant => Ok(()),
            Self::Granted(_) => Err(corrupt_record()),
            Self::Committed(commit) => {
                commit.require_reservation(&reservation.proposal, reservation.grant)
            }
        }
    }
}

pub(in crate::journal) enum ReservationAdmission<'a> {
    Incomplete,
    Sources(&'a [SourceRetentionRange<'a>]),
}

impl JournalStore {
    pub(in crate::journal) async fn require_reservation_admission<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        admission: ReservationAdmission<'_>,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, count) = self
                .load_journal_barrier(attempt, ReadProtection::Conflict)
                .await?;
            let blocked = self
                .load_trim_reservation_inventory(attempt, count, ReadProtection::Conflict)
                .await?
                .is_some_and(|reservation| match &admission {
                    ReservationAdmission::Incomplete => true,
                    ReservationAdmission::Sources(ranges) => ranges.iter().any(|range| {
                        reservation
                            .proposal
                            .candidate
                            .sources
                            .iter()
                            .any(|cut| range.overlaps_cut(cut))
                    }),
                });
            if blocked {
                return Err(StorageError::unsupported(
                    "History custody conflicts with a live trim reservation",
                ));
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn require_trim_reservation_count<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        count: u64,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        self.load_trim_reservation_inventory(attempt, count, protection)
            .await
            .map(|_| ())
    }

    /// Qualify the complete counted inventory before returning the selected
    /// result. Protected lookahead rejects extra entries before variable
    /// child reads.
    pub(in crate::journal) async fn load_trim_reservation_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        expected: u64,
        protection: ReadProtection,
    ) -> StorageResult<Option<TrimReservation>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if expected > crate::constants::MAX_ACTIVE_TRIM_RESERVATIONS {
                return Err(corrupt_record());
            }
            let first =
                self.record_key(crate::constants::TRIM_RESERVATION_INDEX_SUFFIX.to_vec())?;
            let last = self.record_prefix_range(&first)?.bounds().1.clone();
            let range = BoundedRecordRange::try_new(first, last, std::num::NonZeroUsize::MIN)?;
            let page = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range,
                        after: None,
                        max_bytes: self.records.limits.max_bytes(),
                        protection,
                    },
                )
                .await?;
            if page.continuation.is_some()
                || u64::try_from(page.records.len()).map_err(|_| corrupt_record())? != expected
            {
                return Err(corrupt_record());
            }
            let Some(index) = page.records.first() else {
                return Ok(None);
            };
            self.load_indexed_trim_reservation(attempt, index, protection)
                .await
                .map(Some)
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn load_indexed_trim_reservation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        index: &DurableRecord,
        protection: ReadProtection,
    ) -> StorageResult<TrimReservation> {
        let reference = self.decode_trim_reservation_reference(index)?;
        let keys = [
            self.trim_reservation_key(reference.lineage, reference.operation)?,
            self.identity_key(
                crate::constants::PUBLIC_TRIM_OPERATION_SUFFIX,
                &reference.operation,
            )?,
            self.identity_key(
                crate::constants::PUBLIC_TRIM_COMMIT_SUFFIX,
                &reference.operation,
            )?,
            self.identity_key(
                crate::constants::PUBLIC_TRIM_COMPLETION_SUFFIX,
                &reference.operation,
            )?,
            self.identity_key(
                crate::constants::PUBLIC_TRIM_PROPOSAL_SUFFIX,
                &reference.operation,
            )?,
        ];
        let rows = self.records.load(attempt, &keys, protection).await?;
        let observed = self.decode_indexed_trim_reservation(&reference, &rows)?;
        self.require_observed_trim_reservation(attempt, &observed, protection)
            .await?;
        Ok(observed.reservation)
    }

    fn decode_trim_reservation_reference(
        &self,
        index: &DurableRecord,
    ) -> StorageResult<TrimReservationReference> {
        let reference: TrimReservationReference = self.records.decode(index)?;
        if index.revision != Revision::INITIAL
            || index.key
                != self.trim_reservation_index_key(reference.lineage, reference.operation)?
        {
            return Err(corrupt_record());
        }
        Ok(reference)
    }

    fn decode_indexed_trim_reservation(
        &self,
        reference: &TrimReservationReference,
        rows: &[Option<DurableRecord>],
    ) -> StorageResult<TrimReservationObservation> {
        if rows.len() != 5 || rows[3].is_some() {
            return Err(corrupt_record());
        }
        let operation = self.decode_public_trim_record(
            rows[1].as_ref().ok_or_else(corrupt_record)?,
            rows[4].as_ref().ok_or_else(corrupt_record)?,
        )?;
        let commit = self.decode_public_trim_commit(&operation, rows[2].as_ref())?;
        let reservation = self.decode_trim_reservation(
            rows[0].as_ref().ok_or_else(corrupt_record)?,
            operation.proposal,
        )?;
        let evidence = match (operation.decision, commit.as_ref()) {
            (TrimDecision::Granted(grant), None) => TrimReservationGrant::Granted(grant),
            (TrimDecision::Committed(_) | TrimDecision::Installed(_), Some(commit)) => {
                TrimReservationGrant::Committed(commit)
            }
            _ => return Err(corrupt_record()),
        };
        evidence.require_reservation(&reservation)?;
        if reservation.proposal.candidate.lineage != reference.lineage
            || reservation.proposal.operation != reference.operation
            || reservation.grant != reference.grant
        {
            return Err(corrupt_record());
        }
        Ok(TrimReservationObservation {
            reservation,
            decision: operation.decision,
            commit,
        })
    }

    fn decode_trim_reservation(
        &self,
        row: &DurableRecord,
        proposal: PublicTrimProposal,
    ) -> StorageResult<TrimReservation> {
        let reservation: NativeTrimReservation = self.records.decode(row)?;
        if row.revision != Revision::INITIAL
            || reservation.operation != proposal.operation
            || reservation.lineage != proposal.candidate.lineage
            || row.key != self.trim_reservation_key(reservation.lineage, reservation.operation)?
            || reservation.content != self.records.content_digest(&proposal)?
            || reservation.grant
                != self.records.content_digest(&(
                    &self.region,
                    self.group,
                    self.generation,
                    &proposal,
                    reservation.prefix,
                    reservation.qualification,
                ))?
        {
            return Err(corrupt_record());
        }
        Ok(TrimReservation {
            proposal,
            prefix: reservation.prefix,
            grant: reservation.grant,
        })
    }

    pub(super) async fn prepare_trim_grant<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        evidence: (CanonicalDigest, CanonicalDigest),
    ) -> StorageResult<Option<(CanonicalDigest, Vec<RecordChange>)>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let barrier = self
                .load_reservation_barrier(attempt, ReadProtection::Conflict)
                .await?;
            let (_, count) = barrier.reservation_state();
            let exists = self
                .load_trim_reservation_inventory(attempt, count, ReadProtection::Conflict)
                .await?
                .is_some_and(|reservation| reservation.proposal.operation == proposal.operation);
            if exists {
                return Err(corrupt_record());
            }
            if count == crate::constants::MAX_ACTIVE_TRIM_RESERVATIONS {
                return Ok(None);
            }
            let grant = self.records.content_digest(&(
                &self.region,
                self.group,
                self.generation,
                proposal,
                evidence.0,
                evidence.1,
            ))?;
            let reservation = NativeTrimReservation {
                lineage: proposal.candidate.lineage,
                operation: proposal.operation,
                content: self.records.content_digest(proposal)?,
                prefix: evidence.0,
                qualification: evidence.1,
                grant,
            };
            let reference = TrimReservationReference {
                lineage: proposal.candidate.lineage,
                operation: proposal.operation,
                grant,
            };
            let keys = [
                self.trim_reservation_key(reference.lineage, reference.operation)?,
                self.trim_reservation_index_key(reference.lineage, reference.operation)?,
            ];
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            if rows.iter().any(Option::is_some) {
                return Err(corrupt_record());
            }
            Ok(Some((
                grant,
                vec![
                    self.records.prepare(keys[0].clone(), None, &reservation)?,
                    self.records.prepare(keys[1].clone(), None, &reference)?,
                    self.prepare_journal_barrier_count(
                        &barrier,
                        count.checked_add(1).ok_or_else(corrupt_record)?,
                    )?,
                ],
            )))
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn prepare_trim_reservation_release<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        grant: CanonicalDigest,
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let barrier = self
                .load_reservation_barrier(attempt, ReadProtection::Conflict)
                .await?;
            let (_, count) = barrier.reservation_state();
            let reservation = self
                .load_trim_reservation_inventory(attempt, count, ReadProtection::Conflict)
                .await?
                .filter(|reservation| reservation.proposal.operation == proposal.operation)
                .ok_or_else(corrupt_record)?;
            if reservation.proposal != *proposal || reservation.grant != grant {
                return Err(corrupt_record());
            }
            Ok(vec![
                RecordChange::Delete {
                    key: self
                        .trim_reservation_key(proposal.candidate.lineage, proposal.operation)?,
                    expected: Revision::INITIAL,
                },
                RecordChange::Delete {
                    key: self.trim_reservation_index_key(
                        proposal.candidate.lineage,
                        proposal.operation,
                    )?,
                    expected: Revision::INITIAL,
                },
                self.prepare_journal_barrier_count(
                    &barrier,
                    count.checked_sub(1).ok_or_else(corrupt_record)?,
                )?,
            ])
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn require_native_trim_reservation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        grant: TrimReservationGrant<'_>,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, count) = self.load_journal_barrier(attempt, protection).await?;
            let reservation = self
                .load_trim_reservation_inventory(attempt, count, protection)
                .await?
                .filter(|reservation| reservation.proposal == *proposal)
                .ok_or_else(corrupt_record)?;
            grant.require_reservation(&reservation)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn require_absent_trim_reservation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let keys = [
                self.trim_reservation_key(proposal.candidate.lineage, proposal.operation)?,
                self.trim_reservation_index_key(proposal.candidate.lineage, proposal.operation)?,
            ];
            if self
                .records
                .load(attempt, &keys, protection)
                .await?
                .iter()
                .any(Option::is_some)
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn require_observed_trim_reservation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        observed: &TrimReservationObservation,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let proposal = &observed.reservation.proposal;
        let local = &proposal.participant(&self.region)?.state;
        let mut keys = vec![
            self.identity_key(
                crate::constants::CONTROL_SLOT_SUFFIX,
                &local.configuration.table,
            )?,
            self.identity_key(
                crate::constants::CONTROL_PENDING_SUFFIX,
                &proposal.operation,
            )?,
            self.identity_key(
                crate::constants::PUBLIC_TRIM_SOURCE_SCAN_SUFFIX,
                &proposal.operation,
            )?,
            self.identity_key(
                crate::constants::PUBLIC_TRIM_PREPARATION_SUFFIX,
                &proposal.operation,
            )?,
            self.public_trim_grant_turn_key(proposal)?,
            self.public_trim_head_key(local.physical, proposal.candidate.lineage)?,
        ];
        let is_installation_absence_required =
            !matches!(observed.decision, TrimDecision::Installed(_));
        if is_installation_absence_required {
            keys.push(self.public_trim_installation_key(proposal)?);
        }
        let mut rows = self.records.load(attempt, &keys, protection).await?;
        if is_installation_absence_required && rows.pop().ok_or_else(corrupt_record)?.is_some() {
            return Err(corrupt_record());
        }
        let head = rows.pop().flatten().ok_or_else(corrupt_record)?;
        let authority = self
            .qualify_public_trim_head(
                attempt,
                &head,
                (local, proposal.candidate.lineage).into(),
                protection,
            )
            .await?;
        self.require_trim_reservation_ownership(observed, &rows, &authority)
    }

    fn require_trim_reservation_ownership(
        &self,
        observed: &TrimReservationObservation,
        rows: &[Option<DurableRecord>],
        authority: &super::public_trim_authority::PublicTrimAuthority,
    ) -> StorageResult<()> {
        if rows.len() != 5 {
            return Err(corrupt_record());
        }
        let reservation = &observed.reservation;
        let proposal = &reservation.proposal;
        let turn = self.decode_public_trim_grant_turn(
            rows[4].as_ref().ok_or_else(corrupt_record)?,
            proposal,
        )?;
        if let Some(commit) = &observed.commit {
            commit.require_grant_turn(&turn, &self.region)?;
        }
        let local = &proposal.participant(&self.region)?.state;
        let slot = rows[0].as_ref().ok_or_else(corrupt_record)?;
        let pending = rows[1].as_ref().ok_or_else(corrupt_record)?;
        if self.decode_control_slot(slot, local.configuration.table)?
            != super::control_slot::ControlSlotOwner::Trim(proposal.operation)
            || self.decode_pending_control_reference(pending)?
                != crate::control::pending::PendingControlReference::Trim(proposal.operation)
        {
            return Err(corrupt_record());
        }
        if let TrimDecision::Installed(digest) = observed.decision {
            self.require_installed_trim_authority(authority, proposal, digest)?;
        } else {
            authority.require_revision(proposal.expected)?;
        }
        self.require_reserved_trim_prefix(
            rows[2].as_ref().ok_or_else(corrupt_record)?,
            proposal,
            reservation.prefix,
        )?;
        if self.region == proposal.primary {
            self.require_public_trim_preparation(
                rows[3].as_ref().ok_or_else(corrupt_record)?,
                proposal,
            )?;
        } else if rows[3].is_some() {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn trim_reservation_key(
        &self,
        lineage: TableLineage,
        operation: ControlOperationId,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(
            crate::constants::TRIM_RESERVATION_SUFFIX,
            &(lineage, operation),
        )
    }
    fn trim_reservation_index_key(
        &self,
        lineage: TableLineage,
        operation: ControlOperationId,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(
            crate::constants::TRIM_RESERVATION_INDEX_SUFFIX,
            &(lineage, operation),
        )
    }
}
