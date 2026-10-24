//! Native operation/slot/pending transitions precede every trim grant.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, Revision, StorageError, StorageResult};

use super::{JournalStore, control_slot::ControlSlotOwner};
use crate::{
    constants::{
        CONTROL_PENDING_SUFFIX, CONTROL_SLOT_SUFFIX, PUBLIC_TRIM_OPERATION_SUFFIX,
        PUBLIC_TRIM_PROPOSAL_SUFFIX,
    },
    control::pending::PendingControlReference,
    history::table_cut::{CanonicalTableCutAction, CanonicalTableCutRequest},
    native_record::corrupt_record,
    trim::{
        executor::TrimRequest,
        proposal::PublicTrimProposal,
        record::{TrimDecision, TrimObservation, TrimOutcome, TrimWorkState},
    },
};

pub(super) struct TrimOwnership {
    pub(super) slot: Revision,
    pub(super) pending: Revision,
    pub(super) scan: Option<super::public_trim_scan::TrimSourceScanObservation>,
    pub(super) preparation: Option<Revision>,
    pub(super) authorization: Option<Revision>,
}

pub(super) struct TrimOperationObservation {
    pub(super) record: Option<TrimObservation>,
    pub(super) revision: Option<Revision>,
    pub(super) ownership: Option<TrimOwnership>,
    pub(super) cancellation_released: bool,
}

impl JournalStore {
    pub(crate) async fn execute_trim_operation<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &TrimRequest,
            Option<&crate::trim::time::age::QualifiedPrefixAge>,
        ),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<TrimOutcome> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut outcome = self.execute_public_trim(attempt, input.0, encoding).await?;
            if let Some(age) = input.1 {
                outcome = self
                    .grant_public_trim_age(attempt, input.0, outcome, age)
                    .await?;
            }
            self.qualify_public_trim_outcome(attempt, &mut outcome)
                .await?;
            Ok(outcome)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn execute_public_trim<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &TrimRequest,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<TrimOutcome> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let proposal = request.proposal();
            request.require_region(&self.region)?;
            let protection = if matches!(request, TrimRequest::Observe(_)) {
                ReadProtection::Snapshot
            } else {
                ReadProtection::Conflict
            };
            let observed = self
                .observe_public_trim_operation(attempt, proposal, protection)
                .await?;
            if matches!(request, TrimRequest::Prepare(_) | TrimRequest::Advance(_))
                && observed
                    .record
                    .as_ref()
                    .is_none_or(|record| record.decision == TrimDecision::Preparing)
            {
                self.require_public_trim_policy(proposal)?;
            }
            if matches!(
                request,
                TrimRequest::Complete { .. }
                    | TrimRequest::Finish(_)
                    | TrimRequest::ReleaseCompletion { .. }
            ) {
                return self.execute_terminal_trim(attempt, request, observed).await;
            }
            if matches!(
                request,
                TrimRequest::Commit { .. }
                    | TrimRequest::InstallPrimary(_)
                    | TrimRequest::InstallParticipant(_)
            ) {
                return self
                    .execute_certified_trim(attempt, request, observed)
                    .await;
            }
            if matches!(request, TrimRequest::Advance(_)) {
                return self
                    .advance_public_trim_operation(attempt, proposal, encoding, observed)
                    .await;
            }
            let TrimOperationObservation {
                record: existing,
                revision,
                ownership,
                mut cancellation_released,
            } = observed;
            let participants_prepared = ownership
                .as_ref()
                .is_some_and(|owned| owned.preparation.is_some());
            let observation = match request {
                TrimRequest::Observe(_) => Ok(existing),
                TrimRequest::AuthorizeGrant(authorization) => {
                    let record = existing.ok_or_else(corrupt_record)?;
                    self.authorize_public_trim_grant(attempt, authorization, &record)
                        .await?;
                    Ok(Some(record))
                }
                TrimRequest::Commit { .. }
                | TrimRequest::InstallPrimary(_)
                | TrimRequest::InstallParticipant(_)
                | TrimRequest::Advance(_)
                | TrimRequest::Complete { .. }
                | TrimRequest::Finish(_)
                | TrimRequest::ReleaseCompletion { .. } => return Err(corrupt_record()),
                TrimRequest::Prepare(_) => match existing {
                    Some(record) => Ok(Some(record)),
                    None => self
                        .prepare_public_trim(attempt, proposal, encoding)
                        .await
                        .map(Some),
                },
                TrimRequest::ReleaseCancellation { receipts, .. } => {
                    let record = existing.ok_or_else(corrupt_record)?;
                    self.release_public_trim_cancellation(attempt, &record, receipts)
                        .await?;
                    cancellation_released = true;
                    Ok(Some(record))
                }
                TrimRequest::Cancel(_) | TrimRequest::ReleaseParticipantCancellation(_) => {
                    self.cancel_public_trim_operation(
                        attempt,
                        proposal,
                        (existing, revision, ownership),
                    )
                    .await
                }
                TrimRequest::RecordPreparation { receipts, .. } => {
                    let record = existing.ok_or_else(corrupt_record)?;
                    let participants_prepared = self
                        .record_public_trim_preparation(
                            attempt,
                            (&record, receipts),
                            ownership.and_then(|owned| owned.preparation),
                        )
                        .await?;
                    return Ok(TrimOutcome {
                        grant_turn: None,
                        certificate: None,
                        installation: None,
                        completion: None,
                        completion_released: false,
                        observation: Some(record),
                        work: TrimWorkState::Waiting,
                        participants_prepared,
                        cancellation_released,
                        prefix: None,
                    });
                }
            }?;
            Ok(TrimOutcome {
                grant_turn: None,
                certificate: None,
                installation: None,
                completion: None,
                completion_released: false,
                observation,
                work: TrimWorkState::Waiting,
                participants_prepared: participants_prepared
                    && !matches!(
                        request,
                        TrimRequest::Cancel(_) | TrimRequest::ReleaseParticipantCancellation(_)
                    ),
                cancellation_released,
                prefix: None,
            })
        }
        .await;
        settlement.complete(result)
    }

    async fn execute_certified_trim<A: CatalogAttempt + PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &TrimRequest,
        observed: TrimOperationObservation,
    ) -> StorageResult<TrimOutcome> {
        let mut record = observed.record.ok_or_else(corrupt_record)?;
        let participants_prepared = observed
            .ownership
            .as_ref()
            .is_some_and(|owned| owned.preparation.is_some());
        match request {
            TrimRequest::Commit { receipts, .. } => {
                self.commit_public_trim(attempt, &mut record, receipts)
                    .await?
            }
            TrimRequest::InstallPrimary(_) | TrimRequest::InstallParticipant(_) => {
                self.install_public_trim(
                    attempt,
                    request,
                    (&mut record, observed.revision.ok_or_else(corrupt_record)?),
                )
                .await?
            }
            _ => return Err(corrupt_record()),
        }
        Ok(TrimOutcome {
            grant_turn: None,
            observation: Some(record),
            certificate: None,
            installation: None,
            completion: None,
            work: TrimWorkState::Waiting,
            participants_prepared,
            cancellation_released: false,
            completion_released: false,
            prefix: None,
        })
    }

    async fn observe_public_trim_operation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        protection: ReadProtection,
    ) -> StorageResult<TrimOperationObservation> {
        let keys = [
            self.identity_key(PUBLIC_TRIM_OPERATION_SUFFIX, &proposal.operation)?,
            self.identity_key(PUBLIC_TRIM_PROPOSAL_SUFFIX, &proposal.operation)?,
        ];
        let mut rows = self.records.load(attempt, &keys, protection).await?;
        let row = rows[0].take();
        let proposal_row = rows[1].take();
        let record = match (&row, &proposal_row) {
            (Some(row), Some(proposal_row)) => {
                Some(self.decode_public_trim_record(row, proposal_row)?)
            }
            (None, None) => None,
            _ => return Err(corrupt_record()),
        };
        let (ownership, cancellation_released) = if let Some(record) = &record {
            record.require_proposal(proposal)?;
            self.observe_public_trim_ownership(attempt, record, protection)
                .await?
        } else {
            self.require_absent_trim_ownership(attempt, proposal, protection)
                .await?;
            (None, false)
        };
        Ok(TrimOperationObservation {
            record,
            revision: row.as_ref().map(|row| row.revision),
            ownership,
            cancellation_released,
        })
    }

    async fn advance_public_trim_operation<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
        observed: TrimOperationObservation,
    ) -> StorageResult<TrimOutcome> {
        let TrimOperationObservation {
            record,
            ownership,
            cancellation_released,
            ..
        } = observed;
        let Some(record) = record else {
            let record = self
                .prepare_public_trim(attempt, proposal, encoding)
                .await?;
            return Ok(TrimOutcome {
                grant_turn: None,
                certificate: None,
                installation: None,
                completion: None,
                completion_released: false,
                observation: Some(record),
                work: TrimWorkState::Ready,
                participants_prepared: false,
                cancellation_released: false,
                prefix: None,
            });
        };
        let participants_prepared = ownership
            .as_ref()
            .is_some_and(|owned| owned.preparation.is_some());
        let scan = match record.decision {
            TrimDecision::Preparing => {
                self.advance_public_trim_source_scan(
                    attempt,
                    proposal,
                    encoding,
                    ownership.ok_or_else(corrupt_record)?.scan,
                )
                .await?
            }
            TrimDecision::Granted(_)
            | TrimDecision::Committed(_)
            | TrimDecision::Installed(_)
            | TrimDecision::Completed(_)
            | TrimDecision::Cancelled => super::public_trim_scan::TrimScanOutcome {
                work: TrimWorkState::Waiting,
                prefix: None,
            },
        };
        Ok(TrimOutcome {
            grant_turn: None,
            certificate: None,
            installation: None,
            completion: None,
            completion_released: false,
            observation: Some(record),
            work: scan.work,
            participants_prepared,
            cancellation_released,
            prefix: scan.prefix,
        })
    }

    async fn prepare_public_trim<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<TrimObservation> {
        self.require_public_trim_scope(attempt, proposal, encoding)
            .await?;
        let capacity = self.records.observe_discretionary_capacity(attempt).await?;
        let local = proposal.participant(&self.region)?;
        let table = local.state.configuration.table;
        let slot = self.identity_key(CONTROL_SLOT_SUFFIX, &table)?;
        let pending = self.identity_key(CONTROL_PENDING_SUFFIX, &proposal.operation)?;
        let rows = self
            .records
            .load(attempt, &[slot, pending.clone()], ReadProtection::Conflict)
            .await?;
        let slot = rows[0].as_ref().ok_or_else(corrupt_record)?;
        if self.decode_control_slot(slot, table)? != ControlSlotOwner::Idle {
            return Err(StorageError::unsupported("Table control is in progress"));
        }
        if rows[1].is_some() {
            return Err(corrupt_record());
        }
        let record = TrimObservation {
            proposal: proposal.clone(),
            decision: TrimDecision::Preparing,
            released_grant: None,
        };
        let mut changes = Vec::from(self.prepare_new_public_trim_record(&record)?);
        changes.push(self.prepare_control_slot(
            table,
            Some(slot.revision),
            ControlSlotOwner::Trim(proposal.operation),
        )?);
        changes.push(self.records.prepare(
            pending,
            None,
            &PendingControlReference::Trim(proposal.operation),
        )?);
        self.records.store(attempt, &changes).await?;
        self.records
            .require_discretionary_capacity(attempt, capacity)
            .await?;
        Ok(record)
    }

    pub(super) async fn require_public_trim_scope<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let local = proposal.participant(&self.region)?;
            self.require_local_source(&local.state)?;
            let observed = self
                .observe_table_state(
                    attempt,
                    local.state.configuration.table,
                    ReadProtection::Conflict,
                )
                .await?
                .ok_or_else(corrupt_record)?;
            if observed.state() != &local.state {
                return Err(StorageError::validation("Trim table state changed"));
            }
            let authority = self
                .load_public_trim_head(
                    attempt,
                    (&local.state, proposal.candidate.lineage),
                    ReadProtection::Conflict,
                )
                .await?;
            authority.require_revision(proposal.expected)?;
            self.require_public_trim_configuration(attempt, proposal)
                .await?;
            let request = CanonicalTableCutRequest {
                serving_region: self.region.clone(),
                serving_group: self.group,
                table: local.state.name.clone(),
                table_cut: CanonicalTableCutAction::Verify(proposal.candidate.clone()),
            };
            let vector = self
                .load_table_source_vector(attempt, &request, encoding, ReadProtection::Conflict)
                .await?;
            if vector != proposal.candidate {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_public_trim_configuration<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let local = proposal.participant(&self.region)?;
            let current = self
                .load_configuration_control(attempt, &local.state, ReadProtection::Shared)
                .await?;
            if current
                .proposal
                .participants()
                .iter()
                .filter(|participant| {
                    participant.proposed.phase
                        == replication_types::configuration::installed::RegionalTablePhase::Active
                })
                .count()
                != proposal.participants.len()
            {
                return Err(corrupt_record());
            }
            for participant in &proposal.participants {
                if current
                    .proposal
                    .participant(&participant.region)
                    .map_err(|_| corrupt_record())?
                    .proposed
                    != participant.state
                {
                    return Err(corrupt_record());
                }
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn observe_public_trim_ownership<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &TrimObservation,
        protection: ReadProtection,
    ) -> StorageResult<(Option<TrimOwnership>, bool)> {
        let proposal = &record.proposal;
        let local = proposal.participant(&self.region)?;
        let table = local.state.configuration.table;
        let keys = [
            self.identity_key(CONTROL_SLOT_SUFFIX, &table)?,
            self.identity_key(CONTROL_PENDING_SUFFIX, &proposal.operation)?,
            self.identity_key(
                crate::constants::PUBLIC_TRIM_SOURCE_SCAN_SUFFIX,
                &proposal.operation,
            )?,
            self.identity_key(
                crate::constants::PUBLIC_TRIM_PREPARATION_SUFFIX,
                &proposal.operation,
            )?,
            self.public_trim_grant_turn_key(proposal)?,
        ];
        let rows = self.records.load(attempt, &keys, protection).await?;
        let pending = rows[1]
            .as_ref()
            .map(|row| self.decode_pending_control_reference(row))
            .transpose()?;
        let scan = rows[2]
            .as_ref()
            .map(|row| self.decode_public_trim_scan_observation(row, proposal))
            .transpose()?;
        if let Some(row) = &rows[3] {
            self.require_public_trim_preparation(row, proposal)?;
        }
        let turn = rows[4]
            .as_ref()
            .map(|row| self.decode_public_trim_grant_turn(row, proposal))
            .transpose()?;
        let commit = self
            .load_public_trim_commit(attempt, record, protection)
            .await?;
        if matches!(
            record.decision,
            TrimDecision::Granted(_) | TrimDecision::Committed(_) | TrimDecision::Installed(_)
        ) && turn.is_none()
        {
            return Err(corrupt_record());
        }
        if let (Some(commit), Some(turn)) = (&commit, &turn) {
            commit.require_grant_turn(turn, &self.region)?;
        }
        if let Some(released) = self
            .qualify_terminal_trim_custody(attempt, record, &rows, protection)
            .await?
        {
            return Ok((None, released));
        }
        match record.decision {
            TrimDecision::Committed(_) | TrimDecision::Installed(_) => {
                self.require_native_trim_reservation(
                    attempt,
                    proposal,
                    super::trim_reservation::TrimReservationGrant::Committed(
                        commit.as_ref().ok_or_else(corrupt_record)?,
                    ),
                    protection,
                )
                .await?
            }
            TrimDecision::Granted(grant) => {
                self.require_native_trim_reservation(
                    attempt,
                    proposal,
                    super::trim_reservation::TrimReservationGrant::Granted(grant),
                    protection,
                )
                .await?
            }
            TrimDecision::Preparing => {
                self.require_absent_trim_reservation(attempt, proposal, protection)
                    .await?
            }
            TrimDecision::Cancelled => {}
            TrimDecision::Completed(_) => return Err(corrupt_record()),
        }
        let slot = rows[0].as_ref().ok_or_else(corrupt_record)?;
        if self.decode_control_slot(slot, table)? != ControlSlotOwner::Trim(proposal.operation)
            || pending != Some(PendingControlReference::Trim(proposal.operation))
        {
            return Err(corrupt_record());
        }
        if record.decision == TrimDecision::Preparing {
            let authority = self
                .load_public_trim_head(
                    attempt,
                    (&local.state, proposal.candidate.lineage),
                    protection,
                )
                .await?;
            authority.require_revision(proposal.expected)?;
        }
        Ok((
            Some(TrimOwnership {
                slot: slot.revision,
                pending: rows[1].as_ref().ok_or_else(corrupt_record)?.revision,
                scan,
                preparation: rows[3].as_ref().map(|row| row.revision),
                authorization: rows[4].as_ref().map(|row| row.revision),
            }),
            false,
        ))
    }
}

impl JournalStore {
    pub(in crate::journal) async fn grant_public_trim_age<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &TrimRequest,
        mut outcome: TrimOutcome,
        age: &crate::trim::time::age::QualifiedPrefixAge,
    ) -> StorageResult<TrimOutcome> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let proposal = request.proposal();
            if !matches!(request, TrimRequest::Advance(_))
                || (self.region == proposal.primary && !outcome.participants_prepared)
            {
                return Ok(outcome);
            }
            let Some(prefix) = &outcome.prefix else {
                return Ok(outcome);
            };
            if !age.has_native_age(prefix, proposal.policy.minimum_retention_ms) {
                return Ok(outcome);
            }
            let record = outcome.observation.as_mut().ok_or_else(corrupt_record)?;
            if record.decision != TrimDecision::Preparing {
                return Err(corrupt_record());
            }
            if self
                .load_public_trim_grant_turn(attempt, record, ReadProtection::Conflict)
                .await?
                .is_none()
            {
                return Ok(outcome);
            }
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            let Some((grant, mut changes)) = self
                .prepare_trim_grant(
                    attempt,
                    proposal,
                    (prefix.time_binding().0, age.grant_binding()?),
                )
                .await?
            else {
                return Ok(outcome);
            };
            if !age.has_native_age(prefix, proposal.policy.minimum_retention_ms) {
                return Ok(outcome);
            }
            record.decision = TrimDecision::Granted(grant);
            changes.push(self.prepare_public_trim_record(record, Some(Revision::INITIAL))?);
            self.records.store(attempt, &changes).await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await?;
            outcome.work = TrimWorkState::Waiting;
            outcome.prefix = None;
            Ok(outcome)
        }
        .await;
        settlement.complete(result)
    }
}
