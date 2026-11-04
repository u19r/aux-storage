//! Prospective custody joins the barrier only after bounded complete-prefix
//! proof.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use storage_types::{ReadProtection, StorageResult};

use super::ArchiveOriginal;
use crate::{
    archive::{
        ArchiveHold,
        admission::{ArchiveAdmissionPhase, ArchiveAdmissionScan, ArchiveAdmissionWork},
    },
    journal::JournalStore,
    native_record::corrupt_record,
    trim::{original::OriginalPrefixWitness, proposal::PublicTrimProposal},
};

enum ArchiveOriginalProof {
    Disjoint,
    Incomplete,
    Overlap,
}

impl JournalStore {
    pub(in crate::journal) async fn begin_archive_admission<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        hold: &ArchiveHold,
    ) -> StorageResult<ArchiveAdmissionScan> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (barrier, _) = self
                .load_reservation_barrier(attempt, ReadProtection::Conflict)
                .await?
                .reservation_state();
            Ok(ArchiveAdmissionScan::new(
                barrier,
                self.records.content_digest(hold)?,
            ))
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn advance_archive_admission<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        selected: (&ArchiveHold, &mut ArchiveAdmissionScan),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<ArchiveAdmissionWork> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (hold, scan) = selected;
            let (barrier, count) = self
                .load_reservation_barrier(attempt, ReadProtection::Conflict)
                .await?
                .reservation_state();
            let reservation = self
                .load_trim_reservation_inventory(attempt, count, ReadProtection::Conflict)
                .await?;
            let selection = self.records.content_digest(hold)?;
            scan.require_authority((barrier, selection))?;
            if count == 0 {
                scan.require_initial_progress()?;
                return Ok(ArchiveAdmissionWork::Qualified);
            }
            let reservation = reservation.ok_or_else(corrupt_record)?;
            let proposal = &reservation.proposal;
            let page = self
                .load_archive_candidate_page(attempt, hold, encoding.1)
                .await?;
            let scope = match &page {
                ArchiveOriginal::Baseline { scope } | ArchiveOriginal::Event { scope, .. } => {
                    *scope
                }
            };
            if scan.scope.is_some_and(|expected| expected != scope) {
                return Err(corrupt_record());
            }
            scan.scope = Some(scope);
            match self
                .verify_archive_admission_page(attempt, (proposal, scan, page), encoding)
                .await?
            {
                ArchiveOriginalProof::Disjoint => Ok(ArchiveAdmissionWork::Qualified),
                ArchiveOriginalProof::Incomplete => Ok(ArchiveAdmissionWork::Advanced),
                ArchiveOriginalProof::Overlap => Ok(ArchiveAdmissionWork::Waiting),
            }
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn verify_archive_admission_page<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        selected: (
            &PublicTrimProposal,
            &mut ArchiveAdmissionScan,
            ArchiveOriginal,
        ),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<ArchiveOriginalProof> {
        let (proposal, scan, page) = selected;
        match page {
            ArchiveOriginal::Baseline { .. } => {
                if !matches!(scan.phase, ArchiveAdmissionPhase::Classify) {
                    return Err(corrupt_record());
                }
                Ok(ArchiveOriginalProof::Disjoint)
            }
            ArchiveOriginal::Event { event, .. } => {
                self.verify_archive_admission_original(attempt, (proposal, scan, &event), encoding)
                    .await
            }
        }
    }

    async fn verify_archive_admission_original<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        selected: (
            &PublicTrimProposal,
            &mut ArchiveAdmissionScan,
            &replication_types::event::content::CanonicalEvent,
        ),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<ArchiveOriginalProof> {
        let (proposal, scan, event) = selected;
        if event.lineage() != proposal.candidate.lineage {
            if !matches!(scan.phase, ArchiveAdmissionPhase::Classify) {
                return Err(corrupt_record());
            }
            return Ok(ArchiveOriginalProof::Disjoint);
        }
        let original = event.as_ref();
        let ArchiveAdmissionPhase::Original { witness, .. } = &mut scan.phase else {
            if !matches!(scan.phase, ArchiveAdmissionPhase::Classify) {
                return Err(corrupt_record());
            }
            scan.phase = ArchiveAdmissionPhase::Original {
                witness: OriginalPrefixWitness::try_for_proposal(
                    proposal,
                    (original.id.clone(), original.digest),
                )?,
            };
            return Ok(ArchiveOriginalProof::Incomplete);
        };
        if witness.identity != original.id || witness.digest != original.digest {
            return Err(corrupt_record());
        }
        let cut = witness.require_component(proposal)?;
        if witness.after < cut.through {
            let proof = self
                .verify_public_trim_original_publication(attempt, proposal, witness, encoding)
                .await?;
            if proof.has_overlap {
                return Ok(ArchiveOriginalProof::Overlap);
            }
            witness.after = proof.through;
        }
        Ok(if witness.advance_component(proposal)? {
            ArchiveOriginalProof::Disjoint
        } else {
            ArchiveOriginalProof::Incomplete
        })
    }
}
