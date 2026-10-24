//! Prospective custody joins the barrier only after bounded complete-prefix
//! proof.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use storage_types::{ReadProtection, StorageResult};

use super::ArchiveTrimPage;
use crate::{
    archive::{
        ArchiveHold,
        admission::{ArchiveAdmissionPhase, ArchiveAdmissionScan, ArchiveAdmissionWork},
    },
    journal::JournalStore,
    native_record::corrupt_record,
    trim::{original::OriginalPrefixWitness, proposal::PublicTrimProposal},
};

enum ArchiveDependencyProof {
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
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
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
            let ordinal = match &scan.phase {
                ArchiveAdmissionPhase::Dependency { reservation }
                | ArchiveAdmissionPhase::Original { reservation, .. } => *reservation,
            };
            let reservation = self
                .load_trim_reservation_inventory(attempt, count, ReadProtection::Conflict)
                .await?;
            let selection = self.records.content_digest(hold)?;
            scan.require_authority((barrier, selection))?;
            if count == 0 {
                scan.require_initial_progress()?;
                return Ok(ArchiveAdmissionWork::Qualified);
            }
            if ordinal != 0 {
                return Err(corrupt_record());
            }
            let reservation = reservation.ok_or_else(corrupt_record)?;
            let proposal = &reservation.proposal;
            let page = self
                .load_archive_candidate_page(attempt, (hold, scan.ordinal), encoding.1)
                .await?;
            let (scope, next) = match &page {
                ArchiveTrimPage::Independent { scope }
                | ArchiveTrimPage::Manifest { scope, .. } => (*scope, None),
                ArchiveTrimPage::Original { scope, next, .. } => (*scope, *next),
            };
            if scan.scope.is_some_and(|expected| expected != scope) {
                return Err(corrupt_record());
            }
            scan.scope = Some(scope);
            match self
                .verify_archive_admission_page(attempt, (proposal, scan, page), encoding)
                .await?
            {
                ArchiveDependencyProof::Disjoint => {
                    self.advance_archive_admission_dependency(scan, next)
                }
                ArchiveDependencyProof::Incomplete => Ok(ArchiveAdmissionWork::Advanced),
                ArchiveDependencyProof::Overlap => Ok(ArchiveAdmissionWork::Waiting),
            }
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn verify_archive_admission_page<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selected: (
            &PublicTrimProposal,
            &mut ArchiveAdmissionScan,
            ArchiveTrimPage,
        ),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<ArchiveDependencyProof> {
        let (proposal, scan, page) = selected;
        match page {
            ArchiveTrimPage::Independent { .. } => {
                if !matches!(scan.phase, ArchiveAdmissionPhase::Dependency { .. }) {
                    return Err(corrupt_record());
                }
                Ok(ArchiveDependencyProof::Disjoint)
            }
            ArchiveTrimPage::Manifest { manifest, .. } => {
                if !matches!(scan.phase, ArchiveAdmissionPhase::Dependency { .. }) {
                    return Err(corrupt_record());
                }
                if proposal.candidate.overlaps_manifest(&manifest) {
                    return Ok(ArchiveDependencyProof::Overlap);
                }
                Ok(ArchiveDependencyProof::Disjoint)
            }
            ArchiveTrimPage::Original { event, .. } => {
                self.verify_archive_admission_original(attempt, (proposal, scan, &event), encoding)
                    .await
            }
        }
    }

    async fn verify_archive_admission_original<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selected: (
            &PublicTrimProposal,
            &mut ArchiveAdmissionScan,
            &replication_types::event::content::CanonicalEvent,
        ),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<ArchiveDependencyProof> {
        let (proposal, scan, event) = selected;
        if event.lineage() != proposal.candidate.lineage {
            if !matches!(scan.phase, ArchiveAdmissionPhase::Dependency { .. }) {
                return Err(corrupt_record());
            }
            return Ok(ArchiveDependencyProof::Disjoint);
        }
        let original = event.as_ref();
        let ArchiveAdmissionPhase::Original { witness, .. } = &mut scan.phase else {
            let ArchiveAdmissionPhase::Dependency { reservation } = scan.phase else {
                return Err(corrupt_record());
            };
            scan.phase = ArchiveAdmissionPhase::Original {
                reservation,
                witness: OriginalPrefixWitness::try_for_proposal(
                    proposal,
                    (original.id.clone(), original.digest),
                )?,
            };
            return Ok(ArchiveDependencyProof::Incomplete);
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
                return Ok(ArchiveDependencyProof::Overlap);
            }
            witness.after = proof.through;
        }
        Ok(if witness.advance_component(proposal)? {
            ArchiveDependencyProof::Disjoint
        } else {
            ArchiveDependencyProof::Incomplete
        })
    }

    fn advance_archive_admission_dependency(
        &self,
        scan: &mut ArchiveAdmissionScan,
        next: Option<u64>,
    ) -> StorageResult<ArchiveAdmissionWork> {
        let Some(next) = next else {
            return Ok(ArchiveAdmissionWork::Qualified);
        };
        if scan.ordinal.checked_add(1) != Some(next) {
            return Err(corrupt_record());
        }
        scan.ordinal = next;
        scan.phase = ArchiveAdmissionPhase::Dependency { reservation: 0 };
        Ok(ArchiveAdmissionWork::Advanced)
    }
}
