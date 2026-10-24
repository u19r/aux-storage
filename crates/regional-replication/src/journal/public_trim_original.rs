//! Shared public trim classification never proves absence from lookup absence.
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::{
    event::{content::CanonicalEvent, identity::CanonicalEventId, interval::ClosedSourceInterval},
    identity::origin::OriginPosition,
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{
    JournalStore,
    public_trim_archive::ArchiveScan,
    public_trim_scan::{ReceiverScan, SourceScanPhase, SourceScanRecord},
};
use crate::{
    history::source_selection::CanonicalSourceSelection,
    native_record::corrupt_record,
    trim::{original::OriginalPrefixWitness, proposal::PublicTrimProposal},
};

pub(in crate::journal) struct OriginalPublicationProof {
    pub(in crate::journal) through: OriginPosition,
    pub(in crate::journal) digest: CanonicalDigest,
    pub(in crate::journal) has_overlap: bool,
}

pub(in crate::journal) enum TrimOriginalRetention {
    Disjoint,
    Overlap,
    Unresolved,
}

impl JournalStore {
    pub(in crate::journal) async fn classify_public_trim_original<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        event: &CanonicalEvent,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<TrimOriginalRetention> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if event.lineage() != proposal.candidate.lineage {
                return Ok(TrimOriginalRetention::Disjoint);
            }
            let table = &proposal.participant(&self.region)?.state.name;
            let original = event.as_ref();
            let publication = self
                .observe_retained_publication(
                    attempt,
                    (table, &proposal.candidate, &original.id, original.digest),
                    encoding,
                )
                .await?;
            Ok(if publication.is_some() {
                TrimOriginalRetention::Overlap
            } else {
                TrimOriginalRetention::Unresolved
            })
        }
        .await;
        settlement.complete(result)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) enum OriginalContinuation {
    Receiver(ReceiverScan),
    Archive {
        archive: ArchiveScan,
        next: Option<u64>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct OriginalScan {
    continuation: OriginalContinuation,
    witness: OriginalPrefixWitness,
}

impl JournalStore {
    pub(in crate::journal) fn prepare_public_trim_original(
        &self,
        proposal: &PublicTrimProposal,
        scan: &mut SourceScanRecord,
        original: (OriginalContinuation, CanonicalEventId, CanonicalDigest),
    ) -> StorageResult<()> {
        let witness = OriginalPrefixWitness::try_for_proposal(proposal, (original.1, original.2))?;
        scan.phase = SourceScanPhase::Original(OriginalScan {
            continuation: original.0,
            witness,
        });
        Ok(())
    }

    pub(in crate::journal) async fn scan_public_trim_original<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        scan: &mut SourceScanRecord,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let SourceScanPhase::Original(mut original) =
                std::mem::replace(&mut scan.phase, SourceScanPhase::AwaitingQualification)
            else {
                return Err(corrupt_record());
            };
            self.require_public_trim_original_scan(&original, proposal)?;
            self.require_public_trim_original_parent(attempt, &original, encoding.1)
                .await?;
            let cut = proposal
                .candidate
                .sources
                .get(usize::try_from(original.witness.component).map_err(|_| corrupt_record())?)
                .ok_or_else(corrupt_record)?;
            if original.witness.after < cut.through {
                let proof = self
                    .verify_public_trim_original_publication(
                        attempt,
                        proposal,
                        &original.witness,
                        encoding,
                    )
                    .await?;
                scan.vector = self.records.content_digest(&(
                    scan.vector,
                    original.witness.component,
                    proof.digest,
                    &original.witness.identity,
                    original.witness.digest,
                ))?;
                if proof.has_overlap {
                    return self.resume_public_trim_original(
                        scan,
                        original.continuation,
                        TrimOriginalRetention::Overlap,
                    );
                }
                original.witness.after = proof.through;
            }
            if original.witness.advance_component(proposal)? {
                return self.resume_public_trim_original(
                    scan,
                    original.continuation,
                    TrimOriginalRetention::Disjoint,
                );
            }
            scan.phase = SourceScanPhase::Original(original);
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn require_public_trim_original_parent<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        original: &OriginalScan,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        match &original.continuation {
            OriginalContinuation::Receiver(receiver) => {
                self.require_public_trim_receiver_root(attempt, receiver)
                    .await?;
                let (_, event) = self
                    .load_receiver_original(
                        attempt,
                        (&receiver.receiver, receiver.after),
                        (codec, storage_types::ReadProtection::Snapshot),
                    )
                    .await?;
                if event.as_ref().id != original.witness.identity
                    || event.as_ref().digest != original.witness.digest
                {
                    return Err(corrupt_record());
                }
                Ok(())
            }
            OriginalContinuation::Archive { archive, next } => {
                self.require_public_trim_archive_original(
                    attempt,
                    archive,
                    (&original.witness.identity, original.witness.digest, *next),
                    codec,
                )
                .await
            }
        }
    }

    pub(in crate::journal) async fn verify_public_trim_original_publication<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        original: &OriginalPrefixWitness,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<OriginalPublicationProof> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let cut = original.require_component(proposal)?;
            if original.after == cut.through {
                return Err(corrupt_record());
            }
            let through = OriginPosition::from(
                u64::from(original.after)
                    .checked_add(1)
                    .ok_or_else(corrupt_record)?,
            );
            let range = ClosedSourceInterval::try_from((original.after, through))
                .map_err(|_| corrupt_record())?;
            let selection = CanonicalSourceSelection {
                serving_region: self.region.clone(),
                serving_group: self.group,
                table: proposal.participant(&self.region)?.state.name.clone(),
                lineage: cut.lineage,
                membership: cut.membership,
                source: cut.source.clone(),
                origin: cut.origin.clone(),
            };
            let frame = self
                .load_history_frame(attempt, (&selection, range), encoding)
                .await?;
            if through == cut.through && Some(frame.manifest().digest) != cut.terminal_manifest {
                return Err(corrupt_record());
            }
            let mut has_overlap = false;
            for link in &frame.manifest().links {
                if link.event != original.identity {
                    continue;
                }
                if link.digest != original.digest {
                    return Err(corrupt_record());
                }
                has_overlap |= link.lineage == cut.lineage && link.table == cut.source.table;
            }
            Ok(OriginalPublicationProof {
                through,
                digest: frame.manifest().digest,
                has_overlap,
            })
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) fn require_public_trim_original_scan(
        &self,
        original: &OriginalScan,
        proposal: &PublicTrimProposal,
    ) -> StorageResult<()> {
        original.witness.require_component(proposal)?;
        match &original.continuation {
            OriginalContinuation::Receiver(receiver) => {
                self.require_public_trim_receiver_scan(receiver)
            }
            OriginalContinuation::Archive { archive, next } => {
                self.require_public_trim_archive_continuation(archive, *next)
            }
        }
    }

    fn resume_public_trim_original(
        &self,
        scan: &mut SourceScanRecord,
        continuation: OriginalContinuation,
        retention: TrimOriginalRetention,
    ) -> StorageResult<()> {
        match continuation {
            OriginalContinuation::Receiver(receiver) => {
                self.resume_public_trim_receiver(scan, receiver, retention)
            }
            OriginalContinuation::Archive { archive, next } => {
                self.resume_public_trim_archive(scan, (archive, next), retention)
            }
        }
    }
}
