//! Whole-publication source preparation is not hold, time or grant authority.
use infrastructure_provider::record::{DurableRecord, DurableRecordKey};
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::{
    event::interval::ClosedSourceInterval,
    identity::origin::{OriginId, OriginPosition},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, Revision, StorageResult};

use super::JournalStore;
use crate::{
    constants::PUBLIC_TRIM_SOURCE_SCAN_SUFFIX,
    history::{retention::RetentionRevision, source_selection::CanonicalSourceSelection},
    native_record::corrupt_record,
    trim::{proposal::PublicTrimProposal, record::TrimWorkState},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) enum SourceScanPhase {
    Sources {
        component: u32,
        after: OriginPosition,
    },
    Obligations {
        after: Option<DurableRecordKey>,
    },
    Receiver(ReceiverScan),
    Custody(super::public_trim_custody::CustodyScan),
    Archive(super::public_trim_archive::ArchiveScan),
    Original(super::public_trim_original::OriginalScan),
    AwaitingQualification,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct ReceiverScan {
    pub(in crate::journal) index: DurableRecordKey,
    pub(in crate::journal) next: Option<DurableRecordKey>,
    pub(in crate::journal) receiver: OriginId,
    pub(in crate::journal) through: crate::history::view::DeliveryPosition,
    pub(in crate::journal) after: crate::history::view::DeliveryPosition,
    pub(in crate::journal) has_overlap: bool,
    pub(in crate::journal) has_unresolved: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct SourceScanRecord {
    proposal: CanonicalDigest,
    state_revision: Revision,
    catalog_revision: std::num::NonZeroU64,
    completed_sources: u32,
    pub(in crate::journal) retention: RetentionRevision,
    pub(in crate::journal) phase: SourceScanPhase,
    pub(in crate::journal) vector: CanonicalDigest,
    maximum_publisher_ms: u64,
    pub(in crate::journal) overlapping_holds: u64,
    pub(in crate::journal) unresolved_holds: u64,
}

pub(in crate::journal) struct TrimSourceScanObservation {
    pub(in crate::journal) revision: Revision,
    record: SourceScanRecord,
}

/// Issued only by a complete, freshly protected native scan. Never decoded from
/// wire.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CompleteTrimPrefix {
    binding: CanonicalDigest,
    maximum_publisher_ms: u64,
}

impl CompleteTrimPrefix {
    pub(crate) fn time_binding(&self) -> (CanonicalDigest, u64) {
        (self.binding, self.maximum_publisher_ms)
    }
}

pub(in crate::journal) struct TrimScanOutcome {
    pub(in crate::journal) work: TrimWorkState,
    pub(in crate::journal) prefix: Option<CompleteTrimPrefix>,
}

impl SourceScanRecord {
    fn select_scan_work(&self) -> TrimWorkState {
        if !matches!(self.phase, SourceScanPhase::AwaitingQualification) {
            return TrimWorkState::Ready;
        }
        if self.overlapping_holds != 0 || self.unresolved_holds != 0 {
            return TrimWorkState::Waiting;
        }
        TrimWorkState::AwaitingQualification
    }
}

impl JournalStore {
    pub(in crate::journal) async fn advance_public_trim_source_scan<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
        observed: Option<TrimSourceScanObservation>,
    ) -> StorageResult<TrimScanOutcome> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let local = &proposal.participant(&self.region)?.state;
            let table = self
                .observe_table_state(attempt, local.configuration.table, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            let catalog = attempt
                .read_catalog_stamp(&local.name)
                .await?
                .ok_or_else(corrupt_record)?;
            if table.state() != local || catalog.history_id != local.physical {
                return Err(corrupt_record());
            }
            let retention = self
                .observe_retention_revision(
                    attempt,
                    proposal.candidate.lineage,
                    ReadProtection::Conflict,
                )
                .await?;
            let key = self.identity_key(PUBLIC_TRIM_SOURCE_SCAN_SUFFIX, &proposal.operation)?;
            let revision = observed.as_ref().map(|observed| observed.revision);
            let mut scan = match observed.map(|observed| observed.record) {
                Some(scan)
                    if scan.state_revision == table.revision
                        && scan.catalog_revision == catalog.revision
                        && scan.retention == retention =>
                {
                    scan
                }
                _ => self.prepare_public_trim_source_scan(
                    proposal,
                    (
                        self.records.content_digest(proposal)?,
                        table.revision,
                        catalog.revision,
                        retention,
                    ),
                )?,
            };
            if matches!(scan.phase, SourceScanPhase::AwaitingQualification) {
                self.require_public_trim_scope(attempt, proposal, encoding)
                    .await?;
                return self.qualify_public_trim_scan(&scan, revision.ok_or_else(corrupt_record)?);
            }
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            self.advance_public_trim_scan_phase(attempt, proposal, &mut scan, encoding)
                .await?;
            self.require_public_trim_source_completion(proposal, &scan)?;
            if matches!(scan.phase, SourceScanPhase::AwaitingQualification) {
                self.require_public_trim_scope(attempt, proposal, encoding)
                    .await?;
            }
            self.records
                .store(attempt, &[self.records.prepare(key, revision, &scan)?])
                .await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await?;
            self.qualify_public_trim_scan(
                &scan,
                revision.map_or(Ok(Revision::INITIAL), Revision::checked_next)?,
            )
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn require_reserved_trim_prefix(
        &self,
        row: &DurableRecord,
        proposal: &PublicTrimProposal,
        prefix: CanonicalDigest,
    ) -> StorageResult<()> {
        let scan = self.decode_public_trim_source_scan(
            row,
            proposal,
            self.records.content_digest(proposal)?,
        )?;
        let observed = self.qualify_public_trim_scan(&scan, row.revision)?;
        if observed
            .prefix
            .is_none_or(|observed| observed.time_binding().0 != prefix)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn qualify_public_trim_scan(
        &self,
        scan: &SourceScanRecord,
        revision: Revision,
    ) -> StorageResult<TrimScanOutcome> {
        let work = scan.select_scan_work();
        let prefix = if work == TrimWorkState::AwaitingQualification {
            Some(CompleteTrimPrefix {
                binding: self.records.content_digest(&(
                    self.region.clone(),
                    self.group,
                    self.generation,
                    revision,
                    scan,
                ))?,
                maximum_publisher_ms: scan.maximum_publisher_ms,
            })
        } else {
            None
        };
        Ok(TrimScanOutcome { work, prefix })
    }

    async fn advance_public_trim_scan_phase<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        scan: &mut SourceScanRecord,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        match scan.phase {
            SourceScanPhase::Sources { .. } => {
                self.scan_public_trim_publication(attempt, proposal, scan, encoding)
                    .await?
            }
            SourceScanPhase::Obligations { .. } => {
                self.scan_public_trim_obligation(attempt, proposal, scan)
                    .await?
            }
            SourceScanPhase::Receiver(_) => {
                self.scan_public_trim_receiver(attempt, proposal, scan, encoding)
                    .await?
            }
            SourceScanPhase::Custody(_) => {
                self.scan_public_trim_custody(attempt, proposal, scan)
                    .await?
            }
            SourceScanPhase::Archive(_) => {
                self.scan_public_trim_archive(attempt, proposal, scan, encoding)
                    .await?
            }
            SourceScanPhase::Original(_) => {
                self.scan_public_trim_original(attempt, proposal, scan, encoding)
                    .await?
            }
            SourceScanPhase::AwaitingQualification => return Err(corrupt_record()),
        }
        Ok(())
    }

    pub(in crate::journal) fn decode_public_trim_scan_observation(
        &self,
        row: &DurableRecord,
        proposal: &PublicTrimProposal,
    ) -> StorageResult<TrimSourceScanObservation> {
        let record = self.decode_public_trim_source_scan(
            row,
            proposal,
            self.records.content_digest(proposal)?,
        )?;
        Ok(TrimSourceScanObservation {
            revision: row.revision,
            record,
        })
    }

    fn prepare_public_trim_source_scan(
        &self,
        proposal: &PublicTrimProposal,
        stamp: (
            CanonicalDigest,
            Revision,
            std::num::NonZeroU64,
            RetentionRevision,
        ),
    ) -> StorageResult<SourceScanRecord> {
        let first = proposal
            .candidate
            .sources
            .first()
            .ok_or_else(corrupt_record)?;
        Ok(SourceScanRecord {
            proposal: stamp.0,
            state_revision: stamp.1,
            catalog_revision: stamp.2,
            retention: stamp.3,
            completed_sources: 0,
            phase: SourceScanPhase::Sources {
                component: 0,
                after: first.after,
            },
            vector: self.records.content_digest(&(stamp, "trim-publications"))?,
            maximum_publisher_ms: 0,
            overlapping_holds: 0,
            unresolved_holds: 0,
        })
    }

    fn decode_public_trim_source_scan(
        &self,
        row: &DurableRecord,
        proposal: &PublicTrimProposal,
        content: CanonicalDigest,
    ) -> StorageResult<SourceScanRecord> {
        let scan: SourceScanRecord = self.records.decode(row)?;
        if row.key != self.identity_key(PUBLIC_TRIM_SOURCE_SCAN_SUFFIX, &proposal.operation)?
            || scan.proposal != content
        {
            return Err(corrupt_record());
        }
        self.require_public_trim_source_completion(proposal, &scan)?;
        if let SourceScanPhase::Obligations { after: Some(after) } = &scan.phase {
            let first = self.record_key(crate::constants::READER_HOLD_SUFFIX.to_vec())?;
            if !self.record_prefix_range(&first)?.contains(after) {
                return Err(corrupt_record());
            }
        }
        if let SourceScanPhase::Receiver(receiver) = &scan.phase {
            self.require_public_trim_receiver_scan(receiver)?;
        }
        if let SourceScanPhase::Custody(custody) = &scan.phase {
            self.require_public_trim_custody_scan(custody)?;
        }
        if let SourceScanPhase::Archive(archive) = &scan.phase {
            self.require_public_trim_archive_scan(archive)?;
        }
        if let SourceScanPhase::Original(original) = &scan.phase {
            self.require_public_trim_original_scan(original, proposal)?;
        }
        Ok(scan)
    }

    fn require_public_trim_source_completion(
        &self,
        proposal: &PublicTrimProposal,
        scan: &SourceScanRecord,
    ) -> StorageResult<()> {
        if let SourceScanPhase::Sources { component, after } = scan.phase {
            let cut = proposal
                .candidate
                .sources
                .get(usize::try_from(component).map_err(|_| corrupt_record())?)
                .ok_or_else(corrupt_record)?;
            if scan.completed_sources != component
                || scan.overlapping_holds != 0
                || scan.unresolved_holds != 0
                || after < cut.after
                || after > cut.through
                || (after == cut.through && cut.after != cut.through)
            {
                return Err(corrupt_record());
            }
            return Ok(());
        }
        if usize::try_from(scan.completed_sources).map_err(|_| corrupt_record())?
            != proposal.candidate.sources.len()
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    async fn scan_public_trim_publication<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        scan: &mut SourceScanRecord,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        let SourceScanPhase::Sources { component, after } = scan.phase else {
            return Err(corrupt_record());
        };
        let cut = proposal
            .candidate
            .sources
            .get(usize::try_from(component).map_err(|_| corrupt_record())?)
            .ok_or_else(corrupt_record)?;
        if after == cut.through {
            return self.advance_public_trim_component(proposal, scan, component);
        }
        let through =
            OriginPosition::from(u64::from(after).checked_add(1).ok_or_else(corrupt_record)?);
        let range =
            ClosedSourceInterval::try_from((after, through)).map_err(|_| corrupt_record())?;
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
        for link in &frame.manifest().links {
            scan.maximum_publisher_ms = scan
                .maximum_publisher_ms
                .max(link.publication_clock.physical_ms);
        }
        scan.vector =
            self.records
                .content_digest(&(scan.vector, component, frame.manifest().digest))?;
        scan.phase = SourceScanPhase::Sources {
            component,
            after: through,
        };
        if through == cut.through {
            self.advance_public_trim_component(proposal, scan, component)?;
        }
        Ok(())
    }

    fn advance_public_trim_component(
        &self,
        proposal: &PublicTrimProposal,
        scan: &mut SourceScanRecord,
        component: u32,
    ) -> StorageResult<()> {
        let cut = proposal
            .candidate
            .sources
            .get(usize::try_from(component).map_err(|_| corrupt_record())?)
            .ok_or_else(corrupt_record)?;
        if scan.completed_sources != component
            || !matches!(scan.phase, SourceScanPhase::Sources { component: current, after } if current == component && after == cut.through)
        {
            return Err(corrupt_record());
        }
        let next = component.checked_add(1).ok_or_else(corrupt_record)?;
        scan.vector = self.records.content_digest(&(
            scan.vector,
            "trim-component-complete",
            component,
            cut,
        ))?;
        scan.completed_sources = next;
        scan.phase = match proposal
            .candidate
            .sources
            .get(usize::try_from(next).map_err(|_| corrupt_record())?)
        {
            Some(cut) => SourceScanPhase::Sources {
                component: next,
                after: cut.after,
            },
            None => SourceScanPhase::Obligations { after: None },
        };
        Ok(())
    }
}
