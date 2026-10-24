//! Native root inventory preparation; unresolved custody never becomes a grant.
use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
};
use storage_types::{ReadProtection, StorageResult};

use super::{
    JournalStore,
    public_trim_scan::{ReceiverScan, SourceScanPhase, SourceScanRecord},
};
use crate::{
    history::{hold_discovery::ReaderHoldObservation, source_retention::SourceRetentionRange},
    native_record::corrupt_record,
    trim::proposal::PublicTrimProposal,
};

impl JournalStore {
    pub(in crate::journal) async fn scan_public_trim_obligation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        scan: &mut SourceScanRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let SourceScanPhase::Obligations { after } = &scan.phase else {
                return Err(corrupt_record());
            };
            let first = self.record_key(crate::constants::READER_HOLD_SUFFIX.to_vec())?;
            let bounds = self.record_prefix_range(&first)?;
            let range = BoundedRecordRange::try_new(
                first,
                bounds.bounds().1.clone(),
                std::num::NonZeroUsize::MIN,
            )?;
            let page = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range,
                        after: after.clone(),
                        max_bytes: self.records.limits.max_bytes(),
                        protection: ReadProtection::Snapshot,
                    },
                )
                .await?;
            if page.records.len() > 1 {
                return Err(corrupt_record());
            }
            if let Some(row) = page.records.first() {
                if self
                    .prepare_public_trim_custody(attempt, row, (page.continuation.clone(), scan))
                    .await?
                {
                    return Ok(());
                }
                if self
                    .prepare_public_trim_archive(attempt, row, (page.continuation.clone(), scan))
                    .await?
                {
                    return Ok(());
                }
                let observation = self
                    .resolve_native_retention_hold(attempt, row, ReadProtection::Shared)
                    .await?;
                if let ReaderHoldObservation::Receiver { range, .. } = &observation {
                    scan.phase = SourceScanPhase::Receiver(ReceiverScan {
                        index: row.key.clone(),
                        next: page.continuation,
                        receiver: range.receiver.clone(),
                        through: range.through,
                        after: 0.into(),
                        has_overlap: false,
                        has_unresolved: false,
                    });
                    scan.vector = self.records.content_digest(&(
                        scan.vector,
                        &row.key,
                        row.revision,
                        &observation,
                    ))?;
                    return Ok(());
                }
                self.record_public_trim_obligation(proposal, scan, &observation)?;
                scan.vector = self.records.content_digest(&(
                    scan.vector,
                    &row.key,
                    row.revision,
                    &observation,
                ))?;
            }
            scan.phase = match page.continuation {
                Some(after) => SourceScanPhase::Obligations { after: Some(after) },
                None => SourceScanPhase::AwaitingQualification,
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    fn record_public_trim_obligation(
        &self,
        proposal: &PublicTrimProposal,
        scan: &mut SourceScanRecord,
        observation: &ReaderHoldObservation,
    ) -> StorageResult<()> {
        let overlaps = match observation {
            ReaderHoldObservation::Source { cut, .. } => {
                cut.require_inventory()?;
                cut.sources.iter().any(|held| {
                    proposal
                        .candidate
                        .sources
                        .iter()
                        .any(|removed| SourceRetentionRange::Source(held).overlaps_cut(removed))
                })
            }
            ReaderHoldObservation::Capture { range } => proposal
                .candidate
                .sources
                .iter()
                .any(|removed| SourceRetentionRange::Capture(range).overlaps_cut(removed)),
            ReaderHoldObservation::ImportFragmentPending { binding, page } => {
                proposal.candidate.sources.iter().any(|removed| {
                    SourceRetentionRange::Fragment { binding, page }.overlaps_cut(removed)
                })
            }
            _ => {
                scan.unresolved_holds = scan
                    .unresolved_holds
                    .checked_add(1)
                    .ok_or_else(corrupt_record)?;
                return Ok(());
            }
        };
        if overlaps {
            scan.overlapping_holds = scan
                .overlapping_holds
                .checked_add(1)
                .ok_or_else(corrupt_record)?;
        }
        Ok(())
    }
}
