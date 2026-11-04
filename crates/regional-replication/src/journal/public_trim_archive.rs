//! Pending archive dependency progress shares the trim operation's epoch pins.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{
    JournalStore,
    archive::ArchiveOriginal,
    public_trim_original::{OriginalContinuation, TrimOriginalRetention},
    public_trim_scan::{SourceScanPhase, SourceScanRecord},
    reader_hold::RetentionHoldReference,
};
use crate::{
    archive::ArchiveHold, native_record::corrupt_record, trim::proposal::PublicTrimProposal,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct ArchiveScan {
    hold: ArchiveHold,
    next: Option<DurableRecordKey>,
    scope: Option<CanonicalDigest>,
}

impl JournalStore {
    pub(in crate::journal) async fn prepare_public_trim_archive<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &DurableRecord,
        continuation: (Option<DurableRecordKey>, &mut SourceScanRecord),
    ) -> StorageResult<bool> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let RetentionHoldReference::Archive(hold) = self.decode_retention_hold(row)? else {
                return Ok(false);
            };
            self.observe_archive_hold(attempt, &hold, storage_types::ReadProtection::Shared)
                .await?;
            let (next, scan) = continuation;
            scan.vector = self.records.content_digest(&(scan.vector, &hold))?;
            scan.phase = SourceScanPhase::Archive(ArchiveScan {
                hold,
                next,
                scope: None,
            });
            Ok(true)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn scan_public_trim_archive<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        scan: &mut SourceScanRecord,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let SourceScanPhase::Archive(mut archive) =
                std::mem::replace(&mut scan.phase, SourceScanPhase::AwaitingQualification)
            else {
                return Err(corrupt_record());
            };
            self.require_public_trim_archive_scan(&archive)?;
            let page = self
                .load_archive_trim_page(attempt, &archive.hold, encoding.1)
                .await?;
            let (scope, original, retention) = match page {
                ArchiveOriginal::Event { event, scope } => {
                    let retention = self
                        .classify_public_trim_original(attempt, proposal, &event, encoding)
                        .await?;
                    let original = event.as_ref();
                    scan.vector = self.records.content_digest(&(
                        scan.vector,
                        &original.id,
                        original.digest,
                    ))?;
                    (
                        scope,
                        Some((original.id.clone(), original.digest)),
                        retention,
                    )
                }
                ArchiveOriginal::Baseline { scope } => {
                    (scope, None, TrimOriginalRetention::Disjoint)
                }
            };
            if archive.scope.is_some_and(|expected| expected != scope) {
                return Err(corrupt_record());
            }
            archive.scope = Some(scope);
            scan.vector = self.records.content_digest(&(scan.vector, scope))?;
            if matches!(retention, TrimOriginalRetention::Unresolved)
                && let Some((identity, digest)) = original
            {
                return self.prepare_public_trim_original(
                    proposal,
                    scan,
                    (OriginalContinuation::Archive(archive), identity, digest),
                );
            }
            self.resume_public_trim_archive(scan, archive, retention)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) fn resume_public_trim_archive(
        &self,
        scan: &mut SourceScanRecord,
        mut archive: ArchiveScan,
        retention: TrimOriginalRetention,
    ) -> StorageResult<()> {
        if matches!(retention, TrimOriginalRetention::Overlap) {
            scan.overlapping_holds = scan
                .overlapping_holds
                .checked_add(1)
                .ok_or_else(corrupt_record)?;
        }
        if matches!(retention, TrimOriginalRetention::Unresolved) {
            scan.unresolved_holds = scan
                .unresolved_holds
                .checked_add(1)
                .ok_or_else(corrupt_record)?;
        }
        scan.phase = match archive.next.take() {
            Some(after) => SourceScanPhase::Obligations { after: Some(after) },
            None => SourceScanPhase::AwaitingQualification,
        };
        Ok(())
    }

    pub(in crate::journal) async fn require_public_trim_archive_original<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        archive: &ArchiveScan,
        original: (
            &replication_types::event::identity::CanonicalEventId,
            CanonicalDigest,
        ),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let ArchiveOriginal::Event { event, scope } = self
                .load_archive_trim_page(attempt, &archive.hold, codec)
                .await?
            else {
                return Err(corrupt_record());
            };
            if &event.as_ref().id != original.0
                || event.as_ref().digest != original.1
                || archive.scope != Some(scope)
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) fn require_public_trim_archive_continuation(
        &self,
        archive: &ArchiveScan,
    ) -> StorageResult<()> {
        self.require_public_trim_archive_scan(archive)?;
        if archive.scope.is_none() {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) fn require_public_trim_archive_scan(
        &self,
        archive: &ArchiveScan,
    ) -> StorageResult<()> {
        self.require_archive_hold(&archive.hold)?;
        if let Some(next) = &archive.next {
            let first = self.record_key(crate::constants::READER_HOLD_SUFFIX.to_vec())?;
            if !self.record_prefix_range(&first)?.contains(next)
                || next < &self.archive_hold_prefix()?
            {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }
}
