//! Verify every held receiver entry before deciding whether lineage exclusion
//! is safe.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use storage_types::{ReadProtection, StorageResult};

use super::{
    JournalStore,
    public_trim_original::{OriginalContinuation, TrimOriginalRetention},
    public_trim_scan::{ReceiverScan, SourceScanPhase, SourceScanRecord},
};
use crate::{
    history::{hold_discovery::ReaderHoldObservation, view::DeliveryPosition},
    native_record::corrupt_record,
    trim::proposal::PublicTrimProposal,
};

impl JournalStore {
    pub(in crate::journal) async fn scan_public_trim_receiver<
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
            let SourceScanPhase::Receiver(mut receiver) =
                std::mem::replace(&mut scan.phase, SourceScanPhase::AwaitingQualification)
            else {
                return Err(corrupt_record());
            };
            self.require_public_trim_receiver_scan(&receiver)?;
            self.require_public_trim_receiver_root(attempt, &receiver)
                .await?;
            if receiver.after == receiver.through {
                return self.resume_public_trim_receiver(
                    scan,
                    receiver,
                    TrimOriginalRetention::Disjoint,
                );
            }
            let position = receiver.after.checked_next()?;
            let (pointer, event) = self
                .load_receiver_original(
                    attempt,
                    (&receiver.receiver, position),
                    (encoding.1, ReadProtection::Snapshot),
                )
                .await?;
            let retention = self
                .classify_public_trim_original(attempt, proposal, &event, encoding)
                .await?;
            scan.vector =
                self.records
                    .content_digest(&(scan.vector, &pointer, event.as_ref().digest))?;
            receiver.after = position;
            if matches!(retention, TrimOriginalRetention::Unresolved) {
                return self.prepare_public_trim_original(
                    proposal,
                    scan,
                    (
                        OriginalContinuation::Receiver(receiver),
                        event.as_ref().id.clone(),
                        event.as_ref().digest,
                    ),
                );
            }
            self.resume_public_trim_receiver(scan, receiver, retention)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) fn resume_public_trim_receiver(
        &self,
        scan: &mut SourceScanRecord,
        mut receiver: ReceiverScan,
        retention: TrimOriginalRetention,
    ) -> StorageResult<()> {
        receiver.has_overlap |= matches!(retention, TrimOriginalRetention::Overlap);
        receiver.has_unresolved |= matches!(retention, TrimOriginalRetention::Unresolved);
        if receiver.after < receiver.through {
            scan.phase = SourceScanPhase::Receiver(receiver);
            return Ok(());
        }
        if receiver.has_overlap {
            scan.overlapping_holds = scan
                .overlapping_holds
                .checked_add(1)
                .ok_or_else(corrupt_record)?;
        }
        if receiver.has_unresolved {
            scan.unresolved_holds = scan
                .unresolved_holds
                .checked_add(1)
                .ok_or_else(corrupt_record)?;
        }
        scan.phase = match receiver.next.take() {
            Some(after) => SourceScanPhase::Obligations { after: Some(after) },
            None => SourceScanPhase::AwaitingQualification,
        };
        Ok(())
    }

    pub(in crate::journal) fn require_public_trim_receiver_scan(
        &self,
        receiver: &ReceiverScan,
    ) -> StorageResult<()> {
        let first = self.record_key(crate::constants::READER_HOLD_SUFFIX.to_vec())?;
        let range = self.record_prefix_range(&first)?;
        if receiver.receiver != self.origin(receiver.receiver.journal_partition)?
            || receiver.after > receiver.through
            || !range.contains(&receiver.index)
        {
            return Err(corrupt_record());
        }
        if let Some(next) = &receiver.next
            && (!range.contains(next) || next < &receiver.index)
        {
            return Err(corrupt_record());
        }
        if receiver.after == DeliveryPosition::from(0)
            && (receiver.has_overlap || receiver.has_unresolved)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) async fn require_public_trim_receiver_root<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        receiver: &ReceiverScan,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&receiver.index),
                    ReadProtection::Shared,
                )
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let ReaderHoldObservation::Receiver { range, .. } = self
                .resolve_native_retention_hold(attempt, &row, ReadProtection::Shared)
                .await?
            else {
                return Err(corrupt_record());
            };
            if range.receiver != receiver.receiver || range.through != receiver.through {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
