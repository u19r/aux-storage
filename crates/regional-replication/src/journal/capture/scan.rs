use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{
    configuration::proposal::ControlProposal,
    event::{
        baseline::BaselineItem,
        content::{CanonicalEventCodec, SourceImagesRef},
    },
};
use storage_backfill::{ScanPageEnd, ScanTransition};
use storage_provider::{
    CatalogAttempt, DynamoAttempt, ItemTombstoneScan, ItemTombstoneScanAttempt,
    ItemWriteObservation, PrimaryStateScan, PrimaryStateScanAttempt, TableMetadataCache,
    resolve_table_in_attempt,
};
use storage_types::{ItemProvenance, ReadProtection, StorageResult};

use super::{
    record::{CapturePhase, CaptureRecord},
    status,
};
use crate::{bootstrap::SourceCaptureStatus, journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(crate) async fn read_capture_baselines<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        input: (
            Option<infrastructure_provider::record::DurableRecordKey>,
            &CanonicalJsonCodec,
        ),
    ) -> StorageResult<super::super::BaselineAuthorityPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, record) = self
                .load_source_capture(attempt, proposal, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(record.phase, CapturePhase::BaselineReady) {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::PublicationHistory,
                }
                .into());
            }
            self.load_baseline_page(
                attempt,
                (proposal.operation().lineage, input.0),
                (
                    input.1,
                    crate::bootstrap::copy_scope::BaselineCopyBudget {
                        records: self.records.limits.max_records(),
                        bytes: self.records.limits.max_bytes(),
                    },
                ),
            )
            .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn advance_source_capture<
        A: stream_provider::retained_image::RetainedOwnershipAttempt
            + DynamoAttempt
            + CatalogAttempt
            + PrimaryStateScanAttempt
            + ItemTombstoneScanAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        context: (
            &TableMetadataCache,
            &CanonicalJsonCodec,
            &replication_provider::runtime_configuration::limits::WorkLimits,
        ),
    ) -> StorageResult<SourceCaptureStatus> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (row, mut record) = self
                .load_source_capture(attempt, proposal, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            if matches!(
                record.phase,
                CapturePhase::BaselineReady | CapturePhase::Released | CapturePhase::Cancelled
            ) {
                return Ok(status(&record.phase));
            }
            if matches!(record.phase, CapturePhase::Releasing(_)) {
                return self.release_source_capture(attempt, proposal).await;
            }
            if matches!(record.phase, CapturePhase::Cancelling(_)) {
                return self
                    .advance_capture_cancellation(attempt, (row.revision, record))
                    .await;
            }
            let source = self
                .require_capture_table(attempt, proposal, storage_types::ReadProtection::Shared)
                .await?;
            let (table, _) = resolve_table_in_attempt(attempt, context.0, &source.name).await?;
            match record.phase {
                CapturePhase::History => {
                    if self
                        .advance_reenable_inventory(attempt, proposal, (context.1, context.2))
                        .await?
                    {
                        record.phase = CapturePhase::BaselineReady;
                    }
                }
                CapturePhase::Live(_) | CapturePhase::Deleted(_) => {
                    self.scan_source_baseline(attempt, (&mut record, table), context.1)
                        .await?
                }
                _ => self.advance_source_cuts(attempt, &mut record).await?,
            }
            self.store_source_capture(attempt, &record, Some(row.revision))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn scan_source_baseline<
        A: stream_provider::retained_image::RetainedOwnershipAttempt
            + DynamoAttempt
            + PrimaryStateScanAttempt
            + ItemTombstoneScanAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&mut CaptureRecord, Arc<storage_types::BoundTableInfo>),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let (record, table) = input;
        match &record.phase {
            CapturePhase::Live(progress) => {
                let scan =
                    PrimaryStateScan::try_new(Arc::clone(&table), progress.after.clone(), Some(1))?;
                let page = attempt.scan_primary_state(&scan).await?.finish()?;
                for row in page.items {
                    self.adopt_source_row(attempt, (record, Arc::clone(&table), row.key), codec)
                        .await?;
                }
                let end = page
                    .continuation
                    .map_or(ScanPageEnd::Complete(()), ScanPageEnd::Continue);
                record.phase = match progress.advance_page(end).map_err(|_| corrupt_record())? {
                    ScanTransition::Scanning(next) => CapturePhase::Live(next),
                    ScanTransition::CatchingUp(_) => {
                        CapturePhase::Deleted(storage_backfill::ScanProgress {
                            low: (),
                            after: None,
                        })
                    }
                };
            }
            CapturePhase::Deleted(progress) => {
                let scan = ItemTombstoneScan::try_new(
                    Arc::clone(&table),
                    progress.after.clone(),
                    NonZeroU32::MIN,
                    NonZeroUsize::new(self.records.limits.max_bytes().get())
                        .ok_or_else(corrupt_record)?,
                )?;
                let page = attempt.scan_item_tombstones(&scan).await?;
                for row in page.items {
                    self.adopt_source_row(attempt, (record, Arc::clone(&table), row.key), codec)
                        .await?;
                }
                let end = page
                    .continuation
                    .map_or(ScanPageEnd::Complete(()), ScanPageEnd::Continue);
                record.phase = match progress.advance_page(end).map_err(|_| corrupt_record())? {
                    ScanTransition::Scanning(next) => CapturePhase::Deleted(next),
                    ScanTransition::CatchingUp(_) => CapturePhase::High(0),
                };
            }
            _ => return Err(corrupt_record()),
        }
        Ok(())
    }

    async fn adopt_source_row<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + DynamoAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &CaptureRecord,
            Arc<storage_types::BoundTableInfo>,
            storage_types::KeyAttributes,
        ),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let (record, table, row_key) = input;
        let (observed, lifecycle) = attempt.observe_for_write(&table, &row_key).await?;
        let (provenance, image, indexers, body) = match &observed {
            ItemWriteObservation::Live {
                stored, provenance, ..
            } => (
                provenance,
                Some(&stored.item),
                Some(stored.indexers.as_slice()),
                stored.source,
            ),
            ItemWriteObservation::Deleted { provenance, .. } => (provenance, None, None, None),
            ItemWriteObservation::Absent => return Ok(()),
        };
        let key = codec.encode_key(&row_key).map_err(|_| corrupt_record())?;
        if provenance.as_bytes() != ItemProvenance::LOCAL_BYTES {
            let winner = codec
                .decode_winner_provenance(provenance.as_bytes(), &key)
                .map_err(|_| corrupt_record())?;
            if winner.lineage == record.proposal.operation().lineage {
                return Ok(());
            }
        }
        let source = record
            .proposal
            .participant(&record.proposal.operation().home_region)
            .map_err(|_| corrupt_record())?;
        let exact = self
            .load_baseline_image(attempt, (source.proposed.physical, image, body), codec)
            .await?;
        let baseline = BaselineItem {
            source_table: source.proposed.configuration.table,
            lineage: record.proposal.operation().lineage,
            schema: source.proposed.configuration.schema,
            key: row_key,
            canonical_key: key,
            images: SourceImagesRef {
                old: None,
                new: exact.as_ref(),
                old_indexers: None,
                new_indexers: indexers,
            },
        };
        let baseline = self
            .store_baseline(attempt, (&baseline, source.proposed.physical), codec)
            .await?;
        let provenance = codec
            .encode_winner_provenance(&baseline.provenance())
            .map_err(|_| corrupt_record())?;
        self.apply_baseline_effects(
            attempt,
            (table, &baseline, provenance),
            (observed, lifecycle),
            codec,
        )
        .await
    }
}
