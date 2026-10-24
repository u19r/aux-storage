use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::content::CanonicalEventCodec;
use storage_backfill::PreparedGsiPut;
use storage_provider::{
    DynamoAttempt, GsiProjectionRow, GsiProjectionScan, GsiProjectionScanAttempt,
    ItemWriteObservation,
};
use storage_types::{BoundTableInfo, IndexName, IndexerDeclaration, ItemKey, StorageResult};

use super::record::TargetRecord;
use crate::{
    bootstrap::{VerificationProgress, VerificationStage},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn advance_gsi_verification<
        A: PhysicalRecordAttempt + DynamoAttempt + GsiProjectionScanAttempt,
    >(
        &self,
        attempt: &mut A,
        target: (&TargetRecord, Arc<BoundTableInfo>),
        input: (&mut VerificationProgress, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (progress, codec) = input;
            let indexes = target
                .1
                .global_secondary_indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .filter(|index| !index.index_name.is_ttl_index())
                .map(|index| index.index_name.clone())
                .collect::<Vec<_>>();
            match progress.stage.clone() {
                VerificationStage::ProjectionExpected(after) => {
                    let policy = self
                        .load_verification_ttl(attempt, target.0, &target.1)
                        .await?;
                    self.count_expected_projections(
                        attempt,
                        target.0,
                        (&indexes, after),
                        (progress, codec, policy.as_ref()),
                    )
                    .await?;
                }
                VerificationStage::ProjectionRows { index, after, seen } => {
                    let name = indexes.get(index as usize).ok_or_else(corrupt_record)?;
                    let scan = GsiProjectionScan::try_new(
                        Arc::clone(&target.1),
                        name.clone(),
                        after.clone(),
                        Some(1),
                    )?;
                    let page = attempt.scan_gsi_projections(&scan).await?.finish()?;
                    for row in &page.items {
                        self.require_expected_projection(
                            attempt,
                            target.0,
                            (&target.1, name, row),
                            codec,
                        )
                        .await?;
                    }
                    let seen = seen
                        .checked_add(u64::try_from(page.items.len()).map_err(|_| corrupt_record())?)
                        .ok_or_else(corrupt_record)?;
                    let expected = *progress
                        .projection_counts
                        .get(index as usize)
                        .ok_or_else(corrupt_record)?;
                    if seen > expected
                        || (page.continuation.is_some() && page.continuation == after)
                    {
                        return Err(corrupt_record());
                    }
                    progress.stage = if let Some(after) = page.continuation {
                        VerificationStage::ProjectionRows {
                            index,
                            after: Some(after),
                            seen,
                        }
                    } else if seen != expected {
                        return Err(corrupt_record());
                    } else if index as usize + 1 < indexes.len() {
                        VerificationStage::ProjectionRows {
                            index: index + 1,
                            after: None,
                            seen: 0,
                        }
                    } else {
                        VerificationStage::TtlRows {
                            after: None,
                            seen: 0,
                        }
                    };
                }
                _ => return Err(corrupt_record()),
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn count_expected_projections<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        page: (
            &[IndexName],
            Option<infrastructure_provider::record::DurableRecordKey>,
        ),
        input: (
            &mut VerificationProgress,
            &CanonicalJsonCodec,
            Option<&storage_common::TtlConfigRecord>,
        ),
    ) -> StorageResult<()> {
        let (indexes, after) = page;
        if input.0.projection_counts.is_empty() {
            input.0.projection_counts.resize(indexes.len(), 0);
        }
        if input.0.projection_counts.len() != indexes.len() {
            return Err(corrupt_record());
        }
        let page = self
            .load_expected_winners(attempt, root.request.operation.id, after)
            .await?;
        for expected in page.winners {
            let authority = self
                .load_winner_authority(attempt, (&expected.key, &expected.provenance), input.1)
                .await?;
            let images = authority.images()?;
            let Some(image) = images.new else {
                continue;
            };
            if matches!(
                storage_common::PreparedTtlPut::try_new(
                    &root.request.schema.table,
                    input.2,
                    image
                )?,
                storage_common::PreparedTtlPut::NewItem { key: Some(_), .. }
            ) {
                input.0.ttl_count = input
                    .0
                    .ttl_count
                    .checked_add(1)
                    .ok_or_else(corrupt_record)?;
            }
            let declaration = IndexerDeclaration::try_new(
                images.new_indexers.unwrap_or_default().to_vec(),
                root.request.schema.table.max_indexers,
            )?;
            let prepared =
                PreparedGsiPut::try_new(&root.request.schema.table, image, &declaration)?;
            for row in prepared.rows() {
                let ordinal = indexes
                    .iter()
                    .position(|name| *name == row.key.index_name)
                    .ok_or_else(corrupt_record)?;
                input.0.projection_counts[ordinal] = input.0.projection_counts[ordinal]
                    .checked_add(1)
                    .ok_or_else(corrupt_record)?;
            }
        }
        input.0.stage = match page.continuation {
            Some(after) => VerificationStage::ProjectionExpected(Some(after)),
            None if indexes.is_empty() => VerificationStage::TtlRows {
                after: None,
                seen: 0,
            },
            None => VerificationStage::ProjectionRows {
                index: 0,
                after: None,
                seen: 0,
            },
        };
        Ok(())
    }

    async fn require_expected_projection<A: PhysicalRecordAttempt + DynamoAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        input: (&BoundTableInfo, &IndexName, &GsiProjectionRow),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let (table, name, row) = input;
        let source = ItemKey::Table(row.key.table_key_ref().ok_or_else(corrupt_record)?.clone())
            .key_attributes(&table.info)?;
        let canonical = codec.encode_key(&source).map_err(|_| corrupt_record())?;
        let expected = self
            .load_expected_winner(attempt, root.request.operation.id, &canonical)
            .await?
            .ok_or_else(corrupt_record)?;
        let authority = self
            .load_winner_authority(attempt, (&expected.key, &expected.provenance), codec)
            .await?;
        let images = authority.images()?;
        let image = images.new.ok_or_else(corrupt_record)?;
        let declaration = IndexerDeclaration::try_new(
            images.new_indexers.unwrap_or_default().to_vec(),
            table.max_indexers,
        )?;
        let prepared = PreparedGsiPut::try_new(&table.info, image, &declaration)?;
        let projected = prepared
            .rows()
            .find(|projection| &projection.key.index_name == name)
            .ok_or_else(corrupt_record)?;
        if projected.projected_image()?.to_attribute_map()? != row.image.to_attribute_map()? {
            return Err(corrupt_record());
        }
        let (observed, _) = attempt.observe_for_write(table, &source).await?;
        let ItemWriteObservation::Live { revision, .. } = observed else {
            return Err(corrupt_record());
        };
        // Same-projection writes legitimately retain the earlier projection
        // revision. A projection can never originate after its current primary.
        if row.revision.get() > revision.get() {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
