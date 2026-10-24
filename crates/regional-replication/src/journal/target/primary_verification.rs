use std::{num::NonZeroU32, sync::Arc};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::content::CanonicalEventCodec;
use storage_provider::{
    DynamoAttempt, ItemTombstoneScan, ItemTombstoneScanAttempt, ItemWriteObservation,
    PrimaryStateRow, PrimaryStateScan, PrimaryStateScanAttempt,
};
use storage_types::{BoundTableInfo, StorageResult};

use super::record::TargetRecord;
use crate::{
    bootstrap::{VerificationProgress, VerificationStage},
    import::materialized_winner,
    journal::{JournalStore, reconstruction::ExpectedWinner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn advance_primary_verification<
        A: PhysicalRecordAttempt + DynamoAttempt + PrimaryStateScanAttempt + ItemTombstoneScanAttempt,
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
            let policy = self
                .load_verification_ttl(attempt, target.0, &target.1)
                .await?;
            let ttl = (&target.0.request.schema.table, policy.as_ref());
            match &progress.stage {
                VerificationStage::Expected(after) => {
                    let page = self
                        .load_expected_winners(
                            attempt,
                            target.0.request.operation.id,
                            after.clone(),
                        )
                        .await?;
                    for expected in page.winners {
                        let (observed, _) =
                            attempt.observe_for_write(&target.1, &expected.key).await?;
                        self.require_expected_primary(attempt, &expected, (&observed, codec), ttl)
                            .await?;
                        progress.winners = replication_types::protocol::CanonicalDigest::of_json(
                            &(progress.winners, &expected),
                        )
                        .map_err(|_| corrupt_record())?;
                    }
                    progress.stage = page
                        .continuation
                        .map_or(VerificationStage::Live(None), |after| {
                            VerificationStage::Expected(Some(after))
                        });
                }
                VerificationStage::Live(after) => {
                    let scan = PrimaryStateScan::try_new(target.1, after.clone(), Some(1))?;
                    let page = attempt.scan_primary_state(&scan).await?.finish()?;
                    for row in page.items {
                        self.require_reconstructed_primary(attempt, target.0, (&row, codec), ttl)
                            .await?;
                    }
                    if page.continuation.is_some() && &page.continuation == after {
                        return Err(corrupt_record());
                    }
                    progress.stage = page
                        .continuation
                        .map_or(VerificationStage::Deleted(None), |after| {
                            VerificationStage::Live(Some(after))
                        });
                }
                VerificationStage::Deleted(after) => {
                    let scan = ItemTombstoneScan::try_new(
                        target.1,
                        after.clone(),
                        NonZeroU32::MIN,
                        self.records.limits.max_bytes(),
                    )?;
                    let page = attempt.scan_item_tombstones(&scan).await?;
                    for row in page.items {
                        self.require_reconstructed_primary(
                            attempt,
                            target.0,
                            (&row.into(), codec),
                            ttl,
                        )
                        .await?;
                    }
                    if page.continuation.is_some() && &page.continuation == after {
                        return Err(corrupt_record());
                    }
                    progress.stage = page
                        .continuation
                        .map_or(VerificationStage::ProjectionExpected(None), |after| {
                            VerificationStage::Deleted(Some(after))
                        });
                }
                _ => return Err(corrupt_record()),
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn require_reconstructed_primary<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        input: (&PrimaryStateRow, &CanonicalJsonCodec),
        ttl: (
            &storage_types::StoredTableInfo,
            Option<&storage_common::TtlConfigRecord>,
        ),
    ) -> StorageResult<()> {
        let canonical = input
            .1
            .encode_key(&input.0.key)
            .map_err(|_| corrupt_record())?;
        let expected = self
            .load_expected_winner(attempt, root.request.operation.id, &canonical)
            .await?
            .ok_or_else(corrupt_record)?;
        if expected.key != input.0.key {
            return Err(corrupt_record());
        }
        self.require_expected_primary(attempt, &expected, (&input.0.state, input.1), ttl)
            .await
    }

    async fn require_expected_primary<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        expected: &ExpectedWinner,
        input: (&ItemWriteObservation, &CanonicalJsonCodec),
        ttl: (
            &storage_types::StoredTableInfo,
            Option<&storage_common::TtlConfigRecord>,
        ),
    ) -> StorageResult<()> {
        let winner = materialized_winner(
            input.0,
            (expected.provenance.lineage, &expected.provenance.key),
            input.1,
        )?
        .ok_or_else(corrupt_record)?;
        if winner != expected.provenance {
            return Err(corrupt_record());
        }
        let authority = self
            .load_winner_authority(attempt, (&expected.key, &expected.provenance), input.1)
            .await?;
        let observed_images = match input.0 {
            ItemWriteObservation::Live { stored, .. } => {
                (Some(&stored.item), Some(stored.indexers.as_slice()))
            }
            _ => (None, None),
        };
        authority.require_materialized_images(observed_images)?;
        let images = authority.images()?;
        match (images.new, input.0) {
            (Some(image), ItemWriteObservation::Live { ttl_epoch, .. }) => {
                let expected_epoch =
                    match storage_common::PreparedTtlPut::try_new(ttl.0, ttl.1, image)? {
                        storage_common::PreparedTtlPut::Disabled => None,
                        storage_common::PreparedTtlPut::NewItem { key, .. } => {
                            key.map(|key| key.ttl_seconds())
                        }
                    };
                if *ttl_epoch != expected_epoch {
                    return Err(corrupt_record());
                }
            }
            (None, ItemWriteObservation::Deleted { .. }) => {}
            _ => return Err(corrupt_record()),
        }
        Ok(())
    }
}
