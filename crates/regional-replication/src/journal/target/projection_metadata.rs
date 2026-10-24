use std::{num::NonZeroU32, sync::Arc};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_provider::{
    DynamoAttempt, GsiProjectionMetadataAttempt, GsiTombstoneRow, GsiTombstoneScan,
    ItemWriteObservation,
};
use storage_types::{BoundTableInfo, StorageResult};

use super::record::TargetRecord;
use crate::{
    bootstrap::{VerificationProgress, VerificationStage},
    import::materialized_winner,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn advance_projection_metadata<
        A: PhysicalRecordAttempt + DynamoAttempt + GsiProjectionMetadataAttempt,
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
            let VerificationStage::ProjectionTombstones { index, after } = input.0.stage.clone()
            else {
                return Err(corrupt_record());
            };
            let indexes = target
                .1
                .global_secondary_indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .filter(|index| !index.index_name.is_ttl_index())
                .collect::<Vec<_>>();
            let name = &indexes
                .get(index as usize)
                .ok_or_else(corrupt_record)?
                .index_name;
            let scan = GsiTombstoneScan::try_new(
                Arc::clone(&target.1),
                name.clone(),
                after,
                NonZeroU32::MIN,
            )?;
            let page = attempt.scan_gsi_tombstones(&scan).await?.finish(&scan)?;
            for row in &page.rows {
                self.require_projection_tombstone(attempt, (target.0, &target.1), (row, input.1))
                    .await?;
            }
            input.0.stage = match page.continuation {
                Some(after) => VerificationStage::ProjectionTombstones {
                    index,
                    after: Some(after),
                },
                None if index as usize + 1 < indexes.len() => {
                    VerificationStage::ProjectionTombstones {
                        index: index.checked_add(1).ok_or_else(corrupt_record)?,
                        after: None,
                    }
                }
                None => VerificationStage::MaintenanceAdmission,
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn require_projection_tombstone<A: PhysicalRecordAttempt + DynamoAttempt>(
        &self,
        attempt: &mut A,
        target: (&TargetRecord, &BoundTableInfo),
        input: (&GsiTombstoneRow, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        let key = self
            .load_projection_owner(attempt, target.0.request.operation.id, &input.0.address)
            .await?;
        let expected = self
            .load_expected_winner(attempt, target.0.request.operation.id, &key)
            .await?
            .ok_or_else(corrupt_record)?;
        let authority = self
            .load_winner_authority(attempt, (&expected.key, &expected.provenance), input.1)
            .await?;
        let (observed, _) = attempt
            .observe_for_write(target.1, authority.key()?)
            .await?;
        let winner = materialized_winner(&observed, (expected.provenance.lineage, &key), input.1)?
            .ok_or_else(corrupt_record)?;
        if winner != expected.provenance {
            return Err(corrupt_record());
        }
        let revision = match observed {
            ItemWriteObservation::Live { revision, .. }
            | ItemWriteObservation::Deleted { revision, .. } => revision,
            ItemWriteObservation::Absent => return Err(corrupt_record()),
        };
        if input.0.revision.get() > revision.get() {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
