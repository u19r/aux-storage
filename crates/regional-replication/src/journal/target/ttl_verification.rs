use std::{num::NonZeroUsize, sync::Arc};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::content::CanonicalEventCodec;
use storage_common::{PreparedTtlPut, TtlConfigRecord, ttl::TtlIndexKeyToken};
use storage_provider::{
    DynamoAttempt, TtlProjectionScan, TtlProjectionScanAttempt, maintenance::ExpirationIndexLocator,
};
use storage_types::{BoundTableInfo, KeyAttributes, StorageResult};

use super::record::TargetRecord;
use crate::{
    bootstrap::{VerificationProgress, VerificationStage},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn advance_ttl_verification<
        A: PhysicalRecordAttempt + DynamoAttempt + TtlProjectionScanAttempt,
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
            let VerificationStage::TtlRows { after, seen } = input.0.stage.clone() else {
                return Err(corrupt_record());
            };
            let policy = self
                .load_verification_ttl(attempt, target.0, &target.1)
                .await?;
            let scan = TtlProjectionScan::try_new(
                Arc::clone(&target.1),
                after.clone(),
                NonZeroUsize::MIN,
            )?;
            let page = attempt.scan_ttl_projections(&scan).await?;
            for row in &page.rows {
                self.require_expected_ttl(
                    attempt,
                    target.0,
                    (&target.1, row),
                    (policy.as_ref(), input.1),
                )
                .await?;
            }
            let seen = seen
                .checked_add(u64::try_from(page.rows.len()).map_err(|_| corrupt_record())?)
                .ok_or_else(corrupt_record)?;
            if seen > input.0.ttl_count
                || (page.continuation.is_some() && page.continuation == after)
            {
                return Err(corrupt_record());
            }
            input.0.stage = if let Some(after) = page.continuation {
                VerificationStage::TtlRows {
                    after: Some(after),
                    seen,
                }
            } else if seen != input.0.ttl_count {
                return Err(corrupt_record());
            } else {
                VerificationStage::ProjectionMetadataBaselines(None)
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn require_expected_ttl<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        row: (&BoundTableInfo, &ExpirationIndexLocator),
        input: (Option<&TtlConfigRecord>, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        let token = TtlIndexKeyToken::from(row.1.ttl_item_key.clone());
        let key = KeyAttributes::from(token.parse_key_map(&row.0.info)?);
        let canonical = input.1.encode_key(&key).map_err(|_| corrupt_record())?;
        let expected = self
            .load_expected_winner(attempt, root.request.operation.id, &canonical)
            .await?
            .ok_or_else(corrupt_record)?;
        let authority = self
            .load_winner_authority(attempt, (&expected.key, &expected.provenance), input.1)
            .await?;
        let images = authority.images()?;
        let image = images.new.ok_or_else(corrupt_record)?;
        let PreparedTtlPut::NewItem {
            key: Some(expected),
            ..
        } = PreparedTtlPut::try_new(&row.0.info, input.0, image)?
        else {
            return Err(corrupt_record());
        };
        let (seconds, token): (i64, String) = expected.into();
        if seconds != row.1.indexed_expiration_seconds || token != row.1.ttl_item_key {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
