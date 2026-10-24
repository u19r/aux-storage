//! Stream locators bind stable physical history versions or verified effects.

use infrastructure_provider::record::{DurableRecordKey, RecordAddress};
use storage_types::{
    StorageResult, StreamItemId, StreamName, SystemStreamEffectDigest, TableHistoryId,
};

use super::{
    RetainedOwnerKind, RetainedOwnerRecord, RetainedOwnerTransition, error::invalid_image,
};

pub(super) fn history_locator(
    history: TableHistoryId,
    stream: &StreamName,
    version: StreamItemId,
) -> StorageResult<DurableRecordKey> {
    RecordAddress {
        namespace: crate::constants::STREAM_IMAGE_OWNER_NAMESPACE.into(),
        resource: history.as_ref().to_vec(),
        generation: 0,
        suffix: serde_json::to_vec(&(stream, version))?,
    }
    .try_into()
}

pub(super) fn effect_locator(
    history: TableHistoryId,
    effect: &SystemStreamEffectDigest,
) -> StorageResult<DurableRecordKey> {
    RecordAddress {
        namespace: crate::constants::STREAM_IMAGE_OWNER_NAMESPACE.into(),
        resource: history.as_ref().to_vec(),
        generation: 0,
        suffix: effect.as_str().as_bytes().to_vec(),
    }
    .try_into()
}

impl RetainedOwnerRecord {
    pub(crate) async fn publish_stream_batch<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        receipts: Vec<Self>,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let transitions = receipts
                .into_iter()
                .map(|receipt| {
                    receipt.require_stream_role()?;
                    RetainedOwnerTransition::try_new(
                        receipt.kind,
                        receipt.locator.clone(),
                        receipt.history,
                        (None, Some(receipt)),
                    )
                })
                .collect::<StorageResult<Vec<_>>>()?;
            RetainedOwnerTransition::prepare_batch(attempt, transitions).await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub async fn release_stream_batch<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        receipts: Vec<Self>,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            for wave in receipts.chunks(crate::constants::IMAGE_OWNER_READ_RECORDS) {
                let transitions = wave
                    .iter()
                    .map(|receipt| {
                        receipt.require_stream_role()?;
                        RetainedOwnerTransition::try_new(
                            receipt.kind,
                            receipt.locator.clone(),
                            receipt.history,
                            (Some(receipt.clone()), None),
                        )
                    })
                    .collect::<StorageResult<Vec<_>>>()?;
                RetainedOwnerTransition::prepare_batch(attempt, transitions).await?;
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    fn require_stream_role(&self) -> StorageResult<()> {
        if !matches!(
            self.kind,
            RetainedOwnerKind::ItemHistory
                | RetainedOwnerKind::TablePointer
                | RetainedOwnerKind::SystemPointer
                | RetainedOwnerKind::SystemRecord
        ) {
            return Err(invalid_image());
        }
        Ok(())
    }
}
