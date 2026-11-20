//! KV storage of the store's stream-enable stamp.

use storage_provider::StreamLabelClockAttempt;
use storage_types::{StorageError, StorageResult, TimestampMillis};

use crate::{
    KvAttempt, KvMutation, ReadProtection, catalog::KvCatalogAttempt,
    constants::STREAM_LABEL_CLOCK_KEY,
};

impl<A: KvAttempt> StreamLabelClockAttempt for KvCatalogAttempt<'_, A> {
    async fn load_stream_label_clock(&mut self) -> StorageResult<Option<TimestampMillis>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            catalog
                .attempt
                .get(STREAM_LABEL_CLOCK_KEY, ReadProtection::Conflict)
                .await?
                .map(|bytes| decode_stamp(&bytes))
                .transpose()
        }
        .await;
        settlement.complete(result)
    }

    async fn store_stream_label_clock(&mut self, stamp: TimestampMillis) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = catalog.attempt.stage_with_primary_layouts(
            vec![KvMutation::PutUnchunked {
                key: STREAM_LABEL_CLOCK_KEY.to_vec(),
                value: stamp.timestamp_millis().to_be_bytes().to_vec(),
            }],
            &mut catalog.primary_layouts,
        );
        settlement.complete(result)
    }
}

fn decode_stamp(bytes: &[u8]) -> StorageResult<TimestampMillis> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::internal("invalid KV stream label clock"))?;
    Ok(TimestampMillis::from_timestamp(i64::from_be_bytes(bytes)))
}
