//! Validate the durable incarnation that fences all KV maintenance work.

use storage_types::{StorageError, StorageResult, maintenance::TargetIncarnation};

use crate::{
    KvAttempt, KvMutation, ReadProtection, constants::MAINTENANCE_IDENTITY_KEY as IDENTITY_KEY,
};

pub(crate) async fn read_identity<A: KvAttempt>(
    attempt: &mut A,
) -> StorageResult<Option<TargetIncarnation>> {
    decode_identity(attempt.get(IDENTITY_KEY, ReadProtection::Conflict).await?)
}

pub(super) fn decode_identity(value: Option<Vec<u8>>) -> StorageResult<Option<TargetIncarnation>> {
    let Some(bytes) = value else {
        return Ok(None);
    };
    let identity: [u8; 16] = bytes
        .try_into()
        .map_err(|_| StorageError::internal("KV maintenance identity has invalid length"))?;
    Ok(Some(TargetIncarnation::from(identity)))
}

pub(crate) async fn initialize_maintenance<A: KvAttempt>(attempt: &mut A) -> StorageResult<()> {
    if read_identity(attempt).await?.is_none() {
        attempt.stage(vec![KvMutation::Put {
            key: IDENTITY_KEY.to_vec(),
            value: uuid::Uuid::now_v7().as_bytes().to_vec(),
        }])?;
    }
    Ok(())
}
