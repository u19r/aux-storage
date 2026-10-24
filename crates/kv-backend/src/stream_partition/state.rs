//! Durable partition authority, protected by the caller's native attempt.

use storage_common::{
    DEFAULT_ORDERED_LOG_PARTITION_COUNT,
    partition::{PartitionFamilyKind, ResolvedPartitionFamily},
};
use storage_types::{StorageError, StorageResult, StreamName, storage_serde};

use super::key_layout::state_key;
use crate::{KvAttempt, KvMutation, ReadProtection};

pub(crate) fn initial_state(stream_name: &StreamName) -> StorageResult<KvMutation> {
    let family = ResolvedPartitionFamily::new(
        PartitionFamilyKind::OrderedLog,
        DEFAULT_ORDERED_LOG_PARTITION_COUNT,
    );
    Ok(KvMutation::Put {
        key: state_key(stream_name)?,
        value: storage_serde::to_bytes(&family)?,
    })
}

pub(crate) fn delete_state(stream_name: &StreamName) -> StorageResult<KvMutation> {
    Ok(KvMutation::Delete {
        key: state_key(stream_name)?,
    })
}

pub(crate) async fn load_state<A: KvAttempt>(
    attempt: &mut A,
    stream_name: &StreamName,
    protection: ReadProtection,
) -> StorageResult<ResolvedPartitionFamily> {
    let bytes = attempt
        .get(&state_key(stream_name)?, protection)
        .await?
        .ok_or_else(|| StorageError::internal("ordered stream lost its partition authority"))?;
    decode_state(&bytes)
}

pub(super) fn decode_state(bytes: &[u8]) -> StorageResult<ResolvedPartitionFamily> {
    let family: ResolvedPartitionFamily = storage_serde::from_bytes(bytes)?;
    if family.config.family_kind != PartitionFamilyKind::OrderedLog {
        return Err(StorageError::internal(
            "ordered stream has another partition family",
        ));
    }
    family.validate()?;
    Ok(family)
}
