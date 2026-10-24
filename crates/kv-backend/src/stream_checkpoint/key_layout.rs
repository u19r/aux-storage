//! Typed addresses for logical checkpoint state.

use storage_types::{
    StorageResult, SystemStreamCheckpointToken, SystemStreamPosition, SystemStreamScope,
};

use crate::{
    constants::{
        SYSTEM_STREAM_CHECKPOINT_OPERATION_PREFIX, SYSTEM_STREAM_CHECKPOINT_TOKEN_PREFIX,
        SYSTEM_STREAM_PROTECTED_FRONTIER_PREFIX,
    },
    stream_write::key_layout::append_segment,
};

pub(crate) fn token(token: &SystemStreamCheckpointToken) -> Vec<u8> {
    let mut key = SYSTEM_STREAM_CHECKPOINT_TOKEN_PREFIX.to_vec();
    key.extend_from_slice(token.as_str().as_bytes());
    key
}

pub(super) fn operation(operation_key: &str) -> Vec<u8> {
    let mut key = SYSTEM_STREAM_CHECKPOINT_OPERATION_PREFIX.to_vec();
    key.extend_from_slice(
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, operation_key.as_bytes()).as_bytes(),
    );
    key
}

pub(crate) fn frontier(
    scope: &SystemStreamScope,
    position: &SystemStreamPosition,
) -> StorageResult<Vec<u8>> {
    let mut key = frontier_prefix(scope)?;
    append_segment(&mut key, position.shard_id().as_str().as_bytes())?;
    append_segment(&mut key, position.lineage_id().as_str().as_bytes())?;
    Ok(key)
}

pub(crate) fn frontier_prefix(scope: &SystemStreamScope) -> StorageResult<Vec<u8>> {
    let mut key = SYSTEM_STREAM_PROTECTED_FRONTIER_PREFIX.to_vec();
    append_segment(&mut key, scope.scope_id().as_str().as_bytes())?;
    key.extend_from_slice(&scope.generation().to_be_bytes());
    Ok(key)
}
