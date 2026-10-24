//! One KV key layout for GSI build records and private history holds.

use sha2::{Digest as _, Sha256};
use storage_types::{
    BoundTableInfo, IndexName, ItemKey, StorageError, StorageResult, SystemStreamCheckpoint,
    SystemStreamPosition, SystemStreamScope,
};

use crate::{
    catalog::{KvIndexKind, KvTableIdentity},
    constants::{
        GSI_AUTHORITY_FAMILY, GSI_BUILD_FAMILY, GSI_HOLD_FAMILY, GSI_NAME_LOCK_FAMILY,
        GSI_PROJECTION_FAMILY, GSI_RUNNABLE_FAMILY, GSI_TOMBSTONE_FAMILY,
    },
};

pub(super) struct ProjectionKeys {
    pub scope: [u8; 6],
    pub word: Vec<u8>,
    pub live: Vec<u8>,
    pub tombstone: Vec<u8>,
}

impl ProjectionKeys {
    pub(super) fn try_from_suffix(
        table_id: u32,
        index_id: u16,
        suffix: Vec<u8>,
    ) -> StorageResult<Self> {
        let physical = |family: u8, suffix: &[u8]| -> StorageResult<Vec<u8>> {
            let count = 7usize
                .checked_add(suffix.len())
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut key = Vec::new();
            key.try_reserve_exact(count)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            key.push(family);
            key.extend_from_slice(&index_incarnation(table_id, index_id));
            key.extend_from_slice(suffix);
            Ok(key)
        };
        let digest = projection_address(&suffix);
        Ok(Self {
            scope: index_incarnation(table_id, index_id),
            word: suffix,
            live: physical(GSI_AUTHORITY_FAMILY, &digest)?,
            tombstone: physical(GSI_TOMBSTONE_FAMILY, &digest)?,
        })
    }
}

pub(crate) fn projection_address(suffix: &[u8]) -> [u8; 32] {
    Sha256::digest(suffix).into()
}

pub(super) fn projection_keys(
    identity: &KvTableIdentity,
    table: &BoundTableInfo,
    key: &storage_backfill::GsiProjectionKey,
) -> StorageResult<ProjectionKeys> {
    let index_id = identity
        .indexes
        .iter()
        .find(|candidate| {
            candidate.index_name == key.index_name && candidate.index_kind == KvIndexKind::UserGsi
        })
        .ok_or_else(|| StorageError::internal("GSI projection has no KV index identity"))?
        .index_id;
    ProjectionKeys::try_from_suffix(identity.table_id, index_id, projection_suffix(table, key)?)
}

pub(super) fn projection_suffix(
    table: &BoundTableInfo,
    key: &storage_backfill::GsiProjectionKey,
) -> StorageResult<Vec<u8>> {
    ordered_projection_suffix(&key.item_key(&table.info)?)
}

pub(crate) fn ordered_projection_key(
    identity: &KvTableIdentity,
    index_id: u16,
    key: &ItemKey,
) -> StorageResult<Vec<u8>> {
    // A logical comparison/cursor key; only its partition prefix is stored
    // natively.
    Ok([
        vec![GSI_PROJECTION_FAMILY],
        index_incarnation(identity.table_id, index_id).to_vec(),
        ordered_projection_suffix(key)?,
    ]
    .concat())
}

pub(crate) fn ordered_projection_suffix(key: &ItemKey) -> StorageResult<Vec<u8>> {
    let word = compact_partition(key, key.sorted_storage_suffix()?)?;
    if word.len() > crate::constants::GSI_TREE_MAX_WORD_BYTES {
        return Err(StorageError::internal(
            "GSI comparison word exceeds its layout",
        ));
    }
    Ok(word)
}

pub(crate) fn ordered_projection_prefix(key: &ItemKey) -> StorageResult<Vec<u8>> {
    compact_partition(key, key.sorted_storage_prefix()?)
}

fn compact_partition(key: &ItemKey, encoded: Vec<u8>) -> StorageResult<Vec<u8>> {
    let name = key
        .index_id()
        .ok_or_else(|| StorageError::internal("GSI physical key has no index"))?;
    let partition = ItemKey::index_prefix(
        key.table_name().clone(),
        name.clone(),
        key.hash_key().clone(),
        None,
    )
    .sorted_storage_suffix()?;
    let tail = encoded
        .strip_prefix(partition.as_slice())
        .ok_or_else(|| StorageError::internal("GSI partition encoding differs from full key"))?;
    let count = 32usize
        .checked_add(tail.len())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut suffix = Vec::new();
    suffix
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    suffix.extend_from_slice(&projection_address(&partition));
    suffix.extend_from_slice(tail);
    Ok(suffix)
}

pub(crate) fn gsi_name_lock_key(table_id: u32, index: &IndexName) -> Vec<u8> {
    let mut key = Vec::with_capacity(5 + index.as_ref().len());
    key.push(GSI_NAME_LOCK_FAMILY);
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(index.as_ref().as_bytes());
    key
}

/// A GSI incarnation is the table ID followed by its stable index ID.
pub(crate) fn index_incarnation(table_id: u32, index_id: u16) -> [u8; 6] {
    let mut bytes = [0; 6];
    bytes[..4].copy_from_slice(&table_id.to_be_bytes());
    bytes[4..].copy_from_slice(&index_id.to_be_bytes());
    bytes
}

pub(super) fn gsi_build_key(table_id: u32, index_id: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(7);
    key.push(GSI_BUILD_FAMILY);
    key.extend_from_slice(&index_incarnation(table_id, index_id));
    key
}

pub(super) fn gsi_runnable_key(table_id: u32, index_id: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(7);
    key.push(GSI_RUNNABLE_FAMILY);
    key.extend_from_slice(&index_incarnation(table_id, index_id));
    key
}

pub(super) fn hold_key(
    checkpoint: &SystemStreamCheckpoint,
    position: &SystemStreamPosition,
    incarnation: &[u8],
) -> StorageResult<Vec<u8>> {
    let scope = checkpoint.scope();
    let mut key = hold_prefix(scope, position)?;
    let offset = position
        .offset()
        .order()
        .map_err(|_| StorageError::internal("GSI hold offset is invalid"))?;
    let count = position
        .offset()
        .versionstamp()
        .map_or(8, |stamp| stamp.len())
        .checked_add(incarnation.len())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    key.try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    if let Some(stamp) = position.offset().versionstamp() {
        key.extend_from_slice(&stamp);
    } else {
        key.extend_from_slice(&offset.to_be_bytes());
    }
    key.extend_from_slice(incarnation);
    Ok(key)
}

/// Prefix for one lineage's ordered private holds in one source generation.
/// A single range read can observe the minimum consumed offset regardless of
/// the number of GSI builds in other scopes or generations.
pub(crate) fn hold_prefix(
    scope: &SystemStreamScope,
    position: &SystemStreamPosition,
) -> StorageResult<Vec<u8>> {
    let count = [
        scope.scope_id().as_str().len(),
        position.shard_id().as_str().len(),
        position.lineage_id().as_str().len(),
    ]
    .into_iter()
    .try_fold(
        1usize + scope.generation().to_be_bytes().len() + 6,
        |count, length| count.checked_add(length),
    )
    .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut key = Vec::new();
    key.try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    key.push(GSI_HOLD_FAMILY);
    append_segment(&mut key, scope.scope_id().as_str().as_bytes())?;
    key.extend_from_slice(&scope.generation().to_be_bytes());
    append_segment(&mut key, position.shard_id().as_str().as_bytes())?;
    append_segment(&mut key, position.lineage_id().as_str().as_bytes())?;
    Ok(key)
}

fn append_segment(key: &mut Vec<u8>, part: &[u8]) -> StorageResult<()> {
    let len = u16::try_from(part.len())
        .map_err(|_| StorageError::internal("GSI hold identifier is too long"))?;
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(part);
    Ok(())
}
