//! KV key layouts for GSI build records and materialized projections.

use sha2::{Digest as _, Sha256};
use storage_types::{BoundTableInfo, IndexName, ItemKey, StorageError, StorageResult};

use crate::{
    constants::GSI_RUNNABLE_FAMILY,
    table_key_layout::{TableSubFamily, table_key},
};

pub(super) fn entry_key(table_id: u32, index_id: u16, suffix: &[u8]) -> StorageResult<Vec<u8>> {
    table_key(
        table_id,
        TableSubFamily::GsiEntry,
        &[&index_id.to_be_bytes(), suffix],
    )
}

/// All entries of one index incarnation.
pub(crate) fn index_entry_prefix(table_id: u32, index_id: u16) -> StorageResult<Vec<u8>> {
    entry_key(table_id, index_id, &[])
}

fn hash_partition(suffix: &[u8]) -> [u8; 32] {
    Sha256::digest(suffix).into()
}

pub(super) fn projection_suffix(
    table: &BoundTableInfo,
    key: &storage_backfill::GsiProjectionKey,
) -> StorageResult<Vec<u8>> {
    ordered_projection_suffix(&key.item_key(&table.info)?)
}

pub(crate) fn ordered_projection_key(
    table_id: u32,
    index_id: u16,
    key: &ItemKey,
) -> StorageResult<Vec<u8>> {
    // Native order and public continuation evidence use the same complete key.
    entry_key(table_id, index_id, &ordered_projection_suffix(key)?)
}

pub(crate) fn ordered_projection_suffix(key: &ItemKey) -> StorageResult<Vec<u8>> {
    let word = compact_partition(key, key.sorted_storage_suffix()?)?;
    if word.len() > crate::constants::GSI_PROJECTION_SUFFIX_MAX_BYTES {
        return Err(StorageError::internal(
            "GSI ordered suffix exceeds its layout",
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
        .ok_or(storage_types::AttemptFailure::CAPACITY)?;
    let mut suffix = Vec::new();
    suffix
        .try_reserve_exact(count)
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    suffix.extend_from_slice(&hash_partition(&partition));
    suffix.extend_from_slice(tail);
    Ok(suffix)
}

pub(crate) fn gsi_name_lock_key(table_id: u32, index: &IndexName) -> StorageResult<Vec<u8>> {
    table_key(
        table_id,
        TableSubFamily::GsiNameLock,
        &[index.as_ref().as_bytes()],
    )
}

/// A GSI incarnation is the table ID followed by its stable index ID.
pub(crate) fn index_incarnation(table_id: u32, index_id: u16) -> [u8; 6] {
    let mut bytes = [0; 6];
    bytes[..4].copy_from_slice(&table_id.to_be_bytes());
    bytes[4..].copy_from_slice(&index_id.to_be_bytes());
    bytes
}

pub(super) fn gsi_build_key(table_id: u32, index_id: u16) -> StorageResult<Vec<u8>> {
    table_key(
        table_id,
        TableSubFamily::GsiBuild,
        &[&index_id.to_be_bytes()],
    )
}

pub(crate) fn gsi_runnable_key(table_id: u32, index_id: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(7);
    key.push(GSI_RUNNABLE_FAMILY);
    key.extend_from_slice(&index_incarnation(table_id, index_id));
    key
}
