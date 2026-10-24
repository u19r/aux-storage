//! Deleted-item revision authority scoped to one table incarnation.

use storage_provider::canonical_revision_key;
use storage_types::{BoundTableInfo, KeyAttributes, StorageError, StorageResult};

use crate::constants::PRIMARY_TOMBSTONE_PREFIX;

pub(crate) fn primary_tombstone_key(
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<(Vec<u8>, String)> {
    let key_json = canonical_revision_key(key)?;
    Ok((tombstone_json_key(table, &key_json)?, key_json))
}

pub(super) fn tombstone_json_key(table: &BoundTableInfo, key_json: &str) -> StorageResult<Vec<u8>> {
    let count = PRIMARY_TOMBSTONE_PREFIX
        .len()
        .checked_add(table.history_id.as_ref().len())
        .and_then(|count| count.checked_add(8))
        .and_then(|count| count.checked_add(key_json.len()))
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut physical = Vec::new();
    physical
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    physical.extend_from_slice(PRIMARY_TOMBSTONE_PREFIX);
    physical.extend_from_slice(table.history_id.as_ref());
    append_part(&mut physical, key_json.as_bytes())?;
    Ok(physical)
}

fn append_part(key: &mut Vec<u8>, part: &[u8]) -> StorageResult<()> {
    let length = u64::try_from(part.len())
        .map_err(|_| StorageError::internal("KV tombstone identity is too long"))?;
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(part);
    Ok(())
}

pub(super) fn tombstone_prefix(table: &BoundTableInfo) -> StorageResult<Vec<u8>> {
    let count = PRIMARY_TOMBSTONE_PREFIX
        .len()
        .checked_add(table.history_id.as_ref().len())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut prefix = Vec::new();
    prefix
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    prefix.extend_from_slice(PRIMARY_TOMBSTONE_PREFIX);
    prefix.extend_from_slice(table.history_id.as_ref());
    Ok(prefix)
}

pub(super) fn decode_key_json<'a>(table: &BoundTableInfo, key: &'a [u8]) -> StorageResult<&'a str> {
    let suffix = key
        .strip_prefix(PRIMARY_TOMBSTONE_PREFIX)
        .and_then(|key| key.strip_prefix(table.history_id.as_ref().as_slice()))
        .ok_or_else(|| StorageError::internal("KV tombstone belongs to another incarnation"))?;
    let (length, json) = suffix
        .split_at_checked(8)
        .ok_or_else(|| StorageError::internal("KV tombstone key is truncated"))?;
    let length = u64::from_be_bytes(
        length
            .try_into()
            .map_err(|_| StorageError::internal("KV tombstone key length is corrupt"))?,
    );
    if length != json.len() as u64 {
        return Err(StorageError::internal("KV tombstone key length is corrupt"));
    }
    std::str::from_utf8(json).map_err(|_| StorageError::internal("KV tombstone key is not UTF-8"))
}
