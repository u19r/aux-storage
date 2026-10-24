//! Ordered KV TTL index keys used by both RocksDB and FoundationDB.

use storage_common::{TtlIndexKey, ttl::TtlIndexKeyToken};
use storage_provider::maintenance::{ExpirationCandidateCursor, ExpirationIndexLocator};
use storage_types::{StorageError, StorageResult};

use crate::constants::TTL_INDEX_FAMILY;

fn prefix(table_id: u32) -> [u8; 6] {
    let mut key = [0; 6];
    key[0] = TTL_INDEX_FAMILY;
    key[1..5].copy_from_slice(&table_id.to_be_bytes());
    // The zero separator lets scan_end use one as the exclusive upper bound.
    key
}

/// Flipping the sign bit maps signed seconds to lexicographically ordered
/// bytes. The table prefix also bounds the complete signed range without
/// overflow.
pub(crate) fn encode_index_key(table_id: u32, key: &TtlIndexKey) -> StorageResult<Vec<u8>> {
    let (seconds, token): (i64, &str) = key.into();
    let count = 14usize
        .checked_add(token.len())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    bytes.extend_from_slice(&prefix(table_id));
    bytes.extend_from_slice(&encode_seconds(seconds));
    bytes.extend_from_slice(token.as_bytes());
    Ok(bytes)
}

fn encode_seconds(seconds: i64) -> [u8; 8] {
    let mut bytes = seconds.to_be_bytes();
    bytes[0] ^= 0x80;
    bytes
}

pub(super) fn scan_begin(
    table_id: u32,
    cursor: Option<ExpirationCandidateCursor>,
) -> StorageResult<Vec<u8>> {
    let Some(cursor) = cursor else {
        let mut begin = Vec::new();
        begin
            .try_reserve_exact(prefix(table_id).len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        begin.extend_from_slice(&prefix(table_id));
        return Ok(begin);
    };
    let key = TtlIndexKey::new(
        cursor.expiration_seconds,
        TtlIndexKeyToken::from(cursor.ttl_item_key),
    );
    let mut begin = encode_index_key(table_id, &key)?;
    begin
        .try_reserve_exact(1)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    begin.push(0);
    Ok(begin)
}

pub(super) fn scan_end(table_id: u32, cutoff: i64) -> StorageResult<Vec<u8>> {
    let mut end = Vec::new();
    end.try_reserve_exact(if cutoff == i64::MAX { 6 } else { 14 })
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    end.extend_from_slice(&prefix(table_id));
    if cutoff == i64::MAX {
        end[5] = 1;
    } else {
        end.extend_from_slice(&encode_seconds(cutoff + 1));
    }
    Ok(end)
}

pub(super) fn decode_index_key(table_id: u32, key: &[u8]) -> StorageResult<ExpirationIndexLocator> {
    let suffix = key
        .strip_prefix(prefix(table_id).as_slice())
        .ok_or_else(|| StorageError::internal("KV expiration index key has another table"))?;
    let (seconds, token) = suffix
        .split_at_checked(8)
        .ok_or_else(|| StorageError::internal("KV expiration index key is too short"))?;
    if token.is_empty() {
        return Err(StorageError::internal("KV expiration index token is empty"));
    }
    let mut seconds: [u8; 8] = seconds
        .try_into()
        .map_err(|_| StorageError::internal("KV expiration index seconds have invalid width"))?;
    seconds[0] ^= 0x80;
    let seconds = i64::from_be_bytes(seconds);
    let token = std::str::from_utf8(token)
        .map_err(|_| StorageError::internal("KV expiration index token is invalid UTF-8"))?;
    let mut owned = String::new();
    owned
        .try_reserve_exact(token.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    owned.push_str(token);
    Ok(ExpirationIndexLocator {
        indexed_expiration_seconds: seconds,
        ttl_item_key: owned,
    })
}
