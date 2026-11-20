//! Ordered KV TTL index keys used by both RocksDB and FoundationDB.

use storage_common::{TtlIndexKey, ttl::TtlIndexKeyToken};
use storage_provider::maintenance::{ExpirationCandidateCursor, ExpirationIndexLocator};
use storage_types::{StorageError, StorageResult};

use crate::table_key_layout::{TABLE_KEY_PREFIX_BYTES, TableSubFamily, table_prefix};

const PREFIX_BYTES: usize = TABLE_KEY_PREFIX_BYTES + 1;

fn prefix(table_id: u32) -> [u8; PREFIX_BYTES] {
    let mut key = [0; PREFIX_BYTES];
    key[..TABLE_KEY_PREFIX_BYTES]
        .copy_from_slice(&table_prefix(table_id, TableSubFamily::TtlIndex));
    // The zero separator lets scan_end use one as the exclusive upper bound.
    key
}

/// Flipping the sign bit maps signed seconds to lexicographically ordered
/// bytes. The table prefix also bounds the complete signed range without
/// overflow.
pub(crate) fn encode_index_key(table_id: u32, key: &TtlIndexKey) -> StorageResult<Vec<u8>> {
    let token = key.token();
    let count = (PREFIX_BYTES + 8)
        .checked_add(token.len())
        .ok_or(storage_types::AttemptFailure::CAPACITY)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(count)
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    bytes.extend_from_slice(&prefix(table_id));
    key.append_ordered_bytes(&mut bytes)?;
    Ok(bytes)
}

pub(super) fn scan_begin(
    table_id: u32,
    cursor: Option<ExpirationCandidateCursor>,
) -> StorageResult<Vec<u8>> {
    let Some(cursor) = cursor else {
        let mut begin = Vec::new();
        begin
            .try_reserve_exact(prefix(table_id).len())
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
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
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    begin.push(0);
    Ok(begin)
}

pub(super) fn scan_end(table_id: u32, cutoff: i64) -> StorageResult<Vec<u8>> {
    let mut end = Vec::new();
    end.try_reserve_exact(if cutoff == i64::MAX {
        PREFIX_BYTES
    } else {
        PREFIX_BYTES + 8
    })
    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    end.extend_from_slice(&prefix(table_id));
    if cutoff == i64::MAX {
        end[PREFIX_BYTES - 1] = 1;
    } else {
        end.extend_from_slice(&TtlIndexKey::encode_ordered_seconds(cutoff + 1));
    }
    Ok(end)
}

pub(super) fn decode_index_key(table_id: u32, key: &[u8]) -> StorageResult<ExpirationIndexLocator> {
    let suffix = key
        .strip_prefix(prefix(table_id).as_slice())
        .ok_or_else(|| StorageError::internal("KV expiration index key has another table"))?;
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(suffix.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    owned.extend_from_slice(suffix);
    let key = TtlIndexKey::decode_ordered_bytes(owned)?;
    let (seconds, token): (i64, String) = key.into();
    Ok(ExpirationIndexLocator {
        indexed_expiration_seconds: seconds,
        ttl_item_key: token,
    })
}
