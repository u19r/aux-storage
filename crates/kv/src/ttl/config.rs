pub use storage_common::ttl::{
    TtlConfigRecord, TtlSweepLock, augment_item_with_ttl_partition, compute_ttl_partition_value,
    is_ttl_index, parse_ttl_index_key, shard_to_string, ttl_gsi_name, ttl_index_key,
    ttl_index_key_for_item, ttl_index_key_map_from_token, ttl_index_key_token_for_item,
    ttl_index_prefix, ttl_index_range_end, ttl_index_range_start, ttl_value_from_item,
};
use storage_types::{
    AttributeValue, IndexName, ItemKey, StorageError, StorageResult, TableName, WireItem,
};

use crate::keyspace::{
    compact::{self, KeyRange, ParsedCompactKey},
    table_identity::TableIdentity,
};

pub(crate) fn compact_ttl_index_key(
    table_identity: &TableIdentity,
    ttl_seconds: i64,
    key_token: &str,
) -> StorageResult<Vec<u8>> {
    let ttl_seconds = u64::try_from(ttl_seconds).map_err(|_| {
        StorageError::internal("ttl index key cannot encode a negative expiration timestamp")
    })?;
    Ok(compact::ttl_due_key(
        table_identity.table_id,
        ttl_seconds,
        key_token.as_bytes(),
    ))
}

pub(crate) fn compact_ttl_index_key_for_item(
    table_identity: &TableIdentity,
    table_info: &storage_types::StoredTableInfo,
    ttl_attribute: &str,
    item: &std::collections::HashMap<String, AttributeValue>,
) -> StorageResult<Option<Vec<u8>>> {
    let Some(ttl_seconds) = storage_common::ttl::ttl_value_from_item(item, ttl_attribute) else {
        return Ok(None);
    };
    let token = storage_common::ttl::ttl_index_key_token_for_item(table_info, item)?;
    compact_ttl_index_key(table_identity, ttl_seconds, &token).map(Some)
}

pub(crate) fn compact_ttl_index_key_for_wire_item(
    table_identity: &TableIdentity,
    table_info: &storage_types::StoredTableInfo,
    ttl_attribute: &str,
    item: &WireItem,
) -> StorageResult<Option<Vec<u8>>> {
    let Some(ttl_seconds) = storage_common::ttl::ttl_value_from_wire_item(item, ttl_attribute)?
    else {
        return Ok(None);
    };
    let token = storage_common::ttl::ttl_index_key_token_for_wire_item(table_info, item)?;
    compact_ttl_index_key(table_identity, ttl_seconds, &token).map(Some)
}

pub(crate) fn compact_ttl_index_range(
    table_identity: &TableIdentity,
    now_seconds: i64,
) -> StorageResult<KeyRange> {
    let next_second = now_seconds.saturating_add(1);
    Ok(KeyRange {
        start: compact::ttl_due_key(table_identity.table_id, 0, b""),
        end: compact::ttl_due_key(
            table_identity.table_id,
            u64::try_from(next_second).map_err(|_| {
                StorageError::internal("ttl index range cannot encode a negative timestamp")
            })?,
            b"",
        ),
    })
}

pub(crate) fn compact_ttl_index_table_range(table_identity: &TableIdentity) -> KeyRange {
    let start = compact::ttl_due_key(table_identity.table_id, 0, b"");
    let mut end = start[..5].to_vec();
    for index in (0..end.len()).rev() {
        if end[index] < 0xFF {
            end[index] += 1;
            end.truncate(index + 1);
            break;
        }
    }
    KeyRange { start, end }
}

pub(crate) fn parse_compact_ttl_index_key(key: &[u8]) -> StorageResult<Option<(i64, String)>> {
    let parsed = compact::parse_compact_key(key)
        .map_err(|err| StorageError::internal(&format!("ttl index key parse failed: {err}")))?;
    let ParsedCompactKey::TtlDueIndex {
        ttl_seconds, key, ..
    } = parsed
    else {
        return Ok(None);
    };
    let ttl_seconds = i64::try_from(ttl_seconds)
        .map_err(|_| StorageError::internal("ttl index timestamp exceeds i64"))?;
    let token = std::str::from_utf8(key)
        .map_err(|err| StorageError::internal(&format!("ttl index token utf8 failed: {err}")))?;
    Ok(Some((ttl_seconds, token.to_string())))
}

pub fn shard_prefix(
    table_name: &TableName,
    gsi_name: &IndexName,
    shard: u8,
) -> StorageResult<Vec<u8>> {
    let mut prefix = ItemKey::index_prefix_from_name(table_name, gsi_name);
    let shard_value = AttributeValue::S(shard_to_string(shard));
    let serialized = ItemKey::serialize_attribute_value_to_bytes(&shard_value)?;
    let length = match u16::try_from(serialized.len()) {
        Ok(value) => value.min(1023),
        Err(_) => 1023,
    };
    let prefix_bytes = (length << 6).to_be_bytes();
    prefix.extend_from_slice(&prefix_bytes);
    prefix.extend_from_slice(&serialized);
    Ok(prefix)
}
