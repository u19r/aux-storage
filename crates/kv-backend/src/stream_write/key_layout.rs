//! Ordered keys shared by KV stream append, read, cursor, and catalog lowerers.

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName, SystemStreamPosition};
use stream_provider::StreamItem;

use crate::constants::{SYSTEM_STREAM_ITEM_PREFIX, SYSTEM_STREAM_RECORD_PREFIX};

pub(crate) fn item_row_key(row: &StreamItem) -> StorageResult<Vec<u8>> {
    let stream_name = row
        .stream_name
        .as_ref()
        .ok_or_else(|| StorageError::internal("planned Dynamo stream row has no stream name"))?;
    item_key(stream_name, row.id)
}

pub fn item_key(stream_name: &StreamName, id: StreamItemId) -> StorageResult<Vec<u8>> {
    let mut key = item_row_prefix(stream_name)?;
    key.extend_from_slice(id.as_bytes());
    Ok(key)
}

pub fn item_row_prefix(stream_name: &StreamName) -> StorageResult<Vec<u8>> {
    if let Some(history) = stream_name.table_history_id()? {
        let mut key = table_history_row_prefix(history);
        let namespace = history.stream_prefix();
        append_segment(&mut key, &stream_name.as_ref()[namespace.len()..])?;
        return Ok(key);
    }
    let mut key = SYSTEM_STREAM_ITEM_PREFIX.to_vec();
    append_segment(&mut key, stream_name.as_ref())?;
    Ok(key)
}

/// History identity precedes variable-length names so reclamation uses one
/// bounded range.
pub(crate) fn table_history_row_prefix(history: storage_types::TableHistoryId) -> Vec<u8> {
    let mut key = crate::constants::TABLE_HISTORY_ROW_PREFIX.to_vec();
    key.extend_from_slice(history.as_ref());
    key
}

pub(crate) fn read_segment(bytes: &[u8]) -> StorageResult<(&[u8], &[u8])> {
    let Some((length, rest)) = bytes.split_at_checked(2) else {
        return Err(invalid_stream_key_segment());
    };
    let length = u16::from_be_bytes([length[0], length[1]]) as usize;
    rest.split_at_checked(length)
        .ok_or_else(invalid_stream_key_segment)
}

fn invalid_stream_key_segment() -> StorageError {
    StorageError::internal("KV stream key segment is invalid")
}

pub(crate) fn record_prefix(position: &SystemStreamPosition) -> Vec<u8> {
    let mut key = SYSTEM_STREAM_RECORD_PREFIX.to_vec();
    // Logical IDs exclude control characters, including NUL. Termination
    // preserves the same bytewise shard/lineage order as the SQL C collation
    // and the shared trim cursor comparator.
    key.extend_from_slice(position.shard_id().as_str().as_bytes());
    key.push(0);
    key.extend_from_slice(position.lineage_id().as_str().as_bytes());
    key.push(0);
    key
}

pub fn record_key(position: &SystemStreamPosition) -> StorageResult<Vec<u8>> {
    let mut key = record_prefix(position);
    if let Some(stamp) = position.offset().versionstamp() {
        key.extend_from_slice(&stamp);
        return Ok(key);
    }
    let order = position.offset().order().map_err(|error| {
        StorageError::internal(&format!("invalid planned stream offset: {error}"))
    })?;
    key.extend_from_slice(&order.to_be_bytes());
    key.extend_from_slice(position.offset().as_str().as_bytes());
    Ok(key)
}

pub(crate) fn append_segment(key: &mut Vec<u8>, bytes: &[u8]) -> StorageResult<()> {
    let len = u16::try_from(bytes.len()).map_err(|_| {
        StorageError::internal("planned Dynamo stream key segment exceeds 65535 bytes")
    })?;
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(bytes);
    Ok(())
}

pub(crate) fn retired_key(position: &SystemStreamPosition) -> Vec<u8> {
    let record = record_prefix(position);
    let mut key = crate::constants::SYSTEM_STREAM_RETIRED_PREFIX.to_vec();
    key.extend_from_slice(&record[SYSTEM_STREAM_RECORD_PREFIX.len()..]);
    key
}

pub(crate) fn table_record_prefix(history: storage_types::TableHistoryId) -> Vec<u8> {
    let mut key = crate::constants::SYSTEM_STREAM_TABLE_PREFIX.to_vec();
    key.extend_from_slice(history.as_ref());
    key
}

pub(crate) fn table_record_source_key(
    history: storage_types::TableHistoryId,
    key: &[u8],
) -> StorageResult<Vec<u8>> {
    let prefix = table_record_prefix(history);
    let suffix = key
        .strip_prefix(prefix.as_slice())
        .filter(|suffix| !suffix.is_empty())
        .ok_or_else(|| StorageError::internal("System history index has another table ID"))?;
    let mut key = SYSTEM_STREAM_RECORD_PREFIX.to_vec();
    key.extend_from_slice(suffix);
    Ok(key)
}

pub(crate) fn table_record_key(
    history: storage_types::TableHistoryId,
    position: &SystemStreamPosition,
) -> StorageResult<Vec<u8>> {
    let mut key = table_record_prefix(history);
    let record = record_key(position)?;
    key.extend_from_slice(&record[SYSTEM_STREAM_RECORD_PREFIX.len()..]);
    Ok(key)
}
