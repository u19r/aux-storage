//! Decode and verify KV stream rows after physical reads.

use storage_types::{StorageError, StorageResult, StreamItemId};
use stream_provider::{StreamItem, SystemStreamHistoryRecord, decode_stream_item_row};

use crate::KvEntry;

pub(crate) fn decode_stream_row(prefix: &[u8], entry: KvEntry) -> StorageResult<StreamItem> {
    let id = decode_stream_key(prefix, &entry.key)?;
    decode_stream_item_row(id, &entry.value)
}

pub(crate) fn decode_materialized_row(
    prefix: &[u8],
    entry: KvEntry,
) -> StorageResult<stream_provider::StreamItem> {
    let id = decode_stream_key(prefix, &entry.key)?;
    stream_provider::decode_materialized_stream_row(id, &entry.value)
}

pub(super) fn decode_stream_key(prefix: &[u8], key: &[u8]) -> StorageResult<StreamItemId> {
    let id = key
        .strip_prefix(prefix)
        .ok_or_else(|| StorageError::internal("KV stream row has another prefix"))?;
    StreamItemId::try_from(id).map_err(|_| StorageError::internal("KV stream row has invalid ID"))
}

/// Decode the logical position from its native source key.
pub(super) fn decode_history_row(
    entry: KvEntry,
) -> StorageResult<(
    storage_types::SystemHistoryRecord,
    SystemStreamHistoryRecord,
)> {
    let record = crate::stream_write::record::decode_record(&entry.key, &entry.value)?;
    let observation = SystemStreamHistoryRecord {
        position: record.position().clone(),
    };
    Ok((record, observation))
}
