//! Decode and verify KV stream rows after physical reads.

use storage_types::{StorageEnum, StorageError, StorageResult, StreamItemId};
use stream_provider::{
    StreamItem, SystemStreamHistoryRecord, decode_stream_item_row,
    retained_image::system_stream::RetainedSystemStreamRecord,
};

use crate::KvEntry;

pub(crate) fn decode_stream_row(prefix: &[u8], entry: KvEntry) -> StorageResult<StreamItem> {
    let id = decode_stream_key(prefix, &entry.key)?;
    decode_stream_item_row(id, &entry.value)
}

pub(crate) fn decode_stored_stream_row(
    prefix: &[u8],
    entry: KvEntry,
) -> StorageResult<stream_provider::retained_image::stream_row::StoredStreamRow> {
    let id = decode_stream_key(prefix, &entry.key)?;
    stream_provider::retained_image::stream_row::StoredStreamRow::decode(
        id,
        &entry.value,
        crate::constants::STREAM_READ_BYTES.get(),
    )
}

fn decode_stream_key(prefix: &[u8], key: &[u8]) -> StorageResult<StreamItemId> {
    let id = key
        .strip_prefix(prefix)
        .ok_or_else(|| StorageError::internal("KV stream row has another prefix"))?;
    StreamItemId::try_from(id).map_err(|_| StorageError::internal("KV stream row has invalid ID"))
}

/// Decode logical history against its physical pointer observation.
pub(super) fn decode_history_row(
    entry: KvEntry,
) -> StorageResult<(RetainedSystemStreamRecord, SystemStreamHistoryRecord)> {
    let row: crate::stream_write::record::KvSystemStreamRecord =
        crate::stream_write::record::KvSystemStreamRecord::decode(&entry.key, &entry.value)?;
    if crate::stream_write::key_layout::record_key(&row.record.position)? != entry.key {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    let observation = SystemStreamHistoryRecord {
        position: row.record.position.clone(),
        expected_pointer: row.pointer_item_id,
        actual_pointer: None,
    };
    Ok((row.record, observation))
}
