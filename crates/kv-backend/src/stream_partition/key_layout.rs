//! Keys for partition control, ordered rows, and point-read routing.

use storage_common::partition::PartitionLoadSampleRecord;
use storage_types::{StorageError, StorageResult, StreamItemId, StreamName};

use crate::{
    constants::{
        PARTITION_ITEM_INDEX_PREFIX, PARTITION_ROW_PREFIX, PARTITION_SAMPLE_PREFIX,
        PARTITION_STATE_PREFIX,
    },
    stream_write::key_layout::append_segment,
};

pub(crate) fn state_key(stream_name: &StreamName) -> StorageResult<Vec<u8>> {
    scoped_key(PARTITION_STATE_PREFIX, stream_name)
}

pub(super) fn row_prefix(
    stream_name: &StreamName,
    placement_slot: u16,
    partition_id: u16,
) -> StorageResult<Vec<u8>> {
    let mut prefix = scoped_key(PARTITION_ROW_PREFIX, stream_name)?;
    prefix.extend_from_slice(&placement_slot.to_be_bytes());
    prefix.extend_from_slice(&partition_id.to_be_bytes());
    Ok(prefix)
}

pub(super) fn index_prefix(stream_name: &StreamName) -> StorageResult<Vec<u8>> {
    scoped_key(PARTITION_ITEM_INDEX_PREFIX, stream_name)
}

pub(crate) fn index_key(stream_name: &StreamName, id: StreamItemId) -> StorageResult<Vec<u8>> {
    let mut key = index_prefix(stream_name)?;
    key.extend_from_slice(id.as_bytes());
    Ok(key)
}

pub(super) fn encode_route(
    placement_slot: u16,
    partition_id: u16,
) -> [u8; crate::constants::STREAM_ROUTE_BYTES] {
    let [slot_high, slot_low] = placement_slot.to_be_bytes();
    let [id_high, id_low] = partition_id.to_be_bytes();
    [slot_high, slot_low, id_high, id_low]
}

pub(super) fn decode_route(bytes: Vec<u8>) -> StorageResult<(u16, u16)> {
    let [slot_high, slot_low, id_high, id_low]: [u8; crate::constants::STREAM_ROUTE_BYTES] = bytes
        .try_into()
        .map_err(|_| StorageError::internal("partitioned stream item index is invalid"))?;
    Ok((
        u16::from_be_bytes([slot_high, slot_low]),
        u16::from_be_bytes([id_high, id_low]),
    ))
}

pub(super) fn scoped_key(prefix: &[u8], stream_name: &StreamName) -> StorageResult<Vec<u8>> {
    let mut key = prefix.to_vec();
    append_segment(&mut key, stream_name.as_ref())?;
    Ok(key)
}

pub(crate) fn sample_key(
    stream: &StreamName,
    record: &PartitionLoadSampleRecord,
) -> StorageResult<Vec<u8>> {
    let mut key = scoped_key(PARTITION_SAMPLE_PREFIX, stream)?;
    key.extend_from_slice(&record.partition_id.to_be_bytes());
    append_segment(&mut key, record.publisher_id.as_bytes())?;
    key.extend_from_slice(&record.window_start_ms.to_be_bytes());
    Ok(key)
}
