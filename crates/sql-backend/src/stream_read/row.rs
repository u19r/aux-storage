//! Decode and verify SQL stream rows after physical reads.

use storage_types::{StorageEnum, StorageError, StorageResult, StreamItemId, SystemStreamPosition};

use crate::{SqlRow, SqlValue};

pub(super) fn decode_stream_row(row: SqlRow) -> StorageResult<stream_provider::StreamItem> {
    let (id, bytes) = decode_stream_address(row)?;
    stream_provider::decode_stream_item_row(id, &bytes)
}

pub(super) fn decode_stream_address(row: SqlRow) -> StorageResult<(StreamItemId, Vec<u8>)> {
    let [id, bytes] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL stream row has invalid width"))?;
    let (SqlValue::Blob(id), SqlValue::Blob(bytes)) = (id, bytes) else {
        return Err(StorageError::internal("SQL stream row has invalid types"));
    };
    let id = StreamItemId::try_from(id.as_slice())
        .map_err(|_| StorageError::internal("SQL stream row has invalid ID"))?;
    Ok((id, bytes))
}

/// Decode the native canonical address and its self-contained value.
pub(super) fn decode_entry_row(
    row: SqlRow,
) -> StorageResult<stream_provider::PositionedStreamEntry> {
    let [table, shard, lineage, order, offset, value]: [SqlValue; 6] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageEnum::HistoryUnavailable)?;
    let (
        SqlValue::Blob(table),
        SqlValue::Text(shard),
        SqlValue::Text(lineage),
        SqlValue::Blob(order),
        SqlValue::Text(offset),
        SqlValue::Blob(value),
    ) = (table, shard, lineage, order, offset, value)
    else {
        return Err(StorageEnum::HistoryUnavailable.into());
    };
    let (history_id, position) = decode_entry_position(table, shard, lineage, order, offset)?;
    stream_provider::PositionedStreamEntry::decode_value(history_id, position, &value)
}

pub(super) fn decode_observation_row(
    row: SqlRow,
) -> StorageResult<stream_provider::TableHistoryObservation> {
    let [
        SqlValue::Blob(table),
        SqlValue::Text(shard),
        SqlValue::Text(lineage),
        SqlValue::Blob(order),
        SqlValue::Text(offset),
        SqlValue::Integer(created_at),
    ]: [SqlValue; 6] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageEnum::HistoryUnavailable)?
    else {
        return Err(StorageEnum::HistoryUnavailable.into());
    };
    let (history_id, position) = decode_entry_position(table, shard, lineage, order, offset)?;
    Ok(stream_provider::TableHistoryObservation {
        history_id,
        position,
        created_at: storage_types::TimestampMillis::from_timestamp(created_at),
    })
}

fn decode_entry_position(
    table: Vec<u8>,
    shard: String,
    lineage: String,
    order: Vec<u8>,
    offset: String,
) -> StorageResult<(storage_types::TableHistoryId, SystemStreamPosition)> {
    let history_id = storage_types::TableHistoryId::from(
        <[u8; 16]>::try_from(table.as_slice()).map_err(|_| StorageEnum::HistoryUnavailable)?,
    );
    let position = SystemStreamPosition::try_new(shard, lineage, offset)
        .map_err(|_| StorageEnum::HistoryUnavailable)?;
    if order
        != position
            .offset()
            .order()
            .map_err(|_| StorageEnum::HistoryUnavailable)?
            .to_be_bytes()
    {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    Ok((history_id, position))
}
