//! Decode and verify SQL stream rows after physical reads.

use storage_types::{StorageEnum, StorageError, StorageResult, StreamItemId, SystemStreamPosition};
use stream_provider::SystemStreamHistoryRecord;

use crate::{SqlRow, SqlValue};

pub(super) fn decode_materialized_row(row: SqlRow) -> StorageResult<stream_provider::StreamItem> {
    let (id, bytes) = decode_stream_address(row)?;
    stream_provider::decode_materialized_stream_row(id, &bytes)
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

pub(crate) fn decode_history_row(
    row: SqlRow,
) -> StorageResult<(
    storage_types::SystemHistoryRecord,
    SystemStreamHistoryRecord,
)> {
    let [shard, lineage, order, offset, table, record, created_at]: [SqlValue; 7] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageEnum::HistoryUnavailable)?;
    let (
        SqlValue::Text(shard),
        SqlValue::Text(lineage),
        SqlValue::Blob(order),
        SqlValue::Text(offset),
        SqlValue::Blob(record),
        SqlValue::Integer(created_at),
    ) = (shard, lineage, order, offset, record, created_at)
    else {
        return Err(StorageEnum::HistoryUnavailable.into());
    };
    let position = SystemStreamPosition::try_new(shard, lineage, offset)
        .map_err(|_| StorageEnum::HistoryUnavailable)?;
    if order.as_slice()
        != position
            .offset()
            .order()
            .map_err(|_| StorageEnum::HistoryUnavailable)?
            .to_be_bytes()
    {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    let record: storage_types::SystemHistoryRecord =
        storage_types::storage_serde::from_bytes(&record)?;
    let matches_table = match (table, record.table_history_id()) {
        (SqlValue::Null, None) => true,
        (SqlValue::Blob(bytes), Some(history)) => bytes.as_slice() == history.as_ref(),
        _ => false,
    };
    if record.position() != &position
        || record.created_at().timestamp_millis() != created_at
        || !matches_table
    {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    Ok((record, SystemStreamHistoryRecord { position }))
}
