//! Decode and verify SQL stream rows after physical reads.

use storage_types::{
    StorageEnum, StorageError, StorageResult, StreamItemId, SystemStreamPosition, storage_serde,
};
use stream_provider::{
    SystemStreamHistoryRecord, retained_image::system_stream::RetainedSystemStreamRecord,
};

use crate::{SqlRow, SqlValue};

pub(super) fn decode_stored_stream_row(
    row: SqlRow,
) -> StorageResult<stream_provider::retained_image::stream_row::StoredStreamRow> {
    let (id, bytes) = decode_stream_address(row)?;
    stream_provider::retained_image::stream_row::StoredStreamRow::decode(
        id,
        &bytes,
        crate::constants::STREAM_READ_BYTES.get(),
    )
}

fn decode_stream_address(row: SqlRow) -> StorageResult<(StreamItemId, Vec<u8>)> {
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

pub(super) fn decode_history_row(
    row: SqlRow,
) -> StorageResult<(
    RetainedSystemStreamRecord,
    SystemStreamHistoryRecord,
    Option<stream_provider::retained_image::stream_row::StoredStreamRow>,
)> {
    let [shard, lineage, order, offset, pointer, record, actual]: [SqlValue; 7] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageEnum::HistoryUnavailable)?;
    let (
        SqlValue::Text(shard),
        SqlValue::Text(lineage),
        SqlValue::Blob(order),
        SqlValue::Text(offset),
        SqlValue::Blob(pointer),
        SqlValue::Blob(record),
    ) = (shard, lineage, order, offset, pointer, record)
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
    let expected_pointer =
        StreamItemId::try_from(pointer.as_slice()).map_err(|_| StorageEnum::HistoryUnavailable)?;
    let pointer = match actual {
        SqlValue::Blob(bytes) => {
            let item = stream_provider::retained_image::stream_row::StoredStreamRow::decode(
                expected_pointer,
                &bytes,
                crate::constants::STREAM_READ_BYTES.get(),
            )?;
            item.require_address(
                &storage_types::StreamName::system_table_stream(),
                expected_pointer,
            )?;
            Some(item)
        }
        SqlValue::Null => None,
        _ => return Err(StorageEnum::HistoryUnavailable.into()),
    };
    let record =
        RetainedSystemStreamRecord::decode(&record, crate::constants::STREAM_READ_BYTES.get())?;
    if record.position != position {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    let actual_pointer = pointer.as_ref().map(|item| item.metadata().0);
    Ok((
        record,
        SystemStreamHistoryRecord {
            position,
            expected_pointer,
            actual_pointer,
        },
        pointer,
    ))
}

pub(super) fn decode_blob_row<T: serde::de::DeserializeOwned>(row: SqlRow) -> StorageResult<T> {
    storage_serde::from_bytes(&decode_blob(row)?)
}

pub(super) fn decode_blob(row: SqlRow) -> StorageResult<Vec<u8>> {
    let values: [SqlValue; 1] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL stream blob row has invalid width"))?;
    let [SqlValue::Blob(bytes)] = values else {
        return Err(StorageError::internal(
            "SQL stream blob row has invalid type",
        ));
    };
    Ok(bytes)
}

/// The address comes from the physical predicate, not a second stored copy.
pub(super) fn decode_pointer_reference(
    row: SqlRow,
    address: &(&storage_types::StreamName, StreamItemId),
) -> StorageResult<stream_provider::StreamPointerReference> {
    let values: [SqlValue; 3] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL pointer reference has invalid width"))?;
    let [
        SqlValue::Blob(stream),
        SqlValue::Blob(id),
        SqlValue::Integer(owner),
    ] = values
    else {
        return Err(StorageError::internal(
            "SQL pointer reference has invalid types",
        ));
    };
    let owner = u8::try_from(owner)
        .map_err(|_| StorageError::internal("SQL pointer reference has invalid owner"))?;
    let id = StreamItemId::try_from(id.as_slice())
        .map_err(|_| StorageError::internal("SQL pointer reference has invalid ID"))?;
    Ok(stream_provider::StreamPointerReference {
        owner: stream_provider::PointerOwner::try_from(owner)?,
        owner_stream: (*address.0).clone(),
        owner_item_id: address.1,
        item_stream: storage_types::StreamName::new(&stream),
        item_item_id: Some(id).filter(|id| *id != StreamItemId::default()),
    })
}
