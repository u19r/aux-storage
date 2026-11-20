//! Decode physical SQL GSI build rows.

use storage_backfill::{GsiBuild, GsiRecord};
use storage_types::{StorageError, StorageResult};

use crate::{SqlRow, SqlValue};

pub(super) fn decode_build_row(row: SqlRow) -> StorageResult<GsiBuild<Vec<u8>>> {
    let [
        history_id,
        table_name,
        index_name,
        incarnation,
        phase,
        state,
    ] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL GSI build row width is invalid"))?;
    let (
        SqlValue::Blob(history_id),
        SqlValue::Text(table_name),
        SqlValue::Text(index_name),
        SqlValue::Blob(incarnation),
        SqlValue::Text(phase),
        SqlValue::Blob(state),
    ) = (
        history_id,
        table_name,
        index_name,
        incarnation,
        phase,
        state,
    )
    else {
        return Err(StorageError::internal(
            "SQL GSI build row has invalid types",
        ));
    };
    GsiBuild::try_from(GsiRecord {
        history_id: decode_history_id(history_id)?,
        table_name,
        index_name,
        incarnation,
        phase,
        state,
    })
    .map_err(Into::into)
}

fn decode_history_id(bytes: Vec<u8>) -> StorageResult<storage_types::TableHistoryId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| StorageError::internal("Invalid GSI table identity"))?;
    Ok(bytes.into())
}
