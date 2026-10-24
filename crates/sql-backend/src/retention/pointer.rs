//! Protected pointer ownership reads and releases in the caller's SQL attempt.

use storage_types::{StorageError, StorageResult, StreamItemId};

use crate::{SqlQueryPage, SqlValue};

pub(crate) fn decode_first_pointer_reference(
    page: SqlQueryPage,
) -> StorageResult<Option<StreamItemId>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL pointer reference read exceeded one row",
        ));
    }
    page.rows
        .into_iter()
        .next()
        .map(|row| {
            let [SqlValue::Blob(bytes)]: [SqlValue; 1] = row
                .into_values()
                .try_into()
                .map_err(|_| StorageError::internal("SQL pointer reference has invalid columns"))?
            else {
                return Err(StorageError::internal(
                    "SQL pointer reference ID is invalid",
                ));
            };
            StreamItemId::try_from(bytes.as_slice())
                .map_err(|_| StorageError::internal("SQL pointer reference ID has invalid length"))
        })
        .transpose()
}
