use storage_types::StorageResult;

use super::error::map_driver_error;
use crate::{SqlValue, row_view::SqlValueRef};

pub(super) fn decode_row<'native>(
    row: &'native rusqlite::Row<'_>,
) -> StorageResult<Vec<SqlValueRef<'native>>> {
    let mut cells = Vec::new();
    cells
        .try_reserve_exact(row.as_ref().column_count())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for index in 0..row.as_ref().column_count() {
        use rusqlite::types::ValueRef;
        let cell = match row.get_ref(index).map_err(map_driver_error)? {
            ValueRef::Null => SqlValueRef::Null,
            ValueRef::Integer(value) => SqlValueRef::Integer(value),
            ValueRef::Real(value) => SqlValueRef::Real(value),
            ValueRef::Text(value) => {
                SqlValueRef::Text(std::str::from_utf8(value).map_err(|_| {
                    storage_types::StorageError::internal("SQLite native operation failed")
                })?)
            }
            ValueRef::Blob(value) => SqlValueRef::Blob(value),
        };
        cells.push(cell);
    }
    Ok(cells)
}

impl From<SqlValue> for rusqlite::types::Value {
    fn from(value: SqlValue) -> Self {
        match value {
            SqlValue::Null => rusqlite::types::Value::Null,
            SqlValue::Integer(value) => rusqlite::types::Value::Integer(value),
            SqlValue::Boolean(value) => rusqlite::types::Value::Integer(i64::from(value)),
            SqlValue::Real(value) => rusqlite::types::Value::Real(value),
            SqlValue::Text(value) => rusqlite::types::Value::Text(value),
            SqlValue::Blob(value) => rusqlite::types::Value::Blob(value),
        }
    }
}
