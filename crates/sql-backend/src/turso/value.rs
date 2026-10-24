use storage_types::StorageResult;

use super::error::map_driver_error;
use crate::{SqlRow, SqlValue};

pub(super) fn decode_row(row: &turso::Row, width: usize) -> StorageResult<SqlRow> {
    let mut cells = Vec::new();
    cells
        .try_reserve_exact(width)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for index in 0..width {
        cells.push(from_native_value(
            row.get_value(index).map_err(map_driver_error)?,
        ));
    }
    Ok(SqlRow::new(cells))
}

impl From<SqlValue> for turso::Value {
    fn from(value: SqlValue) -> Self {
        match value {
            SqlValue::Null => turso::Value::Null,
            SqlValue::Integer(value) => turso::Value::Integer(value),
            SqlValue::Boolean(value) => turso::Value::Integer(i64::from(value)),
            SqlValue::Real(value) => turso::Value::Real(value),
            SqlValue::Text(value) => turso::Value::Text(value),
            SqlValue::Blob(value) => turso::Value::Blob(value),
        }
    }
}

pub(super) fn from_native_value(value: turso::Value) -> SqlValue {
    match value {
        turso::Value::Null => SqlValue::Null,
        turso::Value::Integer(value) => SqlValue::Integer(value),
        turso::Value::Real(value) => SqlValue::Real(value),
        turso::Value::Text(value) => SqlValue::Text(value),
        turso::Value::Blob(value) => SqlValue::Blob(value),
    }
}
