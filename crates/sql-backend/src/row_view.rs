//! Native row borrows end before the cursor advances; only admitted payloads
//! become owned.

use crate::SqlValue;

pub(crate) enum SqlValueRef<'native> {
    Null,
    Integer(i64),
    Boolean(bool),
    Real(f64),
    Text(&'native str),
    Blob(&'native [u8]),
}

impl SqlValueRef<'_> {
    pub(crate) fn byte_len(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Integer(value) => std::mem::size_of_val(value),
            Self::Real(value) => std::mem::size_of_val(value),
            Self::Boolean(value) => std::mem::size_of_val(value),
            Self::Text(value) => value.len(),
            Self::Blob(value) => value.len(),
        }
    }
}

impl<'native> From<&'native SqlValue> for SqlValueRef<'native> {
    fn from(value: &'native SqlValue) -> Self {
        match value {
            SqlValue::Null => Self::Null,
            SqlValue::Integer(value) => Self::Integer(*value),
            SqlValue::Boolean(value) => Self::Boolean(*value),
            SqlValue::Real(value) => Self::Real(*value),
            SqlValue::Text(value) => Self::Text(value),
            SqlValue::Blob(value) => Self::Blob(value),
        }
    }
}

impl TryFrom<SqlValueRef<'_>> for SqlValue {
    type Error = storage_types::StorageError;

    fn try_from(value: SqlValueRef<'_>) -> Result<Self, Self::Error> {
        use storage_types::InfrastructureResultKind::NativeCapacityRejected;
        Ok(match value {
            SqlValueRef::Null => Self::Null,
            SqlValueRef::Integer(value) => Self::Integer(value),
            SqlValueRef::Boolean(value) => Self::Boolean(value),
            SqlValueRef::Real(value) => Self::Real(value),
            SqlValueRef::Text(value) => {
                let mut owned = String::new();
                owned
                    .try_reserve_exact(value.len())
                    .map_err(|_| NativeCapacityRejected)?;
                owned.push_str(value);
                Self::Text(owned)
            }
            SqlValueRef::Blob(value) => {
                let mut owned = Vec::new();
                owned
                    .try_reserve_exact(value.len())
                    .map_err(|_| NativeCapacityRejected)?;
                owned.extend_from_slice(value);
                Self::Blob(owned)
            }
        })
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl TryFrom<Vec<SqlValueRef<'_>>> for crate::SqlRow {
    type Error = storage_types::StorageError;

    fn try_from(row: Vec<SqlValueRef<'_>>) -> Result<Self, Self::Error> {
        let mut cells = Vec::new();
        cells
            .try_reserve_exact(row.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for value in row {
            cells.push(SqlValue::try_from(value)?);
        }
        Ok(Self::new(cells))
    }
}
