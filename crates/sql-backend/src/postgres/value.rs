use bytes::BytesMut;
use storage_types::{StorageError, StorageResult};
use tokio_postgres::{
    Row,
    types::{FromSql, IsNull, ToSql, Type},
};

use crate::{SqlValue, row_view::SqlValueRef};

pub(super) fn decode_row(row: &Row) -> StorageResult<Vec<SqlValueRef<'_>>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(row.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for (index, column) in row.columns().iter().enumerate() {
        let value = if <&str as FromSql>::accepts(column.type_()) {
            row.try_get::<_, Option<&str>>(index)
                .map(|value| value.map(SqlValueRef::Text))
        } else if <bool as FromSql>::accepts(column.type_()) {
            row.try_get::<_, Option<bool>>(index)
                .map(|value| value.map(|value| SqlValueRef::Integer(i64::from(value))))
        } else if <i64 as FromSql>::accepts(column.type_()) {
            row.try_get::<_, Option<i64>>(index)
                .map(|value| value.map(SqlValueRef::Integer))
        } else if <i32 as FromSql>::accepts(column.type_()) {
            row.try_get::<_, Option<i32>>(index)
                .map(|value| value.map(|value| SqlValueRef::Integer(i64::from(value))))
        } else if <f64 as FromSql>::accepts(column.type_()) {
            row.try_get::<_, Option<f64>>(index)
                .map(|value| value.map(SqlValueRef::Real))
        } else if <f32 as FromSql>::accepts(column.type_()) {
            row.try_get::<_, Option<f32>>(index)
                .map(|value| value.map(|value| SqlValueRef::Real(f64::from(value))))
        } else {
            row.try_get::<_, Option<&[u8]>>(index)
                .map(|value| value.map(SqlValueRef::Blob))
        }
        .map_err(|_| StorageError::internal("Postgres row type is unsupported"))?;
        values.push(value.unwrap_or(SqlValueRef::Null));
    }
    Ok(values)
}

impl ToSql for SqlValue {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Null => Ok(IsNull::Yes),
            Self::Integer(value) => value.to_sql(ty, out),
            Self::Boolean(value) => value.to_sql(ty, out),
            Self::Real(value) => value.to_sql(ty, out),
            Self::Text(value) => value.to_sql(ty, out),
            Self::Blob(value) => value.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Null => Ok(IsNull::Yes),
            Self::Integer(value) => value.to_sql_checked(ty, out),
            Self::Boolean(value) => value.to_sql_checked(ty, out),
            Self::Real(value) => value.to_sql_checked(ty, out),
            Self::Text(value) => value.to_sql_checked(ty, out),
            Self::Blob(value) => value.to_sql_checked(ty, out),
        }
    }
}
