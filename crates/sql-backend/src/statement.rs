use std::borrow::Cow;

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
use storage_types::{InfrastructureContext, StorageError};
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
use storage_types::{InfrastructureResultKind, StorageResult};

use crate::SqlValue;

/// One already-rendered SQL statement and its typed binds.
///
/// Construction belongs to the SQL family lowerer. The native driver must
/// pass `binds` as parameters, never concatenate their contents into `sql`.
pub struct SqlStatement {
    sql: Cow<'static, str>,
    binds: Vec<SqlValue>,
}

impl SqlStatement {
    #[must_use]
    pub fn new(sql: impl Into<Cow<'static, str>>, binds: Vec<SqlValue>) -> Self {
        Self {
            sql: sql.into(),
            binds,
        }
    }

    pub(crate) fn charge_work(
        &self,
        settlement: &storage_types::NativeAttemptSettlement,
    ) -> storage_types::StorageResult<()> {
        let bytes = self.binds.iter().try_fold(self.sql.len(), |bytes, value| {
            bytes
                .checked_add(value.byte_len())
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<SqlValue>()))
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)
        })?;
        settlement.charge_data(bytes)
    }

    #[must_use]
    pub fn into_parts(self) -> (Cow<'static, str>, Vec<SqlValue>) {
        (self.sql, self.binds)
    }

    #[cfg(any(feature = "sqlite", feature = "turso"))]
    pub(crate) fn into_native_parts<V: From<SqlValue>>(
        self,
    ) -> StorageResult<(Cow<'static, str>, Vec<V>)> {
        let mut values = Vec::new();
        values
            .try_reserve_exact(self.binds.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        for value in self.binds {
            values.push(V::from(value));
        }
        Ok((self.sql, values))
    }

    #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
    pub(crate) fn input_shape(&self) -> StorageResult<StatementInputShape> {
        if self.sql.contains('\0') {
            return Err(invalid_input());
        }
        let mut shape = StatementInputShape {
            sql_bytes: self.sql.len(),
            parameters: self.binds.len(),
            value_bytes: 0,
            text_values: 0,
            maximum_value_bytes: 0,
        };
        for value in &self.binds {
            let bytes = value.byte_len();
            shape.value_bytes = shape
                .value_bytes
                .checked_add(bytes)
                .ok_or_else(invalid_input)?;
            shape.maximum_value_bytes = shape.maximum_value_bytes.max(bytes);
            if matches!(value, SqlValue::Text(_)) {
                shape.text_values += 1;
            }
        }
        Ok(shape)
    }

    #[cfg(feature = "fault-injection")]
    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) struct StatementInputShape {
    pub(crate) sql_bytes: usize,
    pub(crate) parameters: usize,
    pub(crate) value_bytes: usize,
    pub(crate) text_values: usize,
    maximum_value_bytes: usize,
}

#[cfg(feature = "sqlite")]
impl StatementInputShape {
    pub(crate) fn require_limits(
        &self,
        sql_bytes: usize,
        parameters: usize,
        value_bytes: usize,
    ) -> StorageResult<()> {
        if self.sql_bytes > sql_bytes
            || self.parameters > parameters
            || self.maximum_value_bytes > value_bytes
        {
            return Err(InfrastructureResultKind::NativeCapacityRejected.into());
        }
        Ok(())
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
fn invalid_input() -> StorageError {
    InfrastructureResultKind::InvalidBound {
        context: InfrastructureContext::NativeCapacity,
    }
    .into()
}

/// Fallible physical SQL rendering. Bind values never enter this syntax buffer.
#[derive(Default)]
pub(crate) struct SqlText {
    text: String,
    rejected: bool,
}

impl SqlText {
    pub(crate) fn append(
        &mut self,
        arguments: std::fmt::Arguments<'_>,
    ) -> storage_types::StorageResult<()> {
        let result = std::fmt::write(self, arguments);
        if result.is_err() || self.rejected {
            self.rejected = true;
            return Err(storage_types::InfrastructureResultKind::NativeCapacityRejected.into());
        }
        Ok(())
    }

    pub(crate) fn append_columns<I>(&mut self, names: I) -> storage_types::StorageResult<()>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        for (ordinal, name) in names.into_iter().enumerate() {
            self.append(format_args!(
                "{}\"{}\"",
                if ordinal == 0 { "" } else { ", " },
                name.as_ref()
            ))?;
        }
        Ok(())
    }

    pub(crate) fn append_assignments<I>(&mut self, names: I) -> storage_types::StorageResult<()>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        for (ordinal, name) in names.into_iter().enumerate() {
            let name = name.as_ref();
            self.append(format_args!(
                "{}\"{name}\" = excluded.\"{name}\"",
                if ordinal == 0 { "" } else { ", " }
            ))?;
        }
        Ok(())
    }

    pub(crate) fn append_parameter(
        &mut self,
        engine: crate::SqlEngine,
        position: usize,
        numeric: bool,
    ) -> storage_types::StorageResult<()> {
        self.append(format_args!(
            "{}{}{position}{}",
            if numeric { "CAST(" } else { "" },
            engine.placeholder_prefix(),
            if numeric { " AS TEXT)::NUMERIC" } else { "" }
        ))
    }

    pub(crate) fn finish(self) -> storage_types::StorageResult<String> {
        if self.rejected {
            return Err(storage_types::InfrastructureResultKind::NativeCapacityRejected.into());
        }
        Ok(self.text)
    }

    pub(crate) fn render(
        arguments: std::fmt::Arguments<'_>,
    ) -> storage_types::StorageResult<String> {
        let mut text = Self::default();
        text.append(arguments)?;
        text.finish()
    }
}

impl std::fmt::Write for SqlText {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        if self.rejected || self.text.try_reserve(text.len()).is_err() {
            self.rejected = true;
            return Err(std::fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}
