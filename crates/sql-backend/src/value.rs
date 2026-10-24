use std::num::NonZeroUsize;

use storage_types::{InfrastructureResultKind, StorageError, StorageResult};

/// Driver-neutral values required by the persisted SQL layout.
/// Deliberately lacks `Debug` so item data is not accidentally logged.
pub enum SqlValue {
    Null,
    Integer(i64),
    Boolean(bool),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

// postgres-types requires Debug for bind values. Never reveal customer data.
impl std::fmt::Debug for SqlValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SqlValue(<redacted>)")
    }
}

impl SqlValue {
    pub(crate) fn byte_len(&self) -> usize {
        crate::row_view::SqlValueRef::from(self).byte_len()
    }
}

/// A positional column selected by a concern-owned statement.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ColumnIndex(usize);

impl ColumnIndex {
    #[must_use]
    pub const fn new(index: usize) -> Self {
        Self(index)
    }
}

/// Native rows are normalized once, then decoded by the SQL family.
/// No provider needs a table or item-specific row decoder.
pub struct SqlRow {
    values: Vec<SqlValue>,
}

impl SqlRow {
    #[must_use]
    pub fn new(values: Vec<SqlValue>) -> Self {
        Self { values }
    }

    pub fn value(&self, column: ColumnIndex) -> StorageResult<&SqlValue> {
        self.values
            .get(column.0)
            .ok_or_else(|| StorageError::internal("SQL row is missing a required column"))
    }

    #[must_use]
    pub fn into_values(self) -> Vec<SqlValue> {
        self.values
    }

    fn byte_len(&self) -> StorageResult<usize> {
        self.values.iter().try_fold(0usize, |total, value| {
            total
                .checked_add(value.byte_len())
                .ok_or_else(|| InfrastructureResultKind::NativeCapacityRejected.into())
        })
    }
}

/// A bounded physical SQL result. `more` means the executed statement had
/// another row after the byte boundary; the SQL lowerer owns continuation
/// predicates and any Dynamo item-size accounting.
#[derive(Default)]
pub struct SqlQueryPage {
    pub rows: Vec<SqlRow>,
    pub more: bool,
    used_bytes: usize,
}

impl SqlQueryPage {
    #[must_use]
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            more: false,
            used_bytes: 0,
        }
    }

    pub(crate) fn charge_work(
        &self,
        settlement: &storage_types::NativeAttemptSettlement,
    ) -> StorageResult<()> {
        let bytes = self.rows.iter().try_fold(self.used_bytes, |bytes, row| {
            row.values
                .len()
                .checked_mul(std::mem::size_of::<SqlValue>())
                .and_then(|cells| cells.checked_add(std::mem::size_of::<SqlRow>()))
                .and_then(|cells| bytes.checked_add(cells))
                .ok_or(InfrastructureResultKind::NativeCapacityRejected)
        })?;
        settlement.charge_data(bytes)
    }

    /// The first row may exceed the budget, guaranteeing forward progress.
    /// A rejected row proves there is a continuation.
    pub fn push(&mut self, row: SqlRow, max_bytes: NonZeroUsize) -> StorageResult<bool> {
        let Some(total) = self.admit_row_bytes(row.byte_len()?, max_bytes)? else {
            return Ok(false);
        };
        self.rows
            .try_reserve(1)
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        self.rows.push(row);
        self.used_bytes = total;
        Ok(true)
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn push_view(
        &mut self,
        row: Vec<crate::row_view::SqlValueRef<'_>>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<bool> {
        let bytes = row.iter().try_fold(0usize, |total, value| {
            total
                .checked_add(value.byte_len())
                .ok_or(InfrastructureResultKind::NativeCapacityRejected)
        })?;
        let Some(total) = self.admit_row_bytes(bytes, max_bytes)? else {
            return Ok(false);
        };
        self.rows
            .try_reserve(1)
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        let row = SqlRow::try_from(row)?;
        self.rows.push(row);
        self.used_bytes = total;
        Ok(true)
    }

    fn admit_row_bytes(
        &mut self,
        bytes: usize,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<Option<usize>> {
        if self.more {
            return Ok(None);
        }
        let total = self
            .used_bytes
            .checked_add(bytes.max(1))
            .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
        if !self.rows.is_empty() && total > max_bytes.get() {
            self.more = true;
            return Ok(None);
        }
        Ok(Some(total))
    }
}
