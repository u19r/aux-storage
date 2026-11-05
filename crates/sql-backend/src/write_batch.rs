//! Reusable multi-row upsert lowering shared by primary and GSI writes.
//!
//! One logical attempt still owns one native transaction. Grouping equal-shape
//! rows into one statement only removes redundant native round trips; every
//! row keeps its own conflict decision and count fence.

use storage_types::{StorageError, StorageResult};

use crate::{
    SqlLayout, SqlStatement, SqlValue, constants::MULTI_ROW_WRITE_BINDS, statement::SqlText,
};

/// The invariant column list and conflict target for one physical relation.
pub(crate) struct UpsertShape {
    columns: Vec<String>,
    numeric_columns: Vec<bool>,
    conflict_columns: Vec<String>,
    revision_guard: bool,
}

impl UpsertShape {
    pub(crate) fn try_new(
        columns: Vec<String>,
        numeric_columns: Vec<bool>,
        conflict_columns: Vec<String>,
        revision_guard: bool,
    ) -> StorageResult<Self> {
        if columns.is_empty()
            || columns.len() != numeric_columns.len()
            || conflict_columns.is_empty()
            || conflict_columns.len() >= columns.len()
        {
            return Err(StorageError::internal(
                "SQL upsert shape has inconsistent columns",
            ));
        }
        Ok(Self {
            columns,
            numeric_columns,
            conflict_columns,
            revision_guard,
        })
    }

    fn same_as(&self, other: &Self) -> bool {
        self.revision_guard == other.revision_guard
            && self.columns == other.columns
            && self.numeric_columns == other.numeric_columns
            && self.conflict_columns == other.conflict_columns
    }

    fn width(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn render(
        &self,
        engine: impl SqlLayout,
        relation: &str,
        rows: Vec<Vec<SqlValue>>,
    ) -> StorageResult<SqlStatement> {
        let width = self.width();
        let capacity = rows
            .len()
            .checked_mul(width)
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let mut binds = Vec::new();
        binds
            .try_reserve_exact(capacity)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let mut sql = SqlText::default();
        sql.append(format_args!("INSERT INTO \"{relation}\" ("))?;
        sql.append_columns(self.columns.iter())?;
        sql.append(format_args!(") VALUES "))?;
        let mut position = 0usize;
        for (row_index, row) in rows.into_iter().enumerate() {
            if row.len() != width {
                return Err(StorageError::internal(
                    "SQL upsert row width differs from its shape",
                ));
            }
            sql.append(format_args!("{}(", if row_index == 0 { "" } else { ", " }))?;
            for (column, value) in row.into_iter().enumerate() {
                if column > 0 {
                    sql.append(format_args!(", "))?;
                }
                position += 1;
                sql.append_parameter(engine.engine(), position, self.numeric_columns[column])?;
                binds.push(value);
            }
            sql.append(format_args!(")"))?;
        }
        sql.append(format_args!(" ON CONFLICT ("))?;
        sql.append_columns(self.conflict_columns.iter())?;
        sql.append(format_args!(") DO UPDATE SET "))?;
        sql.append_assignments(self.columns.iter().skip(self.conflict_columns.len()))?;
        if self.revision_guard {
            sql.append(format_args!(
                " WHERE excluded.__aux_item_version > \"{relation}\".__aux_item_version"
            ))?;
        }
        Ok(SqlStatement::new(sql.finish()?, binds))
    }
}

/// One already-patched row for a physical relation.
pub(crate) struct PreparedUpsertRow {
    pub(crate) relation: String,
    pub(crate) shape: UpsertShape,
    pub(crate) binds: Vec<SqlValue>,
}

#[derive(Default)]
pub(crate) struct UpsertGroups {
    groups: Vec<UpsertGroup>,
}

struct UpsertGroup {
    relation: String,
    shape: UpsertShape,
    rows: Vec<Vec<SqlValue>>,
}

impl UpsertGroups {
    pub(crate) fn stage(&mut self, row: PreparedUpsertRow) -> StorageResult<()> {
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|group| group.relation == row.relation)
        {
            if !group.shape.same_as(&row.shape) {
                return Err(StorageError::internal(
                    "SQL upsert rows for one relation have different shapes",
                ));
            }
            group
                .rows
                .try_reserve(1)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            group.rows.push(row.binds);
            return Ok(());
        }
        self.groups
            .try_reserve(1)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        self.groups.push(UpsertGroup {
            relation: row.relation,
            shape: row.shape,
            rows: vec![row.binds],
        });
        Ok(())
    }

    pub(crate) fn into_statements(
        self,
        engine: impl SqlLayout,
    ) -> StorageResult<Vec<(SqlStatement, usize)>> {
        let mut statements = Vec::new();
        statements
            .try_reserve_exact(self.groups.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for group in self.groups {
            let width = group.shape.width();
            let rows_per_statement = (MULTI_ROW_WRITE_BINDS / width.max(1)).max(1);
            let mut rows = group.rows;
            while !rows.is_empty() {
                let batch_len = rows.len().min(rows_per_statement);
                let remaining = rows.split_off(batch_len);
                let batch = std::mem::replace(&mut rows, remaining);
                statements.push((
                    group.shape.render(engine, &group.relation, batch)?,
                    batch_len,
                ));
            }
        }
        Ok(statements)
    }
}

/// The invariant key columns for one physical relation delete.
pub(crate) struct DeleteShape {
    columns: Vec<String>,
    numeric_columns: Vec<bool>,
}

impl DeleteShape {
    pub(crate) fn try_new(columns: Vec<String>, numeric_columns: Vec<bool>) -> StorageResult<Self> {
        if columns.is_empty() || columns.len() != numeric_columns.len() {
            return Err(StorageError::internal(
                "SQL delete shape has inconsistent columns",
            ));
        }
        Ok(Self {
            columns,
            numeric_columns,
        })
    }

    fn same_as(&self, other: &Self) -> bool {
        self.columns == other.columns && self.numeric_columns == other.numeric_columns
    }

    fn width(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn render(
        &self,
        engine: impl SqlLayout,
        relation: &str,
        rows: Vec<Vec<SqlValue>>,
    ) -> StorageResult<SqlStatement> {
        let width = self.width();
        let capacity = rows
            .len()
            .checked_mul(width)
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let mut binds = Vec::new();
        binds
            .try_reserve_exact(capacity)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let mut sql = SqlText::default();
        sql.append(format_args!("DELETE FROM \"{relation}\" WHERE ("))?;
        sql.append_columns(&self.columns)?;
        sql.append(format_args!(") IN (VALUES "))?;
        let mut position = 0usize;
        for (row_index, row) in rows.into_iter().enumerate() {
            if row.len() != width {
                return Err(StorageError::internal(
                    "SQL delete row width differs from its shape",
                ));
            }
            sql.append(format_args!("{}(", if row_index == 0 { "" } else { ", " }))?;
            for (column, value) in row.into_iter().enumerate() {
                if column > 0 {
                    sql.append(format_args!(", "))?;
                }
                position += 1;
                let is_binary = engine.engine() == crate::SqlEngine::Postgres
                    && matches!(value, SqlValue::Blob(_));
                if is_binary {
                    sql.append(format_args!("CAST("))?;
                }
                sql.append_parameter(engine.engine(), position, self.numeric_columns[column])?;
                if is_binary {
                    sql.append(format_args!(" AS BYTEA)"))?;
                }
                binds.push(value);
            }
            sql.append(format_args!(")"))?;
        }
        sql.append(format_args!(")"))?;
        Ok(SqlStatement::new(sql.finish()?, binds))
    }
}

/// One already-resolved key row for a physical relation delete.
pub(crate) struct PreparedDeleteRow {
    pub(crate) relation: String,
    pub(crate) shape: DeleteShape,
    pub(crate) binds: Vec<SqlValue>,
}

#[derive(Default)]
pub(crate) struct DeleteGroups {
    groups: Vec<DeleteGroup>,
}

struct DeleteGroup {
    relation: String,
    shape: DeleteShape,
    rows: Vec<Vec<SqlValue>>,
}

impl DeleteGroups {
    pub(crate) fn stage(&mut self, row: PreparedDeleteRow) -> StorageResult<()> {
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|group| group.relation == row.relation)
        {
            if !group.shape.same_as(&row.shape) {
                return Err(StorageError::internal(
                    "SQL delete rows for one relation have different shapes",
                ));
            }
            group
                .rows
                .try_reserve(1)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            group.rows.push(row.binds);
            return Ok(());
        }
        self.groups
            .try_reserve(1)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        self.groups.push(DeleteGroup {
            relation: row.relation,
            shape: row.shape,
            rows: vec![row.binds],
        });
        Ok(())
    }

    pub(crate) fn into_statements(
        self,
        engine: impl SqlLayout,
    ) -> StorageResult<Vec<(SqlStatement, usize)>> {
        let mut statements = Vec::new();
        statements
            .try_reserve_exact(self.groups.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for group in self.groups {
            let width = group.shape.width();
            let rows_per_statement = (MULTI_ROW_WRITE_BINDS / width.max(1)).max(1);
            let mut rows = group.rows;
            while !rows.is_empty() {
                let batch_len = rows.len().min(rows_per_statement);
                let remaining = rows.split_off(batch_len);
                let batch = std::mem::replace(&mut rows, remaining);
                statements.push((
                    group.shape.render(engine, &group.relation, batch)?,
                    batch_len,
                ));
            }
        }
        Ok(statements)
    }
}
