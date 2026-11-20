//! One canonical driver path for point and batch statements on the native
//! worker.
use std::num::NonZeroUsize;

use storage_common::diagnostics::{NativeEngine, NativeOperation, trace_native_failure};
use storage_types::{StorageError, StorageResult};

use super::{error::map_driver_error, value::decode_row};
use crate::{SqlQueryPage, SqlStatement};

pub(super) struct NativeStatements<'native> {
    connection: &'native rusqlite::Connection,
    /// `false` in a ReadOnly attempt: a statement that SQLite does not classify
    /// as read-only is refused before it runs.
    writes: bool,
    // A query that SQLite does not classify as read-only, such as
    // `UPDATE ... RETURNING`, can change the database.
    wrote: std::cell::Cell<bool>,
    fault_probe: Option<&'native dyn crate::SqlFaultProbe>,
}

impl<'native> NativeStatements<'native> {
    pub(super) fn new(
        connection: &'native rusqlite::Connection,
        writes: bool,
        fault_probe: Option<&'native dyn crate::SqlFaultProbe>,
    ) -> Self {
        Self {
            connection,
            writes,
            wrote: std::cell::Cell::new(false),
            fault_probe,
        }
    }

    /// Whether a query prepared here was not read-only.
    pub(super) fn wrote(&self) -> bool {
        self.wrote.get()
    }

    pub(super) fn query(
        &self,
        statement: SqlStatement,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<SqlQueryPage> {
        self.require_inputs(std::iter::once(&statement))?;
        self.query_qualified(statement, max_bytes)
    }

    pub(super) fn query_batch(
        &self,
        statements: Vec<(SqlStatement, crate::ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<Vec<SqlQueryPage>> {
        self.require_inputs(statements.iter().map(|(statement, _)| statement))?;
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(statements.len())
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        for (statement, _) in statements {
            pages.push(self.query_qualified(statement, max_bytes)?);
        }
        Ok(pages)
    }

    pub(super) fn execute(&self, statement: SqlStatement) -> StorageResult<u64> {
        self.require_inputs(std::iter::once(&statement))?;
        self.execute_qualified(statement)
    }

    pub(super) fn execute_batch(&self, statements: Vec<SqlStatement>) -> StorageResult<Vec<u64>> {
        self.require_inputs(&statements)?;
        let mut counts = Vec::new();
        counts
            .try_reserve_exact(statements.len())
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        for statement in statements {
            counts.push(self.execute_qualified(statement)?);
        }
        Ok(counts)
    }

    fn require_inputs<'input>(
        &self,
        statements: impl IntoIterator<Item = &'input SqlStatement>,
    ) -> StorageResult<()> {
        let limits = super::record_limits::SqliteRecordLimits::load_connection(self.connection)
            .map_err(map_driver_error)?;
        for statement in statements {
            limits.require_statement(statement)?;
        }
        Ok(())
    }

    fn query_qualified(
        &self,
        statement: SqlStatement,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<SqlQueryPage> {
        let result = self.load_query(statement, max_bytes);
        result.inspect_err(|error| {
            trace_native_failure(NativeEngine::Sqlite, NativeOperation::SqlQuery, error)
        })
    }

    fn execute_qualified(&self, statement: SqlStatement) -> StorageResult<u64> {
        let result = (|| {
            self.check_fault(crate::SqlFaultOperation::Execute, &statement)?;
            let (sql, values) = statement.into_native_parts::<rusqlite::types::Value>()?;
            let mut prepared = self
                .connection
                .prepare_cached(&sql)
                .map_err(map_driver_error)?;
            let affected = prepared
                .execute(rusqlite::params_from_iter(values.iter()))
                .map_err(map_driver_error)?;
            let affected = u64::try_from(affected)
                .map_err(|_| StorageError::internal("SQLite affected count overflow"))?;
            crate::fault_injection::observe_statement(
                self.fault_probe,
                crate::SqlFaultOperation::Execute,
                affected,
            );
            Ok(affected)
        })();
        result.inspect_err(|error| {
            trace_native_failure(NativeEngine::Sqlite, NativeOperation::SqlExecute, error)
        })
    }

    fn load_query(
        &self,
        statement: SqlStatement,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<SqlQueryPage> {
        self.check_fault(crate::SqlFaultOperation::Query, &statement)?;
        let sql = self.fault_probe.map(|_| statement.sql().to_owned());
        let page = self.load_rows(statement, max_bytes)?;
        if let Some(probe) = self.fault_probe {
            probe.observe(crate::SqlNativeEvent::Statement {
                operation: crate::SqlFaultOperation::Query,
                rows: u64::try_from(page.rows.len()).unwrap_or(u64::MAX),
            });
            return Ok(probe.alter_query_page(
                crate::SqlFaultCall {
                    operation: crate::SqlFaultOperation::Query,
                    sql: sql.as_deref().unwrap_or_default(),
                },
                page,
            ));
        }
        Ok(page)
    }

    fn load_rows(
        &self,
        statement: SqlStatement,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<SqlQueryPage> {
        let (sql, values) = statement.into_native_parts::<rusqlite::types::Value>()?;
        let mut prepared = self
            .connection
            .prepare_cached(&sql)
            .map_err(map_driver_error)?;
        if !prepared.readonly() {
            if !self.writes {
                return Err(StorageError::internal(
                    "SQLite ReadOnly attempt refused a writing statement",
                ));
            }
            self.wrote.set(true);
        }
        let mut cursor = prepared
            .query(rusqlite::params_from_iter(values.iter()))
            .map_err(map_driver_error)?;
        let mut page = SqlQueryPage::new();
        while let Some(row) = cursor.next().map_err(map_driver_error)? {
            if !page.push_view(decode_row(row)?, max_bytes)? {
                break;
            }
        }
        Ok(page)
    }

    fn check_fault(
        &self,
        operation: crate::SqlFaultOperation,
        statement: &SqlStatement,
    ) -> StorageResult<()> {
        if let Some(error) = self.fault_probe.and_then(|probe| {
            probe.fail(crate::SqlFaultCall {
                operation,
                sql: statement.sql(),
            })
        }) {
            return Err(error);
        }
        Ok(())
    }
}
