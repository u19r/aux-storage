//! Native statements share the caller's settlement, prepared binds and drained
//! streams.
use std::{borrow::Cow, num::NonZeroUsize};

use futures::{TryStreamExt as _, future::join_all, pin_mut};
use storage_types::{NativeAttemptSettlement, StorageResult};
use tokio_postgres::types::ToSql;

use super::{error::PostgresCallError, pool::Client, value::decode_row};
use crate::{ReadProtection, SqlQueryPage, SqlStatement, SqlValue};

pub(super) struct NativeStatements<'native> {
    client: &'native Client,
    settlement: &'native NativeAttemptSettlement,
    fault_probe: Option<&'native dyn crate::SqlFaultProbe>,
}

pub(super) struct PreparedQuery {
    statement: tokio_postgres::Statement,
    binds: Vec<SqlValue>,
    /// The SQL a probe sees, without the lock suffix; `None` without a probe.
    sql: Option<Cow<'static, str>>,
}

pub(super) struct PreparedCountedWrite {
    statement: tokio_postgres::Statement,
    binds: Vec<super::value::CheckedBind>,
    /// The SQL a probe sees, without the lock suffix; `None` without a probe.
    sql: Option<Cow<'static, str>>,
}

impl PreparedCountedWrite {
    pub(super) fn sql(&self) -> &str {
        self.sql.as_deref().unwrap_or_default()
    }
}

impl<'native> NativeStatements<'native> {
    pub(super) fn new(
        client: &'native Client,
        settlement: &'native NativeAttemptSettlement,
        fault_probe: Option<&'native dyn crate::SqlFaultProbe>,
    ) -> Self {
        Self {
            client,
            settlement,
            fault_probe,
        }
    }

    /// Every Parse/Describe completes before any execution in this window.
    pub(super) async fn prepare_window(
        &self,
        statements: Vec<(SqlStatement, ReadProtection)>,
    ) -> Vec<Result<PreparedQuery, PostgresCallError>> {
        join_all(
            statements
                .into_iter()
                .map(|(statement, protection)| self.prepare_query(statement, protection)),
        )
        .await
    }

    /// Parse/Describe of a write statement. The fault check runs first, as
    /// it did before preparation in the former `execute`.
    pub(super) async fn prepare_execute(
        &self,
        statement: SqlStatement,
    ) -> Result<PreparedQuery, PostgresCallError> {
        self.check_fault(crate::SqlFaultOperation::Execute, Some(statement.sql()))?;
        self.prepare_query(statement, ReadProtection::Snapshot)
            .await
    }

    /// Bind/Execute of a prepared write; written to the connection on the
    /// first poll, so `join!` order is wire order.
    pub(super) async fn execute_prepared(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<u64, PostgresCallError> {
        let params =
            bind_refs(&prepared.binds).inspect_err(|error| self.settlement.reject_error(error))?;
        let changed = self
            .client
            .execute(&prepared.statement, &params)
            .await
            .map_err(PostgresCallError::from)
            .inspect_err(|error| error.reject_settlement(self.settlement))?;
        crate::fault_injection::observe_statement(
            self.fault_probe,
            crate::SqlFaultOperation::Execute,
            changed,
        );
        Ok(changed)
    }

    pub(super) async fn prepare_query(
        &self,
        statement: SqlStatement,
        protection: ReadProtection,
    ) -> Result<PreparedQuery, PostgresCallError> {
        let (sql, binds) = statement.into_parts();
        let original = self.fault_probe.is_some().then(|| sql.clone());
        let sql = append_query_protection(sql, protection)
            .inspect_err(|error| self.settlement.reject_error(error))?;
        let statement = self
            .client
            .prepare_cached(sql.as_ref())
            .await
            .map_err(PostgresCallError::from)
            .inspect_err(|error| error.reject_settlement(self.settlement))?;
        Ok(PreparedQuery {
            statement,
            binds,
            sql: original,
        })
    }

    pub(super) async fn query_prepared(
        &self,
        prepared: PreparedQuery,
        max_bytes: NonZeroUsize,
    ) -> Result<SqlQueryPage, PostgresCallError> {
        self.check_fault(crate::SqlFaultOperation::Query, prepared.sql.as_deref())?;
        let params =
            bind_refs(&prepared.binds).inspect_err(|error| self.settlement.reject_error(error))?;
        let stream = self
            .client
            .query_raw(&prepared.statement, params)
            .await
            .map_err(PostgresCallError::from)
            .inspect_err(|error| error.reject_settlement(self.settlement))?;
        pin_mut!(stream);
        let mut page = SqlQueryPage::new();
        let mut rejected = None;
        while let Some(row) = stream
            .try_next()
            .await
            .map_err(PostgresCallError::from)
            .inspect_err(|error| error.reject_settlement(self.settlement))?
        {
            if !page.more && rejected.is_none() {
                rejected = decode_row(&row)
                    .and_then(|row| page.push_view(row, max_bytes))
                    .err();
                if let Some(error) = &rejected {
                    self.settlement.reject_error(error);
                }
            }
        }
        if let Some(error) = rejected {
            return Err(error.into());
        }
        if let Some(probe) = self.fault_probe {
            probe.observe(crate::SqlNativeEvent::Statement {
                operation: crate::SqlFaultOperation::Query,
                rows: u64::try_from(page.rows.len()).unwrap_or(u64::MAX),
            });
            return Ok(probe.alter_query_page(
                crate::SqlFaultCall {
                    operation: crate::SqlFaultOperation::Query,
                    sql: prepared.sql.as_deref().unwrap_or_default(),
                },
                page,
            ));
        }
        Ok(page)
    }

    pub(super) fn check_counted(
        &self,
        prepared: PreparedQuery,
    ) -> StorageResult<PreparedCountedWrite> {
        self.check_fault(crate::SqlFaultOperation::Query, prepared.sql.as_deref())?;
        if prepared.statement.params().len() != prepared.binds.len() {
            return Err(storage_types::StorageError::internal(
                "Postgres counted write parameter count differs",
            ));
        }
        if prepared.statement.columns().len() != 2
            || prepared
                .statement
                .columns()
                .iter()
                .any(|column| column.type_() != &tokio_postgres::types::Type::INT8)
        {
            return Err(storage_types::StorageError::internal(
                "Postgres counted write returned invalid count types",
            ));
        }
        let mut binds = Vec::new();
        binds
            .try_reserve_exact(prepared.binds.len())
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        for (value, ty) in prepared.binds.into_iter().zip(prepared.statement.params()) {
            binds.push(super::value::CheckedBind::try_new(value, ty)?);
        }
        Ok(PreparedCountedWrite {
            statement: prepared.statement,
            binds,
            sql: prepared.sql,
        })
    }

    /// Counted DML has exactly one fixed two-integer aggregate result. Borrow
    /// binds directly: no bind-reference vector or generic page decoding is
    /// allocated after terminal mutation dispatch.
    pub(super) async fn query_counted(
        &self,
        prepared: &PreparedCountedWrite,
        assertion_error: fn() -> storage_types::StorageError,
    ) -> Result<[i64; 2], PostgresCallError> {
        let select_error = |error: tokio_postgres::Error| {
            let selected =
                if error.code() == Some(&tokio_postgres::error::SqlState::DIVISION_BY_ZERO) {
                    PostgresCallError::CountAssertion(assertion_error())
                } else {
                    PostgresCallError::Native(error)
                };
            selected.reject_settlement(self.settlement);
            selected
        };
        let stream = self
            .client
            .query_raw(
                &prepared.statement,
                prepared
                    .binds
                    .iter()
                    .map(|value| value as &(dyn ToSql + Sync)),
            )
            .await
            .map_err(select_error)?;
        pin_mut!(stream);
        let mut count = None;
        let mut invalid = false;
        while let Some(row) = stream.try_next().await.map_err(select_error)? {
            match (
                row.len(),
                row.try_get::<_, i64>(0),
                row.try_get::<_, i64>(1),
            ) {
                (2, Ok(changed), Ok(1)) if changed >= 0 && count.is_none() => {
                    count = Some([changed, 1])
                }
                _ => invalid = true,
            }
        }
        let count = count.filter(|_| !invalid).ok_or_else(|| {
            let error = storage_types::StorageError::internal(
                "Postgres counted write returned invalid count columns",
            );
            self.settlement.reject_error(&error);
            PostgresCallError::from(error)
        })?;
        #[cfg(feature = "fault-injection")]
        if let (Some(probe), Some(sql)) = (self.fault_probe, prepared.sql.as_deref())
            && let Some(error) = probe.fail_counted_result(sql)
        {
            self.settlement.reject_error(&error);
            return Err(error.into());
        }
        if let Some(probe) = self.fault_probe {
            probe.observe(crate::SqlNativeEvent::Statement {
                operation: crate::SqlFaultOperation::Query,
                rows: 1,
            });
            probe.observe(crate::SqlNativeEvent::RowsWritten {
                rows: count[0].unsigned_abs(),
            });
        }
        Ok(count)
    }

    fn check_fault(
        &self,
        operation: crate::SqlFaultOperation,
        sql: Option<&str>,
    ) -> StorageResult<()> {
        if let Some(probe) = self.fault_probe
            && let Some(error) = probe.fail(crate::SqlFaultCall {
                operation,
                sql: sql.unwrap_or_default(),
            })
        {
            self.settlement.reject_error(&error);
            return Err(error);
        }
        Ok(())
    }
}

fn bind_refs(binds: &[SqlValue]) -> StorageResult<Vec<&(dyn ToSql + Sync)>> {
    let mut params = Vec::new();
    params
        .try_reserve_exact(binds.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for value in binds {
        params.push(value as &(dyn ToSql + Sync));
    }
    Ok(params)
}

pub(super) fn query_protection_suffix(protection: ReadProtection) -> &'static str {
    match protection {
        ReadProtection::Snapshot => "",
        ReadProtection::Shared => " FOR SHARE",
        ReadProtection::Conflict => " FOR UPDATE",
        ReadProtection::Claim => " FOR UPDATE SKIP LOCKED",
    }
}

pub(super) fn append_query_protection(
    sql: Cow<'static, str>,
    protection: ReadProtection,
) -> StorageResult<Cow<'static, str>> {
    let suffix = query_protection_suffix(protection);
    if suffix.is_empty() {
        return Ok(sql);
    }
    let mut sql = match sql {
        Cow::Owned(sql) => sql,
        Cow::Borrowed(sql) => {
            let mut owned = String::new();
            let bytes = sql
                .len()
                .checked_add(suffix.len())
                .ok_or(storage_types::AttemptFailure::CAPACITY)?;
            owned
                .try_reserve_exact(bytes)
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            owned.push_str(sql);
            owned
        }
    };
    sql.try_reserve_exact(suffix.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    sql.push_str(suffix);
    Ok(Cow::Owned(sql))
}
