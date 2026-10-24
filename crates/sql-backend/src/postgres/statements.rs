//! Native statements share the caller's settlement, prepared binds and drained
//! streams.
use std::{borrow::Cow, num::NonZeroUsize};

use deadpool_postgres::Client;
use futures::{TryStreamExt as _, future::join_all, pin_mut};
use storage_common::diagnostics::NativeOperation;
use storage_types::{NativeAttemptSettlement, StorageResult};
use tokio_postgres::types::ToSql;

use super::{
    error::{PostgresCallError, collect_pipeline},
    value::decode_row,
};
use crate::{ReadProtection, SqlQueryPage, SqlStatement, SqlValue};

pub(super) struct NativeStatements<'native> {
    client: &'native Client,
    settlement: &'native NativeAttemptSettlement,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<&'native dyn crate::SqlFaultProbe>,
}

struct PreparedQuery {
    statement: tokio_postgres::Statement,
    binds: Vec<SqlValue>,
    #[cfg(feature = "fault-injection")]
    sql: Cow<'static, str>,
}

impl<'native> NativeStatements<'native> {
    pub(super) fn new(
        client: &'native Client,
        settlement: &'native NativeAttemptSettlement,
        #[cfg(feature = "fault-injection")] fault_probe: Option<&'native dyn crate::SqlFaultProbe>,
    ) -> Self {
        Self {
            client,
            settlement,
            #[cfg(feature = "fault-injection")]
            fault_probe,
        }
    }

    pub(super) async fn query(
        &self,
        statement: SqlStatement,
        protection: ReadProtection,
        max_bytes: NonZeroUsize,
    ) -> Result<SqlQueryPage, PostgresCallError> {
        let prepared = self.prepare_query(statement, protection).await?;
        self.query_prepared(prepared, max_bytes).await
    }

    /// Attempt-scoped preparation settles before observations so a protected
    /// read cannot overtake its preceding item-lock statement.
    pub(super) async fn query_window(
        &self,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<Vec<SqlQueryPage>> {
        let prepared = collect_pipeline(
            join_all(
                statements
                    .into_iter()
                    .map(|(statement, protection)| self.prepare_query(statement, protection)),
            )
            .await,
            NativeOperation::SqlQuery,
        )?;
        collect_pipeline(
            join_all(
                prepared
                    .into_iter()
                    .map(|statement| self.query_prepared(statement, max_bytes)),
            )
            .await,
            NativeOperation::SqlQuery,
        )
    }

    pub(super) async fn execute(&self, statement: SqlStatement) -> Result<u64, PostgresCallError> {
        #[cfg(feature = "fault-injection")]
        self.check_fault(crate::SqlFaultOperation::Execute, statement.sql())?;
        let (sql, binds) = statement.into_parts();
        let params = bind_refs(&binds).inspect_err(|error| self.settlement.reject_error(error))?;
        let prepared = self
            .client
            .prepare(sql.as_ref())
            .await
            .map_err(PostgresCallError::from)
            .inspect_err(|error| error.reject_settlement(self.settlement))?;
        self.client
            .execute(&prepared, &params)
            .await
            .map_err(PostgresCallError::from)
            .inspect_err(|error| error.reject_settlement(self.settlement))
    }

    async fn prepare_query(
        &self,
        statement: SqlStatement,
        protection: ReadProtection,
    ) -> Result<PreparedQuery, PostgresCallError> {
        let (sql, binds) = statement.into_parts();
        #[cfg(feature = "fault-injection")]
        let original = sql.clone();
        let sql = append_query_protection(sql, protection)
            .inspect_err(|error| self.settlement.reject_error(error))?;
        let statement = self
            .client
            .prepare(sql.as_ref())
            .await
            .map_err(PostgresCallError::from)
            .inspect_err(|error| error.reject_settlement(self.settlement))?;
        Ok(PreparedQuery {
            statement,
            binds,
            #[cfg(feature = "fault-injection")]
            sql: original,
        })
    }

    async fn query_prepared(
        &self,
        prepared: PreparedQuery,
        max_bytes: NonZeroUsize,
    ) -> Result<SqlQueryPage, PostgresCallError> {
        #[cfg(feature = "fault-injection")]
        self.check_fault(crate::SqlFaultOperation::Query, &prepared.sql)?;
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
        #[cfg(feature = "fault-injection")]
        if let Some(probe) = self.fault_probe {
            return Ok(probe.alter_query_page(
                crate::SqlFaultCall {
                    operation: crate::SqlFaultOperation::Query,
                    sql: &prepared.sql,
                },
                page,
            ));
        }
        Ok(page)
    }

    #[cfg(feature = "fault-injection")]
    fn check_fault(&self, operation: crate::SqlFaultOperation, sql: &str) -> StorageResult<()> {
        if let Some(error) = self
            .fault_probe
            .and_then(|probe| probe.fail(crate::SqlFaultCall { operation, sql }))
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
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
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
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            owned
                .try_reserve_exact(bytes)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            owned.push_str(sql);
            owned
        }
    };
    sql.try_reserve_exact(suffix.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    sql.push_str(suffix);
    Ok(Cow::Owned(sql))
}
