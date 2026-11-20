//! Asynchronous native statements and bounded pipelines on one leased
//! connection.
use std::num::NonZeroUsize;

use storage_common::diagnostics::{NativeEngine, NativeOperation, trace_native_failure};
use storage_types::StorageResult;

use super::{error::map_driver_error, value::decode_row};
use crate::{SqlQueryPage, SqlStatement};

pub(super) async fn query_batch(
    connection: &turso::Connection,
    queries: Vec<(SqlStatement, crate::ReadProtection)>,
    max_bytes: NonZeroUsize,
    fault_probe: Option<&dyn crate::SqlFaultProbe>,
) -> StorageResult<Vec<SqlQueryPage>> {
    let mut pages = Vec::new();
    pages
        .try_reserve_exact(queries.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for (statement, _) in queries {
        pages.push(query(connection, statement, max_bytes, fault_probe).await?);
    }
    Ok(pages)
}

pub(super) async fn execute_batch(
    connection: &turso::Connection,
    writes: Vec<SqlStatement>,
    fault_probe: Option<&dyn crate::SqlFaultProbe>,
) -> StorageResult<Vec<u64>> {
    let mut counts = Vec::new();
    counts
        .try_reserve_exact(writes.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for statement in writes {
        counts.push(execute(connection, statement, fault_probe).await?);
    }
    Ok(counts)
}

pub(super) async fn execute(
    connection: &turso::Connection,
    statement: SqlStatement,
    fault_probe: Option<&dyn crate::SqlFaultProbe>,
) -> StorageResult<u64> {
    if let Some(probe) = fault_probe
        && let Some(error) = probe.fail(crate::SqlFaultCall {
            operation: crate::SqlFaultOperation::Execute,
            sql: statement.sql(),
        })
    {
        trace_native_failure(NativeEngine::Turso, NativeOperation::SqlExecute, &error);
        return Err(error);
    }
    let wave =
        crate::fault_injection::SqlWave::start_plain(fault_probe, &[statement.sql()]).await?;
    let (sql, values) = statement.into_native_parts::<turso::Value>()?;
    let mut prepared = connection.prepare(&sql).await.map_err(map_driver_error)?;
    let changed = prepared
        .execute(values)
        .await
        .map_err(map_driver_error)
        .inspect_err(|error| {
            trace_native_failure(NativeEngine::Turso, NativeOperation::SqlExecute, error)
        })?;
    crate::fault_injection::end_wave(wave);
    crate::fault_injection::observe_statement(
        fault_probe,
        crate::SqlFaultOperation::Execute,
        changed,
    );
    Ok(changed)
}

pub(super) async fn query(
    connection: &turso::Connection,
    statement: SqlStatement,
    max_bytes: NonZeroUsize,
    fault_probe: Option<&dyn crate::SqlFaultProbe>,
) -> StorageResult<SqlQueryPage> {
    let result = async {
        let sql_for_probe = fault_probe.map(|_| statement.sql().to_owned());
        if let (Some(probe), Some(sql)) = (fault_probe, sql_for_probe.as_deref())
            && let Some(error) = probe.fail(crate::SqlFaultCall {
                operation: crate::SqlFaultOperation::Query,
                sql,
            })
        {
            return Err(error);
        }
        let wave =
            crate::fault_injection::SqlWave::start_plain(fault_probe, &[statement.sql()]).await?;
        let (sql, values) = statement.into_native_parts::<turso::Value>()?;
        let mut prepared = connection.prepare(&sql).await.map_err(map_driver_error)?;
        let mut cursor = prepared.query(values).await.map_err(map_driver_error)?;
        let width = cursor.column_count();
        let mut page = SqlQueryPage::new();
        while let Some(row) = cursor.next().await.map_err(map_driver_error)? {
            if !page.push(decode_row(&row, width)?, max_bytes)? {
                break;
            }
        }
        crate::fault_injection::end_wave(wave);
        if let (Some(probe), Some(sql)) = (fault_probe, sql_for_probe.as_deref()) {
            probe.observe(crate::SqlNativeEvent::Statement {
                operation: crate::SqlFaultOperation::Query,
                rows: u64::try_from(page.rows.len()).unwrap_or(u64::MAX),
            });
            return Ok(probe.alter_query_page(
                crate::SqlFaultCall {
                    operation: crate::SqlFaultOperation::Query,
                    sql,
                },
                page,
            ));
        }
        Ok(page)
    }
    .await;
    result.inspect_err(|error| {
        trace_native_failure(NativeEngine::Turso, NativeOperation::SqlQuery, error)
    })
}
