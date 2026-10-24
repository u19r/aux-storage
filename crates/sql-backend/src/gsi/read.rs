//! Protected GSI lifecycle and readiness observations.

use storage_backfill::{
    GsiActivationState, GsiBuild, GsiBuildPhase, GsiBuildStatus, GsiBuildTarget,
    GsiProjectionLifecycle, GsiRecord,
};
use storage_provider::{CATALOG_READ_BYTES, GsiReadinessSnapshot};
use storage_types::{BoundTableInfo, IndexName, StorageError, StorageResult};

use super::sql_statements::{gsi_build_statement, gsi_status_statement};
use crate::{ReadProtection, SqlAttempt, SqlLayout, SqlQueryPage, SqlRow, SqlStatement, SqlValue};

pub(crate) async fn read_gsi_readiness<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    protection: ReadProtection,
    requested: Option<&IndexName>,
) -> StorageResult<(GsiReadinessSnapshot, Vec<GsiBuildTarget>)> {
    let indexes = table
        .info
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default();
    if requested.is_some_and(|name| !indexes.iter().any(|index| index.index_name == *name)) {
        return Err(StorageError::internal(
            "SQL GSI identity is absent from catalog",
        ));
    }
    if indexes.is_empty() {
        return Ok((GsiReadinessSnapshot::from(Vec::new()), Vec::new()));
    }
    let statement = gsi_status_statement(engine, table, requested);
    let page = attempt
        .query(statement, protection, CATALOG_READ_BYTES)
        .await?;
    let builds = decode_gsi_statuses(page, table, requested)?;
    let targets = builds.iter().map(|build| build.target.clone()).collect();
    Ok((GsiReadinessSnapshot::try_from(builds)?, targets))
}

pub(crate) async fn read_activation_state<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    target: &GsiBuildTarget,
) -> StorageResult<GsiActivationState> {
    let page = attempt
        .query(
            gsi_build_statement(engine, table, Some(&target.index_name)),
            ReadProtection::Conflict,
            CATALOG_READ_BYTES,
        )
        .await?;
    if page.more || page.rows.len() != 1 {
        return Err(GsiActivationState::conflict_error());
    }
    let row = page
        .rows
        .into_iter()
        .next()
        .ok_or_else(GsiActivationState::conflict_error)?;
    GsiActivationState::try_from_build(decode_build_row(row)?, target)
}

pub(crate) fn gsi_lifecycle_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
) -> Option<SqlStatement> {
    if table
        .info
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
        .is_empty()
    {
        return None;
    }
    Some(gsi_status_statement(engine, table, None))
}

pub(crate) fn decode_gsi_lifecycle(
    page: SqlQueryPage,
    table: &BoundTableInfo,
) -> StorageResult<GsiProjectionLifecycle> {
    let builds = decode_gsi_statuses(page, table, None)?;
    Ok(GsiProjectionLifecycle::try_from(builds)?)
}

fn decode_gsi_statuses(
    page: SqlQueryPage,
    table: &BoundTableInfo,
    requested: Option<&IndexName>,
) -> StorageResult<Vec<GsiBuildStatus>> {
    let indexes = table
        .info
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default();
    let expected = indexes
        .iter()
        .filter(|index| requested.is_none_or(|name| index.index_name == *name))
        .count();
    if page.more || page.rows.len() != expected {
        return Err(StorageError::internal(
            "SQL GSI build set does not match table metadata",
        ));
    }

    let mut builds = Vec::with_capacity(indexes.len());
    for row in page.rows {
        let build = decode_status_row(row)?;
        if build.target.table_name != table.info.table_name
            || !indexes.iter().any(|index| {
                index.index_name == build.target.index_name
                    && requested.is_none_or(|name| index.index_name == *name)
            })
        {
            return Err(StorageError::internal("SQL GSI build has no table index"));
        }
        builds.push(build);
    }
    Ok(builds)
}

fn decode_status_row(row: SqlRow) -> StorageResult<GsiBuildStatus> {
    let [table_name, index_name, incarnation, phase] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL GSI status row width is invalid"))?;
    let (
        SqlValue::Text(table_name),
        SqlValue::Text(index_name),
        SqlValue::Blob(incarnation),
        SqlValue::Text(phase),
    ) = (table_name, index_name, incarnation, phase)
    else {
        return Err(StorageError::internal(
            "SQL GSI status row has invalid types",
        ));
    };
    Ok(GsiBuildStatus {
        target: GsiBuildTarget::try_from((table_name, index_name, incarnation))?,
        phase: GsiBuildPhase::try_from(phase.as_str())?,
    })
}

pub(super) fn decode_build_row(row: SqlRow) -> StorageResult<GsiBuild<Vec<u8>>> {
    let [table_name, index_name, incarnation, phase, state] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL GSI build row width is invalid"))?;
    let (
        SqlValue::Text(table_name),
        SqlValue::Text(index_name),
        SqlValue::Blob(incarnation),
        SqlValue::Text(phase),
        SqlValue::Blob(state),
    ) = (table_name, index_name, incarnation, phase, state)
    else {
        return Err(StorageError::internal(
            "SQL GSI build row has invalid types",
        ));
    };
    GsiBuild::try_from(GsiRecord {
        table_name,
        index_name,
        incarnation,
        phase,
        state,
    })
    .map_err(Into::into)
}
