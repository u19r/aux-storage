//! Find one runnable GSI build and verify its private history holds.

use storage_backfill::{GsiBuild, GsiBuildTarget};
use storage_provider::CATALOG_READ_BYTES;
use storage_types::{StorageError, StorageResult, SystemStreamPosition, SystemStreamScope};

use super::{read::decode_build_row, sql_statements};
use crate::{ColumnIndex, ReadProtection, SqlAttempt, SqlLayout, SqlRow, SqlValue};

pub(crate) async fn scan_build_candidates<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    after: Option<&GsiBuildTarget>,
) -> StorageResult<[Option<GsiBuild<Vec<u8>>>; 2]> {
    let mut statements = vec![sql_statements::candidate_statement(engine, after)];
    if after.is_some() {
        statements.push(sql_statements::candidate_statement(engine, None));
    }
    let pages = attempt
        .query_batch(statements, ReadProtection::Snapshot, CATALOG_READ_BYTES)
        .await?;
    let expected_pages = if after.is_some() { 2 } else { 1 };
    if pages.len() != expected_pages {
        return Err(StorageError::internal(
            "SQL GSI candidate batch returned the wrong page count",
        ));
    }
    let mut candidates = pages.into_iter().map(decode_candidate_page);
    let after_candidate = candidates
        .next()
        .ok_or_else(|| StorageError::internal("SQL GSI candidate batch has no first page"))??;
    let first_candidate = candidates.next().transpose()?.flatten();
    Ok([after_candidate, first_candidate])
}

fn decode_candidate_page(page: crate::SqlQueryPage) -> StorageResult<Option<GsiBuild<Vec<u8>>>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL GSI candidate query returned multiple rows",
        ));
    }
    let Some(row) = page.rows.into_iter().next() else {
        return Ok(None);
    };
    decode_build_row(row).map(Some)
}

pub(crate) async fn read_candidate_holds<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    build: &GsiBuild<Vec<u8>>,
) -> StorageResult<storage_provider::maintenance::GsiCandidateObservation> {
    let pages = attempt
        .query_batch(
            vec![
                sql_statements::candidate_binding_statement(engine, &build.target),
                sql_statements::read_hold_statement(engine, build),
            ],
            ReadProtection::Conflict,
            CATALOG_READ_BYTES,
        )
        .await?;
    let [binding, page]: [crate::SqlQueryPage; 2] = pages.try_into().map_err(|_| {
        StorageError::internal("SQL GSI candidate validation returned the wrong page count")
    })?;
    if binding.more || binding.rows.len() > 1 || page.more {
        return Err(StorageError::internal(
            "SQL GSI candidate read exceeded its bounds",
        ));
    }
    let observed = binding
        .rows
        .into_iter()
        .next()
        .map(decode_build_row)
        .transpose()?;
    let holds = page
        .rows
        .iter()
        .map(decode_hold_row)
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(storage_provider::maintenance::GsiCandidateObservation {
        build: observed,
        holds,
    })
}

fn decode_hold_row(row: &SqlRow) -> StorageResult<(SystemStreamScope, SystemStreamPosition)> {
    let (
        SqlValue::Text(scope),
        SqlValue::Integer(generation),
        SqlValue::Text(shard),
        SqlValue::Text(lineage),
        SqlValue::Integer(offset),
    ) = (
        row.value(ColumnIndex::new(0))?,
        row.value(ColumnIndex::new(1))?,
        row.value(ColumnIndex::new(2))?,
        row.value(ColumnIndex::new(3))?,
        row.value(ColumnIndex::new(4))?,
    )
    else {
        return Err(invalid_hold());
    };
    let scope = SystemStreamScope::try_new(
        scope.as_str(),
        u64::try_from(*generation).map_err(|_| invalid_hold())?,
    )
    .map_err(|_| invalid_hold())?;
    let position = SystemStreamPosition::try_new(
        shard.as_str(),
        lineage.as_str(),
        format!(
            "{:020}",
            u64::try_from(*offset).map_err(|_| invalid_hold())?
        ),
    )
    .map_err(|_| invalid_hold())?;
    Ok((scope, position))
}

fn invalid_hold() -> StorageError {
    StorageError::internal("GSI private retention hold does not match its build state")
}
