//! Find one runnable GSI build and protect its candidate binding.

use storage_backfill::{GsiBuild, GsiBuildTarget};
use storage_provider::CATALOG_READ_BYTES;
use storage_types::{StorageError, StorageResult};

use super::{read::decode_build_row, sql_statements};
use crate::{ReadProtection, SqlAttempt, SqlLayout};

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

pub(crate) async fn read_candidate_binding<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    build: &GsiBuild<Vec<u8>>,
) -> StorageResult<Option<GsiBuild<Vec<u8>>>> {
    let page = attempt
        .query(
            sql_statements::candidate_binding_statement(engine, &build.target),
            ReadProtection::Conflict,
            CATALOG_READ_BYTES,
        )
        .await?;
    decode_candidate_page(page)
}
