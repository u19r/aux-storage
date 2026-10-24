//! Validate the durable incarnation that fences all SQL maintenance work.

use storage_provider::CATALOG_READ_BYTES;
use storage_types::{StorageError, StorageResult, maintenance::TargetIncarnation};

use super::sql_statements;
use crate::{ReadProtection, SqlAttempt, SqlLayout, SqlQueryPage, SqlValue};

pub(crate) async fn initialize_maintenance<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    attempt
        .execute(sql_statements::create_identity(engine))
        .await?;
    attempt
        .execute(sql_statements::seed_identity(engine))
        .await?;
    attempt
        .execute(sql_statements::create_progress(engine))
        .await?;
    read_identity(attempt, engine).await?;
    Ok(())
}

pub(crate) async fn read_identity<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<TargetIncarnation> {
    let page = attempt
        .query(
            sql_statements::read_identity(engine),
            ReadProtection::Conflict,
            CATALOG_READ_BYTES,
        )
        .await?;
    decode_identity(page)
}

pub(super) fn decode_identity(page: SqlQueryPage) -> StorageResult<TargetIncarnation> {
    if page.more || page.rows.len() != 1 {
        return Err(StorageError::internal(
            "SQL maintenance identity is missing or duplicated",
        ));
    }
    let row = page
        .rows
        .into_iter()
        .next()
        .ok_or_else(|| StorageError::internal("SQL maintenance identity is missing"))?;
    let [value]: [SqlValue; 1] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL maintenance identity row has invalid width"))?;
    let SqlValue::Blob(bytes) = value else {
        return Err(StorageError::internal(
            "SQL maintenance identity row has invalid type",
        ));
    };
    let identity: [u8; 16] = bytes
        .try_into()
        .map_err(|_| StorageError::internal("SQL maintenance identity has invalid length"))?;
    Ok(TargetIncarnation::from(identity))
}
