//! Protected task progress shared by SQL maintenance workflows.

use serde::de::DeserializeOwned;
use storage_provider::maintenance::ProgressWrite;
use storage_types::{
    MissExpectation, Revision, StorageError, StorageResult,
    maintenance::{MaintenanceKind, TaskProgress},
};

use super::sql_statements;
use crate::{SqlAttempt, SqlLayout, SqlQueryPage, SqlStatement, SqlValue};

pub(super) fn decode_progress<P: DeserializeOwned>(
    page: SqlQueryPage,
    kind: MaintenanceKind,
) -> StorageResult<Option<TaskProgress<P>>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL maintenance task has invalid cardinality",
        ));
    }
    let Some(row) = page.rows.into_iter().next() else {
        return Ok(None);
    };
    let [target, revision, bytes]: [SqlValue; 3] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL maintenance task row has invalid width"))?;
    let (SqlValue::Blob(target), SqlValue::Integer(revision), SqlValue::Blob(bytes)) =
        (target, revision, bytes)
    else {
        return Err(StorageError::internal(
            "SQL maintenance task row has invalid types",
        ));
    };
    let progress: TaskProgress<P> =
        storage_provider::maintenance::decode_task_progress(&bytes, kind)?;
    if progress.authority.scope.target.as_ref() != target
        || encode_revision(progress.revision)? != revision
    {
        return Err(StorageError::internal(
            "SQL maintenance task columns differ from progress",
        ));
    }
    Ok(Some(progress))
}

/// Conditional insert or update of one progress record.
pub(crate) async fn store_progress<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    write: ProgressWrite,
) -> StorageResult<()> {
    if attempt.execute(progress_statement(engine, write)?).await? != 1 {
        return Err(StorageError::write_miss(
            "maintenance_progress",
            MissExpectation::Revision,
        ));
    }
    Ok(())
}

pub(crate) fn progress_statement(
    engine: impl SqlLayout,
    write: ProgressWrite,
) -> StorageResult<SqlStatement> {
    let target = write.target.as_ref().to_vec();
    let revision = encode_revision(write.revision)?;
    let statement = match write.expected {
        Some(expected) => sql_statements::update_progress(
            engine,
            write.kind,
            target,
            encode_revision(expected)?,
            revision,
            write.bytes,
        ),
        None => sql_statements::insert_progress(engine, write.kind, target, revision, write.bytes),
    };
    Ok(statement)
}

fn encode_revision(revision: Revision) -> StorageResult<i64> {
    i64::try_from(u64::from(revision))
        .map_err(|_| StorageError::internal("maintenance task revision exceeds SQL range"))
}
