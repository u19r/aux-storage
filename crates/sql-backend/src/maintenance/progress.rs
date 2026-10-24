//! Protected task progress shared by SQL maintenance workflows.

use serde::{Serialize, de::DeserializeOwned};
use storage_types::{
    Revision, StorageEnum, StorageError, StorageResult,
    maintenance::{MaintenanceKind, TaskProgress},
    storage_serde,
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
    let progress: TaskProgress<P> = storage_serde::from_bytes(&bytes)?;
    if progress.authority.scope.work != kind
        || progress.authority.scope.target.as_ref() != target
        || encode_revision(progress.revision)? != revision
    {
        return Err(StorageError::internal(
            "SQL maintenance task columns differ from progress",
        ));
    }
    Ok(Some(progress))
}

pub(crate) async fn write_progress<A: SqlAttempt, P: Serialize>(
    attempt: &mut A,
    engine: impl SqlLayout,
    kind: MaintenanceKind,
    expected: Option<Revision>,
    next: TaskProgress<P>,
) -> StorageResult<()> {
    let statement = progress_statement(engine, kind, expected, &next)?;
    if attempt.execute(statement).await? != 1 {
        return Err(StorageEnum::TransactionConflict {
            message: "maintenance task progress changed during this attempt".to_owned(),
        }
        .into());
    }
    Ok(())
}

pub(crate) fn progress_statement<P: Serialize>(
    engine: impl SqlLayout,
    kind: MaintenanceKind,
    expected: Option<Revision>,
    next: &TaskProgress<P>,
) -> StorageResult<SqlStatement> {
    if next.authority.scope.work != kind {
        return Err(StorageError::internal(
            "maintenance task progress has the wrong kind",
        ));
    }
    let target = next.authority.scope.target.as_ref().to_vec();
    let revision = encode_revision(next.revision)?;
    let bytes = storage_serde::to_bytes(next)?;
    let statement = match expected {
        Some(expected) => sql_statements::update_progress(
            engine,
            kind,
            target,
            encode_revision(expected)?,
            revision,
            bytes,
        ),
        None => sql_statements::insert_progress(engine, kind, target, revision, bytes),
    };
    Ok(statement)
}

fn encode_revision(revision: Revision) -> StorageResult<i64> {
    i64::try_from(u64::from(revision))
        .map_err(|_| StorageError::internal("maintenance task revision exceeds SQL range"))
}
