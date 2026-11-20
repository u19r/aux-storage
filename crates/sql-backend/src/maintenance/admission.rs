//! One protected SQL batch for independent maintenance admission facts.

use serde::de::DeserializeOwned;
use storage_provider::CATALOG_READ_BYTES;
use storage_types::{
    StorageError, StorageResult,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
};

use super::{identity, progress, sql_statements};
use crate::{ReadProtection, SqlAttempt, SqlEngine, SqlLayout};

pub(crate) async fn read_admission<A: SqlAttempt, P: DeserializeOwned>(
    attempt: &mut A,
    engine: impl SqlLayout,
    kind: MaintenanceKind,
) -> StorageResult<(TargetIncarnation, Option<TaskProgress<P>>)> {
    // Postgres: an advisory lock for each maintenance kind replaces the row
    // locks, so admission assigns no transaction ID before the catalog and
    // item locks of the attempt (stream positions come from that ID). A
    // stale snapshot read is safe: the progress write is conditional.
    let mut statements = Vec::with_capacity(3);
    let protection = if engine.engine() == SqlEngine::Postgres {
        statements.push((
            sql_statements::admission_lock(kind),
            ReadProtection::Snapshot,
        ));
        ReadProtection::Snapshot
    } else {
        ReadProtection::Conflict
    };
    statements.push((sql_statements::read_identity(engine), protection));
    statements.push((sql_statements::read_progress(engine, kind), protection));
    let mut pages = attempt
        .query_batch_protected(statements, CATALOG_READ_BYTES)
        .await?;
    if engine.engine() == SqlEngine::Postgres && !pages.is_empty() {
        pages.remove(0);
    }
    let [identity, progress]: [_; 2] = pages.try_into().map_err(|_| {
        StorageError::internal("SQL maintenance admission returned the wrong number of pages")
    })?;
    Ok((
        identity::decode_identity(identity)?,
        progress::decode_progress(progress, kind)?,
    ))
}
