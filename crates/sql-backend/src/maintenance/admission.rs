//! One protected SQL batch for independent maintenance admission facts.

use serde::de::DeserializeOwned;
use storage_provider::CATALOG_READ_BYTES;
use storage_types::{
    StorageError, StorageResult,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
};

use super::{identity, progress, sql_statements};
use crate::{ReadProtection, SqlAttempt, SqlLayout};

pub(crate) async fn read_admission<A: SqlAttempt, P: DeserializeOwned>(
    attempt: &mut A,
    engine: impl SqlLayout,
    kind: MaintenanceKind,
) -> StorageResult<(TargetIncarnation, Option<TaskProgress<P>>)> {
    let pages = attempt
        .query_batch(
            vec![
                sql_statements::read_identity(engine),
                sql_statements::read_progress(engine, kind),
            ],
            ReadProtection::Conflict,
            CATALOG_READ_BYTES,
        )
        .await?;
    let [identity, progress]: [_; 2] = pages.try_into().map_err(|_| {
        StorageError::internal("SQL maintenance admission returned the wrong number of pages")
    })?;
    Ok((
        identity::decode_identity(identity)?,
        progress::decode_progress(progress, kind)?,
    ))
}
