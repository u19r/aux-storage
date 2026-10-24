//! Install GSI state and register build records with history holds.

use storage_backfill::{GsiBuild, GsiStateRecord};
use storage_types::{StorageError, StorageResult, TableHistoryId};

use super::sql_statements::{gsi_schema, history_hold_statement, insert_build_statement};
use crate::{SqlAttempt, SqlLayout, SqlStatement};

/// Install the existing GSI build tables before publishing a SQL store.
pub async fn initialize_gsi<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    for sql in gsi_schema(engine) {
        attempt.execute(SqlStatement::new(sql, vec![])).await?;
    }
    Ok(())
}

/// Register a decided build and its stream-history holds inside the caller's
/// native attempt. The caller has captured a protected SystemStream frontier.
pub async fn register_gsi_builds<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    table_history_id: TableHistoryId,
    builds: Vec<GsiBuild<()>>,
) -> StorageResult<()> {
    let mut statements = Vec::new();
    for build in builds {
        let state = GsiStateRecord::try_from(&build.state)?;
        statements.push(insert_build_statement(engine, &build, state));
        for position in build.state.required_stream_history().frontier() {
            statements.push(history_hold_statement(
                engine,
                table_history_id,
                &build,
                position,
            )?);
        }
    }
    let expected = statements.len();
    let changed = attempt.execute_batch(statements).await?;
    if changed.len() != expected || changed.into_iter().any(|count| count != 1) {
        return Err(StorageError::internal(
            "SQL GSI registration changed the wrong row count",
        ));
    }
    Ok(())
}
