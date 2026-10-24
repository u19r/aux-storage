//! Atomic receipt issuance and minimum protected frontier on SQL.

use storage_types::{StorageEnum, StorageResult, SystemStreamCheckpoint};
use stream_provider::SystemStreamCheckpointIssue;

use super::sql_statements;
use crate::{SqlAttempt, SqlLayout};

pub(crate) async fn initialize_checkpoints<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    for statement in sql_statements::schema(engine) {
        // run DDL changes serially
        attempt.execute(statement).await?;
    }
    Ok(())
}

pub(crate) async fn write_checkpoint<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    issue: &SystemStreamCheckpointIssue,
    checkpoint: &SystemStreamCheckpoint,
) -> StorageResult<()> {
    let receipt = issue.receipt(checkpoint);
    let changed = attempt
        .execute(sql_statements::insert(engine, issue, &receipt)?)
        .await?;
    if changed != 1 {
        return Err(StorageEnum::TransactionConflict {
            message: "SystemStream checkpoint was issued concurrently".to_owned(),
        }
        .into());
    }
    let statements = checkpoint
        .frontier()
        .iter()
        .map(|position| sql_statements::protect_frontier(engine, checkpoint.scope(), position))
        .collect::<StorageResult<Vec<_>>>()?;
    attempt.execute_batch(statements).await?;
    Ok(())
}
