//! Retention state and conditional marker completion on one SQL attempt.

use storage_provider::{
    StreamTrimStateRecord, StreamTrimStateWrite,
    maintenance::{PreparedRetentionFinish, RetentionStateWrite},
};
use storage_types::{StorageError, StorageResult};

use super::sql_statements;
use crate::{SqlAttempt, SqlLayout};

pub async fn initialize_retention<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    for statement in sql_statements::schema(engine) {
        attempt.execute(statement).await?;
    }
    Ok(())
}

pub async fn write_stream_retention<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    write: StreamTrimStateWrite,
) -> StorageResult<()> {
    attempt
        .execute_batch(stream_retention_statements(engine, write)?)
        .await?;
    Ok(())
}

pub(crate) fn stream_retention_statements(
    engine: impl SqlLayout,
    write: StreamTrimStateWrite,
) -> StorageResult<Vec<crate::SqlStatement>> {
    let mut record: StreamTrimStateRecord = write.try_into()?;
    let next_marker = record.next_marker.take();
    let replaced_marker = record.replaced_marker.take();
    let mut statements = vec![sql_statements::save_state(engine, record)];
    if let Some(marker) = replaced_marker {
        statements.push(sql_statements::remove_marker(engine, marker));
    }
    if let Some(marker) = next_marker {
        statements.push(sql_statements::save_marker(engine, marker));
    }
    Ok(statements)
}

/// Conditional delete of each finish's marker (marker payload and, when
/// expected, the stored state are compared by the statement). One batch.
pub(crate) async fn delete_stream_trim_markers<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    finishes: &[PreparedRetentionFinish],
) -> StorageResult<storage_types::Exact<bool>> {
    let deletes = finishes
        .iter()
        .map(|finish| {
            sql_statements::delete_marker(engine, &finish.record, finish.marker_blob.clone())
        })
        .collect();
    let counts = attempt.execute_batch_exact(deletes).await?;
    counts.try_map(|changed| match changed {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(StorageError::internal(
            "SQL stream trim marker delete is not unique",
        )),
    })
}

/// Store each state, then every next marker, in one batch (today's order).
pub(crate) async fn store_stream_trim_states<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    states: Vec<RetentionStateWrite>,
) -> StorageResult<()> {
    let mut statements = Vec::with_capacity(states.len());
    let mut markers = Vec::new();
    for RetentionStateWrite {
        record: mut write, ..
    } in states
    {
        if let Some(marker) = write.next_marker.take() {
            markers.push(sql_statements::save_marker(engine, marker));
        }
        statements.push(sql_statements::save_state(engine, write));
    }
    statements.extend(markers);
    attempt.execute_batch(statements).await?;
    Ok(())
}
