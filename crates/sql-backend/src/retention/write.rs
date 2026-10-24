//! Retention state and conditional marker completion on one SQL attempt.

use storage_provider::{
    StreamRetentionFinishRecord, StreamTrimStateRecord, StreamTrimStateWrite,
    maintenance::StreamRetentionFinish,
};
use storage_types::{StorageError, StorageResult, storage_serde};

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
    let mut statements = vec![sql_statements::save_state(engine, record)];
    if let Some(marker) = next_marker {
        statements.push(sql_statements::save_marker(engine, marker));
    }
    Ok(statements)
}

pub(crate) async fn finish_stream_trim_markers<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    finishes: Vec<StreamRetentionFinish>,
) -> StorageResult<()> {
    let prepared = finishes
        .into_iter()
        .map(|finish| {
            let marker_blob = storage_serde::to_bytes(&finish.marker)?;
            let record: StreamRetentionFinishRecord = finish.try_into()?;
            if !record.valid_update {
                return Err(StorageError::internal(
                    "stream trim finish has an invalid state update",
                ));
            }
            Ok((record, marker_blob))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let deletes = prepared
        .iter()
        .map(|(record, marker_blob)| {
            sql_statements::delete_marker(engine, record, marker_blob.clone())
        })
        .collect();
    let counts = attempt.execute_batch(deletes).await?;
    if counts.len() != prepared.len() {
        return Err(StorageError::internal(
            "SQL stream trim marker delete batch count differs",
        ));
    }
    let mut states = Vec::new();
    let mut markers = Vec::new();
    for ((record, _), changed) in prepared.into_iter().zip(counts) {
        if changed > 1 {
            return Err(StorageError::internal(
                "SQL stream trim marker delete is not unique",
            ));
        }
        if changed == 1
            && let Some(mut write) = record.state_write
        {
            if let Some(marker) = write.next_marker.take() {
                markers.push(sql_statements::save_marker(engine, marker));
            }
            states.push(sql_statements::save_state(engine, write));
        }
    }
    states.extend(markers);
    attempt.execute_batch(states).await?;
    Ok(())
}
