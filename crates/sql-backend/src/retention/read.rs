//! Bounded retention discovery and protected wave observations.

use storage_provider::{
    StreamTrimDueMarker, StreamTrimDueRecord, StreamTrimScope, StreamTrimState,
    maintenance::StreamRetentionObservation,
};
use storage_types::{ReadProtection, StorageError, StorageResult, TimestampMillis, storage_serde};

use super::sql_statements;
use crate::{
    SqlAttempt, SqlEngine, SqlLayout, SqlQueryPage, SqlRow, SqlValue,
    constants::RETENTION_READ_BYTES,
};

pub(crate) async fn list_due_stream_trim_markers<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    due_before: TimestampMillis,
    limit: usize,
) -> StorageResult<Vec<StreamTrimDueMarker>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(limit)
        .map_err(|_| StorageError::validation("stream trim marker page limit is too large"))?;
    let statement = sql_statements::due_markers(engine, *due_before, limit);
    let page = if engine.engine() == SqlEngine::Postgres {
        // Both independent reads share the existing discovery attempt and query
        // wave.
        let pages = attempt
            .query_batch_exact(
                vec![statement, sql_statements::oldest_assigned_transaction_age()],
                ReadProtection::Snapshot,
                RETENTION_READ_BYTES,
            )
            .await?;
        let [markers, age]: [SqlQueryPage; 2] = pages.into_array()?;
        let age_ms = decode_oldest_assigned_transaction_age(age)?;
        metrics::gauge!("storage.postgres.stream.frontier.oldest_xact.age.ms").set(age_ms as f64);
        markers
    } else {
        attempt
            .query(statement, ReadProtection::Snapshot, RETENTION_READ_BYTES)
            .await?
    };
    page.rows.into_iter().map(decode_due_marker).collect()
}

pub(crate) async fn load_stream_trim_states<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    scopes: &[StreamTrimScope],
    protection: ReadProtection,
) -> StorageResult<Vec<Option<StreamTrimState>>> {
    let mut order = (0..scopes.len()).collect::<Vec<_>>();
    order.sort_unstable_by(|left, right| scopes[*left].scope_id.cmp(&scopes[*right].scope_id));
    let statements = order
        .iter()
        .map(|index| sql_statements::state(engine, &scopes[*index].scope_id))
        .collect();
    let pages = attempt
        .query_batch_exact(statements, protection, RETENTION_READ_BYTES)
        .await?;
    let mut states = vec![None; scopes.len()];
    for (index, page) in order.into_iter().zip(pages) {
        if page.more || page.rows.len() > 1 {
            return Err(StorageError::internal(
                "SQL stream trim state identity is not unique",
            ));
        }
        states[index] = page
            .rows
            .into_iter()
            .next()
            .map(|row| decode_trim_state(row, &scopes[index]))
            .transpose()?;
    }
    Ok(states)
}

pub(crate) async fn load_stream_trim_observations<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    markers: &[StreamTrimDueMarker],
) -> StorageResult<storage_types::Exact<StreamRetentionObservation>> {
    let mut order = (0..markers.len()).collect::<Vec<_>>();
    order.sort_unstable_by(|left, right| {
        markers[*left]
            .scope
            .scope_id
            .cmp(&markers[*right].scope.scope_id)
    });
    let mut statements = order
        .iter()
        .map(|index| sql_statements::state(engine, &markers[*index].scope.scope_id))
        .collect::<Vec<_>>();
    statements.extend(
        order
            .iter()
            .map(|index| {
                Ok(sql_statements::marker(
                    engine,
                    markers[*index].clone().try_into()?,
                ))
            })
            .collect::<StorageResult<Vec<_>>>()?,
    );
    let pages = attempt
        .query_batch_exact(statements, ReadProtection::Conflict, RETENTION_READ_BYTES)
        .await?;
    let mut pages = pages.into_iter();
    let mut states = vec![None; markers.len()];
    for &index in &order {
        let page = pages.next().ok_or_else(|| {
            StorageError::internal("SQL stream trim state observation is missing")
        })?;
        if page.more || page.rows.len() > 1 {
            return Err(StorageError::internal(
                "SQL stream trim state identity is not unique",
            ));
        }
        states[index] = page
            .rows
            .into_iter()
            .next()
            .map(|row| decode_trim_state(row, &markers[index].scope))
            .transpose()?;
    }
    let mut present = vec![false; markers.len()];
    for index in order {
        let marker = &markers[index];
        let page = pages.next().ok_or_else(|| {
            StorageError::internal("SQL stream trim marker observation is missing")
        })?;
        if page.more || page.rows.len() > 1 {
            return Err(StorageError::internal(
                "SQL stream trim marker identity is not unique",
            ));
        }
        if let Some(row) = page.rows.into_iter().next() {
            let [SqlValue::Blob(blob)]: [SqlValue; 1] =
                row.into_values().try_into().map_err(|_| {
                    StorageError::internal("SQL stream trim marker has invalid columns")
                })?
            else {
                return Err(StorageError::internal(
                    "SQL stream trim marker payload is invalid",
                ));
            };
            if blob != storage_serde::to_bytes(marker)? {
                return Err(StorageError::internal(
                    "SQL stream trim marker payload differs",
                ));
            }
            present[index] = true;
        }
    }
    storage_types::Exact::try_new(
        markers.len(),
        present.into_iter().zip(states).map(Into::into).collect(),
    )
}

fn decode_due_marker(row: SqlRow) -> StorageResult<StreamTrimDueMarker> {
    let values: [SqlValue; 4] = row.into_values().try_into().map_err(|_| {
        StorageError::internal("SQL stream trim due marker has invalid column count")
    })?;
    let [
        SqlValue::Integer(due_at),
        SqlValue::Text(scope_id),
        SqlValue::Integer(policy_version),
        SqlValue::Blob(marker_blob),
    ] = values
    else {
        return Err(StorageError::internal(
            "SQL stream trim due marker has invalid columns",
        ));
    };
    StreamTrimDueRecord {
        due_at,
        scope_id,
        policy_version,
        marker_blob,
    }
    .try_into()
}

pub(crate) fn decode_trim_state(
    row: SqlRow,
    scope: &StreamTrimScope,
) -> StorageResult<StreamTrimState> {
    let values: [SqlValue; 2] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL stream trim state has invalid column count"))?;
    let [SqlValue::Text(scope_id), SqlValue::Blob(blob)] = values else {
        return Err(StorageError::internal(
            "SQL stream trim state has invalid columns",
        ));
    };
    let state: StreamTrimState = storage_serde::from_bytes(&blob)?;
    state.require_valid_frontier()?;
    if scope_id != scope.scope_id || state.scope != *scope {
        return Err(StorageError::internal(
            "SQL stream trim state has another scope",
        ));
    }
    Ok(state)
}

pub(super) fn decode_oldest_assigned_transaction_age(page: SqlQueryPage) -> StorageResult<u64> {
    if page.more || page.rows.len() != 1 {
        return Err(StorageError::internal(
            "Postgres transaction age row count is invalid",
        ));
    }
    let [row]: [SqlRow; 1] = page
        .rows
        .try_into()
        .map_err(|_| StorageError::internal("Postgres transaction age row is missing"))?;
    let [SqlValue::Integer(age_ms)]: [SqlValue; 1] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("Postgres transaction age columns are invalid"))?
    else {
        return Err(StorageError::internal(
            "Postgres transaction age is not an integer",
        ));
    };
    // Host clock corrections can put a transaction start after the observation
    // time.
    Ok(age_ms.max(0) as u64)
}
