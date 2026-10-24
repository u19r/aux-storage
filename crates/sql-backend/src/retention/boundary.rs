//! Physical item-history boundaries read inside the retention wave's SQL
//! attempt.

use storage_provider::{
    ItemHistoryRetention, StreamTrimBoundary, StreamTrimScope, StreamTrimScopeBoundaries,
    stream_retention_kind,
};
use storage_types::{ReadProtection, StorageError, StorageResult, StreamItemId};

use super::{pointer::decode_first_pointer_reference, sql_statements};
use crate::{
    SqlAttempt, SqlLayout, SqlQueryPage, SqlRow, SqlValue, constants::RETENTION_READ_BYTES,
};

pub(crate) async fn load_stream_trim_boundaries<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    scopes: &[StreamTrimScope],
    protected_table_floor: Option<StreamItemId>,
) -> StorageResult<Vec<StreamTrimScopeBoundaries>> {
    let mut statements = Vec::with_capacity(scopes.len() * 2);
    let mut table_scopes = Vec::with_capacity(scopes.len());
    for scope in scopes {
        let stream = stream_retention_kind(scope)?;
        if matches!(stream, stream_provider::StreamRowKind::Table(_)) {
            table_scopes.push(true);
        } else {
            statements.push(sql_statements::latest_stream_item(engine, stream.value()));
            statements.push(sql_statements::first_pointer_reference(
                engine,
                stream.value(),
            ));
            table_scopes.push(false);
        }
    }
    let expected = statements.len();
    let pages = attempt
        .query_batch(statements, ReadProtection::Conflict, RETENTION_READ_BYTES)
        .await?;
    if pages.len() != expected {
        return Err(StorageError::internal(
            "SQL retention boundary batch count differs",
        ));
    }
    let mut pages = pages.into_iter();
    let mut observations = Vec::with_capacity(scopes.len());
    let mut references = Vec::new();
    for (scope, table_scope) in scopes.iter().zip(table_scopes) {
        if table_scope {
            observations.push(None);
            continue;
        }
        let latest = pages
            .next()
            .ok_or_else(|| StorageError::internal("SQL retention latest page is absent"))?;
        let pointer = pages
            .next()
            .ok_or_else(|| StorageError::internal("SQL retention pointer page is absent"))?;
        let referenced = decode_first_pointer_reference(pointer)?;
        if let Some(reference) = referenced {
            references.push(sql_statements::referenced_history(
                engine,
                stream_retention_kind(scope)?.value(),
                reference,
            ));
        }
        observations.push(Some(ItemHistoryRetention {
            latest: decode_latest_item_id(latest)?,
            referenced,
            at_reference: [None; 2],
        }));
    }
    // The reference is a result of the first wave. Fetch its exact image ID
    // and predecessor together, batching all scopes in this dependent wave.
    // Only keys are needed; do not hydrate or decode item payloads.
    let expected = references.len();
    let pages = attempt
        .query_batch(references, ReadProtection::Conflict, RETENTION_READ_BYTES)
        .await?;
    if pages.len() != expected {
        return Err(StorageError::internal(
            "SQL referenced history batch count differs",
        ));
    }
    let mut pages = pages.into_iter();
    observations
        .into_iter()
        .map(|observation| {
            let Some(mut observation) = observation else {
                return Ok(StreamTrimScopeBoundaries {
                    latest_item_id: None,
                    protected_boundary: protected_table_floor
                        .map(|item_id| StreamTrimBoundary { item_id }),
                    retained_table_pointer_boundary: None,
                });
            };
            if observation.referenced.is_some() {
                let page = pages.next().ok_or_else(|| {
                    StorageError::internal("SQL referenced history page is absent")
                })?;
                if page.rows.len() > 2 {
                    return Err(StorageError::internal(
                        "SQL referenced history page is unbounded",
                    ));
                }
                for (slot, row) in observation.at_reference.iter_mut().zip(page.rows) {
                    *slot = Some(decode_item_id(row)?);
                }
            }
            StreamTrimScopeBoundaries::for_item_history(observation)
        })
        .collect()
}

fn decode_latest_item_id(page: SqlQueryPage) -> StorageResult<Option<StreamItemId>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL latest stream item is not unique",
        ));
    }
    page.rows.into_iter().next().map(decode_item_id).transpose()
}

fn decode_item_id(row: SqlRow) -> StorageResult<StreamItemId> {
    let [SqlValue::Blob(bytes)]: [SqlValue; 1] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL latest stream item has invalid columns"))?
    else {
        return Err(StorageError::internal(
            "SQL latest stream item ID is invalid",
        ));
    };
    StreamItemId::try_from(bytes.as_slice())
        .map_err(|_| StorageError::internal("SQL latest stream item ID has invalid length"))
}
