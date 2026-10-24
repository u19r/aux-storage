//! Batched point reads of original stream effects and records.

use storage_types::{
    StorageError, StorageResult, StreamItemId, StreamName, SystemStreamEffectDigest,
    SystemStreamPosition,
};
use stream_provider::{DynamoStreamEffectRow, StreamItem, StreamPointerReference};

use super::{row, sql_statements};
use crate::{
    ReadProtection, SqlAttempt, SqlLayout, SqlRow, SqlStatement, constants::STREAM_READ_BYTES,
};

pub(crate) async fn load_stream_effects<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    effects: &[SystemStreamEffectDigest],
) -> StorageResult<Vec<Option<DynamoStreamEffectRow>>> {
    let statements = effects
        .iter()
        .map(|effect| sql_statements::read_effect(engine, effect))
        .collect();
    load_point_rows(attempt, statements, protection, |_, row| {
        row::decode_blob_row(row)
    })
    .await
}

pub(crate) async fn load_stream_records<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    positions: &[SystemStreamPosition],
) -> StorageResult<
    Vec<
        Option<stream_provider::retained_image::verified_system_stream::VerifiedSystemStreamRecord>,
    >,
> {
    let statements = positions
        .iter()
        .map(|position| sql_statements::read_record(engine, position))
        .collect::<StorageResult<Vec<_>>>()?;
    let records = load_point_rows(attempt, statements, protection, |index, row| {
        let record: stream_provider::retained_image::system_stream::RetainedSystemStreamRecord =
            stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::decode(
                &row::decode_blob(row)?,
                STREAM_READ_BYTES.get(),
            )?;
        if record.position != positions[index] {
            return Err(StorageError::internal(
                "SystemStream record differs from its requested position",
            ));
        }
        Ok(record)
    })
    .await?;
    let mut catalog = crate::catalog::SqlCatalogAttempt::new(attempt, engine, protection);
    stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::hydrate_optional(
        &mut catalog,
        records,
        crate::constants::STREAM_READ_BYTES.get(),
    )
    .await
}

pub(crate) async fn load_stream_items<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    addresses: &[(&StreamName, StreamItemId)],
) -> StorageResult<Vec<Option<StreamItem>>> {
    // The original committed receipt supplies the inclusive point bound.
    let statements = addresses
        .iter()
        .map(|(stream, id)| sql_statements::read_item_through(engine, stream, *id, *id))
        .collect();
    let rows = load_point_rows(attempt, statements, protection, |index, row| {
        let (stream, id) = &addresses[index];
        let row = row::decode_stored_stream_row(row)?;
        row.require_address(stream, *id)?;
        Ok(row)
    })
    .await?;
    let mut catalog = crate::catalog::SqlCatalogAttempt::new(attempt, engine, protection);
    stream_provider::retained_image::stream_row::StoredStreamRow::hydrate_optional(
        &mut catalog,
        rows,
        STREAM_READ_BYTES.get(),
    )
    .await
}

pub(crate) async fn load_stream_references<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    addresses: &[(&StreamName, StreamItemId)],
) -> StorageResult<Vec<Option<StreamPointerReference>>> {
    let statements = addresses
        .iter()
        .map(|(stream, id)| sql_statements::read_reference(engine, stream, *id))
        .collect();
    load_point_rows(attempt, statements, protection, |index, row| {
        row::decode_pointer_reference(row, &addresses[index])
    })
    .await
}

async fn load_point_rows<A: SqlAttempt, T>(
    attempt: &mut A,
    statements: Vec<SqlStatement>,
    protection: ReadProtection,
    mut decode: impl FnMut(usize, SqlRow) -> StorageResult<T> + Send,
) -> StorageResult<Vec<Option<T>>> {
    if statements.is_empty() {
        return Ok(Vec::new());
    }
    let count = statements.len();
    let pages = attempt
        .query_batch(statements, protection, STREAM_READ_BYTES)
        .await?;
    if pages.len() != count {
        return Err(StorageError::internal(
            "SQL stream point batch count differs",
        ));
    }
    pages
        .into_iter()
        .enumerate()
        .map(|(index, page)| {
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "SQL stream point read exceeds its bound",
                ));
            }
            page.rows
                .into_iter()
                .next()
                .map(|row| decode(index, row))
                .transpose()
        })
        .collect()
}
