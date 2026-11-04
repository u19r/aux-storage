//! Ordered stream reads on the caller's SQL attempt.
use storage_types::{StorageError, StorageResult, StreamItemId, StreamName};
use stream_provider::{
    StreamItem, StreamReadKind, StreamReadRequest, StreamReadResult, StreamRowKind, StreamRowPage,
    StreamRowReadKind, SystemStreamHistoryRead, SystemStreamHistoryReadPage, SystemStreamReadKind,
};

use super::{row, sql_statements};
use crate::{
    ReadProtection, SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt,
    constants::STREAM_READ_BYTES, stream_checkpoint, stream_history,
};

pub(crate) async fn read_stream<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    request: StreamReadKind<'_>,
) -> StorageResult<StreamReadResult> {
    match request {
        StreamReadKind::System(SystemStreamReadKind::Rows(request))
        | StreamReadKind::Table(request)
        | StreamReadKind::Item(request)
        | StreamReadKind::User(request) => match request.into_parts() {
            (stream, StreamRowReadKind::Page(request)) => {
                read_stream_rows(attempt, engine, protection, stream, request)
                    .await
                    .map(StreamReadResult::Items)
            }
            (stream, StreamRowReadKind::Point { item_id, .. }) => {
                read_stream_item(attempt, engine, protection, stream, item_id)
                    .await
                    .map(StreamReadResult::Item)
            }
        },
        StreamReadKind::System(SystemStreamReadKind::Catalog) => {
            let history = stream_history::read_history(attempt, engine).await?;
            Ok(StreamReadResult::SystemCatalog(history))
        }
        StreamReadKind::System(SystemStreamReadKind::Checkpoint(lookup)) => {
            stream_checkpoint::read_receipt(attempt, engine, lookup)
                .await
                .map(StreamReadResult::SystemCheckpoint)
        }
    }
}

pub(crate) async fn read_system_history_pages<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    reads: &[SystemStreamHistoryRead],
) -> StorageResult<Vec<SystemStreamHistoryReadPage>> {
    let retired = crate::stream_allocation::load_retired_offsets(
        attempt,
        engine,
        &reads
            .iter()
            .map(|read| read.after.clone())
            .collect::<Vec<_>>(),
    )
    .await?;
    let statements = reads
        .iter()
        .map(|read| sql_statements::read_history(engine, read))
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = attempt
        .query_batch(
            statements,
            ReadProtection::Conflict,
            crate::constants::SYSTEM_STREAM_PAGE_READ_BYTES,
        )
        .await?;
    if pages.len() != reads.len() {
        return Err(StorageError::internal(
            "SQL system history batch count differs",
        ));
    }
    pages
        .into_iter()
        .zip(reads)
        .zip(retired)
        .map(|((mut page, read), retired_through)| {
            let more = page.more || page.rows.len() > read.limit as usize;
            page.rows.truncate(read.limit as usize);
            let decoded = page
                .rows
                .into_iter()
                .map(row::decode_history_row)
                .collect::<StorageResult<Vec<_>>>()?;
            let (records, observations) = decoded.into_iter().unzip();
            Ok(SystemStreamHistoryReadPage {
                records,
                observations,
                more,
                retired_through,
            })
        })
        .collect()
}

async fn read_stream_rows<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    stream: &StreamRowKind,
    request: &StreamReadRequest,
) -> StorageResult<StreamRowPage> {
    let page = attempt
        .query(
            sql_statements::read_rows(engine, stream, request),
            protection,
            STREAM_READ_BYTES,
        )
        .await?;
    decode_row_page(page)
}

pub(crate) async fn read_stream_item<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    stream: &StreamRowKind,
    item_id: StreamItemId,
) -> StorageResult<Option<StreamItem>> {
    let page = attempt
        .query(
            sql_statements::read_item(engine, stream, item_id),
            protection,
            STREAM_READ_BYTES,
        )
        .await?;
    decode_row_item(page, (stream.value(), item_id))
}

fn decode_row_item(
    page: crate::SqlQueryPage,
    address: (&StreamName, StreamItemId),
) -> StorageResult<Option<StreamItem>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal("SQL stream item read is not unique"));
    }
    let Some(row) = page.rows.into_iter().next() else {
        return Ok(None);
    };
    let row = row::decode_materialized_row(row)?;
    row.require_address(address.0, address.1)?;
    Ok(Some(row))
}

pub(crate) async fn read_user_page_through<A: SqlAttempt>(
    catalog: &mut SqlCatalogAttempt<'_, A, impl SqlLayout>,
    stream: &StreamName,
    request: &StreamReadRequest,
    through: StreamItemId,
) -> StorageResult<StreamRowPage> {
    let page = catalog
        .attempt
        .query(
            sql_statements::read_rows_through(catalog.engine, stream, request, through),
            catalog.protection,
            STREAM_READ_BYTES,
        )
        .await?;
    decode_row_page(page)
}

pub(crate) async fn read_user_item_through<A: SqlAttempt>(
    catalog: &mut SqlCatalogAttempt<'_, A, impl SqlLayout>,
    stream: &StreamName,
    id: StreamItemId,
    through: StreamItemId,
) -> StorageResult<Option<StreamItem>> {
    let page = catalog
        .attempt
        .query(
            sql_statements::read_item_through(catalog.engine, stream, id, through),
            catalog.protection,
            STREAM_READ_BYTES,
        )
        .await?;
    decode_row_item(page, (stream, id))
}

fn decode_row_page(page: crate::SqlQueryPage) -> StorageResult<StreamRowPage> {
    let rows = page
        .rows
        .into_iter()
        .map(row::decode_materialized_row)
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(StreamRowPage {
        rows,
        more: page.more,
    })
}
