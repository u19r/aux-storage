//! Ordered stream reads on the caller's SQL attempt.
use storage_types::{StorageError, StorageResult, StreamItemId, StreamName};
use stream_provider::{
    StreamItem, StreamReadKind, StreamReadRequest, StreamReadResult, StreamRowPage,
    StreamRowReadKind, SystemStreamHistoryRead, SystemStreamHistoryReadPage,
};

use super::{row, sql_statements};
use crate::{
    ReadProtection, SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt, constants::STREAM_READ_BYTES,
};

pub(crate) async fn read_stream<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    request: StreamReadKind<'_>,
) -> StorageResult<StreamReadResult> {
    match request.read {
        StreamRowReadKind::Page(read) => {
            read_stream_rows(attempt, engine, protection, request.stream, read)
                .await
                .map(StreamReadResult::Items)
        }
        StreamRowReadKind::Point {
            partitioning_mode: _,
            item_id,
        } => read_stream_item(attempt, engine, protection, request.stream, item_id)
            .await
            .map(StreamReadResult::Item),
    }
}

pub(crate) async fn read_system_history_pages<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
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
        .query_batch_exact(
            statements,
            protection,
            crate::constants::SYSTEM_STREAM_PAGE_READ_BYTES,
        )
        .await?;
    let mut result = Vec::with_capacity(reads.len());
    let mut histories = std::collections::HashSet::new();
    for ((mut page, read), retired_through) in pages.into_iter().zip(reads).zip(retired) {
        let more = page.more || page.rows.len() > read.limit as usize;
        page.rows.truncate(read.limit as usize);
        let records = page
            .rows
            .into_iter()
            .map(row::decode_entry_row)
            .collect::<StorageResult<Vec<_>>>()?;
        histories.extend(records.iter().map(|record| record.history_id));
        result.push((records, more, retired_through));
    }
    let mut histories = histories.into_iter().collect::<Vec<_>>();
    histories.sort_by(|left, right| left.as_ref().cmp(right.as_ref()));
    let states =
        super::table_catalog::load_history_table_states(attempt, engine, protection, &histories)
            .await?;
    let states = histories
        .into_iter()
        .zip(states)
        .collect::<std::collections::HashMap<_, _>>();
    let result = result
        .into_iter()
        .map(|(records, more, retired_through)| {
            let records = records
                .into_iter()
                .map(|record| {
                    let state = states
                        .get(&record.history_id)
                        .ok_or_else(|| {
                            StorageError::internal("SQL incarnation observation missing")
                        })?
                        .clone();
                    Ok((record, state))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            Ok(SystemStreamHistoryReadPage {
                records,
                more,
                retired_through,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(result)
}

async fn read_stream_rows<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    stream: &storage_types::StreamName,
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
    stream: &storage_types::StreamName,
    item_id: StreamItemId,
) -> StorageResult<Option<StreamItem>> {
    let page = attempt
        .query(
            sql_statements::read_item(engine, stream, item_id),
            protection,
            STREAM_READ_BYTES,
        )
        .await?;
    decode_row_item(page, (stream, item_id))
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
    let row = row::decode_stream_row(row)?;
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
        .map(row::decode_stream_row)
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(StreamRowPage {
        rows,
        more: page.more,
    })
}
