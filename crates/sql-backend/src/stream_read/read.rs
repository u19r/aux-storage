//! Ordered stream reads on the caller's SQL attempt.
use storage_types::{ItemHistoryAddress, StorageError, StorageResult, StreamItemId, StreamName};
use stream_provider::{
    StreamItem, StreamReadKind, StreamReadRequest, StreamReadResult, StreamRowKind, StreamRowPage,
    StreamRowReadKind, SystemStreamHistoryRead, SystemStreamHistoryReadPage, SystemStreamReadKind,
    retained_image::stream_row::StoredStreamRow,
};

use super::{row, sql_statements};
use crate::{
    ReadProtection, SqlAttempt, SqlEngine, SqlLayout, catalog::SqlCatalogAttempt,
    constants::STREAM_READ_BYTES, stream_checkpoint, stream_cursor, stream_history,
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
        StreamReadKind::System(SystemStreamReadKind::Cursors) => {
            stream_cursor::list_system_cursors(attempt, engine, protection)
                .await
                .map(StreamReadResult::SystemCursors)
        }
    }
}

pub(crate) async fn read_system_history_pages<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    reads: &[SystemStreamHistoryRead],
) -> StorageResult<Vec<SystemStreamHistoryReadPage>> {
    let retired = if engine.engine() == SqlEngine::Postgres {
        crate::stream_allocation::load_retired_offsets(attempt, engine, reads).await?
    } else {
        vec![None; reads.len()]
    };
    let statements = reads
        .iter()
        .map(|read| sql_statements::read_history(engine, read))
        .collect::<StorageResult<Vec<_>>>()?;
    // Postgres protects the join through the caller's serializable attempt;
    // FOR UPDATE cannot lock its nullable item side.
    let protection = if engine.engine() == SqlEngine::Postgres {
        ReadProtection::Snapshot
    } else {
        ReadProtection::Conflict
    };
    let pages = attempt
        .query_batch(
            statements,
            protection,
            crate::constants::SYSTEM_STREAM_PAGE_READ_BYTES,
        )
        .await?;
    if pages.len() != reads.len() {
        return Err(StorageError::internal(
            "SQL system history batch count differs",
        ));
    }
    let mut pending = Vec::with_capacity(pages.len());
    let mut groups = Vec::with_capacity(pages.len());
    let mut pointer_groups = Vec::with_capacity(pages.len());
    for ((page, read), retired) in pages.into_iter().zip(reads).zip(retired) {
        let page = decode_system_history_page(page, read.limit)?;
        groups.push(page.records);
        pending.push((page.observations, page.more, retired));
        pointer_groups.push(page.pointers);
    }
    let mut catalog = SqlCatalogAttempt::new(attempt, engine, ReadProtection::Conflict);
    let hydrated = stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::hydrate_history_groups(&mut catalog, groups, pointer_groups, STREAM_READ_BYTES.get()).await?;
    Ok(hydrated
        .into_iter()
        .zip(pending)
        .map(
            |((records, pointers), (observations, more, retired_through))| {
                SystemStreamHistoryReadPage {
                    records,
                    observations,
                    pointers,
                    more,
                    retired_through,
                }
            },
        )
        .collect())
}

pub(crate) async fn read_item_history_pages<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    reads: &[(ItemHistoryAddress, StreamReadRequest)],
) -> StorageResult<Vec<StreamRowPage>> {
    let statements = reads
        .iter()
        .map(|(history, read)| {
            sql_statements::read_rows(
                engine,
                &StreamRowKind::Item(history.physical_stream_name()),
                read,
            )
        })
        .collect();
    let pages = attempt
        .query_batch(statements, protection, STREAM_READ_BYTES)
        .await?;
    if pages.len() != reads.len() {
        return Err(StorageError::internal(
            "SQL item history batch count differs",
        ));
    }
    let pending = pages
        .into_iter()
        .map(decode_stored_page)
        .collect::<StorageResult<Vec<_>>>()?;
    let (groups, more): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
    let mut catalog = SqlCatalogAttempt::new(attempt, engine, protection);
    let hydrated =
        StoredStreamRow::hydrate_groups(&mut catalog, groups, STREAM_READ_BYTES.get()).await?;
    Ok(hydrated
        .into_iter()
        .zip(more)
        .map(|(rows, more)| StreamRowPage { rows, more })
        .collect())
}

struct SystemHistoryPage {
    records: Vec<stream_provider::retained_image::system_stream::RetainedSystemStreamRecord>,
    observations: Vec<stream_provider::SystemStreamHistoryRecord>,
    pointers: Vec<Option<StoredStreamRow>>,
    more: bool,
}

fn decode_system_history_page(
    mut page: crate::SqlQueryPage,
    limit: u32,
) -> StorageResult<SystemHistoryPage> {
    page.more |= page.rows.len() > limit as usize;
    page.rows.truncate(limit as usize);
    let mut records = Vec::with_capacity(page.rows.len());
    let mut observations = Vec::with_capacity(page.rows.len());
    let mut pointers = Vec::with_capacity(page.rows.len());
    for row in page.rows {
        let (record, observation, pointer) = row::decode_history_row(row)?;
        records.push(record);
        observations.push(observation);
        pointers.push(pointer);
    }
    let (records, pointers, excluded) = stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::select_pointer_prefix(records, pointers, STREAM_READ_BYTES.get())?;
    observations.truncate(records.len());
    page.more |= excluded;
    Ok(SystemHistoryPage {
        records,
        observations,
        pointers,
        more: page.more,
    })
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
    decode_row_page(attempt, engine, protection, page).await
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
    decode_row_item(attempt, engine, protection, page, (stream.value(), item_id)).await
}

async fn decode_row_item<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    page: crate::SqlQueryPage,
    address: (&StreamName, StreamItemId),
) -> StorageResult<Option<StreamItem>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal("SQL stream item read is not unique"));
    }
    let Some(row) = page.rows.into_iter().next() else {
        return Ok(None);
    };
    let row = row::decode_stored_stream_row(row)?;
    row.require_address(address.0, address.1)?;
    let mut catalog = SqlCatalogAttempt::new(attempt, engine, protection);
    let mut items =
        StoredStreamRow::hydrate(&mut catalog, vec![row], STREAM_READ_BYTES.get()).await?;
    Ok(items.pop())
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
    decode_row_page(catalog.attempt, catalog.engine, catalog.protection, page).await
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
    decode_row_item(
        catalog.attempt,
        catalog.engine,
        catalog.protection,
        page,
        (stream, id),
    )
    .await
}

async fn decode_row_page<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    page: crate::SqlQueryPage,
) -> StorageResult<StreamRowPage> {
    let (rows, more) = decode_stored_page(page)?;
    let mut catalog = SqlCatalogAttempt::new(attempt, engine, protection);
    let rows = StoredStreamRow::hydrate(&mut catalog, rows, STREAM_READ_BYTES.get()).await?;
    Ok(StreamRowPage { rows, more })
}

fn decode_stored_page(page: crate::SqlQueryPage) -> StorageResult<(Vec<StoredStreamRow>, bool)> {
    let rows = page
        .rows
        .into_iter()
        .map(row::decode_stored_stream_row)
        .collect::<StorageResult<Vec<_>>>()?;
    let (rows, excluded) = StoredStreamRow::select_prefix(rows, STREAM_READ_BYTES.get())?;
    Ok((rows, page.more || excluded))
}
