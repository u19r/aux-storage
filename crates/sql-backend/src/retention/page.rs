//! Bounded physical stream reclamation inside the retention wave's SQL attempt.

use storage_provider::{
    StreamDurationTrimPageRequest, StreamDurationTrimPageResult, StreamTrimRow,
    stream_retention_kind,
};
use storage_types::{
    ItemStreamVersion, ReadProtection, StorageError, StorageResult, StreamItemId, StreamName,
    TimestampMillis,
};
use stream_provider::PointerOwner;

use super::sql_statements;
use crate::{
    SqlAttempt, SqlLayout, SqlQueryPage, SqlRow, SqlStatement, SqlValue,
    constants::{RETENTION_DELETE_BATCH_ROWS, RETENTION_READ_BYTES},
};

struct TrimCandidates {
    stream_name: StreamName,
    ids: Vec<StreamItemId>,
    table_stream: bool,
    rows: Vec<stream_provider::retained_image::stream_row::StoredStreamRow>,
}

pub(crate) async fn trim_stream_pages<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    requests: Vec<StreamDurationTrimPageRequest>,
) -> StorageResult<Vec<StreamDurationTrimPageResult>> {
    let mut streams = Vec::with_capacity(requests.len());
    let mut statements = Vec::with_capacity(requests.len());
    for request in &requests {
        let stream = stream_retention_kind(&request.scope)?;
        let stream_name = stream.value().clone();
        statements.push(sql_statements::trim_candidates(
            engine,
            &stream_name,
            *request.cutoff_timestamp,
            request.max_deleted_item_id,
            request.page_limit.into(),
        ));
        streams.push((
            stream_name,
            matches!(stream, stream_provider::StreamRowKind::Table(_)),
        ));
    }
    let pages = attempt
        .query_batch(statements, ReadProtection::Conflict, RETENTION_READ_BYTES)
        .await?;
    if pages.len() != requests.len() {
        return Err(StorageError::internal(
            "SQL stream trim candidate batch count differs",
        ));
    }
    let candidates = streams
        .into_iter()
        .zip(pages)
        .map(|((stream_name, table_stream), page)| {
            if page.more && page.rows.is_empty() {
                return Err(StorageError::internal(
                    "SQL stream trim page cannot advance",
                ));
            }
            let rows = page
                .rows
                .into_iter()
                .map(|row| decode_retirement_row(row, &stream_name))
                .collect::<StorageResult<Vec<_>>>()?;
            let ids = rows.iter().map(|row| row.metadata().0).collect();
            Ok(TrimCandidates {
                stream_name,
                ids,
                table_stream,
                rows,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;

    let receipts = candidates
        .iter()
        .map(|candidate| {
            stream_provider::retained_image::stream_row::StoredStreamRow::retirement_receipts(
                &candidate.rows,
            )
        })
        .collect::<StorageResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    {
        let mut catalog = crate::catalog::SqlCatalogAttempt::new(
            attempt,
            engine,
            storage_types::ReadProtection::Conflict,
        );
        stream_provider::retained_image::RetainedOwnerRecord::release_stream_batch(
            &mut catalog,
            receipts,
        )
        .await?;
    }

    // Owner references must be removed before their stream rows. Each phase
    // batches all independent statements across the wave.
    delete_batches(attempt, engine, &candidates, true).await?;
    delete_batches(attempt, engine, &candidates, false).await?;

    let statements = candidates
        .iter()
        .map(|candidate| sql_statements::first_remaining(engine, &candidate.stream_name))
        .collect();
    let first_pages = attempt
        .query_batch(statements, ReadProtection::Conflict, RETENTION_READ_BYTES)
        .await?;
    if first_pages.len() != candidates.len() {
        return Err(StorageError::internal(
            "SQL first retained stream batch count differs",
        ));
    }
    candidates
        .into_iter()
        .zip(first_pages)
        .map(|(candidate, page)| {
            let first = decode_first_remaining(page)?;
            Ok(StreamDurationTrimPageResult {
                deleted_rows: candidate.ids.len(),
                first_remaining_version: first
                    .as_ref()
                    .map(|row| ItemStreamVersion::from(row.item_id)),
                first_remaining_timestamp: first.map(|row| row.created_at),
            })
        })
        .collect()
}

fn decode_retirement_row(
    row: SqlRow,
    stream: &StreamName,
) -> StorageResult<stream_provider::retained_image::stream_row::StoredStreamRow> {
    let mut values = row.into_values();
    let bytes = values
        .pop()
        .ok_or_else(|| StorageError::internal("SQL stream retirement descriptor is missing"))?;
    let SqlValue::Blob(bytes) = bytes else {
        return Err(StorageError::internal(
            "SQL stream retirement descriptor type is invalid",
        ));
    };
    let evidence = decode_trim_row(SqlRow::new(values))?;
    let row = stream_provider::retained_image::stream_row::StoredStreamRow::decode(
        evidence.item_id,
        &bytes,
        RETENTION_READ_BYTES.get(),
    )?;
    row.require_address(stream, evidence.item_id)?;
    if row.metadata().1 != evidence.created_at {
        return Err(StorageError::internal(
            "SQL stream retirement timestamp differs from descriptor",
        ));
    }
    Ok(row)
}

async fn delete_batches<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    candidates: &[TrimCandidates],
    owners: bool,
) -> StorageResult<()> {
    let mut statements = Vec::<SqlStatement>::new();
    let mut expected = Vec::new();
    for candidate in candidates {
        if owners && !candidate.table_stream {
            continue;
        }
        for chunk in candidate.ids.chunks(RETENTION_DELETE_BATCH_ROWS) {
            statements.push(if owners {
                sql_statements::release_pointer_owners(
                    engine,
                    PointerOwner::Table,
                    &candidate.stream_name,
                    chunk,
                )
            } else {
                sql_statements::delete_stream_rows(engine, &candidate.stream_name, chunk)
            });
            expected.push(chunk.len());
        }
    }
    if statements.is_empty() {
        return Ok(());
    }
    let counts = attempt.execute_batch(statements).await?;
    if counts.len() != expected.len()
        || counts
            .into_iter()
            .zip(expected)
            .any(|(count, size)| count != size as u64)
    {
        return Err(StorageError::internal(
            "SQL stream trim delete count differs from candidates",
        ));
    }
    Ok(())
}

fn decode_first_remaining(page: SqlQueryPage) -> StorageResult<Option<StreamTrimRow>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL first retained stream row is not unique",
        ));
    }
    page.rows
        .into_iter()
        .next()
        .map(decode_trim_row)
        .transpose()
}

fn decode_trim_row(row: SqlRow) -> StorageResult<StreamTrimRow> {
    let [id, created_at]: [SqlValue; 2] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL stream trim row has invalid width"))?;
    let (SqlValue::Blob(id), SqlValue::Integer(created_at)) = (id, created_at) else {
        return Err(StorageError::internal(
            "SQL stream trim row has invalid types",
        ));
    };
    let item_id = StreamItemId::try_from(id.as_slice())
        .map_err(|_| StorageError::internal("SQL stream trim row has invalid ID"))?;
    Ok(StreamTrimRow {
        item_id,
        created_at: TimestampMillis::from_timestamp(created_at),
    })
}
