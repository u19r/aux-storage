//! Protected item histories referenced by SystemStream pointers.

use storage_backfill::{GsiReferencedHistoryItem, GsiReferencedHistoryRead};
use storage_types::{StorageError, StorageResult, StreamItemId};
use stream_provider::retained_image::stream_row::StoredStreamRow;

use super::sql_statements;
use crate::{
    ReadProtection, SqlAttempt, SqlLayout, SqlQueryPage, SqlRow, SqlValue,
    constants::{
        GSI_HISTORY_CHUNK_BYTES as HISTORY_CHUNK_BYTES,
        GSI_HISTORY_CHUNK_ROWS as HISTORY_CHUNK_ROWS,
    },
};

pub(crate) async fn read_referenced_histories<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    reads: &[GsiReferencedHistoryRead<'_>],
) -> StorageResult<Vec<GsiReferencedHistoryItem>> {
    let mut items = Vec::with_capacity(reads.len() * 2);
    let chunks = reads.chunks(HISTORY_CHUNK_ROWS).collect::<Vec<_>>();
    let statements = chunks
        .iter()
        .map(|chunk| sql_statements::referenced_histories_statement(engine, chunk))
        .collect();
    let pages = attempt
        .query_batch(statements, ReadProtection::Snapshot, HISTORY_CHUNK_BYTES)
        .await?;
    if pages.len() != chunks.len() {
        return Err(StorageError::internal(
            "SQL GSI history query batch count differs",
        ));
    }
    for (chunk, page) in chunks.into_iter().zip(pages) {
        items.extend(decode_history_chunk(chunk, page)?);
    }
    let (ordinals, rows): (Vec<_>, Vec<_>) = items.into_iter().unzip();
    let maximum = reads
        .len()
        .checked_mul(HISTORY_CHUNK_BYTES.get())
        .ok_or_else(|| StorageError::internal("GSI history work overflows"))?;
    let mut catalog = crate::catalog::SqlCatalogAttempt::new(
        attempt,
        engine,
        storage_types::ReadProtection::Conflict,
    );
    let hydrated = StoredStreamRow::hydrate(&mut catalog, rows, maximum).await?;
    Ok(ordinals
        .into_iter()
        .zip(hydrated)
        .map(|(candidate_ordinal, item)| GsiReferencedHistoryItem {
            candidate_ordinal,
            item,
        })
        .collect())
}

fn decode_history_chunk(
    reads: &[GsiReferencedHistoryRead<'_>],
    page: SqlQueryPage,
) -> StorageResult<Vec<(usize, StoredStreamRow)>> {
    if page.more || page.rows.len() > reads.len() * 2 {
        return Err(StorageError::internal(
            "SQL GSI referenced history page was truncated",
        ));
    }
    let mut items = Vec::with_capacity(page.rows.len());
    let mut previous: Option<(usize, StreamItemId)> = None;
    let mut ordinal_count = 0;
    for row in page.rows {
        let (ordinal, item) = decode_history_row(reads, row)?;
        let item_id = item.metadata().0;
        if previous.is_some_and(|(last, id)| ordinal < last || ordinal == last && item_id >= id) {
            return Err(StorageError::internal(
                "SQL GSI referenced history is unordered",
            ));
        }
        ordinal_count = if previous.is_some_and(|(last, _)| last == ordinal) {
            ordinal_count + 1
        } else {
            1
        };
        if ordinal_count > 2 {
            return Err(StorageError::internal(
                "SQL GSI history returned too many images",
            ));
        }
        previous = Some((ordinal, item_id));
        items.push((reads[ordinal].candidate_ordinal, item));
    }
    Ok(items)
}

fn decode_history_row(
    reads: &[GsiReferencedHistoryRead<'_>],
    row: SqlRow,
) -> StorageResult<(usize, StoredStreamRow)> {
    let [ordinal, id, bytes]: [SqlValue; 3] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL GSI image row has invalid width"))?;
    let (SqlValue::Integer(ordinal), SqlValue::Blob(id), SqlValue::Blob(bytes)) =
        (ordinal, id, bytes)
    else {
        return Err(StorageError::internal(
            "SQL GSI image row has invalid types",
        ));
    };
    let ordinal = usize::try_from(ordinal)
        .map_err(|_| StorageError::internal("SQL GSI image ordinal is invalid"))?;
    let read = reads
        .get(ordinal)
        .ok_or_else(|| StorageError::internal("SQL GSI image ordinal exceeds request"))?;
    let id = StreamItemId::try_from(id.as_slice())
        .map_err(|_| StorageError::internal("SQL GSI image ID is invalid"))?;
    if id > StreamItemId::from(read.through) {
        return Err(StorageError::internal(
            "SQL GSI image exceeds requested version",
        ));
    }
    let physical = read.physical_stream_name();
    let item = StoredStreamRow::decode(id, &bytes, HISTORY_CHUNK_BYTES.get())?;
    item.require_address(&physical, id)?;
    Ok((ordinal, item))
}
