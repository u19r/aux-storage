//! Protected item histories referenced by SystemStream pointers.

use storage_backfill::{GsiReferencedHistoryItem, GsiReferencedHistoryRead};
use storage_types::{StorageError, StorageResult, StreamItemId, StreamName};
use stream_provider::retained_image::stream_row::StoredStreamRow;

use crate::{
    KvAttempt, KvRange, KvRangePage, ReadProtection,
    constants::{GSI_HISTORY_BYTES, GSI_HISTORY_ROWS},
    stream_write::key_layout::item_row_prefix,
};

pub(crate) async fn read_referenced_histories<A: KvAttempt>(
    attempt: &mut A,
    reads: &[GsiReferencedHistoryRead<'_>],
) -> StorageResult<Vec<GsiReferencedHistoryItem>> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    let mut physicals = Vec::with_capacity(reads.len());
    let mut ranges = Vec::with_capacity(reads.len());
    for read in reads {
        let (physical, range) = history_range(read)?;
        physicals.push(physical);
        ranges.push(range);
    }
    let pages = attempt.scan_batch(ranges, ReadProtection::Conflict).await?;
    if pages.len() != reads.len() {
        return Err(StorageError::internal(
            "KV GSI image batch returned the wrong count",
        ));
    }
    let mut items = Vec::with_capacity(reads.len() * 2);
    for ((read, physical), page) in reads.iter().zip(physicals).zip(pages) {
        items.extend(decode_history_page(read, &physical, page)?);
    }
    let (ordinals, rows): (Vec<_>, Vec<_>) = items.into_iter().unzip();
    let maximum = reads
        .len()
        .checked_mul(GSI_HISTORY_BYTES.get())
        .ok_or_else(|| StorageError::internal("GSI history work overflows"))?;
    let mut catalog =
        crate::catalog::KvCatalogAttempt::new(attempt, storage_types::ReadProtection::Conflict);
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

fn history_range(read: &GsiReferencedHistoryRead<'_>) -> StorageResult<(StreamName, KvRange)> {
    let physical = read.physical_stream_name();
    let prefix = item_row_prefix(&physical)?;
    let mut end = prefix.clone();
    end.extend_from_slice(StreamItemId::from(read.through).as_bytes());
    end.push(0);
    Ok((
        physical,
        KvRange {
            entry_bounds: None,
            begin: prefix,
            end,
            reverse: true,
            limit: GSI_HISTORY_ROWS,
            max_bytes: GSI_HISTORY_BYTES,
        },
    ))
}

fn decode_history_page(
    read: &GsiReferencedHistoryRead<'_>,
    physical: &StreamName,
    page: KvRangePage,
) -> StorageResult<Vec<(usize, StoredStreamRow)>> {
    if page.entries.len() > 2 || page.more && page.entries.len() < 2 {
        return Err(StorageError::internal("KV GSI image page was truncated"));
    }
    let prefix = item_row_prefix(physical)?;
    let mut previous = None;
    let mut items = Vec::with_capacity(page.entries.len());
    for entry in page.entries {
        let id = StreamItemId::try_from(
            entry
                .key
                .strip_prefix(prefix.as_slice())
                .ok_or_else(|| StorageError::internal("KV GSI image has another prefix"))?,
        )
        .map_err(|_| StorageError::internal("KV GSI image ID is invalid"))?;
        if id > StreamItemId::from(read.through) || previous.is_some_and(|last| id >= last) {
            return Err(StorageError::internal("KV GSI image is unordered"));
        }
        let item = StoredStreamRow::decode(id, &entry.value, GSI_HISTORY_BYTES.get())?;
        item.require_address(physical, id)?;
        previous = Some(id);
        items.push((read.candidate_ordinal, item));
    }
    Ok(items)
}
