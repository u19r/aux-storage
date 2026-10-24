//! Bounded physical stream reclamation inside the retention wave's KV attempt.

use std::num::NonZeroU32;

use storage_provider::{
    StreamDurationTrimPageRequest, StreamDurationTrimPageResult, stream_retention_kind,
};
use storage_types::{
    ItemStreamVersion, StorageError, StorageResult, StreamItemId, StreamName, TimestampMillis,
};
use stream_provider::PointerOwner;

use super::pointer::release_pointer_owners;
use crate::{
    KvAttempt, KvMutation, KvRange, KvRangePage, ReadProtection, constants::RETENTION_READ_BYTES,
    range::prefix_upper_bound, stream_read::row::decode_stored_stream_row,
    stream_write::key_layout::item_row_prefix,
};

// Bound outstanding FoundationDB ranges and decoded rows in a large wave.
const TRIM_RANGES_PER_BATCH: usize = 16;

pub(crate) async fn trim_stream_pages<A: KvAttempt>(
    attempt: &mut A,
    requests: Vec<StreamDurationTrimPageRequest>,
) -> StorageResult<Vec<StreamDurationTrimPageResult>> {
    let mut results = Vec::with_capacity(requests.len());
    // One await per bounded group, not per range: scan_batch overlaps its
    // FoundationDB get_range calls. Later groups wait to cap decoded rows.
    for chunk in requests.chunks(TRIM_RANGES_PER_BATCH) {
        results.extend(trim_chunk(attempt, chunk).await?);
    }
    Ok(results)
}

async fn trim_chunk<A: KvAttempt>(
    attempt: &mut A,
    requests: &[StreamDurationTrimPageRequest],
) -> StorageResult<Vec<StreamDurationTrimPageResult>> {
    let mut scopes = Vec::with_capacity(requests.len());
    let mut ranges = Vec::with_capacity(requests.len());
    for request in requests {
        let stream = stream_retention_kind(&request.scope)?;
        let stream_name = stream.value().clone();
        let prefix = item_row_prefix(&stream_name)?;
        ranges.push(page_range(
            &prefix,
            NonZeroU32::new(u32::from(request.page_limit) + 1)
                .ok_or_else(|| StorageError::internal("stream trim page limit is zero"))?,
        )?);
        scopes.push((
            stream_name,
            prefix,
            matches!(stream, stream_provider::StreamRowKind::Table(_)),
        ));
    }

    // FDB issues these independent get_range calls concurrently on one native
    // transaction. Chunked values are hydrated in one follow-up batch.
    let pages = attempt.scan_batch(ranges, ReadProtection::Conflict).await?;
    if pages.len() != requests.len() {
        return Err(StorageError::internal(
            "KV stream trim scan batch count differs",
        ));
    }
    let mut deleted_counts = Vec::with_capacity(requests.len());
    let mut first_remaining = Vec::with_capacity(requests.len());
    let mut missing_first = Vec::new();
    let mut owners = Vec::new();
    let mut mutations = Vec::new();
    let mut retiring_rows = Vec::new();
    for (index, ((request, (stream_name, prefix, table_stream)), page)) in
        requests.iter().zip(&scopes).zip(pages).enumerate()
    {
        if page.more && page.entries.is_empty() {
            return Err(StorageError::internal("KV stream trim page cannot advance"));
        }
        let mut deleted = 0;
        let mut first = None;
        for (row_index, entry) in page.entries.into_iter().enumerate() {
            let key = entry.key.clone();
            let row = decode_stored_stream_row(prefix, entry)?;
            let (item_id, created_at) = row.metadata();
            row.require_address(stream_name, item_id)?;
            if row_index < usize::from(request.page_limit)
                && created_at <= request.cutoff_timestamp
                && request
                    .max_deleted_item_id
                    .is_none_or(|maximum| item_id <= maximum)
            {
                deleted += 1;
                if *table_stream {
                    owners.push((stream_name.clone(), item_id));
                }
                retiring_rows.push(row);
                mutations.push(KvMutation::Delete { key });
            } else if first.is_none() {
                first = Some((item_id, created_at));
            }
        }
        deleted_counts.push(deleted);
        if first.is_none() && page.more {
            missing_first.push(index);
        }
        first_remaining.push(first);
    }

    {
        let mut catalog =
            crate::catalog::KvCatalogAttempt::new(attempt, storage_types::ReadProtection::Conflict);
        let receipts =
            stream_provider::retained_image::stream_row::StoredStreamRow::retirement_receipts(
                &retiring_rows,
            )?;
        stream_provider::retained_image::RetainedOwnerRecord::release_stream_batch(
            &mut catalog,
            receipts,
        )
        .await?;
    }

    // Pointer keys depend on owner values, so these are two ordered batches.
    if !owners.is_empty() {
        mutations.extend(release_pointer_owners(attempt, PointerOwner::Table, &owners).await?);
    }
    if !mutations.is_empty() {
        attempt.stage(mutations)?;
    }

    // A byte-limited page can stop before its lookahead row. Only those
    // scopes need a read after staging, and their ranges run concurrently.
    let first_ranges = missing_first
        .iter()
        .map(|&index| page_range(&scopes[index].1, NonZeroU32::MIN))
        .collect::<StorageResult<Vec<_>>>()?;
    let first_pages = if first_ranges.is_empty() {
        Vec::new()
    } else {
        attempt
            .scan_batch(first_ranges, ReadProtection::Conflict)
            .await?
    };
    if first_pages.len() != missing_first.len() {
        return Err(StorageError::internal(
            "KV first retained stream batch count differs",
        ));
    }
    for (index, page) in missing_first.into_iter().zip(first_pages) {
        let (stream_name, prefix, _) = &scopes[index];
        first_remaining[index] = decode_first_remaining(page, stream_name, prefix)?;
    }
    scopes
        .into_iter()
        .zip(deleted_counts)
        .zip(first_remaining)
        .map(|((_, deleted_rows), first)| {
            Ok(StreamDurationTrimPageResult {
                deleted_rows,
                first_remaining_version: first.as_ref().map(|(id, _)| ItemStreamVersion::from(*id)),
                first_remaining_timestamp: first.map(|(_, timestamp)| timestamp),
            })
        })
        .collect()
}

fn decode_first_remaining(
    page: KvRangePage,
    stream_name: &StreamName,
    prefix: &[u8],
) -> StorageResult<Option<(StreamItemId, TimestampMillis)>> {
    if page.entries.len() > 1 || page.more && page.entries.is_empty() {
        return Err(StorageError::internal(
            "KV first retained stream row is not unique",
        ));
    }
    page.entries
        .into_iter()
        .next()
        .map(|entry| {
            let row = decode_stored_stream_row(prefix, entry)?;
            let (item_id, created_at) = row.metadata();
            row.require_address(stream_name, item_id)?;
            Ok((item_id, created_at))
        })
        .transpose()
}

fn page_range(prefix: &[u8], limit: NonZeroU32) -> StorageResult<KvRange> {
    Ok(KvRange {
        entry_bounds: None,
        begin: prefix.to_vec(),
        end: prefix_upper_bound(prefix)
            .ok_or_else(|| StorageError::internal("KV stream prefix has no upper bound"))?,
        reverse: false,
        limit,
        max_bytes: RETENTION_READ_BYTES,
    })
}
