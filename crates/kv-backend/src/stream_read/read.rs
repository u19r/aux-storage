//! Ordered stream reads on the caller's KV attempt.
use storage_types::{StorageEnum, StorageError, StorageResult, StreamItemId, StreamName};
use stream_provider::{
    StreamItem, StreamPartitioningMode, StreamReadKind, StreamReadRequest, StreamReadResult,
    StreamRowPage, StreamRowReadKind, SystemStreamHistoryRead, SystemStreamHistoryReadPage,
};

use super::{range, row};
use crate::{KvAttempt, KvRangePage, ReadProtection, catalog::KvCatalogAttempt, stream_partition};

pub(crate) async fn read_stream<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    request: StreamReadKind<'_>,
) -> StorageResult<StreamReadResult> {
    match request.read {
        StreamRowReadKind::Page(read) => {
            read_stream_rows(attempt, protection, request.stream, read)
                .await
                .map(StreamReadResult::Items)
        }
        StreamRowReadKind::Point {
            partitioning_mode,
            item_id,
        } => read_stream_item(
            attempt,
            protection,
            request.stream,
            partitioning_mode,
            item_id,
        )
        .await
        .map(StreamReadResult::Item),
    }
}

pub(crate) async fn read_system_history_pages<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    reads: &[SystemStreamHistoryRead],
) -> StorageResult<Vec<SystemStreamHistoryReadPage>> {
    let ranges = reads
        .iter()
        .map(|read| {
            let mut range = range::history_range(read)?;
            range.limit =
                std::num::NonZeroU32::new(read.limit + 1).ok_or(StorageEnum::HistoryUnavailable)?;
            Ok(range)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = attempt.scan_batch(ranges, protection).await?;
    if pages.len() != reads.len() {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    let retired = load_retired_positions(
        attempt,
        protection,
        &reads
            .iter()
            .map(|read| read.after.clone())
            .collect::<Vec<_>>(),
    )
    .await?;
    let mut result = Vec::with_capacity(reads.len());
    let mut addresses = Vec::new();
    let mut keys = Vec::new();
    let mut histories = std::collections::HashSet::new();
    let mut remaining_bytes = crate::constants::SYSTEM_STREAM_PAGE_READ_BYTES.get();
    let mut prefix_complete = true;
    for ((mut page, read), retired_through) in pages.into_iter().zip(reads).zip(retired) {
        let mut more = page.more || page.entries.len() > read.limit as usize;
        page.entries.truncate(read.limit as usize);
        let mut count = 0;
        for entry in page.entries {
            let position = crate::stream_write::record::decode_position(&read.after, &entry.key)?;
            let reference = crate::stream_write::record::FeedReference::decode(&entry.value)?;
            if reference.encoded_length == 0 {
                return Err(StorageEnum::HistoryUnavailable.into());
            }
            // Admit a complete global prefix before any image hydration. Later
            // references remain unread progress even across lineage pages.
            if !prefix_complete
                || (!addresses.is_empty() && reference.encoded_length > remaining_bytes)
            {
                prefix_complete = false;
                more = true;
                continue;
            }
            // The target is soft for one complete first entry: a legal MODIFY
            // can carry two images while SYSTEM publishes only its new image.
            remaining_bytes = remaining_bytes.saturating_sub(reference.encoded_length);
            keys.push(crate::stream_write::key_layout::entry_key(
                reference.history_id,
                &position,
            )?);
            histories.insert(reference.history_id);
            addresses.push((reference, position));
            count += 1;
        }
        result.push((count, more, retired_through));
    }
    // Entry heads and independent incarnation facts share a native batch;
    // chunked canonical values add the existing bounded hydration wave. The
    // combined allowance uses the admitted complete entries plus the separate
    // catalog budget. Native value headers still enforce per-value bounds.
    let value_bytes = addresses.iter().try_fold(
        crate::constants::CATALOG_BYTES.get(),
        |total, (reference, _)| {
            total
                .checked_add(reference.encoded_length)
                .ok_or(StorageEnum::HistoryUnavailable)
        },
    )?;
    let value_bytes =
        std::num::NonZeroUsize::new(value_bytes).ok_or(StorageEnum::HistoryUnavailable)?;
    let mut histories = histories.into_iter().collect::<Vec<_>>();
    histories.sort_by(|left, right| left.as_ref().cmp(right.as_ref()));
    for history in &histories {
        keys.push(crate::catalog::metadata_key(
            crate::catalog::table_id_from_history(*history)?,
        ));
    }
    let mut values = attempt
        .get_batch_bounded(keys, protection, value_bytes)
        .await?;
    if values.len() != addresses.len() + histories.len() {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    let metadata_values = values.split_off(addresses.len());
    if metadata_values
        .iter()
        .flatten()
        .map(Vec::len)
        .sum::<usize>()
        > crate::constants::CATALOG_BYTES.get()
    {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    let states = histories
        .into_iter()
        .zip(metadata_values)
        .map(|(history, bytes)| {
            let state = bytes
                .map(|bytes| crate::catalog::decode_table_history_state(&bytes, history))
                .transpose()?;
            Ok((history, state))
        })
        .collect::<StorageResult<std::collections::HashMap<_, _>>>()?;
    let mut values = values.into_iter();
    let mut addresses = addresses.into_iter();
    let mut pages = Vec::with_capacity(result.len());
    for (count, more, retired_through) in result {
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            let (reference, position) = addresses.next().ok_or(StorageEnum::HistoryUnavailable)?;
            let history = reference.history_id;
            let bytes = values
                .next()
                .flatten()
                .ok_or(StorageEnum::HistoryUnavailable)?;
            let state = states
                .get(&history)
                .ok_or(StorageEnum::HistoryUnavailable)?
                .clone();
            if bytes.len() != reference.encoded_length {
                return Err(StorageEnum::HistoryUnavailable.into());
            }
            let entry =
                stream_provider::PositionedStreamEntry::decode_value(history, position, &bytes)?;
            if entry.entry.created_at() != reference.created_at {
                return Err(StorageEnum::HistoryUnavailable.into());
            }
            records.push((entry, state));
        }
        pages.push(SystemStreamHistoryReadPage {
            records,
            more,
            retired_through,
        });
    }
    Ok(pages)
}

pub(crate) async fn load_retired_positions<A: KvAttempt>(
    attempt: &A,
    protection: ReadProtection,
    origins: &[storage_types::SystemStreamPosition],
) -> StorageResult<storage_types::Exact<Option<storage_types::SystemStreamPosition>>> {
    let keys = origins
        .iter()
        .map(crate::stream_write::key_layout::retired_key)
        .collect::<Vec<_>>();
    let values = attempt.get_batch_exact(keys, protection).await?;
    values.try_zip_map(origins, |origin, value| {
        value
            .map(|bytes| {
                if origin.offset().versionstamp().is_some() {
                    let stamp = bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| StorageError::internal("Invalid source retirement stamp"))?;
                    origin
                        .with_versionstamp(stamp)
                        .map_err(|_| StorageError::internal("Invalid source retirement position"))
                } else {
                    let bytes = <[u8; 8]>::try_from(bytes)
                        .map_err(|_| StorageError::internal("Invalid source retirement offset"))?;
                    storage_types::SystemStreamPosition::try_new(
                        origin.shard_id().as_str(),
                        origin.lineage_id().as_str(),
                        format!("{:020}", u64::from_be_bytes(bytes)),
                    )
                    .map_err(|_| StorageError::internal("Invalid source retirement position"))
                }
            })
            .transpose()
    })
}

async fn read_stream_rows<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    stream_name: &StreamName,
    request: &StreamReadRequest,
) -> StorageResult<StreamRowPage> {
    if request.partitioning_mode == StreamPartitioningMode::KeyOrdered {
        return stream_partition::read::read_stream_rows(attempt, protection, stream_name, request)
            .await;
    }
    let Some((prefix, key_range)) = range::read_range(stream_name, request)? else {
        return Ok(StreamRowPage {
            rows: Vec::new(),
            more: false,
        });
    };
    let page = attempt.scan(key_range, protection).await?;
    decode_row_page(&prefix, page)
}

pub(crate) async fn read_stream_item<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    stream_name: &StreamName,
    partitioning_mode: StreamPartitioningMode,
    item_id: StreamItemId,
) -> StorageResult<Option<StreamItem>> {
    if partitioning_mode == StreamPartitioningMode::KeyOrdered {
        return stream_partition::read::read_stream_item(attempt, protection, stream_name, item_id)
            .await;
    }
    let (prefix, key) = range::item_key(stream_name, item_id)?;
    let Some(value) = attempt.get(&key, protection).await? else {
        return Ok(None);
    };
    let row = row::decode_stream_row(&prefix, crate::KvEntry::new(key, value))?;
    row.require_address(stream_name, item_id)?;
    Ok(Some(row))
}

pub(crate) async fn read_user_page_through<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    stream: &StreamName,
    request: &StreamReadRequest,
    through: StreamItemId,
) -> StorageResult<StreamRowPage> {
    if request.partitioning_mode == StreamPartitioningMode::KeyOrdered {
        return stream_partition::read::read_user_page_through(catalog, stream, request, through)
            .await;
    }
    let prefix = crate::stream_write::key_layout::item_row_prefix(stream)?;
    let Some((prefix, range)) = range::read_range_for_prefix(request, prefix, None, Some(through))?
    else {
        return Ok(StreamRowPage {
            rows: Vec::new(),
            more: false,
        });
    };
    let page = catalog.attempt.scan(range, catalog.protection).await?;
    decode_row_page(&prefix, page)
}

pub(crate) async fn read_user_item_through<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    stream: &StreamName,
    id: StreamItemId,
    through: StreamItemId,
) -> StorageResult<Option<StreamItem>> {
    if id > through {
        return Ok(None);
    }
    read_stream_item(
        catalog.attempt,
        catalog.protection,
        stream,
        StreamPartitioningMode::Single,
        id,
    )
    .await
}

fn decode_row_page(prefix: &[u8], page: KvRangePage) -> StorageResult<StreamRowPage> {
    let rows = page
        .entries
        .into_iter()
        .map(|entry| row::decode_stream_row(prefix, entry))
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(StreamRowPage {
        rows,
        more: page.more,
    })
}
