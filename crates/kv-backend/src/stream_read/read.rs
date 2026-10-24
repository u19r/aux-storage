//! Ordered stream reads on the caller's KV attempt.
use storage_types::{
    ItemHistoryAddress, StorageEnum, StorageError, StorageResult, StreamItemId, StreamName,
};
use stream_provider::{
    StreamItem, StreamPartitioningMode, StreamReadKind, StreamReadRequest, StreamReadResult,
    StreamRowPage, StreamRowReadKind, SystemStreamHistoryRead, SystemStreamHistoryReadPage,
    SystemStreamHistoryRecord, SystemStreamReadKind, retained_image::stream_row::StoredStreamRow,
};

use super::{range, row};
use crate::{
    KvAttempt, KvRangePage, ReadProtection, catalog::KvCatalogAttempt, stream_checkpoint,
    stream_cursor, stream_history, stream_partition,
};

pub(crate) async fn read_stream<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    request: StreamReadKind<'_>,
) -> StorageResult<StreamReadResult> {
    match request {
        StreamReadKind::System(SystemStreamReadKind::Rows(request))
        | StreamReadKind::Table(request)
        | StreamReadKind::Item(request)
        | StreamReadKind::User(request) => match request.into_parts() {
            (stream, StreamRowReadKind::Page(request)) => {
                read_stream_rows(attempt, protection, stream.value(), request)
                    .await
                    .map(StreamReadResult::Items)
            }
            (
                stream,
                StreamRowReadKind::Point {
                    partitioning_mode,
                    item_id,
                },
            ) => read_stream_item(
                attempt,
                protection,
                stream.value(),
                partitioning_mode,
                item_id,
            )
            .await
            .map(StreamReadResult::Item),
        },
        StreamReadKind::System(SystemStreamReadKind::Catalog) => {
            let history = match protection {
                ReadProtection::Snapshot => {
                    stream_history::read_history_for_catalog(attempt).await?
                }
                ReadProtection::Shared | ReadProtection::Conflict => {
                    stream_history::read_history_protected(attempt).await?.1
                }
            };
            Ok(StreamReadResult::SystemCatalog(history))
        }
        StreamReadKind::System(SystemStreamReadKind::Checkpoint(lookup)) => {
            stream_checkpoint::read_receipt(attempt, lookup)
                .await
                .map(StreamReadResult::SystemCheckpoint)
        }
        StreamReadKind::System(SystemStreamReadKind::Cursors) => {
            stream_cursor::list_system_cursors(attempt, protection)
                .await
                .map(StreamReadResult::SystemCursors)
        }
    }
}

pub(crate) async fn read_system_history_pages<A: KvAttempt>(
    attempt: &mut A,
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
    let pages = attempt.scan_batch(ranges, ReadProtection::Conflict).await?;
    if pages.len() != reads.len() {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    let pending = pages
        .into_iter()
        .zip(reads)
        .map(|(mut page, read)| {
            page.more |= page.entries.len() > read.limit as usize;
            page.entries.truncate(read.limit as usize);
            decode_history_page(page)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let system_stream = StreamName::system_table_stream();
    let mut pointer_keys = pending
        .iter()
        .flat_map(|(_, observations, _)| observations.iter())
        .map(|observation| {
            range::item_key(&system_stream, observation.expected_pointer).map(|(_, key)| key)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let pointer_count = pointer_keys.len();
    let native_reads = reads
        .iter()
        .filter(|read| read.after.offset().versionstamp().is_some())
        .collect::<Vec<_>>();
    pointer_keys.extend(
        native_reads
            .iter()
            .map(|read| crate::stream_write::key_layout::retired_key(&read.after)),
    );
    let values = attempt
        .get_batch(pointer_keys, ReadProtection::Conflict)
        .await?;
    if values.len() != pointer_count + native_reads.len() {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    let mut values = values.into_iter();
    let pointers = values.by_ref().take(pointer_count).collect::<Vec<_>>();
    let mut retired = values;
    let retired = reads
        .iter()
        .map(|read| {
            if read.after.offset().versionstamp().is_none() {
                return Ok(None);
            }
            retired
                .next()
                .ok_or_else(|| StorageError::internal("Native retained frontier is missing"))?
                .map(|bytes| {
                    let stamp = bytes.as_slice().try_into().map_err(|_| {
                        StorageError::internal("Native retained frontier is invalid")
                    })?;
                    read.after
                        .with_versionstamp(stamp)
                        .map_err(|_| StorageError::internal("Native retained position is invalid"))
                })
                .transpose()
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let mut values = pointers.into_iter();
    let pages = pending
        .into_iter()
        .zip(retired)
        .map(|((records, mut observations, more), retired_through)| {
            let pointers = observations
                .iter_mut()
                .map(|observation| {
                    decode_history_pointer(
                        &system_stream,
                        observation,
                        values.next().ok_or_else(|| {
                            StorageError::internal("history pointer observation is missing")
                        })?,
                    )
                })
                .collect::<StorageResult<Vec<_>>>()?;
            Ok((records, observations, pointers, more, retired_through))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let mut pending = Vec::with_capacity(pages.len());
    let mut groups = Vec::with_capacity(pages.len());
    let mut pointer_groups = Vec::with_capacity(pages.len());
    for (records, mut observations, pointers, more, retired_through) in pages {
        let (records, pointers, excluded) = stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::select_pointer_prefix(records, pointers, crate::constants::STREAM_READ_BYTES.get())?;
        observations.truncate(records.len());
        groups.push(records);
        pointer_groups.push(pointers);
        pending.push((observations, more || excluded, retired_through));
    }
    let mut catalog = KvCatalogAttempt::new(attempt, ReadProtection::Conflict);
    let hydrated = stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::hydrate_history_groups(&mut catalog, groups, pointer_groups, crate::constants::STREAM_READ_BYTES.get()).await?;
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

pub(crate) async fn read_item_history_pages<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    reads: &[(ItemHistoryAddress, StreamReadRequest)],
) -> StorageResult<Vec<StreamRowPage>> {
    let ranges = reads
        .iter()
        .map(|(history, read)| range::read_range(&history.physical_stream_name(), read))
        .collect::<StorageResult<Vec<_>>>()?;
    let mut physical = Vec::with_capacity(reads.len());
    for range in ranges {
        let Some((prefix, range)) = range else {
            return Err(StorageError::internal("item history read has empty range"));
        };
        physical.push((prefix, range));
    }
    let pages = attempt
        .scan_batch(
            physical.iter().map(|(_, range)| range.clone()).collect(),
            protection,
        )
        .await?;
    if pages.len() != reads.len() {
        return Err(StorageError::internal(
            "KV item history batch count differs",
        ));
    }
    let pending = pages
        .into_iter()
        .zip(physical)
        .map(|(page, (prefix, _))| decode_stored_page(&prefix, page))
        .collect::<StorageResult<Vec<_>>>()?;
    let (groups, more): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
    let mut catalog = KvCatalogAttempt::new(attempt, protection);
    let hydrated = StoredStreamRow::hydrate_groups(
        &mut catalog,
        groups,
        crate::constants::STREAM_READ_BYTES.get(),
    )
    .await?;
    Ok(hydrated
        .into_iter()
        .zip(more)
        .map(|(rows, more)| StreamRowPage { rows, more })
        .collect())
}

fn decode_history_page(
    page: KvRangePage,
) -> StorageResult<(
    Vec<stream_provider::retained_image::system_stream::RetainedSystemStreamRecord>,
    Vec<SystemStreamHistoryRecord>,
    bool,
)> {
    let mut records = Vec::with_capacity(page.entries.len());
    let mut observations = Vec::with_capacity(page.entries.len());
    for entry in page.entries {
        let (record, observation) = row::decode_history_row(entry)?;
        records.push(record);
        observations.push(observation);
    }
    let (records, excluded) =
        stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::select_prefix(
            records,
            crate::constants::STREAM_READ_BYTES.get(),
        )?;
    observations.truncate(records.len());
    Ok((records, observations, page.more || excluded))
}

fn decode_history_pointer(
    stream: &StreamName,
    observation: &mut SystemStreamHistoryRecord,
    value: Option<Vec<u8>>,
) -> StorageResult<Option<StoredStreamRow>> {
    let Some(bytes) = value else {
        return Ok(None);
    };
    let item = StoredStreamRow::decode(
        observation.expected_pointer,
        &bytes,
        crate::constants::STREAM_READ_BYTES.get(),
    )?;
    item.require_address(stream, observation.expected_pointer)?;
    observation.actual_pointer = Some(item.metadata().0);
    Ok(Some(item))
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
    decode_row_page(attempt, protection, &prefix, page).await
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
    let row = row::decode_stored_stream_row(&prefix, crate::KvEntry::new(key, value))?;
    row.require_address(stream_name, item_id)?;
    let mut catalog = KvCatalogAttempt::new(attempt, protection);
    let mut items = StoredStreamRow::hydrate(
        &mut catalog,
        vec![row],
        crate::constants::STREAM_READ_BYTES.get(),
    )
    .await?;
    Ok(items.pop())
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
    decode_row_page(catalog.attempt, catalog.protection, &prefix, page).await
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

async fn decode_row_page<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    prefix: &[u8],
    page: KvRangePage,
) -> StorageResult<StreamRowPage> {
    let (rows, more) = decode_stored_page(prefix, page)?;
    let mut catalog = KvCatalogAttempt::new(attempt, protection);
    let rows = StoredStreamRow::hydrate(
        &mut catalog,
        rows,
        crate::constants::STREAM_READ_BYTES.get(),
    )
    .await?;
    Ok(StreamRowPage { rows, more })
}

fn decode_stored_page(
    prefix: &[u8],
    page: KvRangePage,
) -> StorageResult<(Vec<StoredStreamRow>, bool)> {
    let rows = page
        .entries
        .into_iter()
        .map(|entry| row::decode_stored_stream_row(prefix, entry))
        .collect::<StorageResult<Vec<_>>>()?;
    let (rows, excluded) =
        StoredStreamRow::select_prefix(rows, crate::constants::STREAM_READ_BYTES.get())?;
    Ok((rows, page.more || excluded))
}
