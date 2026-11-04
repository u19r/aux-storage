//! Ordered stream reads on the caller's KV attempt.
use storage_types::{StorageEnum, StorageError, StorageResult, StreamItemId, StreamName};
use stream_provider::{
    StreamItem, StreamPartitioningMode, StreamReadKind, StreamReadRequest, StreamReadResult,
    StreamRowPage, StreamRowReadKind, SystemStreamHistoryRead, SystemStreamHistoryReadPage,
    SystemStreamReadKind,
};

use super::{range, row};
use crate::{
    KvAttempt, KvRangePage, ReadProtection, catalog::KvCatalogAttempt, stream_checkpoint,
    stream_history, stream_partition,
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
    let retired = load_retired_positions(
        attempt,
        &reads
            .iter()
            .map(|read| read.after.clone())
            .collect::<Vec<_>>(),
    )
    .await?;
    pages
        .into_iter()
        .zip(reads)
        .zip(retired)
        .map(|((mut page, read), retired_through)| {
            let more = page.more || page.entries.len() > read.limit as usize;
            page.entries.truncate(read.limit as usize);
            let decoded = page
                .entries
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

pub(crate) async fn load_retired_positions<A: KvAttempt>(
    attempt: &A,
    origins: &[storage_types::SystemStreamPosition],
) -> StorageResult<Vec<Option<storage_types::SystemStreamPosition>>> {
    let keys = origins
        .iter()
        .map(crate::stream_write::key_layout::retired_key)
        .collect::<Vec<_>>();
    let values = if keys.is_empty() {
        Vec::new()
    } else {
        attempt.get_batch(keys, ReadProtection::Conflict).await?
    };
    if values.len() != origins.len() {
        return Err(StorageError::internal(
            "Source retirement observation count differs",
        ));
    }
    origins
        .iter()
        .zip(values)
        .map(|(origin, value)| {
            value
                .map(|bytes| {
                    if origin.offset().versionstamp().is_some() {
                        let stamp = bytes.as_slice().try_into().map_err(|_| {
                            StorageError::internal("Invalid source retirement stamp")
                        })?;
                        origin.with_versionstamp(stamp).map_err(|_| {
                            StorageError::internal("Invalid source retirement position")
                        })
                    } else {
                        let bytes = <[u8; 8]>::try_from(bytes).map_err(|_| {
                            StorageError::internal("Invalid source retirement offset")
                        })?;
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
        .collect()
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
    let row = row::decode_materialized_row(&prefix, crate::KvEntry::new(key, value))?;
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
        .map(|entry| row::decode_materialized_row(prefix, entry))
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(StreamRowPage {
        rows,
        more: page.more,
    })
}
