//! Batched qualified table-index ranges on the caller's KV attempt.

use storage_types::{StorageError, StorageResult};
use stream_provider::{TableHistoryIndexAttempt, TableHistoryIndexPage, TableHistoryIndexRead};

use crate::{KvAttempt, KvRange, KvRangePage, catalog::KvCatalogAttempt};

impl<A: KvAttempt> TableHistoryIndexAttempt for KvCatalogAttempt<'_, A> {
    async fn load_table_history_boundaries(
        &mut self,
        reads: &[stream_provider::TableHistoryBoundaryRead],
    ) -> StorageResult<storage_types::Exact<Option<storage_types::SystemStreamPosition>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let stamped = matches!(
                catalog.attempt.backend_engine(),
                storage_common::diagnostics::NativeEngine::FoundationDb
            );
            let ranges = reads
                .iter()
                .map(|read| {
                    let index_read = TableHistoryIndexRead {
                        lane: read.lane.clone(),
                        exclusive_start: None,
                        through: read.through,
                        direction: read.direction,
                        limit: std::num::NonZeroU32::MIN,
                    };
                    let mut range = prepare_index_range(&index_read, stamped)?;
                    if let Some((_, range)) = &mut range {
                        range.limit = std::num::NonZeroU32::MIN;
                    }
                    Ok(range)
                })
                .collect::<StorageResult<Vec<_>>>()?;
            // The native head key contains the complete address. Do not hydrate
            // its canonical value chunks merely to discover this boundary.
            let pages = catalog
                .attempt
                .native_scan_batch_exact(
                    ranges
                        .iter()
                        .filter_map(|range| range.as_ref().map(|(_, range)| range.clone()))
                        .collect(),
                    catalog.protection,
                )
                .await?;
            let mut pages = pages.into_iter();
            ranges
                .into_iter()
                .zip(reads)
                .map(|(range, read)| {
                    let Some((prefix, _)) = range else {
                        return Ok(None);
                    };
                    let mut page = pages.next().ok_or_else(invalid_index_range)?;
                    if page.entries.len() > 1 || page.more && page.entries.is_empty() {
                        return Err(invalid_index_range());
                    }
                    page.entries
                        .pop()
                        .map(|entry| {
                            decode_index_position(&read.lane, stamped, &prefix, &entry.key)
                        })
                        .transpose()
                })
                .collect::<StorageResult<Vec<_>>>()
                .and_then(|positions| storage_types::Exact::try_new(reads.len(), positions))
        }
        .await;
        operation.complete(result)
    }

    async fn load_table_history_observations(
        &mut self,
        reads: &[TableHistoryIndexRead],
    ) -> StorageResult<storage_types::Exact<stream_provider::TableHistoryObservationPage>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let stamped = matches!(
                catalog.attempt.backend_engine(),
                storage_common::diagnostics::NativeEngine::FoundationDb
            );
            let ranges = reads
                .iter()
                .map(|read| prepare_index_range(read, stamped))
                .collect::<StorageResult<Vec<_>>>()?;
            let requested = ranges
                .iter()
                .filter_map(|range| range.as_ref().map(|(_, range)| range.clone()))
                .collect();
            let pages = crate::value_layout::load_range_heads(
                catalog.attempt,
                requested,
                catalog.protection,
            )
            .await?;
            let mut pages = pages.into_iter();
            let mut output = Vec::with_capacity(reads.len());
            let mut keys = Vec::new();
            let mut heads = Vec::new();
            for (read, range) in reads.iter().zip(ranges) {
                let mut entries = Vec::new();
                let mut more = false;
                if let Some((prefix, _)) = range {
                    let page = pages.next().ok_or_else(invalid_index_range)?;
                    if page.entries.len() > read.limit.get() as usize + 1
                        || page.more && page.entries.is_empty()
                    {
                        return Err(invalid_index_range());
                    }
                    more = page.more || page.entries.len() > read.limit.get() as usize;
                    for entry in page.entries.into_iter().take(read.limit.get() as usize) {
                        let position =
                            decode_index_position(&read.lane, stamped, &prefix, &entry.key)?;
                        entries.push((read.lane.history_id(), position));
                        keys.push(entry.key);
                        heads.push(Some(entry.value));
                    }
                }
                output.push((entries, more));
            }
            let prefixes = crate::value_prefix::resolve_value_prefixes(
                catalog.attempt,
                &keys,
                heads,
                catalog.protection,
                std::num::NonZeroUsize::new(
                    stream_provider::PositionedStreamEntry::VALUE_PREFIX_BYTES,
                )
                .ok_or_else(invalid_index_range)?,
            )
            .await?;
            let mut prefixes = prefixes.into_iter();
            output
                .into_iter()
                .map(|(positions, more)| {
                    let entries = positions
                        .into_iter()
                        .map(|(history_id, position)| {
                            let prefix =
                                prefixes.next().flatten().ok_or_else(invalid_index_range)?;
                            let created_at =
                                stream_provider::PositionedStreamEntry::decode_created_at(
                                    &prefix.bytes,
                                )?;
                            Ok(stream_provider::TableHistoryObservation {
                                history_id,
                                position,
                                created_at,
                            })
                        })
                        .collect::<StorageResult<Vec<_>>>()?;
                    let last_position = entries.last().map(|entry| entry.position.clone());
                    Ok(stream_provider::TableHistoryObservationPage {
                        entries,
                        more,
                        last_position,
                    })
                })
                .collect::<StorageResult<Vec<_>>>()
                .and_then(|pages| storage_types::Exact::try_new(reads.len(), pages))
        }
        .await;
        operation.complete(result)
    }

    async fn load_table_history_rows(
        &mut self,
        reads: &[TableHistoryIndexRead],
    ) -> StorageResult<storage_types::Exact<TableHistoryIndexPage>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let ranges = reads
                .iter()
                .map(|read| {
                    prepare_index_range(
                        read,
                        matches!(
                            catalog.attempt.backend_engine(),
                            storage_common::diagnostics::NativeEngine::FoundationDb
                        ),
                    )
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let requested = ranges
                .iter()
                .filter_map(|range| range.as_ref().map(|(_, range)| range.clone()))
                .collect::<Vec<_>>();
            let pages = catalog
                .attempt
                .scan_batch_exact(requested, catalog.protection)
                .await?;
            let mut pages = pages.into_iter();
            ranges
                .into_iter()
                .zip(reads)
                .map(|(range, read)| match range {
                    Some((prefix, _)) => decode_index_page(
                        read,
                        matches!(
                            catalog.attempt.backend_engine(),
                            storage_common::diagnostics::NativeEngine::FoundationDb
                        ),
                        &prefix,
                        pages.next().ok_or_else(invalid_index_range)?,
                    ),
                    None => Ok(TableHistoryIndexPage {
                        entries: Vec::new(),
                        last_position: None,
                        more: false,
                    }),
                })
                .collect::<StorageResult<Vec<_>>>()
                .and_then(|pages| storage_types::Exact::try_new(reads.len(), pages))
        }
        .await;
        operation.complete(result)
    }
}

fn lane_position(
    read: &TableHistoryIndexRead,
) -> StorageResult<storage_types::SystemStreamPosition> {
    storage_types::SystemStreamPosition::try_new(
        read.lane.shard_id().as_str(),
        read.lane.lineage_id().as_str(),
        "0",
    )
    .map_err(|_| invalid_index_range())
}

fn sequence_suffix(sequence: storage_types::TableStreamSequence, versionstamped: bool) -> Vec<u8> {
    let key = sequence.native_key();
    if versionstamped {
        return key.as_bytes().to_vec();
    }
    sequence_order(sequence).to_be_bytes().to_vec()
}

fn sequence_order(sequence: storage_types::TableStreamSequence) -> u64 {
    let [_, _, _, _, order @ ..] = *sequence.native_key().as_bytes();
    u64::from_be_bytes(order)
}

fn prepare_index_range(
    read: &TableHistoryIndexRead,
    stamped: bool,
) -> StorageResult<Option<(Vec<u8>, KvRange)>> {
    let position = lane_position(read)?;
    let prefix = crate::stream_write::key_layout::entry_prefix(read.lane.history_id(), &position)?;
    let mut begin = prefix.clone();
    let mut end = prefix.clone();
    if stamped {
        end.extend(sequence_suffix(read.through, true));
        end.push(0);
    } else {
        let order = sequence_order(read.through);
        if let Some(next) = order.checked_add(1) {
            end.extend(next.to_be_bytes());
        } else {
            end = crate::range::prefix_upper_bound(&prefix).ok_or_else(invalid_index_range)?;
        }
    }
    if let Some(start) = read.exclusive_start {
        match read.direction {
            stream_provider::ReadDirection::Forward => {
                if stamped {
                    begin.extend(sequence_suffix(start, true));
                    begin.push(0);
                } else {
                    let order = sequence_order(start);
                    let Some(next) = order.checked_add(1) else {
                        return Ok(None);
                    };
                    begin.extend(next.to_be_bytes());
                }
            }
            stream_provider::ReadDirection::Backward => {
                let mut upper = prefix.clone();
                upper.extend(sequence_suffix(start, stamped));
                end = end.min(upper);
            }
        }
    }
    if begin >= end {
        return Ok(None);
    }
    Ok(Some((
        prefix,
        KvRange {
            begin,
            end,
            reverse: matches!(read.direction, stream_provider::ReadDirection::Backward),
            limit: read
                .limit
                .get()
                .checked_add(1)
                .and_then(std::num::NonZeroU32::new)
                .ok_or_else(invalid_index_range)?,
            max_bytes: crate::constants::STREAM_READ_BYTES,
            entry_bounds: None,
        },
    )))
}

fn decode_index_page(
    read: &TableHistoryIndexRead,
    stamped: bool,
    prefix: &[u8],
    page: KvRangePage,
) -> StorageResult<TableHistoryIndexPage> {
    let mut entries = page
        .entries
        .into_iter()
        .map(|entry| {
            let position = decode_index_position(&read.lane, stamped, prefix, &entry.key)?;
            stream_provider::PositionedStreamEntry::decode_value(
                read.lane.history_id(),
                position,
                &entry.value,
            )
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let more = page.more || entries.len() > read.limit.get() as usize;
    entries.truncate(read.limit.get() as usize);
    let last_position = entries.last().map(|entry| entry.position.clone());
    Ok(TableHistoryIndexPage {
        entries,
        more,
        last_position,
    })
}

fn invalid_index_range() -> StorageError {
    StorageError::internal("Invalid KV table history entry range")
}

fn decode_index_position(
    lane: &storage_types::TableHistoryLane,
    stamped: bool,
    prefix: &[u8],
    key: &[u8],
) -> StorageResult<storage_types::SystemStreamPosition> {
    let suffix = key.strip_prefix(prefix).ok_or_else(invalid_index_range)?;
    let position = if stamped {
        storage_types::SystemStreamPosition::try_new(
            lane.shard_id().as_str(),
            lane.lineage_id().as_str(),
            "0",
        )
        .map_err(|_| invalid_index_range())?
        .with_versionstamp(suffix.try_into().map_err(|_| invalid_index_range())?)
        .map_err(|_| invalid_index_range())?
    } else {
        let (order, text) = suffix.split_at_checked(8).ok_or_else(invalid_index_range)?;
        let offset = std::str::from_utf8(text).map_err(|_| invalid_index_range())?;
        let position = storage_types::SystemStreamPosition::try_new(
            lane.shard_id().as_str(),
            lane.lineage_id().as_str(),
            offset,
        )
        .map_err(|_| invalid_index_range())?;
        if order
            != position
                .offset()
                .order()
                .map_err(|_| invalid_index_range())?
                .to_be_bytes()
        {
            return Err(invalid_index_range());
        }
        position
    };
    Ok(position)
}
