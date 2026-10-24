//! KV bounds and keys for ordered stream pages and point reads.

use std::num::NonZeroU32;

use storage_types::{StorageEnum, StorageError, StorageResult, StreamItemId, StreamName};
use stream_provider::{ReadDirection, StreamReadRequest, SystemStreamHistoryRead};

use crate::{
    KvRange, constants::STREAM_READ_BYTES, range::prefix_upper_bound,
    stream_write::key_layout::item_row_prefix,
};

pub(super) fn read_range(
    stream_name: &StreamName,
    request: &StreamReadRequest,
) -> StorageResult<Option<(Vec<u8>, KvRange)>> {
    let prefix = item_row_prefix(stream_name)?;
    read_range_for_prefix(request, prefix, None, None)
}

pub(crate) fn read_range_for_prefix(
    request: &StreamReadRequest,
    prefix: Vec<u8>,
    opened_after_id: Option<StreamItemId>,
    sealed_after_id: Option<StreamItemId>,
) -> StorageResult<Option<(Vec<u8>, KvRange)>> {
    let mut begin = prefix.clone();
    let mut end = prefix_upper_bound(&prefix)
        .ok_or_else(|| StorageError::internal("KV stream prefix has no upper bound"))?;
    let lower = match request.direction {
        ReadDirection::Forward => request.exclusive_start.max(opened_after_id),
        ReadDirection::Backward => opened_after_id,
    };
    if let Some(lower) = lower {
        begin.extend_from_slice(lower.as_bytes());
        begin.push(0);
    }
    let upper = match (request.direction, request.exclusive_start, sealed_after_id) {
        (ReadDirection::Backward, Some(start), Some(sealed)) if sealed < start => {
            Some((sealed, true))
        }
        (ReadDirection::Backward, Some(start), _) => Some((start, false)),
        (_, _, sealed) => sealed.map(|sealed| (sealed, true)),
    };
    if let Some((upper, inclusive)) = upper {
        end = prefix.clone();
        end.extend_from_slice(upper.as_bytes());
        if inclusive {
            end.push(0);
        }
    }
    if begin >= end {
        return Ok(None);
    }
    let limit = NonZeroU32::new(request.limit.get() + 1)
        .ok_or_else(|| StorageError::internal("stream read limit overflowed"))?;
    Ok(Some((
        prefix,
        KvRange {
            entry_bounds: None,
            begin,
            end,
            reverse: matches!(request.direction, ReadDirection::Backward),
            limit,
            max_bytes: STREAM_READ_BYTES,
        },
    )))
}

pub(super) fn item_key(
    stream_name: &StreamName,
    item_id: StreamItemId,
) -> StorageResult<(Vec<u8>, Vec<u8>)> {
    let prefix = item_row_prefix(stream_name)?;
    let mut key = prefix.clone();
    key.extend_from_slice(item_id.as_bytes());
    Ok((prefix, key))
}

/// Logical history uses lineage offsets rather than ordinary stream item IDs.
pub(super) fn history_range(read: &SystemStreamHistoryRead) -> StorageResult<KvRange> {
    let prefix = crate::stream_write::key_layout::record_prefix(&read.after);
    if let Some(stamp) = read.after.offset().versionstamp() {
        let mut begin = prefix.clone();
        begin.extend_from_slice(&stamp);
        begin.push(0);
        let end = if let Some(version) = read.through.checked_add(1) {
            let mut end = prefix.clone();
            end.extend_from_slice(&version.to_be_bytes());
            end
        } else {
            prefix_upper_bound(&prefix).ok_or(StorageEnum::HistoryUnavailable)?
        };
        return Ok(KvRange {
            entry_bounds: None,
            begin,
            end,
            reverse: false,
            limit: NonZeroU32::new(read.limit).ok_or(StorageEnum::HistoryUnavailable)?,
            max_bytes: crate::constants::SYSTEM_STREAM_PAGE_READ_BYTES,
        });
    }
    let after = read
        .after
        .offset()
        .order()
        .map_err(|_| StorageEnum::HistoryUnavailable)?;
    let first = after
        .checked_add(1)
        .ok_or(StorageEnum::HistoryUnavailable)?;
    let mut begin = prefix.clone();
    begin.extend_from_slice(&first.to_be_bytes());
    let end = if let Some(next) = read.through.checked_add(1) {
        let mut end = prefix.clone();
        end.extend_from_slice(&next.to_be_bytes());
        end
    } else {
        prefix_upper_bound(&prefix).ok_or(StorageEnum::HistoryUnavailable)?
    };
    Ok(KvRange {
        entry_bounds: None,
        begin,
        end,
        reverse: false,
        limit: NonZeroU32::new(read.limit).ok_or(StorageEnum::HistoryUnavailable)?,
        max_bytes: crate::constants::SYSTEM_STREAM_PAGE_READ_BYTES,
    })
}
