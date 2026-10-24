use std::num::{NonZeroU32, NonZeroUsize};

use bytes::Bytes;
use scheduled_bg_jobs::store::{
    RangeDirection, RangeEntry, RangePage, RangeRequest, StoreCapabilities, StoreKey,
};
use storage_provider::{RawOrderedJobStoreError, job_store::finish_range_page};
use storage_types::StorageError;

use super::{DATA_PREFIX, KvRawOrderedJobStore, data_key};
use crate::{KvAttempt, KvBackend, KvEntry, KvRange, ReadProtection, range::prefix_upper_bound};

pub(super) async fn read<B: KvBackend + 'static>(
    store: &KvRawOrderedJobStore<B>,
    request: RangeRequest,
) -> Result<RangePage, RawOrderedJobStoreError> {
    let physical = physical_range(&request, store.capabilities)?;
    let page = store
        .read(move |attempt| {
            Box::pin(async move { attempt.scan(physical, ReadProtection::Snapshot).await })
        })
        .await?;
    let rows = page
        .entries
        .into_iter()
        .map(|entry| decode_entry(entry, store.capabilities.max_value_bytes().get()));
    finish_range_page(&request, rows, page.more)
}

fn physical_range(
    request: &RangeRequest,
    capabilities: StoreCapabilities,
) -> Result<KvRange, RawOrderedJobStoreError> {
    let mut begin = data_key(request.start().map_or(&[][..], StoreKey::as_bytes))?;
    let mut end = match request.end() {
        Some(key) => data_key(key.as_bytes())?,
        None => prefix_upper_bound(DATA_PREFIX).ok_or_else(|| {
            RawOrderedJobStoreError::Backend(StorageError::internal(
                "scheduled-job prefix has no upper bound",
            ))
        })?,
    };
    let reverse = request.direction() == RangeDirection::Reverse;
    if let Some(cursor) = request.continuation() {
        let cursor = data_key(cursor.as_bytes())?;
        if reverse {
            end = end.min(cursor);
        } else {
            let mut next = cursor;
            next.try_reserve_exact(1).map_err(|_| {
                RawOrderedJobStoreError::Backend(
                    storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
                )
            })?;
            next.push(0);
            begin = begin.max(next);
        }
    }
    let limit = request.item_limit().get().checked_add(1).ok_or_else(|| {
        RawOrderedJobStoreError::Backend(StorageError::internal(
            "scheduled-job range limit overflowed",
        ))
    })?;
    let rejected = || {
        RawOrderedJobStoreError::Backend(
            storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
        )
    };
    let key_bytes = capabilities
        .max_key_bytes()
        .get()
        .checked_add(DATA_PREFIX.len())
        .ok_or_else(rejected)?;
    let max_bytes = request
        .byte_limit()
        .get()
        .checked_add(capabilities.max_value_bytes().get())
        .and_then(|bytes| bytes.checked_add(key_bytes))
        .and_then(|bytes| {
            (DATA_PREFIX.len())
                .checked_mul(limit as usize)
                .and_then(|prefixes| bytes.checked_add(prefixes))
        })
        .ok_or_else(rejected)?;
    Ok(KvRange {
        entry_bounds: Some(crate::KvEntryBounds::new(
            NonZeroUsize::new(key_bytes).ok_or_else(rejected)?,
            capabilities.max_value_bytes(),
        )),
        begin,
        end,
        reverse,
        limit: NonZeroU32::new(limit).ok_or_else(|| {
            RawOrderedJobStoreError::Backend(StorageError::internal(
                "scheduled-job range limit is zero",
            ))
        })?,
        max_bytes: NonZeroUsize::new(max_bytes).ok_or_else(|| {
            RawOrderedJobStoreError::Backend(StorageError::internal(
                "scheduled-job range bytes are zero",
            ))
        })?,
    })
}

fn decode_entry(
    entry: KvEntry,
    max_value_bytes: usize,
) -> Result<RangeEntry, RawOrderedJobStoreError> {
    let key = entry
        .key
        .strip_prefix(DATA_PREFIX)
        .ok_or(RawOrderedJobStoreError::Corrupt("scheduled job range key"))?;
    let key = StoreKey::try_from(key)
        .map_err(|_| RawOrderedJobStoreError::Corrupt("scheduled job range key"))?;
    if entry.value.len() > max_value_bytes {
        return Err(RawOrderedJobStoreError::Corrupt(
            "scheduled job range value bytes",
        ));
    }
    Ok(RangeEntry {
        key,
        value: Bytes::from(entry.value),
    })
}
