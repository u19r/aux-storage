//! Physical item-history boundaries read inside the retention wave's KV
//! attempt.

use std::num::NonZeroU32;

use storage_provider::{
    ItemHistoryRetention, StreamTrimBoundary, StreamTrimScope, StreamTrimScopeBoundaries,
    stream_retention_kind,
};
use storage_types::{StorageError, StorageResult, StreamItemId};

use super::pointer::{decode_first_pointer_reference, pointer_reference_range};
use crate::{
    KvAttempt, KvRange, KvRangePage, ReadProtection, constants::RETENTION_READ_BYTES,
    range::prefix_upper_bound, stream_write::key_layout::item_row_prefix,
};

pub(crate) async fn load_stream_trim_boundaries<A: KvAttempt>(
    attempt: &mut A,
    scopes: &[StreamTrimScope],
    protected_table_floor: Option<StreamItemId>,
    protection: ReadProtection,
) -> StorageResult<Vec<StreamTrimScopeBoundaries>> {
    let mut streams = Vec::with_capacity(scopes.len());
    let mut ranges = Vec::with_capacity(scopes.len() * 2);
    for scope in scopes {
        let stream = stream_retention_kind(scope)?;
        if matches!(stream, stream_provider::StreamRowKind::Table(_)) {
            streams.push(None);
        } else {
            let name = stream.value().clone();
            let prefix = item_row_prefix(&name)?;
            let end = prefix_upper_bound(&prefix)
                .ok_or_else(|| StorageError::internal("KV stream prefix has no upper bound"))?;
            ranges.push(KvRange {
                entry_bounds: None,
                begin: prefix.clone(),
                end,
                reverse: true,
                limit: NonZeroU32::MIN,
                max_bytes: RETENTION_READ_BYTES,
            });
            ranges.push(pointer_reference_range(&name)?);
            streams.push(Some((name, prefix)));
        }
    }
    let expected = ranges.len();
    // Only key order matters for latest IDs; do not hydrate full stream items
    // merely to read their keys. Pointer rows are checked from their headers.
    let pages = attempt.native_scan_batch(ranges, protection).await?;
    if pages.len() != expected {
        return Err(StorageError::internal(
            "KV retention boundary batch count differs",
        ));
    }
    let mut pages = pages.into_iter();
    let mut observations = Vec::with_capacity(scopes.len());
    let mut references = Vec::new();
    for stream in streams {
        let Some((name, prefix)) = stream else {
            observations.push(None);
            continue;
        };
        let latest = pages
            .next()
            .ok_or_else(|| StorageError::internal("KV retention latest page is absent"))?;
        let pointer = pages
            .next()
            .ok_or_else(|| StorageError::internal("KV retention pointer page is absent"))?;
        let referenced = decode_first_pointer_reference(&name, pointer)?;
        if let Some(reference) = referenced {
            let mut through = prefix.clone();
            through.extend_from_slice(reference.as_bytes());
            references.push(KvRange {
                entry_bounds: None,
                begin: prefix.clone(),
                end: prefix_upper_bound(&through).ok_or_else(|| {
                    StorageError::internal("KV referenced history has no upper bound")
                })?,
                reverse: true,
                limit: NonZeroU32::MIN.saturating_add(1),
                max_bytes: RETENTION_READ_BYTES,
            });
        }
        observations.push(Some((
            prefix.clone(),
            ItemHistoryRetention {
                latest: decode_latest_item_id(latest, &prefix)?,
                referenced,
                at_reference: [None; 2],
            },
        )));
    }
    // The reference is a result of the first wave. Fetch its exact image ID
    // and predecessor together, batching all scopes in this dependent wave.
    // Only keys are needed; do not hydrate or decode item payloads.
    let expected = references.len();
    let pages = attempt.native_scan_batch(references, protection).await?;
    if pages.len() != expected {
        return Err(StorageError::internal(
            "KV referenced history batch count differs",
        ));
    }
    let mut pages = pages.into_iter();
    observations
        .into_iter()
        .map(|observation| {
            let Some((prefix, mut observation)) = observation else {
                return Ok(StreamTrimScopeBoundaries {
                    latest_item_id: None,
                    protected_boundary: protected_table_floor
                        .map(|item_id| StreamTrimBoundary { item_id }),
                    retained_table_pointer_boundary: None,
                });
            };
            if observation.referenced.is_some() {
                let page = pages.next().ok_or_else(|| {
                    StorageError::internal("KV referenced history page is absent")
                })?;
                if page.entries.len() > 2 {
                    return Err(StorageError::internal(
                        "KV referenced history page is unbounded",
                    ));
                }
                for (slot, entry) in observation.at_reference.iter_mut().zip(page.entries) {
                    let suffix = entry.key.strip_prefix(prefix.as_slice()).ok_or_else(|| {
                        StorageError::internal("KV referenced history has another prefix")
                    })?;
                    *slot = Some(StreamItemId::try_from(suffix).map_err(|_| {
                        StorageError::internal("KV referenced history ID is invalid")
                    })?);
                }
            }
            StreamTrimScopeBoundaries::for_item_history(observation)
        })
        .collect()
}

fn decode_latest_item_id(page: KvRangePage, prefix: &[u8]) -> StorageResult<Option<StreamItemId>> {
    if page.entries.len() > 1 || page.more && page.entries.is_empty() {
        return Err(StorageError::internal(
            "KV latest stream item is not unique",
        ));
    }
    page.entries
        .into_iter()
        .next()
        .map(|entry| {
            let suffix = entry.key.strip_prefix(prefix).ok_or_else(|| {
                StorageError::internal("KV latest stream item has another prefix")
            })?;
            StreamItemId::try_from(suffix)
                .map_err(|_| StorageError::internal("KV latest stream item ID is invalid"))
        })
        .transpose()
}
