//! Protected record observations, mutations and range hydration share one byte
//! envelope.

use storage_types::{StorageError, StorageResult};

use crate::{
    KvNativeLayout,
    constants::{KV_CHUNK_BYTES, KV_CHUNK_KEY_OVERHEAD},
    value_layout::lowered_put_batch_bytes,
};

/// One protected batch plus one maximal range; unrelated workflow effects
/// remain separate.
pub(crate) fn bound_protected_record_bytes(
    key_bytes: usize,
    limits: infrastructure_provider::limits::InfrastructureLimits,
    page_bytes: std::num::NonZeroUsize,
    layout: KvNativeLayout,
) -> StorageResult<usize> {
    let records = limits.max_records().get();
    let values = records
        .checked_mul(crate::constants::INFRASTRUCTURE_RECORD_HEADER_BYTES)
        .and_then(|headers| limits.max_bytes().get().checked_add(headers))
        .ok_or_else(overflow)?;
    let batch = bound_protected_batch_bytes(key_bytes, values, records, layout)?;
    let Some(metadata) = layout.conflict_ranges.require_metadata()? else {
        return Ok(batch);
    };
    let chunks = values
        .div_ceil(KV_CHUNK_BYTES)
        .checked_add(records - 1)
        .ok_or_else(overflow)?;
    let mutations = records
        .checked_mul(2)
        .and_then(|count| count.checked_add(chunks))
        .ok_or_else(overflow)?;
    let shape = ProtectedRangeShape::try_new(records, mutations, page_bytes.get())?;
    let key = key_bytes
        .checked_add(layout.prefix_bytes)
        .and_then(|bytes| bytes.checked_add(KV_CHUNK_KEY_OVERHEAD))
        .ok_or_else(overflow)?;
    batch
        .checked_add(shape.bound_bytes(key, metadata.get())?)
        .ok_or_else(overflow)
}

struct ProtectedRangeShape {
    chunk_reads: usize,
    maximum_range_intervals: usize,
}

impl ProtectedRangeShape {
    fn try_new(records: usize, mutations: usize, page_bytes: usize) -> StorageResult<Self> {
        let range_reads = records.checked_add(1).ok_or_else(overflow)?;
        let chunk_reads = page_bytes
            .div_ceil(KV_CHUNK_BYTES)
            .checked_add(records)
            .ok_or_else(overflow)?;
        let maximum_range_intervals = mutations
            .checked_mul(2)
            .and_then(|count| count.checked_add(range_reads))
            .ok_or_else(overflow)?;
        Ok(Self {
            chunk_reads,
            maximum_range_intervals,
        })
    }

    fn bound_bytes(&self, key_bytes: usize, metadata: usize) -> StorageResult<usize> {
        // The qualified read-your-writes map coalesces the union of reads;
        // staged mutation endpoints can split that union into more intervals.
        let interval = key_bytes
            .checked_add(1)
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(metadata))
            .ok_or_else(overflow)?;
        let ranges = interval
            .checked_mul(self.maximum_range_intervals)
            .ok_or_else(overflow)?;
        let points = bound_point_conflict_bytes(key_bytes, metadata)?
            .checked_mul(self.chunk_reads)
            .ok_or_else(overflow)?;
        ranges.checked_add(points).ok_or_else(overflow)
    }
}

/// Bounds one protected batch, not unrelated effects sharing its native
/// attempt.
fn bound_protected_batch_bytes(
    key_bytes: usize,
    value_bytes: usize,
    records: usize,
    layout: KvNativeLayout,
) -> StorageResult<usize> {
    let mutations = lowered_put_batch_bytes(key_bytes, value_bytes, records, layout)?
        .checked_add(layout.batch_metadata_bytes)
        .ok_or_else(overflow)?;
    if records == 0 {
        return Ok(mutations);
    }
    let key = key_bytes
        .checked_add(layout.prefix_bytes)
        .ok_or_else(overflow)?;
    let chunks = if layout.value_bytes.is_some() {
        value_bytes
            .div_ceil(KV_CHUNK_BYTES)
            .checked_add(records - 1)
            .ok_or_else(overflow)?
    } else {
        0
    };
    let mutation_count = records
        .checked_mul(2)
        .and_then(|count| count.checked_add(chunks))
        .ok_or_else(overflow)?;
    let metadata = mutation_count
        .checked_mul(layout.mutation_metadata_bytes)
        .ok_or_else(overflow)?;
    // Native point deletion is a clear range, with two head keys instead of
    // the one head key charged by a put. Bound mixed stores/deletes as well.
    let deletion = key
        .checked_add(1)
        .and_then(|bytes| bytes.checked_mul(records))
        .ok_or_else(overflow)?;
    let mut bytes = mutations
        .checked_add(metadata)
        .and_then(|bytes| bytes.checked_add(deletion))
        .ok_or_else(overflow)?;
    if let Some(metadata) = layout.conflict_ranges.require_metadata()? {
        let head = bound_point_conflict_bytes(key, metadata.get())?;
        let chunk = bound_point_conflict_bytes(
            key.checked_add(KV_CHUNK_KEY_OVERHEAD)
                .ok_or_else(overflow)?,
            metadata.get(),
        )?;
        // Protect the complete old observation, the CAS revision prefix and
        // the new write. Old hydration and new lowering each fit the batch's
        // payload allowance, independently; CAS adds one old chunk per record.
        // Do not rely on the native client coalescing repeated conflict ranges.
        let chunk_points = chunks
            .checked_mul(2)
            .and_then(|count| count.checked_add(records))
            .ok_or_else(overflow)?;
        let points = head
            .checked_mul(records)
            .and_then(|bytes| bytes.checked_mul(3))
            .and_then(|bytes| bytes.checked_add(chunk.checked_mul(chunk_points)?))
            .ok_or_else(overflow)?;
        let cleanup = key
            .checked_add(KV_CHUNK_KEY_OVERHEAD - 2)
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(metadata.get()))
            .and_then(|bytes| bytes.checked_mul(records))
            .ok_or_else(overflow)?;
        // Final-owner qualification scans the inverse prefix after edge
        // removal. Its protected range is independent of CAS points and chunk
        // cleanup.
        let scans = key
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(metadata.get()))
            .and_then(|bytes| bytes.checked_mul(records))
            .ok_or_else(overflow)?;
        bytes = bytes
            .checked_add(points)
            .and_then(|bytes| bytes.checked_add(cleanup))
            .and_then(|bytes| bytes.checked_add(scans))
            .ok_or_else(overflow)?;
    }
    Ok(bytes)
}

fn bound_point_conflict_bytes(key_bytes: usize, metadata: usize) -> StorageResult<usize> {
    // A point range ends at key + zero; both endpoint addresses are charged.
    key_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(1))
        .and_then(|bytes| bytes.checked_add(metadata))
        .ok_or_else(overflow)
}

fn overflow() -> StorageError {
    StorageError::internal("protected KV attempt footprint overflowed")
}
