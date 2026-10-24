//! One logical value layout for local and distributed KV drivers.

use storage_types::{StorageError, StorageResult};

use crate::{
    KvAttempt, KvEntry, KvMutation, KvRangePage, ReadProtection,
    constants::{
        KV_CHUNK_BYTES, KV_CHUNK_HEADER_BYTES, KV_CHUNK_KEY_FAMILY, KV_CHUNK_KEY_OVERHEAD,
        KV_MAX_VALUE_BYTES, KV_VALUE_CHUNKED_TAG, KV_VALUE_INLINE_TAG,
    },
    value_hydration::resolve_values,
};

pub(crate) enum ValueHeader<'a> {
    Inline(&'a [u8]),
    Chunked { length: usize, count: usize },
}

pub(crate) fn logical_value_len(raw: &[u8]) -> StorageResult<usize> {
    match parse_header(raw)? {
        ValueHeader::Inline(value) => Ok(value.len()),
        ValueHeader::Chunked { length, .. } => Ok(length),
    }
}

/// Decode a physical family that never stores chunks, before any chunk I/O.
pub(crate) fn decode_unchunked_value(raw: &[u8], maximum: usize) -> StorageResult<&[u8]> {
    match parse_header(raw)? {
        ValueHeader::Inline(value) if value.len() <= maximum => Ok(value),
        _ => Err(StorageError::internal("Invalid unchunked KV value")),
    }
}

/// Bound a batch's shared payload allowance and per-record lowering overhead.
pub(crate) fn lowered_put_batch_bytes(
    key_bytes: usize,
    value_bytes: usize,
    records: usize,
    layout: crate::KvNativeLayout,
) -> StorageResult<usize> {
    let invalid = || StorageError::internal("KV mutation batch bound overflowed");
    let key = key_bytes
        .checked_add(layout.prefix_bytes)
        .ok_or_else(invalid)?;
    let mut overhead = key.checked_add(KV_CHUNK_HEADER_BYTES).ok_or_else(invalid)?;
    let mut bytes = value_bytes;
    if layout.value_bytes.is_some() {
        let range = key
            .checked_add(KV_CHUNK_KEY_OVERHEAD - 2)
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or_else(invalid)?;
        overhead = overhead.checked_add(range).ok_or_else(invalid)?;
        // Sum ceil(each / chunk) is at most ceil(total / chunk) + count - 1.
        // Charge every record's possible partial chunk without repeating
        // payloads.
        let chunks = value_bytes
            .div_ceil(KV_CHUNK_BYTES)
            .checked_add(records.saturating_sub(1))
            .ok_or_else(invalid)?;
        let chunk_keys = key
            .checked_add(KV_CHUNK_KEY_OVERHEAD)
            .and_then(|bytes| bytes.checked_mul(chunks))
            .ok_or_else(invalid)?;
        bytes = bytes.checked_add(chunk_keys).ok_or_else(invalid)?;
    }
    bytes
        .checked_add(overhead.checked_mul(records).ok_or_else(invalid)?)
        .ok_or_else(invalid)
}

pub(crate) fn parse_header(raw: &[u8]) -> StorageResult<ValueHeader<'_>> {
    match raw.last().copied() {
        Some(KV_VALUE_INLINE_TAG) if raw.len() - 1 <= KV_MAX_VALUE_BYTES => {
            Ok(ValueHeader::Inline(&raw[..raw.len() - 1]))
        }
        Some(KV_VALUE_CHUNKED_TAG) if raw.len() == KV_CHUNK_HEADER_BYTES => {
            let length = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
            let count = u16::from_be_bytes([raw[4], raw[5]]) as usize;
            if !valid_chunk_header(length, count) {
                return Err(StorageError::internal("KV chunk header is invalid"));
            }
            Ok(ValueHeader::Chunked { length, count })
        }
        _ => Err(StorageError::internal("KV value has an invalid layout tag")),
    }
}

fn valid_chunk_header(length: usize, count: usize) -> bool {
    length > 0 && length <= KV_MAX_VALUE_BYTES && count == length.div_ceil(KV_CHUNK_BYTES)
}

pub(crate) fn require_chunk_length(
    chunk: &[u8],
    length: usize,
    number: usize,
) -> StorageResult<()> {
    let expected = length
        .saturating_sub(number.saturating_mul(KV_CHUNK_BYTES))
        .min(KV_CHUNK_BYTES);
    if chunk.len() != expected || expected == 0 {
        return Err(StorageError::internal(
            "KV chunk length differs from its layout",
        ));
    }
    Ok(())
}

/// A checked logical address with its maintained chunk-prefix length proof.
pub(crate) struct ChunkAddress<K> {
    key: K,
    length: u16,
}

impl<K: AsRef<[u8]>> ChunkAddress<K> {
    pub(crate) fn prepare(key: K) -> StorageResult<Self> {
        let length = u16::try_from(key.as_ref().len())
            .map_err(|_| StorageError::internal("KV value key exceeds chunk layout"))?;
        Ok(Self { key, length })
    }

    pub(crate) fn chunk_key(&self, number: usize) -> StorageResult<Vec<u8>> {
        encode_chunk_key(self.key.as_ref(), self.length, number)
    }

    pub(crate) fn clear_range(&self) -> StorageResult<KvMutation> {
        let begin = chunk_prefix(self.key.as_ref(), self.length)?;
        let mut end = Vec::new();
        end.try_reserve_exact(begin.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        end.extend_from_slice(&begin);
        while let Some(byte) = end.pop() {
            if byte != u8::MAX {
                end.push(byte + 1);
                return Ok(KvMutation::DeleteRange { begin, end });
            }
        }
        Err(StorageError::internal("KV chunk range has no upper bound"))
    }

    pub(crate) fn into_key(self) -> K {
        self.key
    }
}

fn encode_chunk_key(key: &[u8], length: u16, number: usize) -> StorageResult<Vec<u8>> {
    let number = u16::try_from(number)
        .map_err(|_| StorageError::internal("KV value has too many chunks"))?;
    let mut physical = chunk_prefix(key, length)?;
    physical.extend_from_slice(&number.to_be_bytes());
    Ok(physical)
}

pub(crate) fn chunk_prefix(key: &[u8], key_length: u16) -> StorageResult<Vec<u8>> {
    let bytes = key
        .len()
        .checked_add(KV_CHUNK_KEY_OVERHEAD)
        .ok_or_else(|| StorageError::internal("KV chunk address length overflowed"))?;
    let mut prefix = Vec::new();
    prefix
        .try_reserve_exact(bytes)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    prefix.extend_from_slice(KV_CHUNK_KEY_FAMILY);
    prefix.extend_from_slice(&key_length.to_be_bytes());
    prefix.extend_from_slice(key);
    Ok(prefix)
}

pub(crate) async fn load_value_heads<A: KvAttempt + ?Sized>(
    attempt: &A,
    keys: &[Vec<u8>],
    protection: ReadProtection,
    allowance: crate::KvValueAllowance,
) -> StorageResult<Vec<Option<Vec<u8>>>> {
    #[cfg(feature = "fault-injection")]
    if let Some(error) = attempt.fault_probe().and_then(|probe| {
        probe.fail(crate::KvFaultCall {
            operation: crate::KvFaultOperation::GetBatch,
        })
    }) {
        return Err(error);
    }
    let raw = attempt
        .native_get_batch(keys, protection, allowance)
        .await?;
    #[cfg(feature = "fault-injection")]
    let raw = if let Some(probe) = attempt.fault_probe() {
        probe.alter_get_batch(
            crate::KvFaultCall {
                operation: crate::KvFaultOperation::GetBatch,
            },
            raw,
        )
    } else {
        raw
    };
    if raw.len() != keys.len() {
        return Err(StorageError::internal(
            "native KV batch returned the wrong count",
        ));
    }
    Ok(raw)
}

pub(crate) enum PageValueLayout {
    Chunkable,
    Unchunked(std::num::NonZeroUsize),
}

pub(crate) async fn load_page<A: KvAttempt + ?Sized>(
    attempt: &A,
    range: crate::KvRange,
    protection: ReadProtection,
    layout: PageValueLayout,
) -> StorageResult<KvRangePage> {
    let result = async {
        #[cfg(feature = "fault-injection")]
        if let Some(error) = attempt.fault_probe().and_then(|probe| {
            probe.fail(crate::KvFaultCall {
                operation: crate::KvFaultOperation::Scan,
            })
        }) {
            return Err(error);
        }
        let maximum = range.max_bytes.get();
        let page = attempt.native_scan(range, protection).await?;
        match layout {
            PageValueLayout::Chunkable => resolve_page(attempt, page, maximum, protection).await,
            PageValueLayout::Unchunked(bound) => {
                let entries = page
                    .entries
                    .into_iter()
                    .map(|mut entry| {
                        decode_unchunked_value(&entry.value, bound.get())?;
                        let _ = entry.value.pop();
                        Ok(entry)
                    })
                    .collect::<StorageResult<Vec<_>>>()?;
                Ok(KvRangePage {
                    entries,
                    more: page.more,
                })
            }
        }
    }
    .await;
    result.inspect_err(|error| {
        storage_common::diagnostics::trace_native_failure(
            attempt.backend_engine(),
            storage_common::diagnostics::NativeOperation::KvScan,
            error,
        )
    })
}

pub(crate) async fn resolve_page<A: KvAttempt + ?Sized>(
    attempt: &A,
    page: KvRangePage,
    max_bytes: usize,
    protection: ReadProtection,
) -> StorageResult<KvRangePage> {
    let (page, value_bytes) = trim_page(page, max_bytes)?;
    let raw = page
        .entries
        .into_iter()
        .map(|entry| (entry.key, Some(entry.value)))
        .collect();
    let raw: Vec<(Vec<u8>, Option<Vec<u8>>)> = raw;
    let values = resolve_values(
        attempt,
        raw,
        protection,
        crate::KvValueAllowance::new(KV_MAX_VALUE_BYTES, value_bytes),
    )
    .await?;
    let entries = restore_entries(values.into_iter())?;
    Ok(KvRangePage {
        entries,
        more: page.more,
    })
}

fn trim_page(page: KvRangePage, max_bytes: usize) -> StorageResult<(KvRangePage, usize)> {
    let mut retained = Vec::new();
    let mut used = 0usize;
    let mut value_bytes = 0usize;
    let mut more = page.more;
    for entry in page.entries {
        let length = logical_value_len(&entry.value)?;
        let size = entry
            .key
            .len()
            .checked_add(length)
            .ok_or_else(page_inventory_overflow)?;
        let next = used.checked_add(size).ok_or_else(page_inventory_overflow)?;
        if !retained.is_empty() && next > max_bytes {
            more = true;
            break;
        }
        used = next;
        value_bytes = value_bytes
            .checked_add(length)
            .ok_or_else(page_inventory_overflow)?;
        retained.push(entry);
    }
    Ok((
        KvRangePage {
            entries: retained,
            more,
        },
        value_bytes,
    ))
}

/// Qualify every retained page before any shared chunk hydration request.
pub(crate) async fn resolve_pages<A: KvAttempt + ?Sized>(
    attempt: &A,
    pages: Vec<KvRangePage>,
    budgets: Vec<usize>,
    protection: ReadProtection,
) -> StorageResult<Vec<KvRangePage>> {
    if pages.len() != budgets.len() {
        return Err(page_inventory_overflow());
    }
    let mut shapes = Vec::with_capacity(pages.len());
    let mut raw = Vec::new();
    let mut value_bytes = 0usize;
    for (page, budget) in pages.into_iter().zip(budgets) {
        let (page, bytes) = trim_page(page, budget)?;
        value_bytes = value_bytes
            .checked_add(bytes)
            .ok_or_else(page_inventory_overflow)?;
        shapes.push((page.entries.len(), page.more));
        raw.extend(
            page.entries
                .into_iter()
                .map(|entry| (entry.key, Some(entry.value))),
        );
    }
    let values = resolve_values(
        attempt,
        raw,
        protection,
        crate::KvValueAllowance::new(KV_MAX_VALUE_BYTES, value_bytes),
    )
    .await?;
    restore_pages(values, shapes)
}

fn restore_pages(
    values: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    shapes: Vec<(usize, bool)>,
) -> StorageResult<Vec<KvRangePage>> {
    let count = shapes.iter().try_fold(0usize, |count, (entries, _)| {
        count
            .checked_add(*entries)
            .ok_or_else(page_inventory_overflow)
    })?;
    if count != values.len() {
        return Err(page_inventory_overflow());
    }
    let mut values = values.into_iter();
    shapes
        .into_iter()
        .map(|(count, more)| {
            let entries = restore_entries(values.by_ref().take(count))?;
            Ok(KvRangePage { entries, more })
        })
        .collect()
}

fn page_inventory_overflow() -> StorageError {
    StorageError::internal("KV range hydration inventory overflowed")
}

fn restore_entries(
    values: impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
) -> StorageResult<Vec<KvEntry>> {
    values
        .map(|(key, value)| {
            value
                .map(|value| KvEntry::new(key, value))
                .ok_or_else(|| StorageError::internal("KV range value disappeared"))
        })
        .collect()
}
