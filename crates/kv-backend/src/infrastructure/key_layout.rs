//! Layout-1 tuple order preserves raw namespace, resource and suffix ordering.

use std::num::{NonZeroU32, NonZeroUsize};

use infrastructure_provider::{
    limits::InfrastructureLimits,
    page::RecordPageRequest,
    record::{DurableRecordKey, RecordAddress},
};
use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageError, StorageResult};

use crate::{
    KvEntryBounds, KvRange,
    constants::{
        INFRASTRUCTURE_KEY_FRAMING_BYTES, INFRASTRUCTURE_RECORD_HEADER_BYTES,
        INFRASTRUCTURE_RECORD_PREFIX, KV_CHUNK_HEADER_BYTES,
    },
};

pub(super) fn encode_key(key: &DurableRecordKey) -> StorageResult<Vec<u8>> {
    let address = key.as_ref();
    let logical = address
        .namespace
        .len()
        .checked_add(address.resource.len())
        .and_then(|bytes| bytes.checked_add(address.suffix.len()))
        .and_then(|bytes| bytes.checked_add(16))
        .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(encoded_key_length_bound(logical)?)
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    bytes.extend_from_slice(INFRASTRUCTURE_RECORD_PREFIX);
    append_segment(&mut bytes, address.namespace.as_bytes());
    append_segment(&mut bytes, &address.resource);
    bytes.extend_from_slice(&address.generation.to_be_bytes());
    append_segment(&mut bytes, &address.suffix);
    Ok(bytes)
}

pub(super) fn encode_namespace_prefix(namespace: &str) -> StorageResult<Vec<u8>> {
    let size = namespace
        .len()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(INFRASTRUCTURE_RECORD_PREFIX.len() + 2))
        .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    bytes.extend_from_slice(INFRASTRUCTURE_RECORD_PREFIX);
    append_segment(&mut bytes, namespace.as_bytes());
    Ok(bytes)
}

pub(super) fn encoded_key_length_bound(logical_bytes: usize) -> StorageResult<usize> {
    // Logical addresses charge 16 fixed bytes; native keys have generation (8),
    // three terminators (6), and escaping. At least one namespace byte cannot
    // be zero, while resource/suffix bytes may all require escaping.
    logical_bytes
        .checked_sub(16)
        .filter(|bytes| *bytes >= 2)
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_sub(1))
        .and_then(|bytes| {
            bytes.checked_add(INFRASTRUCTURE_RECORD_PREFIX.len() + INFRASTRUCTURE_KEY_FRAMING_BYTES)
        })
        .ok_or_else(|| {
            InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::RecordAddress,
            }
            .into()
        })
}

pub(super) fn decode_key(bytes: &[u8]) -> StorageResult<DurableRecordKey> {
    let mut bytes = bytes
        .strip_prefix(INFRASTRUCTURE_RECORD_PREFIX)
        .ok_or_else(invalid_key)?;
    let namespace = String::from_utf8(take_segment(&mut bytes)?).map_err(|_| invalid_key())?;
    let resource = take_segment(&mut bytes)?;
    let generation = bytes.get(..8).ok_or_else(invalid_key)?;
    let generation = u64::from_be_bytes(generation.try_into().map_err(|_| invalid_key())?);
    bytes = &bytes[8..];
    let suffix = take_segment(&mut bytes)?;
    if !bytes.is_empty() {
        return Err(invalid_key());
    }
    RecordAddress {
        namespace,
        resource,
        generation,
        suffix,
    }
    .try_into()
}

pub(super) fn encode_range(
    request: &RecordPageRequest,
    limits: InfrastructureLimits,
) -> StorageResult<KvRange> {
    let (first, last, count) = request.range.bounds();
    let mut begin = encode_key(first)?;
    if let Some(after) = &request.after {
        begin = encode_key(after)?;
        begin
            .try_reserve(1)
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        begin.push(0);
    }
    let limit = count
        .get()
        .checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
        .and_then(NonZeroU32::new)
        .ok_or(InfrastructureResultKind::InvalidBound {
            context: InfrastructureContext::RecordPage,
        })?;
    let (entry_bounds, max_bytes) = lower_page_bounds(request.max_bytes, limits, limit)?;
    Ok(KvRange {
        entry_bounds: Some(entry_bounds),
        begin,
        end: encode_key(last)?,
        reverse: false,
        limit,
        max_bytes,
    })
}

pub(super) fn lower_page_bounds(
    max_bytes: NonZeroUsize,
    limits: InfrastructureLimits,
    count: NonZeroU32,
) -> StorageResult<(KvEntryBounds, NonZeroUsize)> {
    let invalid = || InfrastructureResultKind::InvalidBound {
        context: InfrastructureContext::RecordPage,
    };
    let key = encoded_key_length_bound(limits.max_key_bytes().get())?;
    let value = limits
        .max_record_bytes()
        .get()
        .checked_add(INFRASTRUCTURE_RECORD_HEADER_BYTES)
        .ok_or_else(invalid)?;
    let row = key
        .checked_add(value)
        .and_then(|bytes| bytes.checked_add(KV_CHUNK_HEADER_BYTES))
        .ok_or_else(invalid)?;
    let framing = INFRASTRUCTURE_RECORD_PREFIX.len()
        + INFRASTRUCTURE_KEY_FRAMING_BYTES
        + INFRASTRUCTURE_RECORD_HEADER_BYTES
        + KV_CHUNK_HEADER_BYTES;
    let budget = max_bytes
        .get()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(framing.checked_mul(count.get() as usize)?))
        .and_then(|bytes| bytes.checked_add(row))
        .and_then(NonZeroUsize::new)
        .ok_or_else(invalid)?;
    let bounds = KvEntryBounds::new(
        NonZeroUsize::new(key).ok_or_else(invalid)?,
        NonZeroUsize::new(value).ok_or_else(invalid)?,
    );
    Ok((bounds, budget))
}

fn append_segment(bytes: &mut Vec<u8>, segment: &[u8]) {
    for byte in segment {
        bytes.push(*byte);
        if *byte == 0 {
            bytes.push(255);
        }
    }
    bytes.extend_from_slice(&[0, 0]);
}

fn take_segment(bytes: &mut &[u8]) -> StorageResult<Vec<u8>> {
    let mut segment = Vec::new();
    loop {
        let (byte, rest) = bytes.split_first().ok_or_else(invalid_key)?;
        *bytes = rest;
        if *byte != 0 {
            segment
                .try_reserve(1)
                .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
            segment.push(*byte);
            continue;
        }
        let (escape, rest) = bytes.split_first().ok_or_else(invalid_key)?;
        *bytes = rest;
        match *escape {
            0 => return Ok(segment),
            255 => {
                segment
                    .try_reserve(1)
                    .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
                segment.push(0);
            }
            _ => return Err(invalid_key()),
        }
    }
}

fn invalid_key() -> StorageError {
    InfrastructureResultKind::Encoding {
        context: InfrastructureContext::RecordAddress,
    }
    .into()
}
