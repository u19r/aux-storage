//! Complete logical head and physical chunk inventories precede value
//! reconstruction.
use storage_types::{StorageError, StorageResult};

use crate::{
    KvAttempt, ReadProtection,
    value_layout::{ChunkAddress, ValueHeader, parse_header, require_chunk_length},
};

enum ValueSlot {
    Absent,
    Inline(Vec<u8>),
    Chunked { length: usize, count: usize },
}

impl ValueSlot {
    fn decode_bounded(
        raw: Option<Vec<u8>>,
        allowance: &mut crate::KvValueAllowance,
    ) -> StorageResult<Self> {
        let Some(raw) = raw else {
            return Ok(Self::Absent);
        };
        let header = parse_header(&raw)?;
        let length = match &header {
            ValueHeader::Inline(value) => value.len(),
            ValueHeader::Chunked { length, .. } => *length,
        };
        allowance.require_value_bytes(length)?;
        match header {
            ValueHeader::Inline(_) => {
                let mut value = raw;
                let _ = value.pop();
                Ok(Self::Inline(value))
            }
            ValueHeader::Chunked { length, count } => Ok(Self::Chunked { length, count }),
        }
    }
}

struct ChunkedValue {
    slot: usize,
    length: usize,
    chunks: std::ops::Range<usize>,
}

struct PreparedValueHydration {
    values: Vec<ChunkedValue>,
    keys: Vec<Vec<u8>>,
    bytes: usize,
}

impl PreparedValueHydration {
    fn prepare_reads(
        raw: &mut [(Vec<u8>, Option<Vec<u8>>)],
        mut allowance: crate::KvValueAllowance,
    ) -> StorageResult<Self> {
        let mut prepared = Self {
            values: Vec::new(),
            keys: Vec::new(),
            bytes: 0,
        };
        prepared
            .values
            .try_reserve_exact(raw.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (slot, (_, value)) in raw.iter_mut().enumerate() {
            match ValueSlot::decode_bounded(value.take(), &mut allowance)? {
                ValueSlot::Absent => {}
                ValueSlot::Inline(inline) => *value = Some(inline),
                ValueSlot::Chunked { length, count } => {
                    prepared.bytes = prepared
                        .bytes
                        .checked_add(length)
                        .ok_or_else(invalid_chunks)?;
                    let start = prepared.values.last().map_or(0, |value| value.chunks.end);
                    let end = start.checked_add(count).ok_or_else(invalid_chunks)?;
                    prepared.values.push(ChunkedValue {
                        slot,
                        length,
                        chunks: start..end,
                    });
                }
            }
        }
        prepared.prepare_chunk_keys(raw)?;
        Ok(prepared)
    }

    fn prepare_chunk_keys(&mut self, raw: &[(Vec<u8>, Option<Vec<u8>>)]) -> StorageResult<()> {
        let mut addresses = Vec::new();
        addresses
            .try_reserve_exact(self.values.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for value in &self.values {
            addresses.push(ChunkAddress::prepare(raw[value.slot].0.as_slice())?);
        }
        let count = self.values.last().map_or(0, |value| value.chunks.end);
        self.keys
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (value, address) in self.values.iter().zip(addresses) {
            for number in 0..value.chunks.len() {
                self.keys.push(address.chunk_key(number)?);
            }
        }
        Ok(())
    }

    fn validate_chunks(&self, chunks: &[Option<Vec<u8>>]) -> StorageResult<()> {
        if chunks.len() != self.keys.len() {
            return Err(invalid_chunks());
        }
        for value in &self.values {
            for (number, chunk) in chunks[value.chunks.clone()].iter().enumerate() {
                require_chunk_length(
                    chunk.as_deref().ok_or_else(invalid_chunks)?,
                    value.length,
                    number,
                )?;
            }
        }
        Ok(())
    }

    fn hydrate_values(
        self,
        raw: &mut [(Vec<u8>, Option<Vec<u8>>)],
        chunks: Vec<Option<Vec<u8>>>,
    ) -> StorageResult<()> {
        self.validate_chunks(&chunks)?;
        let mut chunks = chunks.into_iter();
        for prepared in self.values {
            let mut value = Vec::new();
            value
                .try_reserve_exact(prepared.length)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for _ in prepared.chunks {
                let chunk = chunks.next().flatten().ok_or_else(invalid_chunks)?;
                value.extend_from_slice(&chunk);
            }
            raw[prepared.slot].1 = Some(value);
        }
        Ok(())
    }
}

pub(crate) async fn resolve_values<A: KvAttempt + ?Sized>(
    attempt: &A,
    mut raw: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    protection: ReadProtection,
    allowance: crate::KvValueAllowance,
) -> StorageResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    let prepared = PreparedValueHydration::prepare_reads(&mut raw, allowance)?;
    if prepared.keys.is_empty() {
        return Ok(raw);
    }
    let chunks = attempt
        .native_get_batch(
            &prepared.keys,
            protection,
            crate::KvValueAllowance::for_chunks(prepared.bytes),
        )
        .await?;
    prepared.hydrate_values(&mut raw, chunks)?;
    Ok(raw)
}

fn invalid_chunks() -> StorageError {
    StorageError::internal("Native KV chunk inventory differs from its prepared value layout")
}
