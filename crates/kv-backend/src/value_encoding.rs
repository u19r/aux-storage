//! Consuming value preparation separates operand admission from payload copies.

use storage_types::{InfrastructureResultKind, StorageResult};

use crate::constants::{
    KV_CHUNK_BYTES, KV_CHUNK_HEADER_BYTES, KV_MAX_VALUE_BYTES, KV_VALUE_CHUNKED_TAG,
    KV_VALUE_INLINE_TAG,
};

pub(crate) struct PreparedValue {
    value: Vec<u8>,
    encoding: ValueEncoding,
}

enum ValueEncoding {
    Inline,
    Chunked { length: u32, count: u16 },
}

impl PreparedValue {
    pub(crate) fn prepare(value: Vec<u8>, native_limit: usize) -> StorageResult<Self> {
        let encoding = ValueEncoding::prepare(value.len(), native_limit)?;
        Ok(Self { value, encoding })
    }

    pub(crate) fn prepare_inline(value: Vec<u8>, native_limit: usize) -> StorageResult<Self> {
        let prepared = Self::prepare(value, native_limit)?;
        if !matches!(prepared.encoding, ValueEncoding::Inline) {
            return Err(capacity_rejected());
        }
        Ok(prepared)
    }

    pub(crate) fn chunk_count(&self) -> usize {
        match self.encoding {
            ValueEncoding::Inline => 0,
            ValueEncoding::Chunked { count, .. } => usize::from(count),
        }
    }

    pub(crate) fn encode(mut self) -> StorageResult<(Vec<u8>, Vec<Vec<u8>>)> {
        match self.encoding {
            ValueEncoding::Inline => {
                self.value
                    .try_reserve_exact(1)
                    .map_err(|_| capacity_rejected())?;
                self.value.push(KV_VALUE_INLINE_TAG);
                Ok((self.value, Vec::new()))
            }
            ValueEncoding::Chunked { length, count } => {
                let mut head = Vec::new();
                head.try_reserve_exact(KV_CHUNK_HEADER_BYTES)
                    .map_err(|_| capacity_rejected())?;
                head.extend_from_slice(&length.to_be_bytes());
                head.extend_from_slice(&count.to_be_bytes());
                head.push(KV_VALUE_CHUNKED_TAG);
                let mut chunks = Vec::new();
                chunks
                    .try_reserve_exact(usize::from(count))
                    .map_err(|_| capacity_rejected())?;
                for bytes in self.value.chunks(KV_CHUNK_BYTES) {
                    let mut chunk = Vec::new();
                    chunk
                        .try_reserve_exact(bytes.len())
                        .map_err(|_| capacity_rejected())?;
                    chunk.extend_from_slice(bytes);
                    chunks.push(chunk);
                }
                Ok((head, chunks))
            }
        }
    }
}

impl ValueEncoding {
    fn prepare(length: usize, native_limit: usize) -> StorageResult<Self> {
        if length > KV_MAX_VALUE_BYTES {
            return Err(capacity_rejected());
        }
        if length
            .checked_add(1)
            .is_some_and(|bytes| bytes <= native_limit)
        {
            return Ok(Self::Inline);
        }
        if native_limit < KV_CHUNK_BYTES {
            return Err(capacity_rejected());
        }
        let count =
            u16::try_from(length.div_ceil(KV_CHUNK_BYTES)).map_err(|_| capacity_rejected())?;
        let length = u32::try_from(length).map_err(|_| capacity_rejected())?;
        Ok(Self::Chunked { length, count })
    }
}

fn capacity_rejected() -> storage_types::StorageError {
    InfrastructureResultKind::NativeCapacityRejected.into()
}
