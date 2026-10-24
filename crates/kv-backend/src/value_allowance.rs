//! Physical response lengths are admitted before owned value copies.

use storage_types::{StorageError, StorageResult};

use crate::constants::{KV_CHUNK_BYTES, KV_CHUNK_HEADER_BYTES};

pub struct KvValueAllowance {
    maximum_value_bytes: usize,
    remaining_bytes: usize,
}

impl KvValueAllowance {
    pub fn new(maximum_value_bytes: usize, maximum_batch_bytes: usize) -> Self {
        Self {
            maximum_value_bytes,
            remaining_bytes: maximum_batch_bytes,
        }
    }

    pub(crate) fn for_head_reads(&self, count: usize) -> StorageResult<Self> {
        let logical = count.saturating_mul(self.maximum_value_bytes);
        let framing = count
            .checked_mul(KV_CHUNK_HEADER_BYTES)
            .ok_or_else(invalid_allowance)?;
        let batch = self
            .remaining_bytes
            .min(logical)
            .checked_add(framing)
            .ok_or_else(invalid_allowance)?;
        let scalar = self
            .maximum_value_bytes
            .checked_add(KV_CHUNK_HEADER_BYTES)
            .ok_or_else(invalid_allowance)?;
        Ok(Self::new(scalar, batch))
    }

    pub(crate) fn for_unqualified_values(count: usize) -> Self {
        let maximum = crate::constants::KV_UNQUALIFIED_VALUE_BYTES;
        Self::new(maximum, maximum.saturating_mul(count))
    }

    pub(crate) fn for_chunks(maximum_batch_bytes: usize) -> Self {
        Self::new(KV_CHUNK_BYTES, maximum_batch_bytes)
    }

    pub(crate) fn copy_value(&mut self, value: Option<&[u8]>) -> StorageResult<Option<Vec<u8>>> {
        let Some(value) = value else {
            return Ok(None);
        };
        self.require_value_bytes(value.len())?;
        let mut copied = Vec::new();
        copied
            .try_reserve_exact(value.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        copied.extend_from_slice(value);
        Ok(Some(copied))
    }

    pub(crate) fn require_value_bytes(&mut self, bytes: usize) -> StorageResult<()> {
        if bytes > self.maximum_value_bytes {
            return Err(invalid_allowance());
        }
        self.remaining_bytes = self
            .remaining_bytes
            .checked_sub(bytes)
            .ok_or_else(invalid_allowance)?;
        Ok(())
    }
}

fn invalid_allowance() -> StorageError {
    storage_types::InfrastructureResultKind::NativeCapacityRejected.into()
}
