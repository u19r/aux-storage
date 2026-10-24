//! Native addresses and operands qualify before dispatch, after family
//! lowering.

use std::sync::Arc;

use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageResult};

use crate::{
    KvMutation, KvVersionstampedMutation,
    constants::{
        FOUNDATIONDB_MAX_KEY_BYTES, FOUNDATIONDB_NATIVE_VALUE_BYTES, KV_VERSIONSTAMP_PLACEHOLDER,
    },
};

pub(super) struct NativeInputLimits {
    pub(super) prefix: Arc<[u8]>,
}

impl NativeInputLimits {
    pub(super) fn new(prefix: Arc<[u8]>) -> Self {
        Self { prefix }
    }

    pub(super) fn point(&self, key: &[u8]) -> StorageResult<()> {
        self.address(key.len(), FOUNDATIONDB_MAX_KEY_BYTES)
    }

    pub(super) fn range(&self, begin: &[u8], end: &[u8]) -> StorageResult<()> {
        // A half-open interval can end at key + zero without naming a stored
        // key.
        self.address(begin.len(), FOUNDATIONDB_MAX_KEY_BYTES + 1)?;
        self.address(end.len(), FOUNDATIONDB_MAX_KEY_BYTES + 1)
    }

    fn address(&self, key_bytes: usize, maximum: usize) -> StorageResult<()> {
        if self
            .prefix
            .len()
            .checked_add(key_bytes)
            .is_none_or(|bytes| bytes > maximum)
        {
            return Err(InfrastructureResultKind::NativeCapacityRejected.into());
        }
        Ok(())
    }

    pub(super) fn value(value: &[u8]) -> StorageResult<()> {
        if value.len() > FOUNDATIONDB_NATIVE_VALUE_BYTES {
            return Err(InfrastructureResultKind::NativeCapacityRejected.into());
        }
        Ok(())
    }

    pub(super) fn observed_value(value: &[u8]) -> StorageResult<()> {
        Self::value(value).map_err(|_| invalid_input())
    }

    pub(super) fn mutations(&self, changes: &[KvMutation]) -> StorageResult<()> {
        for change in changes {
            match change {
                KvMutation::Put { key, value } | KvMutation::PutUnchunked { key, value } => {
                    self.point(key)?;
                    Self::value(value)?;
                }
                KvMutation::Delete { key } | KvMutation::DeleteUnchunked { key } => {
                    self.point(key)?
                }
                KvMutation::DeleteRange { begin, end } => self.range(begin, end)?,
            }
        }
        Ok(())
    }

    pub(super) fn versionstamped_key(
        &self,
        change: &KvVersionstampedMutation,
    ) -> StorageResult<()> {
        let length = Self::versionstamped_key_bytes(change)?;
        self.address(length, FOUNDATIONDB_MAX_KEY_BYTES)?;
        Self::value(&change.value)
    }

    pub(super) fn versionstamped_key_bytes(
        change: &KvVersionstampedMutation,
    ) -> StorageResult<usize> {
        let length = change
            .prefix
            .len()
            .checked_add(KV_VERSIONSTAMP_PLACEHOLDER.len())
            .and_then(|bytes| bytes.checked_add(change.user_bytes.len()))
            .and_then(|bytes| bytes.checked_add(change.suffix.len()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<u32>()))
            .ok_or_else(invalid_input)?;
        Ok(length)
    }

    pub(super) fn versionstamped_value(value: &[u8], stamp_offset: u32) -> StorageResult<()> {
        let offset = usize::try_from(stamp_offset).map_err(|_| invalid_input())?;
        if offset
            .checked_add(KV_VERSIONSTAMP_PLACEHOLDER.len())
            .is_none_or(|end| end > value.len())
        {
            return Err(invalid_input());
        }
        if value
            .len()
            .checked_add(std::mem::size_of::<u32>())
            .is_none_or(|bytes| bytes > FOUNDATIONDB_NATIVE_VALUE_BYTES)
        {
            return Err(InfrastructureResultKind::NativeCapacityRejected.into());
        }
        Ok(())
    }

    pub(super) fn row(key: &[u8], value: &[u8], begin: &[u8], end: &[u8]) -> StorageResult<()> {
        if key.len() > FOUNDATIONDB_MAX_KEY_BYTES || key < begin || key >= end {
            return Err(invalid_input());
        }
        Self::observed_value(value)
    }
}

fn invalid_input() -> storage_types::StorageError {
    InfrastructureResultKind::InvalidBound {
        context: InfrastructureContext::NativeCapacity,
    }
    .into()
}
