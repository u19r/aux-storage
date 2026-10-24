//! Own complete native addresses before issuing any request from an inventory.
use storage_types::{InfrastructureResultKind, StorageError, StorageResult};

use super::input_limits::NativeInputLimits;
use crate::{KvRange, constants::KV_VERSIONSTAMP_PLACEHOLDER};

pub(super) struct PreparedRange(pub(super) KvRange);

impl NativeInputLimits {
    pub(super) fn prepare_points(&self, keys: &[Vec<u8>]) -> StorageResult<Vec<Vec<u8>>> {
        for key in keys {
            self.point(key)?;
        }
        let mut physical = Vec::new();
        physical
            .try_reserve_exact(keys.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        for key in keys {
            physical.push(self.copy_key(key)?);
        }
        Ok(physical)
    }

    pub(super) fn prepare_bounds(
        &self,
        begin: &[u8],
        end: &[u8],
    ) -> StorageResult<(Vec<u8>, Vec<u8>)> {
        self.range(begin, end)?;
        Ok((self.copy_key(begin)?, self.copy_key(end)?))
    }

    pub(super) fn prepare_range(&self, mut range: KvRange) -> StorageResult<PreparedRange> {
        self.range(&range.begin, &range.end)?;
        range.begin = self.prefix_key(range.begin)?;
        range.end = self.prefix_key(range.end)?;
        Ok(PreparedRange(range))
    }

    pub(super) fn prepare_ranges(&self, ranges: Vec<KvRange>) -> StorageResult<Vec<PreparedRange>> {
        for range in &ranges {
            self.range(&range.begin, &range.end)?;
        }
        let mut physical = Vec::new();
        physical
            .try_reserve_exact(ranges.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        for range in ranges {
            physical.push(self.prepare_range(range)?);
        }
        Ok(physical)
    }

    pub(super) fn prefix_key(&self, mut key: Vec<u8>) -> StorageResult<Vec<u8>> {
        let logical_bytes = key.len();
        let bytes = self
            .prefix
            .len()
            .checked_add(logical_bytes)
            .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
        key.try_reserve_exact(self.prefix.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        key.resize(bytes, 0);
        key.copy_within(..logical_bytes, self.prefix.len());
        key[..self.prefix.len()].copy_from_slice(&self.prefix);
        Ok(key)
    }

    pub(super) fn logical_key<'a>(&self, physical: &'a [u8]) -> StorageResult<&'a [u8]> {
        physical
            .strip_prefix(self.prefix.as_ref())
            .ok_or_else(|| StorageError::internal("FoundationDB key escaped its subspace"))
    }

    fn copy_key(&self, key: &[u8]) -> StorageResult<Vec<u8>> {
        let bytes = self
            .prefix
            .len()
            .checked_add(key.len())
            .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
        let mut physical = Vec::new();
        physical
            .try_reserve_exact(bytes)
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        physical.extend_from_slice(&self.prefix);
        physical.extend_from_slice(key);
        Ok(physical)
    }

    pub(super) fn prepare_stamped_key(
        &self,
        change: crate::KvVersionstampedMutation,
    ) -> StorageResult<(Vec<u8>, Vec<u8>)> {
        self.versionstamped_key(&change)?;
        let offset = self
            .prefix
            .len()
            .checked_add(change.prefix.len())
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
        let bytes = self
            .prefix
            .len()
            .checked_add(Self::versionstamped_key_bytes(&change)?)
            .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
        let mut key = change.prefix;
        key.try_reserve_exact(bytes - key.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        key = self.prefix_key(key)?;
        key.extend_from_slice(&KV_VERSIONSTAMP_PLACEHOLDER);
        key.extend_from_slice(&change.user_bytes);
        key.extend_from_slice(&change.suffix);
        key.extend_from_slice(&offset.to_le_bytes());
        Ok((key, change.value))
    }
}
