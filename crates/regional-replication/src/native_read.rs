//! Plan complete ordered read inventories before issuing bounded native waves.
use std::ops::Range;

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use storage_types::{ReadProtection, StorageResult};

use crate::native_record::{NativeRecords, corrupt_record};

impl NativeRecords {
    pub(crate) fn read_waves<T>(
        &self,
        entries: &[T],
        key: impl for<'a> Fn(&'a T) -> &'a DurableRecordKey,
    ) -> StorageResult<Vec<Range<usize>>> {
        Self::prepare_read_waves(entries, key, self.limits)
    }

    fn prepare_read_waves<T>(
        entries: &[T],
        key: impl for<'a> Fn(&'a T) -> &'a DurableRecordKey,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<Vec<Range<usize>>> {
        let mut waves = Vec::new();
        let mut start = 0;
        let mut bytes = 0usize;
        for (position, entry) in entries.iter().enumerate() {
            let size = limits
                .validate_key(key(entry))?
                .checked_add(limits.max_record_bytes().get())
                .ok_or_else(corrupt_record)?;
            let next = bytes.checked_add(size);
            if position - start == limits.max_records().get()
                || next.is_none_or(|bytes| bytes > limits.max_bytes().get())
            {
                waves
                    .try_reserve(1)
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                waves.push(start..position);
                start = position;
                bytes = size;
            } else {
                bytes = next.ok_or_else(corrupt_record)?;
            }
        }
        if start < entries.len() {
            waves
                .try_reserve(1)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            waves.push(start..entries.len());
        }
        Ok(waves)
    }

    pub(crate) async fn load<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        keys: &[DurableRecordKey],
        protection: ReadProtection,
    ) -> StorageResult<Vec<Option<DurableRecord>>> {
        self.load_bounded(attempt, keys, protection, self.limits.max_record_bytes())
            .await
    }

    pub(crate) async fn load_bounded<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        keys: &[DurableRecordKey],
        protection: ReadProtection,
        maximum: std::num::NonZeroUsize,
    ) -> StorageResult<Vec<Option<DurableRecord>>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if maximum > self.limits.max_record_bytes() {
                return Err(corrupt_record());
            }
            let limits = infrastructure_provider::limits::InfrastructureLimits::try_new(
                self.limits.max_key_bytes(),
                maximum,
                self.limits.max_bytes(),
                self.limits.max_records(),
            )?;
            let waves = Self::prepare_read_waves(keys, |key| key, limits)?;
            let mut rows = Vec::new();
            rows.try_reserve_exact(keys.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for wave in waves {
                rows.extend(
                    self.load_batch(attempt, &keys[wave], protection, limits)
                        .await?,
                );
            }
            Ok(rows)
        }
        .await;
        settlement_guard.complete(result)
    }
    pub(crate) async fn load_batch<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        keys: &[DurableRecordKey],
        protection: ReadProtection,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<Vec<Option<DurableRecord>>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let limits = limits.project_read_keys(keys)?;
            let rows = attempt.load_records(keys, protection, limits).await?;
            limits.validate_observations(keys, &rows)?;
            Ok(rows)
        }
        .await;
        settlement_guard.complete(result)
    }
}
