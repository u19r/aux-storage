use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use scheduled_bg_jobs::store::{StoreCapabilities, StoreCapabilitiesInput, StoreLevel};
use storage_types::{StorageError, StorageResult};

use crate::constants::{
    SCHEDULED_JOB_MAX_GET_MANY_KEYS, SCHEDULED_JOB_MAX_RANGE_ITEMS,
    SCHEDULED_JOB_MAX_TRANSACTION_BYTES, SCHEDULED_JOB_MAX_TRANSACTION_KEYS,
};

/// Portable scheduled-job budgets. Families supply only physical limits.
pub fn scheduled_job_capabilities(
    level: StoreLevel,
    max_key_bytes: usize,
    max_value_bytes: usize,
    max_transaction_duration: Duration,
) -> StorageResult<StoreCapabilities> {
    if max_transaction_duration.is_zero()
        || std::time::Instant::now()
            .checked_add(max_transaction_duration)
            .is_none()
    {
        return Err(StorageError::validation(
            "scheduled-job duration cannot form a finite deadline",
        ));
    }
    let key_bytes = NonZeroUsize::new(max_key_bytes)
        .ok_or_else(|| StorageError::validation("scheduled-job key limit is zero"))?;
    let value_bytes = NonZeroUsize::new(max_value_bytes)
        .ok_or_else(|| StorageError::validation("scheduled-job value limit is zero"))?;
    StoreCapabilities::try_from(StoreCapabilitiesInput {
        level,
        max_key_bytes: key_bytes,
        max_value_bytes: value_bytes,
        max_get_many_keys: nonzero_u32(SCHEDULED_JOB_MAX_GET_MANY_KEYS)?,
        max_get_many_key_bytes: nonzero(SCHEDULED_JOB_MAX_TRANSACTION_BYTES)?,
        max_transaction_bytes: nonzero(SCHEDULED_JOB_MAX_TRANSACTION_BYTES)?,
        max_transaction_keys: nonzero_u32(SCHEDULED_JOB_MAX_TRANSACTION_KEYS)?,
        max_range_items: nonzero_u32(SCHEDULED_JOB_MAX_RANGE_ITEMS)?,
        max_range_bytes: nonzero(SCHEDULED_JOB_MAX_TRANSACTION_BYTES)?,
        max_transaction_duration,
    })
    .map_err(|_| StorageError::internal("invalid scheduled-job capabilities"))
}

fn nonzero(value: usize) -> StorageResult<NonZeroUsize> {
    NonZeroUsize::new(value).ok_or_else(|| StorageError::internal("scheduled-job limit is zero"))
}

fn nonzero_u32(value: u32) -> StorageResult<NonZeroU32> {
    NonZeroU32::new(value).ok_or_else(|| StorageError::internal("scheduled-job limit is zero"))
}
