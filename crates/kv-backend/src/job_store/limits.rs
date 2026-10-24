use bg_job_store::ScheduledJobStoreConfig;
use scheduled_bg_jobs::store::{StoreCapabilities, StoreLevel};
use storage_provider::job_store::scheduled_job_capabilities;
use storage_types::{StorageError, StorageResult};

use super::DATA_PREFIX;
use crate::{KvBackend, KvTopology};

pub(super) fn capabilities<B: KvBackend>(
    backend: &B,
    config: ScheduledJobStoreConfig,
) -> StorageResult<StoreCapabilities> {
    let layout = backend.native_layout();
    let max_key_bytes = layout
        .maximum_logical_key_bytes()
        .checked_sub(DATA_PREFIX.len())
        .map(|remaining| remaining.min(65_536))
        .filter(|remaining| *remaining > 0)
        .ok_or_else(|| StorageError::validation("scheduled-job key prefix exceeds driver limit"))?;
    let level = match backend.topology() {
        KvTopology::Local => StoreLevel::Local,
        KvTopology::Distributed => StoreLevel::Distributed,
    };
    let max_value_bytes = layout
        .maximum_logical_value_bytes()
        .min(crate::constants::SCHEDULED_JOB_VALUE_BYTES);
    scheduled_job_capabilities(
        level,
        max_key_bytes,
        max_value_bytes,
        config.max_transaction_duration,
    )
}

pub(crate) fn point_allowance(count: usize) -> StorageResult<crate::KvValueAllowance> {
    let maximum = crate::constants::SCHEDULED_JOB_VALUE_BYTES;
    let total = maximum
        .checked_mul(count)
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    Ok(crate::KvValueAllowance::new(maximum, total))
}
