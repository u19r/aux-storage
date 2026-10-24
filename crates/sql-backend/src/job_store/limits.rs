use bg_job_store::ScheduledJobStoreConfig;
use scheduled_bg_jobs::store::{StoreCapabilities, StoreLevel};
use storage_provider::job_store::scheduled_job_capabilities;
use storage_types::StorageResult;

use crate::SqlEngine;

pub(super) fn capabilities(
    engine: SqlEngine,
    config: ScheduledJobStoreConfig,
) -> StorageResult<StoreCapabilities> {
    let (level, max_key_bytes) = match engine {
        SqlEngine::Postgres => (StoreLevel::Distributed, 2_048),
        SqlEngine::Sqlite | SqlEngine::Turso => (StoreLevel::Local, 65_536),
    };
    scheduled_job_capabilities(
        level,
        max_key_bytes,
        1024 * 1024,
        config.max_transaction_duration,
    )
}
