use std::sync::{Mutex, OnceLock};

use foundationdb::{api::FdbApiBuilder, options::NetworkOption};
use storage_types::{StorageError, StorageResult};

use super::error::map_operation_error;

static NETWORK_CACHE_READ_VERSION_MS: OnceLock<u16> = OnceLock::new();
static NETWORK_INIT: Mutex<()> = Mutex::new(());

/// Stop and join the process network after all application runtimes and native
/// handles have dropped, before C++ exit handlers destroy client monitor state.
/// This is terminal once the network has started; repeated shutdown is safe.
pub fn shutdown_foundationdb_network() -> StorageResult<()> {
    let observation = super::shutdown_observation::complete_observation();
    let shutdown = foundationdb::api::stop_network().map_err(map_operation_error);
    observation.and(shutdown)
}

pub(super) fn ensure_network(cache_read_version_ms: u16) -> StorageResult<()> {
    if let Some(existing) = NETWORK_CACHE_READ_VERSION_MS.get() {
        return validate_policy(*existing, cache_read_version_ms);
    }
    let _initializing = NETWORK_INIT
        .lock()
        .map_err(|_| StorageError::internal("FoundationDB network initialization lock poisoned"))?;
    if let Some(existing) = NETWORK_CACHE_READ_VERSION_MS.get() {
        return validate_policy(*existing, cache_read_version_ms);
    }
    let builder = FdbApiBuilder::default()
        .build()
        .map_err(map_operation_error)?;
    let builder = if cache_read_version_ms == 0 {
        builder
    } else {
        let seconds = format!(
            "{}.{:03}",
            cache_read_version_ms / 1000,
            cache_read_version_ms % 1000
        );
        builder
            .set_option(NetworkOption::DisableClientBypass)
            .map_err(map_operation_error)?
            .set_option(NetworkOption::Knob(format!(
                "max_version_cache_lag={seconds}"
            )))
            .map_err(map_operation_error)?
    };
    builder.boot().map_err(map_operation_error)?;
    NETWORK_CACHE_READ_VERSION_MS
        .set(cache_read_version_ms)
        .map_err(|_| StorageError::internal("FoundationDB network already initialized"))
}

fn validate_policy(existing: u16, requested: u16) -> StorageResult<()> {
    if requested == 0 || requested == existing {
        return Ok(());
    }
    Err(StorageError::validation(
        "FoundationDB cache_read_version_ms differs from the process network policy",
    ))
}
