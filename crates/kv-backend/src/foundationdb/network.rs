use std::sync::{Mutex, OnceLock};

use foundationdb::{api::FdbApiBuilder, options::NetworkOption};
use storage_types::{StorageError, StorageResult};

use super::error::map_operation_error;

static NETWORK_POLICY: OnceLock<NetworkPolicy> = OnceLock::new();
static NETWORK_INIT: Mutex<()> = Mutex::new(());

/// Stop and join the process network after all application runtimes and native
/// handles have dropped, before C++ exit handlers destroy client monitor state.
/// This is terminal once the network has started; repeated shutdown is safe.
pub fn shutdown_foundationdb_network() -> StorageResult<()> {
    let observation = super::shutdown_observation::complete_observation();
    let shutdown = foundationdb::api::stop_network().map_err(map_operation_error);
    observation.and(shutdown)
}

/// Process network options. FoundationDB applies them once, before the
/// network starts, for every database of the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct NetworkPolicy {
    pub(super) cache_read_version_ms: u16,
    /// Client `reply_byte_limit` knob: the most bytes of one range reply.
    pub(super) reply_byte_limit: u32,
}

pub(super) fn ensure_network(policy: NetworkPolicy) -> StorageResult<()> {
    let limits = storage_types::FOUNDATIONDB_MIN_REPLY_BYTE_LIMIT
        ..=storage_types::FOUNDATIONDB_MAX_REPLY_BYTE_LIMIT;
    if !limits.contains(&policy.reply_byte_limit) {
        return Err(StorageError::validation(format!(
            "FoundationDB reply_byte_limit must be between {} and {}",
            limits.start(),
            limits.end()
        )));
    }
    if let Some(existing) = NETWORK_POLICY.get() {
        return validate_policy(*existing, policy);
    }
    let _initializing = NETWORK_INIT
        .lock()
        .map_err(|_| StorageError::internal("FoundationDB network initialization lock poisoned"))?;
    if let Some(existing) = NETWORK_POLICY.get() {
        return validate_policy(*existing, policy);
    }
    let builder = FdbApiBuilder::default()
        .build()
        .map_err(map_operation_error)?
        .set_option(NetworkOption::Knob(format!(
            "reply_byte_limit={}",
            policy.reply_byte_limit
        )))
        .map_err(map_operation_error)?;
    let builder = if policy.cache_read_version_ms == 0 {
        builder
    } else {
        let seconds = format!(
            "{}.{:03}",
            policy.cache_read_version_ms / 1000,
            policy.cache_read_version_ms % 1000
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
    NETWORK_POLICY
        .set(policy)
        .map_err(|_| StorageError::internal("FoundationDB network already initialized"))
}

/// The first backend of the process sets the knobs. A later reply limit that
/// differs keeps the first one and logs a warning; a later read-version cache
/// age that differs is an error, because it changes write validation.
fn validate_policy(existing: NetworkPolicy, requested: NetworkPolicy) -> StorageResult<()> {
    if requested.reply_byte_limit != existing.reply_byte_limit {
        tracing::warn!(
            requested = requested.reply_byte_limit,
            applied = existing.reply_byte_limit,
            "FoundationDB reply_byte_limit differs from the process network; the first value \
             applies"
        );
    }
    if requested.cache_read_version_ms == 0
        || requested.cache_read_version_ms == existing.cache_read_version_ms
    {
        return Ok(());
    }
    Err(StorageError::validation(
        "FoundationDB cache_read_version_ms differs from the process network policy",
    ))
}
