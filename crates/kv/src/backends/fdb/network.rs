use std::sync::{Arc, Mutex, OnceLock};

use foundationdb::{
    Database,
    api::{FdbApiBuilder, NetworkAutoStop},
    options,
};
use storage_types::{StorageError, StorageResult};

use super::{error::map_fdb_error, store::FoundationDbConfig};

pub(super) struct FoundationDbNetworkInner {
    guard: Mutex<Option<NetworkAutoStop>>,
}

impl FoundationDbNetworkInner {
    fn new(guard: NetworkAutoStop) -> Self {
        Self {
            guard: Mutex::new(Some(guard)),
        }
    }
}

#[derive(Clone)]
pub(super) enum FoundationDbNetworkOwnership {
    Owned {
        _network: Arc<FoundationDbNetworkInner>,
    },
    Simulated,
}

static NETWORK_HANDLE: OnceLock<Arc<FoundationDbNetworkInner>> = OnceLock::new();
static NETWORK_POLICY: OnceLock<FoundationDbNetworkPolicy> = OnceLock::new();
static NETWORK_INIT: Mutex<()> = Mutex::new(());

unsafe extern "C" {
    fn atexit(callback: extern "C" fn()) -> std::ffi::c_int;
}

extern "C" fn shutdown_foundationdb_network_at_exit() {
    let Some(network) = NETWORK_HANDLE.get() else {
        return;
    };
    if let Ok(mut guard) = network.guard.lock() {
        drop(guard.take());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FoundationDbNetworkPolicy {
    pub(super) grv_cache_lag_ms: Option<u16>,
}

impl FoundationDbNetworkPolicy {
    pub(super) fn for_config(config: &FoundationDbConfig) -> Self {
        Self {
            grv_cache_lag_ms: (config.cache_read_version_ms > 0)
                .then_some(config.cache_read_version_ms),
        }
    }
}

pub(super) fn init_network(
    config: &FoundationDbConfig,
) -> StorageResult<FoundationDbNetworkOwnership> {
    let network = init_network_inner(config)?;
    Ok(FoundationDbNetworkOwnership::Owned { _network: network })
}

pub(super) fn validate_network_policy(
    existing: FoundationDbNetworkPolicy,
    requested: FoundationDbNetworkPolicy,
) -> StorageResult<()> {
    match (existing.grv_cache_lag_ms, requested.grv_cache_lag_ms) {
        (None, None) => Ok(()),
        (Some(existing_lag_ms), Some(requested_lag_ms)) if existing_lag_ms == requested_lag_ms => {
            Ok(())
        }
        (Some(_), None) => Ok(()),
        (None, Some(requested_lag_ms)) => Err(StorageError::validation(format!(
            "foundationdb cache_read_version_ms={requested_lag_ms} requires process-level network \
             options on the first FoundationDB connection; this process already initialized \
             FoundationDB without GRV caching"
        ))),
        (Some(existing_lag_ms), Some(requested_lag_ms)) => Err(StorageError::validation(format!(
            "foundationdb cache_read_version_ms mismatch in one process: existing \
             cache_read_version_ms={existing_lag_ms}, requested \
             cache_read_version_ms={requested_lag_ms}"
        ))),
    }
}

pub(super) fn validate_simulated_database_config(config: &FoundationDbConfig) -> StorageResult<()> {
    if config.cache_read_version_ms > 0 {
        return Err(StorageError::validation(format!(
            "foundationdb simulated database cannot configure cache_read_version_ms={} because \
             the simulator already owns the FoundationDB network",
            config.cache_read_version_ms
        )));
    }
    Ok(())
}

pub(super) fn open_database(config: &FoundationDbConfig) -> StorageResult<Database> {
    let database = if let Some(path) = config.cluster_file_path.as_deref() {
        Database::from_path(path)
    } else {
        Database::default()
    };
    database.map_err(|err| map_fdb_error("open FoundationDB database", err))
}

fn apply_network_policy(
    builder: foundationdb::api::NetworkBuilder,
    policy: FoundationDbNetworkPolicy,
) -> StorageResult<foundationdb::api::NetworkBuilder> {
    let Some(grv_cache_lag_ms) = policy.grv_cache_lag_ms else {
        return Ok(builder);
    };

    let builder = builder
        .set_option(options::NetworkOption::DisableClientBypass)
        .map_err(|err| map_fdb_error("set disable_client_bypass", err))?;
    builder
        .set_option(options::NetworkOption::Knob(format!(
            "max_version_cache_lag={}",
            format_grv_cache_lag_seconds(grv_cache_lag_ms)
        )))
        .map_err(|err| map_fdb_error("set max_version_cache_lag knob", err))
}

fn format_grv_cache_lag_seconds(lag_ms: u16) -> String {
    let whole_seconds = lag_ms / 1000;
    let milliseconds = lag_ms % 1000;
    if milliseconds == 0 {
        return whole_seconds.to_string();
    }

    let fraction = format!("{milliseconds:03}");
    let fraction = fraction.trim_end_matches('0');
    format!("{whole_seconds}.{fraction}")
}

fn init_network_inner(config: &FoundationDbConfig) -> StorageResult<Arc<FoundationDbNetworkInner>> {
    let requested_policy = FoundationDbNetworkPolicy::for_config(config);

    if let Some(existing) = NETWORK_HANDLE.get() {
        if let Some(existing_policy) = NETWORK_POLICY.get().copied() {
            validate_network_policy(existing_policy, requested_policy)?;
        }
        return Ok(Arc::clone(existing));
    }

    let _lock = NETWORK_INIT
        .lock()
        .map_err(|_| StorageError::internal("foundationdb network init mutex poisoned"))?;

    if let Some(existing) = NETWORK_HANDLE.get() {
        if let Some(existing_policy) = NETWORK_POLICY.get().copied() {
            validate_network_policy(existing_policy, requested_policy)?;
        }
        return Ok(Arc::clone(existing));
    }

    let builder = FdbApiBuilder::default()
        .build()
        .map_err(|err| map_fdb_error("initialize FoundationDB API", err))?;
    let builder = apply_network_policy(builder, requested_policy)?;
    let guard = {
        builder
            .boot()
            .map_err(|err| map_fdb_error("start FoundationDB network", err))?
    };
    let network = Arc::new(FoundationDbNetworkInner::new(guard));

    NETWORK_POLICY
        .set(requested_policy)
        .map_err(|_| StorageError::internal("FoundationDB network policy already initialized"))?;
    NETWORK_HANDLE
        .set(Arc::clone(&network))
        .map_err(|_| StorageError::internal("FoundationDB network already initialized"))?;
    let registered = unsafe { atexit(shutdown_foundationdb_network_at_exit) };
    if registered != 0 {
        return Err(StorageError::internal(
            "failed to register FoundationDB network shutdown hook",
        ));
    }

    Ok(network)
}

#[cfg(test)]
mod network_tests;
