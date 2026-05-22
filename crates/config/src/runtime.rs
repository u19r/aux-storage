use std::sync::{Arc, RwLock};

use crate::{Config, RemoteDefaultStorageMode};

pub trait ConfigManager: Send + Sync {
    fn config(&self) -> Arc<Config>;

    fn trusted_proxy_ips(&self) -> Vec<std::net::IpAddr> {
        self.config().root.http.trusted_proxy_ips.clone()
    }

    fn default_storage_mode(&self) -> RemoteDefaultStorageMode {
        self.config()
            .root
            .features
            .backends
            .remote
            .as_ref()
            .map(|remote| remote.default_storage_mode)
            .unwrap_or_default()
    }

    fn default_tenant_storage_mode(&self) -> RemoteDefaultStorageMode {
        self.default_storage_mode()
    }
}

pub trait MutableConfigManager: ConfigManager {
    fn replace_config(&self, config: Arc<Config>);
}

#[derive(Debug, Clone)]
pub struct StaticConfigManager {
    config: Arc<Config>,
}

impl StaticConfigManager {
    #[must_use]
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

impl ConfigManager for StaticConfigManager {
    fn config(&self) -> Arc<Config> {
        Arc::clone(&self.config)
    }
}

#[derive(Debug, Clone)]
pub struct SharedConfigManager {
    config: Arc<RwLock<Arc<Config>>>,
}

impl SharedConfigManager {
    #[must_use]
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
        }
    }
}

impl ConfigManager for SharedConfigManager {
    fn config(&self) -> Arc<Config> {
        self.config
            .read()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("recovering poisoned config manager read lock");
                poisoned.into_inner()
            })
            .clone()
    }
}

impl MutableConfigManager for SharedConfigManager {
    fn replace_config(&self, config: Arc<Config>) {
        *self.config.write().unwrap_or_else(|poisoned| {
            tracing::warn!("recovering poisoned config manager write lock");
            poisoned.into_inner()
        }) = config;
    }
}
