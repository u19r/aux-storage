use std::sync::{Arc, RwLock};

use crate::Config;

pub trait ConfigManager: Send + Sync {
    fn config(&self) -> Arc<Config>;
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

impl SharedConfigManager {
    pub fn replace_config(&self, config: Arc<Config>) {
        *self.config.write().unwrap_or_else(|poisoned| {
            tracing::warn!("recovering poisoned config manager write lock");
            poisoned.into_inner()
        }) = config;
    }
}
