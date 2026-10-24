use std::collections::HashSet;

use storage_provider::{StorageBackend, StorageConnectionRegistry};
use storage_types::{StorageError, StorageResult};

use super::DatabaseManagerRuntimeOptions;

impl DatabaseManagerRuntimeOptions {
    pub(crate) fn load_regional_connections(
        &mut self,
        files: &std::collections::HashMap<String, std::path::PathBuf>,
    ) -> StorageResult<()> {
        self.regional_connections
            .try_reserve(files.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (id, path) in files {
            if self.regional_connections.contains_key(id) {
                return Err(StorageError::validation(
                    "regional connection is configured more than once",
                ));
            }
            let file = std::fs::File::open(path)
                .map_err(|_| StorageError::validation("regional startup file cannot be opened"))?;
            // Bound parsing before native initialization; payloads are never
            // logged.
            let mut input =
                std::io::Read::take(file, crate::constants::REGIONAL_STARTUP_FILE_BYTES + 1);
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact((crate::constants::REGIONAL_STARTUP_FILE_BYTES + 1) as usize)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            std::io::Read::read_to_end(&mut input, &mut bytes)
                .map_err(|_| StorageError::validation("regional startup file cannot be read"))?;
            if bytes.len() as u64 > crate::constants::REGIONAL_STARTUP_FILE_BYTES {
                return Err(StorageError::validation(
                    "regional startup file exceeds its byte budget",
                ));
            }
            let startup = serde_json::from_slice(&bytes)
                .map_err(|_| StorageError::validation("invalid regional startup configuration"))?;
            self.regional_connections.insert(id.clone(), startup);
        }
        Ok(())
    }

    /// Reject invalid connection selection before opening any native store.
    pub(crate) fn validate_regional_connections(
        &self,
        registry: &StorageConnectionRegistry,
    ) -> StorageResult<()> {
        if !self.regional_connections.is_empty() && !self.enable_database_jobs {
            return Err(StorageError::validation(
                "regional startup requires enabled database jobs",
            ));
        }
        let mut origins = HashSet::with_capacity(self.regional_connections.len());
        let mut endpoints = HashSet::with_capacity(self.regional_connections.len());
        for (id, startup) in &self.regional_connections {
            let connection = registry.connections.get(id).ok_or_else(|| {
                StorageError::validation(
                    "regional configuration names an unknown storage connection",
                )
            })?;
            if matches!(connection.backend_type, StorageBackend::Remote) {
                return Err(StorageError::validation(
                    "regional startup requires a native backend",
                ));
            }
            self.admission_config
                .require_reserved_attempts(startup.budgets.reserved_attempts)?;
            let deployment = &startup.configuration;
            if !origins.insert((&deployment.region, deployment.group))
                || !endpoints.insert(deployment.endpoint)
            {
                return Err(StorageError::validation(
                    "regional connections must have distinct native origins and listener endpoints",
                ));
            }
        }
        Ok(())
    }
}
