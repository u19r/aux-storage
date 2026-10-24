//! Bind manager-owned native job stores and maintenance before publishing
//! workers.
use storage_types::{StorageError, StorageResult};

use super::construction::PreparedStorageProviders;
use crate::admission::AdmissionRegistry;

pub(super) struct NativeConnectionResources {
    connection_id: String,
    config: crate::admission::AdmissionConfig,
    job_store: bg_job_store::ScheduledJobStore,
    maintenance: Option<std::sync::Arc<dyn crate::provider_bundle::MaintenanceProviderSource>>,
}

impl NativeConnectionResources {
    pub(super) fn new(
        connection_id: String,
        config: crate::admission::AdmissionConfig,
        job_store: bg_job_store::ScheduledJobStore,
        maintenance: Option<std::sync::Arc<dyn crate::provider_bundle::MaintenanceProviderSource>>,
    ) -> Self {
        Self {
            connection_id,
            config,
            job_store,
            maintenance,
        }
    }

    fn bind(&self, registry: &AdmissionRegistry) -> StorageResult<()> {
        let controller = registry
            .for_connection(&self.connection_id)
            .ok_or_else(|| StorageError::internal("native connection admission is missing"))?;
        let admission = controller.native_admission();
        self.job_store
            .bind_native_admission(std::sync::Arc::clone(&admission))?;
        if let Some(source) = &self.maintenance {
            source.bind(admission)?;
        }
        Ok(())
    }
}

impl PreparedStorageProviders {
    pub(super) fn qualify_bundle_admission(
        registry: &storage_provider::StorageConnectionRegistry,
        options: &super::DatabaseManagerRuntimeOptions,
        bundles: &std::collections::HashMap<String, crate::provider_bundle::StorageProviderBundle>,
    ) -> StorageResult<std::collections::HashMap<String, crate::admission::AdmissionConfig>> {
        let mut configs = std::collections::HashMap::with_capacity(registry.connections.len());
        for (id, connection) in &registry.connections {
            let bundle = bundles
                .get(id)
                .ok_or_else(|| StorageError::internal("initialized storage bundle is missing"))?;
            #[cfg(feature = "regional")]
            if bundle.regional_services.is_some() && !options.enable_database_jobs {
                return Err(StorageError::validation(
                    "regional storage requires its shared database job driver",
                ));
            }
            let config = match bundle.native_attempt_capacity {
                Some(capacity) => options.admission_config.qualify_native_capacity(capacity)?,
                None if matches!(
                    connection.backend_type,
                    storage_provider::StorageBackend::Remote
                ) =>
                {
                    crate::admission::AdmissionConfig::try_new(options.admission_config)
                        .map_err(|error| StorageError::validation(error.to_string()))?
                }
                None => {
                    return Err(StorageError::validation(
                        "initialized native storage bundle is missing its driver capacity report",
                    ));
                }
            };
            #[cfg(feature = "regional")]
            if let Some(services) = &bundle.regional_services {
                let reserve = config
                    .enabled
                    .then(|| std::num::NonZeroUsize::new(config.control_reserve_concurrency))
                    .flatten();
                services.require_recovery_reserve(reserve)?;
            }
            configs.insert(id.clone(), config);
        }
        Ok(configs)
    }

    pub(super) fn native_admission_config(
        &self,
        connection_id: &str,
        fallback: crate::admission::AdmissionConfig,
    ) -> crate::admission::AdmissionConfig {
        match self
            .native_admission
            .iter()
            .find(|resources| resources.connection_id == connection_id)
        {
            Some(resources) => resources.config,
            None => fallback,
        }
    }

    pub(super) fn bind_native_admission(&self, registry: &AdmissionRegistry) -> StorageResult<()> {
        for resources in &self.native_admission {
            resources.bind(registry)?;
        }
        Ok(())
    }
}
