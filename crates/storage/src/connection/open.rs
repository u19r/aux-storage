use std::sync::Arc;

use storage_common::native_admission::{AttemptAdmission, NativeAdmissionBinding};
use storage_types::{StorageError, StorageResult, context::ErrorContext as _};

use super::model::Connection;
#[cfg(feature = "remote")]
use crate::admission::AdmissionConfig;
use crate::{
    admission::AdmissionController,
    database_manager::DatabaseManagerRuntimeOptions,
    maintenance_runtime::MaintenanceRuntime,
    provider_bundle::{InitializedStore, NativeStore, StoreKind},
};

impl Connection {
    /// The one place that turns an initialized store into a connection.
    /// `destination_namespace` is `Some` exactly when database jobs run.
    pub(crate) async fn open(
        id: &str,
        store: InitializedStore,
        options: &DatabaseManagerRuntimeOptions,
        destination_namespace: Option<&str>,
    ) -> StorageResult<Self> {
        match store.kind {
            StoreKind::Native(store) => {
                Self::open_native(id, store, options, destination_namespace).await
            }
            #[cfg(feature = "remote")]
            StoreKind::Remote { provider, jobs } => {
                if options.regional_connections.contains_key(id) {
                    return Err(StorageError::validation(
                        "regional replication needs database jobs",
                    ));
                }
                bootstrap_jobs(&jobs, id).await?;
                Self::remote(id, provider, jobs, options)
            }
        }
    }

    async fn open_native(
        id: &str,
        store: Box<dyn NativeStore>,
        options: &DatabaseManagerRuntimeOptions,
        destination_namespace: Option<&str>,
    ) -> StorageResult<Self> {
        // Admission first, from the capacity of the driver that serves it.
        let config = options
            .admission_config
            .qualify_native_capacity(store.attempt_capacity()?)?;
        let admission = AdmissionController::new(id, config)
            .map_err(|error| StorageError::validation(error.to_string()))?;
        let native_admission = admission.native_admission();
        let binding = Arc::new(NativeAdmissionBinding::bound(Arc::clone(&native_admission)));
        // The job store is bound before anything else can see it. (Drivers keep
        // a `OnceLock` behind `bind_native_admission`; that is ARCH-1's lane.)
        let jobs = store.scheduled_job_store(options.scheduled_job_store_config)?;
        jobs.bind_native_admission(native_admission)?;
        bootstrap_jobs(&jobs, id).await?;
        let native = store.capabilities(&binding);
        let regional_config = options.regional_connections.get(id).cloned();
        let (maintenance, regional) = if options.enable_database_jobs {
            let repositories =
                store.maintenance(AttemptAdmission::Background(Arc::clone(&binding)));
            let pair = crate::regional::regional_pair(id, regional_config, &native.regional);
            let runtime = MaintenanceRuntime::try_new(
                destination_namespace.ok_or_else(|| {
                    StorageError::internal("storage destination namespace is missing")
                })?,
                jobs.clone(),
                repositories,
                options.database_job_intervals,
                pair.as_ref().map(crate::regional::RegionalPairHandler::new),
            )?;
            let regional = crate::regional::regional_attachment(pair, &runtime)?;
            (Some(runtime), regional)
        } else if regional_config.is_some() {
            return Err(StorageError::validation(
                "regional replication needs database jobs",
            ));
        } else {
            (None, None)
        };
        Ok(Self {
            id: id.to_owned(),
            admission,
            dynamo: store.dynamo(),
            jobs,
            native: Some(native),
            durability: store.durability_health(),
            maintenance,
            regional,
        })
    }

    /// A remote connection: admission from configuration only, no native
    /// capabilities, no maintenance. Does not bootstrap its job store.
    #[cfg(feature = "remote")]
    pub(crate) fn remote(
        id: &str,
        provider: Arc<storage_remote::RemoteStorageProvider>,
        jobs: bg_job_store::ScheduledJobStore,
        options: &DatabaseManagerRuntimeOptions,
    ) -> StorageResult<Self> {
        let config = AdmissionConfig::try_new(options.admission_config)
            .map_err(|error| StorageError::validation(error.to_string()))?;
        let admission = AdmissionController::new(id, config)
            .map_err(|error| StorageError::validation(error.to_string()))?;
        Ok(Self {
            id: id.to_owned(),
            admission,
            dynamo: provider,
            jobs,
            native: None,
            durability: None,
            maintenance: None,
            regional: None,
        })
    }
}

async fn bootstrap_jobs(jobs: &bg_job_store::ScheduledJobStore, id: &str) -> StorageResult<()> {
    jobs.bootstrap()
        .await
        .map_err(|error| StorageError::internal(&error.to_string()))
        .with_context(|| format!("bootstrap scheduled job store for connection {id}"))
}
