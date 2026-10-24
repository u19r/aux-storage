use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bg_job_store::ScheduledJobStore;
use storage_provider::{
    DynamoOperations, StorageBackend, StorageConfig, StorageConnectionConfig,
    StorageConnectionRegistry, SystemStreamTrimRepository, TableReclaimRepository,
    maintenance::GsiLifecycleRepository,
};
use storage_types::{StorageError, StorageResult, VecMap, context::ErrorContext as _};
use stream_provider::StreamOperations;

use crate::{
    admission::AdmissionRegistry,
    database_manager::{DatabaseManager, DatabaseManagerRuntimeOptions},
    maintenance_runtime::MaintenanceRuntime,
    provider_bundle::{
        MaintenanceRepositories, StorageProviderBundle, create_storage_provider_bundle,
    },
    tables::Tables,
};

pub(crate) struct PreparedStorageProviders {
    pub dynamo_providers: VecMap<String, Arc<dyn DynamoOperations>>,
    pub stream_providers: HashMap<String, Arc<dyn StreamOperations>>,
    pub queue_providers: HashMap<String, Arc<dyn queue_provider::QueueOperations>>,
    pub pubsub_providers: HashMap<String, Arc<dyn pubsub_provider::PubsubProvider>>,
    pub default_delivery_source: Option<Arc<dyn crate::provider_bundle::DeliveryProviderSource>>,

    pub default_ttl_sweep_repository: Option<Arc<dyn storage_provider::TtlSweepRepository>>,
    pub default_system_stream_trim_repository: Option<Arc<dyn SystemStreamTrimRepository>>,
    pub default_table_reclaim_repository: Option<Arc<dyn TableReclaimRepository>>,
    pub default_gsi_lifecycle_repository: Arc<dyn GsiLifecycleRepository>,
    pub maintenance_runtimes: Vec<Arc<MaintenanceRuntime>>,
    pub default_scheduled_job_store: Option<ScheduledJobStore>,
    pub(super) native_admission: Vec<super::native_admission::NativeConnectionResources>,
    #[cfg(feature = "regional")]
    pub regional_services: Vec<(
        String,
        regional_replication::runtime::PreparedRegionalServices,
    )>,
}

impl PreparedStorageProviders {
    fn validate_maintenance_support(
        registry: &StorageConnectionRegistry,
        runtime_options: &DatabaseManagerRuntimeOptions,
    ) -> StorageResult<()> {
        if !runtime_options.enable_database_jobs {
            return Ok(());
        }
        if let Some((connection_id, _)) = registry
            .connections
            .iter()
            .find(|(_, connection)| matches!(connection.backend_type, StorageBackend::Remote))
        {
            return Err(StorageError::unsupported(&format!(
                "storage connection '{connection_id}' delegates database maintenance to its \
                 remote service"
            )));
        }
        Ok(())
    }

    pub(super) fn destination_namespaces(
        registry: &StorageConnectionRegistry,
    ) -> StorageResult<HashMap<String, String>> {
        let mut physical_destinations = HashMap::with_capacity(registry.connections.len());
        let mut namespaces = HashMap::with_capacity(registry.connections.len());
        for (connection_id, connection) in &registry.connections {
            let namespace = physical_destination_namespace(connection)?;
            if let Some(existing) =
                physical_destinations.insert(namespace.clone(), connection_id.clone())
            {
                return Err(StorageError::validation(format!(
                    "storage connections '{existing}' and '{connection_id}' alias one physical \
                     destination"
                )));
            }
            namespaces.insert(connection_id.clone(), namespace);
        }
        Ok(namespaces)
    }
}

fn storage_config_for_connection(connection: &StorageConnectionConfig) -> StorageConfig {
    StorageConfig {
        backend_type: connection.backend_type.clone(),
        connection_string: connection.connection_string.clone(),
        file_path: connection.file_path.clone(),
        sqlite: connection.sqlite.clone(),
        postgres: connection.postgres.clone(),
        turso: connection.turso.clone(),
        rocksdb: connection.rocksdb.clone(),
        foundationdb: connection.foundationdb.clone(),
        remote: connection.remote.clone(),
    }
}

pub(super) fn single_connection_registry(config: StorageConfig) -> StorageConnectionRegistry {
    StorageConnectionRegistry {
        default_connection_id: "default".to_string(),
        connections: HashMap::from([(
            "default".to_string(),
            storage_provider::StorageConnectionConfig {
                backend_type: config.backend_type,
                connection_string: config.connection_string,
                file_path: config.file_path,
                sqlite: config.sqlite,
                postgres: config.postgres,
                turso: config.turso,
                rocksdb: config.rocksdb,
                foundationdb: config.foundationdb,
                remote: config.remote,
            },
        )]),
    }
}

fn physical_destination_namespace(connection: &StorageConnectionConfig) -> StorageResult<String> {
    let identity = match &connection.backend_type {
        StorageBackend::SQLite => {
            let path = connection
                .file_path
                .as_deref()
                .or(connection.connection_string.as_deref())
                .ok_or_else(|| StorageError::validation("SQLite destination path is missing"))?;
            if is_ephemeral_sqlite_destination(path) {
                static NEXT_EPHEMERAL_DESTINATION: AtomicU64 = AtomicU64::new(1);
                format!(
                    "sqlite-memory:{}",
                    NEXT_EPHEMERAL_DESTINATION.fetch_add(1, Ordering::Relaxed)
                )
            } else {
                format!("sqlite:{}", canonical_local_destination(path)?)
            }
        }
        StorageBackend::RocksDB => {
            let path = connection
                .file_path
                .as_deref()
                .or(connection.connection_string.as_deref())
                .ok_or_else(|| StorageError::validation("RocksDB destination path is missing"))?;
            format!("rocksdb:{}", canonical_local_destination(path)?)
        }
        StorageBackend::Postgres => canonical_postgres_destination(
            connection
                .postgres
                .as_ref()
                .map(|settings| settings.dsn.as_str())
                .or(connection.connection_string.as_deref())
                .ok_or_else(|| StorageError::validation("Postgres destination DSN is missing"))?,
        )?,
        StorageBackend::Turso => canonical_turso_destination(
            connection
                .connection_string
                .as_deref()
                .ok_or_else(|| StorageError::validation("Turso destination URL is missing"))?,
        )?,
        StorageBackend::FoundationDb => {
            let settings = connection.foundationdb.as_ref().ok_or_else(|| {
                StorageError::validation("FoundationDB destination settings are missing")
            })?;
            let cluster = settings
                .cluster_file
                .as_deref()
                .map(canonical_local_destination)
                .transpose()?
                .unwrap_or_else(|| "default-cluster".to_owned());
            format!(
                "foundationdb:{cluster}:{}:{}",
                settings.tenant_name.as_deref().unwrap_or_default(),
                settings.subspace_prefix.as_deref().unwrap_or_default()
            )
        }
        StorageBackend::Remote => {
            let settings = connection.remote.as_ref().ok_or_else(|| {
                StorageError::validation("remote destination settings are missing")
            })?;
            let mut endpoints = settings
                .endpoint_urls
                .iter()
                .map(|endpoint| canonical_network_destination(endpoint, None))
                .collect::<StorageResult<Vec<_>>>()?;
            endpoints.sort_unstable();
            format!(
                "remote:{}:{}",
                endpoints.join(","),
                settings.region.as_deref().unwrap_or_default()
            )
        }
    };
    Ok(format!("{:x}", md5::compute(identity.as_bytes())))
}

fn canonical_local_destination(path: &str) -> StorageResult<String> {
    let path = std::fs::canonicalize(path)
        .or_else(|_| std::path::absolute(path))
        .map_err(|_| StorageError::validation("storage destination path is invalid"))?;
    Ok(path.to_string_lossy().into_owned())
}

fn is_ephemeral_sqlite_destination(path: &str) -> bool {
    let normalized = path.strip_prefix("sqlite:").unwrap_or(path);
    normalized == ":memory:" || normalized.starts_with(":memory:?")
}

fn canonical_postgres_destination(dsn: &str) -> StorageResult<String> {
    let url = url::Url::parse(dsn)
        .map_err(|_| StorageError::validation("Postgres destination DSN is invalid"))?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return Err(StorageError::validation(
            "Postgres destination DSN scheme is invalid",
        ));
    }
    let endpoint = canonical_network_destination(dsn, Some(5432))?
        .split_once("://")
        .map_or_else(String::new, |(_, destination)| destination.to_owned());
    let schema = url
        .query_pairs()
        .find(|(name, _)| name == "options")
        .and_then(|(_, value)| postgres_search_path(&value))
        .unwrap_or_else(|| "public".to_owned());
    Ok(format!("postgres:{endpoint}:schema={schema}"))
}

fn postgres_search_path(options: &str) -> Option<String> {
    let setting = options
        .split_whitespace()
        .find_map(|part| part.strip_prefix("-csearch_path="))
        .or_else(|| options.strip_prefix("-csearch_path="))?;
    setting
        .split(',')
        .next()
        .map(str::trim)
        .filter(|schema| !schema.is_empty())
        .map(str::to_owned)
}

fn canonical_turso_destination(value: &str) -> StorageResult<String> {
    if value == ":memory:" {
        static NEXT_EPHEMERAL_DESTINATION: AtomicU64 = AtomicU64::new(1);
        return Ok(format!(
            "turso-memory:{}",
            NEXT_EPHEMERAL_DESTINATION.fetch_add(1, Ordering::Relaxed)
        ));
    }
    if value.contains("://") {
        return canonical_network_destination(value, None)
            .map(|destination| format!("turso:{destination}"));
    }
    canonical_local_destination(value).map(|path| format!("turso:{path}"))
}

fn canonical_network_destination(value: &str, default_port: Option<u16>) -> StorageResult<String> {
    let url = url::Url::parse(value)
        .map_err(|_| StorageError::validation("storage destination URL is invalid"))?;
    let host = url
        .host_str()
        .ok_or_else(|| StorageError::validation("storage destination host is missing"))?
        .to_ascii_lowercase();
    let host = if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    };
    let port = url
        .port()
        .or(default_port)
        .map_or_else(String::new, |port| format!(":{port}"));
    let path = url.path().trim_end_matches('/');
    Ok(format!("{}://{host}{port}{path}", url.scheme()))
}

pub(crate) async fn prepare_ordinary_storage_providers(
    registry: &StorageConnectionRegistry,
    runtime_options: &DatabaseManagerRuntimeOptions,
) -> StorageResult<(PreparedStorageProviders, Option<MaintenanceRepositories>)> {
    PreparedStorageProviders::validate_maintenance_support(registry, runtime_options)?;
    #[cfg(feature = "regional")]
    runtime_options.validate_regional_connections(registry)?;
    let bundles = futures::future::try_join_all(registry.connections.iter().map(
        |(id, connection)| async move {
            let bundle = create_storage_provider_bundle(
                storage_config_for_connection(connection),
                runtime_options,
                id,
            )
            .await
            .with_context(|| format!("create storage provider for connection {id}"))?;
            Ok::<_, StorageError>((id.clone(), bundle))
        },
    ))
    .await?
    .into_iter()
    .collect();
    prepare_initialized_storage_providers(registry, runtime_options, bundles).await
}

async fn prepare_initialized_storage_providers(
    registry: &StorageConnectionRegistry,
    runtime_options: &DatabaseManagerRuntimeOptions,
    mut bundles: HashMap<String, StorageProviderBundle>,
) -> StorageResult<(PreparedStorageProviders, Option<MaintenanceRepositories>)> {
    PreparedStorageProviders::validate_maintenance_support(registry, runtime_options)?;
    let mut admission_configs =
        PreparedStorageProviders::qualify_bundle_admission(registry, runtime_options, &bundles)?;
    let destination_namespaces = if runtime_options.enable_database_jobs {
        PreparedStorageProviders::destination_namespaces(registry)?
    } else {
        HashMap::new()
    };
    let mut dynamo_providers: VecMap<String, Arc<dyn DynamoOperations>> =
        VecMap::with_capacity(registry.connections.len());
    let mut stream_providers: HashMap<String, Arc<dyn StreamOperations>> =
        HashMap::with_capacity(registry.connections.len());
    let mut queue_providers: HashMap<String, Arc<dyn queue_provider::QueueOperations>> =
        HashMap::with_capacity(registry.connections.len());
    let mut pubsub_providers: HashMap<String, Arc<dyn pubsub_provider::PubsubProvider>> =
        HashMap::with_capacity(registry.connections.len());
    let mut default_scheduled_job_store: Option<ScheduledJobStore> = None;
    let mut default_maintenance_repositories = None;
    let mut default_ttl_sweep_repository = None;
    let mut default_system_stream_trim_repository = None;
    let mut default_table_reclaim_repository = None;
    let mut default_delivery_source = None;

    let mut default_gsi_lifecycle_repository: Option<Arc<dyn GsiLifecycleRepository>> = None;
    let mut maintenance_runtimes = Vec::new();
    let mut native_admission = Vec::new();
    #[cfg(feature = "regional")]
    let mut regional_services = Vec::new();

    for connection_id in registry.connections.keys() {
        let bundle = bundles
            .remove(connection_id)
            .ok_or_else(|| StorageError::internal("initialized storage bundle is missing"))?;
        let destination_namespace = if runtime_options.enable_database_jobs {
            Some(
                destination_namespaces
                    .get(connection_id)
                    .cloned()
                    .ok_or_else(|| {
                        StorageError::internal("maintenance destination namespace was not selected")
                    })?,
            )
        } else {
            None
        };
        let StorageProviderBundle {
            native_attempt_capacity,
            #[cfg(feature = "regional")]
                regional_services: bound_regional_services,
            database: provider,
            dynamo,
            stream,

            ttl_sweep_repository,
            system_stream_trim_repository,
            table_reclaim_repository,
            gsi_lifecycle_repository,
            queue: queue_provider,
            pubsub: pubsub_provider,
            delivery_source,
            maintenance,
            maintenance_source,
            scheduled_job_store,
        } = bundle;
        if let Some(provider) = provider.as_ref() {
            provider
                .initialize_storage()
                .await
                .with_context(|| format!("initialize storage for connection {connection_id}"))?;
            provider
                .initialize_stream()
                .await
                .map_err(|error| StorageError::internal(&error.to_string()))
                .with_context(|| format!("initialize stream for connection {connection_id}"))?;
        }
        scheduled_job_store
            .bootstrap()
            .await
            .map_err(|error| StorageError::internal(&error.to_string()))
            .with_context(|| {
                format!("bootstrap scheduled job store for connection {connection_id}")
            })?;
        if let Some(queue_provider) = queue_provider {
            queue_providers.insert(connection_id.clone(), queue_provider);
        }
        if let Some(pubsub_provider) = pubsub_provider {
            pubsub_providers.insert(connection_id.clone(), pubsub_provider);
        }
        if connection_id == &registry.default_connection_id {
            default_maintenance_repositories = maintenance.clone();
            default_delivery_source = delivery_source;

            default_ttl_sweep_repository = ttl_sweep_repository;
            default_system_stream_trim_repository = system_stream_trim_repository;
            default_table_reclaim_repository = table_reclaim_repository.clone();
        }
        let mut admitted_maintenance_source = None;
        if runtime_options.enable_database_jobs {
            let mut repositories = maintenance.ok_or_else(|| {
                StorageError::unsupported(
                    "configured backend does not execute local database maintenance",
                )
            })?;
            let mut scheduled_reclaim = table_reclaim_repository;
            if let Some(source) = maintenance_source {
                let (admitted, reclaim) = source.prepare(repositories.job_catalog.clone())?;
                repositories = admitted;
                scheduled_reclaim = Some(reclaim);
                admitted_maintenance_source = Some(source);
            }
            maintenance_runtimes.push(MaintenanceRuntime::try_new(
                destination_namespace.as_deref().ok_or_else(|| {
                    StorageError::internal("storage destination namespace is missing")
                })?,
                scheduled_job_store.clone(),
                repositories,
                scheduled_reclaim,
                runtime_options.database_job_intervals,
            )?);
        }
        #[cfg(feature = "regional")]
        if let Some(services) = bound_regional_services {
            regional_services.push((connection_id.clone(), services));
        }
        if native_attempt_capacity.is_some() {
            let config = admission_configs
                .remove(connection_id)
                .ok_or_else(|| StorageError::internal("qualified native admission is missing"))?;
            native_admission.push(super::native_admission::NativeConnectionResources::new(
                connection_id.clone(),
                config,
                scheduled_job_store.clone(),
                admitted_maintenance_source,
            ));
        }
        if connection_id == &registry.default_connection_id {
            default_scheduled_job_store = Some(scheduled_job_store);
            default_gsi_lifecycle_repository = Some(gsi_lifecycle_repository);
        }
        dynamo_providers.insert(connection_id.clone(), dynamo);
        if let Some(stream) = stream {
            stream_providers.insert(connection_id.clone(), stream);
        }
    }

    let default_gsi_lifecycle_repository = default_gsi_lifecycle_repository.ok_or_else(|| {
        StorageError::internal("default GSI lifecycle repository was not selected")
    })?;
    Ok((
        PreparedStorageProviders {
            dynamo_providers,
            stream_providers,
            queue_providers,
            pubsub_providers,
            default_delivery_source,

            default_ttl_sweep_repository,
            default_system_stream_trim_repository,
            default_table_reclaim_repository,
            default_gsi_lifecycle_repository,
            maintenance_runtimes,
            default_scheduled_job_store,
            native_admission,
            #[cfg(feature = "regional")]
            regional_services,
        },
        default_maintenance_repositories,
    ))
}

impl DatabaseManager {
    /// Assemble all capabilities and runners around a caller-initialized store.
    /// The configuration identifies the physical destination for admission and
    /// job fencing.
    pub async fn from_initialized_bundle(
        config: StorageConfig,
        bundle: StorageProviderBundle,
        runtime_options: DatabaseManagerRuntimeOptions,
    ) -> StorageResult<Self> {
        #[cfg(feature = "regional")]
        if !runtime_options.regional_connections.is_empty() {
            return Err(StorageError::validation(
                "caller-initialized bundles cannot also request native regional initialization",
            ));
        }
        let registry = single_connection_registry(config);
        let bundles = HashMap::from([(registry.default_connection_id.clone(), bundle)]);
        let (prepared, _) =
            prepare_initialized_storage_providers(&registry, &runtime_options, bundles).await?;
        let mut manager = Self::assemble_from_prepared(registry, runtime_options, prepared)?;
        manager.finish_ordinary_initialization().await?;
        Ok(manager)
    }

    pub async fn new_with_config(config: StorageConfig) -> StorageResult<Self> {
        Self::new_with_config_and_runtime_options(config, DatabaseManagerRuntimeOptions::default())
            .await
    }

    pub async fn new_with_config_and_runtime_options(
        config: StorageConfig,
        runtime_options: DatabaseManagerRuntimeOptions,
    ) -> StorageResult<Self> {
        let registry = single_connection_registry(config);
        Self::new_with_connection_registry_and_runtime_options(registry, runtime_options).await
    }

    pub async fn new_with_connection_registry(
        registry: StorageConnectionRegistry,
    ) -> StorageResult<Self> {
        Self::new_with_connection_registry_and_runtime_options(
            registry,
            DatabaseManagerRuntimeOptions::default(),
        )
        .await
    }

    pub async fn new_with_connection_registry_and_runtime_options(
        registry: StorageConnectionRegistry,
        runtime_options: DatabaseManagerRuntimeOptions,
    ) -> StorageResult<Self> {
        let (prepared, _) = prepare_ordinary_storage_providers(&registry, &runtime_options).await?;
        let mut manager = Self::assemble_from_prepared(registry, runtime_options, prepared)?;
        manager.finish_ordinary_initialization().await?;
        Ok(manager)
    }

    pub(crate) fn assemble_from_prepared(
        registry: StorageConnectionRegistry,
        runtime_options: DatabaseManagerRuntimeOptions,
        prepared: PreparedStorageProviders,
    ) -> StorageResult<Self> {
        if let Some(metrics_facade) = runtime_options.metrics_facade.clone() {
            metrics_facade::set_metrics_facade(metrics_facade);
        }

        let admission_configs = registry
            .connections
            .keys()
            .map(|connection_id| {
                (
                    connection_id.clone(),
                    prepared
                        .native_admission_config(connection_id, runtime_options.admission_config),
                )
            })
            .collect::<Vec<_>>();
        let admission_registry = AdmissionRegistry::new_with_connection_configs(
            registry.default_connection_id.clone(),
            admission_configs,
            runtime_options.admission_config,
        )
        .map_err(|error| StorageError::validation(error.to_string()))?;
        prepared.bind_native_admission(&admission_registry)?;
        let PreparedStorageProviders {
            dynamo_providers,
            stream_providers,
            queue_providers,
            pubsub_providers,
            default_delivery_source,
            maintenance_runtimes,
            default_scheduled_job_store,
            native_admission: _,
            #[cfg(feature = "regional")]
            regional_services,

            default_ttl_sweep_repository,
            default_system_stream_trim_repository,
            default_table_reclaim_repository,
            default_gsi_lifecycle_repository,
        } = prepared;
        #[cfg(feature = "regional")]
        let regional_services = regional_services
            .into_iter()
            .map(|(connection_id, services)| {
                let controller = admission_registry
                    .for_connection(&connection_id)
                    .ok_or_else(|| {
                        StorageError::internal("regional connection admission is missing")
                    })?;
                services.bind_native_admission(&connection_id, controller.native_admission())
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let default_dynamo = dynamo_providers
            .get(&registry.default_connection_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::validation(format!(
                    "default storage connection '{}' not found",
                    registry.default_connection_id
                ))
            })?;
        Ok(Self {
            point_reads: std::sync::OnceLock::new(),
            dynamo: default_dynamo,
            scheduled_job_store: default_scheduled_job_store,
            stream: stream_providers
                .get(&registry.default_connection_id)
                .cloned(),

            gsi_lifecycle_repository: Some(default_gsi_lifecycle_repository),
            maintenance_runtimes,
            background_services: tokio::sync::Mutex::new(
                super::service_shutdown::BackgroundServiceState::Running(
                    super::service_shutdown::BackgroundServices {
                        maintenance: Vec::new(),
                        #[cfg(feature = "regional")]
                        regional: Vec::new(),
                        #[cfg(feature = "regional")]
                        pending_regional: regional_services,
                    },
                ),
            ),
            system_stream_trim_repository: default_system_stream_trim_repository,
            table_reclaim_repository: default_table_reclaim_repository,
            ttl_sweep_repository: default_ttl_sweep_repository,
            queue_provider: queue_providers
                .get(&registry.default_connection_id)
                .cloned(),
            pubsub_provider: pubsub_providers
                .get(&registry.default_connection_id)
                .cloned(),
            delivery_providers: default_delivery_source
                .map(|source| source.bind(admission_registry.default_controller().clone())),
            dynamo_registry: dynamo_providers,
            admission_registry,
            single_table_mode: runtime_options.enable_single_table_mode,
        })
    }

    pub(crate) async fn finish_ordinary_initialization(&mut self) -> StorageResult<()> {
        let result = self.initialize_system_tables_and_jobs().await;
        if result.is_err() {
            let _ = self.shutdown_background_services().await;
        }
        result
    }

    async fn initialize_system_tables_and_jobs(&mut self) -> StorageResult<()> {
        Tables::create_sys_namespaces_table(self)
            .await
            .context("create sys namespaces table during database manager construction")?;
        Tables::create_sys_analytics_table(self)
            .await
            .context("create sys analytics table during database manager construction")?;
        let seeded = futures::future::try_join_all(
            std::mem::take(&mut self.maintenance_runtimes)
                .into_iter()
                .map(|runtime| runtime.initialize_seeds()),
        )
        .await?;
        self.background_services.get_mut().publish_services(seeded)
    }
    pub fn new_with_mocks<T>(storage: Arc<T>) -> StorageResult<Self>
    where T: DynamoOperations + 'static {
        let dynamo: Arc<dyn DynamoOperations> = storage.clone();
        let admission_registry = AdmissionRegistry::new(
            "default",
            ["default".to_string()],
            crate::admission::AdmissionConfig::default(),
        )
        .map_err(|error| StorageError::validation(error.to_string()))?;
        Ok(Self {
            point_reads: std::sync::OnceLock::new(),
            dynamo: dynamo.clone(),
            stream: None,
            scheduled_job_store: None,
            ttl_sweep_repository: None,
            gsi_lifecycle_repository: None,
            maintenance_runtimes: Vec::new(),
            background_services: tokio::sync::Mutex::default(),
            system_stream_trim_repository: None,
            table_reclaim_repository: None,
            queue_provider: None,
            pubsub_provider: None,
            delivery_providers: None,
            dynamo_registry: [("default".to_owned(), dynamo)].into_iter().collect(),
            admission_registry,
            single_table_mode: false,
        })
    }
}

pub(super) fn immediate_sqlite_test_config() -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(":memory:".to_string()),
        file_path: None,
        sqlite: Some(storage_provider::SqliteSettings {
            immediate_gsi_consistency: true,
            ..storage_provider::SqliteSettings::default()
        }),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}
