use std::{collections::HashMap, sync::Arc};

use storage_provider::{
    StorageBackend, StorageConfig, StorageConnectionConfig, StorageConnectionRegistry,
    StorageProvider,
};
use storage_types::{
    ReadSequenceProviderCapabilities, StorageError, StorageResult, context::ErrorContext as _,
};
use stream::StreamProvider;
use tokio::sync::RwLock;

use crate::{
    admission::AdmissionRegistry,
    cache_coordinator::{StorageAuthoritativeCacheOptions, StorageCacheServices},
    create_storage_provider_bundle,
    database_manager::{DatabaseManager, DatabaseManagerRuntimeOptions},
    namespace_routing::{CutoverWatcher, NamespaceRequestRewriter, NamespaceRouteResolver},
    newtypes::DatabaseTrait,
    point_read_cache::{PointReadCache, noop_point_read_cache},
    query_proof_cache::{QueryProofCache, noop_query_proof_cache},
    tables::Tables,
};

impl DatabaseManager {
    pub async fn new_for_test() -> StorageResult<Self> {
        Self::new_for_test_with_runtime_options(DatabaseManagerRuntimeOptions::default()).await
    }

    pub async fn new_for_test_with_config(config: StorageConfig) -> StorageResult<Self> {
        let mut manager = Self::new_with_config_and_runtime_options_and_caches(
            config,
            DatabaseManagerRuntimeOptions::default(),
            noop_point_read_cache(),
            noop_query_proof_cache(),
        )
        .await?;
        manager.run_gsi_maintenance = true;
        Tables::create_sys_jobs_table(&manager).await?;
        Ok(manager)
    }

    pub async fn new_for_test_with_runtime_options(
        runtime_options: DatabaseManagerRuntimeOptions,
    ) -> StorageResult<Self> {
        let database_path = ":memory:".to_string();

        let config = StorageConfig {
            backend_type: StorageBackend::SQLite,
            connection_string: Some(database_path),
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        };

        let mut manager = Self::new_with_config_and_runtime_options_and_caches(
            config,
            runtime_options,
            noop_point_read_cache(),
            noop_query_proof_cache(),
        )
        .await?;
        manager.run_gsi_maintenance = true;
        Tables::create_sys_jobs_table(&manager).await?;
        Ok(manager)
    }

    #[allow(dead_code)]
    pub(crate) async fn new_for_test_with_caches(
        point_read_cache: Arc<dyn PointReadCache>,
        query_proof_cache: Arc<dyn QueryProofCache>,
    ) -> StorageResult<Self> {
        let database_path = ":memory:".to_string();

        let config = StorageConfig {
            backend_type: StorageBackend::SQLite,
            connection_string: Some(database_path),
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        };

        let mut manager = Self::new_with_config_and_runtime_options_and_caches(
            config,
            DatabaseManagerRuntimeOptions::default(),
            point_read_cache,
            query_proof_cache,
        )
        .await?;
        manager.run_gsi_maintenance = true;
        Tables::create_sys_jobs_table(&manager).await?;
        Ok(manager)
    }

    #[allow(dead_code)]
    pub(crate) async fn new_for_test_with_point_read_cache(
        point_read_cache: Arc<dyn PointReadCache>,
    ) -> StorageResult<Self> {
        Self::new_for_test_with_caches(point_read_cache, noop_query_proof_cache()).await
    }

    pub async fn new_with_config(config: StorageConfig) -> StorageResult<Self> {
        Self::new_with_config_and_background_refresh(config, true).await
    }

    pub async fn new_with_config_and_background_refresh(
        config: StorageConfig,
        enable_background_refresh: bool,
    ) -> StorageResult<Self> {
        Self::new_with_config_and_runtime_options(
            config,
            DatabaseManagerRuntimeOptions {
                enable_background_refresh,
                ..DatabaseManagerRuntimeOptions::default()
            },
        )
        .await
    }

    pub async fn new_with_config_and_runtime_options(
        config: StorageConfig,
        runtime_options: DatabaseManagerRuntimeOptions,
    ) -> StorageResult<Self> {
        Self::new_with_config_and_runtime_options_and_point_read_cache(
            config,
            runtime_options,
            noop_point_read_cache(),
        )
        .await
    }

    pub(crate) async fn new_with_config_and_runtime_options_and_point_read_cache(
        config: StorageConfig,
        runtime_options: DatabaseManagerRuntimeOptions,
        point_read_cache: Arc<dyn PointReadCache>,
    ) -> StorageResult<Self> {
        Self::new_with_config_and_runtime_options_and_caches(
            config,
            runtime_options,
            point_read_cache,
            noop_query_proof_cache(),
        )
        .await
    }

    pub(crate) async fn new_with_config_and_runtime_options_and_caches(
        config: StorageConfig,
        runtime_options: DatabaseManagerRuntimeOptions,
        point_read_cache: Arc<dyn PointReadCache>,
        query_proof_cache: Arc<dyn QueryProofCache>,
    ) -> StorageResult<Self> {
        let registry = StorageConnectionRegistry {
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
        };
        Self::new_with_connection_registry_and_runtime_options_and_caches(
            registry,
            runtime_options,
            point_read_cache,
            query_proof_cache,
        )
        .await
    }

    pub async fn new_with_connection_registry(
        registry: StorageConnectionRegistry,
    ) -> StorageResult<Self> {
        Self::new_with_connection_registry_and_background_refresh(registry, true).await
    }

    pub async fn new_with_connection_registry_and_background_refresh(
        registry: StorageConnectionRegistry,
        enable_background_refresh: bool,
    ) -> StorageResult<Self> {
        Self::new_with_connection_registry_and_runtime_options(
            registry,
            DatabaseManagerRuntimeOptions {
                enable_background_refresh,
                ..DatabaseManagerRuntimeOptions::default()
            },
        )
        .await
    }

    pub async fn new_with_connection_registry_and_runtime_options(
        registry: StorageConnectionRegistry,
        runtime_options: DatabaseManagerRuntimeOptions,
    ) -> StorageResult<Self> {
        Self::new_with_connection_registry_and_runtime_options_and_point_read_cache(
            registry,
            runtime_options,
            noop_point_read_cache(),
        )
        .await
    }

    pub(crate) async fn new_with_connection_registry_and_runtime_options_and_point_read_cache(
        registry: StorageConnectionRegistry,
        runtime_options: DatabaseManagerRuntimeOptions,
        point_read_cache: Arc<dyn PointReadCache>,
    ) -> StorageResult<Self> {
        Self::new_with_connection_registry_and_runtime_options_and_caches(
            registry,
            runtime_options,
            point_read_cache,
            noop_query_proof_cache(),
        )
        .await
    }

    pub(crate) async fn new_with_connection_registry_and_runtime_options_and_caches(
        registry: StorageConnectionRegistry,
        runtime_options: DatabaseManagerRuntimeOptions,
        point_read_cache: Arc<dyn PointReadCache>,
        query_proof_cache: Arc<dyn QueryProofCache>,
    ) -> StorageResult<Self> {
        if let Some(metrics_facade) = runtime_options.metrics_facade.clone() {
            metrics_facade::set_metrics_facade(metrics_facade);
        }

        let default_connection = registry
            .connections
            .get(&registry.default_connection_id)
            .ok_or_else(|| {
                StorageError::validation(format!(
                    "default storage connection '{}' not found",
                    registry.default_connection_id
                ))
            })?;
        let supports_multi_region_replication_control_plane = default_connection
            .backend_type
            .supports_multi_region_replication_control_plane();
        let supports_read_sequence_mapped_range =
            connection_supports_read_sequence_mapped_range(default_connection);
        let admission_configs = registry
            .connections
            .iter()
            .map(|(connection_id, connection)| {
                admission_config_for_connection(runtime_options.admission_config, connection)
                    .map(|config| (connection_id.clone(), config))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let admission_registry = AdmissionRegistry::new_with_connection_configs(
            registry.default_connection_id.clone(),
            admission_configs,
            runtime_options.admission_config,
        )
        .map_err(|error| StorageError::validation(error.to_string()))?;
        let default_admission_controller = admission_registry.default_controller().clone();
        let read_sequence_capabilities =
            read_sequence_capabilities_for_connection(default_connection);
        let mut providers: HashMap<String, Arc<dyn DatabaseTrait>> =
            HashMap::with_capacity(registry.connections.len());
        let mut background_providers: HashMap<String, Arc<dyn DatabaseTrait>> =
            HashMap::with_capacity(registry.connections.len());
        let mut queue_providers: HashMap<String, Arc<dyn queue_provider::QueueProvider>> =
            HashMap::with_capacity(registry.connections.len());
        let mut pubsub_providers: HashMap<String, Arc<dyn pubsub_provider::PubsubProvider>> =
            HashMap::with_capacity(registry.connections.len());

        for (connection_id, connection) in &registry.connections {
            let config = StorageConfig {
                backend_type: connection.backend_type.clone(),
                connection_string: connection.connection_string.clone(),
                file_path: connection.file_path.clone(),
                sqlite: connection.sqlite.clone(),
                postgres: connection.postgres.clone(),
                turso: connection.turso.clone(),
                rocksdb: connection.rocksdb.clone(),
                foundationdb: connection.foundationdb.clone(),
                remote: connection.remote.clone(),
            };
            let bundle = create_storage_provider_bundle(
                config,
                runtime_options.enable_database_jobs,
                runtime_options.database_job_intervals,
            )
            .await
            .with_context(|| format!("create storage provider for connection {connection_id}"))?;
            let background_provider = bundle.background_database;
            let provider = bundle.database;
            provider
                .initialize_storage()
                .await
                .with_context(|| format!("initialize storage for connection {connection_id}"))?;
            provider
                .initialize_stream()
                .await
                .map_err(|error| StorageError::internal(&error.to_string()))
                .with_context(|| format!("initialize stream for connection {connection_id}"))?;
            if let Some(queue_provider) = bundle.queue {
                queue_providers.insert(connection_id.clone(), queue_provider);
            }
            if let Some(pubsub_provider) = bundle.pubsub {
                pubsub_providers.insert(connection_id.clone(), pubsub_provider);
            }
            if let Some(background_provider) = background_provider {
                background_providers.insert(connection_id.clone(), background_provider);
            }
            providers.insert(connection_id.clone(), Arc::from(provider));
        }

        let default_provider = providers
            .get(&registry.default_connection_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::validation(format!(
                    "default storage connection '{}' not found",
                    registry.default_connection_id
                ))
            })?;
        let default_background_provider = background_providers
            .get(&registry.default_connection_id)
            .cloned();

        let run_gsi_maintenance = runtime_options
            .run_gsi_maintenance_after_write
            .unwrap_or_else(|| {
                std::env::var("AUX_GSI_MAINTENANCE")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false)
            });

        let route_resolver = Arc::new(NamespaceRouteResolver::new(
            registry.default_connection_id.clone(),
            Arc::clone(&default_provider),
            runtime_options.enable_background_refresh,
        ));
        // Start the watcher only after the manager has created its system
        // tables. The watcher performs a control-plane query immediately;
        // starting it here races that query with creation of `sys` and can
        // consume the only foreground connection in a one-connection pool.
        let mut manager = Self {
            storage: default_provider,
            background_storage: default_background_provider,
            queue_provider: queue_providers
                .get(&registry.default_connection_id)
                .cloned(),
            pubsub_provider: pubsub_providers
                .get(&registry.default_connection_id)
                .cloned(),
            connection_registry: Some(providers),
            admission_registry,
            route_resolver: Some(route_resolver),
            request_rewriter: NamespaceRequestRewriter::new(),
            single_node_sync_mode: runtime_options.enable_single_node_sync_mode,
            single_table_mode: runtime_options.enable_single_table_mode,
            cache_services: StorageCacheServices::new(
                point_read_cache,
                query_proof_cache,
                runtime_options.authoritative_cache_options,
            ),
            cutover_watcher_task: None,
            run_gsi_maintenance,
            #[cfg(all(test, feature = "cache-write-planner"))]
            pause_after_storage_write: runtime_options.pause_after_storage_write.clone(),
            supports_multi_region_replication_control_plane,
            supports_read_sequence_mapped_range,
            read_sequence_capabilities,
            table_info_cache: RwLock::new(HashMap::new()),
        };
        Tables::create_sys_namespaces_table(&manager)
            .await
            .context("create sys namespaces table during database manager construction")?;
        // Best effort preload to warm ST routes on process boot. This is a
        // control-plane query and must use the reserved admission lane.
        if let Some(route_resolver) = manager.route_resolver.as_ref().cloned() {
            let _ = manager
                .run_control_admitted(
                    registry.default_connection_id.as_str(),
                    move |_provider| async move {
                        route_resolver.preload_shared_table_namespaces().await
                    },
                )
                .await;
        }
        Tables::create_sys_analytics_table(&manager)
            .await
            .context("create sys analytics table during database manager construction")?;
        Tables::create_sys_jobs_table(&manager)
            .await
            .context("create sys jobs table during database manager construction")?;
        manager
            .maybe_create_sys_storage_replication_table()
            .await
            .context("create sys storage replication table during database manager construction")?;
        if let Some(providers) = manager.connection_registry.as_ref() {
            for provider in providers.values() {
                provider
                    .complete_initialization()
                    .await
                    .context("complete storage provider initialization")?;
            }
        }
        if runtime_options.enable_background_watchers
            && let Some(route_resolver) = manager.route_resolver.as_ref().cloned()
        {
            manager.cutover_watcher_task = Some(
                Arc::new(CutoverWatcher::new(
                    route_resolver,
                    Arc::clone(&manager.storage),
                    default_admission_controller,
                ))
                .start(),
            );
        }
        Ok(manager)
    }

    /// Return the provider for bounded background/control work which cannot
    /// use the request-oriented `DatabaseManager` methods directly.
    ///
    /// Postgres returns a provider backed by its independent background pool;
    /// backends without a separate pool use the foreground provider. Foreground
    /// request paths must use an admitted provider handle instead.
    #[must_use]
    pub fn maintenance_provider(&self) -> Arc<dyn StorageProvider> {
        self.background_storage
            .clone()
            .map(|provider| provider as Arc<dyn StorageProvider>)
            .unwrap_or_else(|| self.storage.clone())
    }

    pub fn new_with_mocks<T>(storage: Arc<T>) -> StorageResult<Self>
    where T: StorageProvider + StreamProvider + 'static {
        let storage: Arc<dyn DatabaseTrait> = storage;
        let admission_registry = AdmissionRegistry::new(
            "default",
            ["default".to_string()],
            crate::admission::AdmissionConfig::default(),
        )
        .map_err(|error| StorageError::validation(error.to_string()))?;
        Ok(Self {
            storage,
            background_storage: None,
            queue_provider: None,
            pubsub_provider: None,
            connection_registry: None,
            admission_registry,
            route_resolver: None,
            request_rewriter: NamespaceRequestRewriter::new(),
            single_node_sync_mode: false,
            single_table_mode: false,
            cache_services: StorageCacheServices::new(
                noop_point_read_cache(),
                noop_query_proof_cache(),
                StorageAuthoritativeCacheOptions::default(),
            ),
            cutover_watcher_task: None,
            run_gsi_maintenance: true,
            #[cfg(all(test, feature = "cache-write-planner"))]
            pause_after_storage_write: None,
            supports_multi_region_replication_control_plane: true,
            supports_read_sequence_mapped_range: false,
            read_sequence_capabilities: ReadSequenceProviderCapabilities::default(),
            table_info_cache: RwLock::new(HashMap::new()),
        })
    }
}

fn connection_supports_read_sequence_mapped_range(connection: &StorageConnectionConfig) -> bool {
    matches!(connection.backend_type, StorageBackend::FoundationDb)
}

pub(super) fn read_sequence_capabilities_for_connection(
    connection: &StorageConnectionConfig,
) -> ReadSequenceProviderCapabilities {
    ReadSequenceProviderCapabilities {
        eventual_reads: true,
        strong_reads: true,
        transactional_reads: !matches!(connection.backend_type, StorageBackend::Remote),
        transactional_snapshots: connection_transactional_snapshots(connection),
        immediate_gsi_consistency: connection_immediate_gsi_consistency(connection),
    }
}

fn connection_transactional_snapshots(connection: &StorageConnectionConfig) -> bool {
    match connection.backend_type {
        StorageBackend::SQLite => sqlite_file_backed_snapshots_supported(connection),
        StorageBackend::Postgres => true,
        StorageBackend::Turso => true,
        StorageBackend::RocksDB | StorageBackend::FoundationDb => true,
        StorageBackend::Remote => false,
    }
}

fn sqlite_file_backed_snapshots_supported(connection: &StorageConnectionConfig) -> bool {
    let path = connection.connection_string.as_deref().unwrap_or("main.db");
    if path == ":memory:" {
        return false;
    }
    let force_file_backed = connection
        .sqlite
        .as_ref()
        .is_some_and(|settings| settings.force_file_backed_database);
    force_file_backed
        || (!cfg!(test) && std::env::var("RUST_TEST_THREADS").is_err() && !path.contains("test"))
}

fn connection_immediate_gsi_consistency(connection: &StorageConnectionConfig) -> bool {
    match connection.backend_type {
        StorageBackend::SQLite => connection
            .sqlite
            .as_ref()
            .is_some_and(|settings| settings.immediate_gsi_consistency),
        StorageBackend::Turso => connection
            .turso
            .as_ref()
            .is_some_and(|settings| settings.immediate_gsi_consistency),
        StorageBackend::Postgres => connection
            .postgres
            .as_ref()
            .is_some_and(|settings| settings.immediate_gsi_consistency),
        StorageBackend::RocksDB => connection
            .rocksdb
            .as_ref()
            .is_some_and(|settings| settings.immediate_gsi_consistency),
        StorageBackend::FoundationDb => connection
            .foundationdb
            .as_ref()
            .is_some_and(|settings| settings.immediate_gsi_consistency),
        StorageBackend::Remote => false,
    }
}

pub(super) fn admission_config_for_connection(
    common: crate::admission::AdmissionConfig,
    connection: &StorageConnectionConfig,
) -> StorageResult<crate::admission::AdmissionConfig> {
    if !matches!(connection.backend_type, StorageBackend::Postgres) {
        return Ok(common);
    }

    let pool_size = connection
        .postgres
        .as_ref()
        .map(|settings| settings.max_pool_size)
        .ok_or_else(|| StorageError::validation("postgres connection is missing pool settings"))?;
    if pool_size == 0 {
        return Err(StorageError::validation(
            "postgres max_pool_size must be greater than zero",
        ));
    }

    let effective_maximum = common.effective_maximum().min(pool_size);
    if effective_maximum < common.minimum_concurrency {
        return Err(StorageError::validation(format!(
            "postgres max_pool_size ({pool_size}) is below admission minimum_concurrency ({})",
            common.minimum_concurrency
        )));
    }
    let maximum_concurrency = effective_maximum
        .checked_add(common.control_reserve_concurrency)
        .ok_or_else(|| StorageError::validation("postgres admission maximum overflow"))?;
    crate::admission::AdmissionConfig::try_new(crate::admission::AdmissionConfig {
        maximum_concurrency,
        ..common
    })
    .map_err(|error| StorageError::validation(error.to_string()))
}
