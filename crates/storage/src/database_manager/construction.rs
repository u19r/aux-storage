use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

use storage_provider::{StorageBackend, StorageConfig, StorageConnectionRegistry};
use storage_types::{StorageError, StorageResult, VecMap, context::ErrorContext as _};

use crate::{
    connection::Connection,
    database_manager::{
        DatabaseManager, DatabaseManagerRuntimeOptions, ROUTED_DEFAULT_CONNECTION_ID,
    },
    provider_bundle::{InitializedStore, open_store},
    tables::Tables,
};

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
            "storage connection '{connection_id}' delegates database maintenance to its remote \
             service"
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

/// Registry checks that run once, before any store opens.
pub(super) struct ConnectionPlan {
    registry: StorageConnectionRegistry,
    options: DatabaseManagerRuntimeOptions,
    destination_namespaces: HashMap<String, String>,
}

impl ConnectionPlan {
    pub(super) fn new(
        registry: StorageConnectionRegistry,
        options: DatabaseManagerRuntimeOptions,
    ) -> StorageResult<Self> {
        validate_maintenance_support(&registry, &options)?;
        if let Some(unknown) = options
            .regional_connections
            .keys()
            .find(|id| !registry.connections.contains_key(*id))
        {
            return Err(StorageError::validation(format!(
                "regional file for unknown connection '{unknown}'"
            )));
        }
        let destination_namespaces = if options.enable_database_jobs {
            destination_namespaces(&registry)?
        } else {
            HashMap::new()
        };
        Ok(Self {
            registry,
            options,
            destination_namespaces,
        })
    }

    pub(super) fn default_connection_id(&self) -> &str {
        &self.registry.default_connection_id
    }

    /// Open every configured store (the five-provider match), concurrently.
    pub(super) async fn open_stores(&self) -> StorageResult<HashMap<String, InitializedStore>> {
        let stores = futures::future::try_join_all(self.registry.connections.iter().map(
            |(id, connection)| async move {
                let store = open_store(connection.clone(), &self.options, id)
                    .await
                    .with_context(|| format!("create storage provider for connection {id}"))?;
                Ok::<_, StorageError>((id.clone(), store))
            },
        ))
        .await?;
        Ok(stores.into_iter().collect())
    }

    /// Build every connection and the manager. Creates no system table and
    /// starts no job.
    pub(super) async fn assemble(
        self,
        mut stores: HashMap<String, InitializedStore>,
    ) -> StorageResult<DatabaseManager> {
        if let Some(metrics_facade) = self.options.metrics_facade.clone() {
            metrics_facade::set_metrics_facade(metrics_facade);
        }
        let mut default_connection = None;
        let mut named_connections =
            VecMap::with_capacity(self.registry.connections.len().saturating_sub(1));
        for connection_id in self.registry.connections.keys() {
            let store = stores
                .remove(connection_id)
                .ok_or_else(|| StorageError::internal("initialized storage store is missing"))?;
            let destination_namespace = if self.options.enable_database_jobs {
                Some(
                    self.destination_namespaces
                        .get(connection_id)
                        .map(String::as_str)
                        .ok_or_else(|| {
                            StorageError::internal(
                                "maintenance destination namespace was not selected",
                            )
                        })?,
                )
            } else {
                None
            };
            let connection =
                Connection::open(connection_id, store, &self.options, destination_namespace)
                    .await?;
            if connection_id == &self.registry.default_connection_id {
                default_connection = Some(connection);
            } else {
                named_connections.insert(connection_id.clone(), connection);
            }
        }
        let default_connection = default_connection.ok_or_else(|| {
            StorageError::validation(format!(
                "default storage connection '{}' not found",
                self.registry.default_connection_id
            ))
        })?;
        Ok(DatabaseManager::from_connections(
            default_connection,
            named_connections,
            self.options.enable_single_table_mode,
        ))
    }
}

pub(super) fn single_connection_registry(config: StorageConfig) -> StorageConnectionRegistry {
    StorageConnectionRegistry {
        default_connection_id: ROUTED_DEFAULT_CONNECTION_ID.to_owned(),
        connections: HashMap::from([(ROUTED_DEFAULT_CONNECTION_ID.to_owned(), config)]),
    }
}

fn physical_destination_namespace(connection: &StorageConfig) -> StorageResult<String> {
    let identity = match &connection.backend_type {
        StorageBackend::SQLite => {
            let path = connection
                .local_path()
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
                .local_path()
                .ok_or_else(|| StorageError::validation("RocksDB destination path is missing"))?;
            format!("rocksdb:{}", canonical_local_destination(path)?)
        }
        StorageBackend::Postgres => canonical_postgres_destination(
            connection
                .postgres
                .as_ref()
                .map(|settings| settings.dsn.as_str())
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

impl DatabaseManager {
    pub(super) fn from_connections(
        default_connection: Connection,
        named_connections: VecMap<String, Connection>,
        single_table_mode: bool,
    ) -> Self {
        Self {
            point_reads: std::sync::OnceLock::new(),
            default_connection,
            named_connections,
            background_services: tokio::sync::Mutex::new(
                super::service_shutdown::BackgroundServiceState::Running(
                    super::service_shutdown::BackgroundServices::default(),
                ),
            ),
            single_table_mode,
        }
    }

    /// Assemble a manager around a caller-initialized store. The configuration
    /// identifies the physical destination for admission and job fencing.
    pub async fn from_initialized_store(
        config: StorageConfig,
        store: InitializedStore,
        runtime_options: DatabaseManagerRuntimeOptions,
    ) -> StorageResult<Self> {
        let plan = ConnectionPlan::new(single_connection_registry(config), runtime_options)?;
        let stores = HashMap::from([(plan.default_connection_id().to_owned(), store)]);
        let mut manager = plan.assemble(stores).await?;
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
        let plan = ConnectionPlan::new(registry, runtime_options)?;
        let stores = plan.open_stores().await?;
        let mut manager = plan.assemble(stores).await?;
        manager.finish_ordinary_initialization().await?;
        Ok(manager)
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
        let runtimes = std::iter::once(&mut self.default_connection)
            .chain(self.named_connections.values_mut())
            .filter_map(|connection| connection.maintenance.take())
            .collect::<Vec<_>>();
        let seeded = futures::future::try_join_all(
            runtimes
                .into_iter()
                .map(|runtime| runtime.initialize_seeds()),
        )
        .await?;
        let mut servers = Vec::new();
        for connection in self.connections() {
            if let Some(attachment) = &connection.regional {
                servers.push(
                    crate::regional::start_regional_server(&connection.id, attachment).await?,
                );
            }
        }
        self.background_services
            .get_mut()
            .publish_services(seeded, servers)
    }
}
