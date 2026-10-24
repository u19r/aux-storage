use std::{collections::HashMap, error::Error, path::Path, sync::Arc};

use axum::Router;
use metrics_exporter_prometheus::PrometheusBuilder;
use storage::{AdmissionConfig, DatabaseManager, DatabaseManagerRuntimeOptions};
use storage_api::{
    AppState, MetricsEndpointConfig, PrometheusMetricsEndpointConfig, server_router_with_metrics,
};
use storage_provider::{
    RemoteCredentialStrategy, RemoteStorageSettings, RemoteTimeoutOverrides, SqliteSettings,
    StorageBackend, StorageConfig, StorageConnectionConfig, StorageConnectionRegistry,
};
use tokio::{net::TcpListener, task::JoinHandle};

pub type FixtureResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub struct RunningApi {
    pub base_url: String,
    pub storage_url: String,
    task: JoinHandle<()>,
    _database: Arc<DatabaseManager>,
}

impl Drop for RunningApi {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct RunningFixture {
    pub origin: RunningApi,
    pub remote: RunningApi,
    pub front_root: String,
    pub origin_queue_endpoint: String,
}

/// Builds both provider-backed API hosts because the standalone production
/// launcher intentionally rejects a top-level Remote backend.
pub async fn start_fixture(database_path: &Path) -> FixtureResult<RunningFixture> {
    let origin = start_api(
        sqlite_config(database_path),
        DatabaseManagerRuntimeOptions::default(),
        true,
    )
    .await?;
    let remote = start_remote_api(&origin.base_url, admission_test_runtime_options()).await?;
    let front_root = remote.base_url.clone();
    let origin_queue_endpoint = format!("{}/queue", origin.base_url);
    Ok(RunningFixture {
        origin,
        remote,
        front_root,
        origin_queue_endpoint,
    })
}

fn admission_test_runtime_options() -> DatabaseManagerRuntimeOptions {
    DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .admission_config(AdmissionConfig {
            enabled: true,
            initial_sustainable_throughput_rps: 4,
            initial_latency_estimate_ms: 1,
            minimum_concurrency: 1,
            maximum_concurrency: 2,
            control_reserve_concurrency: 1,
            queue_capacity: 1,
            max_queue_wait_ms: 100,
        })
        .build()
}

#[allow(dead_code)]
pub fn remote_config(origin_endpoint: &str) -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::Remote,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(remote_settings(origin_endpoint)),
    }
}

pub fn remote_connection_registry(origin_endpoint: &str) -> StorageConnectionRegistry {
    let connection = |origin_endpoint| StorageConnectionConfig {
        backend_type: StorageBackend::Remote,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(remote_settings(origin_endpoint)),
    };
    StorageConnectionRegistry {
        default_connection_id: "primary".to_string(),
        connections: HashMap::from([
            ("primary".to_string(), connection(origin_endpoint)),
            ("secondary".to_string(), connection(origin_endpoint)),
        ]),
    }
}

fn remote_settings(origin_endpoint: &str) -> RemoteStorageSettings {
    RemoteStorageSettings {
        endpoint_urls: vec![origin_endpoint.to_string()],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: Some(RemoteTimeoutOverrides {
            connect_timeout_ms: Some(500),
            request_timeout_ms: Some(5_000),
        }),
    }
}

fn sqlite_config(database_path: &Path) -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(database_path.to_string_lossy().into_owned()),
        file_path: None,
        sqlite: Some(SqliteSettings::default()),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

async fn start_api(
    config: StorageConfig,
    runtime_options: DatabaseManagerRuntimeOptions,
    enable_queue: bool,
) -> FixtureResult<RunningApi> {
    let database = Arc::new(
        DatabaseManager::new_with_config_and_runtime_options(config, runtime_options).await?,
    );
    start_api_with_database(database, enable_queue).await
}

async fn start_remote_api(
    origin_endpoint: &str,
    runtime_options: DatabaseManagerRuntimeOptions,
) -> FixtureResult<RunningApi> {
    let database = Arc::new(
        DatabaseManager::new_with_connection_registry_and_runtime_options(
            remote_connection_registry(origin_endpoint),
            runtime_options,
        )
        .await?,
    );
    start_api_with_database(database, false).await
}

async fn start_api_with_database(
    database: Arc<DatabaseManager>,
    enable_queue: bool,
) -> FixtureResult<RunningApi> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let mut app_state = AppState::new(database.clone());
    if enable_queue {
        let queue_provider = database
            .initialization_queue_provider()
            .ok_or("SQLite origin does not expose a queue provider")?;
        app_state = app_state.with_messaging(
            Some(Arc::new(queue::QueueManager::new(queue_provider))),
            format!("http://{address}"),
            "000000000000".to_string(),
            #[cfg(feature = "pubsub")]
            None,
        );
    }
    let app_state = Arc::new(app_state);
    let metrics_handle = PrometheusBuilder::new().build_recorder().handle();
    let router = server_router_with_metrics(
        app_state,
        true,
        MetricsEndpointConfig {
            enabled: true,
            prometheus: Some(PrometheusMetricsEndpointConfig {
                handle: metrics_handle,
            }),
        },
    );
    let task = tokio::spawn(serve(listener, router));
    let base_url = format!("http://{address}");
    Ok(RunningApi {
        storage_url: format!("{base_url}/storage"),
        base_url,
        task,
        _database: database,
    })
}

async fn serve(listener: TcpListener, router: Router) {
    let _ = axum::serve(listener, router).await;
}
