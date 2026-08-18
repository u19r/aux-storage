use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use config::load_optional_with_overrides;
use http_error::HttpApiError;
#[cfg(feature = "rocksdb")]
use kv::RocksDbKvStore;
#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
use kv::SortedKvDbStorageProvider;
#[cfg(feature = "foundationdb")]
use kv::{FoundationDbConfig, FoundationDbKvStore};
use pubsub::{
    PubsubError, PubsubManager, PubsubProvider, decode_query_request, render_query_api_error,
    render_query_success,
};
use queue::QueueManager;
use queue_provider::QueueProvider;
#[cfg(feature = "postgres")]
use sql::PostgresStorageProvider;
#[cfg(feature = "sqlite")]
use sql::SQLiteStorageProvider;
#[cfg(feature = "turso")]
use sql::TursoStorageProvider;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const DELIVERY_IDLE_INTERVAL: Duration = Duration::from_secs(1);
const DELIVERY_ERROR_INTERVAL: Duration = Duration::from_secs(5);
const DELIVERY_BATCH_LIMIT: usize = 100;

#[derive(Parser, Debug)]
#[command(name = "pubsub")]
pub(crate) struct Args {
    #[arg(long, value_enum)]
    pub(crate) storage: Option<PubsubStorageArg>,
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    #[arg(long)]
    pub(crate) host: Option<String>,
    #[arg(long)]
    pub(crate) port: Option<u16>,
    #[arg(long)]
    pub(crate) foundationdb_cluster_file: Option<String>,
    #[arg(long)]
    pub(crate) foundationdb_subspace_prefix: Option<String>,
    #[arg(long)]
    pub(crate) foundationdb_tenant_name: Option<String>,
    #[arg(long)]
    pub(crate) foundationdb_cache_read_version_ms: Option<u16>,
    #[arg(long)]
    pub(crate) foundationdb_report_conflicting_keys: bool,
    #[arg(long)]
    pub(crate) config: Option<PathBuf>,
    #[arg(long = "overrides", value_name = "PATH=VALUE")]
    pub(crate) overrides: Vec<String>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub(crate) enum PubsubStorageArg {
    #[value(name = "sqlite")]
    SQLite,
    #[value(name = "turso")]
    Turso,
    #[value(name = "postgres")]
    Postgres,
    #[value(name = "rocksdb")]
    RocksDb,
    #[value(name = "foundationdb")]
    FoundationDb,
}

#[derive(Clone)]
struct AppState {
    manager: Arc<PubsubManager>,
}

pub(crate) struct PubsubRuntimeProviders {
    pubsub: Arc<dyn PubsubProvider>,
    queue: Arc<dyn QueueProvider>,
}

fn shared_runtime_providers<P>(provider: P) -> PubsubRuntimeProviders
where P: PubsubProvider + QueueProvider + 'static {
    let provider = Arc::new(provider);
    PubsubRuntimeProviders {
        pubsub: provider.clone(),
        queue: provider,
    }
}

pub(crate) async fn initialize_pubsub_runtime(
    providers: PubsubRuntimeProviders,
) -> Result<PubsubManager, Box<dyn std::error::Error>> {
    providers.pubsub.initialize().await?;
    providers.queue.initialize().await?;
    let queue_manager = Arc::new(QueueManager::new(providers.queue));
    Ok(PubsubManager::builder()
        .provider(providers.pubsub)
        .queue_manager(queue_manager)
        .build()?)
}

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let args = Args::parse();
    let config = load_pubsub_config_from_args(&args)?;
    let bind_addr = pubsub_bind_addr(&args, config.root.http.bind_addr.as_str())?;
    let providers = create_pubsub_runtime_providers(&config.root.features.backends).await?;
    let manager = Arc::new(initialize_pubsub_runtime(providers).await?);
    let app_state = Arc::new(AppState {
        manager: manager.clone(),
    });

    let app = Router::new()
        .route("/", post(pubsub_endpoint))
        .route("/up", get(|| async { StatusCode::OK }))
        .route("/ready", get(ready))
        .route("/health", get(health))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(addr = %bind_addr, "pubsub server listening");
    let delivery_worker = spawn_delivery_worker(manager);
    let server_result = axum::serve(listener, app).await;
    delivery_worker.abort();
    server_result?;
    Ok(())
}

fn spawn_delivery_worker(manager: Arc<PubsubManager>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match manager.process_due_deliveries(DELIVERY_BATCH_LIMIT).await {
                Ok(0) => tokio::time::sleep(DELIVERY_IDLE_INTERVAL).await,
                Ok(_) => tokio::task::yield_now().await,
                Err(error) => {
                    tracing::warn!(error = %error, "pubsub delivery worker failed; retrying");
                    tokio::time::sleep(DELIVERY_ERROR_INTERVAL).await;
                }
            }
        }
    })
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,pubsub=info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

async fn ready() -> impl IntoResponse {
    (StatusCode::OK, "ready")
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "healthy")
}

async fn pubsub_endpoint(State(app_state): State<Arc<AppState>>, body: Bytes) -> Response {
    let request_id = Uuid::now_v7().to_string();
    let action = match decode_query_request(&body) {
        Ok(action) => action,
        Err(error) => return error_response(&error, &request_id),
    };
    match app_state.manager.execute_query_action(action).await {
        Ok(success) => {
            (StatusCode::OK, render_query_success(&success, &request_id)).into_response()
        }
        Err(error) => error_response(&error, &request_id),
    }
}

fn error_response(error: &PubsubError, request_id: &str) -> Response {
    let error = HttpApiError::from(error);
    let status =
        StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, render_query_api_error(&error, request_id)).into_response()
}

pub(crate) async fn create_pubsub_runtime_providers(
    backends: &config::Backends,
) -> Result<PubsubRuntimeProviders, Box<dyn std::error::Error>> {
    ensure_no_remote_pubsub_backend(backends)?;
    let selected = selected_pubsub_backend(backends);
    match selected {
        #[cfg(feature = "sqlite")]
        PubsubStorageArg::SQLite => create_sqlite_pubsub_provider(backends).await,
        #[cfg(not(feature = "sqlite"))]
        PubsubStorageArg::SQLite => {
            Err(std::io::Error::other("sqlite pubsub backend is not enabled in this build").into())
        }
        #[cfg(feature = "turso")]
        PubsubStorageArg::Turso => create_turso_pubsub_provider(backends).await,
        #[cfg(not(feature = "turso"))]
        PubsubStorageArg::Turso => {
            Err(std::io::Error::other("turso pubsub backend is not enabled in this build").into())
        }
        #[cfg(feature = "postgres")]
        PubsubStorageArg::Postgres => create_postgres_pubsub_provider(backends).await,
        #[cfg(not(feature = "postgres"))]
        PubsubStorageArg::Postgres => Err(std::io::Error::other(
            "postgres pubsub backend is not enabled in this build",
        )
        .into()),
        #[cfg(feature = "rocksdb")]
        PubsubStorageArg::RocksDb => create_rocksdb_pubsub_provider(backends),
        #[cfg(not(feature = "rocksdb"))]
        PubsubStorageArg::RocksDb => {
            Err(std::io::Error::other("rocksdb pubsub backend is not enabled in this build").into())
        }
        #[cfg(feature = "foundationdb")]
        PubsubStorageArg::FoundationDb => create_foundationdb_pubsub_provider(backends),
        #[cfg(not(feature = "foundationdb"))]
        PubsubStorageArg::FoundationDb => Err(std::io::Error::other(
            "foundationdb pubsub backend is not enabled in this build",
        )
        .into()),
    }
}

pub(crate) fn ensure_no_remote_pubsub_backend(
    backends: &config::Backends,
) -> Result<(), std::io::Error> {
    if let Some(remote) = &backends.remote
        && !remote.endpoint_urls.is_empty()
    {
        return Err(std::io::Error::other(
            "remote pubsub backend is configured but no remote SNS pubsub provider is linked in \
             this binary",
        ));
    }
    Ok(())
}

pub(crate) fn sqlite_pubsub_db_path(backends: &config::Backends) -> String {
    backends
        .sqlite
        .as_ref()
        .map(|sqlite| sqlite.db_path.clone())
        .unwrap_or_else(|| "storage.sqlite3".to_string())
}

#[cfg(any(test, feature = "turso"))]
pub(crate) fn turso_pubsub_db_path(backends: &config::Backends) -> String {
    backends
        .turso
        .as_ref()
        .map(|turso| turso.db_path.clone())
        .unwrap_or_else(|| "storage.turso.db".to_string())
}

#[cfg(any(test, feature = "rocksdb"))]
pub(crate) fn rocksdb_pubsub_db_path(backends: &config::Backends) -> String {
    backends
        .rocksdb
        .as_ref()
        .map(|rocksdb| rocksdb.db_path.clone())
        .unwrap_or_else(|| "storage.rocksdb".to_string())
}

#[cfg(feature = "sqlite")]
async fn create_sqlite_pubsub_provider(
    backends: &config::Backends,
) -> Result<PubsubRuntimeProviders, Box<dyn std::error::Error>> {
    let db_path = sqlite_pubsub_db_path(backends);
    Ok(shared_runtime_providers(
        SQLiteStorageProvider::new(&db_path).await?,
    ))
}

#[cfg(feature = "turso")]
async fn create_turso_pubsub_provider(
    backends: &config::Backends,
) -> Result<PubsubRuntimeProviders, Box<dyn std::error::Error>> {
    let db_path = turso_pubsub_db_path(backends);
    Ok(shared_runtime_providers(
        TursoStorageProvider::new(&db_path).await?,
    ))
}

#[cfg(feature = "postgres")]
async fn create_postgres_pubsub_provider(
    backends: &config::Backends,
) -> Result<PubsubRuntimeProviders, Box<dyn std::error::Error>> {
    let postgres = backends.postgres.as_ref().ok_or_else(|| {
        std::io::Error::other("postgres pubsub backend requires postgres settings")
    })?;
    let provider = PostgresStorageProvider::new_with_tls(
        &postgres.dsn,
        postgres.max_pool_size,
        postgres.background_max_pool_size,
        postgres.tls,
    )
    .await?;
    Ok(shared_runtime_providers(provider))
}

#[cfg(feature = "rocksdb")]
fn create_rocksdb_pubsub_provider(
    backends: &config::Backends,
) -> Result<PubsubRuntimeProviders, Box<dyn std::error::Error>> {
    let db_path = rocksdb_pubsub_db_path(backends);
    let store = RocksDbKvStore::new(db_path.into())?;
    Ok(shared_runtime_providers(SortedKvDbStorageProvider::new(
        store,
    )))
}

#[cfg(feature = "foundationdb")]
fn foundationdb_pubsub_config_from_backends(backends: &config::Backends) -> FoundationDbConfig {
    let mut fdb_config = FoundationDbConfig::default();
    let foundationdb = backends.foundationdb.as_ref();
    if let Some(cluster_file_path) = foundationdb.and_then(|cfg| cfg.cluster_file.clone()) {
        fdb_config.cluster_file_path = Some(cluster_file_path);
    }
    if let Some(prefix) = foundationdb.and_then(|cfg| cfg.subspace_prefix.clone()) {
        fdb_config.subspace_prefix = Some(prefix.into_bytes());
    }
    if let Some(tenant) = foundationdb.and_then(|cfg| cfg.tenant_name.clone()) {
        fdb_config.tenant_name = Some(tenant.into_bytes());
    }
    if let Some(foundationdb) = foundationdb {
        fdb_config.cache_read_version_ms = foundationdb.cache_read_version_ms;
        fdb_config.report_conflicting_keys = foundationdb.report_conflicting_keys;
    }
    fdb_config
}

#[cfg(feature = "foundationdb")]
fn create_foundationdb_pubsub_provider(
    backends: &config::Backends,
) -> Result<PubsubRuntimeProviders, Box<dyn std::error::Error>> {
    let store = FoundationDbKvStore::connect(foundationdb_pubsub_config_from_backends(backends))?;
    Ok(shared_runtime_providers(SortedKvDbStorageProvider::new(
        store,
    )))
}

fn load_pubsub_config_from_args(args: &Args) -> Result<Arc<config::Config>, std::io::Error> {
    let mut overrides = pubsub_config_overrides(args);
    overrides.extend(parse_override_args(args.overrides.as_slice())?);
    load_optional_with_overrides(args.config.as_deref(), overrides.as_slice())
        .map_err(|err| std::io::Error::other(err.to_string()))
}

pub(crate) fn pubsub_bind_addr(
    args: &Args,
    configured: &str,
) -> Result<SocketAddr, std::net::AddrParseError> {
    match (&args.host, args.port) {
        (Some(host), Some(port)) => format!("{host}:{port}").parse(),
        (Some(host), None) => format!("{host}:9466").parse(),
        (None, Some(port)) => format!("0.0.0.0:{port}").parse(),
        (None, None) => configured.parse(),
    }
}

pub(crate) fn pubsub_config_overrides(args: &Args) -> Vec<(String, String)> {
    let mut overrides = Vec::new();
    if let Some(storage) = args.storage.clone() {
        select_pubsub_backend(storage, &mut overrides);
    }
    if let Some(db_path) = &args.db_path {
        overrides.push((
            pubsub_db_path_override_path(args.storage.as_ref()).to_string(),
            db_path.clone(),
        ));
    }
    if let Some(cluster_file) = &args.foundationdb_cluster_file {
        overrides.push((
            "features.backends.foundationdb.cluster_file".to_string(),
            cluster_file.clone(),
        ));
    }
    if let Some(prefix) = &args.foundationdb_subspace_prefix {
        overrides.push((
            "features.backends.foundationdb.subspace_prefix".to_string(),
            prefix.clone(),
        ));
    }
    if let Some(tenant) = &args.foundationdb_tenant_name {
        overrides.push((
            "features.backends.foundationdb.tenant_name".to_string(),
            tenant.clone(),
        ));
    }
    if let Some(cache_read_version_ms) = args.foundationdb_cache_read_version_ms {
        overrides.push((
            "features.backends.foundationdb.cache_read_version_ms".to_string(),
            cache_read_version_ms.to_string(),
        ));
    }
    if args.foundationdb_report_conflicting_keys {
        overrides.push((
            "features.backends.foundationdb.report_conflicting_keys".to_string(),
            args.foundationdb_report_conflicting_keys.to_string(),
        ));
    }
    overrides
}

fn select_pubsub_backend(storage: PubsubStorageArg, overrides: &mut Vec<(String, String)>) {
    for backend_path in pubsub_backend_override_paths() {
        overrides.push((backend_path.to_string(), "null".to_string()));
    }
    overrides.push((
        pubsub_backend_override_path(&storage).to_string(),
        "{}".to_string(),
    ));
}

pub(crate) fn parse_override_args(
    args: &[String],
) -> Result<Vec<(String, String)>, std::io::Error> {
    args.iter()
        .map(|arg| {
            let (path, value) = arg
                .split_once('=')
                .ok_or_else(|| std::io::Error::other("override must use PATH=VALUE format"))?;
            if path.trim().is_empty() {
                return Err(std::io::Error::other("override path must not be empty"));
            }
            Ok((path.to_string(), value.to_string()))
        })
        .collect()
}

pub(crate) fn selected_pubsub_backend(backends: &config::Backends) -> PubsubStorageArg {
    if backends.turso.is_some() {
        PubsubStorageArg::Turso
    } else if backends.postgres.is_some() {
        PubsubStorageArg::Postgres
    } else if backends.rocksdb.is_some() {
        PubsubStorageArg::RocksDb
    } else if backends.foundationdb.is_some() {
        PubsubStorageArg::FoundationDb
    } else {
        PubsubStorageArg::SQLite
    }
}

fn pubsub_backend_override_paths() -> [&'static str; 6] {
    [
        "features.backends.sqlite",
        "features.backends.turso",
        "features.backends.postgres",
        "features.backends.rocksdb",
        "features.backends.foundationdb",
        "features.backends.remote",
    ]
}

pub(crate) fn pubsub_backend_override_path(storage: &PubsubStorageArg) -> &'static str {
    match storage {
        PubsubStorageArg::SQLite => "features.backends.sqlite",
        PubsubStorageArg::Turso => "features.backends.turso",
        PubsubStorageArg::Postgres => "features.backends.postgres",
        PubsubStorageArg::RocksDb => "features.backends.rocksdb",
        PubsubStorageArg::FoundationDb => "features.backends.foundationdb",
    }
}

pub(crate) fn pubsub_db_path_override_path(storage: Option<&PubsubStorageArg>) -> &'static str {
    match storage.unwrap_or(&PubsubStorageArg::SQLite) {
        PubsubStorageArg::SQLite => "features.backends.sqlite.db_path",
        PubsubStorageArg::Turso => "features.backends.turso.db_path",
        PubsubStorageArg::Postgres => "features.backends.sqlite.db_path",
        PubsubStorageArg::RocksDb => "features.backends.rocksdb.db_path",
        PubsubStorageArg::FoundationDb => "features.backends.sqlite.db_path",
    }
}
