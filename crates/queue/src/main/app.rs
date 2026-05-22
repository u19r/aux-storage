use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use config::{
    RemoteBackendConfig, RemoteCredentialsConfig, RemoteStaticCredentialsConfig,
    RemoteTimeoutOverrides, load_optional_with_overrides,
};
use http_error::HttpApiError;
use metrics_exporter_prometheus::PrometheusHandle;
use queue::{
    ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityRequest, CreateQueueRequest,
    DeleteMessageBatchRequest, DeleteMessageRequest, DeleteQueueRequest, FoundationDbSettings,
    GetQueueAttributesRequest, GetQueueUrlRequest, ListQueuesRequest, PostgresSettings,
    PurgeQueueRequest, QueueBackend, QueueConfig as ProviderQueueConfig, QueueManager,
    QueueProvider, ReceiveMessageRequest, RemoteCredentialStrategy, RemoteQueueSettings,
    RemoteSigv4Settings, RemoteStaticCredentials, SendMessageBatchRequest, SendMessageRequest,
    SetQueueAttributesRequest, create_queue_provider,
};
use queue_provider::{
    SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE, SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
    SQS_MISSING_PARAMETER_ERROR_TYPE,
};
use serde::Serialize;
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use crate::protocol::{
    QueueAction, QueueProtocol, api_error_response, decode_request, error_response, ok_response,
    wire_error_response,
};

#[derive(Parser, Debug)]
#[command(name = "queue")]
pub(crate) struct Args {
    #[arg(long, value_enum)]
    pub(crate) storage: Option<QueueStorageArg>,
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    #[arg(long)]
    pub(crate) postgres_dsn: Option<String>,
    #[arg(long, default_value_t = 16)]
    pub(crate) postgres_max_pool_size: usize,
    #[arg(long, default_value_t = true)]
    pub(crate) postgres_tls: bool,
    #[arg(long)]
    pub(crate) host: Option<String>,
    #[arg(long)]
    pub(crate) port: Option<u16>,
    #[arg(long)]
    pub(crate) public_base_url: Option<String>,
    #[arg(long, default_value = "000000000000")]
    pub(crate) account_id: String,
    #[arg(long)]
    pub(crate) foundationdb_cluster_file: Option<String>,
    #[arg(long)]
    pub(crate) foundationdb_subspace_prefix: Option<String>,
    #[arg(long)]
    pub(crate) foundationdb_tenant_name: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub(crate) foundationdb_cache_read_version_ms: u16,
    #[arg(long)]
    pub(crate) foundationdb_report_conflicting_keys: bool,
    #[arg(long)]
    pub(crate) config: Option<PathBuf>,
    #[arg(long = "overrides", value_name = "PATH=VALUE")]
    pub(crate) overrides: Vec<String>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub(crate) enum QueueStorageArg {
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

impl From<QueueStorageArg> for QueueBackend {
    fn from(value: QueueStorageArg) -> Self {
        match value {
            QueueStorageArg::SQLite => QueueBackend::SQLite,
            QueueStorageArg::Turso => QueueBackend::Turso,
            QueueStorageArg::Postgres => QueueBackend::Postgres,
            QueueStorageArg::RocksDb => QueueBackend::RocksDB,
            QueueStorageArg::FoundationDb => QueueBackend::FoundationDb,
        }
    }
}

#[derive(Clone)]
struct AppState {
    manager: Arc<QueueManager>,
    public_base_url: String,
    account_id: String,
    metrics_handle: PrometheusHandle,
}

pub(crate) fn queue_url(base_url: &str, account_id: &str, queue_name: &str) -> String {
    format!(
        "{}/{}/{}",
        base_url.trim_end_matches('/'),
        account_id,
        queue_name
    )
}

pub(crate) async fn run_with_runtime_threads(
    runtime_worker_threads: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let metrics_handle =
        metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;
    let args = Args::parse();
    let config = load_queue_config_from_args(&args)?;
    let bind_addr = queue_bind_addr(&args, config.root.http.bind_addr.as_str())?;
    let public_base_url = args
        .public_base_url
        .clone()
        .unwrap_or_else(|| format!("http://{}:{}", bind_addr.ip(), bind_addr.port()));
    let storage = create_queue_provider(provider_queue_config_from_backends(
        &config.root.features.backends,
    )?)
    .await?;
    storage.initialize().await?;
    let storage: Arc<dyn QueueProvider> = Arc::from(storage);
    let manager = Arc::new(QueueManager::new(storage.clone()));
    let app_state = Arc::new(AppState {
        manager,
        public_base_url,
        account_id: args.account_id,
        metrics_handle,
    });

    let app = Router::new()
        .route("/", post(queue_endpoint))
        .route("/up", get(|| async { StatusCode::OK }))
        .route("/ready", get(ready))
        .route("/health", get(health))
        .route("/internal/metrics", get(metrics))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(
        addr = %bind_addr,
        runtime_worker_threads,
        "queue server listening"
    );
    axum::serve(listener, app).await?;
    Ok(())
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,queue=info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn load_queue_config_from_args(args: &Args) -> Result<Arc<config::Config>, std::io::Error> {
    let mut overrides = queue_config_overrides(args);
    overrides.extend(parse_override_args(args.overrides.as_slice())?);
    load_optional_with_overrides(args.config.as_deref(), overrides.as_slice())
        .map_err(|err| std::io::Error::other(err.to_string()))
}

fn queue_bind_addr(args: &Args, configured: &str) -> Result<SocketAddr, std::net::AddrParseError> {
    match (&args.host, args.port) {
        (Some(host), Some(port)) => format!("{host}:{port}").parse(),
        (Some(host), None) => format!("{host}:9324").parse(),
        (None, Some(port)) => format!("0.0.0.0:{port}").parse(),
        (None, None) => configured.parse(),
    }
}

fn queue_config_overrides(args: &Args) -> Vec<(String, String)> {
    let mut overrides = Vec::new();
    if let Some(storage) = args.storage.clone() {
        select_queue_backend(storage, &mut overrides);
    }
    if let Some(db_path) = &args.db_path {
        let path = match args.storage.as_ref().unwrap_or(&QueueStorageArg::SQLite) {
            QueueStorageArg::SQLite => "features.backends.sqlite.db_path",
            QueueStorageArg::Turso => "features.backends.turso.db_path",
            QueueStorageArg::RocksDb => "features.backends.rocksdb.db_path",
            QueueStorageArg::Postgres | QueueStorageArg::FoundationDb => {
                "features.backends.sqlite.db_path"
            }
        };
        overrides.push((path.to_string(), db_path.clone()));
    }
    let postgres_selected = args
        .storage
        .as_ref()
        .is_some_and(|storage| matches!(storage, QueueStorageArg::Postgres));
    if let Some(dsn) = &args.postgres_dsn {
        overrides.push(("features.backends.postgres.dsn".to_string(), dsn.clone()));
    }
    if postgres_selected || args.postgres_dsn.is_some() {
        overrides.push((
            "features.backends.postgres.max_pool_size".to_string(),
            args.postgres_max_pool_size.to_string(),
        ));
        overrides.push((
            "features.backends.postgres.tls".to_string(),
            args.postgres_tls.to_string(),
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
    if args.foundationdb_cache_read_version_ms > 0 {
        overrides.push((
            "features.backends.foundationdb.cache_read_version_ms".to_string(),
            args.foundationdb_cache_read_version_ms.to_string(),
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

fn select_queue_backend(storage: QueueStorageArg, overrides: &mut Vec<(String, String)>) {
    for backend_path in [
        "features.backends.sqlite",
        "features.backends.turso",
        "features.backends.postgres",
        "features.backends.rocksdb",
        "features.backends.foundationdb",
        "features.backends.remote",
    ] {
        overrides.push((backend_path.to_string(), "null".to_string()));
    }
    let selected = match storage {
        QueueStorageArg::SQLite => "features.backends.sqlite",
        QueueStorageArg::Turso => "features.backends.turso",
        QueueStorageArg::Postgres => "features.backends.postgres",
        QueueStorageArg::RocksDb => "features.backends.rocksdb",
        QueueStorageArg::FoundationDb => "features.backends.foundationdb",
    };
    overrides.push((selected.to_string(), "{}".to_string()));
}

fn parse_override_args(args: &[String]) -> Result<Vec<(String, String)>, std::io::Error> {
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

#[cfg(test)]
pub(crate) fn queue_config_from_args(args: &Args) -> Result<ProviderQueueConfig, std::io::Error> {
    if args
        .storage
        .as_ref()
        .is_some_and(|storage| matches!(storage, QueueStorageArg::Postgres))
        && args.postgres_dsn.as_deref().is_none_or(str::is_empty)
    {
        return Err(std::io::Error::other(
            "--postgres-dsn is required when --storage postgres",
        ));
    }
    let config = load_queue_config_from_args(args)?;
    provider_queue_config_from_backends(&config.root.features.backends)
}

fn provider_queue_config_from_backends(
    backends: &config::Backends,
) -> Result<ProviderQueueConfig, std::io::Error> {
    Ok(if let Some(sqlite) = &backends.sqlite {
        sqlite_queue_provider_config(sqlite)
    } else if let Some(turso) = &backends.turso {
        turso_queue_provider_config(turso)
    } else if let Some(postgres) = &backends.postgres {
        postgres_queue_provider_config(postgres)
    } else if let Some(rocksdb) = &backends.rocksdb {
        rocksdb_queue_provider_config(rocksdb)
    } else if let Some(foundationdb) = &backends.foundationdb {
        foundationdb_queue_provider_config(foundationdb)
    } else if let Some(remote) = &backends.remote {
        remote_queue_provider_config(remote)?
    } else {
        default_queue_provider_config()
    })
}

fn base_queue_provider_config(backend_type: QueueBackend) -> ProviderQueueConfig {
    ProviderQueueConfig {
        backend_type,
        connection_string: None,
        file_path: None,
        postgres: None,
        foundationdb: None,
        remote: None,
    }
}

pub(crate) fn sqlite_queue_provider_config(
    sqlite: &config::SqliteBackendConfig,
) -> ProviderQueueConfig {
    let mut config = base_queue_provider_config(QueueBackend::SQLite);
    config.connection_string = Some(sqlite.db_path.clone());
    config
}

pub(crate) fn turso_queue_provider_config(
    turso: &config::TursoBackendConfig,
) -> ProviderQueueConfig {
    let mut config = base_queue_provider_config(QueueBackend::Turso);
    config.connection_string = Some(turso.db_path.clone());
    config
}

pub(crate) fn postgres_queue_provider_config(
    postgres: &config::PostgresBackendConfig,
) -> ProviderQueueConfig {
    let mut config = base_queue_provider_config(QueueBackend::Postgres);
    config.connection_string = Some(postgres.dsn.clone());
    config.postgres = Some(PostgresSettings {
        dsn: postgres.dsn.clone(),
        max_pool_size: postgres.max_pool_size,
        tls: postgres.tls,
    });
    config
}

pub(crate) fn rocksdb_queue_provider_config(
    rocksdb: &config::RocksdbBackendConfig,
) -> ProviderQueueConfig {
    let mut config = base_queue_provider_config(QueueBackend::RocksDB);
    config.connection_string = Some(rocksdb.db_path.clone());
    config
}

pub(crate) fn foundationdb_queue_provider_config(
    foundationdb: &config::FoundationdbBackendConfig,
) -> ProviderQueueConfig {
    let mut config = base_queue_provider_config(QueueBackend::FoundationDb);
    config.foundationdb = Some(FoundationDbSettings {
        cluster_file: foundationdb.cluster_file.clone(),
        tenant_name: foundationdb.tenant_name.clone(),
        subspace_prefix: foundationdb.subspace_prefix.clone(),
        cache_read_version_ms: foundationdb.cache_read_version_ms,
        report_conflicting_keys: foundationdb.report_conflicting_keys,
    });
    config
}

pub(crate) fn remote_queue_provider_config(
    remote: &RemoteBackendConfig,
) -> Result<ProviderQueueConfig, std::io::Error> {
    let mut config = base_queue_provider_config(QueueBackend::Remote);
    config.remote = Some(remote_queue_settings(remote)?);
    Ok(config)
}

fn default_queue_provider_config() -> ProviderQueueConfig {
    let mut config = base_queue_provider_config(QueueBackend::SQLite);
    config.connection_string = Some("queue.sqlite3".to_string());
    config
}

fn remote_queue_settings(
    remote: &RemoteBackendConfig,
) -> Result<RemoteQueueSettings, std::io::Error> {
    let settings = RemoteQueueSettings {
        endpoint_urls: remote.endpoint_urls.clone(),
        region: remote.region.clone(),
        tls: remote.tls,
        credentials: remote_credentials(remote.credentials.as_ref())?,
        timeouts: remote.timeout_overrides.as_ref().map(remote_timeouts),
        sigv4: RemoteSigv4Settings {
            enabled: false,
            service_name: "sqs".to_string(),
        },
    };
    settings
        .validate()
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    Ok(settings)
}

fn remote_credentials(
    credentials: Option<&RemoteCredentialsConfig>,
) -> Result<RemoteCredentialStrategy, std::io::Error> {
    let Some(credentials) = credentials else {
        return Ok(RemoteCredentialStrategy::DefaultChain);
    };
    if let Some(static_credentials) = credentials.r#static.as_ref() {
        return Ok(RemoteCredentialStrategy::Static(remote_static_credentials(
            static_credentials,
        )));
    }
    Ok(RemoteCredentialStrategy::DefaultChain)
}

fn remote_static_credentials(
    credentials: &RemoteStaticCredentialsConfig,
) -> RemoteStaticCredentials {
    RemoteStaticCredentials {
        access_key_id: credentials.access_key.clone(),
        secret_access_key: credentials.secret_key.clone(),
        session_token: credentials.session_token.clone(),
    }
}

fn remote_timeouts(timeouts: &RemoteTimeoutOverrides) -> queue::RemoteTimeoutOverrides {
    queue::RemoteTimeoutOverrides {
        connect_timeout_ms: timeouts.connect_timeout_ms,
        request_timeout_ms: timeouts.request_timeout_ms,
    }
}

async fn ready() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ready" })))
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "healthy" })))
}

async fn metrics(State(app_state): State<Arc<AppState>>) -> impl IntoResponse {
    let body = app_state.metrics_handle.render();
    #[cfg(feature = "foundationdb")]
    let body = {
        let mut body = body;
        body.push_str(&kv::backends::fdb::foundationdb_operation_metrics_snapshot());
        body.push_str(&kv::queue::metrics::queue_operation_metrics_snapshot());
        body
    };
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

async fn queue_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = Uuid::now_v7().to_string();
    let wire_request = match decode_request(&headers, body) {
        Ok(request) => request,
        Err(error) => {
            return wire_error_response(&request_id, protocol_from_headers(&headers), error);
        }
    };
    let protocol = wire_request.protocol;
    let payload = wire_request.payload;

    match wire_request.action {
        QueueAction::CreateQueue => {
            handle_create_queue(&request_id, protocol, app_state.as_ref(), payload).await
        }
        QueueAction::DeleteQueue => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                DeleteQueueRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.delete_queue(req).await },
            )
            .await
        }
        QueueAction::ListQueues => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                ListQueuesRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.list_queues(req).await },
            )
            .await
        }
        QueueAction::GetQueueUrl => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                GetQueueUrlRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.get_queue_url(req).await },
            )
            .await
        }
        QueueAction::GetQueueAttributes => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                GetQueueAttributesRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.get_queue_attributes(req).await },
            )
            .await
        }
        QueueAction::SetQueueAttributes => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                SetQueueAttributesRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.set_queue_attributes(req).await },
            )
            .await
        }
        QueueAction::PurgeQueue => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                PurgeQueueRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.purge_queue(req).await },
            )
            .await
        }
        QueueAction::SendMessage => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                SendMessageRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.send_message(req).await },
            )
            .await
        }
        QueueAction::SendMessageBatch => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                SendMessageBatchRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.send_message_batch(req).await },
            )
            .await
        }
        QueueAction::ReceiveMessage => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                ReceiveMessageRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.receive_message(req).await },
            )
            .await
        }
        QueueAction::DeleteMessage => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                DeleteMessageRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move {
                    app_state.manager.delete_message(req).await?;
                    Ok(json!({}))
                },
            )
            .await
        }
        QueueAction::DeleteMessageBatch => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                DeleteMessageBatchRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.delete_message_batch(req).await },
            )
            .await
        }
        QueueAction::ChangeMessageVisibility => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                ChangeMessageVisibilityRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move {
                    app_state.manager.change_message_visibility(req).await?;
                    Ok(json!({}))
                },
            )
            .await
        }
        QueueAction::ChangeMessageVisibilityBatch => {
            handle_manager(
                &request_id,
                protocol,
                wire_request.action,
                ChangeMessageVisibilityBatchRequest::from_json(payload)
                    .map_err(|err| std::io::Error::other(err.message)),
                |req| async move { app_state.manager.change_message_visibility_batch(req).await },
            )
            .await
        }
    }
}

fn protocol_from_headers(headers: &HeaderMap) -> QueueProtocol {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if content_type.starts_with("application/x-www-form-urlencoded") {
        QueueProtocol::Query
    } else {
        QueueProtocol::Json
    }
}

async fn handle_create_queue(
    request_id: &str,
    protocol: QueueProtocol,
    app_state: &AppState,
    payload: Value,
) -> Response {
    let request = match CreateQueueRequest::from_json(payload) {
        Ok(request) => request,
        Err(err) => {
            let wire_error = validation_wire_error(&err.message);
            return error_response(
                request_id,
                protocol,
                StatusCode::BAD_REQUEST,
                wire_error.code,
                wire_error.message.as_str(),
            );
        }
    };
    let queue_url = queue_url(
        &app_state.public_base_url,
        &app_state.account_id,
        &request.queue_name,
    );
    match app_state
        .manager
        .create_queue_with_url(request, queue_url)
        .await
    {
        Ok(response) => ok_response(request_id, protocol, QueueAction::CreateQueue, &response),
        Err(err) => queue_error_response(request_id, protocol, err),
    }
}

async fn handle_manager<Request, Handler, Fut, T>(
    request_id: &str,
    protocol: QueueProtocol,
    action: QueueAction,
    request: Result<Request, std::io::Error>,
    handler: Handler,
) -> Response
where
    Handler: FnOnce(Request) -> Fut,
    Fut: std::future::Future<Output = queue::QueueResult<T>>,
    T: Serialize,
{
    let request = match request {
        Ok(request) => request,
        Err(err) => {
            let wire_error = validation_wire_error(&err.to_string());
            return error_response(
                request_id,
                protocol,
                StatusCode::BAD_REQUEST,
                wire_error.code,
                wire_error.message.as_str(),
            );
        }
    };
    match handler(request).await {
        Ok(value) => ok_response(request_id, protocol, action, &value),
        Err(err) => queue_error_response(request_id, protocol, err),
    }
}

struct ValidationWireError {
    code: &'static str,
    message: String,
}

fn validation_wire_error(message: &str) -> ValidationWireError {
    if message.starts_with("Id ") && message.ends_with(" repeated.") {
        return ValidationWireError {
            code: SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE,
            message: message.to_string(),
        };
    }
    if message == "The request must contain the parameter MessageBody." {
        return ValidationWireError {
            code: SQS_MISSING_PARAMETER_ERROR_TYPE,
            message: message.to_string(),
        };
    }

    ValidationWireError {
        code: SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
        message: message.to_string(),
    }
}

fn queue_error_response(
    request_id: &str,
    protocol: QueueProtocol,
    err: queue::QueueError,
) -> Response {
    api_error_response(request_id, protocol, HttpApiError::from(err))
}
