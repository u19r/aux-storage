use std::sync::Arc;

use axum::{
    Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Json},
    routing::{get, post},
};
use http_error::{ErrorResponse, HttpApiError};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::{Value, json};
use storage::DatabaseManager;
use storage_types::StorageError;

use crate::{constants, types::AppState};

const DYNAMODB_JSON_BODY_LIMIT_BYTES: usize = 5 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct MetricsEndpointConfig {
    pub enabled: bool,
    pub prometheus: Option<PrometheusMetricsEndpointConfig>,
}

impl Default for MetricsEndpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prometheus: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrometheusMetricsEndpointConfig {
    pub handle: PrometheusHandle,
    pub bearer_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ServiceRoutePaths {
    pub storage: String,
    pub queue: String,
    pub pubsub: String,
}

impl Default for ServiceRoutePaths {
    fn default() -> Self {
        Self {
            storage: constants::BASE_PATH.to_string(),
            queue: "/queue".to_string(),
            pubsub: "/pubsub".to_string(),
        }
    }
}

#[derive(Debug)]
struct PrometheusMetricsState {
    handle: PrometheusHandle,
    bearer_token: Option<String>,
}

pub fn router(app_state: Arc<AppState>) -> Router {
    Router::new()
        .route("/up", get(up))
        .route("/ready", get(ready))
        .route("/health", get(health_status))
        .route(
            "/",
            post(crate::routes::dynamodb::dynamodb_endpoint).layer(middleware::from_fn_with_state(
                app_state.clone(),
                fixed_ingress_ceiling,
            )),
        )
        .with_state(app_state)
        .layer(DefaultBodyLimit::max(DYNAMODB_JSON_BODY_LIMIT_BYTES))
}

pub fn server_router(app_state: Arc<AppState>, enable_internal_helper_routes: bool) -> Router {
    server_router_with_metrics(
        app_state,
        enable_internal_helper_routes,
        MetricsEndpointConfig::default(),
    )
}

pub fn server_router_with_metrics(
    app_state: Arc<AppState>,
    enable_internal_helper_routes: bool,
    metrics_config: MetricsEndpointConfig,
) -> Router {
    server_router_with_metrics_and_routes(
        app_state,
        enable_internal_helper_routes,
        metrics_config,
        ServiceRoutePaths::default(),
    )
}

pub fn server_router_with_metrics_and_routes(
    app_state: Arc<AppState>,
    enable_internal_helper_routes: bool,
    metrics_config: MetricsEndpointConfig,
    routes: ServiceRoutePaths,
) -> Router {
    let prefixed_router = router(app_state.clone());
    let mut router = Router::new()
        .route("/up", get(up))
        .route("/ready", get(ready))
        .route("/health", get(health_status))
        .route(
            "/",
            post(crate::routes::dynamodb::dynamodb_endpoint).layer(middleware::from_fn_with_state(
                app_state.clone(),
                fixed_ingress_ceiling,
            )),
        )
        .with_state(app_state.clone())
        .nest(&routes.storage, prefixed_router)
        .nest(
            &routes.storage,
            internal_replication_router(app_state.clone()),
        );

    #[cfg(feature = "queue")]
    {
        router = router.nest(
            &routes.queue,
            Router::new()
                .route("/", post(crate::routes::queue::queue_endpoint))
                .layer(middleware::from_fn_with_state(
                    app_state.clone(),
                    fixed_ingress_ceiling,
                ))
                .with_state(app_state.clone()),
        );
    }

    #[cfg(feature = "pubsub")]
    {
        router = router.nest(
            &routes.pubsub,
            Router::new()
                .route("/", post(crate::routes::pubsub::pubsub_endpoint))
                .layer(middleware::from_fn_with_state(
                    app_state.clone(),
                    fixed_ingress_ceiling,
                ))
                .with_state(app_state.clone()),
        );
    }

    if metrics_config.enabled
        && let Some(prometheus) = metrics_config.prometheus
    {
        router = router.merge(prometheus_metrics_router(prometheus));
    }

    if enable_internal_helper_routes {
        router.merge(internal_helper_router(app_state))
    } else {
        router
    }
    .layer(DefaultBodyLimit::max(DYNAMODB_JSON_BODY_LIMIT_BYTES))
}

async fn fixed_ingress_ceiling(
    State(app_state): State<Arc<AppState>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> axum::response::Response {
    let Ok(permit) = app_state.ingress_semaphore.clone().try_acquire_owned() else {
        let error: (StatusCode, Json<ErrorResponse>) = HttpApiError::service_unavailable(1).into();
        return error.into_response();
    };
    let response = next.run(request).await;
    drop(permit);
    response
}

fn prometheus_metrics_router(config: PrometheusMetricsEndpointConfig) -> Router {
    let state = Arc::new(PrometheusMetricsState {
        handle: config.handle,
        bearer_token: config.bearer_token,
    });
    Router::new()
        .route("/metrics", get(prometheus_metrics_endpoint))
        .with_state(state)
}

async fn prometheus_metrics_endpoint(
    State(state): State<Arc<PrometheusMetricsState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(status) = authorize_prometheus_metrics(&headers, state.bearer_token.as_deref()) {
        let message = if status == StatusCode::UNAUTHORIZED {
            "missing bearer token"
        } else {
            "invalid bearer token"
        };
        return (status, message).into_response();
    }

    let mut metrics = state.handle.render();
    append_metrics_facade_metrics(&mut metrics);
    append_storage_backend_metrics(&mut metrics);

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics,
    )
        .into_response()
}

fn append_metrics_facade_metrics(metrics: &mut String) {
    let snapshot = metrics_facade::metrics_crate_facade_cache_snapshot();
    metrics.push('\n');
    metrics.push_str(
        "# HELP metrics_facade_handle_cache_entries Cached metrics facade handles by kind.\n",
    );
    metrics.push_str("# TYPE metrics_facade_handle_cache_entries gauge\n");
    metrics.push_str(&format!(
        "metrics_facade_handle_cache_entries{{kind=\"counter\"}} {}\n",
        snapshot.counters
    ));
    metrics.push_str(&format!(
        "metrics_facade_handle_cache_entries{{kind=\"gauge\"}} {}\n",
        snapshot.gauges
    ));
    metrics.push_str(&format!(
        "metrics_facade_handle_cache_entries{{kind=\"histogram\"}} {}\n",
        snapshot.histograms
    ));
}

fn append_storage_backend_metrics(_metrics: &mut String) {
    #[cfg(feature = "foundationdb")]
    {
        _metrics.push('\n');
        _metrics.push_str(&storage::foundationdb_operation_metrics_snapshot());
    }
}

pub(crate) fn authorize_prometheus_metrics(
    headers: &HeaderMap,
    expected_token: Option<&str>,
) -> Result<(), StatusCode> {
    let Some(expected_token) = expected_token else {
        return Ok(());
    };
    let header_value = headers
        .get(header::AUTHORIZATION)
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let header_value = header_value.to_str().map_err(|_| StatusCode::FORBIDDEN)?;
    let token = header_value
        .strip_prefix("Bearer ")
        .ok_or(StatusCode::FORBIDDEN)?;
    if token == expected_token {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

pub fn internal_helper_router(app_state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/_internal/test/clear-all-tables",
            post(crate::routes::internal::clear_all_tables_endpoint),
        )
        .route(
            "/_internal/test/background-jobs/{job_name}",
            post(crate::routes::internal::run_background_job_endpoint),
        )
        .route(
            "/_internal/test/cache-diagnostics",
            get(crate::routes::internal::cache_diagnostics_endpoint),
        )
        .route(
            "/_internal/test/table-stream-records",
            post(crate::routes::internal::append_table_stream_record_endpoint),
        )
        .route(
            "/_internal/storage/streams/records",
            post(crate::routes::internal::get_stream_records_endpoint),
        )
        .route(
            "/_internal/test/admission/hold",
            post(crate::routes::internal::hold_admission_endpoint),
        )
        .route(
            "/_internal/test/admission/release",
            post(crate::routes::internal::release_admission_endpoint),
        )
        .route(
            "/_internal/test/admission/diagnostics",
            get(crate::routes::internal::admission_diagnostics_endpoint),
        )
        .with_state(app_state)
}

pub fn internal_replication_router(app_state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/_internal/storage/replication/apply",
            post(crate::routes::internal::apply_replication_endpoint),
        )
        .route(
            "/_internal/storage/replication/logical-backfill/import",
            post(crate::routes::internal::import_replication_logical_backfill_endpoint),
        )
        .route(
            "/_internal/storage/replication/heartbeat",
            post(crate::routes::internal::replication_heartbeat_endpoint),
        )
        .route(
            "/_internal/storage/replication/health",
            get(crate::routes::internal::replication_health_endpoint),
        )
        .route(
            "/_internal/sync/health",
            get(crate::routes::internal::sync_health_endpoint),
        )
        .route(
            "/_internal/sync/raft/learners",
            post(crate::routes::internal::sync_raft_add_learner_endpoint),
        )
        .route(
            "/_internal/sync/raft/learners/{node_id}/promote",
            post(crate::routes::internal::sync_raft_promote_learner_endpoint),
        )
        .route(
            "/_internal/sync/raft/append",
            post(crate::routes::internal::sync_raft_append_endpoint),
        )
        .route(
            "/_internal/sync/raft/snapshot",
            post(crate::routes::internal::sync_raft_snapshot_endpoint),
        )
        .route(
            "/_internal/sync/raft/vote",
            post(crate::routes::internal::sync_raft_vote_endpoint),
        )
        .with_state(app_state)
}

pub fn mount_router(app_state: &Arc<AppState>) -> (&'static str, Router) {
    (constants::BASE_PATH, router(app_state.clone()))
}

pub async fn up() -> StatusCode {
    StatusCode::OK
}

pub async fn ready(State(app_state): State<Arc<AppState>>) -> (StatusCode, Json<Value>) {
    match check_storage_ready(app_state.db_manager.as_ref()).await {
        Ok(()) => {
            app_state.health.record_success();
            (
                StatusCode::OK,
                Json(json!({
                    "status": "ready",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                })),
            )
        }
        Err(err) => {
            let message = err.to_string();
            app_state.health.record_failure(message.clone());
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "status": "not_ready",
                    "error": message,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                })),
            )
        }
    }
}

pub async fn health_status(State(app_state): State<Arc<AppState>>) -> (StatusCode, Json<Value>) {
    let summary = app_state.health.status();
    if summary.healthy {
        (
            StatusCode::OK,
            Json(json!({
                "status": "healthy",
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "unhealthy",
                "reason": summary.reason,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })),
        )
    }
}

async fn check_storage_ready(db: &DatabaseManager) -> Result<(), StorageError> {
    db.check_ready()
        .await
        .map(|_| ())
        .map_err(|err| StorageError::internal(&format!("storage readiness check failed: {err}")))
}

pub fn mount_router_with_generic_config<T>(_config: &Arc<T>) -> (&'static str, Router) {
    (
        constants::BASE_PATH,
        Router::new().route(
            "/",
            post(|| async {
                axum::response::Json(
                    serde_json::json!({"error": "Storage API requires proper initialization"}),
                )
            }),
        ),
    )
}

#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    use utoipa::OpenApi;

    #[derive(OpenApi)]
    #[openapi(
        paths(
            crate::routes::dynamodb::dynamodb_endpoint,
            crate::routes::queue::queue_endpoint,
            crate::routes::pubsub::pubsub_endpoint,
            crate::routes::internal::apply_replication_endpoint,
            crate::routes::internal::replication_heartbeat_endpoint,
            crate::routes::internal::replication_health_endpoint
        ),
        components(schemas(http_error::ErrorResponse)),
        info(title = "Storage API", version = env!("CARGO_PKG_VERSION"))
    )]
    struct Doc;
    prefix_paths(Doc::openapi(), constants::BASE_PATH)
}

fn prefix_paths(mut doc: utoipa::openapi::OpenApi, base_path: &str) -> utoipa::openapi::OpenApi {
    let base = base_path.trim_end_matches('/');
    if base.is_empty() || base == "/" {
        return doc;
    }

    let paths = std::mem::take(&mut doc.paths.paths);
    for (path, item) in paths {
        if path == "/queue" || path == "/pubsub" {
            doc.paths.paths.insert(path, item);
            continue;
        }

        let prefixed = if path == "/" {
            base.to_string()
        } else if path.starts_with('/') {
            format!("{base}{path}")
        } else {
            format!("{base}/{path}")
        };
        doc.paths.paths.insert(prefixed, item);
    }
    doc
}
