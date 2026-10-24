use std::{
    future::{Future, IntoFuture, poll_fn},
    sync::Arc,
    time::Duration,
};

use axum::{Router, http::HeaderValue};
use config::{self, StorageApiLaunchConfig, Tracing};
use metrics_exporter_prometheus::PrometheusBuilder;
use storage_types::{StorageEnum, StorageError, StorageResult, context::WrappedError as _};
use tokio::{net::TcpListener, runtime::Builder, signal, sync::oneshot, task::JoinHandle};
use tower_http::cors::CorsLayer;
#[cfg(feature = "tokio-console")]
use tracing_subscriber::layer::Layer as _;
use tracing_subscriber::{
    EnvFilter, fmt,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

use crate::{
    MetricsEndpointConfig,
    config_watch::spawn as spawn_config_watch,
    constants::{PROMETHEUS_LATENCY_MS_BUCKETS, PROMETHEUS_UPKEEP_INTERVAL},
    runtime_config::{ensure_backend_matches, resolve_filter, storage_config_from_backends},
};

async fn await_shutdown_signal(grace: Duration) {
    #[cfg(unix)]
    {
        let mut terminate = match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(sig) => sig,
            Err(err) => {
                tracing::warn!(target = "shutdown", error = %err, "failed to install SIGTERM handler; falling back to Ctrl+C only");
                if let Err(err) = signal::ctrl_c().await {
                    tracing::warn!(target = "shutdown", error = %err, "failed to await Ctrl+C shutdown signal");
                }
                tracing::info!(
                    target = "shutdown",
                    "shutdown signal received; draining for {:?}",
                    grace
                );
                tokio::time::sleep(grace).await;
                return;
            }
        };

        tokio::select! {
            res = signal::ctrl_c() => {
                if let Err(err) = res {
                    tracing::warn!(target = "shutdown", error = %err, "failed to await Ctrl+C shutdown signal");
                }
            }
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    if let Err(err) = signal::ctrl_c().await {
        tracing::warn!(target = "shutdown", error = %err, "failed to await Ctrl+C shutdown signal");
    }

    tracing::info!(
        target = "shutdown",
        "shutdown signal received; draining for {:?}",
        grace
    );
    tokio::time::sleep(grace).await;
}

pub fn run() -> StorageResult<()> {
    let launch = StorageApiLaunchConfig::from_args(std::env::args_os())
        .map_err(|err| StorageError::internal(&format!("Failed to load launch config: {err}")))?;
    let tracing_cfg: Tracing = launch.effective.tracing.clone();
    let (filter, source) = resolve_filter(&tracing_cfg);
    println!("Using log filter (source: {source}): {filter}");
    init_tracing(filter).map_err(|err| StorageError::internal(&err.to_string()))?;

    let runtime = Builder::new_multi_thread()
        .worker_threads(runtime_worker_threads())
        .enable_all()
        .build()
        .map_err(|err| StorageError::internal(&format!("tokio runtime: {err}")))?;
    runtime.block_on(async_main(launch))
}

async fn async_main(launch: StorageApiLaunchConfig) -> StorageResult<()> {
    if let Err(err) = run_service(launch).await {
        let (root, contexts) = err.recursive_context(Vec::new());
        if let StorageEnum::InternalServerError { message } = root {
            println!("Internal error detail: {message}");
        }
        if !contexts.is_empty() {
            println!("Context:");
            for (idx, context) in contexts.iter().enumerate() {
                println!("  {}. {context}", idx + 1);
            }
        }
        println!("{err:?}");
        return Err(err);
    }
    Ok(())
}

fn runtime_worker_threads() -> usize {
    std::env::var("STORAGE_API_WORKER_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from))
}

async fn run_service(launch: StorageApiLaunchConfig) -> StorageResult<()> {
    let effective = launch.effective;
    if effective.enable_internal_helper_routes && !cfg!(feature = "internal-test-routes") {
        return Err(StorageError::validation(
            "internal helper routes require the internal-test-routes build feature",
        ));
    }
    let config_arc = effective.config.clone();

    let config_manager = Some(Arc::new(config::runtime::SharedConfigManager::new(
        config_arc.clone(),
    )) as Arc<dyn config::runtime::MutableConfigManager>);

    println!("Starting DynamoDB Server Impersonation...");
    println!("Resolving storage backend configuration...");

    let metrics_config = metrics_endpoint_config(&effective.metrics)
        .map_err(|err| StorageError::internal(&format!("failed to initialize metrics: {err}")))?;
    spawn_prometheus_upkeep(&metrics_config);

    ensure_backend_matches(&effective.backends)?;
    let storage_config = storage_config_from_backends(&effective.backends)?;
    let effective_backend = storage_config.backend_type.clone();

    println!("Storage backend: {effective_backend:?}");
    println!("Initializing storage backend...");

    let _config_reload_guard = if effective.enable_background_workers {
        if let (Some(path), Some(config_manager)) =
            (effective.config_watch_path.clone(), config_manager.clone())
        {
            match spawn_config_watch(path, config_arc.clone(), config_manager) {
                Ok(guard) => Some(guard),
                Err(err) => {
                    tracing::warn!(target = "config", error = %err, "failed to start configuration watcher");
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    tracing::info!(
        target: "config",
        roles = ?effective.root.roles,
        "Loaded configuration"
    );
    let cors_layer = cors_layer(&effective.cors);
    let database_job_intervals = storage::common::DatabaseJobIntervals {
        gsi_backfill_interval_ms: storage::common::JobIntervalMillis(
            effective.root.jobs.gsi_backfill_interval_ms,
        ),
        ..Default::default()
    };

    let db_manager = Arc::new(
        storage::startup::database_manager_from_features(
            &effective.root.features,
            storage::DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(effective.enable_background_workers)
                .database_job_intervals(database_job_intervals)
                .build(),
        )
        .await?,
    );
    println!("Storage backend initialized.");
    let result = super::service_host::serve(
        Arc::clone(&db_manager),
        effective,
        metrics_config,
        cors_layer,
    )
    .await;
    let background_result = db_manager.shutdown_background_services().await;
    result?;
    background_result
}

pub(crate) async fn stop_http_server(
    server_task: JoinHandle<Result<(), std::io::Error>>,
    server_shutdown: oneshot::Sender<()>,
) {
    let _ = server_shutdown.send(());
    let _ = server_task.await;
}

pub(crate) async fn run_after_listener_ready<F>(
    ready: oneshot::Receiver<()>,
    action: F,
) -> StorageResult<()>
where
    F: Future<Output = StorageResult<()>>,
{
    ready.await.map_err(|_| {
        StorageError::internal("storage-api server stopped before listener readiness")
    })?;
    action.await
}

pub(crate) fn spawn_http_server(
    listener: TcpListener,
    router: Router,
    shutdown_grace: Duration,
) -> (
    JoinHandle<Result<(), std::io::Error>>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        let shutdown_signal = async move {
            tokio::select! {
                _ = await_shutdown_signal(shutdown_grace) => {}
                _ = shutdown_rx => {}
            }
        };
        let mut server = Box::pin(
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_signal)
                .into_future(),
        );
        let mut ready_tx = Some(ready_tx);
        poll_fn(move |cx| poll_server_with_readiness(server.as_mut(), &mut ready_tx, cx)).await
    });
    (server_task, ready_rx, shutdown_tx)
}

pub(crate) fn poll_server_with_readiness<F>(
    mut server: std::pin::Pin<&mut F>,
    ready_tx: &mut Option<oneshot::Sender<()>>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<F::Output>
where
    F: Future,
{
    let result = server.as_mut().poll(cx);
    if result.is_pending() {
        if let Some(ready_tx) = ready_tx.take() {
            let _ = ready_tx.send(());
        }
    } else {
        let _ = ready_tx.take();
    }
    result
}

fn init_tracing(filter: EnvFilter) -> Result<(), TryInitError> {
    #[cfg(feature = "tokio-console")]
    if tokio_console_enabled() {
        tracing_subscriber::registry()
            .with(
                console_subscriber::ConsoleLayer::builder()
                    .with_default_env()
                    .spawn(),
            )
            .with(
                fmt::layer()
                    .json()
                    .with_current_span(false)
                    .with_span_list(false)
                    .with_ansi(false)
                    .with_writer(std::io::stdout)
                    .with_filter(filter),
            )
            .try_init()?;
        tracing::info!(
            target = "diagnostics",
            "tokio console enabled; connect with `tokio-console`"
        );
        return Ok(());
    }

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .json()
                .with_current_span(false)
                .with_span_list(false)
                .with_ansi(false)
                .with_writer(std::io::stdout),
        )
        .try_init()
}

#[cfg(feature = "tokio-console")]
fn tokio_console_enabled() -> bool {
    std::env::var("STORAGE_API_TOKIO_CONSOLE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn metrics_endpoint_config(
    metrics: &config::MetricsConfig,
) -> Result<MetricsEndpointConfig, metrics_exporter_prometheus::BuildError> {
    if !metrics.enabled {
        return Ok(MetricsEndpointConfig {
            enabled: false,
            prometheus: None,
        });
    }

    let handle = prometheus_builder()?.install_recorder()?;
    Ok(MetricsEndpointConfig {
        enabled: true,
        prometheus: Some(crate::PrometheusMetricsEndpointConfig { handle }),
    })
}

fn prometheus_builder() -> Result<PrometheusBuilder, metrics_exporter_prometheus::BuildError> {
    PrometheusBuilder::new().set_buckets(PROMETHEUS_LATENCY_MS_BUCKETS)
}

fn spawn_prometheus_upkeep(metrics_config: &MetricsEndpointConfig) {
    let Some(prometheus) = metrics_config.prometheus.as_ref() else {
        return;
    };
    let handle = prometheus.handle.clone();
    spawn_named("storage-api-prometheus-upkeep", async move {
        let mut interval = tokio::time::interval(PROMETHEUS_UPKEEP_INTERVAL);
        loop {
            interval.tick().await;
            handle.run_upkeep();
        }
    });
}

fn spawn_named<F>(task_name: &str, fut: F) -> JoinHandle<()>
where F: Future<Output = ()> + Send + 'static {
    #[cfg(tokio_unstable)]
    {
        match tokio::task::Builder::new().name(task_name).spawn(fut) {
            Ok(handle) => handle,
            Err(err) => {
                tracing::warn!(target: "runtime", error = %err, task_name, "failed to spawn named task");
                tokio::spawn(async {})
            }
        }
    }

    #[cfg(not(tokio_unstable))]
    {
        let _ = task_name;
        tokio::spawn(fut)
    }
}

fn cors_layer(cors: &config::Cors) -> CorsLayer {
    let origins = &cors.allow_origins;
    let mut layer = CorsLayer::new()
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PUT,
            axum::http::Method::PATCH,
            axum::http::Method::DELETE,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers([axum::http::header::CONTENT_TYPE]);
    if origins.iter().any(|origin| origin == "*") {
        layer = layer.allow_origin(tower_http::cors::Any);
    } else {
        let vals: Vec<HeaderValue> = origins
            .iter()
            .filter_map(|origin| HeaderValue::from_str(origin).ok())
            .collect();
        if !vals.is_empty() {
            layer = layer.allow_origin(vals);
        }
    }
    layer
}
