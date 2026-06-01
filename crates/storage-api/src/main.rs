use std::{future::Future, sync::Arc, time::Duration};

use axum::http::HeaderValue;
use config::{self, StorageApiLaunchConfig, Tracing};
use metrics_exporter_prometheus::PrometheusBuilder;
use storage_api::{
    AppState, HttpReplicationPeerClient, MetricsEndpointConfig, ReplicationRuntimeConfig,
    ServiceRoutePaths, StorageApiManagerOptions, StorageReplicationRuntime, SyncHealthReporter,
    SyncLearnerJoinHandler, SyncRaftRpcHandler, SyncReadBarrier, SyncWriteProposer,
    build_sync_raft_runtime_adapter, ensure_backend_matches, resolve_filter,
    server_router_with_metrics_and_routes, shutdown_grace_period, spawn_config_watch,
    storage_config_from_backends,
};
pub use storage_provider::{StorageBackend, StorageConfig};
use storage_types::{StorageEnum, StorageError, StorageResult, context::WrappedError as _};
use tokio::{net::TcpListener, runtime::Builder, signal, task::JoinHandle};
use tower_http::cors::CorsLayer;
#[cfg(feature = "tokio-console")]
use tracing_subscriber::layer::Layer as _;
use tracing_subscriber::{
    EnvFilter, fmt,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

const PROMETHEUS_UPKEEP_INTERVAL: Duration = Duration::from_secs(5);
const PROMETHEUS_LATENCY_MS_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
    1000.0, 2500.0, 5000.0, 10000.0,
];

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

fn main() -> StorageResult<()> {
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
    if let Err(err) = run(launch).await {
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

async fn run(launch: StorageApiLaunchConfig) -> StorageResult<()> {
    let effective = launch.effective;
    let config_arc = effective.config.clone();
    let replication_runtime_config =
        ReplicationRuntimeConfig::from_settings(&effective.storage_replication)?;
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
    tracing::debug!(
        target: "config",
        effective = %config_arc.effective_pretty(),
        "Effective configuration (after overrides)"
    );
    let cors_layer = cors_layer(&effective.cors);

    let db_manager = Arc::new(
        storage::startup::database_manager_from_features(
            &effective.root.features,
            storage::DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(effective.enable_background_workers)
                .enable_background_refresh(
                    effective.root.features.cache_ttls.enable_background_refresh,
                )
                .enable_background_watchers(effective.enable_background_workers)
                .run_gsi_maintenance_after_write(None)
                .build(),
        )
        .await?,
    );
    println!("Storage backend initialized.");
    let sync_raft_runtime = build_sync_raft_runtime_adapter(
        db_manager.clone(),
        &effective.storage_sync_replication,
        &storage_config,
    )
    .await?;
    #[cfg(feature = "queue")]
    let queue_manager = {
        let queue_provider = db_manager.queue_provider().ok_or_else(|| {
            StorageError::internal("configured backend does not expose a queue provider")
        })?;
        queue_provider.initialize().await.map_err(|err| {
            StorageError::internal(&format!("failed to initialize queue provider: {err}"))
        })?;
        Some(Arc::new(queue::QueueManager::new(queue_provider)))
    };

    #[cfg(feature = "pubsub")]
    let pubsub_manager = {
        let pubsub_provider = db_manager.pubsub_provider().ok_or_else(|| {
            StorageError::internal("configured backend does not expose a pubsub provider")
        })?;
        pubsub_provider.initialize().await.map_err(|err| {
            StorageError::internal(&format!("failed to initialize pubsub provider: {err}"))
        })?;
        let mut builder = pubsub::PubsubManager::builder().provider(pubsub_provider);
        #[cfg(feature = "queue")]
        if let Some(queue_manager) = queue_manager.clone() {
            builder = builder.queue_manager(queue_manager);
        }
        Some(Arc::new(builder.build().map_err(|err| {
            StorageError::internal(&format!("failed to initialize pubsub manager: {err}"))
        })?))
    };

    let mut app_state = AppState::new_with_manager_options(
        db_manager.clone(),
        StorageApiManagerOptions {
            self_region: replication_runtime_config
                .as_ref()
                .map(|config| config.self_region.clone()),
            sync_write_proposer: sync_raft_runtime
                .as_ref()
                .map(|adapter| adapter.clone() as Arc<dyn SyncWriteProposer>),
            sync_read_barrier: sync_raft_runtime
                .as_ref()
                .map(|adapter| adapter.clone() as Arc<dyn SyncReadBarrier>),
            sync_health_reporter: sync_raft_runtime
                .as_ref()
                .map(|adapter| adapter.clone() as Arc<dyn SyncHealthReporter>),
            ..StorageApiManagerOptions::default()
        },
    );
    if let Some(token) = effective
        .storage_sync_replication
        .sync_internal_token
        .as_deref()
    {
        app_state = app_state.with_sync_internal_token(token.to_string());
    }
    if let Some(replication_runtime_config) = replication_runtime_config.as_ref() {
        app_state = app_state.with_replication_service_tokens(
            replication_runtime_config
                .peers
                .iter()
                .map(|peer| peer.service_token.clone()),
        );
    }
    if let Some(adapter) = sync_raft_runtime.as_ref() {
        app_state =
            app_state.with_sync_raft_rpc_handler(adapter.clone() as Arc<dyn SyncRaftRpcHandler>);
        app_state = app_state
            .with_sync_learner_join_handler(adapter.clone() as Arc<dyn SyncLearnerJoinHandler>);
    }
    #[cfg(any(feature = "queue", feature = "pubsub"))]
    let mut app_state = app_state;
    #[cfg(feature = "queue")]
    {
        app_state.queue_manager = queue_manager;
        app_state.queue_public_base_url = effective
            .root
            .queue
            .public_base_url
            .clone()
            .unwrap_or_else(|| {
                route_public_base_url(&effective.bind_addr, &effective.root.http.routes.queue)
            });
        app_state.queue_account_id = effective.root.queue.account_id.clone();
    }
    #[cfg(feature = "pubsub")]
    {
        app_state.pubsub_manager = pubsub_manager;
    }
    let app_state = Arc::new(app_state);

    let _replication_runtime_handles =
        if let Some(replication_runtime_config) = replication_runtime_config.clone() {
            let peer_client = Arc::new(HttpReplicationPeerClient::new()?);
            let mut runtime = StorageReplicationRuntime::new(
                db_manager.clone(),
                replication_runtime_config,
                peer_client,
            );
            if let Some(adapter) = sync_raft_runtime.as_ref() {
                runtime = runtime
                    .with_sync_health_reporter(adapter.clone() as Arc<dyn SyncHealthReporter>);
                runtime =
                    runtime.with_sync_write_proposer(adapter.clone() as Arc<dyn SyncWriteProposer>);
            }
            let handles = runtime.spawn();
            tracing::info!(
                peer_count = handles.len(),
                "started multi-region replication runtime"
            );
            Some(handles)
        } else {
            None
        };

    let router = server_router_with_metrics_and_routes(
        app_state.clone(),
        effective.enable_internal_helper_routes,
        metrics_config,
        ServiceRoutePaths {
            storage: effective.root.http.routes.storage.clone(),
            queue: effective.root.http.routes.queue.clone(),
            pubsub: effective.root.http.routes.pubsub.clone(),
        },
    );

    let bind_addr = effective.bind_addr;
    let router = router.layer(cors_layer);

    if launch.inputs.config_path.is_some() {
        println!("Using runtime config bindings.");
    }

    let addr: std::net::SocketAddr = bind_addr.parse().map_err(|err| {
        StorageError::internal(&format!("invalid bind address {bind_addr}: {err}"))
    })?;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| StorageError::internal(&err.to_string()))?;
    println!("Listening on {bind_addr}");
    let shutdown_grace = shutdown_grace_period();
    axum::serve(listener, router)
        .with_graceful_shutdown(await_shutdown_signal(shutdown_grace))
        .await
        .map_err(|err| StorageError::internal(&err.to_string()))?;

    Ok(())
}

#[cfg(feature = "queue")]
fn route_public_base_url(bind_addr: &str, route: &str) -> String {
    let host_port = bind_addr
        .strip_prefix("0.0.0.0:")
        .map(|port| format!("127.0.0.1:{port}"))
        .unwrap_or_else(|| bind_addr.to_string());
    format!("http://{}{}", host_port, route)
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
        prometheus: Some(storage_api::PrometheusMetricsEndpointConfig {
            handle,
            bearer_token: metrics.prometheus.bearer_token.clone(),
        }),
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
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
        ]);
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
