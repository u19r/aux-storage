//! Public service construction and intake lifetime after native store
//! publication.

use std::sync::{Arc, atomic::AtomicBool};

use axum::Router;
use config::StorageApiLaunchEffectiveConfig;
use storage::DatabaseManager;
use storage_types::{StorageError, StorageResult};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

use super::{
    AppState, MetricsEndpointConfig,
    router::{ServiceRoutePaths, server_router_with_metrics_and_routes},
    runtime_config::shutdown_grace_period,
    service::{run_after_listener_ready, spawn_http_server, stop_http_server},
};

pub(super) async fn serve(
    database: Arc<DatabaseManager>,
    effective: StorageApiLaunchEffectiveConfig,
    metrics: MetricsEndpointConfig,
    cors: CorsLayer,
) -> StorageResult<()> {
    let state = Arc::new(prepare_app_state(database, &effective)?);
    let router = server_router_with_metrics_and_routes(
        Arc::clone(&state),
        effective.enable_internal_helper_routes,
        metrics,
        ServiceRoutePaths {
            storage: effective.root.http.routes.storage.clone(),
            #[cfg(feature = "queue")]
            queue: effective.root.http.routes.queue.clone(),
            #[cfg(feature = "pubsub")]
            pubsub: effective.root.http.routes.pubsub.clone(),
        },
    )
    .layer(cors);
    let address: std::net::SocketAddr = effective
        .bind_addr
        .parse()
        .map_err(|_| StorageError::validation("invalid HTTP bind address"))?;
    let listener = TcpListener::bind(address)
        .await
        .map_err(|_| StorageError::internal("failed to bind HTTP listener"))?;
    serve_listener(listener, router, state).await
}

async fn serve_listener(
    listener: TcpListener,
    router: Router,
    state: Arc<AppState>,
) -> StorageResult<()> {
    #[cfg(feature = "pubsub")]
    let delivery_manager = prepare_delivery_manager(&state)?;
    let (server, ready, stop) = spawn_http_server(listener, router, shutdown_grace_period());
    if let Err(error) = run_after_listener_ready(ready, state.db_manager.check_ready()).await {
        stop_http_server(server, stop).await;
        return Err(error);
    }
    #[cfg(feature = "pubsub")]
    let worker = match pubsub::PubsubDeliveryWorker::start(
        delivery_manager,
        crate::constants::PUBSUB_DELIVERY_BATCH_LIMIT,
        crate::constants::PUBSUB_DELIVERY_IDLE_INTERVAL,
        crate::constants::PUBSUB_DELIVERY_ERROR_INTERVAL,
    ) {
        Ok(worker) => worker,
        Err(_) => {
            stop_http_server(server, stop).await;
            return Err(StorageError::internal(
                "failed to start pubsub delivery worker",
            ));
        }
    };
    state.publish_startup_readiness();
    let result = server.await;
    #[cfg(feature = "pubsub")]
    let delivery_result = worker.shutdown().await;
    result
        .map_err(|_| StorageError::internal("HTTP server task failed"))?
        .map_err(|_| StorageError::internal("HTTP server failed"))?;
    #[cfg(feature = "pubsub")]
    delivery_result
        .map_err(|_| StorageError::internal("pubsub delivery worker failed during shutdown"))?;
    Ok(())
}

fn prepare_app_state(
    database: Arc<DatabaseManager>,
    _effective: &StorageApiLaunchEffectiveConfig,
) -> StorageResult<AppState> {
    let state = AppState::new(database).with_startup_readiness(Arc::new(AtomicBool::new(false)));
    #[cfg(any(feature = "queue", feature = "pubsub"))]
    let mut state = state;
    #[cfg(feature = "queue")]
    {
        let provider = state
            .db_manager
            .initialization_queue_provider()
            .ok_or_else(|| {
                StorageError::internal("configured backend does not expose a queue provider")
            })?;
        state.queue_manager = Some(Arc::new(queue::QueueManager::new(provider)));
        state.queue_public_base_url = _effective
            .root
            .queue
            .public_base_url
            .clone()
            .unwrap_or_else(|| {
                route_public_base_url(&_effective.bind_addr, &_effective.root.http.routes.queue)
            });
        state.queue_account_id = _effective.root.queue.account_id.clone();
    }
    #[cfg(feature = "pubsub")]
    {
        state.pubsub_manager = Some(prepare_pubsub_manager(&state)?);
    }
    Ok(state)
}

#[cfg(feature = "pubsub")]
fn prepare_pubsub_manager(state: &AppState) -> StorageResult<Arc<pubsub::PubsubManager>> {
    let provider = state
        .db_manager
        .initialization_pubsub_provider()
        .ok_or_else(|| {
            StorageError::internal("configured backend does not expose a pubsub provider")
        })?;
    build_pubsub_manager(
        provider,
        #[cfg(feature = "queue")]
        state.queue_manager.clone(),
    )
}

#[cfg(feature = "pubsub")]
fn prepare_delivery_manager(state: &AppState) -> StorageResult<Arc<pubsub::PubsubManager>> {
    let provider = state
        .db_manager
        .initialization_delivery_pubsub_provider()
        .ok_or_else(|| {
            StorageError::internal(
                "configured backend does not expose an admitted delivery provider",
            )
        })?;
    #[cfg(feature = "queue")]
    let queue = state
        .db_manager
        .initialization_delivery_queue_provider()
        .map(|provider| Arc::new(queue::QueueManager::new(provider)));
    build_pubsub_manager(
        provider,
        #[cfg(feature = "queue")]
        queue,
    )
}

#[cfg(feature = "pubsub")]
fn build_pubsub_manager(
    provider: Arc<dyn pubsub::PubsubProvider>,
    #[cfg(feature = "queue")] queue: Option<Arc<queue::QueueManager>>,
) -> StorageResult<Arc<pubsub::PubsubManager>> {
    let builder = pubsub::PubsubManager::builder().provider(provider);
    #[cfg(feature = "queue")]
    let builder = match queue {
        Some(queue) => builder.queue_manager(queue),
        None => builder,
    };
    builder
        .build()
        .map(Arc::new)
        .map_err(|_| StorageError::internal("failed to initialize pubsub manager"))
}

#[cfg(feature = "queue")]
fn route_public_base_url(bind_addr: &str, route: &str) -> String {
    let host_port = bind_addr
        .strip_prefix("0.0.0.0:")
        .map(|port| format!("127.0.0.1:{port}"))
        .unwrap_or_else(|| bind_addr.to_string());
    format!("http://{}{}", host_port, route)
}
