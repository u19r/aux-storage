//! Public service construction and intake lifetime after native store
//! publication.

use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use axum::Router;
use config::StorageApiLaunchEffectiveConfig;
use storage::DatabaseManager;
use storage_types::{StorageError, StorageResult};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

use super::{
    AppState, MetricsEndpointConfig,
    router::{ServiceRoutePaths, server_router_with_metrics_and_routes},
    service::{run_after_listener_ready, spawn_http_server, stop_http_server},
};

pub(super) async fn serve(
    database: Arc<DatabaseManager>,
    effective: StorageApiLaunchEffectiveConfig,
    metrics: MetricsEndpointConfig,
    cors: CorsLayer,
) -> StorageResult<()> {
    let root = &effective.config.root;
    let state = Arc::new(prepare_app_state(database, root, &root.http.bind_addr)?);
    let router = server_router_with_metrics_and_routes(
        Arc::clone(&state),
        effective.enable_internal_helper_routes,
        metrics,
        ServiceRoutePaths {
            storage: root.http.routes.storage.clone(),
            queue: root.http.routes.queue.clone(),
            pubsub: root.http.routes.pubsub.clone(),
        },
    )
    .layer(cors);
    let address: std::net::SocketAddr = root
        .http
        .bind_addr
        .parse()
        .map_err(|_| StorageError::validation("invalid HTTP bind address"))?;
    let listener = TcpListener::bind(address)
        .await
        .map_err(|_| StorageError::internal("failed to bind HTTP listener"))?;
    let shutdown_grace = Duration::from_secs(root.features.runtime.shutdown_grace_seconds);
    serve_listener(listener, router, state, shutdown_grace).await
}

pub(crate) async fn serve_listener(
    listener: TcpListener,
    router: Router,
    state: Arc<AppState>,
    shutdown_grace: Duration,
) -> StorageResult<()> {
    let (server, ready, stop) = spawn_http_server(listener, router, shutdown_grace);
    if let Err(error) = run_after_listener_ready(ready, state.db_manager.check_ready()).await {
        stop_http_server(server, stop).await;
        return Err(error);
    }
    log_foreign_topic_arns(&state).await;
    // The database's background services own the delivery runner and stop it
    // before native admission drains (`shutdown_background_services`).
    if let Some(messaging) = state.messaging.as_ref()
        && let Err(error) = messaging.start_delivery(&state.db_manager).await
    {
        stop_http_server(server, stop).await;
        return Err(error);
    }
    state.publish_startup_readiness();
    let mut server = server;
    // A failed native sync stops the store for good: begin the normal
    // shutdown and exit with a failure so that the supervisor restarts the
    // process, whose startup barrier then proves the disk.
    let (result, durability_failed) = tokio::select! {
        result = &mut server => (result, false),
        () = state.db_manager.native_durability_failed() => {
            let _ = stop.send(());
            (server.await, true)
        }
    };
    result
        .map_err(|_| StorageError::internal("HTTP server task failed"))?
        .map_err(|_| StorageError::internal("HTTP server failed"))?;
    if durability_failed {
        return Err(StorageError::from(
            storage_types::AttemptFailure::Unavailable(
                storage_types::NativeUnavailable::DurabilityFailed,
            ),
        ));
    }
    Ok(())
}

pub(crate) fn prepare_app_state(
    database: Arc<DatabaseManager>,
    root: &config::RootConfig,
    bind_addr: &str,
) -> StorageResult<AppState> {
    let messaging =
        storage::messaging::MessagingServices::build(&database, messaging_config(root, bind_addr))?;
    Ok(AppState::new(database)
        .with_startup_readiness(Arc::new(AtomicBool::new(false)))
        .with_messaging_services(messaging))
}

/// The messaging services that `queue.*` and `pubsub.*` switch on.
pub(crate) fn messaging_config(
    root: &config::RootConfig,
    bind_addr: &str,
) -> storage::messaging::MessagingConfig {
    storage::messaging::MessagingConfig {
        queue: root.queue.enabled.then(|| queue::QueueEndpoint {
            public_base_url: root
                .queue
                .public_base_url
                .clone()
                .unwrap_or_else(|| route_public_base_url(bind_addr, &root.http.routes.queue)),
        }),
        pubsub: root.pubsub.enabled.then(|| pubsub_delivery(&root.pubsub)),
        identity: messaging_identity(&root.queue),
    }
}

/// The file settings of the delivery runner (review D12). The loader checked
/// every rule (`config::PubsubConfig::validate`, finding 9).
pub(crate) fn pubsub_delivery(
    config: &config::PubsubConfig,
) -> storage::messaging::PubsubServiceConfig {
    // `SubscribeURL` and `UnsubscribeURL` need the public URL of the pubsub
    // route; without `pubsub.public_base_url` they stay empty.
    let delivery = pubsub::PubsubDeliveryConfig {
        timing: pubsub::DeliveryTiming::from(&config.delivery),
        subscribe_url_base: config.public_base_url.clone(),
        unsubscribe_url_base: config.public_base_url.clone(),
        ..pubsub::PubsubDeliveryConfig::default()
    };
    storage::messaging::PubsubServiceConfig {
        delivery,
        runner: pubsub::DeliveryRunnerConfig::from(&config.delivery),
    }
}

/// A stored topic keeps the ARN it was created with. When the configured
/// identity changed after topics were created, log one error at startup
/// (W3 design 8.2); the old topics stay reachable by their old ARN only.
async fn log_foreign_topic_arns(state: &AppState) {
    // At most this many pages of 100 topics are read at startup (review D13).
    const MAX_PAGES: usize = 100;
    let (Some(manager), Some(messaging)) =
        (state.pubsub_manager.as_ref(), state.messaging.as_ref())
    else {
        return;
    };
    let identity = messaging.identity();
    let prefix = format!(
        "arn:{}:sns:{}:{}:",
        identity.partition, identity.region, identity.account_id
    );
    let mut foreign = 0_usize;
    let mut next_token = None;
    let mut complete = false;
    for _ in 0..MAX_PAGES {
        let Ok(page) = manager
            .list_topics(pubsub::ListTopicsRequest { next_token })
            .await
        else {
            return;
        };
        foreign += page
            .topics
            .iter()
            .filter(|topic| !topic.topic_arn.as_str().starts_with(&prefix))
            .count();
        next_token = page.next_token;
        if next_token.is_none() {
            complete = true;
            break;
        }
    }
    if foreign > 0 {
        tracing::error!(
            foreign_topics = foreign,
            all_topics_read = complete,
            "stored SNS topic ARNs use another partition, region or account than queue.* \
             configuration"
        );
    }
}

/// One identity for queue ARNs and SNS ARNs (review C2).
fn messaging_identity(queue: &config::QueueConfig) -> storage_types::MessagingIdentity {
    storage_types::MessagingIdentity {
        partition: queue.partition.clone(),
        region: queue.region.clone(),
        account_id: queue.account_id.clone(),
    }
}

fn route_public_base_url(bind_addr: &str, route: &str) -> String {
    let host_port = bind_addr
        .strip_prefix("0.0.0.0:")
        .map(|port| format!("127.0.0.1:{port}"))
        .unwrap_or_else(|| bind_addr.to_string());
    format!("http://{}{}", host_port, route)
}
