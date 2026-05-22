//! Standalone DynamoDB-compatible storage API service assembly.
//!
//! Downstream service hosts should prefer the curated re-exports from this
//! crate root instead of depending on implementation modules directly.

mod batch_get_wire_response;
mod config_watch;
pub mod constants;
mod errors;
mod get_wire_response;
mod health;
pub(crate) mod manager;
pub mod multi_region_harness;
pub mod multi_region_harness_cli;
mod query_wire_response;
mod raw_dynamodb_response;
mod replication_logical_import;
mod replication_runtime;
mod router;
mod routes;
mod runtime_config;
mod sync_raft_http_client;
mod sync_raft_peer_status;
mod sync_replication_startup;
mod sync_response_correlation;
mod types;

#[cfg(test)]
mod cli_tests;
#[cfg(test)]
mod multi_region_cache_oracle_tests;
#[cfg(test)]
mod multi_region_harness_cli_tests;
#[cfg(test)]
mod multi_region_simulation_tests;
#[cfg(test)]
mod query_wire_response_tests;
#[cfg(test)]
mod quint_sync_peer_auth_transport_tests;
#[cfg(test)]
mod quint_sync_raft_startup_policy_tests;
#[cfg(test)]
mod quint_sync_response_correlation_tests;
#[cfg(test)]
mod raw_dynamodb_response_tests;
#[cfg(test)]
mod replication_runtime_tests;
#[cfg(test)]
mod router_tests;
#[cfg(test)]
mod sync_raft_http_client_tests;
#[cfg(test)]
mod sync_replication_startup_tests;

pub use config_watch::{ConfigWatchGuard, spawn as spawn_config_watch};
pub use manager::{
    StorageApiManager, StorageApiManagerImpl, StorageApiManagerOptions, SyncHealthReporter,
    SyncRaftRuntimeAdapter, SyncReadBarrier, SyncWriteProposer,
};
pub use notify::Result as ConfigWatchResult;
pub use replication_runtime::{
    HttpReplicationPeerClient, ReplicationPeerClient, ReplicationPeerConfig,
    ReplicationRuntimeConfig, StorageReplicationRuntime,
};
pub use router::{
    MetricsEndpointConfig, PrometheusMetricsEndpointConfig, ServiceRoutePaths, health_status,
    internal_helper_router, internal_replication_router, mount_router,
    mount_router_with_generic_config, openapi, ready, router, server_router,
    server_router_with_metrics, server_router_with_metrics_and_routes, up,
};
pub use runtime_config::{
    FilterSource, ensure_backend_matches, resolve_filter, shutdown_grace_period,
    storage_config_from_backends,
};
pub use storage_provider::{StorageBackend, StorageConfig};
pub use sync_raft_http_client::HttpSyncRaftRpcClient;
pub use sync_replication_startup::build_sync_raft_runtime_adapter;
pub use types::{
    AppState, Response, SyncLearnerJoinHandler, SyncLearnerJoinRequest, SyncLearnerJoinResponse,
    SyncLearnerPromotionResponse, SyncRaftRpcHandler,
};
