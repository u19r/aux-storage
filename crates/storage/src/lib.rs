//! Public storage facade and embedded runtime entry point.
//!
//! Consumers should depend on this crate for storage manager APIs, request and
//! entity types, derive macros, provider contracts, and selected
//! backend-neutral helpers. Implementation crates remain available inside the
//! workspace, but this crate is the supported downstream surface.

pub mod common {
    pub use storage_common::*;
}

pub mod condition {
    pub use storage_condition::*;
}

pub mod derive {
    pub use storage_derive::*;
}

pub mod provider {
    pub use storage_provider::*;
}

pub mod types {
    pub use storage_types::*;
}

#[cfg(feature = "rocksdb")]
pub use kv::{RocksDbKvStore, SortedKvDbStorageProvider};
#[cfg(feature = "foundationdb")]
pub use kv::{foundationdb_operation_metrics_reset, foundationdb_operation_metrics_snapshot};

mod builder;
pub mod cache;
#[cfg(feature = "cache-write-planner")]
pub(crate) use cache::write_planner as cache_write_planner;
pub(crate) use cache::{
    batch_get_runtime as cache_batch_get_runtime, coordinator as cache_coordinator,
    point_read as point_read_cache, point_read_runtime as cache_point_read_runtime,
    point_read_store as point_read_cache_store, point_read_types as point_read_cache_types,
    query_proof as query_proof_cache, query_proof_request, query_proof_store, query_proof_types,
    query_runtime as cache_query_runtime, read_observability as cache_read_observability,
};
pub use cache_coordinator::StorageAuthoritativeCacheOptions;
pub use cache_read_observability::{StorageCacheReadDiagnostics, storage_cache_read_diagnostics};
#[cfg(feature = "remote")]
pub use storage_remote::RemoteStorageProvider;

mod newtypes;
pub use builder::{create_storage_provider, create_storage_provider_bundle};
mod constants;
mod database_manager;
mod dynamo_json;
mod migration;
pub(crate) use migration::namespace_routing;
pub use migration::{index_keys as migration_index_keys, namespace_migration};
mod multi_region;
pub(crate) use multi_region::{metrics as multi_region_metrics, model as multi_region_model};
pub mod startup;
mod updated_at_apply;
#[cfg(test)]
mod updated_at_apply_tests;
#[cfg(all(test, feature = "cache-write-planner"))]
pub(crate) use database_manager::DatabaseManagerTestPauseHandle;
pub use database_manager::{
    CappedStorageError, CreateCappedEntityInput, DatabaseManager, DatabaseManagerRuntimeOptions,
    DatabaseManagerRuntimeOptionsBuilder, DeleteCappedEntityInput, DeleteItemInput,
    InProcessReadSequence, InProcessReadSequenceLimits, InProcessReadSequenceStats,
    PutItemEntityEncodeInput, PutItemInput, QueryIndexInput, QueryTableInput,
    ReplicationMutationApplyOutcome, ResolvedBatchGetPlan, ResolvedGetItem,
    ResolvedStorageOperation, ScanTableInput, UpdateItemInput,
};
pub use multi_region_metrics::{
    increment_multi_region_apply_total, increment_multi_region_auth_failure_total,
    record_multi_region_heartbeat_rtt, record_multi_region_heartbeat_staleness,
    record_multi_region_replication_lag, record_multi_region_sender_queue_depth,
};
pub use multi_region_model::{
    OutboundReplicationBatch, OutboundReplicationMutationRecord, PeerCheckpointRecord,
    PeerReplicationStatusRecord, TableBootstrapCursorRecord, TableReplicationConfigRecord,
    peer_checkpoint_put_request, table_bootstrap_cursor_put_request,
};
pub use namespace_migration::{
    BeginDualWriteInput, CompleteCutoverInput, DualWriteCoordinator,
    MigrationBackfillEntitySummary, MigrationBackfillInput, MigrationBackfillSummary,
};
pub use namespace_routing::{
    CutoverEvent, CutoverEventStatus, CutoverWatcher, NamespaceRequestRewriter, NamespaceRoute,
    NamespaceRouteResolver, NamespaceSourceTable, NamespaceStorageMigrationMode,
    NamespaceStorageMode, RouteTarget, is_retryable_pause_error, namespace_source_table,
};
pub use point_read_cache::{
    AuthoritativePointReadHit, AuthoritativePointReadPurpose, AuthoritativePointReadResult,
    DurableAbsenceProof, DurableItemRevision, InMemoryPointReadCache, InMemoryPointReadCacheConfig,
    NoopPointReadCache, PointReadBatchGetResult, PointReadCache, PointReadCacheEvictionPolicy,
    PointReadGetRequest, PointReadGetResult, noop_point_read_cache,
};
pub use query_proof_cache::{
    InMemoryQueryProofCache, NoopQueryProofCache, QueryProofCache, QueryProofMaterializedPage,
    noop_query_proof_cache,
};
pub use query_proof_types::{
    InMemoryQueryProofCacheConfig, QueryCoverageRange, QueryCoverageState, QueryManifestEntry,
    QueryManifestKey, QueryManifestSnapshot, QueryProofCacheEvictionPolicy,
};
pub use storage_types::*;

mod tables;
pub use tables::Tables;

#[cfg(test)]
mod builder_tests;
#[cfg(test)]
mod database_manager_tests;
#[cfg(test)]
mod storage_tests;

#[cfg(test)]
mod startup_tests;
