use std::{collections::BTreeMap, net::IpAddr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    backends::{Backends, StorageConnectionsConfig},
    cache::{
        CacheTtlOverridesConfig, DistributedCacheConfig, StoragePointReadCacheConfig,
        StorageQueryProofCacheConfig,
    },
    constants::{
        DEFAULT_CONFIG_VERSION, DEFAULT_HTTP_BIND_ADDR, DEFAULT_JOBS_JITTER_PERCENT,
        DEFAULT_JOBS_MAX_IMMEDIATE_WORKERS, DEFAULT_LOG_DESTINATION,
        DEFAULT_SLOW_OPERATION_LOG_THRESHOLD_MS, DEFAULT_STORAGE_REPLICATION_BATCH_BYTE_LIMIT,
        DEFAULT_STORAGE_REPLICATION_BATCH_MUTATION_LIMIT,
        DEFAULT_STORAGE_REPLICATION_HEARTBEAT_INTERVAL_MS,
        DEFAULT_STORAGE_REPLICATION_HEARTBEAT_JITTER_MS,
        DEFAULT_STORAGE_REPLICATION_POLL_INTERVAL_MS, DEFAULT_TRACING_LOG_LEVEL,
    },
    messaging::{PubsubConfig, QueueConfig},
    sync_replication::StorageSyncReplicationConfig,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RootConfig {
    #[serde(rename = "$schema")]
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub description: Option<String>,
    #[serde(default = "default_version")]
    #[schemars(default = "default_version")]
    pub version: String,
    #[serde(default = "default_roles")]
    #[schemars(default = "default_roles")]
    pub roles: Vec<AppRole>,
    #[serde(default)]
    #[schemars(default)]
    pub http: HttpConfig,
    #[serde(default)]
    #[schemars(default)]
    pub features: Features,
    #[serde(default)]
    #[schemars(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    #[schemars(default)]
    pub pubsub: PubsubConfig,
    #[serde(default)]
    #[schemars(default)]
    pub jobs: Jobs,
}

impl Default for RootConfig {
    fn default() -> Self {
        Self {
            schema: None,
            description: None,
            version: default_version(),
            roles: default_roles(),
            http: HttpConfig::default(),
            features: Features::default(),
            queue: QueueConfig::default(),
            pubsub: PubsubConfig::default(),
            jobs: Jobs::default(),
        }
    }
}

impl RootConfig {
    #[must_use]
    pub fn background_workers_enabled(&self) -> bool {
        self.features.runtime.enable_background_workers
    }
}

fn default_version() -> String {
    DEFAULT_CONFIG_VERSION.to_string()
}

fn default_roles() -> Vec<AppRole> {
    vec![AppRole::Api, AppRole::DatabaseJobs]
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AppRole {
    Api,
    DatabaseJobs,
    PeriodicJobs,
    ImmediateJobs,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct HttpConfig {
    #[serde(default = "default_bind_addr")]
    #[schemars(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default)]
    #[schemars(default)]
    pub routes: HttpRoutesConfig,
    #[serde(default)]
    #[schemars(default)]
    pub trusted_proxy_ips: Vec<IpAddr>,
    #[serde(default)]
    #[schemars(default)]
    pub cors: Cors,
    #[serde(default)]
    #[schemars(default)]
    pub slow_operation_log_ms: SlowOperationLogThresholds,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            routes: HttpRoutesConfig::default(),
            trusted_proxy_ips: Vec::new(),
            cors: Cors::default(),
            slow_operation_log_ms: SlowOperationLogThresholds::default(),
        }
    }
}

fn default_bind_addr() -> String {
    DEFAULT_HTTP_BIND_ADDR.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct HttpRoutesConfig {
    #[serde(default = "default_storage_route")]
    #[schemars(default = "default_storage_route")]
    pub storage: String,
    #[serde(default = "default_queue_route")]
    #[schemars(default = "default_queue_route")]
    pub queue: String,
    #[serde(default = "default_pubsub_route")]
    #[schemars(default = "default_pubsub_route")]
    pub pubsub: String,
}

impl Default for HttpRoutesConfig {
    fn default() -> Self {
        Self {
            storage: default_storage_route(),
            queue: default_queue_route(),
            pubsub: default_pubsub_route(),
        }
    }
}

fn default_storage_route() -> String {
    "/storage".to_string()
}

fn default_queue_route() -> String {
    "/queue".to_string()
}

fn default_pubsub_route() -> String {
    "/pubsub".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[schemars(deny_unknown_fields)]
pub struct Cors {
    #[serde(default)]
    #[schemars(default)]
    pub allow_origins: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SlowOperationLogThresholds {
    #[serde(
        rename = "default",
        default = "default_slow_operation_log_threshold_ms"
    )]
    #[schemars(
        rename = "default",
        default = "default_slow_operation_log_threshold_ms"
    )]
    pub default: u64,
    #[serde(flatten, default)]
    pub overrides: BTreeMap<String, u64>,
}

impl SlowOperationLogThresholds {
    #[must_use]
    pub fn threshold_ms_for(&self, operation_name: &str) -> u64 {
        self.overrides
            .get(operation_name)
            .copied()
            .unwrap_or(self.default)
    }
}

impl Default for SlowOperationLogThresholds {
    fn default() -> Self {
        Self {
            default: default_slow_operation_log_threshold_ms(),
            overrides: BTreeMap::new(),
        }
    }
}

fn default_slow_operation_log_threshold_ms() -> u64 {
    DEFAULT_SLOW_OPERATION_LOG_THRESHOLD_MS
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[schemars(deny_unknown_fields)]
pub struct Features {
    #[serde(default = "default_backends")]
    #[schemars(default = "default_backends")]
    pub backends: Backends,
    #[serde(default)]
    #[schemars(default)]
    pub storage_connections: Option<StorageConnectionsConfig>,
    #[serde(default)]
    #[schemars(default)]
    pub tracing: Tracing,
    #[serde(default)]
    #[schemars(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    #[schemars(default)]
    pub runtime: RuntimeFeatures,
    #[serde(default)]
    #[schemars(default)]
    pub cache_ttls: CacheTtlOverridesConfig,
    #[serde(default)]
    #[schemars(default)]
    pub storage_point_read_cache: StoragePointReadCacheConfig,
    #[serde(default)]
    #[schemars(default)]
    pub storage_query_proof_cache: StorageQueryProofCacheConfig,
    #[serde(default)]
    #[schemars(default)]
    pub distributed_cache: DistributedCacheConfig,
    #[serde(default)]
    #[schemars(default)]
    pub storage_sync_replication: StorageSyncReplicationConfig,
}

fn default_backends() -> Backends {
    Backends::sqlite_default()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct Tracing {
    #[serde(default = "default_tracing_log_level")]
    #[schemars(default = "default_tracing_log_level")]
    pub log_level: Option<String>,
    #[serde(default = "default_log_destination")]
    #[schemars(default = "default_log_destination")]
    pub log_destination: String,
    #[serde(default)]
    #[schemars(default)]
    pub traces: Vec<TracingTrace>,
}

impl Default for Tracing {
    fn default() -> Self {
        Self {
            log_level: default_tracing_log_level(),
            log_destination: default_log_destination(),
            traces: Vec::new(),
        }
    }
}

fn default_tracing_log_level() -> Option<String> {
    Some(DEFAULT_TRACING_LOG_LEVEL.to_string())
}

fn default_log_destination() -> String {
    DEFAULT_LOG_DESTINATION.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    #[schemars(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    #[schemars(default)]
    pub prometheus: PrometheusMetricsConfig,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prometheus: PrometheusMetricsConfig::default(),
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PrometheusMetricsConfig {
    #[serde(default)]
    #[schemars(default)]
    pub bearer_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct TracingTrace {
    pub module_path: String,
    pub log_level: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEnvironment {
    Development,
    Test,
    Staging,
    Production,
}

impl RuntimeEnvironment {
    #[must_use]
    pub const fn allows_debug_endpoints(self) -> bool {
        matches!(self, Self::Development | Self::Test)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RuntimeFeatures {
    #[serde(default = "default_runtime_environment")]
    #[schemars(default = "default_runtime_environment")]
    pub environment: RuntimeEnvironment,
    #[serde(default = "default_true")]
    #[schemars(default = "default_true")]
    pub enable_background_workers: bool,
}

impl Default for RuntimeFeatures {
    fn default() -> Self {
        Self {
            environment: default_runtime_environment(),
            enable_background_workers: true,
        }
    }
}

fn default_runtime_environment() -> RuntimeEnvironment {
    RuntimeEnvironment::Production
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct Jobs {
    #[serde(default = "default_jobs_max_immediate_workers")]
    #[schemars(default = "default_jobs_max_immediate_workers")]
    pub max_immediate_workers: usize,
    #[serde(default = "default_jobs_jitter_percent")]
    #[schemars(default = "default_jobs_jitter_percent")]
    pub jitter_percent: u8,
    #[serde(default)]
    #[schemars(default)]
    pub storage_replication: StorageReplicationConfig,
}

impl Default for Jobs {
    fn default() -> Self {
        Self {
            max_immediate_workers: default_jobs_max_immediate_workers(),
            jitter_percent: default_jobs_jitter_percent(),
            storage_replication: StorageReplicationConfig::default(),
        }
    }
}

fn default_jobs_max_immediate_workers() -> usize {
    DEFAULT_JOBS_MAX_IMMEDIATE_WORKERS
}

fn default_jobs_jitter_percent() -> u8 {
    DEFAULT_JOBS_JITTER_PERCENT
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct StorageReplicationConfig {
    #[serde(default)]
    #[schemars(default)]
    pub enabled: bool,
    #[serde(default)]
    #[schemars(default)]
    pub self_region: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub peers: Vec<StorageReplicationPeerConfig>,
    #[serde(default = "default_storage_replication_poll_interval_ms")]
    #[schemars(default = "default_storage_replication_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_storage_replication_heartbeat_interval_ms")]
    #[schemars(default = "default_storage_replication_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_storage_replication_heartbeat_jitter_ms")]
    #[schemars(default = "default_storage_replication_heartbeat_jitter_ms")]
    pub heartbeat_jitter_ms: u64,
    #[serde(default = "default_storage_replication_batch_mutation_limit")]
    #[schemars(default = "default_storage_replication_batch_mutation_limit")]
    pub batch_mutation_limit: u32,
    #[serde(default = "default_storage_replication_batch_byte_limit")]
    #[schemars(default = "default_storage_replication_batch_byte_limit")]
    pub batch_byte_limit: u64,
}

impl Default for StorageReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            self_region: None,
            peers: Vec::new(),
            poll_interval_ms: default_storage_replication_poll_interval_ms(),
            heartbeat_interval_ms: default_storage_replication_heartbeat_interval_ms(),
            heartbeat_jitter_ms: default_storage_replication_heartbeat_jitter_ms(),
            batch_mutation_limit: default_storage_replication_batch_mutation_limit(),
            batch_byte_limit: default_storage_replication_batch_byte_limit(),
        }
    }
}

impl StorageReplicationConfig {
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct StorageReplicationPeerConfig {
    pub region_name: String,
    pub endpoint_url: String,
    pub service_token: String,
}

fn default_storage_replication_poll_interval_ms() -> u64 {
    DEFAULT_STORAGE_REPLICATION_POLL_INTERVAL_MS
}

fn default_storage_replication_heartbeat_interval_ms() -> u64 {
    DEFAULT_STORAGE_REPLICATION_HEARTBEAT_INTERVAL_MS
}

fn default_storage_replication_heartbeat_jitter_ms() -> u64 {
    DEFAULT_STORAGE_REPLICATION_HEARTBEAT_JITTER_MS
}

fn default_storage_replication_batch_mutation_limit() -> u32 {
    DEFAULT_STORAGE_REPLICATION_BATCH_MUTATION_LIMIT
}

fn default_storage_replication_batch_byte_limit() -> u64 {
    DEFAULT_STORAGE_REPLICATION_BATCH_BYTE_LIMIT
}
