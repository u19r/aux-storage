use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::constants::{
    DEFAULT_DISTRIBUTED_CACHE_VNODES_PER_NODE, DEFAULT_STORAGE_POINT_READ_CACHE_CAPACITY,
    DEFAULT_STORAGE_POINT_READ_CACHE_MEGABYTES_PER_CORE,
    DEFAULT_STORAGE_POINT_READ_CACHE_TTL_SECONDS,
    DEFAULT_STORAGE_QUERY_PROOF_CACHE_MAX_COVERAGE_RANGES,
    DEFAULT_STORAGE_QUERY_PROOF_CACHE_MAX_MANIFEST_ENTRIES,
    DEFAULT_STORAGE_QUERY_PROOF_CACHE_MAX_QUERY_SPACES,
};

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct CacheTtlOverridesConfig {
    #[serde(default = "default_true")]
    #[schemars(default = "default_true")]
    pub enable_background_refresh: bool,
}

impl Default for CacheTtlOverridesConfig {
    fn default() -> Self {
        Self {
            enable_background_refresh: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StoragePointReadCacheEvictionPolicy {
    Lru,
    TwoQueue,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StoragePointReadCacheMemoryBudgetMode {
    AutoPerCore { megabytes_per_core: u64 },
    FixedBytes { bytes: u64 },
    PercentOfAvailableMemory { percent: u8 },
}

impl Default for StoragePointReadCacheMemoryBudgetMode {
    fn default() -> Self {
        Self::AutoPerCore {
            megabytes_per_core: DEFAULT_STORAGE_POINT_READ_CACHE_MEGABYTES_PER_CORE,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
#[schemars(deny_unknown_fields)]
pub struct StoragePointReadCacheMemoryBudgetConfig {
    #[serde(default)]
    #[schemars(default)]
    pub mode: StoragePointReadCacheMemoryBudgetMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct StoragePointReadCacheConfig {
    #[serde(default)]
    #[schemars(default)]
    pub enabled: bool,
    #[serde(default = "default_storage_point_read_cache_capacity")]
    #[schemars(default = "default_storage_point_read_cache_capacity")]
    pub capacity: usize,
    #[serde(default)]
    #[schemars(default)]
    pub max_bytes: Option<usize>,
    #[serde(default)]
    #[schemars(default)]
    pub memory_budget: StoragePointReadCacheMemoryBudgetConfig,
    #[serde(default = "default_storage_point_read_cache_ttl_seconds")]
    #[schemars(default = "default_storage_point_read_cache_ttl_seconds")]
    pub ttl_seconds: u64,
    #[serde(default = "default_storage_point_read_cache_eviction_policy")]
    #[schemars(default = "default_storage_point_read_cache_eviction_policy")]
    pub eviction_policy: StoragePointReadCacheEvictionPolicy,
    #[serde(default)]
    #[schemars(default)]
    pub authoritative_strong_point_reads: bool,
    #[serde(default)]
    #[schemars(default)]
    pub authoritative_write_preimages: bool,
    #[serde(default)]
    #[schemars(default)]
    pub strong_read_through_warming: bool,
}

impl Default for StoragePointReadCacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            capacity: default_storage_point_read_cache_capacity(),
            max_bytes: None,
            memory_budget: StoragePointReadCacheMemoryBudgetConfig::default(),
            ttl_seconds: default_storage_point_read_cache_ttl_seconds(),
            eviction_policy: default_storage_point_read_cache_eviction_policy(),
            authoritative_strong_point_reads: false,
            authoritative_write_preimages: false,
            strong_read_through_warming: false,
        }
    }
}

fn default_storage_point_read_cache_capacity() -> usize {
    DEFAULT_STORAGE_POINT_READ_CACHE_CAPACITY
}

fn default_storage_point_read_cache_ttl_seconds() -> u64 {
    DEFAULT_STORAGE_POINT_READ_CACHE_TTL_SECONDS
}

fn default_storage_point_read_cache_eviction_policy() -> StoragePointReadCacheEvictionPolicy {
    StoragePointReadCacheEvictionPolicy::TwoQueue
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageQueryProofCacheEvictionPolicy {
    PartitionLru,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct StorageQueryProofCacheConfig {
    #[serde(default)]
    #[schemars(default)]
    pub enabled: bool,
    #[serde(default = "default_storage_query_proof_cache_max_query_spaces")]
    #[schemars(default = "default_storage_query_proof_cache_max_query_spaces")]
    pub max_query_spaces: usize,
    #[serde(default = "default_storage_query_proof_cache_max_manifest_entries")]
    #[schemars(default = "default_storage_query_proof_cache_max_manifest_entries")]
    pub max_manifest_entries: usize,
    #[serde(default = "default_storage_query_proof_cache_max_coverage_ranges")]
    #[schemars(default = "default_storage_query_proof_cache_max_coverage_ranges")]
    pub max_coverage_ranges: usize,
    #[serde(default = "default_storage_query_proof_cache_eviction_policy")]
    #[schemars(default = "default_storage_query_proof_cache_eviction_policy")]
    pub eviction_policy: StorageQueryProofCacheEvictionPolicy,
}

impl Default for StorageQueryProofCacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_query_spaces: default_storage_query_proof_cache_max_query_spaces(),
            max_manifest_entries: default_storage_query_proof_cache_max_manifest_entries(),
            max_coverage_ranges: default_storage_query_proof_cache_max_coverage_ranges(),
            eviction_policy: default_storage_query_proof_cache_eviction_policy(),
        }
    }
}

fn default_storage_query_proof_cache_max_query_spaces() -> usize {
    DEFAULT_STORAGE_QUERY_PROOF_CACHE_MAX_QUERY_SPACES
}

fn default_storage_query_proof_cache_max_manifest_entries() -> usize {
    DEFAULT_STORAGE_QUERY_PROOF_CACHE_MAX_MANIFEST_ENTRIES
}

fn default_storage_query_proof_cache_max_coverage_ranges() -> usize {
    DEFAULT_STORAGE_QUERY_PROOF_CACHE_MAX_COVERAGE_RANGES
}

fn default_storage_query_proof_cache_eviction_policy() -> StorageQueryProofCacheEvictionPolicy {
    StorageQueryProofCacheEvictionPolicy::PartitionLru
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct DistributedCacheConfig {
    #[serde(default)]
    #[schemars(default)]
    pub enabled: bool,
    #[serde(default = "default_distributed_cache_vnodes")]
    #[schemars(default = "default_distributed_cache_vnodes")]
    pub vnodes_per_node: usize,
}

impl Default for DistributedCacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            vnodes_per_node: default_distributed_cache_vnodes(),
        }
    }
}

fn default_distributed_cache_vnodes() -> usize {
    DEFAULT_DISTRIBUTED_CACHE_VNODES_PER_NODE
}
