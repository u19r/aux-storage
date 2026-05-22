use storage_types::{KeyAttributes, TableName};

pub type QueryCoverageRange = storage_cache::RuntimeCoverageRange<String>;

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct QueryManifestKey {
    pub table_name: TableName,
    pub index_name: Option<String>,
    pub partition_key_json: String,
}

impl Eq for QueryManifestKey {}

impl Ord for QueryManifestKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.table_name
            .to_string()
            .cmp(&other.table_name.to_string())
            .then_with(|| self.index_name.cmp(&other.index_name))
            .then_with(|| self.partition_key_json.cmp(&other.partition_key_json))
    }
}

impl PartialOrd for QueryManifestKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryManifestEntry {
    pub primary_key: KeyAttributes,
    pub query_space_key: KeyAttributes,
    pub primary_key_json: String,
    pub sort_key_order_repr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryCoverageState {
    pub covered_ranges: Vec<QueryCoverageRange>,
    pub current_schema_ranges: Vec<QueryCoverageRange>,
    pub continuity_broken: bool,
    pub rebuilding: bool,
    pub schema_fingerprint: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryManifestSnapshot {
    pub key: QueryManifestKey,
    pub entries: Vec<QueryManifestEntry>,
    pub coverage: QueryCoverageState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryProofCacheEvictionPolicy {
    PartitionLru,
}

#[derive(Debug, Clone, Copy)]
pub struct InMemoryQueryProofCacheConfig {
    pub max_query_spaces: usize,
    pub max_manifest_entries: usize,
    pub max_coverage_ranges: usize,
    pub eviction_policy: QueryProofCacheEvictionPolicy,
}

impl Default for InMemoryQueryProofCacheConfig {
    fn default() -> Self {
        Self {
            max_query_spaces: 2_048,
            max_manifest_entries: 200_000,
            max_coverage_ranges: 20_000,
            eviction_policy: QueryProofCacheEvictionPolicy::PartitionLru,
        }
    }
}
