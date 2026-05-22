use std::collections::{BTreeMap, BTreeSet, HashMap};

use storage_cache::push_unique_runtime_coverage_range;
use storage_types::{KeyAttributes, TableName};

use crate::{
    query_proof_request::{DerivedQueryManifestEntry, DerivedQueryPage},
    query_proof_store::state::{
        clear_partition_coverage, insert_manifest_entry, remove_manifest_entry,
    },
    query_proof_types::{
        InMemoryQueryProofCacheConfig, QueryCoverageState, QueryManifestEntry, QueryManifestKey,
        QueryManifestSnapshot,
    },
};

pub type QueryProofMaterializedPage =
    storage_cache::RuntimeQueryProofMaterializedPage<KeyAttributes>;
pub type PreparedQueryProofRead = storage_cache::RuntimePreparedQueryProofRead<KeyAttributes>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct LruKey {
    pub(super) tick: u64,
    pub(super) manifest_key: QueryManifestKey,
}

#[derive(Debug, Clone)]
pub(crate) struct QueryManifestPartitionState {
    pub(super) key: QueryManifestKey,
    pub(super) entries: BTreeMap<String, QueryManifestEntry>,
    pub(super) ordered_entry_keys: BTreeSet<QueryManifestOrderKey>,
    pub(super) page_witnesses: BTreeMap<QueryPageWitnessKey, QueryPageWitness>,
    pub(super) coverage: QueryCoverageState,
    pub(super) lru: LruKey,
}

type QueryPageWitnessKey = storage_cache::RuntimePageWitnessKey<String>;
type QueryPageWitness = storage_cache::RuntimePageWitness<String>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct QueryManifestOrderKey {
    pub(super) sort_key_order_repr: Option<String>,
    pub(super) primary_key_json: String,
}

pub(crate) struct InMemoryQueryProofCacheState {
    pub(super) config: InMemoryQueryProofCacheConfig,
    pub(super) partitions: HashMap<QueryManifestKey, QueryManifestPartitionState>,
    pub(super) lru_order: BTreeSet<LruKey>,
    pub(super) total_manifest_entries: usize,
    pub(super) total_coverage_ranges: usize,
    pub(super) next_tick: u64,
}

impl InMemoryQueryProofCacheState {
    pub(crate) fn new(config: InMemoryQueryProofCacheConfig) -> Self {
        Self {
            config,
            partitions: HashMap::new(),
            lru_order: BTreeSet::new(),
            total_manifest_entries: 0,
            total_coverage_ranges: 0,
            next_tick: 0,
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.config.max_query_spaces > 0
            && self.config.max_manifest_entries > 0
            && self.config.max_coverage_ranges > 0
    }

    pub(crate) fn snapshot_base_partition(
        &self,
        key: &QueryManifestKey,
    ) -> Option<QueryManifestSnapshot> {
        let partition = self.partitions.get(key)?;
        Some(QueryManifestSnapshot {
            key: partition.key.clone(),
            entries: partition
                .ordered_entry_keys
                .iter()
                .filter_map(|order_key| partition.entries.get(&order_key.primary_key_json))
                .cloned()
                .collect(),
            coverage: partition.coverage.clone(),
        })
    }

    pub(crate) fn record_put(&mut self, derived: DerivedQueryManifestEntry) {
        if !self.is_enabled() {
            return;
        }

        let manifest_key = derived.manifest_key.clone();
        let schema_fingerprint = derived.schema_fingerprint;
        self.ensure_partition(manifest_key.clone(), schema_fingerprint);
        self.touch_partition(&manifest_key);

        let Some(partition) = self.partitions.get_mut(&manifest_key) else {
            return;
        };

        clear_partition_coverage(
            partition,
            schema_fingerprint,
            &mut self.total_coverage_ranges,
        );

        insert_manifest_entry(partition, derived.entry, &mut self.total_manifest_entries);
        self.evict_over_budget();
    }

    pub(crate) fn record_delete(&mut self, derived: DerivedQueryManifestEntry) {
        if !self.is_enabled() {
            return;
        }

        let manifest_key = derived.manifest_key.clone();
        let schema_fingerprint = derived.schema_fingerprint;
        self.ensure_partition(manifest_key.clone(), schema_fingerprint);
        self.touch_partition(&manifest_key);

        let mut remove_partition = false;
        if let Some(partition) = self.partitions.get_mut(&manifest_key) {
            remove_manifest_entry(
                partition,
                &derived.entry.primary_key_json,
                &mut self.total_manifest_entries,
            );
            clear_partition_coverage(
                partition,
                schema_fingerprint,
                &mut self.total_coverage_ranges,
            );
            remove_partition = partition.entries.is_empty();
        }

        if remove_partition {
            self.remove_partition(&manifest_key);
        } else {
            self.evict_over_budget();
        }
    }

    pub(crate) fn record_index_transition(
        &mut self,
        old_entry: Option<DerivedQueryManifestEntry>,
        new_entry: Option<DerivedQueryManifestEntry>,
    ) {
        if !self.is_enabled() {
            return;
        }

        let mut touched_manifest_keys = BTreeSet::new();
        if let Some(old_entry) = old_entry.as_ref() {
            touched_manifest_keys.insert(old_entry.manifest_key.clone());
        }
        if let Some(new_entry) = new_entry.as_ref() {
            touched_manifest_keys.insert(new_entry.manifest_key.clone());
        }

        for manifest_key in &touched_manifest_keys {
            let schema_fingerprint = new_entry
                .as_ref()
                .filter(|entry| entry.manifest_key == *manifest_key)
                .map(|entry| entry.schema_fingerprint)
                .or_else(|| {
                    old_entry
                        .as_ref()
                        .filter(|entry| entry.manifest_key == *manifest_key)
                        .map(|entry| entry.schema_fingerprint)
                });
            let Some(schema_fingerprint) = schema_fingerprint else {
                continue;
            };
            self.ensure_partition(manifest_key.clone(), schema_fingerprint);
            self.touch_partition(manifest_key);
            let Some(partition) = self.partitions.get_mut(manifest_key) else {
                continue;
            };
            clear_partition_coverage(
                partition,
                schema_fingerprint,
                &mut self.total_coverage_ranges,
            );
        }

        if let Some(old_entry) = old_entry {
            let manifest_key = old_entry.manifest_key.clone();
            let mut remove_partition = false;
            if let Some(partition) = self.partitions.get_mut(&manifest_key) {
                remove_manifest_entry(
                    partition,
                    &old_entry.entry.primary_key_json,
                    &mut self.total_manifest_entries,
                );
                remove_partition = partition.entries.is_empty();
            }
            if remove_partition {
                self.remove_partition(&manifest_key);
            }
        }

        if let Some(new_entry) = new_entry {
            let manifest_key = new_entry.manifest_key.clone();
            self.ensure_partition(manifest_key.clone(), new_entry.schema_fingerprint);
            if let Some(partition) = self.partitions.get_mut(&manifest_key) {
                insert_manifest_entry(partition, new_entry.entry, &mut self.total_manifest_entries);
            }
        }

        self.evict_over_budget();
    }

    pub(crate) fn invalidate_partition_coverage(
        &mut self,
        manifest_key: QueryManifestKey,
        schema_fingerprint: u64,
    ) {
        if !self.is_enabled() {
            return;
        }

        self.ensure_partition(manifest_key.clone(), schema_fingerprint);
        self.touch_partition(&manifest_key);

        if let Some(partition) = self.partitions.get_mut(&manifest_key) {
            clear_partition_coverage(
                partition,
                schema_fingerprint,
                &mut self.total_coverage_ranges,
            );
        }

        self.evict_over_budget();
    }

    pub(crate) fn record_query_page(&mut self, page: DerivedQueryPage) {
        if !self.is_enabled() {
            return;
        }

        let manifest_key = page.manifest_key.clone();
        self.ensure_partition(manifest_key.clone(), page.schema_fingerprint);
        self.touch_partition(&manifest_key);

        let Some(partition) = self.partitions.get_mut(&manifest_key) else {
            return;
        };

        if partition.coverage.schema_fingerprint != page.schema_fingerprint {
            clear_partition_coverage(
                partition,
                page.schema_fingerprint,
                &mut self.total_coverage_ranges,
            );
        }

        for entry in page.entries {
            insert_manifest_entry(partition, entry, &mut self.total_manifest_entries);
        }

        if let Some(coverage_range) = page.coverage_range {
            self.total_coverage_ranges = self
                .total_coverage_ranges
                .saturating_sub(partition.coverage.covered_ranges.len())
                .saturating_sub(partition.coverage.current_schema_ranges.len());
            push_unique_runtime_coverage_range(
                &mut partition.coverage.covered_ranges,
                coverage_range.clone(),
            );
            push_unique_runtime_coverage_range(
                &mut partition.coverage.current_schema_ranges,
                coverage_range,
            );
            partition.coverage.continuity_broken = false;
            partition.coverage.rebuilding = false;
            self.total_coverage_ranges += partition.coverage.covered_ranges.len();
            self.total_coverage_ranges += partition.coverage.current_schema_ranges.len();
        }

        if let Some(page_witness) = page.page_witness {
            partition
                .page_witnesses
                .insert(page_witness.key.clone(), page_witness);
        }

        self.evict_over_budget();
    }
    pub(crate) fn invalidate_table(&mut self, table_name: &TableName) {
        let manifest_keys = self
            .partitions
            .keys()
            .filter(|key| &key.table_name == table_name)
            .cloned()
            .collect::<Vec<_>>();
        for manifest_key in manifest_keys {
            self.remove_partition(&manifest_key);
        }
    }

    pub(crate) fn invalidate_index_query_spaces(&mut self, table_name: &TableName) {
        let manifest_keys = self
            .partitions
            .keys()
            .filter(|key| &key.table_name == table_name && key.index_name.is_some())
            .cloned()
            .collect::<Vec<_>>();
        for manifest_key in manifest_keys {
            self.remove_partition(&manifest_key);
        }
    }
}
