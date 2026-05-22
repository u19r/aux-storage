use std::collections::{BTreeMap, BTreeSet};

use crate::{
    query_proof_store::{
        InMemoryQueryProofCacheState, LruKey, QueryManifestEntry, QueryManifestKey,
        QueryManifestOrderKey, QueryManifestPartitionState,
    },
    query_proof_types::{QueryCoverageState, QueryProofCacheEvictionPolicy},
};

impl InMemoryQueryProofCacheState {
    pub(super) fn ensure_partition(
        &mut self,
        manifest_key: QueryManifestKey,
        schema_fingerprint: u64,
    ) {
        if self.partitions.contains_key(&manifest_key) {
            return;
        }

        let lru = LruKey {
            tick: self.next_tick(),
            manifest_key: manifest_key.clone(),
        };
        self.lru_order.insert(lru.clone());
        self.partitions.insert(
            manifest_key.clone(),
            QueryManifestPartitionState {
                key: manifest_key,
                entries: BTreeMap::new(),
                ordered_entry_keys: BTreeSet::new(),
                page_witnesses: BTreeMap::new(),
                coverage: QueryCoverageState {
                    covered_ranges: Vec::new(),
                    current_schema_ranges: Vec::new(),
                    continuity_broken: false,
                    rebuilding: false,
                    schema_fingerprint,
                },
                lru,
            },
        );
    }

    pub(super) fn touch_partition(&mut self, manifest_key: &QueryManifestKey) {
        let next_tick = self.next_tick();
        let Some(partition) = self.partitions.get_mut(manifest_key) else {
            return;
        };
        let old_lru = partition.lru.clone();
        self.lru_order.remove(&old_lru);
        let new_lru = LruKey {
            tick: next_tick,
            manifest_key: manifest_key.clone(),
        };
        partition.lru = new_lru.clone();
        self.lru_order.insert(new_lru);
    }

    pub(super) fn evict_over_budget(&mut self) {
        while self.partitions.len() > self.config.max_query_spaces
            || self.total_manifest_entries > self.config.max_manifest_entries
            || self.total_coverage_ranges > self.config.max_coverage_ranges
        {
            let Some(lru) = self.lru_order.iter().next().cloned() else {
                break;
            };
            match self.config.eviction_policy {
                QueryProofCacheEvictionPolicy::PartitionLru => {
                    self.remove_partition(&lru.manifest_key);
                }
            }
        }
    }

    pub(super) fn remove_partition(&mut self, manifest_key: &QueryManifestKey) {
        let Some(partition) = self.partitions.remove(manifest_key) else {
            return;
        };
        self.lru_order.remove(&partition.lru);
        self.total_manifest_entries = self
            .total_manifest_entries
            .saturating_sub(partition.entries.len());
        self.total_coverage_ranges = self
            .total_coverage_ranges
            .saturating_sub(partition.coverage.covered_ranges.len())
            .saturating_sub(partition.coverage.current_schema_ranges.len());
    }

    fn next_tick(&mut self) -> u64 {
        let tick = self.next_tick;
        self.next_tick = self.next_tick.saturating_add(1);
        tick
    }
}

impl QueryManifestOrderKey {
    pub(super) fn from_entry(entry: &QueryManifestEntry) -> Self {
        Self {
            sort_key_order_repr: entry.sort_key_order_repr.clone(),
            primary_key_json: entry.primary_key_json.clone(),
        }
    }
}

pub(super) fn clear_partition_coverage(
    partition: &mut QueryManifestPartitionState,
    schema_fingerprint: u64,
    total_coverage_ranges: &mut usize,
) {
    *total_coverage_ranges = total_coverage_ranges
        .saturating_sub(partition.coverage.covered_ranges.len())
        .saturating_sub(partition.coverage.current_schema_ranges.len());
    partition.coverage.schema_fingerprint = schema_fingerprint;
    partition.coverage.covered_ranges.clear();
    partition.coverage.current_schema_ranges.clear();
    partition.page_witnesses.clear();
}

pub(super) fn insert_manifest_entry(
    partition: &mut QueryManifestPartitionState,
    entry: QueryManifestEntry,
    total_manifest_entries: &mut usize,
) {
    if let Some(previous) = partition
        .entries
        .insert(entry.primary_key_json.clone(), entry.clone())
    {
        let _ = partition
            .ordered_entry_keys
            .remove(&QueryManifestOrderKey::from_entry(&previous));
    } else {
        *total_manifest_entries += 1;
    }
    partition
        .ordered_entry_keys
        .insert(QueryManifestOrderKey::from_entry(&entry));
}

pub(super) fn remove_manifest_entry(
    partition: &mut QueryManifestPartitionState,
    primary_key_json: &str,
    total_manifest_entries: &mut usize,
) {
    let Some(previous) = partition.entries.remove(primary_key_json) else {
        return;
    };
    let _ = partition
        .ordered_entry_keys
        .remove(&QueryManifestOrderKey::from_entry(&previous));
    *total_manifest_entries = total_manifest_entries.saturating_sub(1);
}
