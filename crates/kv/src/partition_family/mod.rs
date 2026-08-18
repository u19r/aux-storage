use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use storage_types::{StorageResult, StreamItemId, StreamName};
use tracing::warn;

pub(crate) mod model;
#[cfg(test)]
mod model_tests;

pub use model::*;

use crate::sorted_kv_store::SortedKvStore;

#[async_trait::async_trait]
pub trait PartitionFamilyKvStore: SortedKvStore {
    /// Stable namespace identity embedded in FoundationDB Tuple keys. Other
    /// sorted providers have one logical namespace and therefore use empty
    /// bytes; FoundationDB supplies its configured tenant identity.
    fn tenant_keyspace(&self) -> Vec<u8> {
        Vec::new()
    }

    /// Whether this store has the exact physical namespace required by the
    /// canonical FoundationDB mapped-range descriptor.  Providers must use
    /// this capability before constructing a mapper; a generic sorted store
    /// must never infer that its compact keys are Tuple-compatible.
    fn supports_read_sequence_mapped_range(&self) -> bool {
        false
    }

    /// FoundationDB API version proved by the backend binding/configuration.
    /// A zero value means that mapped range is unavailable.
    fn read_sequence_mapped_range_api_version(&self) -> u32 {
        0
    }

    /// Execute the provider-owned FoundationDB mapped-range primitive.  The
    /// default is an explicit capability miss; the API layer falls back to the
    /// ordinary validated DAG without attempting to interpret physical bytes.
    async fn read_sequence_mapped_range(
        &self,
        _request: storage_provider::ReadSequenceMappedRangeRequest,
    ) -> StorageResult<Option<storage_provider::ReadSequenceMappedRangePage>> {
        Ok(None)
    }

    fn supports_partition_families(&self) -> bool;

    async fn append_partitioned_ordered_log_item(
        &self,
        stream_name: &StreamName,
        routing_key: &[u8],
        value: &[u8],
        fallback_item_id: StreamItemId,
    ) -> StorageResult<Option<StreamItemId>>;

    async fn load_partition_family_state_raw(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
    ) -> StorageResult<Option<ResolvedPartitionFamily>> {
        if !self.supports_partition_families() {
            return Ok(None);
        }

        let config_key = partition_family_config_key(family_kind, family_component);
        let Some(config_bytes) = self.get(&config_key, true).await? else {
            return Ok(None);
        };
        let config = parse_partition_family_config(&config_bytes)?;

        let partition_prefix = partition_info_prefix(family_kind, family_component);
        let partition_entries = self.get_prefix(&partition_prefix, true, None, true).await?;
        let mut partitions = Vec::with_capacity(partition_entries.items.len());
        for (_key, value) in partition_entries.items {
            partitions.push(parse_partition_info(&value)?);
        }
        partitions.sort_unstable_by(|left, right| {
            left.hash_start_inclusive
                .cmp(&right.hash_start_inclusive)
                .then_with(|| left.partition_id.cmp(&right.partition_id))
        });

        Ok(Some(ResolvedPartitionFamily { config, partitions }))
    }

    async fn drain_runtime_partition_load_samples(
        &self,
    ) -> StorageResult<Vec<RuntimePartitionLoadSample>>;

    fn partition_runtime_load_hint(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
        partition_id: u16,
    ) -> u64;

    async fn wait_for_change(&self, key: &[u8], timeout: Duration) -> StorageResult<bool>;

    async fn split_partitioned_ordered_log_family(
        &self,
        family_component: &str,
        partition_id: u16,
        now_ms: i64,
    ) -> StorageResult<bool>;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PartitionFamilyCacheKey {
    family_kind: PartitionFamilyKind,
    family_component: String,
}

impl PartitionFamilyCacheKey {
    #[must_use]
    pub(crate) fn new(family_kind: PartitionFamilyKind, family_component: &str) -> Self {
        Self {
            family_kind,
            family_component: family_component.to_string(),
        }
    }

    #[must_use]
    pub(crate) fn watch_key(&self) -> Vec<u8> {
        partition_family_config_key(self.family_kind, &self.family_component)
    }
}

#[derive(Clone)]
pub(crate) struct PartitionFamilyCacheEntry {
    pub(crate) family: ResolvedPartitionFamily,
    pub(crate) generation: u64,
}

impl PartitionFamilyCacheEntry {
    #[must_use]
    pub(crate) fn new(family: ResolvedPartitionFamily, generation: u64) -> Self {
        Self { family, generation }
    }
}

#[derive(Default)]
pub(crate) struct PartitionFamilyWatchRegistry {
    generations: Mutex<HashMap<PartitionFamilyCacheKey, u64>>,
}

impl PartitionFamilyWatchRegistry {
    pub(crate) fn register_generation(
        &self,
        key: PartitionFamilyCacheKey,
        generation: u64,
    ) -> bool {
        let mut generations = self.lock_generations();
        match generations.get(&key) {
            Some(existing_generation) if *existing_generation == generation => false,
            _ => {
                generations.insert(key, generation);
                true
            }
        }
    }

    pub(crate) fn remove_if_generation(
        &self,
        key: &PartitionFamilyCacheKey,
        generation: u64,
    ) -> bool {
        let mut generations = self.lock_generations();
        if generations.get(key).copied() == Some(generation) {
            generations.remove(key);
            return true;
        }
        false
    }

    pub(crate) fn remove(&self, key: &PartitionFamilyCacheKey) {
        self.lock_generations().remove(key);
    }

    fn lock_generations(&self) -> MutexGuard<'_, HashMap<PartitionFamilyCacheKey, u64>> {
        match self.generations.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("partition family watch registry mutex poisoned, recovering inner state");
                poisoned.into_inner()
            }
        }
    }
}
