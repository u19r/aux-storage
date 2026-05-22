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

pub(crate) use model::*;

use crate::sorted_kv_store::SortedKvStore;

#[async_trait::async_trait]
pub trait PartitionFamilyKvStore: SortedKvStore {
    fn supports_partition_families(&self) -> bool;

    async fn append_partitioned_ordered_log_item(
        &self,
        stream_name: &StreamName,
        routing_key: &[u8],
        value: &[u8],
        fallback_item_id: StreamItemId,
    ) -> StorageResult<Option<StreamItemId>>;

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
