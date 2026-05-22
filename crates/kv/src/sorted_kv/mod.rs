use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bg_jobs::JobManager;
use lru_ttl_cache::{CacheConfig, LruTtlCache};
use storage_common::ttl::TtlConfigRecord;
use storage_types::{StorageResult, StoredTableInfo, TableName};
use uuid::Uuid;

pub(crate) use crate::partition_family::PartitionFamilyCacheKey;
use crate::{
    constants::{
        PARTITION_FAMILY_CACHE_CAPACITY, PARTITION_FAMILY_CACHE_TTL_SECONDS,
        PARTITION_FAMILY_CACHE_WATCH_TIMEOUT_SECONDS, TABLE_CACHE_CAPACITY,
        TABLE_CACHE_TTL_SECONDS, TABLE_METADATA_HOT_CACHE_CAPACITY,
        TABLE_METADATA_HOT_CACHE_TTL_MILLIS, TTL_CONFIG_CACHE_CAPACITY,
        TTL_CONFIG_CACHE_TTL_SECONDS,
    },
    keys::table_metadata_key,
    partition_family::{
        PartitionFamilyCacheEntry, PartitionFamilyKvStore, PartitionFamilyWatchRegistry,
        ResolvedPartitionFamily,
    },
    partition_runtime_load::RuntimePartitionLoadTracker,
    sorted_kv_store::SortedKvStore,
};

pub(crate) struct TtlConfigCacheEntry {
    config: Option<TtlConfigRecord>,
}

impl TtlConfigCacheEntry {
    pub(crate) fn new(config: Option<TtlConfigRecord>) -> Self {
        Self { config }
    }

    pub(crate) fn config(&self) -> Option<TtlConfigRecord> {
        self.config.clone()
    }
}

#[derive(Clone)]
pub struct SortedKvDbStorageProvider<S: SortedKvStore> {
    pub(crate) kv_store: S,
    pub(crate) table_cache_lru: LruTtlCache<TableName, Arc<StoredTableInfo>>,
    table_metadata_hot_cache: Arc<TableMetadataHotCache>,
    pub(crate) ttl_config_cache_lru: LruTtlCache<TableName, Arc<TtlConfigCacheEntry>>,
    pub(crate) partition_family_cache_lru:
        LruTtlCache<PartitionFamilyCacheKey, Arc<PartitionFamilyCacheEntry>>,
    pub(crate) partition_family_watch_registry: Arc<PartitionFamilyWatchRegistry>,
    pub(crate) partition_family_cache_generation: Arc<AtomicU64>,
    pub(crate) runtime_partition_load_tracker: RuntimePartitionLoadTracker,
    pub(crate) queue_receive_hint_cursors: Arc<Mutex<HashMap<String, usize>>>,
    pub(crate) partition_sample_publisher_id: Arc<String>,
    pub(crate) job_manager: JobManager,
    pub(crate) database_jobs_enabled: bool,
    pub(crate) immediate_gsi_consistency: bool,
}

impl<S: SortedKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub fn new(kv_store: S) -> Self {
        Self {
            kv_store,
            table_cache_lru: LruTtlCache::new(
                CacheConfig::new()
                    .with_capacity(TABLE_CACHE_CAPACITY)
                    .with_ttl(Duration::from_secs(TABLE_CACHE_TTL_SECONDS)),
            ),
            table_metadata_hot_cache: Arc::new(TableMetadataHotCache::new()),
            ttl_config_cache_lru: LruTtlCache::new(
                CacheConfig::new()
                    .with_capacity(TTL_CONFIG_CACHE_CAPACITY)
                    .with_ttl(Duration::from_secs(TTL_CONFIG_CACHE_TTL_SECONDS)),
            ),
            partition_family_cache_lru: LruTtlCache::new(
                CacheConfig::new()
                    .with_capacity(PARTITION_FAMILY_CACHE_CAPACITY)
                    .with_ttl(Duration::from_secs(PARTITION_FAMILY_CACHE_TTL_SECONDS)),
            ),
            partition_family_watch_registry: Arc::new(PartitionFamilyWatchRegistry::default()),
            partition_family_cache_generation: Arc::new(AtomicU64::new(0)),
            runtime_partition_load_tracker: RuntimePartitionLoadTracker::default(),
            queue_receive_hint_cursors: Arc::new(Mutex::new(HashMap::new())),
            partition_sample_publisher_id: Arc::new(Uuid::now_v7().to_string()),
            job_manager: JobManager::new_for_test(),
            database_jobs_enabled: true,
            immediate_gsi_consistency: false,
        }
    }

    #[must_use]
    pub fn with_job_manager(mut self, job_manager: JobManager) -> Self {
        self.job_manager = job_manager;
        self
    }

    #[must_use]
    pub fn with_database_jobs_enabled(mut self, enabled: bool) -> Self {
        self.database_jobs_enabled = enabled;
        self
    }

    #[must_use]
    pub fn with_immediate_gsi_consistency(mut self, enabled: bool) -> Self {
        self.immediate_gsi_consistency = enabled;
        self
    }

    pub(crate) async fn get_table_metadata_from_name(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<StoredTableInfo>> {
        if let Some(stored_table_info) = self.get_table_metadata_from_name_arc(table_name).await? {
            return Ok(Some((*stored_table_info).clone()));
        }

        Ok(None)
    }

    pub(crate) async fn get_table_metadata_from_name_arc(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<Arc<StoredTableInfo>>> {
        if let Some(stored_table_info) = self.table_metadata_hot_cache.get(table_name) {
            return Ok(Some(stored_table_info));
        }

        if let Some(stored_table_info) = self.table_cache_lru.get(table_name) {
            self.table_metadata_hot_cache
                .insert(table_name.clone(), Arc::clone(&stored_table_info));
            return Ok(Some(stored_table_info));
        }

        let key = table_metadata_key(table_name);

        match self.kv_store.get(&key, true).await? {
            Some(data) => {
                let stored_table_info: StoredTableInfo =
                    storage_types::storage_serde::from_bytes(&data)?;
                let stored_table_info = Arc::new(stored_table_info);
                self.cache_table_metadata(table_name.clone(), Arc::clone(&stored_table_info));
                Ok(Some(stored_table_info))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn cache_table_metadata(
        &self,
        table_name: TableName,
        table_info: Arc<StoredTableInfo>,
    ) {
        self.table_metadata_hot_cache
            .insert(table_name.clone(), Arc::clone(&table_info));
        self.table_cache_lru.insert(table_name, table_info);
    }

    pub(crate) fn invalidate_table_metadata_cache(&self, table_name: &TableName) {
        self.table_metadata_hot_cache.remove(table_name);
        self.table_cache_lru.remove(table_name);
    }

    pub(crate) fn invalidate_partition_family_cache(&self, key: &PartitionFamilyCacheKey) {
        let removed_generation = self
            .partition_family_cache_lru
            .remove(key)
            .map(|entry| entry.generation);
        match removed_generation {
            Some(generation) => {
                self.partition_family_watch_registry
                    .remove_if_generation(key, generation);
            }
            None => {
                self.partition_family_watch_registry.remove(key);
            }
        }
    }
}

pub(crate) struct TableMetadataHotCache {
    entries: RwLock<Vec<TableMetadataHotCacheEntry>>,
}

struct TableMetadataHotCacheEntry {
    table_name: TableName,
    table_info: Arc<StoredTableInfo>,
    expires_at: Instant,
}

impl TableMetadataHotCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::with_capacity(TABLE_METADATA_HOT_CACHE_CAPACITY)),
        }
    }

    pub(crate) fn get(&self, table_name: &TableName) -> Option<Arc<StoredTableInfo>> {
        let now = Instant::now();
        let entries = match self.entries.read() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };

        entries
            .iter()
            .find(|entry| &entry.table_name == table_name && entry.expires_at > now)
            .map(|entry| Arc::clone(&entry.table_info))
    }

    pub(crate) fn insert(&self, table_name: TableName, table_info: Arc<StoredTableInfo>) {
        let now = Instant::now();
        let expires_at = now + Duration::from_millis(TABLE_METADATA_HOT_CACHE_TTL_MILLIS);
        let mut entries = match self.entries.write() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };

        entries.retain(|entry| entry.table_name != table_name && entry.expires_at > now);
        if entries.len() >= TABLE_METADATA_HOT_CACHE_CAPACITY {
            entries.remove(0);
        }

        entries.push(TableMetadataHotCacheEntry {
            table_name,
            table_info,
            expires_at,
        });
    }

    pub(crate) fn remove(&self, table_name: &TableName) {
        let mut entries = match self.entries.write() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };

        entries.retain(|entry| &entry.table_name != table_name);
    }
}

impl<S: PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(crate) fn cached_partition_family(
        &self,
        key: &PartitionFamilyCacheKey,
    ) -> Option<ResolvedPartitionFamily> {
        let entry = self.partition_family_cache_lru.get(key)?;
        Some(entry.family.clone())
    }

    pub(crate) fn cache_partition_family(
        &self,
        key: PartitionFamilyCacheKey,
        family: ResolvedPartitionFamily,
    ) {
        let generation = self
            .partition_family_cache_generation
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let entry = Arc::new(PartitionFamilyCacheEntry::new(family, generation));
        self.partition_family_cache_lru.insert(key.clone(), entry);

        if !self.kv_store.supports_partition_families() {
            return;
        }

        let should_spawn_watch = self
            .partition_family_watch_registry
            .register_generation(key.clone(), generation);
        if !should_spawn_watch {
            return;
        }

        let kv_store = self.kv_store.clone();
        let cache_lru = self.partition_family_cache_lru.clone();
        let watch_registry = Arc::clone(&self.partition_family_watch_registry);
        let watch_key = key.watch_key();

        tokio::spawn(async move {
            match kv_store
                .wait_for_change(
                    &watch_key,
                    Duration::from_secs(PARTITION_FAMILY_CACHE_WATCH_TIMEOUT_SECONDS),
                )
                .await
            {
                Ok(true) | Err(_) => {
                    if watch_registry.remove_if_generation(&key, generation) {
                        cache_lru.remove(&key);
                    }
                }
                Ok(false) => {
                    watch_registry.remove_if_generation(&key, generation);
                }
            }
        });
    }
}
