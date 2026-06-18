use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bg_jobs::JobManager;
use lru_ttl_cache::{CacheConfig, LruTtlCache};
use storage_common::ttl::TtlConfigRecord;
use storage_types::{StorageError, StorageResult, StoredTableInfo, TableName};
use uuid::Uuid;

pub(crate) use crate::partition_family::PartitionFamilyCacheKey;
use crate::{
    constants::{
        PARTITION_FAMILY_CACHE_CAPACITY, PARTITION_FAMILY_CACHE_TTL_SECONDS,
        PARTITION_FAMILY_CACHE_WATCH_TIMEOUT_SECONDS, TABLE_CACHE_CAPACITY,
        TABLE_CACHE_TTL_SECONDS, TTL_CONFIG_CACHE_CAPACITY, TTL_CONFIG_CACHE_TTL_SECONDS,
    },
    keyspace::{
        compact::{self, TableStorageId},
        table_identity::StoredTableMetadata,
    },
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
    pub(crate) table_identity_by_name_lru: LruTtlCache<TableName, Arc<StoredTableMetadata>>,
    pub(crate) table_identity_by_id_lru: LruTtlCache<TableStorageId, Arc<StoredTableMetadata>>,
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
    pub(crate) database_job_intervals: storage_common::DatabaseJobIntervals,
    pub(crate) immediate_gsi_consistency: bool,
    pub(crate) gsi_propagation_governor: Arc<storage_common::GsiPropagationGovernor>,
}

impl<S: SortedKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub fn new(kv_store: S) -> Self {
        Self {
            kv_store,
            table_identity_by_name_lru: LruTtlCache::new(
                CacheConfig::new()
                    .with_capacity(TABLE_CACHE_CAPACITY)
                    .with_ttl(Duration::from_secs(TABLE_CACHE_TTL_SECONDS)),
            ),
            table_identity_by_id_lru: LruTtlCache::new(
                CacheConfig::new()
                    .with_capacity(TABLE_CACHE_CAPACITY)
                    .with_ttl(Duration::from_secs(TABLE_CACHE_TTL_SECONDS)),
            ),
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
            database_job_intervals: storage_common::DatabaseJobIntervals::default(),
            immediate_gsi_consistency: false,
            gsi_propagation_governor: Arc::new(storage_common::GsiPropagationGovernor::default()),
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
    pub fn with_database_job_intervals(
        mut self,
        intervals: storage_common::DatabaseJobIntervals,
    ) -> Self {
        self.database_job_intervals = intervals;
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
        if let Some(metadata) = self.get_table_identity_from_name(table_name).await? {
            return Ok(Some(Arc::new(metadata.table_info.clone())));
        }

        Ok(None)
    }

    pub(crate) async fn get_table_identity_from_name(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<Arc<StoredTableMetadata>>> {
        if let Some(metadata) = self.table_identity_by_name_lru.get(table_name) {
            let lookup_key = compact::table_name_lookup_key(table_name.as_ref().as_bytes());
            let cached_table_id = metadata.identity.table_id;
            let cache_is_durable = match self.kv_store.get(&lookup_key, true).await? {
                Some(table_id_bytes)
                    if decode_table_storage_id(&table_id_bytes)? == cached_table_id =>
                {
                    let metadata_key = compact::table_metadata_key(cached_table_id);
                    match self.kv_store.get(&metadata_key, true).await? {
                        Some(data) => {
                            let stored: StoredTableMetadata =
                                storage_types::storage_serde::from_bytes(&data)?;
                            !stored.identity.deleted && stored.identity.table_name == *table_name
                        }
                        None => false,
                    }
                }
                _ => false,
            };
            if cache_is_durable {
                record_table_identity_cache("name_metadata", "hit");
                return Ok(Some(metadata));
            }
            record_table_identity_cache("name_metadata", "stale");
            self.invalidate_table_metadata_cache(table_name);
            self.table_identity_by_id_lru.remove(&cached_table_id);
        }
        record_table_identity_cache("name_metadata", "miss");

        let lookup_key = compact::table_name_lookup_key(table_name.as_ref().as_bytes());
        let Some(table_id_bytes) = self.kv_store.get(&lookup_key, true).await? else {
            record_table_identity_cache("name_lookup", "miss");
            return Ok(None);
        };
        record_table_identity_cache("name_lookup", "hit");
        let table_id = decode_table_storage_id(&table_id_bytes)?;
        let Some(metadata) = self.get_table_identity_from_id(table_id).await? else {
            return Err(StorageError::internal(&format!(
                "table name lookup points to missing metadata: table={table_name}, id={}",
                table_id.get()
            )));
        };
        if metadata.identity.deleted || metadata.identity.table_name != *table_name {
            return Err(StorageError::internal(&format!(
                "table name lookup points to stale metadata: table={table_name}, id={}",
                table_id.get()
            )));
        }
        self.cache_table_identity(Arc::clone(&metadata));
        Ok(Some(metadata))
    }

    pub(crate) async fn get_table_identity_from_id(
        &self,
        table_id: TableStorageId,
    ) -> StorageResult<Option<Arc<StoredTableMetadata>>> {
        if let Some(metadata) = self.table_identity_by_id_lru.get(&table_id) {
            record_table_identity_cache("id_metadata", "hit");
            return Ok(Some(metadata));
        }
        record_table_identity_cache("id_metadata", "miss");

        let key = compact::table_metadata_key(table_id);
        match self.kv_store.get(&key, true).await? {
            Some(data) => {
                let metadata: StoredTableMetadata =
                    storage_types::storage_serde::from_bytes(&data)?;
                let metadata = Arc::new(metadata);
                self.cache_table_identity(Arc::clone(&metadata));
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn cache_table_identity(&self, metadata: Arc<StoredTableMetadata>) {
        if metadata.identity.deleted {
            self.invalidate_table_metadata_cache(&metadata.identity.table_name);
            self.table_identity_by_id_lru
                .insert(metadata.identity.table_id, metadata);
            return;
        }
        self.table_identity_by_name_lru
            .insert(metadata.identity.table_name.clone(), Arc::clone(&metadata));
        self.table_identity_by_id_lru
            .insert(metadata.identity.table_id, Arc::clone(&metadata));
    }

    pub(crate) fn invalidate_table_metadata_cache(&self, table_name: &TableName) {
        self.table_identity_by_name_lru.remove(table_name);
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

pub(crate) fn encode_table_storage_id(table_id: TableStorageId) -> Vec<u8> {
    table_id.get().to_be_bytes().to_vec()
}

pub(crate) fn decode_table_storage_id(bytes: &[u8]) -> StorageResult<TableStorageId> {
    if bytes.len() != 4 {
        return Err(StorageError::internal(&format!(
            "invalid table storage id width: expected 4 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(TableStorageId::new(u32::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3],
    ])))
}

fn record_table_identity_cache(cache: &'static str, outcome: &'static str) {
    metrics::counter!(
        "storage.table_identity.cache.total",
        "cache" => cache,
        "outcome" => outcome,
    )
    .increment(1);
}
