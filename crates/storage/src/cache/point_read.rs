use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use storage_types::{BatchGetItemRequest, StorageResult, WireItem};

use crate::point_read_cache_store::{
    InMemoryPointReadCacheState, PointReadCacheKey, PointReadCacheValue,
};
pub use crate::point_read_cache_types::{
    AuthoritativePointReadHit, AuthoritativePointReadPurpose, AuthoritativePointReadResult,
    DurableAbsenceProof, DurableItemRevision, InMemoryPointReadCacheConfig,
    PointReadBatchGetResult, PointReadCacheEvictionPolicy, PointReadGetRequest, PointReadGetResult,
};

#[async_trait]
pub trait PointReadCache: Send + Sync {
    fn is_enabled(&self) -> bool {
        false
    }

    /// Claim a monotonic write version before performing a DB write. The
    /// version must be passed to `write_put` / `write_delete` after the DB
    /// write completes. This prevents stale values from overwriting newer
    /// entries when two writes to the same key race.
    fn claim_write_version(&self) -> u64 {
        0
    }

    /// Mark a key as having an in-flight write. Reads for this key will
    /// return `Miss` until `complete_write` is called, preventing stale
    /// reads during the window between DB write start and cache update.
    async fn prepare_write(&self, _request: &PointReadGetRequest) -> StorageResult<()> {
        Ok(())
    }

    /// Clear the in-flight write mark for a key. Must be called after the
    /// cache has been updated (or on error) to resume serving from cache.
    async fn complete_write(&self, _request: &PointReadGetRequest) -> StorageResult<()> {
        Ok(())
    }

    /// Signal that cache continuity has been lost (e.g. replication gap,
    /// connection failure). All reads will return `Miss` until `rebuild`
    /// is called.
    fn mark_continuity_broken(&self) {}

    /// Clear all entries, bump the epoch, and restore cache to a serving
    /// state after continuity was broken.
    fn rebuild(&self) {}

    /// Return the current cache epoch. The epoch increments on each
    /// `rebuild`, allowing callers to detect stale references.
    fn epoch(&self) -> u64 {
        0
    }

    async fn get_eventual(
        &self,
        request: &PointReadGetRequest,
    ) -> StorageResult<PointReadGetResult>;

    async fn get_authoritative(
        &self,
        _request: &PointReadGetRequest,
        _purpose: AuthoritativePointReadPurpose,
    ) -> StorageResult<AuthoritativePointReadResult> {
        Ok(AuthoritativePointReadResult::Miss)
    }

    async fn batch_get_eventual(
        &self,
        request: &BatchGetItemRequest,
    ) -> StorageResult<PointReadBatchGetResult>;

    async fn batch_get_authoritative(
        &self,
        request: &BatchGetItemRequest,
        _purpose: AuthoritativePointReadPurpose,
    ) -> StorageResult<PointReadBatchGetResult> {
        Ok(PointReadBatchGetResult {
            responses: HashMap::new(),
            unresolved_request_items: request.request_items.clone(),
        })
    }

    async fn write_put(
        &self,
        request: &PointReadGetRequest,
        item: &WireItem,
        write_version: u64,
    ) -> StorageResult<()>;

    async fn write_put_with_revision(
        &self,
        request: &PointReadGetRequest,
        item: &WireItem,
        revision: DurableItemRevision,
        write_version: u64,
    ) -> StorageResult<()> {
        let _ = revision;
        self.write_put(request, item, write_version).await
    }

    async fn write_delete(
        &self,
        request: &PointReadGetRequest,
        write_version: u64,
    ) -> StorageResult<()>;

    async fn write_delete_with_absence_proof(
        &self,
        request: &PointReadGetRequest,
        proof: DurableAbsenceProof,
        write_version: u64,
    ) -> StorageResult<()> {
        let _ = proof;
        self.write_delete(request, write_version).await
    }

    async fn invalidate(&self, request: &PointReadGetRequest) -> StorageResult<()>;
}

#[derive(Debug, Default)]
pub struct NoopPointReadCache;

#[async_trait]
impl PointReadCache for NoopPointReadCache {
    async fn get_eventual(
        &self,
        _request: &PointReadGetRequest,
    ) -> StorageResult<PointReadGetResult> {
        Ok(PointReadGetResult::Miss)
    }

    async fn get_authoritative(
        &self,
        _request: &PointReadGetRequest,
        _purpose: AuthoritativePointReadPurpose,
    ) -> StorageResult<AuthoritativePointReadResult> {
        Ok(AuthoritativePointReadResult::Miss)
    }

    async fn batch_get_eventual(
        &self,
        request: &BatchGetItemRequest,
    ) -> StorageResult<PointReadBatchGetResult> {
        Ok(PointReadBatchGetResult {
            responses: HashMap::new(),
            unresolved_request_items: request.request_items.clone(),
        })
    }

    async fn batch_get_authoritative(
        &self,
        request: &BatchGetItemRequest,
        _purpose: AuthoritativePointReadPurpose,
    ) -> StorageResult<PointReadBatchGetResult> {
        Ok(PointReadBatchGetResult {
            responses: HashMap::new(),
            unresolved_request_items: request.request_items.clone(),
        })
    }

    async fn write_put(
        &self,
        _request: &PointReadGetRequest,
        _item: &WireItem,
        _write_version: u64,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn write_delete(
        &self,
        _request: &PointReadGetRequest,
        _write_version: u64,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn invalidate(&self, _request: &PointReadGetRequest) -> StorageResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct InMemoryPointReadCache {
    state: Arc<Mutex<InMemoryPointReadCacheState>>,
    write_version_counter: Arc<AtomicU64>,
}

impl InMemoryPointReadCache {
    #[must_use]
    pub fn new(config: InMemoryPointReadCacheConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryPointReadCacheState::new(config))),
            write_version_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    fn cache_key(request: &PointReadGetRequest) -> StorageResult<PointReadCacheKey> {
        PointReadCacheKey::new(&request.table_name, &request.key)
    }

    fn lock_state(&self) -> MutexGuard<'_, InMemoryPointReadCacheState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("point-read cache mutex poisoned, recovering inner state");
                poisoned.into_inner()
            }
        }
    }
}

#[async_trait]
impl PointReadCache for InMemoryPointReadCache {
    fn is_enabled(&self) -> bool {
        self.lock_state().is_enabled()
    }

    fn claim_write_version(&self) -> u64 {
        self.write_version_counter.fetch_add(1, Ordering::Relaxed)
    }

    async fn prepare_write(&self, request: &PointReadGetRequest) -> StorageResult<()> {
        let cache_key = Self::cache_key(request)?;
        let mut state = self.lock_state();
        state.prepare_write(cache_key);
        Ok(())
    }

    async fn complete_write(&self, request: &PointReadGetRequest) -> StorageResult<()> {
        let cache_key = Self::cache_key(request)?;
        let mut state = self.lock_state();
        state.complete_write(&cache_key);
        Ok(())
    }

    fn mark_continuity_broken(&self) {
        let mut state = self.lock_state();
        state.mark_continuity_broken();
    }

    fn rebuild(&self) {
        let mut state = self.lock_state();
        state.rebuild();
    }

    fn epoch(&self) -> u64 {
        let state = self.lock_state();
        state.epoch()
    }

    async fn get_eventual(
        &self,
        request: &PointReadGetRequest,
    ) -> StorageResult<PointReadGetResult> {
        let cache_key = Self::cache_key(request)?;
        let mut state = self.lock_state();
        let Some(value) = state.get(&cache_key, Instant::now()) else {
            return Ok(PointReadGetResult::Miss);
        };
        Ok(PointReadGetResult::Hit(Box::new(value.into_wire_item())))
    }

    async fn get_authoritative(
        &self,
        request: &PointReadGetRequest,
        _purpose: AuthoritativePointReadPurpose,
    ) -> StorageResult<AuthoritativePointReadResult> {
        let cache_key = Self::cache_key(request)?;
        let mut state = self.lock_state();
        let Some(value) = state.get(&cache_key, Instant::now()) else {
            return Ok(AuthoritativePointReadResult::Miss);
        };
        Ok(AuthoritativePointReadResult::Hit(Box::new(
            value.into_authoritative_hit(),
        )))
    }

    async fn batch_get_eventual(
        &self,
        request: &BatchGetItemRequest,
    ) -> StorageResult<PointReadBatchGetResult> {
        let mut responses = HashMap::new();
        let mut unresolved_request_items = HashMap::new();
        let now = Instant::now();
        let mut state = self.lock_state();

        for (table_name, keys_and_attributes) in &request.request_items {
            let mut cached_items = Vec::new();
            let mut unresolved_keys = Vec::new();

            for key in &keys_and_attributes.keys {
                let cache_key = PointReadCacheKey::new(table_name, key)?;
                match state.get(&cache_key, now) {
                    Some(value) => {
                        if let Some(item) = value.into_wire_item() {
                            cached_items.push(item);
                        }
                    }
                    None => unresolved_keys.push(key.clone()),
                }
            }

            if !cached_items.is_empty() {
                responses.insert(table_name.clone(), cached_items);
            }
            if !unresolved_keys.is_empty() {
                let mut unresolved = keys_and_attributes.clone();
                unresolved.keys = unresolved_keys.into();
                unresolved_request_items.insert(table_name.clone(), unresolved);
            }
        }

        Ok(PointReadBatchGetResult {
            responses,
            unresolved_request_items,
        })
    }

    async fn batch_get_authoritative(
        &self,
        request: &BatchGetItemRequest,
        _purpose: AuthoritativePointReadPurpose,
    ) -> StorageResult<PointReadBatchGetResult> {
        self.batch_get_eventual(request).await
    }

    async fn write_put(
        &self,
        request: &PointReadGetRequest,
        item: &WireItem,
        write_version: u64,
    ) -> StorageResult<()> {
        self.write_put_value(request, item, None, write_version)
            .await
    }

    async fn write_put_with_revision(
        &self,
        request: &PointReadGetRequest,
        item: &WireItem,
        revision: DurableItemRevision,
        write_version: u64,
    ) -> StorageResult<()> {
        self.write_put_value(request, item, Some(revision), write_version)
            .await
    }

    async fn write_delete(
        &self,
        request: &PointReadGetRequest,
        write_version: u64,
    ) -> StorageResult<()> {
        self.write_delete_value(request, None, write_version).await
    }

    async fn write_delete_with_absence_proof(
        &self,
        request: &PointReadGetRequest,
        proof: DurableAbsenceProof,
        write_version: u64,
    ) -> StorageResult<()> {
        self.write_delete_value(request, Some(proof), write_version)
            .await
    }

    async fn invalidate(&self, request: &PointReadGetRequest) -> StorageResult<()> {
        let cache_key = Self::cache_key(request)?;
        let mut state = self.lock_state();
        state.remove(&cache_key);
        Ok(())
    }
}

impl InMemoryPointReadCache {
    async fn write_put_value(
        &self,
        request: &PointReadGetRequest,
        item: &WireItem,
        revision: Option<DurableItemRevision>,
        write_version: u64,
    ) -> StorageResult<()> {
        let cache_key = Self::cache_key(request)?;
        let mut state = self.lock_state();
        state.insert(
            cache_key,
            PointReadCacheValue::Present {
                item: Box::new(item.clone()),
                revision,
            },
            Instant::now(),
            write_version,
        );
        Ok(())
    }

    async fn write_delete_value(
        &self,
        request: &PointReadGetRequest,
        proof: Option<DurableAbsenceProof>,
        write_version: u64,
    ) -> StorageResult<()> {
        let cache_key = Self::cache_key(request)?;
        let mut state = self.lock_state();
        state.insert(
            cache_key,
            PointReadCacheValue::Absent { proof },
            Instant::now(),
            write_version,
        );
        Ok(())
    }
}

#[must_use]
pub fn noop_point_read_cache() -> Arc<dyn PointReadCache> {
    Arc::new(NoopPointReadCache)
}
