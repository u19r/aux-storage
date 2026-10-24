use std::{
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Instant,
};

use futures::{StreamExt, TryStreamExt, stream};
use lru_ttl_cache::{
    RefreshAdmission, RefreshQueue, RefreshQueueLimits, RefreshTicket, RefreshWork, SharedLoad,
    SharedLoadError,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use super::{
    PointReadClient, PointReadEntry, PointReadRequest, PointReadStore,
    constants::{
        POINT_READ_BATCH_KEYS, POINT_READ_PLANNED_BYTES, POINT_READ_RETAINED_BYTES,
        POINT_READ_SOURCE_KEYS, POINT_READ_SOURCE_SUBSCRIBERS, POINT_READ_SOURCE_TIMEOUT,
    },
    generation::SourceGenerations,
    source::{SourceAdmission, SourceKey},
    worker::SourceWorkers,
};
use crate::types::{StorageError, StorageResult, VecMap};

/// One physical source namespace: this database client, strong reads, full
/// items. Table and native canonical key form the per-source identity.
pub struct PointReadSource {
    db: Arc<dyn PointReadClient>,
    queue: RefreshQueue<SourceKey>,
    loads: SharedLoad<super::generation::ReadValidity, PointReadEntry, StorageError>,
    refresh_executor: Mutex<Weak<lru_ttl_cache::RefreshExecutor>>,
    responses: Arc<Semaphore>,
    plans: Arc<Semaphore>,
    identities: SourceGenerations,
    workers: OnceLock<SourceWorkers>,
}

impl PointReadSource {
    pub fn new(db: Arc<dyn PointReadClient>) -> Self {
        Self {
            db,
            queue: RefreshQueue::new(RefreshQueueLimits::default()),
            loads: SharedLoad::new(POINT_READ_SOURCE_KEYS, POINT_READ_SOURCE_SUBSCRIBERS),
            responses: Arc::new(Semaphore::new(POINT_READ_RETAINED_BYTES)),
            plans: Arc::new(Semaphore::new(POINT_READ_PLANNED_BYTES)),
            identities: SourceGenerations::default(),
            workers: OnceLock::new(),
            refresh_executor: Mutex::new(Weak::new()),
        }
    }

    pub async fn fetch_store(&self, plan: super::PointReadPlan) -> StorageResult<PointReadStore> {
        let observer = plan.observer;
        let (requests, _) = plan.into_parts();
        let requests = requests.as_slice();
        if requests.is_empty() {
            return Ok(PointReadStore::default());
        }
        let plan_budget = self.admit_plan(requests)?;
        self.workers.get_or_init(|| {
            SourceWorkers::start(
                Arc::clone(&self.db),
                self.queue.clone(),
                Arc::clone(&self.responses),
            )
        });
        let entries: Vec<_> = stream::iter(requests.iter().cloned())
            .map(|request| self.fetch_shared(request, observer))
            .buffer_unordered(POINT_READ_BATCH_KEYS)
            .try_collect()
            .await?;
        Ok(PointReadStore::from_entries(
            VecMap::from_iter(entries),
            plan_budget,
        ))
    }

    async fn fetch_shared(
        &self,
        request: Arc<PointReadRequest>,
        observer: super::observer::PointReadObserver,
    ) -> StorageResult<(String, PointReadEntry)> {
        let deadline = Instant::now() + POINT_READ_SOURCE_TIMEOUT;
        let validity = self.identities.capture(Arc::clone(&request), deadline)?;
        let result = self
            .loads
            .fetch(&validity, || async {
                let read = self.identities.start_read(validity.clone())?;
                let (sender, receiver) = oneshot::channel();
                let key = SourceKey::new(Arc::clone(&request), sender, read, observer);
                self.fetch_source(key, receiver, deadline).await
            })
            .await;
        if validity.is_obsolete() {
            return Err(StorageError::service_unavailable(1));
        }
        result
            .map(|entry| (request.fingerprint.clone(), entry))
            .map_err(|error| match error {
                SharedLoadError::Source(error) => error.as_ref().clone(),
                SharedLoadError::Full | SharedLoadError::Cancelled => {
                    StorageError::service_unavailable(1)
                }
            })
    }

    fn admit_plan(
        &self,
        requests: &[Arc<PointReadRequest>],
    ) -> StorageResult<OwnedSemaphorePermit> {
        let bytes = requests
            .iter()
            .try_fold(0usize, |total, request| {
                total.checked_add(request.charge.get())
            })
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or_else(|| StorageError::service_unavailable(1))?;
        Arc::clone(&self.plans)
            .try_acquire_many_owned(bytes)
            .map_err(|_| StorageError::service_unavailable(1))
    }

    pub fn stats(
        &self,
    ) -> (
        lru_ttl_cache::SharedLoadStats,
        lru_ttl_cache::RefreshQueueStats,
    ) {
        (self.loads.stats(), self.queue.stats())
    }

    /// Related decoded caches share this bounded execution population.
    pub fn refresh_executor(&self) -> Arc<lru_ttl_cache::RefreshExecutor> {
        let mut current = self
            .refresh_executor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(executor) = current.upgrade() {
            return executor;
        }
        let executor = Arc::new(lru_ttl_cache::RefreshExecutor::new());
        *current = Arc::downgrade(&executor);
        executor
    }

    /// Bind cache transitions to this physical source namespace.
    /// The mapper runs under the cache mutex and must be pure, synchronous,
    /// nonblocking, nonreentrant and panic-free. Invalid mappings fence all
    /// active source identities rather than silently retaining an old cohort.
    pub fn bind_cache_config<K: 'static, V, E>(
        self: &Arc<Self>,
        config: lru_ttl_cache::FetchingCacheConfig<K, V, E>,
        mapper: impl Fn(&K) -> StorageResult<(crate::types::TableName, crate::types::KeyAttributes)>
        + Send
        + Sync
        + 'static,
    ) -> lru_ttl_cache::FetchingCacheConfig<K, V, E> {
        let source = Arc::clone(self);
        config.with_invalidation(move |key| {
            let identity =
                mapper(key).and_then(|(table, key)| super::key_fingerprint(&table, &key));
            source.identities.invalidate(identity.as_deref().ok());
        })
    }

    pub async fn fetch_decoded<T: crate::types::TryFromWireItem>(
        &self,
        table: crate::types::TableName,
        key: impl Into<crate::types::KeyAttributes>,
    ) -> StorageResult<Option<T>> {
        let key = key.into();
        let mut plan = super::PointReadPlan::default();
        plan.add_item(table.clone(), key.clone())?;
        self.fetch_store(plan).await?.get_decoded(&table, &key)
    }

    async fn fetch_source(
        &self,
        key: SourceKey,
        receiver: oneshot::Receiver<StorageResult<PointReadEntry>>,
        deadline: Instant,
    ) -> StorageResult<PointReadEntry> {
        if key.is_obsolete() {
            return Err(StorageError::service_unavailable(1));
        }
        let work = RefreshWork::new(deadline, key.request.charge, key.demand());
        if self
            .queue
            .try_schedule(RefreshTicket::new(key.clone(), key.read.id(), work))
            != RefreshAdmission::Queued
        {
            return Err(StorageError::service_unavailable(1));
        }
        let admission = SourceAdmission {
            key,
            queue: self.queue.clone(),
        };
        tokio::time::timeout_at(deadline.into(), admission.receive_result(receiver))
            .await
            .map_err(|_| StorageError::service_unavailable(1))?
            .map_err(|_| StorageError::service_unavailable(1))?
    }
}

impl Drop for PointReadSource {
    fn drop(&mut self) {
        self.queue.close();
    }
}
