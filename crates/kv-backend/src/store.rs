//! One initialized KV family store for both native drivers.

use std::{future::Future, sync::Arc, time::Duration};

use futures::future::BoxFuture;
use storage_provider::{DynamoPort, TableMetadataCache, TableUpdateTransaction};
use storage_types::{
    AttemptMode, AttemptPlan, AttemptRequest, Clock, DeadlineClass, StorageResult, SystemClock,
    TimestampMillis, UpdateTableRequest, UpdateTableResponse,
};

use super::{
    KvAttempt, KvBackend, KvTopology, catalog::KvCatalogAttempt,
    maintenance::initialize_maintenance, stream_history::initialize_system_stream,
};

/// Format initialisation rounds after the first that an unknown commit
/// outcome may start. Each round is one complete operation with its own
/// deadline and conflict retries.
pub(crate) const FORMAT_UNKNOWN_OUTCOME_RETRIES: u32 = 3;

/// Wait before a round that follows an unknown outcome: FoundationDB 1021
/// usually reports a commit proxy or resolver recovery in progress.
pub(crate) const FORMAT_UNKNOWN_OUTCOME_DELAY: Duration = Duration::from_millis(100);

pub struct KvStore<B> {
    pub(crate) backend: Arc<B>,
    pub(crate) metadata_cache: Arc<TableMetadataCache>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) queue_wake: Option<Arc<queue_provider::QueueWakeBoard>>,
    pub(crate) queue_catalog_hints: Arc<queue_provider::QueueCatalogHints>,
    pub(crate) admission: storage_common::native_admission::AttemptAdmission,
}

impl<B> Clone for KvStore<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            metadata_cache: Arc::clone(&self.metadata_cache),
            clock: Arc::clone(&self.clock),
            queue_wake: self.queue_wake.clone(),
            queue_catalog_hints: Arc::clone(&self.queue_catalog_hints),
            admission: self.admission.clone(),
        }
    }
}

impl<B> storage_common::native_admission::AdmittedStore for KvStore<B> {
    fn admitted(&self, admission: storage_common::native_admission::AttemptAdmission) -> Self {
        Self {
            admission,
            ..self.clone()
        }
    }
}

impl<B: KvBackend> KvStore<B> {
    /// Publish a store only after every current KV format is atomically ready.
    pub async fn initialize_format(
        backend: B,
        initial_stream_lineages: storage_types::InitialStreamLineages,
    ) -> StorageResult<Self> {
        Self::initialize_format_with_clock(backend, initial_stream_lineages, Arc::new(SystemClock))
            .await
    }

    /// Initialization and published operations share the selected time owner.
    /// Native simulation supplies its executor-compatible retry clock here.
    pub async fn initialize_format_with_clock(
        backend: B,
        initial_stream_lineages: storage_types::InitialStreamLineages,
        clock: Arc<dyn Clock>,
    ) -> StorageResult<Self> {
        initialize_format_rounds(&backend, initial_stream_lineages, clock.as_ref()).await?;
        let queue_wake = match backend.topology() {
            KvTopology::Local => Some(queue_provider::QueueWakeBoard::local()),
            KvTopology::Distributed => backend.queue_wake_board(),
        };
        Ok(Self {
            backend: Arc::new(backend),
            metadata_cache: Arc::new(TableMetadataCache::new()),
            clock,
            queue_wake,
            queue_catalog_hints: Arc::default(),
            admission: storage_common::native_admission::AttemptAdmission::Caller,
        })
    }

    /// Replace the empty table metadata cache with one whose entries live for
    /// `ttl`; see `TableMetadataCache::with_ttl`.
    #[must_use]
    pub fn with_metadata_cache_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.metadata_cache = Arc::new(TableMetadataCache::with_ttl(ttl));
        self
    }

    /// Override time for this store; normal construction uses the system clock.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
}

/// Format initialisation re-runs after an unknown commit outcome, unlike a
/// customer operation. No caller receives the unknown round's answer: the
/// store is published only after a round succeeds. Every write of a round is
/// decided by a conflict-protected read of the same key in the same
/// transaction, so a later round reads what an unknown round committed and
/// validates it, and two rounds exclude each other like two nodes that start
/// at once. A different stored format is still an error.
async fn initialize_format_rounds<B: KvBackend>(
    backend: &B,
    initial_stream_lineages: storage_types::InitialStreamLineages,
    clock: &dyn Clock,
) -> StorageResult<()> {
    let mut unknown_retries = 0_u32;
    loop {
        match initialize_format_round(backend, initial_stream_lineages, clock).await {
            Err(error)
                if error.is_commit_unknown()
                    && unknown_retries < FORMAT_UNKNOWN_OUTCOME_RETRIES =>
            {
                unknown_retries += 1;
                storage_common::telemetry::record_provider_count(
                    "native",
                    "format_unknown_retries",
                    1,
                );
                clock.sleep(FORMAT_UNKNOWN_OUTCOME_DELAY).await?;
            }
            result => return result,
        }
    }
}

/// One format initialisation operation: a fresh deadline and conflict
/// retries; every attempt reads each format key before it decides.
async fn initialize_format_round<B: KvBackend>(
    backend: &B,
    initial_stream_lineages: storage_types::InitialStreamLineages,
    clock: &dyn Clock,
) -> StorageResult<()> {
    storage_common::retry::retry_native_conflicts(
        clock,
        storage_provider::DYNAMO_WRITE_RETRY,
        backend.attempt_deadlines(),
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::SchemaChange),
        |attempt_request| {
            backend.with_attempt(attempt_request, move |attempt| {
                Box::pin(async move {
                    crate::expiration::initialize_index_format(attempt).await?;
                    initialize_system_stream(attempt, initial_stream_lineages).await?;
                    initialize_maintenance(attempt).await?;
                    crate::infrastructure::layout::initialize_layout(attempt).await
                })
            })
        },
    )
    .await
    .map(|_layout_identity| ())
}

// Both public protocol adapters share native view construction and acknowledged
// invalidation.
impl<B: KvBackend + 'static> KvStore<B> {
    /// One Dynamo or stream attempt in this store's admission lane. A committed
    /// catalog change invalidates the route cache before the permit is
    /// released.
    pub(crate) async fn with_catalog_attempt<R, F>(
        &self,
        request: AttemptRequest,
        operation: F,
    ) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut KvCatalogAttempt<'native, B::Attempt>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        let backend = Arc::clone(&self.backend);
        let metadata_cache = Arc::clone(&self.metadata_cache);
        let attempt = async move {
            let (result, changed) = backend
                .with_attempt(request, move |native| {
                    Box::pin(async move {
                        let result = operation(&mut KvCatalogAttempt::new(
                            native,
                            request.mode.read_protection(),
                        ))
                        .await?;
                        Ok((result, native.catalog_changed()))
                    })
                })
                .await?;
            if changed {
                metadata_cache.invalidate_routes();
            }
            Ok(result)
        };
        self.admission.settle(request, attempt).await
    }

    pub(crate) async fn with_versionstamped_catalog_attempt<R, F>(
        &self,
        request: AttemptRequest,
        operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut KvCatalogAttempt<'native, B::Attempt>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        if self.backend.topology() != KvTopology::Distributed {
            return self
                .with_catalog_attempt(request, operation)
                .await
                .map(|result| (result, None));
        }
        let backend = Arc::clone(&self.backend);
        let metadata_cache = Arc::clone(&self.metadata_cache);
        let attempt = async move {
            let ((result, changed), stamp) = backend
                .with_versionstamped_attempt(request, move |native| {
                    Box::pin(async move {
                        let result = operation(&mut KvCatalogAttempt::new(
                            native,
                            storage_types::ReadProtection::Conflict,
                        ))
                        .await?;
                        Ok((result, native.catalog_changed()))
                    })
                })
                .await?;
            if changed {
                metadata_cache.invalidate_routes();
            }
            Ok((result, stamp))
        };
        self.admission.settle(request, attempt).await
    }
}

impl<B: KvBackend> Clock for KvStore<B> {
    fn now(&self) -> TimestampMillis {
        self.clock.now()
    }

    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        self.clock.sleep(duration)
    }
}

impl<B: KvBackend + 'static> storage_types::AttemptDeadlineSource for KvStore<B> {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.backend.attempt_deadlines()
    }
}

impl<B: KvBackend + 'static> DynamoPort for KvStore<B> {
    fn ordinary_write_read_version_policy(&self) -> storage_types::ReadVersionPolicy {
        self.backend.ordinary_write_read_version_policy()
    }

    type PreparedWrites = crate::write_preparation::KvPreparedWrites;
    fn prepare_put_items(
        &self,
        items: &[&storage_provider::BoundPutItem],
    ) -> StorageResult<Self::PreparedWrites> {
        Self::PreparedWrites::try_new(items)
    }

    type Attempt<'native>
        = KvCatalogAttempt<'native, B::Attempt>
    where Self: 'native;

    fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }

    fn native_transaction_byte_limit(
        &self,
    ) -> infrastructure_provider::physical_store::NativeTransactionByteLimit {
        self.backend.native_layout().transaction_bytes
    }

    fn metadata_cache(&self) -> Arc<TableMetadataCache> {
        Arc::clone(&self.metadata_cache)
    }

    fn with_attempt<R, F>(
        &self,
        request: AttemptRequest,
        step: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        self.with_catalog_attempt(request, step)
    }

    async fn update_table_once(
        &self,
        attempt_request: AttemptRequest,
        request: UpdateTableRequest,
        captured_at: TimestampMillis,
        bindings: storage_provider::RequestTableBindings,
    ) -> StorageResult<UpdateTableResponse> {
        self.with_catalog_attempt(attempt_request, move |attempt| {
            Box::pin(async move {
                let (_, response) = attempt
                    .execute_table_update(&request, captured_at, &bindings)
                    .await?
                    .into_result()?;
                Ok(response)
            })
        })
        .await
    }
}

impl<B: KvBackend> storage_common::native_admission::NativeAttemptSource for KvStore<B> {
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.backend.bind_work_limits(limits)
    }

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        self.backend.attempt_capacity()
    }

    fn durability_health(&self) -> Option<storage_common::durability::DurabilityHealth> {
        self.backend.durability_health()
    }
}
