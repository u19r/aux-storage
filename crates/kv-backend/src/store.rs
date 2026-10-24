//! One initialized KV family store for both native drivers.

use std::{future::Future, sync::Arc};

use futures::future::BoxFuture;
use storage_provider::{DynamoPort, TableMetadataCache, TableUpdateTransaction};
use storage_types::{
    AttemptMode, Clock, StorageResult, SystemClock, TimestampMillis, UpdateTableRequest,
    UpdateTableResponse,
};

use super::{
    KvBackend, KvTopology, catalog::KvCatalogAttempt, maintenance::initialize_maintenance,
    stream_history::initialize_system_stream,
};

pub struct KvStore<B> {
    pub(crate) backend: Arc<B>,
    metadata_cache: Arc<TableMetadataCache>,
    clock: Arc<dyn Clock>,
}

impl<B> Clone for KvStore<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            metadata_cache: Arc::clone(&self.metadata_cache),
            clock: Arc::clone(&self.clock),
        }
    }
}

impl<B: KvBackend> KvStore<B> {
    /// Publish a store only after every current KV format is atomically ready.
    pub async fn initialize_format(backend: B) -> StorageResult<Self> {
        storage_common::retry::retry_native_conflicts(
            &SystemClock,
            storage_provider::DYNAMO_WRITE_RETRY,
            || {
                backend.with_attempt(AttemptMode::ReadWrite, |attempt| {
                    Box::pin(async move {
                        crate::expiration::initialize_index_format(attempt).await?;
                        initialize_system_stream(attempt).await?;
                        initialize_maintenance(attempt).await?;
                        crate::infrastructure::layout::initialize_layout(attempt).await
                    })
                })
            },
        )
        .await?;
        Ok(Self {
            backend: Arc::new(backend),
            metadata_cache: Arc::new(TableMetadataCache::new()),
            clock: Arc::new(SystemClock),
        })
    }

    /// Override time for this store; normal construction uses the system clock.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
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

impl<B: KvBackend + 'static> DynamoPort for KvStore<B> {
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

    fn metadata_cache(&self) -> Arc<TableMetadataCache> {
        Arc::clone(&self.metadata_cache)
    }

    fn with_dynamo_attempt<R, F>(
        &self,
        mode: AttemptMode,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        let cache = Arc::clone(&self.metadata_cache);
        self.backend.with_attempt(mode, move |native| {
            Box::pin(async move {
                let mut attempt = KvCatalogAttempt::new(native, mode.read_protection())
                    .with_metadata_cache(cache);
                let result = operation(&mut attempt).await?;
                stream_provider::retained_image::RetainedOwnershipAttempt::finalize_ownership(
                    &mut attempt,
                )
                .await?;
                Ok(result)
            })
        })
    }

    async fn with_committed_dynamo_attempt<R, F>(
        &self,
        operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        if self.backend.topology() == KvTopology::Distributed {
            let cache = Arc::clone(&self.metadata_cache);
            self.backend
                .with_versionstamped_attempt(move |native| {
                    Box::pin(async move {
                        let mut attempt = KvCatalogAttempt::new(native, storage_types::ReadProtection::Conflict)
                            .with_metadata_cache(cache);
                        let result = operation(&mut attempt).await?;
                    stream_provider::retained_image::RetainedOwnershipAttempt::finalize_ownership(&mut attempt).await?;
                    Ok(result)
                    })
                })
                .await
        } else {
            self.with_dynamo_attempt(AttemptMode::ReadWrite, operation)
                .await
                .map(|value| (value, None))
        }
    }

    async fn update_table_once(
        &self,
        request: UpdateTableRequest,
        captured_at: TimestampMillis,
    ) -> StorageResult<UpdateTableResponse> {
        self.backend
            .with_attempt(AttemptMode::ReadWrite, move |native| {
                Box::pin(async move {
                    let mut attempt =
                        KvCatalogAttempt::new(native, storage_types::ReadProtection::Conflict);
                    let (_, response) = attempt
                        .execute_table_update(&request, captured_at)
                        .await?
                        .into_result()?;
                    stream_provider::retained_image::RetainedOwnershipAttempt::finalize_ownership(
                        &mut attempt,
                    )
                    .await?;
                    Ok(response)
                })
            })
            .await
    }
}

impl<B: KvBackend> storage_provider::native_admission::NativeAttemptSource for KvStore<B> {
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.backend.bind_work_limits(limits)
    }

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        self.backend.attempt_capacity()
    }
}
