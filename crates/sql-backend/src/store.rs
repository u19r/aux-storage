//! One initialized SQL family store for all three native drivers.

use std::{future::Future, sync::Arc};

use futures::future::BoxFuture;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort, TableMetadataCache, TableUpdateTransaction,
};
use storage_types::{
    AttemptMode, Clock, StorageResult, SystemClock, TimestampMillis, UpdateTableRequest,
    UpdateTableResponse,
};

use super::{SqlBackend, catalog::SqlCatalogAttempt};

pub struct SqlStore<B> {
    pub(crate) backend: Arc<B>,
    pub(crate) metadata_cache: Arc<TableMetadataCache>,
    clock: Arc<dyn Clock>,
}

impl<B> Clone for SqlStore<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            metadata_cache: Arc::clone(&self.metadata_cache),
            clock: Arc::clone(&self.clock),
        }
    }
}

impl<B: SqlBackend> SqlStore<B> {
    /// Publish a store only after every current SQL family format is ready.
    pub async fn initialize_schema(backend: B) -> StorageResult<Self> {
        let engine = backend.engine();
        retry_native_conflicts(&SystemClock, DYNAMO_WRITE_RETRY, || {
            backend.with_schema_attempt(move |attempt| {
                Box::pin(async move {
                    #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
                    crate::job_store::initialize(attempt, engine).await?;
                    crate::data_schema::initialize_data_schema(attempt, engine).await?;
                    crate::infrastructure::layout::initialize_layout(attempt, engine).await?;
                    Ok(())
                })
            })
        })
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

impl<B: SqlBackend> Clock for SqlStore<B> {
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

impl<B: SqlBackend + 'static> DynamoPort for SqlStore<B> {
    type PreparedWrites = crate::write_preparation::SqlPreparedWrites;
    fn prepare_put_items(
        &self,
        items: &[&storage_provider::BoundPutItem],
    ) -> StorageResult<Self::PreparedWrites> {
        Self::PreparedWrites::try_new(self.backend.engine(), items)
    }

    type Attempt<'native>
        = SqlCatalogAttempt<'native, B::Attempt>
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
        let engine = self.backend.engine();
        let cache = Arc::clone(&self.metadata_cache);
        self.backend
            .with_data_attempt(mode, crate::SqlDataAccess::Write, move |native| {
                Box::pin(async move {
                    let mut attempt =
                        SqlCatalogAttempt::new(native, engine, mode.read_protection())
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

    fn with_dynamo_schema_attempt<R, F>(
        &self,
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
        let engine = self.backend.engine();
        self.backend.with_schema_attempt(move |native| {
            Box::pin(async move {
                let mut attempt =
                    SqlCatalogAttempt::new(native, engine, storage_types::ReadProtection::Conflict);
                let result = operation(&mut attempt).await?;
                stream_provider::retained_image::RetainedOwnershipAttempt::finalize_ownership(
                    &mut attempt,
                )
                .await?;
                Ok(result)
            })
        })
    }

    async fn update_table_once(
        &self,
        request: UpdateTableRequest,
        captured_at: TimestampMillis,
    ) -> StorageResult<UpdateTableResponse> {
        let engine = self.backend.engine();
        self.backend
            .with_schema_attempt(move |native| {
                Box::pin(async move {
                    let mut attempt = SqlCatalogAttempt::new(
                        native,
                        engine,
                        storage_types::ReadProtection::Conflict,
                    );
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

impl<B: SqlBackend + 'static> storage_provider::maintenance::PartitionReconciliationSource
    for SqlStore<B>
{
    fn partition_reconciliation(
        self: Arc<Self>,
    ) -> Option<Arc<dyn storage_provider::maintenance::PartitionReconciliationRepository>> {
        None
    }
}

impl<B: SqlBackend> storage_provider::native_admission::NativeAttemptSource for SqlStore<B> {
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.backend.bind_work_limits(limits)
    }

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        self.backend.attempt_capacity()
    }
}
