//! Scheduled maintenance admits each native attempt before repository erasure.
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use futures::future::BoxFuture;
use storage_provider::{
    DynamoPort,
    native_admission::{NativeAdmission, NativeAdmissionBinding, NativeWork},
};
use storage_types::{AttemptMode, Clock, StorageResult, TimestampMillis};

use super::models::MaintenanceRepositories;

pub(crate) trait MaintenanceProviderSource: Send + Sync {
    fn prepare(
        &self,
        catalog: scheduled_bg_jobs::JobCatalog,
    ) -> StorageResult<(
        MaintenanceRepositories,
        Arc<dyn storage_provider::TableReclaimRepository>,
    )>;
    fn bind(&self, admission: Arc<dyn NativeAdmission>) -> StorageResult<()>;
}

pub(super) struct NativeMaintenanceSource<P> {
    port: Arc<P>,
    binding: Arc<NativeAdmissionBinding>,
}

#[derive(Clone)]
struct NativeMaintenancePort<P> {
    port: Arc<P>,
    binding: Arc<NativeAdmissionBinding>,
}

impl<P> NativeMaintenanceSource<P> {
    pub(super) fn new(port: Arc<P>) -> Self {
        Self {
            port,
            binding: Arc::new(NativeAdmissionBinding::default()),
        }
    }
}

impl<
    P: DynamoPort
        + queue_provider::QueuePort
        + storage_provider::maintenance::PartitionReconciliationSource,
> MaintenanceProviderSource for NativeMaintenanceSource<P>
where for<'native> <P as DynamoPort>::Attempt<'native>:
        storage_provider::maintenance::ExpirationPort
            + storage_provider::maintenance::GsiPort
            + storage_provider::maintenance::StreamRetentionPort
            + storage_provider::TableReclaimAttempt
{
    fn prepare(
        &self,
        catalog: scheduled_bg_jobs::JobCatalog,
    ) -> StorageResult<(
        MaintenanceRepositories,
        Arc<dyn storage_provider::TableReclaimRepository>,
    )> {
        let port = Arc::new(NativeMaintenancePort {
            port: Arc::clone(&self.port),
            binding: Arc::clone(&self.binding),
        });
        let partition =
            Arc::clone(&self.port).admitted_partition_reconciliation(Arc::clone(&self.binding))?;
        Ok((
            MaintenanceRepositories {
                job_catalog: catalog,
                expiration_repository: port.clone(),
                gsi_build_repository: port.clone(),
                partition_reconciliation_repository: partition,
                queue_message_cleanup_repository: Some(port.clone()),
                retention_repository: port.clone(),
            },
            port,
        ))
    }

    fn bind(&self, admission: Arc<dyn NativeAdmission>) -> StorageResult<()> {
        self.binding.bind(admission)
    }
}

impl<P: Clock> Clock for NativeMaintenancePort<P> {
    fn now(&self) -> TimestampMillis {
        self.port.now()
    }

    fn sleep(
        &self,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        self.port.sleep(duration)
    }
}

impl<P: DynamoPort> DynamoPort for NativeMaintenancePort<P> {
    type PreparedWrites = P::PreparedWrites;
    type Attempt<'native>
        = P::Attempt<'native>
    where Self: 'native;

    fn prepare_put_items(
        &self,
        items: &[&storage_provider::BoundPutItem],
    ) -> StorageResult<Self::PreparedWrites> {
        self.port.prepare_put_items(items)
    }

    fn clock(&self) -> Arc<dyn Clock> {
        self.port.clock()
    }

    fn metadata_cache(&self) -> Arc<storage_provider::TableMetadataCache> {
        self.port.metadata_cache()
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
        let port = Arc::clone(&self.port);
        self.binding
            .settle(NativeWork::Background, move || async move {
                port.with_dynamo_attempt(mode, operation).await
            })
    }

    fn with_committed_dynamo_attempt<R, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = StorageResult<(R, Option<[u8; 10]>)>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        let port = Arc::clone(&self.port);
        self.binding
            .settle(NativeWork::Background, move || async move {
                port.with_committed_dynamo_attempt(operation).await
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
        let port = Arc::clone(&self.port);
        self.binding
            .settle(NativeWork::Background, move || async move {
                port.with_dynamo_schema_attempt(operation).await
            })
    }

    fn update_table_once(
        &self,
        request: storage_types::UpdateTableRequest,
        captured_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<storage_types::UpdateTableResponse>> + Send {
        let port = Arc::clone(&self.port);
        self.binding
            .settle(NativeWork::Background, move || async move {
                port.update_table_once(request, captured_at).await
            })
    }
}

impl<P: queue_provider::QueuePort + 'static> queue_provider::QueuePort
    for NativeMaintenancePort<P>
{
    type Attempt<'native>
        = P::Attempt<'native>
    where Self: 'native;

    fn with_queue_attempt<R, F>(
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
        let port = Arc::clone(&self.port);
        self.binding
            .settle(NativeWork::Background, move || async move {
                port.with_queue_attempt(mode, operation).await
            })
    }
}
