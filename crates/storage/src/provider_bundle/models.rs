use std::sync::Arc;
#[cfg(feature = "remote")]
use std::time::Duration;

use storage_provider::{
    DynamoOperations, StreamRetentionRepository, SystemStreamTrimRepository,
    TableReclaimRepository,
    maintenance::{
        ExpirationRepository, GsiBuildRepository, GsiLifecycleRepository,
        PartitionReconciliationRepository, PartitionReconciliationSource,
    },
};
use stream_provider::StreamOperations;

use crate::newtypes::DatabaseTrait;

#[derive(Clone)]
pub(crate) struct MaintenanceRepositories {
    pub job_catalog: scheduled_bg_jobs::JobCatalog,
    pub expiration_repository: Arc<dyn ExpirationRepository>,
    pub gsi_build_repository: Arc<dyn GsiBuildRepository>,
    pub partition_reconciliation_repository: Option<Arc<dyn PartitionReconciliationRepository>>,
    pub queue_message_cleanup_repository:
        Option<Arc<dyn queue_provider::QueueMessageCleanupRepository>>,
    pub retention_repository: Arc<dyn StreamRetentionRepository + Send + Sync>,
}

pub struct StorageProviderBundle {
    pub(crate) native_attempt_capacity: Option<storage_types::NativeAttemptCapacity>,
    #[cfg(feature = "regional")]
    pub(crate) regional_services: Option<regional_replication::runtime::PreparedRegionalServices>,
    pub(crate) database: Option<Arc<dyn DatabaseTrait>>,
    pub(crate) dynamo: Arc<dyn DynamoOperations>,
    pub(crate) stream: Option<Arc<dyn StreamOperations>>,

    pub(crate) ttl_sweep_repository: Option<Arc<dyn storage_provider::TtlSweepRepository>>,
    pub(crate) system_stream_trim_repository: Option<Arc<dyn SystemStreamTrimRepository>>,
    pub(crate) table_reclaim_repository: Option<Arc<dyn TableReclaimRepository>>,
    pub(crate) gsi_lifecycle_repository: Arc<dyn GsiLifecycleRepository>,
    pub(crate) queue: Option<Arc<dyn queue_provider::QueueOperations>>,
    pub(crate) pubsub: Option<Arc<dyn pubsub_provider::PubsubProvider>>,
    pub(crate) delivery_source: Option<Arc<dyn super::delivery::DeliveryProviderSource>>,
    pub(crate) maintenance: Option<MaintenanceRepositories>,
    pub(crate) maintenance_source: Option<Arc<dyn super::maintenance::MaintenanceProviderSource>>,
    pub(crate) scheduled_job_store: bg_job_store::ScheduledJobStore,
}

impl StorageProviderBundle {
    pub fn try_from_initialized_store<S>(
        store: Arc<S>,
        config: bg_job_store::ScheduledJobStoreConfig,
    ) -> storage_types::StorageResult<Self>
    where
        S: storage_provider::native_admission::NativeAttemptSource
            + storage_provider::DynamoPort
            + bg_job_store::ScheduledJobStoreSource
            + StreamOperations
            + queue_provider::QueueOperations
            + queue_provider::QueueMessageCleanupRepository
            + pubsub_provider::PubsubProvider
            + pubsub_provider::PubsubPort
            + queue_provider::QueuePort
            + ExpirationRepository
            + GsiBuildRepository
            + PartitionReconciliationSource
            + GsiLifecycleRepository
            + StreamRetentionRepository
            + SystemStreamTrimRepository
            + TableReclaimRepository
            + storage_provider::TtlSweepRepository
            + 'static,
        for<'native> <S as storage_provider::DynamoPort>::Attempt<'native>:
            storage_provider::maintenance::ExpirationPort
                + storage_provider::maintenance::GsiPort
                + storage_provider::maintenance::StreamRetentionPort
                + storage_provider::TableReclaimAttempt,
    {
        let native_attempt_capacity = Some(store.attempt_capacity()?);
        let scheduled_job_store = store.scheduled_job_store(config)?;
        Ok(Self {
            native_attempt_capacity,
            #[cfg(feature = "regional")]
            regional_services: None,
            database: None,
            dynamo: store.clone(),
            stream: Some(store.clone()),

            ttl_sweep_repository: Some(store.clone()),
            system_stream_trim_repository: Some(store.clone()),
            table_reclaim_repository: Some(store.clone()),
            gsi_lifecycle_repository: store.clone(),
            queue: Some(store.clone()),
            pubsub: Some(store.clone()),
            delivery_source: Some(Arc::new(super::delivery::NativeDeliverySource::new(
                store.clone(),
            ))),
            maintenance_source: Some(Arc::new(super::maintenance::NativeMaintenanceSource::new(
                store.clone(),
            ))),
            maintenance: Some(MaintenanceRepositories {
                job_catalog: scheduled_bg_jobs::JobCatalog::new(),
                partition_reconciliation_repository: store.clone().partition_reconciliation(),
                expiration_repository: store.clone(),
                gsi_build_repository: store.clone(),
                queue_message_cleanup_repository: Some(store.clone()),
                retention_repository: store,
            }),
            scheduled_job_store,
        })
    }
}

#[cfg(feature = "remote")]
pub struct RemoteScheduledJobStoreConfig {
    pub(super) endpoint_urls: Vec<String>,
    pub(super) request_timeout: Duration,
}
