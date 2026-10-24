use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use regional_replication::runtime::BoundRegionalServices;
use storage_provider::{
    DynamoPort, StreamRetentionRepository, SystemStreamTrimRepository, TableReclaimRepository,
    maintenance::{
        ExpirationRepository, GsiBuildRepository, GsiLifecycleRepository,
        PartitionReconciliationSource,
    },
};
use storage_types::{StorageError, StorageResult};
use stream_provider::StreamOperations;

use super::StorageProviderBundle;

impl StorageProviderBundle {
    /// Derive journal identity and codec bounds from the qualified deployment,
    /// bind intake, then attach handlers to the existing native job store.
    pub(crate) async fn initialize_regional_store<S>(
        store: Arc<S>,
        config: bg_job_store::ScheduledJobStoreConfig,
        startup: regional_replication::runtime::RegionalStartup,
    ) -> StorageResult<Self>
    where
        S: storage_provider::native_admission::NativeAttemptSource
            + DynamoPort
            + queue_provider::QueuePort
            + infrastructure_provider::physical_store::PhysicalRecordStore
            + bg_job_store::ScheduledJobStoreSource
            + StreamOperations
            + stream_provider::StreamPort
            + queue_provider::QueueOperations
            + queue_provider::QueueMessageCleanupRepository
            + pubsub_provider::PubsubProvider
            + pubsub_provider::PubsubPort
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
            PhysicalRecordAttempt
                + storage_provider::maintenance::ExpirationPort
                + storage_provider::maintenance::GsiPort
                + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>
                + storage_provider::PrimaryStateScanAttempt
                + storage_provider::ItemTombstoneScanAttempt
                + storage_provider::GsiProjectionScanAttempt
                + storage_provider::TtlProjectionScanAttempt
                + storage_provider::GsiProjectionMetadataAttempt
                + storage_provider::maintenance::StreamRetentionPort
                + storage_provider::TableReclaimAttempt,
        for<'native> <S as stream_provider::StreamPort>::Attempt<'native>:
            stream_provider::retained_image::RetainedOwnershipAttempt
                + storage_provider::CatalogAttempt,
    {
        let runtime =
            regional_replication::runtime::RegionalRuntime::initialize(Arc::clone(&store), startup)
                .await
                .map_err(map_startup_error)?;
        let services = runtime.bind_services().await.map_err(map_startup_error)?;
        Self::try_from_bound_regional_store(store, config, services)
    }

    /// Preserve every ordinary provider and maintenance owner while selecting
    /// the regional Dynamo executor over the exact same initialized store.
    pub fn try_from_bound_regional_store<S>(
        store: Arc<S>,
        config: bg_job_store::ScheduledJobStoreConfig,
        services: BoundRegionalServices<S>,
    ) -> StorageResult<Self>
    where
        S: storage_provider::native_admission::NativeAttemptSource
            + DynamoPort
            + queue_provider::QueuePort
            + bg_job_store::ScheduledJobStoreSource
            + StreamOperations
            + stream_provider::StreamPort
            + queue_provider::QueueOperations
            + queue_provider::QueueMessageCleanupRepository
            + pubsub_provider::PubsubProvider
            + pubsub_provider::PubsubPort
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
            PhysicalRecordAttempt
                + storage_provider::maintenance::ExpirationPort
                + storage_provider::maintenance::GsiPort
                + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>
                + storage_provider::PrimaryStateScanAttempt
                + storage_provider::ItemTombstoneScanAttempt
                + storage_provider::GsiProjectionScanAttempt
                + storage_provider::TtlProjectionScanAttempt
                + storage_provider::GsiProjectionMetadataAttempt
                + storage_provider::maintenance::StreamRetentionPort
                + storage_provider::TableReclaimAttempt,
        for<'native> <S as stream_provider::StreamPort>::Attempt<'native>:
            stream_provider::retained_image::RetainedOwnershipAttempt
                + storage_provider::CatalogAttempt,
    {
        let runtime = services.runtime();
        if !runtime.uses_port(&store) {
            return Err(StorageError::validation(
                "regional runtime and provider bundle require the same native store",
            ));
        }
        let mut bundle = Self::try_from_initialized_store(store, config)?;
        let operations = runtime.operations();
        let maintenance = bundle.maintenance.as_mut().ok_or_else(|| {
            StorageError::internal("regional maintenance repositories are absent")
        })?;
        maintenance.job_catalog = services.job_catalog();
        maintenance.expiration_repository = operations.clone();
        maintenance.gsi_build_repository = operations.clone();
        maintenance.retention_repository = operations.clone();
        let repositories = runtime.scheduled_repositories()?;
        maintenance.partition_reconciliation_repository = repositories.partition;
        maintenance.queue_message_cleanup_repository = Some(repositories.queue);
        bundle.maintenance_source = None;
        bundle.table_reclaim_repository = Some(operations.clone());
        bundle.ttl_sweep_repository = Some(operations.clone());
        bundle.dynamo = operations;
        bundle.stream = Some(runtime.streams());
        bundle.regional_services = Some(services.prepare());
        Ok(bundle)
    }
}

fn map_startup_error(error: regional_replication::transport::TransportError) -> StorageError {
    match error {
        regional_replication::transport::TransportError::Native(error) => error,
        regional_replication::transport::TransportError::Configuration => {
            StorageError::validation("invalid regional startup configuration")
        }
        error => StorageError::internal(&format!("regional startup failed: {error}")),
    }
}
