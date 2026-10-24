use std::sync::Arc;

use storage_provider::{
    StreamRetentionRepository, SystemStreamTrimRepository, TableReclaimRepository,
    maintenance::{
        ExpirationRepository, GsiBuildRepository, GsiLifecycleRepository,
        PartitionReconciliationSource,
    },
};
use storage_types::StorageResult;
use stream_provider::StreamOperations;

use super::StorageProviderBundle;
use crate::DatabaseManagerRuntimeOptions;

#[cfg(feature = "regional")]
pub(super) async fn prepare_native_bundle<S>(
    store: Arc<S>,
    options: &DatabaseManagerRuntimeOptions,
    connection_id: &str,
) -> StorageResult<StorageProviderBundle>
where
    S: storage_provider::native_admission::NativeAttemptSource
        + storage_provider::DynamoPort
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
        infrastructure_provider::physical_attempt::PhysicalRecordAttempt
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
    options
        .admission_config
        .qualify_native_capacity(store.attempt_capacity()?)?;
    match options.regional_connections.get(connection_id) {
        Some(startup) => {
            StorageProviderBundle::initialize_regional_store(
                store,
                options.scheduled_job_store_config,
                startup.clone(),
            )
            .await
        }
        None => StorageProviderBundle::try_from_initialized_store(
            store,
            options.scheduled_job_store_config,
        ),
    }
}

// Ordinary builds do not depend on native protected-record capabilities.
#[cfg(not(feature = "regional"))]
pub(super) async fn prepare_native_bundle<S>(
    store: Arc<S>,
    options: &DatabaseManagerRuntimeOptions,
    _connection_id: &str,
) -> StorageResult<StorageProviderBundle>
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
    StorageProviderBundle::try_from_initialized_store(store, options.scheduled_job_store_config)
}
