//! One initialized native family store (`SqlStore<_>`, `KvStore<_>`) before its
//! connection exists. Capability values are cast from it only after the
//! connection's admission controller exists, so admitted values are born bound.
use std::sync::Arc;

use bg_job_store::{ScheduledJobStore, ScheduledJobStoreConfig};
use storage_common::native_admission::{AdmittedStore, AttemptAdmission, NativeAdmissionBinding};
use storage_provider::{
    DynamoOperations, MaintenanceOperations, StorageStreamOperations,
    maintenance::PartitionReconciliationSource,
};
use storage_types::{NativeAttemptCapacity, StorageResult};

use super::models::MaintenanceRepositories;
use crate::{
    connection::NativeCapabilities,
    messaging::{MessagingStore, MessagingStores},
};

pub(crate) trait NativeStore: Send + Sync {
    fn attempt_capacity(&self) -> StorageResult<NativeAttemptCapacity>;
    fn durability_health(&self) -> Option<storage_common::durability::DurabilityHealth>;
    fn scheduled_job_store(
        &self,
        config: ScheduledJobStoreConfig,
    ) -> StorageResult<ScheduledJobStore>;
    fn dynamo(&self) -> Arc<dyn DynamoOperations>;
    /// Capabilities of the connection. Values without a lane are admitted by
    /// their caller; the customer and delivery messaging values admit each
    /// attempt through `binding`.
    fn capabilities(&self, binding: &Arc<NativeAdmissionBinding>) -> NativeCapabilities;
    /// Maintenance repositories over one store value in `lane`.
    fn maintenance(&self, lane: AttemptAdmission) -> MaintenanceRepositories;
}

pub(crate) struct InitializedNativeStore<S>(pub(crate) Arc<S>);

impl<S> NativeStore for InitializedNativeStore<S>
where
    S: storage_common::native_admission::NativeAttemptSource
        + AdmittedStore
        + storage_provider::DynamoPort
        + bg_job_store::ScheduledJobStoreSource
        + StorageStreamOperations
        + queue_provider::QueueOperations
        + queue_provider::QueueMessageCleanupRepository
        + pubsub_provider::PubsubProvider
        + pubsub_provider::PubsubPort
        + queue_provider::QueuePort
        + PartitionReconciliationSource
        + MaintenanceOperations
        + 'static,
    for<'native> <S as storage_provider::DynamoPort>::Attempt<'native>:
        storage_provider::PrimaryStateScanAttempt + storage_provider::RegionalPairingAttempt,
{
    fn attempt_capacity(&self) -> StorageResult<NativeAttemptCapacity> {
        storage_common::native_admission::NativeAttemptSource::attempt_capacity(self.0.as_ref())
    }

    fn durability_health(&self) -> Option<storage_common::durability::DurabilityHealth> {
        storage_common::native_admission::NativeAttemptSource::durability_health(self.0.as_ref())
    }

    fn scheduled_job_store(
        &self,
        config: ScheduledJobStoreConfig,
    ) -> StorageResult<ScheduledJobStore> {
        self.0.scheduled_job_store(config)
    }

    fn dynamo(&self) -> Arc<dyn DynamoOperations> {
        self.0.clone()
    }

    fn capabilities(&self, binding: &Arc<NativeAdmissionBinding>) -> NativeCapabilities {
        let store = &self.0;
        NativeCapabilities {
            stream: store.clone(),
            regional: store.clone(),
            maintenance: store.clone(),
            messaging: {
                // Customer requests (SQS and SNS) and scheduled delivery share
                // this connection's controller.
                let customer =
                    Arc::new(store.admitted(AttemptAdmission::Foreground(Arc::clone(binding))));
                MessagingStores {
                    caller: MessagingStore {
                        queue: store.clone(),
                        pubsub: store.clone(),
                    },
                    customer: MessagingStore {
                        queue: customer.clone(),
                        pubsub: customer,
                    },
                    delivery: Arc::new(
                        store.admitted(AttemptAdmission::Background(Arc::clone(binding))),
                    ),
                }
            },
        }
    }

    fn maintenance(&self, lane: AttemptAdmission) -> MaintenanceRepositories {
        // Every maintenance job shares this one store value.
        let store = Arc::new(self.0.admitted(lane));
        let partition = PartitionReconciliationSource::partition_reconciliation(Arc::clone(&store));
        MaintenanceRepositories {
            job_catalog: scheduled_bg_jobs::JobCatalog::new(),
            operations: store.clone(),
            partition_reconciliation_repository: partition,
            queue_message_cleanup_repository: Some(store.clone()),
            pubsub_stale_subscriptions: Some(store),
        }
    }
}
