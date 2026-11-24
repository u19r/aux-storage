use std::sync::Arc;
#[cfg(feature = "remote")]
use std::time::Duration;

use storage_common::native_admission::AdmittedStore;
use storage_provider::{
    MaintenanceOperations, StorageStreamOperations,
    maintenance::{PartitionReconciliationRepository, PartitionReconciliationSource},
};

#[derive(Clone)]
pub(crate) struct MaintenanceRepositories {
    pub job_catalog: scheduled_bg_jobs::JobCatalog,
    /// Every scheduled maintenance step of this store value.
    pub operations: Arc<dyn MaintenanceOperations>,
    pub partition_reconciliation_repository: Option<Arc<dyn PartitionReconciliationRepository>>,
    pub queue_message_cleanup_repository:
        Option<Arc<dyn queue_provider::QueueMessageCleanupRepository>>,
    /// The pubsub facade of this store; the stale pending-subscription sweep
    /// calls `PubsubProvider::sweep_stale_subscriptions`.
    pub pubsub_stale_subscriptions: Option<Arc<dyn pubsub_provider::PubsubProvider>>,
}

/// One initialized store before its connection exists. A caller that opened a
/// native store itself passes it to `DatabaseManager::from_initialized_store`.
pub struct InitializedStore {
    pub(crate) kind: StoreKind,
}

pub(crate) enum StoreKind {
    Native(Box<dyn super::native_store::NativeStore>),
    #[cfg(feature = "remote")]
    Remote {
        provider: Arc<storage_remote::RemoteStorageProvider>,
        jobs: bg_job_store::ScheduledJobStore,
    },
}

impl InitializedStore {
    pub fn native<S>(store: Arc<S>) -> Self
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
        Self {
            kind: StoreKind::Native(Box::new(super::native_store::InitializedNativeStore(store))),
        }
    }
}

#[cfg(feature = "remote")]
pub struct RemoteScheduledJobStoreConfig {
    pub(super) endpoint_urls: Vec<String>,
    pub(super) request_timeout: Duration,
    pub(super) internal_service_token: Option<String>,
}
