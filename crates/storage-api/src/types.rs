use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use serde::Deserialize;
use storage::DatabaseManager;
use tokio::sync::Semaphore;
#[cfg(any(test, feature = "internal-test-routes"))]
use tokio::sync::{Mutex, Notify};

use crate::{health::HealthTracker, manager::StorageApiManagerImpl};

#[derive(Clone)]
pub struct AppState {
    pub db_manager: Arc<DatabaseManager>,
    pub storage_manager: Arc<StorageApiManagerImpl>,
    #[cfg(feature = "queue")]
    pub queue_manager: Option<Arc<queue::QueueManager>>,
    #[cfg(feature = "queue")]
    pub queue_public_base_url: String,
    #[cfg(feature = "queue")]
    pub queue_account_id: String,
    #[cfg(feature = "pubsub")]
    pub pubsub_manager: Option<Arc<pubsub::PubsubManager>>,
    pub health: Arc<HealthTracker>,
    startup_readiness: Arc<AtomicBool>,
    #[cfg(any(test, feature = "internal-test-routes"))]
    pub(crate) admission_test_holds: Arc<Mutex<Vec<storage::admission::AdmissionPermit>>>,
    #[cfg(any(test, feature = "internal-test-routes"))]
    pub(crate) admission_test_release: Arc<Notify>,
    pub(crate) ingress_semaphore: Arc<Semaphore>,
}

impl AppState {
    #[must_use]
    pub fn new(db_manager: Arc<DatabaseManager>) -> Self {
        let storage_manager = Arc::new(StorageApiManagerImpl::new(db_manager.clone()));
        Self::with_manager(db_manager, storage_manager)
    }

    #[must_use]
    pub fn with_manager(
        db_manager: Arc<DatabaseManager>,
        storage_manager: Arc<StorageApiManagerImpl>,
    ) -> Self {
        let ingress_limit = db_manager.fixed_ingress_limit();
        Self {
            db_manager,
            storage_manager,
            #[cfg(feature = "queue")]
            queue_manager: None,
            #[cfg(feature = "queue")]
            queue_public_base_url: String::new(),
            #[cfg(feature = "queue")]
            queue_account_id: "000000000000".to_string(),
            #[cfg(feature = "pubsub")]
            pubsub_manager: None,
            health: Arc::new(HealthTracker::new()),
            startup_readiness: Arc::new(AtomicBool::new(true)),
            #[cfg(any(test, feature = "internal-test-routes"))]
            admission_test_holds: Arc::new(Mutex::new(Vec::new())),
            #[cfg(any(test, feature = "internal-test-routes"))]
            admission_test_release: Arc::new(Notify::new()),
            ingress_semaphore: Arc::new(Semaphore::new(ingress_limit)),
        }
    }

    #[must_use]
    pub fn with_startup_readiness(mut self, readiness: Arc<AtomicBool>) -> Self {
        self.startup_readiness = readiness;
        self
    }

    pub(crate) fn publish_startup_readiness(&self) {
        self.startup_readiness.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_startup_ready(&self) -> bool {
        self.startup_readiness.load(Ordering::Acquire)
    }

    #[cfg(any(feature = "queue", feature = "pubsub"))]
    #[must_use]
    pub fn with_messaging(
        mut self,
        #[cfg(feature = "queue")] queue_manager: Option<Arc<queue::QueueManager>>,
        #[cfg(feature = "queue")] queue_public_base_url: String,
        #[cfg(feature = "queue")] queue_account_id: String,
        #[cfg(feature = "pubsub")] pubsub_manager: Option<Arc<pubsub::PubsubManager>>,
    ) -> Self {
        #[cfg(feature = "queue")]
        {
            self.queue_manager = queue_manager;
            self.queue_public_base_url = queue_public_base_url;
            self.queue_account_id = queue_account_id;
        }
        #[cfg(feature = "pubsub")]
        {
            self.pubsub_manager = pubsub_manager;
        }
        self
    }
}

#[derive(Debug, Clone)]

pub enum Response {
    CreateTable(storage_types::CreateTableResponse),
    ListTables(storage_types::ListTablesResponse),
    DeleteTable(storage_types::DeleteTableResponse),
    DescribeTable(storage_types::DescribeTableResponse),
    PutItem(storage_types::PutItemResponse),
    GetItem(storage_types::GetItemResponse),
    DeleteItem(storage_types::DeleteItemResponse),
    UpdateItem(storage_types::UpdateItemResponse),
    UpdateTable(storage_types::UpdateTableResponse),
    UpdateTimeToLive(storage_types::UpdateTimeToLiveResponse),
    Scan(storage_types::ScanResponse),
    Query(storage_types::QueryResponse),
    QuerySourcePage(storage_provider::RangeReadPage<storage_provider::RangeReadItem>),
    BatchWriteItem(storage_types::BatchWriteItemResponse),
    BatchGetItem(storage_types::BatchGetItemResponse),
    TransactWriteItems(storage_types::TransactWriteItemsResponse),
    TransactGetItems(storage_types::TransactGetItemsResponse),
    ReadSequence(storage_types::ReadSequenceResponse),
    GetStreamRecords(storage_types::GetStreamRecordsResponse),
    DescribeSystemStream(storage_types::DescribeSystemStreamResponse),
    ListStreams(storage_types::ListStreamsResponse),
    DescribeStream(storage_types::DescribeStreamResponse),
    GetShardIterator(storage_types::GetShardIteratorResponse),
    GetRecords(storage_types::GetRecordsResponse),
    DescribeTimeToLive(storage_types::DescribeTimeToLiveResponse),
    ListChangeIndexMarkers(storage_provider::ListChangeIndexMarkersResponse),
    CanonicalHistory(Arc<storage::CanonicalHistoryResponse>),
    Raw(serde_json::Value),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct UpdateContinuousBackupsRequest {
    pub table_name: String,
    pub point_in_time_recovery_specification: PointInTimeRecoverySpecification,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct PointInTimeRecoverySpecification {
    pub point_in_time_recovery_enabled: bool,
}
