use std::sync::Arc;

use bg_job_store::ScheduledJobStore;
use storage_provider::{
    DynamoOperations, MaintenanceOperations, RegionalOperations, StorageStreamOperations,
};

use crate::{
    admission::AdmissionController, maintenance_runtime::MaintenanceRuntime,
    regional::RegionalAttachment,
};

/// Every capability of one configured connection. Built only by
/// `Connection::open` (or the remote test constructor), with its admission
/// controller first, so every admitted store value is bound from birth.
pub(crate) struct Connection {
    /// Registry connection id.
    pub(crate) id: String,
    pub(crate) admission: AdmissionController,
    pub(crate) dynamo: Arc<dyn DynamoOperations>,
    pub(crate) jobs: ScheduledJobStore,
    /// `None` for a remote connection.
    pub(crate) native: Option<NativeCapabilities>,
    /// Stop state of a native store whose commits become visible before they
    /// are on disk; readiness reports it.
    pub(crate) durability: Option<storage_common::durability::DurabilityHealth>,
    /// Built when database jobs run; taken and seeded by manager
    /// initialization.
    pub(crate) maintenance: Option<MaintenanceRuntime>,
    /// Present only when this connection has a regional file and jobs run.
    pub(crate) regional: Option<RegionalAttachment>,
}

/// Capabilities that only a native store has.
pub(crate) struct NativeCapabilities {
    pub(crate) stream: Arc<dyn StorageStreamOperations>,
    pub(crate) regional: Arc<dyn RegionalOperations>,
    /// Caller-lane maintenance commands (TTL sweep commands, system trim
    /// pages, test table cleanup); scheduled jobs use the background value.
    pub(crate) maintenance: Arc<dyn MaintenanceOperations>,
    /// Queue and pubsub store values, one per admission lane.
    pub(crate) messaging: crate::messaging::MessagingStores,
}

impl Connection {
    pub(crate) fn stream(&self) -> storage_types::StorageResult<Arc<dyn StorageStreamOperations>> {
        self.native
            .as_ref()
            .map(|native| Arc::clone(&native.stream))
            .ok_or_else(|| {
                storage_types::StorageError::unsupported("stream operations are unavailable")
            })
    }
}
