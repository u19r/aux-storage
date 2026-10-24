use std::sync::Arc;

use bg_job_store::ScheduledJobStore;
use storage_provider::{
    DynamoOperations, ListChangeIndexMarkersRequest, SystemStreamTrimRepository,
    TableReclaimRepository, TtlSweepRepository, maintenance::GsiLifecycleRepository,
};
use storage_types::{
    KeyAttributes, StorageError, StorageResult, StoredTableInfo, TableName, VecMap,
};
use stream::StreamError;
use stream_provider::StreamOperations;

use crate::{
    admission::{AdmissionClass, AdmissionController, AdmissionPermit, AdmissionRegistry},
    database_manager::constants::ROUTED_DEFAULT_CONNECTION_ID,
    maintenance_runtime::MaintenanceRuntime,
};

#[derive(Clone)]
pub struct ResolvedStorageOperation {
    pub(super) logical_table_name: TableName,
    pub table_info: Arc<StoredTableInfo>,
}

impl ResolvedStorageOperation {
    pub fn validate_key(self, key: KeyAttributes) -> StorageResult<ResolvedGetItem> {
        storage_types::validate_key_attributes_for_schema(&self.table_info.key_schema, &key)?;
        Ok(ResolvedGetItem {
            operation: self,
            key,
        })
    }
}

pub struct ResolvedGetItem {
    pub(super) operation: ResolvedStorageOperation,
    pub(super) key: KeyAttributes,
}

pub struct DatabaseManager {
    pub(super) point_reads: std::sync::OnceLock<Arc<crate::point_read::PointReadSource>>,
    pub(super) dynamo: Arc<dyn DynamoOperations>,
    pub(super) stream: Option<Arc<dyn StreamOperations>>,
    pub(super) scheduled_job_store: Option<ScheduledJobStore>,

    pub(super) gsi_lifecycle_repository: Option<Arc<dyn GsiLifecycleRepository>>,
    pub(super) maintenance_runtimes: Vec<Arc<MaintenanceRuntime>>,
    pub(super) background_services:
        tokio::sync::Mutex<super::service_shutdown::BackgroundServiceState>,
    pub(super) system_stream_trim_repository: Option<Arc<dyn SystemStreamTrimRepository>>,
    pub(super) table_reclaim_repository: Option<Arc<dyn TableReclaimRepository>>,
    pub(super) ttl_sweep_repository: Option<Arc<dyn TtlSweepRepository>>,
    pub(super) queue_provider: Option<Arc<dyn queue_provider::QueueOperations>>,
    pub(super) pubsub_provider: Option<Arc<dyn pubsub_provider::PubsubProvider>>,
    pub(super) delivery_providers: Option<crate::provider_bundle::DeliveryProviders>,
    pub(super) dynamo_registry: VecMap<String, Arc<dyn DynamoOperations>>,
    pub(super) admission_registry: AdmissionRegistry,
    pub(super) single_table_mode: bool,
}

impl DatabaseManager {
    /// Return the bootstrapped native job store for the default physical
    /// connection. Remote and mock providers do not supply a native store.
    #[must_use]
    pub fn scheduled_job_store(&self) -> Option<ScheduledJobStore> {
        self.scheduled_job_store.clone()
    }

    pub(super) const fn single_table_mode_enabled(&self) -> bool {
        self.single_table_mode
    }

    pub(super) fn admission_controller_for_connection(
        &self,
        connection_id: &str,
    ) -> StorageResult<&AdmissionController> {
        self.admission_registry
            .for_connection(connection_id)
            // `default` is the stable synthetic route id used by the
            // single-backend API. A named connection registry may declare a
            // different physical default, so use that controller when no
            // literal `default` connection exists.
            .or_else(|| {
                (connection_id == ROUTED_DEFAULT_CONNECTION_ID)
                    .then(|| self.admission_registry.default_controller())
            })
            .ok_or_else(|| {
                StorageError::validation(format!(
                    "storage connection '{connection_id}' is not configured"
                ))
            })
    }

    /// Acquire a foreground permit for an externally owned admission interval.
    /// Provider operations use the admitted dispatch methods so response
    /// cancellation cannot release capacity before native settlement.
    pub async fn acquire_admission(
        &self,
        connection_id: &str,
        class: AdmissionClass,
    ) -> StorageResult<AdmissionPermit> {
        let controller = self.admission_controller_for_connection(connection_id)?;
        controller
            .acquire(class)
            .await
            .map_err(|rejection| StorageError::service_unavailable(rejection.retry_after_seconds))
    }

    pub async fn run_dynamo_admitted<F, Fut, T>(
        &self,
        connection_id: &str,
        class: AdmissionClass,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn DynamoOperations>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let dynamo = self.dynamo_for_connection(connection_id)?;
        self.admission_controller_for_connection(connection_id)?
            .settle_foreground(class, || operation(dynamo))
            .await
    }

    pub(crate) async fn run_dynamo_control<F, Fut, T>(
        &self,
        connection_id: &str,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn DynamoOperations>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let dynamo = self.dynamo_for_connection(connection_id)?;
        self.run_scoped_control(connection_id, move || operation(dynamo))
            .await
    }

    pub(crate) async fn run_scoped_control<F, Fut, T>(
        &self,
        connection_id: &str,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        self.admission_controller_for_connection(connection_id)?
            .settle_native(
                storage_provider::native_admission::NativeWork::Recovery,
                operation,
            )
            .await
    }

    fn dynamo_for_connection(
        &self,
        connection_id: &str,
    ) -> StorageResult<Arc<dyn DynamoOperations>> {
        self.dynamo_registry
            .get(connection_id)
            .or_else(|| (connection_id == ROUTED_DEFAULT_CONNECTION_ID).then_some(&self.dynamo))
            .cloned()
            .ok_or_else(|| {
                StorageError::validation(format!(
                    "storage connection '{connection_id}' is not configured"
                ))
            })
    }

    pub async fn run_stream_admitted<F, Fut, T>(
        &self,
        connection_id: &str,
        class: AdmissionClass,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn StreamOperations>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let stream = self.stream_for_connection(connection_id)?;
        self.admission_controller_for_connection(connection_id)?
            .settle_foreground(class, || operation(stream))
            .await
    }

    pub(crate) async fn run_stream_control<F, Fut, T>(
        &self,
        connection_id: &str,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn StreamOperations>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let stream = self.stream_for_connection(connection_id)?;
        self.run_scoped_control(connection_id, move || operation(stream))
            .await
    }

    fn stream_for_connection(
        &self,
        connection_id: &str,
    ) -> StorageResult<Arc<dyn StreamOperations>> {
        self.stream
            .as_ref()
            .filter(|_| connection_id == ROUTED_DEFAULT_CONNECTION_ID)
            .cloned()
            .ok_or_else(|| StorageError::unsupported("stream operations are unavailable"))
    }

    /// Append a TestUtil table-stream record to the manager's native database
    /// using the logical table name as the stream identity.
    pub async fn append_table_stream_record(
        &self,
        table_name: &TableName,
        item_data: &[u8],
    ) -> StorageResult<storage_types::StreamItemId> {
        self.append_table_stream_record_for_operation(
            stream_provider::StreamAppendOperationId::fresh(),
            table_name,
            item_data,
        )
        .await
    }

    pub async fn append_table_stream_record_for_operation(
        &self,
        operation: stream_provider::StreamAppendOperationId,
        table_name: &TableName,
        item_data: &[u8],
    ) -> StorageResult<storage_types::StreamItemId> {
        let table_name = table_name.clone();
        let item_data = item_data.to_vec();
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            move |stream| async move {
                let database_call = metrics_facade::begin_database_call("append_table_stream");
                let result = stream
                    .append_table_item(operation, table_name, &item_data)
                    .await;
                drop(database_call);
                result.map_err(|error| StorageError::from(StreamError::into_storage_enum(error)))
            },
        )
        .await
    }

    pub async fn reconcile_table_stream_record(
        &self,
        operation: stream_provider::StreamAppendOperationId,
        table: TableName,
        data: Vec<u8>,
    ) -> StorageResult<stream_provider::TableAppendReconciliation> {
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::PointRead,
            move |stream| async move {
                stream
                    .reconcile_table_append(operation, table, &data)
                    .await
                    .map_err(|error| StorageError::from(StreamError::into_storage_enum(error)))
            },
        )
        .await
    }

    #[cfg(not(feature = "regional"))]
    pub async fn read_canonical_history_response(
        &self,
        _payload: serde_json::Value,
    ) -> StorageResult<super::CanonicalHistoryResponse> {
        Err(StorageError::unsupported(
            "Canonical regional history is unavailable",
        ))
    }

    /// Read change-index markers through the same bounded range-read lane as
    /// other provider reads.
    pub async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<storage_provider::ChangeIndexMarker>> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            |provider| async move {
                let database_call =
                    metrics_facade::begin_database_call("list_change_index_markers");
                let result = provider.list_change_index_markers(request).await;
                drop(database_call);
                result
            },
        )
        .await
    }

    #[must_use]
    pub fn admission_controller(&self, connection_id: &str) -> Option<AdmissionController> {
        self.admission_controller_for_connection(connection_id)
            .ok()
            .cloned()
    }

    #[must_use]
    pub fn default_admission_controller(&self) -> AdmissionController {
        self.admission_registry.default_controller().clone()
    }

    #[must_use]
    pub fn fixed_ingress_limit(&self) -> usize {
        self.admission_registry.fixed_ingress_limit()
    }

    pub async fn resolve_storage_operation(
        &self,
        table_name: TableName,
    ) -> StorageResult<ResolvedStorageOperation> {
        Ok(ResolvedStorageOperation {
            table_info: self.load_physical_table_info(&table_name).await?,
            logical_table_name: table_name,
        })
    }

    #[must_use]
    pub fn initialization_queue_provider(
        &self,
    ) -> Option<Arc<dyn queue_provider::QueueOperations>> {
        self.queue_provider.clone()
    }

    /// Scheduled delivery yields to queued foreground work on the selected
    /// connection.
    pub fn initialization_delivery_pubsub_provider(
        &self,
    ) -> Option<Arc<dyn pubsub_provider::PubsubProvider>> {
        self.delivery_providers
            .as_ref()
            .map(|providers| Arc::clone(&providers.pubsub))
    }

    /// Background SQS sends use the same connection as delivery claims and
    /// receipts.
    pub fn initialization_delivery_queue_provider(
        &self,
    ) -> Option<Arc<dyn queue_provider::QueueOperations>> {
        self.delivery_providers
            .as_ref()
            .map(|providers| Arc::clone(&providers.queue))
    }

    #[must_use]
    pub fn initialization_pubsub_provider(
        &self,
    ) -> Option<Arc<dyn pubsub_provider::PubsubProvider>> {
        self.pubsub_provider.clone()
    }
}
