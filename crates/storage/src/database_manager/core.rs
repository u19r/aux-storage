use std::sync::Arc;

use bg_job_store::ScheduledJobStore;
use storage_provider::{DynamoOperations, StorageStreamOperations};
use storage_types::{
    KeyAttributes, StorageError, StorageResult, StoredTableInfo, TableName, VecMap,
};

use crate::{
    admission::{AdmissionClass, AdmissionController, AdmissionPermit},
    connection::Connection,
    database_manager::constants::ROUTED_DEFAULT_CONNECTION_ID,
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
    /// The registry default connection. The synthetic route id `default`
    /// resolves to it.
    pub(super) default_connection: Connection,
    /// Every other configured connection, keyed by registry id.
    pub(super) named_connections: VecMap<String, Connection>,
    pub(super) background_services:
        tokio::sync::Mutex<super::service_shutdown::BackgroundServiceState>,
    pub(super) single_table_mode: bool,
}

impl DatabaseManager {
    /// The one connection resolver: the registry id, else the synthetic route
    /// id `default` names the default connection.
    pub(crate) fn connection(&self, connection_id: &str) -> StorageResult<&Connection> {
        if connection_id == self.default_connection.id {
            return Ok(&self.default_connection);
        }
        if let Some(connection) = self.named_connections.get(connection_id) {
            return Ok(connection);
        }
        if connection_id == ROUTED_DEFAULT_CONNECTION_ID {
            return Ok(&self.default_connection);
        }
        Err(StorageError::validation(format!(
            "storage connection '{connection_id}' is not configured"
        )))
    }

    pub(super) fn connections(&self) -> impl Iterator<Item = &Connection> {
        std::iter::once(&self.default_connection).chain(self.named_connections.values())
    }

    /// The regional service of one native connection, when its regional file is
    /// configured.
    pub(crate) fn regional_service(
        &self,
        connection_id: &str,
    ) -> Option<&Arc<crate::regional::RegionalService>> {
        self.connection(connection_id)
            .ok()?
            .regional
            .as_ref()
            .map(|attachment| &attachment.service)
    }

    /// The scheduled job store of the default connection. Always `Some`; the
    /// `Option` stays so aux-fn callers do not change in this package.
    #[must_use]
    pub fn scheduled_job_store(&self) -> Option<ScheduledJobStore> {
        Some(self.default_connection.jobs.clone())
    }

    pub(super) const fn single_table_mode_enabled(&self) -> bool {
        self.single_table_mode
    }

    pub(super) fn admission_controller_for_connection(
        &self,
        connection_id: &str,
    ) -> StorageResult<&AdmissionController> {
        Ok(&self.connection(connection_id)?.admission)
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
        let connection = self.connection(connection_id)?;
        let dynamo = Arc::clone(&connection.dynamo);
        connection
            .admission
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
        let dynamo = Arc::clone(&self.connection(connection_id)?.dynamo);
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
                storage_common::native_admission::NativeWork::Recovery,
                operation,
            )
            .await
    }

    pub async fn run_stream_admitted<F, Fut, T>(
        &self,
        connection_id: &str,
        class: AdmissionClass,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn StorageStreamOperations>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let connection = self.connection(connection_id)?;
        let stream = connection.stream()?;
        connection
            .admission
            .settle_foreground(class, || operation(stream))
            .await
    }

    pub(crate) async fn run_stream_control<F, Fut, T>(
        &self,
        connection_id: &str,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn StorageStreamOperations>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let stream = self.connection(connection_id)?.stream()?;
        self.run_scoped_control(connection_id, move || operation(stream))
            .await
    }

    /// Append a TestUtil record to one accepted immutable table incarnation.
    pub async fn append_table_stream_record(
        &self,
        target: &storage_types::TableHistoryTarget,
        item_data: &[u8],
    ) -> StorageResult<storage_types::StreamItemId> {
        self.append_table_stream_record_for_operation(
            stream_provider::StreamAppendOperationId::fresh(),
            target,
            item_data,
        )
        .await
    }

    pub async fn append_table_stream_record_for_operation(
        &self,
        operation: stream_provider::StreamAppendOperationId,
        target: &storage_types::TableHistoryTarget,
        item_data: &[u8],
    ) -> StorageResult<storage_types::StreamItemId> {
        let target = target.clone();
        let item_data = item_data.to_vec();
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            move |stream| async move {
                let database_call = metrics_facade::begin_database_call("append_table_stream");
                let result = stream
                    .append_table_item(operation, target, &item_data)
                    .await;
                drop(database_call);
                result
            },
        )
        .await
    }

    pub async fn reconcile_table_stream_record(
        &self,
        operation: stream_provider::StreamAppendOperationId,
        target: storage_types::TableHistoryTarget,
        data: Vec<u8>,
    ) -> StorageResult<stream_provider::TableAppendReconciliation> {
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::PointRead,
            move |stream| async move {
                stream
                    .reconcile_table_append(operation, target, &data)
                    .await
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
        self.default_connection.admission.clone()
    }

    #[must_use]
    pub fn fixed_ingress_limit(&self) -> usize {
        crate::admission::fixed_ingress_limit(
            self.connections().map(|connection| &connection.admission),
        )
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

    /// The queue and pubsub store values of the default connection in
    /// `lane`; `None` for a remote connection. Customer values take one
    /// admission permit per native attempt.
    #[must_use]
    pub fn messaging_store(
        &self,
        lane: crate::messaging::MessagingLane,
    ) -> Option<crate::messaging::MessagingStore> {
        self.default_messaging_stores()
            .map(|stores| stores.lane(lane))
    }

    /// Every messaging lane of the default connection, the delivery lane
    /// included (`MessagingServices`).
    pub(crate) fn default_messaging_stores(&self) -> Option<&crate::messaging::MessagingStores> {
        self.default_connection
            .native
            .as_ref()
            .map(|native| &native.messaging)
    }
}
