#[cfg(all(test, feature = "cache-write-planner"))]
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::{collections::HashMap, sync::Arc};

use bg_jobs::BackgroundJobName;
use storage_common::GSI_UPDATE_JOB;
use storage_provider::{
    ListChangeIndexMarkersRequest, ReadSequenceExecution, ReadSequenceExecutionBudget,
    StorageProvider, StorageProviderReadContext,
};
use storage_types::{
    AllOld, AttributeValue, CreateTableRequest, DescribeTimeToLiveResponse, IndexName,
    KeyAttributes, KeySchemaElement, PutItemResponse, QueryTableRequest, ReadSequenceConsistency,
    ReadSequenceProviderCapabilities, ReturnValuesOldNewUpdated, StorageEnum, StorageError,
    StorageResult, StoredTableInfo, TableName, TableNamespace, TableStatus, TransactEncodeItem,
    TryIntoWireItem, UpdateItemResponse, UpdateTimeToLiveRequest, UpdateTimeToLiveResponse,
    WireItem, context::WrappedError as _,
};
use stream::StreamProvider;
#[cfg(all(test, feature = "cache-write-planner"))]
use tokio::sync::Notify;
use tokio::{sync::RwLock, task::JoinHandle};
use typed_builder::TypedBuilder;

#[cfg(feature = "cache-write-planner")]
use crate::cache_write_planner::{StorageCachePlannerLoad, StorageCacheWritePlanner};
use crate::{
    admission::{
        AdmissionClass, AdmissionController, AdmissionOutcome, AdmissionPermit, AdmissionRegistry,
    },
    cache_batch_get_runtime::StorageBatchGetCacheRuntime,
    cache_coordinator::StorageCacheServices,
    cache_point_read_runtime::StoragePointReadCacheRuntime,
    cache_query_runtime::{StorageCacheQueryRuntime, StorageCacheQueryRuntimeLoad},
    database_manager::{
        constants::{
            CAPPED_ENTITY_COUNTER_DELTA_VALUE, CAPPED_ENTITY_COUNTER_ENTITY_TYPE_NAME,
            CAPPED_ENTITY_COUNTER_ENTITY_TYPE_VALUE, CAPPED_ENTITY_COUNTER_MAX_VALUE,
            CAPPED_ENTITY_COUNTER_PK, CAPPED_ENTITY_COUNTER_VALUE_ATTR,
            CAPPED_ENTITY_COUNTER_VALUE_NAME, CAPPED_ENTITY_COUNTER_ZERO_VALUE,
            OCC_CREATE_CONDITION, OCC_UPDATE_CONDITION, OCC_VERSION_ATTR, OCC_VERSION_NAME,
            ROUTED_DEFAULT_CONNECTION_ID,
        },
        operation_metrics::record_storage_operation,
        routed_write_ops::{RoutedWriteTargetRole, ensure_route_writes_not_paused},
        wire_item_ops::PutItemPayload,
    },
    namespace_routing::{
        NamespaceRequestRewriter, NamespaceRoute, NamespaceRouteRecord, NamespaceRouteResolver,
        NamespaceStorageMode, RouteTarget, is_shared_table_enabled_namespace_route,
        parse_namespace_route_record, reject_direct_shared_table_access,
    },
    newtypes::DatabaseTrait,
    tables::Tables,
};

#[cfg(all(test, feature = "cache-write-planner"))]
#[derive(Debug, Default)]
struct DatabaseManagerTestPauseState {
    armed: AtomicBool,
    reached: AtomicBool,
    reached_notify: Notify,
    resume_notify: Notify,
}

#[cfg(all(test, feature = "cache-write-planner"))]
#[derive(Clone, Debug, Default)]
pub(crate) struct DatabaseManagerTestPauseHandle {
    state: Arc<DatabaseManagerTestPauseState>,
}

#[derive(Clone)]
pub struct ResolvedStorageOperation {
    pub(super) logical_table_name: TableName,
    pub(super) table_info: Arc<StoredTableInfo>,
    pub(super) route: Option<NamespaceRoute>,
}

pub struct ResolvedBatchGetPlan {
    pub(super) operations: HashMap<TableName, ResolvedStorageOperation>,
}

impl ResolvedBatchGetPlan {
    #[must_use]
    pub fn new(operations: Vec<ResolvedStorageOperation>) -> Self {
        Self {
            operations: operations
                .into_iter()
                .map(|operation| (operation.logical_table_name.clone(), operation))
                .collect(),
        }
    }
}

impl ResolvedStorageOperation {
    #[must_use]
    pub fn table_info(&self) -> &StoredTableInfo {
        &self.table_info
    }

    pub fn validate_key(self, key: KeyAttributes) -> StorageResult<ResolvedGetItem> {
        storage_types::validate_key_attributes_for_schema(&self.table_info.key_schema, &key)?;
        Ok(ResolvedGetItem {
            operation: self,
            key,
        })
    }

    pub(super) fn ensure_table(
        &self,
        table_name: &TableName,
        operation: &'static str,
    ) -> StorageResult<()> {
        if &self.logical_table_name != table_name {
            return Err(StorageError::internal(&format!(
                "resolved storage operation does not match {operation} table"
            )));
        }
        Ok(())
    }
}

pub struct ResolvedGetItem {
    pub(super) operation: ResolvedStorageOperation,
    pub(super) key: KeyAttributes,
}

#[cfg(all(test, feature = "cache-write-planner"))]
impl DatabaseManagerTestPauseHandle {
    #[must_use]
    pub(crate) fn armed() -> Self {
        let handle = Self::default();
        handle.arm();
        handle
    }

    pub(crate) fn arm(&self) {
        self.state.armed.store(true, AtomicOrdering::SeqCst);
        self.state.reached.store(false, AtomicOrdering::SeqCst);
    }

    pub(crate) async fn wait_until_reached(&self) {
        loop {
            if self.state.reached.load(AtomicOrdering::SeqCst) {
                return;
            }
            let notified = self.state.reached_notify.notified();
            if self.state.reached.load(AtomicOrdering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn resume(&self) {
        self.state.armed.store(false, AtomicOrdering::SeqCst);
        self.state.resume_notify.notify_waiters();
    }

    async fn maybe_pause(&self) {
        if !self.state.armed.load(AtomicOrdering::SeqCst) {
            return;
        }

        self.state.reached.store(true, AtomicOrdering::SeqCst);
        self.state.reached_notify.notify_waiters();

        loop {
            if !self.state.armed.load(AtomicOrdering::SeqCst) {
                return;
            }
            let notified = self.state.resume_notify.notified();
            if !self.state.armed.load(AtomicOrdering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) fn occ_version_expression_names() -> HashMap<String, String> {
    HashMap::from([(OCC_VERSION_NAME.to_string(), OCC_VERSION_ATTR.to_string())])
}

pub(crate) fn capped_entity_counter_key(entity_type: &str) -> HashMap<String, AttributeValue> {
    storage_types::KeyOwned::pk_sk(CAPPED_ENTITY_COUNTER_PK, entity_type).into_map()
}

pub(crate) fn capped_entity_counter_expression_names() -> HashMap<String, String> {
    HashMap::from([
        (
            CAPPED_ENTITY_COUNTER_VALUE_NAME.to_string(),
            CAPPED_ENTITY_COUNTER_VALUE_ATTR.to_string(),
        ),
        (
            CAPPED_ENTITY_COUNTER_ENTITY_TYPE_NAME.to_string(),
            storage_types::single_table_entity::ENTITY_TYPE_ATTR.to_string(),
        ),
    ])
}

pub(crate) fn capped_entity_counter_expression_values(
    delta: i64,
    entity_type: &str,
    max_value: Option<u64>,
    zero_value: Option<u64>,
) -> HashMap<String, AttributeValue> {
    let mut values = HashMap::from([
        (
            CAPPED_ENTITY_COUNTER_DELTA_VALUE.to_string(),
            AttributeValue::N(delta.to_string()),
        ),
        (
            CAPPED_ENTITY_COUNTER_ENTITY_TYPE_VALUE.to_string(),
            AttributeValue::S(entity_type.to_string()),
        ),
    ]);
    if let Some(max_value) = max_value {
        values.insert(
            CAPPED_ENTITY_COUNTER_MAX_VALUE.to_string(),
            AttributeValue::N(max_value.to_string()),
        );
    }
    if let Some(zero_value) = zero_value {
        values.insert(
            CAPPED_ENTITY_COUNTER_ZERO_VALUE.to_string(),
            AttributeValue::N(zero_value.to_string()),
        );
    }
    values
}

pub(crate) fn is_conditional_failure(error: &StorageError) -> bool {
    matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed | StorageEnum::TransactionCanceled { .. }
    )
}

pub(crate) fn transaction_canceled_reason_is_conditional(reasons: &[String], index: usize) -> bool {
    reasons
        .get(index)
        .is_some_and(|reason| reason == "ConditionalCheckFailed")
}

pub(crate) fn update_item_return_values_rewritable_from_post_image(
    return_values: Option<&ReturnValuesOldNewUpdated>,
) -> bool {
    matches!(
        return_values,
        None | Some(ReturnValuesOldNewUpdated::None)
            | Some(ReturnValuesOldNewUpdated::AllNew)
            | Some(ReturnValuesOldNewUpdated::UpdatedNew)
    )
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct PutItemInput {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option))]
    pub item: PutItemPayload,
    pub indexers: Option<Vec<String>>,

    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_values: Option<AllOld>,
    #[builder(setter(!strip_option))]
    pub return_old_on_condition_failure: bool,
    pub aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct PutItemEntityEncodeInput<'a, T> {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option, !into))]
    pub item: &'a T,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_values: Option<AllOld>,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct DeleteItemInput {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(setter(!strip_option))]
    pub key: KeyAttributes,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[builder(setter(!strip_option))]
    pub return_old_on_condition_failure: bool,
    pub aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct ScanTableInput {
    #[builder(!default)]
    pub table_name: TableName,
    #[builder(setter(strip_option))]
    pub index_name: Option<IndexName>,
    #[builder(setter(strip_option))]
    pub limit: Option<u32>,
    #[builder(setter(strip_option))]
    pub exclusive_start_key: Option<String>,
    pub consistent_read: bool,
}

#[derive(TypedBuilder, Debug)]
#[builder(field_defaults(default, setter(into)))]
pub struct QueryTableInput {
    #[builder(!default)]
    pub table_name: TableName,
    #[builder(!default)]
    pub key_condition_expression: String,
    #[builder(setter(strip_option))]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    #[builder(setter(strip_option))]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[builder(setter(strip_option))]
    pub limit: Option<u32>,
    #[builder(setter(strip_option))]
    pub exclusive_start_key: Option<String>,
    #[builder(setter(strip_option))]
    pub scan_index_forward: Option<bool>,
    pub consistent_read: bool,
}

#[derive(TypedBuilder, Debug)]
#[builder(field_defaults(default, setter(into)))]
pub struct QueryIndexInput {
    #[builder(!default)]
    pub table_name: TableName,
    #[builder(!default)]
    pub index_name: IndexName,
    #[builder(!default)]
    pub key_condition_expression: String,
    #[builder(setter(strip_option))]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    #[builder(setter(strip_option))]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[builder(setter(strip_option))]
    pub projection_expression: Option<String>,
    #[builder(setter(strip_option))]
    pub limit: Option<u32>,
    #[builder(setter(strip_option))]
    pub exclusive_start_key: Option<String>,
    #[builder(setter(strip_option))]
    pub scan_index_forward: Option<bool>,
}

impl From<QueryTableInput> for QueryTableRequest {
    fn from(input: QueryTableInput) -> Self {
        Self {
            table_name: input.table_name,
            index_name: None,
            key_condition_expression: input.key_condition_expression,
            expression_attribute_names: input.expression_attribute_names,
            expression_attribute_values: input.expression_attribute_values,
            projection_expression: None,
            limit: input.limit,
            exclusive_start_key: input.exclusive_start_key,
            scan_index_forward: input.scan_index_forward,
            consistent_read: input.consistent_read,
        }
    }
}

impl From<QueryIndexInput> for QueryTableRequest {
    fn from(input: QueryIndexInput) -> Self {
        Self {
            table_name: input.table_name,
            index_name: Some(input.index_name),
            key_condition_expression: input.key_condition_expression,
            expression_attribute_names: input.expression_attribute_names,
            expression_attribute_values: input.expression_attribute_values,
            projection_expression: input.projection_expression,
            limit: input.limit,
            exclusive_start_key: input.exclusive_start_key,
            scan_index_forward: input.scan_index_forward,
            consistent_read: false,
        }
    }
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct UpdateItemInput {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option))]
    pub key: KeyAttributes,
    #[builder(!default, setter(!strip_option))]
    pub update_expression: String,
    pub indexers: Option<Vec<String>>,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_values: Option<ReturnValuesOldNewUpdated>,
    #[builder(setter(!strip_option))]
    pub return_old_on_condition_failure: bool,
    pub aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
}

fn entity_indexer_names<T>() -> Vec<String>
where T: storage_types::single_table_entity::SingleTableEntity {
    T::INDEXERS
        .iter()
        .map(|indexer| indexer.attribute_name().to_string())
        .collect()
}

#[derive(Debug)]
pub enum CappedStorageError {
    CapacityExceededError,
    ItemExistError,
    ItemNotExistsError,
    StorageError(StorageError),
}

impl std::fmt::Display for CappedStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapacityExceededError => f.write_str("entity capacity exceeded"),
            Self::ItemExistError => f.write_str("item already exists"),
            Self::ItemNotExistsError => f.write_str("item does not exist"),
            Self::StorageError(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for CappedStorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StorageError(error) => Some(error),
            _ => None,
        }
    }
}

impl From<StorageError> for CappedStorageError {
    fn from(value: StorageError) -> Self {
        Self::StorageError(value)
    }
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct CreateCappedEntityInput<'a, T> {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option, !into))]
    pub item: &'a T,
    #[builder(!default, setter(!strip_option))]
    pub counted_entity_type: String,
    #[builder(!default, setter(!strip_option))]
    pub max_value: u64,
    pub additional_transact_items: Vec<TransactEncodeItem>,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct DeleteCappedEntityInput {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option))]
    pub key: KeyAttributes,
    #[builder(!default, setter(!strip_option))]
    pub counted_entity_type: String,
}

pub struct DatabaseManager {
    pub(super) storage: Arc<dyn DatabaseTrait>,
    pub(super) background_storage: Option<Arc<dyn DatabaseTrait>>,
    pub(super) queue_provider: Option<Arc<dyn queue_provider::QueueProvider>>,
    pub(super) pubsub_provider: Option<Arc<dyn pubsub_provider::PubsubProvider>>,
    pub(super) connection_registry: Option<HashMap<String, Arc<dyn DatabaseTrait>>>,
    pub(super) admission_registry: AdmissionRegistry,
    pub(super) route_resolver: Option<Arc<NamespaceRouteResolver>>,
    pub(super) request_rewriter: NamespaceRequestRewriter,
    pub(super) single_node_sync_mode: bool,
    pub(super) single_table_mode: bool,
    pub(super) cache_services: StorageCacheServices,
    pub(super) cutover_watcher_task: Option<JoinHandle<()>>,
    pub(super) run_gsi_maintenance: bool,
    #[cfg(all(test, feature = "cache-write-planner"))]
    pub(super) pause_after_storage_write: Option<DatabaseManagerTestPauseHandle>,
    pub(super) supports_multi_region_replication_control_plane: bool,
    pub(super) supports_read_sequence_mapped_range: bool,
    pub(super) read_sequence_capabilities: ReadSequenceProviderCapabilities,
    pub(super) table_info_cache: RwLock<HashMap<TableName, Arc<StoredTableInfo>>>,
}

/// Provider handle which cannot be used without observing its outcome.
pub struct AdmittedProvider {
    provider: Arc<dyn DatabaseTrait>,
    permit: AdmissionPermit,
}

struct AdmittedReadContext {
    context: Box<dyn StorageProviderReadContext>,
    provider: Arc<dyn DatabaseTrait>,
    permit: Option<AdmissionPermit>,
    started: std::time::Instant,
    observation: std::sync::Mutex<ReadContextObservation>,
}

#[derive(Default)]
struct ReadContextObservation {
    had_failure: bool,
    had_pressure: bool,
}

/// Drain connection-wide provider pressure when an admitted future is
/// cancelled before it reaches its normal outcome boundary.  Remote retry
/// markers are deliberately connection-scoped, so leaving one behind would
/// misclassify the next request on the same connection.
struct ProviderPressureDrainGuard {
    provider: Arc<dyn DatabaseTrait>,
}

impl Drop for ProviderPressureDrainGuard {
    fn drop(&mut self) {
        let _ = self.provider.take_admission_pressure_signal();
    }
}

impl AdmittedReadContext {
    fn observe_result<T>(&self, result: &StorageResult<T>) {
        let mut observation = self
            .observation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if result.is_err() {
            observation.had_failure = true;
        }
        if self.provider.take_admission_pressure_signal()
            || result.as_ref().is_err_and(is_admission_pressure)
        {
            observation.had_pressure = true;
        }
    }
}

#[async_trait::async_trait]
impl StorageProviderReadContext for AdmittedReadContext {
    fn take_retryable_read_failure(&self) -> bool {
        self.context.take_retryable_read_failure()
    }

    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        let result = self
            .context
            .get_item(table_name, key, consistent_read)
            .await;
        self.observe_result(&result);
        result
    }

    async fn batch_get_item(
        &self,
        request: storage_types::BatchGetItemRequest,
    ) -> StorageResult<storage_types::BatchGetWireItemResponse> {
        let result = self.context.batch_get_item(request).await;
        self.observe_result(&result);
        result
    }

    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let result = self.context.query_table(request).await;
        self.observe_result(&result);
        result
    }
}

impl Drop for AdmittedReadContext {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        let mut observation = self
            .observation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        observation.had_pressure |= self.provider.take_admission_pressure_signal();
        let latency = self.started.elapsed();
        let outcome = if observation.had_pressure {
            AdmissionOutcome::RetryablePressure(latency)
        } else if observation.had_failure {
            AdmissionOutcome::Failure(latency)
        } else {
            AdmissionOutcome::Success(latency)
        };
        permit.complete(outcome);
    }
}

#[derive(Clone, Copy)]
pub(super) enum AdmissionLane {
    Foreground(AdmissionClass),
    Control,
}

impl AdmittedProvider {
    pub async fn run<F, Fut, T>(self, operation: F) -> StorageResult<T>
    where
        F: FnOnce(&dyn StorageProvider) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>>,
    {
        let provider = Arc::clone(&self.provider);
        let _pressure_guard = ProviderPressureDrainGuard {
            provider: Arc::clone(&provider),
        };
        let started = std::time::Instant::now();
        let result = operation(provider.as_ref()).await;
        let latency = started.elapsed();
        let provider_pressure = provider.take_admission_pressure_signal();
        let outcome = match &result {
            Ok(_) if provider_pressure => AdmissionOutcome::SuccessAfterPressure(latency),
            Ok(_) => AdmissionOutcome::Success(latency),
            Err(error) if provider_pressure || is_admission_pressure(error) => {
                AdmissionOutcome::RetryablePressure(latency)
            }
            Err(_) => AdmissionOutcome::Failure(latency),
        };
        self.permit.complete(outcome);
        result
    }

    /// Run a provider operation which needs the complete database trait (for
    /// example a stream or read-context call) while retaining the same
    /// admission accounting as [`Self::run`].
    pub(crate) async fn run_database<F, Fut, T>(self, operation: F) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn DatabaseTrait>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>>,
    {
        let provider = Arc::clone(&self.provider);
        let _pressure_guard = ProviderPressureDrainGuard {
            provider: Arc::clone(&provider),
        };
        let started = std::time::Instant::now();
        let result = operation(Arc::clone(&provider)).await;
        let latency = started.elapsed();
        let provider_pressure = provider.take_admission_pressure_signal();
        let outcome = match &result {
            Ok(_) if provider_pressure => AdmissionOutcome::SuccessAfterPressure(latency),
            Ok(_) => AdmissionOutcome::Success(latency),
            Err(error) if provider_pressure || is_admission_pressure(error) => {
                AdmissionOutcome::RetryablePressure(latency)
            }
            Err(_) => AdmissionOutcome::Failure(latency),
        };
        self.permit.complete(outcome);
        result
    }

    /// Run a stream-provider operation while retaining admission accounting.
    ///
    /// Stream operations use their own error type, so the provider pressure
    /// signal is the only backend-neutral overload classification available at
    /// this boundary. The permit still covers exactly the stream future.
    pub async fn run_stream<F, Fut, T, E>(self, operation: F) -> Result<T, E>
    where
        F: FnOnce(Arc<dyn StreamProvider>) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let provider = Arc::clone(&self.provider);
        let _pressure_guard = ProviderPressureDrainGuard {
            provider: Arc::clone(&provider),
        };
        let started = std::time::Instant::now();
        let result = operation(Arc::clone(&provider) as Arc<dyn StreamProvider>).await;
        let latency = started.elapsed();
        let provider_pressure = provider.take_admission_pressure_signal();
        let outcome = match (&result, provider_pressure) {
            (Ok(_), true) => AdmissionOutcome::SuccessAfterPressure(latency),
            (Ok(_), false) => AdmissionOutcome::Success(latency),
            (Err(_), true) => AdmissionOutcome::RetryablePressure(latency),
            (Err(_), false) => AdmissionOutcome::Failure(latency),
        };
        self.permit.complete(outcome);
        result
    }
}

#[cfg(feature = "cache-write-planner")]
impl StorageCachePlannerLoad for DatabaseManager {
    async fn get_table_info_for_cache(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        Ok(self.get_table_info_arc(table_name).await?.as_ref().clone())
    }

    async fn get_table_info_with_pending_for_cache(
        &self,
        table_name: &TableName,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<StoredTableInfo> {
        self.get_table_info_with_pending(table_name, pending_routes)
            .await
    }

    async fn get_item_map_with_resolved_operation_for_cache(
        &self,
        operation: &ResolvedStorageOperation,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let item = self
            .get_item_with_resolved_operation(operation.clone().validate_key(key)?, consistent_read)
            .await?;
        item.map(WireItem::into_attribute_map).transpose()
    }

    async fn get_item_map_with_consistent_read_for_cache(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        self.get_item_map_with_consistent_read(table_name, key, consistent_read)
            .await
    }

    async fn get_item_map_with_consistent_read_with_pending_for_cache(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        self.get_item_map_with_consistent_read_with_pending(
            table_name,
            key,
            consistent_read,
            pending_routes,
        )
        .await
    }
}

impl StorageCacheQueryRuntimeLoad for DatabaseManager {
    async fn get_table_info_for_cache_query(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        Ok(self.get_table_info_arc(table_name).await?.as_ref().clone())
    }

    async fn get_item_with_consistent_read_for_cache_query(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        self.get_item_with_consistent_read(table_name, key, consistent_read)
            .await
    }
}

impl DatabaseManager {
    #[cfg(feature = "cache-write-planner")]
    pub(super) fn cache_write_planner(&self) -> StorageCacheWritePlanner<'_, Self> {
        StorageCacheWritePlanner::new(self, self.cache_services.query_proof_enabled())
    }

    pub(super) const fn single_table_mode_enabled(&self) -> bool {
        self.single_table_mode
    }

    pub(super) const fn single_node_sync_mode_enabled(&self) -> bool {
        self.single_node_sync_mode
    }

    #[cfg(feature = "cache-write-planner")]
    pub(super) fn cache_write_effects_enabled(&self) -> bool {
        self.cache_services.point_read_enabled() || self.cache_services.query_proof_enabled()
    }

    pub(super) fn empty_cache_write_effects(&self) -> storage_cache::RuntimeWriteEffects {
        storage_cache::RuntimeWriteEffects {
            point_read: Vec::new(),
            query_proof: Vec::new(),
        }
    }

    pub(super) fn cache_query_runtime(&self) -> StorageCacheQueryRuntime<'_, Self> {
        StorageCacheQueryRuntime::new(&self.cache_services, self)
    }

    pub(super) fn cache_batch_get_runtime(&self) -> StorageBatchGetCacheRuntime<'_> {
        StorageBatchGetCacheRuntime::new(&self.cache_services)
    }

    pub(super) fn cache_point_read_runtime(&self) -> StoragePointReadCacheRuntime<'_> {
        StoragePointReadCacheRuntime::new(&self.cache_services)
    }

    pub(crate) const fn multi_region_replication_unsupported_message() -> &'static str {
        "storage replication is not supported with remote storage"
    }

    pub(crate) const fn supports_multi_region_replication_control_plane(&self) -> bool {
        self.supports_multi_region_replication_control_plane
    }

    pub(crate) fn default_supports_guarded_writes(&self) -> bool {
        self.storage.supports_guarded_writes()
    }

    pub(crate) fn default_supports_guarded_transaction_writes(&self) -> bool {
        self.storage.supports_guarded_transaction_writes()
    }

    pub const fn read_sequence_capabilities(&self) -> ReadSequenceProviderCapabilities {
        self.read_sequence_capabilities
    }

    pub fn supports_read_sequence_mapped_range(&self) -> bool {
        self.supports_read_sequence_mapped_range
    }

    pub async fn begin_read_sequence_read_context(
        &self,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<Box<dyn StorageProviderReadContext>> {
        let admitted = self
            .admit_provider(ROUTED_DEFAULT_CONNECTION_ID, AdmissionClass::RangeRead)
            .await?;
        let started = std::time::Instant::now();
        let provider = Arc::clone(&admitted.provider);
        let _pressure_guard = ProviderPressureDrainGuard {
            provider: Arc::clone(&provider),
        };
        let database_call = metrics_facade::begin_database_call("read_sequence.begin_context");
        let result = provider.begin_read_sequence_read_context(consistency).await;
        drop(database_call);
        match result {
            Ok(context) => Ok(Box::new(AdmittedReadContext {
                context,
                provider,
                permit: Some(admitted.permit),
                started,
                observation: std::sync::Mutex::new(ReadContextObservation::default()),
            })),
            Err(error) => {
                let latency = started.elapsed();
                let outcome =
                    if provider.take_admission_pressure_signal() || is_admission_pressure(&error) {
                        AdmissionOutcome::RetryablePressure(latency)
                    } else {
                        AdmissionOutcome::Failure(latency)
                    };
                admitted.permit.complete(outcome);
                Err(error)
            }
        }
    }

    pub(crate) fn ensure_multi_region_replication_control_plane_supported(
        &self,
    ) -> StorageResult<()> {
        if self.supports_multi_region_replication_control_plane() {
            return Ok(());
        }

        Err(StorageError::validation(
            Self::multi_region_replication_unsupported_message(),
        ))
    }

    #[cfg(all(test, feature = "cache-write-planner"))]
    pub(super) async fn maybe_pause_after_storage_write_for_test(&self) {
        if let Some(handle) = self.pause_after_storage_write.as_ref() {
            handle.maybe_pause().await;
        }
    }

    #[cfg(not(all(test, feature = "cache-write-planner")))]
    pub(super) async fn maybe_pause_after_storage_write_for_test(&self) {}

    pub(super) async fn maybe_create_sys_storage_replication_table(&self) -> StorageResult<()> {
        if !self.supports_multi_region_replication_control_plane() {
            return Ok(());
        }

        Tables::create_sys_storage_replication_table(self).await
    }

    #[must_use]
    pub(crate) fn route_resolver(&self) -> Option<Arc<NamespaceRouteResolver>> {
        self.route_resolver.clone()
    }

    /// Resolve a namespace route through the manager-owned admission boundary.
    /// Cache-only hits stay off the provider path; misses acquire a point-read
    /// permit before the resolver can fetch routing metadata.
    pub async fn resolve_namespace_route(
        &self,
        namespace: &TableNamespace,
    ) -> StorageResult<NamespaceRoute> {
        let resolver = self.route_resolver.clone().ok_or_else(|| {
            StorageError::validation("namespace routing is not enabled for this database")
        })?;
        if let Some(route) = resolver.cached_route(namespace).await? {
            return Ok(route);
        }
        let namespace = namespace.clone();
        self.run_default_admitted(AdmissionClass::PointRead, move |_provider| async move {
            resolver.resolve_route(&namespace).await
        })
        .await
    }

    /// Seed a freshly committed single-location route in the manager's local
    /// cache. This is a cache-maintenance operation and performs no provider
    /// I/O; future misses still resolve through `resolve_namespace_route`.
    pub fn seed_namespace_route_for_cache(
        &self,
        namespace: TableNamespace,
        storage_mode: NamespaceStorageMode,
        loc: u16,
    ) {
        if let Some(resolver) = self.route_resolver.as_ref() {
            resolver.seed_single_route(namespace, storage_mode, loc);
        }
    }

    /// Invalidate one namespace route after its metadata commit. The next
    /// route resolution is admitted before it reads the provider.
    pub fn invalidate_namespace_route_cache(&self, namespace: &TableNamespace) {
        if let Some(resolver) = self.route_resolver.as_ref() {
            resolver.invalidate_namespace(namespace);
        }
    }

    /// Invalidate a shared-location descriptor after control-plane metadata
    /// changes. This only changes the local cache.
    pub fn invalidate_location_route_cache(&self, loc: u16) {
        if let Some(resolver) = self.route_resolver.as_ref() {
            resolver.invalidate_location(loc);
        }
    }

    #[cfg(test)]
    pub(crate) fn provider_for_connection_for_migration(
        &self,
        connection_id: &str,
    ) -> StorageResult<Arc<dyn DatabaseTrait>> {
        self.provider_for_connection(connection_id)
    }

    fn admission_controller_for_connection(
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

    /// Acquire a foreground permit for a named physical connection.  Provider
    /// operations can use this seam while their individual call sites move to
    /// admitted handles; no raw provider accessor is exposed here.
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

    pub async fn admit_provider(
        &self,
        connection_id: &str,
        class: AdmissionClass,
    ) -> StorageResult<AdmittedProvider> {
        let permit = self.acquire_admission(connection_id, class).await?;
        let provider = self.provider_for_connection(connection_id)?;
        Ok(AdmittedProvider { provider, permit })
    }

    /// Acquire the default connection for a stream operation. Keeping this
    /// acquisition at the manager boundary prevents route code from reaching
    /// into an unobserved stream provider handle.
    pub async fn admit_default_provider(
        &self,
        class: AdmissionClass,
    ) -> StorageResult<AdmittedProvider> {
        self.admit_provider(ROUTED_DEFAULT_CONNECTION_ID, class)
            .await
    }

    /// Execute one foreground provider future under the named connection's
    /// admission controller.  Keeping this helper at the manager boundary
    /// makes the permit's lifetime match the actual provider future rather
    /// than request parsing, cache work, or response encoding.
    pub(super) async fn run_admitted<F, Fut, T>(
        &self,
        connection_id: &str,
        class: AdmissionClass,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn DatabaseTrait>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>>,
    {
        self.run_admitted_lane(connection_id, AdmissionLane::Foreground(class), operation)
            .await
    }

    pub(super) async fn run_default_admitted<F, Fut, T>(
        &self,
        class: AdmissionClass,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn DatabaseTrait>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>>,
    {
        self.run_admitted(ROUTED_DEFAULT_CONNECTION_ID, class, operation)
            .await
    }

    /// Execute a provider-owned optimized read-sequence plan under the
    /// default connection's range-read admission lane. The provider handle
    /// remains private to the manager, so callers cannot bypass observation.
    pub async fn execute_read_sequence_plan_with_budget(
        &self,
        plan: &storage_types::ReadSequencePlan,
        consistency: ReadSequenceConsistency,
        continuation: Option<&str>,
        budget: ReadSequenceExecutionBudget,
    ) -> StorageResult<ReadSequenceExecution> {
        self.run_default_admitted(AdmissionClass::RangeRead, |provider| async move {
            let database_call = metrics_facade::begin_database_call("read_sequence.plan");
            let result = provider
                .execute_read_sequence_plan_with_budget(plan, consistency, continuation, budget)
                .await;
            drop(database_call);
            result
        })
        .await
    }

    /// Read change-index markers through the same bounded range-read lane as
    /// other provider reads.
    pub async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<storage_provider::ChangeIndexMarker>> {
        self.run_default_admitted(AdmissionClass::RangeRead, |provider| async move {
            let database_call = metrics_facade::begin_database_call("list_change_index_markers");
            let result = provider.list_change_index_markers(request).await;
            drop(database_call);
            result
        })
        .await
    }

    /// Run a provider-context future (where the context itself owns the
    /// backend handle) under the same admission accounting as direct calls.
    pub(super) async fn run_admitted_operation<F, Fut, T>(
        &self,
        connection_id: &str,
        class: AdmissionClass,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>>,
    {
        let permit = self.acquire_admission(connection_id, class).await?;
        let provider = self.provider_for_connection(connection_id)?;
        let _pressure_guard = ProviderPressureDrainGuard {
            provider: Arc::clone(&provider),
        };
        let started = std::time::Instant::now();
        let result = operation().await;
        let latency = started.elapsed();
        let provider_pressure = provider.take_admission_pressure_signal();
        let outcome = match &result {
            Ok(_) if provider_pressure => AdmissionOutcome::SuccessAfterPressure(latency),
            Ok(_) => AdmissionOutcome::Success(latency),
            Err(error) if provider_pressure || is_admission_pressure(error) => {
                AdmissionOutcome::RetryablePressure(latency)
            }
            Err(_) => AdmissionOutcome::Failure(latency),
        };
        permit.complete(outcome);
        result
    }

    pub(crate) async fn run_control_admitted<F, Fut, T>(
        &self,
        connection_id: &str,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn DatabaseTrait>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>>,
    {
        self.run_admitted_lane(connection_id, AdmissionLane::Control, operation)
            .await
    }

    async fn run_admitted_lane<F, Fut, T>(
        &self,
        connection_id: &str,
        lane: AdmissionLane,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnOnce(Arc<dyn DatabaseTrait>) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>>,
    {
        match lane {
            AdmissionLane::Foreground(class) => {
                self.admit_provider(connection_id, class)
                    .await?
                    .run_database(operation)
                    .await
            }
            AdmissionLane::Control => {
                let controller = self.admission_controller_for_connection(connection_id)?;
                let permit = controller.try_acquire_control().map_err(|rejection| {
                    StorageError::service_unavailable(rejection.retry_after_seconds)
                })?;
                let provider = self.provider_for_connection(connection_id)?;
                let _pressure_guard = ProviderPressureDrainGuard {
                    provider: Arc::clone(&provider),
                };
                // Provider pressure signals are connection-wide.  Drain all
                // signals at the control boundary so background retries cannot
                // be misattributed to the next foreground request.  Control
                // work deliberately does not update foreground goodput or
                // latency baselines, but its pressure remains observable.
                let result = operation(Arc::clone(&provider)).await;
                let provider_pressure = provider.take_admission_pressure_signal();
                let error_pressure = result.as_ref().is_err_and(is_admission_pressure);
                controller.record_control_pressure(provider_pressure, error_pressure);
                drop(permit);
                result
            }
        }
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

    /// Reserve the default connection's control lane for a background worker
    /// that owns its provider call sequence outside the manager facade.
    pub fn acquire_default_control_permit(&self) -> StorageResult<crate::ControlPermit> {
        self.admission_registry
            .default_controller()
            .try_acquire_control()
            .map_err(|rejection| StorageError::service_unavailable(rejection.retry_after_seconds))
    }

    pub(super) fn provider_for_connection(
        &self,
        connection_id: &str,
    ) -> StorageResult<Arc<dyn DatabaseTrait>> {
        if let Some(registry) = &self.connection_registry {
            if let Some(provider) = registry.get(connection_id) {
                return Ok(Arc::clone(provider));
            }
            // Keep the synthetic route id usable when the configured
            // physical default has a name other than `default`.  The manager
            // already stores that provider separately; this fallback therefore
            // cannot accidentally select a secondary connection.
            if connection_id == ROUTED_DEFAULT_CONNECTION_ID {
                return Ok(Arc::clone(&self.storage));
            }
            return Err(StorageError::validation(format!(
                "storage connection '{connection_id}' is not configured"
            )));
        }
        Ok(Arc::clone(&self.storage))
    }

    pub(super) async fn resolve_namespace_route_for_table(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<NamespaceRoute>> {
        let Some(route_resolver) = self.route_resolver.as_ref() else {
            return Ok(None);
        };
        reject_direct_shared_table_access(table_name)?;
        if !is_shared_table_enabled_namespace_route(table_name) {
            return Ok(None);
        }
        let Some(namespace) = Tables::parse_namespace_table_name(table_name) else {
            return Ok(None);
        };
        if let Some(route) = route_resolver.cached_route(&namespace).await? {
            return Ok(Some(route));
        }
        let route_resolver = Arc::clone(route_resolver);
        match self
            .run_default_admitted(AdmissionClass::PointRead, move |_provider| async move {
                route_resolver.resolve_route(&namespace).await
            })
            .await
        {
            Ok(route) => Ok(Some(route)),
            Err(error)
                if matches!(
                    error.to_enum(),
                    StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn resolve_namespace_route_for_table_with_pending(
        &self,
        table_name: &TableName,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Option<NamespaceRoute>> {
        if let Some(route) = self.resolve_namespace_route_for_table(table_name).await? {
            return Ok(Some(route));
        }
        let Some(route_resolver) = self.route_resolver.as_ref() else {
            return Ok(None);
        };
        if !is_shared_table_enabled_namespace_route(table_name) {
            return Ok(None);
        }
        let Some(namespace) = Tables::parse_namespace_table_name(table_name) else {
            return Ok(None);
        };
        let Some(route_record) = pending_routes.get(&namespace) else {
            return Ok(None);
        };
        if let Some(route) = route_resolver
            .cached_route_for_record(&namespace, route_record)
            .await?
        {
            return Ok(Some(route));
        }
        let route_record = route_record.clone();
        self.run_default_admitted(AdmissionClass::PointRead, move |_provider| async move {
            route_resolver
                .route_for_record(&namespace, &route_record)
                .await
        })
        .await
        .map(Some)
    }

    #[cfg(feature = "cache-write-planner")]
    pub(super) async fn get_table_info_with_pending(
        &self,
        table_name: &TableName,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<StoredTableInfo> {
        Ok(self
            .get_table_info_arc_with_pending(table_name, pending_routes)
            .await?
            .as_ref()
            .clone())
    }

    #[cfg(feature = "cache-write-planner")]
    pub(crate) async fn get_table_info_arc_with_pending(
        &self,
        table_name: &TableName,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Arc<StoredTableInfo>> {
        if let Some(route) = self
            .resolve_namespace_route_for_table_with_pending(table_name, pending_routes)
            .await?
        {
            let table_info = self
                .run_admitted(
                    &route.read_target.connection_id,
                    AdmissionClass::PointRead,
                    |provider| async move {
                        record_storage_operation(
                            "get_table_info",
                            provider.get_table_info(&route.read_target.table_name),
                        )
                        .await
                    },
                )
                .await;
            return Ok(Arc::new(table_info?));
        }
        self.get_table_info_arc(table_name).await
    }

    #[cfg(feature = "cache-write-planner")]
    pub(super) async fn get_item_map_with_consistent_read_with_pending(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        if let Some(route) = self
            .resolve_namespace_route_for_table_with_pending(&table_name, pending_routes)
            .await?
        {
            let mut routed_key = key;
            if route.storage_mode == NamespaceStorageMode::SharedTable {
                self.request_rewriter
                    .rewrite_key_for_shared_table(&route.namespace, &mut routed_key)?;
            }
            let mut item = self
                .run_admitted(
                    &route.read_target.connection_id,
                    AdmissionClass::PointRead,
                    |provider| async move {
                        record_storage_operation(
                            "get_item",
                            provider.get_item(
                                route.read_target.table_name.clone(),
                                routed_key,
                                consistent_read,
                            ),
                        )
                        .await
                    },
                )
                .await?;
            if route.storage_mode == NamespaceStorageMode::SharedTable
                && let Some(item_ref) = item.as_mut()
            {
                self.request_rewriter
                    .normalize_wire_item_from_shared_table(&route.namespace, item_ref)?;
            }
            return item.map(WireItem::into_attribute_map).transpose();
        }
        self.get_item_map_with_consistent_read(table_name, key, consistent_read)
            .await
    }

    pub(super) async fn invalidate_table_info_cache_for_targets(&self, targets: &[RouteTarget]) {
        if targets.is_empty() {
            return;
        }
        let mut write = self.table_info_cache.write().await;
        for table_name in targets.iter().map(|target| &target.table_name) {
            write.remove(table_name);
        }
    }

    pub(super) fn pending_namespace_routes_from_transact_items(
        transact_items: &[TransactEncodeItem],
    ) -> StorageResult<HashMap<TableNamespace, NamespaceRouteRecord>> {
        let mut pending_routes = HashMap::new();
        for item in transact_items {
            let Some(put) = item.put.as_ref() else {
                continue;
            };
            if put.table_name != Tables::sys_namespaces() {
                continue;
            }
            let map = put.item.item().to_attribute_map()?;
            let Some((namespace, route_record)) = parse_namespace_route_record(map)? else {
                continue;
            };
            pending_routes.insert(namespace, route_record);
        }
        Ok(pending_routes)
    }

    pub(super) fn pending_namespace_routes_from_transact_write_items(
        transact_items: &[storage_types::TransactWriteItem],
    ) -> StorageResult<HashMap<TableNamespace, NamespaceRouteRecord>> {
        let mut pending_routes = HashMap::new();
        for item in transact_items {
            let Some(put) = item.put.as_ref() else {
                continue;
            };
            if put.table_name != Tables::sys_namespaces() {
                continue;
            }
            let Some((namespace, route_record)) = parse_namespace_route_record(put.item.clone())?
            else {
                continue;
            };
            pending_routes.insert(namespace, route_record);
        }
        Ok(pending_routes)
    }

    pub(super) async fn maybe_run_gsi_maintenance_for_target(
        &self,
        target: &RouteTarget,
    ) -> StorageResult<()> {
        if !self.run_gsi_maintenance {
            return Ok(());
        }
        let _ = self
            .run_control_admitted(&target.connection_id, |provider| async move {
                let database_call = metrics_facade::begin_database_call("gsi_maintenance");
                let result = provider.run_job(GSI_UPDATE_JOB).await;
                drop(database_call);
                result
            })
            .await;
        Ok(())
    }

    pub(super) async fn maybe_run_gsi_maintenance_for_connection(
        &self,
        connection_id: &str,
    ) -> StorageResult<()> {
        if !self.run_gsi_maintenance {
            return Ok(());
        }
        let _ = self
            .run_control_admitted(connection_id, |provider| async move {
                let database_call = metrics_facade::begin_database_call("gsi_maintenance");
                let result = provider.run_job(GSI_UPDATE_JOB).await;
                drop(database_call);
                result
            })
            .await;
        Ok(())
    }

    pub(super) async fn maybe_run_gsi_maintenance_default(&self) {
        if !self.run_gsi_maintenance {
            return;
        }
        let _ = self
            .run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, |provider| async move {
                let database_call = metrics_facade::begin_database_call("gsi_maintenance");
                let result = provider.run_job(GSI_UPDATE_JOB).await;
                drop(database_call);
                result
            })
            .await;
    }

    pub(super) async fn maybe_run_gsi_maintenance(&self) {
        self.maybe_run_gsi_maintenance_default().await;
    }

    // Shared-table routing can fan out one logical write into multiple
    // physical targets during dual-write migrations. This helper centralizes
    // pause checks, per-target execution, and cache/GSI bookkeeping so the
    // call sites can stay focused on request shaping (happy-path-left).
    pub(super) async fn execute_routed_write_targets<T, F, Fut>(
        &self,
        route: &NamespaceRoute,
        lane: AdmissionLane,
        empty_targets_error: &'static str,
        mut execute: F,
    ) -> StorageResult<T>
    where
        F: FnMut(Arc<dyn DatabaseTrait>, &RouteTarget, usize, RoutedWriteTargetRole) -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>>,
    {
        ensure_route_writes_not_paused(route)?;

        let mut primary_response: Option<T> = None;
        for (index, target) in route.write_targets.iter().enumerate() {
            let target_role = RoutedWriteTargetRole::for_index(index);
            let response = self
                .run_admitted_lane(&target.connection_id, lane, |provider| {
                    execute(provider, target, index, target_role)
                })
                .await?;
            if primary_response.is_none() {
                primary_response = Some(response);
            }
            self.maybe_pause_after_storage_write_for_test().await;
            self.maybe_run_gsi_maintenance_for_target(target).await?;
        }
        self.invalidate_table_info_cache_for_targets(&route.write_targets)
            .await;

        primary_response.ok_or_else(|| StorageError::internal(empty_targets_error))
    }

    pub async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        self.run_default_admitted(AdmissionClass::Write, |provider| async move {
            record_storage_operation("create_table", provider.create_table(request)).await
        })
        .await?;
        self.table_info_cache
            .write()
            .await
            .remove(&request.table_name);
        self.cache_query_runtime()
            .invalidate_table(&request.table_name)
            .await?;
        Ok(())
    }

    pub async fn create_table_on_connection(
        &self,
        connection_id: &str,
        request: &CreateTableRequest,
    ) -> StorageResult<()> {
        self.run_admitted(
            connection_id,
            AdmissionClass::Write,
            |provider| async move {
                record_storage_operation("create_table", provider.create_table(request)).await
            },
        )
        .await?;
        self.table_info_cache
            .write()
            .await
            .remove(&request.table_name);
        self.cache_query_runtime()
            .invalidate_table(&request.table_name)
            .await?;
        Ok(())
    }

    pub async fn get_table_info(&self, table_name: &TableName) -> StorageResult<StoredTableInfo> {
        Ok(self.get_table_info_arc(table_name).await?.as_ref().clone())
    }

    /// Read physical table metadata through the reserved control lane.
    ///
    /// This is for bounded control-plane work which must address a physical
    /// table directly (for example, a shared tenant table) and therefore
    /// cannot use the logical namespace-routing path used by
    /// [`Self::get_table_info`].  Foreground request code must use
    /// [`Self::get_table_info`] instead.
    pub async fn get_table_info_for_control(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        self.run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, |provider| async move {
            record_storage_operation(
                "get_table_info_for_control",
                provider.get_table_info(table_name),
            )
            .await
        })
        .await
    }

    /// Load the physical table metadata referenced by an internal system
    /// stream pointer. System streams contain physical source-table names,
    /// including shared-table names that must never be accepted as direct
    /// public CRUD targets, so this boundary intentionally skips logical
    /// namespace routing while retaining the default connection's admission
    /// lane.
    pub async fn get_stream_source_table_info(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        self.run_default_admitted(AdmissionClass::PointRead, |provider| async move {
            record_storage_operation(
                "get_stream_source_table_info",
                provider.get_table_info(table_name),
            )
            .await
        })
        .await
    }

    pub async fn resolve_storage_operation(
        &self,
        table_name: TableName,
    ) -> StorageResult<ResolvedStorageOperation> {
        let route = self.resolve_namespace_route_for_table(&table_name).await?;
        let table_info = if let Some(route) = route.as_ref() {
            Arc::new(
                self.run_admitted(
                    &route.read_target.connection_id,
                    AdmissionClass::PointRead,
                    |provider| async move {
                        record_storage_operation(
                            "get_table_info",
                            provider.get_table_info(&route.read_target.table_name),
                        )
                        .await
                    },
                )
                .await?,
            )
        } else {
            self.get_unrouted_table_info_arc(&table_name).await?
        };
        Ok(ResolvedStorageOperation {
            logical_table_name: table_name,
            table_info,
            route,
        })
    }

    pub(crate) async fn get_table_info_arc(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Arc<StoredTableInfo>> {
        Ok(self
            .resolve_storage_operation(table_name.clone())
            .await?
            .table_info)
    }

    async fn get_unrouted_table_info_arc(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Arc<StoredTableInfo>> {
        if let Some(cached) = self.table_info_cache.read().await.get(table_name).cloned()
            && cached.as_ref().table_status == TableStatus::Active
        {
            return Ok(cached);
        }

        let info = self
            .run_default_admitted(AdmissionClass::PointRead, |provider| async move {
                record_storage_operation("get_table_info", provider.get_table_info(table_name))
                    .await
            })
            .await?;
        let info = Arc::new(info);
        let mut cache = self.table_info_cache.write().await;
        if info.as_ref().table_status == TableStatus::Active {
            cache.insert(table_name.clone(), Arc::clone(&info));
        } else {
            cache.remove(table_name);
        }
        Ok(info)
    }

    pub async fn get_table_key_schema(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Vec<KeySchemaElement>> {
        Ok(self
            .get_table_info_arc(table_name)
            .await?
            .key_schema
            .clone())
    }

    pub async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        if let Some(route) = self.resolve_namespace_route_for_table(table_name).await? {
            return self
                .run_admitted(
                    &route.read_target.connection_id,
                    AdmissionClass::PointRead,
                    |provider| async move {
                        record_storage_operation(
                            "table_exists",
                            provider.table_exists(&route.read_target.table_name),
                        )
                        .await
                    },
                )
                .await;
        }
        self.run_default_admitted(AdmissionClass::PointRead, |provider| async move {
            record_storage_operation("table_exists", provider.table_exists(table_name)).await
        })
        .await
    }

    pub async fn update_table_status(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()> {
        if let Some(route) = self.resolve_namespace_route_for_table(table_name).await? {
            let target_table_name = route.read_target.table_name.clone();
            self.run_admitted(
                &route.read_target.connection_id,
                AdmissionClass::Write,
                |provider| async move {
                    record_storage_operation(
                        "update_table_status",
                        provider.update_table_status(&target_table_name, status),
                    )
                    .await
                },
            )
            .await?;
            self.invalidate_table_info_cache_for_targets(std::slice::from_ref(&route.read_target))
                .await;
            self.cache_query_runtime()
                .invalidate_table(table_name)
                .await?;
            return Ok(());
        }
        self.run_default_admitted(AdmissionClass::Write, |provider| async move {
            record_storage_operation(
                "update_table_status",
                provider.update_table_status(table_name, status),
            )
            .await
        })
        .await?;
        self.table_info_cache.write().await.remove(table_name);
        self.cache_query_runtime()
            .invalidate_table(table_name)
            .await?;
        Ok(())
    }

    pub async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        self.run_default_admitted(AdmissionClass::Write, |provider| async move {
            record_storage_operation("update_time_to_live", provider.update_time_to_live(request))
                .await
        })
        .await
    }

    pub async fn describe_time_to_live(
        &self,
        table_name: &TableName,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        self.run_default_admitted(AdmissionClass::PointRead, |provider| async move {
            record_storage_operation(
                "describe_time_to_live",
                provider.describe_time_to_live(table_name),
            )
            .await
        })
        .await
    }

    pub async fn list_tables(
        &self,
        limit: Option<u32>,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<(Vec<StoredTableInfo>, Option<TableName>)> {
        let tables = self
            .run_default_admitted(AdmissionClass::RangeRead, |provider| async move {
                record_storage_operation(
                    "list_tables",
                    provider.list_tables(limit.unwrap_or(10_000), exclusive_start_table_name),
                )
                .await
            })
            .await?;
        let last_evaluated_table_name = if tables.len() >= limit.unwrap_or(1_000) as usize {
            tables.last().map(|t| t.table_name.clone())
        } else {
            None
        };
        Ok((tables, last_evaluated_table_name))
    }

    /// Run the lightweight process-readiness probe through the reserved
    /// control lane.  Readiness must remain observable while foreground work
    /// is being shed and must not consume an adaptive foreground permit.
    pub async fn check_ready(&self) -> StorageResult<()> {
        self.run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, |provider| async move {
            let database_call = metrics_facade::begin_database_call("list_tables");
            let result = provider.list_tables(1, None).await.map(|_| ());
            drop(database_call);
            result
        })
        .await
    }

    pub async fn clear_all_tables(&self) -> StorageResult<()> {
        let sys_jobs_table = Tables::sys_jobs();
        let sys_storage_replication_table = Tables::sys_storage_replication();
        // Remove job-lock table first so background jobs cannot mutate
        // application tables during cleanup.
        if let Err(error) = self.delete_table(&sys_jobs_table).await
            && !matches!(
                error.to_enum(),
                StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
            )
        {
            return Err(error);
        }
        if let Err(error) = self.delete_table(&sys_storage_replication_table).await
            && !matches!(
                error.to_enum(),
                StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
            )
        {
            return Err(error);
        }

        loop {
            let tables = self
                .run_default_admitted(AdmissionClass::RangeRead, |provider| async move {
                    record_storage_operation("list_tables", provider.list_tables(1_000, None)).await
                })
                .await?;
            let mut removed_any = false;
            for table in tables {
                if table.table_name == sys_jobs_table
                    || table.table_name == sys_storage_replication_table
                {
                    continue;
                }
                self.delete_table(&table.table_name).await?;
                removed_any = true;
            }
            if !removed_any {
                break;
            }
        }
        // Background jobs rely on this table for distributed locks; test cleanup
        // deletes everything, so restore it for subsequent scenarios.
        Tables::create_sys_jobs_table(self).await?;
        self.maybe_create_sys_storage_replication_table().await?;
        self.table_info_cache.write().await.clear();
        Ok(())
    }

    pub async fn delete_table(&self, table_name: &TableName) -> StorageResult<()> {
        if let Some(route) = self.resolve_namespace_route_for_table(table_name).await? {
            let target_table_name = route.read_target.table_name.clone();
            self.run_admitted(
                &route.read_target.connection_id,
                AdmissionClass::Write,
                |provider| async move {
                    record_storage_operation(
                        "delete_table",
                        provider.delete_table(&target_table_name),
                    )
                    .await
                },
            )
            .await?;
            self.invalidate_table_info_cache_for_targets(std::slice::from_ref(&route.read_target))
                .await;
            self.cache_query_runtime()
                .invalidate_table(table_name)
                .await?;
            return Ok(());
        }
        self.run_default_admitted(AdmissionClass::Write, |provider| async move {
            record_storage_operation("delete_table", provider.delete_table(table_name)).await
        })
        .await?;
        self.table_info_cache.write().await.remove(table_name);
        self.cache_query_runtime()
            .invalidate_table(table_name)
            .await?;
        Ok(())
    }

    #[must_use]
    pub fn initialization_stream_provider(&self) -> Arc<dyn StreamProvider> {
        Arc::clone(&self.storage) as Arc<dyn StreamProvider>
    }

    #[must_use]
    pub fn initialization_queue_provider(&self) -> Option<Arc<dyn queue_provider::QueueProvider>> {
        self.queue_provider.clone()
    }

    #[must_use]
    pub fn initialization_pubsub_provider(
        &self,
    ) -> Option<Arc<dyn pubsub_provider::PubsubProvider>> {
        self.pubsub_provider.clone()
    }

    pub async fn put_item_encode<T>(
        &self,
        table_name: TableName,
        item: &T,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
    ) -> StorageResult<PutItemResponse>
    where
        T: TryIntoWireItem,
    {
        let item = item.try_into_wire_item()?;
        self.put_item(PutItemInput {
            table_name,
            item: item.into(),
            indexers: None,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure: false,
            aux_item_stream_ttl_hours: None,
        })
        .await
    }

    pub async fn put_item_entity_encode<T>(
        &self,
        input: PutItemEntityEncodeInput<'_, T>,
    ) -> StorageResult<PutItemResponse>
    where
        T: storage_types::single_table_entity::SingleTableEntity + TryIntoWireItem,
    {
        let entity = storage_types::single_table_entity::to_wire_entity(input.item)
            .map_err(|err| StorageError::internal(&err.to_string()))?;
        let (mut item, indexers) = entity.into_write_parts();
        crate::updated_at_apply::stamp_wire_item_now(&mut item)?;
        self.put_item(PutItemInput {
            table_name: input.table_name,
            item: item.into(),
            indexers,
            condition_expression: input.condition_expression,
            expression_attribute_names: input.expression_attribute_names,
            expression_attribute_values: input.expression_attribute_values,
            return_values: input.return_values,
            return_old_on_condition_failure: false,
            aux_item_stream_ttl_hours: None,
        })
        .await
    }

    pub async fn put_new_entity_encode<T>(
        &self,
        table_name: TableName,
        item: &T,
    ) -> StorageResult<PutItemResponse>
    where
        T: storage_types::single_table_entity::SingleTableEntity + TryIntoWireItem,
    {
        self.put_item_entity_encode(
            PutItemEntityEncodeInput::builder()
                .table_name(table_name)
                .item(item)
                .condition_expression(Some(
                    "attribute_not_exists(pk) AND attribute_not_exists(sk)".to_string(),
                ))
                .build(),
        )
        .await
    }

    pub async fn update_item_entity<T>(
        &self,
        mut input: UpdateItemInput,
    ) -> StorageResult<UpdateItemResponse>
    where
        T: storage_types::single_table_entity::SingleTableEntity,
    {
        input.indexers = Some(entity_indexer_names::<T>());
        self.update_item(input).await
    }

    /// Put a brand new item enforcing that no OCC version exists yet.
    ///
    /// This will set `_v = 0` on the stored item (overwriting any supplied
    /// value) and applies a condition `attribute_not_exists(#v)` (with
    /// `ExpressionAttributeNames`) so that if
    /// the item already exists (was previously created) the write fails
    /// with a conditional check error.
    pub async fn put_new(
        &self,
        mut item: HashMap<String, AttributeValue>,
        table_name: TableName,
    ) -> StorageResult<PutItemResponse> {
        // Force initial version 0
        item.insert(
            OCC_VERSION_ATTR.to_string(),
            AttributeValue::N("0".to_string()),
        );
        self.put_item(
            PutItemInput::builder()
                .table_name(table_name)
                .item(item)
                .condition_expression(OCC_CREATE_CONDITION.to_string())
                .expression_attribute_names(occ_version_expression_names())
                .build(),
        )
        .await
    }

    /// Update (or create-if-missing) with optimistic concurrency control.
    ///
    /// `expected_v` semantics:
    /// - Some(v): condition `attribute_not_exists(#v) OR #v = :v`; stored item
    ///   gets `_v = v+1`. This allows idempotent create if two writers race
    ///   where the first wins (no version yet) and later updates require
    ///   matching version.
    /// - None: performs an unconditional write (bootstrap/admin paths) setting
    ///   `_v = 0` if not already present.
    pub async fn put_update(
        &self,
        mut item: HashMap<String, AttributeValue>,
        table_name: TableName,
        expected_v: Option<u64>,
    ) -> StorageResult<PutItemResponse> {
        let next_v = expected_v.map_or(0, |v| v + 1);
        item.insert(
            OCC_VERSION_ATTR.to_string(),
            AttributeValue::N(next_v.to_string()),
        );
        if let Some(v) = expected_v {
            return self
                .put_item(
                    PutItemInput::builder()
                        .table_name(table_name)
                        .item(item)
                        .condition_expression(OCC_UPDATE_CONDITION.to_string())
                        .expression_attribute_names(occ_version_expression_names())
                        .expression_attribute_values(HashMap::from([(
                            ":v".to_string(),
                            AttributeValue::N(v.to_string()),
                        )]))
                        .build(),
                )
                .await;
        }

        self.put_item(
            PutItemInput::builder()
                .table_name(table_name)
                .item(item)
                .build(),
        )
        .await
    }

    pub async fn update_table(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse> {
        let table_name = request.table_name.clone();
        let replica_updates = request.replica_updates.clone();
        if let Some(replica_updates) = replica_updates.as_ref()
            && !replica_updates.is_empty()
        {
            self.ensure_multi_region_replication_control_plane_supported()?;
            crate::multi_region::validate_replica_updates(replica_updates)?;
        }
        let mut provider_request = request;
        provider_request.replica_updates = None;

        // Providers continue to own table-shape mutations such as GSI and stream
        // settings. Multi-region replica metadata is intercepted here and stored
        // in the control-plane table.
        let mut response = self
            .run_default_admitted(AdmissionClass::Write, |provider| async move {
                record_storage_operation("update_table", provider.update_table(provider_request))
                    .await
            })
            .await?;

        if let Some(replica_updates) = replica_updates.as_ref()
            && !replica_updates.is_empty()
        {
            let config = self
                .apply_replica_updates(&table_name, replica_updates)
                .await?;
            response.table_description.replicas = Some(config.replicas);
            response.table_description.multi_region_consistency =
                Some(config.multi_region_consistency);
        } else {
            let (replicas, multi_region_consistency) =
                self.get_multi_region_table_state(&table_name).await?;
            response.table_description.replicas = replicas;
            response.table_description.multi_region_consistency = multi_region_consistency;
        }

        self.table_info_cache.write().await.remove(&table_name);
        self.cache_query_runtime()
            .invalidate_table(&table_name)
            .await?;
        Ok(response)
    }

    // Exposed for tests; harmless in production (no-op on backends without
    // implementation)
    pub async fn run_job(&self, name: impl ToString) {
        let Ok(name) = name.to_string().parse::<BackgroundJobName>() else {
            return;
        };
        let _ = self
            .run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, |provider| async move {
                let database_call = metrics_facade::begin_database_call("background_job");
                let result = provider.run_job(name).await;
                drop(database_call);
                result
            })
            .await;
    }

    #[cfg(all(test, feature = "sqlite"))]
    pub(crate) fn route_resolver_for_tests(&self) -> Option<Arc<NamespaceRouteResolver>> {
        self.route_resolver.clone()
    }

    #[cfg(all(test, feature = "sqlite"))]
    pub(crate) fn default_storage_for_tests(&self) -> Arc<dyn DatabaseTrait> {
        Arc::clone(&self.storage)
    }

    /// Update a physical table through the reserved control lane.
    ///
    /// This deliberately does not perform logical namespace routing or
    /// replica-control-plane bookkeeping. It is restricted to direct
    /// physical table maintenance such as enabling streams on a shared table;
    /// callers performing ordinary table mutations must use
    /// [`Self::update_table`].
    pub async fn update_table_for_control(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse> {
        if request
            .replica_updates
            .as_ref()
            .is_some_and(|updates| !updates.is_empty())
        {
            return Err(StorageError::validation(
                "control table updates cannot mutate replica metadata",
            ));
        }
        let table_name = request.table_name.clone();
        let mut provider_request = request;
        provider_request.replica_updates = None;
        let response = self
            .run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, |provider| async move {
                record_storage_operation(
                    "update_table_for_control",
                    provider.update_table(provider_request),
                )
                .await
            })
            .await?;
        self.table_info_cache.write().await.remove(&table_name);
        self.cache_query_runtime()
            .invalidate_table(&table_name)
            .await?;
        Ok(response)
    }
}

pub(crate) fn is_admission_pressure(error: &StorageError) -> bool {
    match error.to_enum() {
        StorageEnum::ProvisionedThroughputExceeded { .. }
        | StorageEnum::Throttled { .. }
        | StorageEnum::LimitExceeded { .. }
        | StorageEnum::RequestLimitExceeded
        | StorageEnum::ServiceUnavailable { .. } => true,
        StorageEnum::AwsService {
            code: Some(code), ..
        } => matches!(
            code.as_str(),
            "ServiceUnavailable"
                | "ServiceUnavailableException"
                | "RequestTimeout"
                | "RequestTimeoutException"
                | "ThrottlingException"
                | "ProvisionedThroughputExceededException"
                | "LimitExceededException"
        ),
        _ => false,
    }
}

impl Drop for DatabaseManager {
    fn drop(&mut self) {
        if let Some(task) = self.cutover_watcher_task.take() {
            task.abort();
        }
    }
}
