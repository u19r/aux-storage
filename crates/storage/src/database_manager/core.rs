#[cfg(all(test, feature = "cache-write-planner"))]
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::{collections::HashMap, sync::Arc};

use bg_jobs::BackgroundJobName;
use storage_common::GSI_UPDATE_JOB;
use storage_provider::StorageProviderReadContext;
use storage_types::{
    AllOld, AttributeValue, CreateTableRequest, DescribeTimeToLiveResponse, IndexName,
    KeyAttributes, KeySchemaElement, PutItemResponse, QueryTableRequest, ReadSequenceConsistency,
    ReadSequenceProviderCapabilities, ReturnValuesOldNewUpdated, StorageEnum, StorageError,
    StorageResult, StoredTableInfo, TableName, TableNamespace, TableStatus, TransactEncodeItem,
    TryIntoWireItem, UpdateTimeToLiveRequest, UpdateTimeToLiveResponse, WireItem,
    context::WrappedError as _,
};
use stream::StreamProvider;
#[cfg(all(test, feature = "cache-write-planner"))]
use tokio::sync::Notify;
use tokio::{sync::RwLock, task::JoinHandle};
use typed_builder::TypedBuilder;

#[cfg(feature = "cache-write-planner")]
use crate::cache_write_planner::{StorageCachePlannerLoad, StorageCacheWritePlanner};
#[cfg(feature = "cache-write-planner")]
use crate::namespace_routing::NamespaceStorageMode;
use crate::{
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
        RouteTarget, is_shared_table_enabled_namespace_route, parse_namespace_route_record,
        reject_direct_shared_table_access,
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
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_values: Option<ReturnValuesOldNewUpdated>,
    #[builder(setter(!strip_option))]
    pub return_old_on_condition_failure: bool,
    pub aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
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
    pub(super) queue_provider: Option<Arc<dyn queue_provider::QueueProvider>>,
    pub(super) pubsub_provider: Option<Arc<dyn pubsub_provider::PubsubProvider>>,
    pub(super) connection_registry: Option<HashMap<String, Arc<dyn DatabaseTrait>>>,
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
    pub(super) read_sequence_capabilities: ReadSequenceProviderCapabilities,
    pub(super) table_info_cache: RwLock<HashMap<TableName, Arc<StoredTableInfo>>>,
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

    pub(crate) fn database_trait_provider(&self) -> &Arc<dyn DatabaseTrait> {
        &self.storage
    }

    pub(crate) const fn supports_multi_region_replication_control_plane(&self) -> bool {
        self.supports_multi_region_replication_control_plane
    }

    pub const fn read_sequence_capabilities(&self) -> ReadSequenceProviderCapabilities {
        self.read_sequence_capabilities
    }

    pub async fn begin_read_sequence_read_context(
        &self,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<Box<dyn StorageProviderReadContext>> {
        self.storage
            .begin_read_sequence_read_context(consistency)
            .await
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
    pub fn route_resolver(&self) -> Option<Arc<NamespaceRouteResolver>> {
        self.route_resolver.clone()
    }

    pub(crate) fn provider_for_connection_for_migration(
        &self,
        connection_id: &str,
    ) -> StorageResult<Arc<dyn DatabaseTrait>> {
        self.provider_for_connection(connection_id)
    }

    #[must_use]
    pub(crate) fn control_plane_for_migration(&self) -> Arc<dyn DatabaseTrait> {
        Arc::clone(&self.storage)
    }

    pub(super) fn provider_for_connection(
        &self,
        connection_id: &str,
    ) -> StorageResult<Arc<dyn DatabaseTrait>> {
        if let Some(registry) = &self.connection_registry {
            return registry.get(connection_id).cloned().ok_or_else(|| {
                StorageError::validation(format!(
                    "storage connection '{connection_id}' is not configured"
                ))
            });
        }
        Ok(Arc::clone(&self.storage))
    }

    pub(super) fn provider_for_request_connection(
        &self,
        connection_id: &str,
    ) -> StorageResult<Arc<dyn DatabaseTrait>> {
        if connection_id == ROUTED_DEFAULT_CONNECTION_ID {
            return Ok(Arc::clone(&self.storage));
        }
        self.provider_for_connection(connection_id)
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
        match route_resolver.resolve_route(&namespace).await {
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
        route_resolver
            .route_for_record(&namespace, route_record)
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
            let provider = self.provider_for_connection(&route.read_target.connection_id)?;
            let table_info = record_storage_operation(
                "get_table_info",
                provider.get_table_info(&route.read_target.table_name),
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
            let provider = self.provider_for_connection(&route.read_target.connection_id)?;
            let mut item = record_storage_operation(
                "get_item",
                provider.get_item(
                    route.read_target.table_name.clone(),
                    routed_key,
                    consistent_read,
                ),
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
            let map = put.item.clone().into_attribute_map()?;
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
        let provider = self.provider_for_connection(&target.connection_id)?;
        let _ = provider.run_job(GSI_UPDATE_JOB).await;
        Ok(())
    }

    pub(super) async fn maybe_run_gsi_maintenance_for_connection(
        &self,
        connection_id: &str,
    ) -> StorageResult<()> {
        if !self.run_gsi_maintenance {
            return Ok(());
        }
        let provider = self.provider_for_request_connection(connection_id)?;
        let _ = provider.run_job(GSI_UPDATE_JOB).await;
        Ok(())
    }

    pub(super) async fn maybe_run_gsi_maintenance_default(&self) {
        if !self.run_gsi_maintenance {
            return;
        }
        let _ = self.storage.run_job(GSI_UPDATE_JOB).await;
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
            let provider = self.provider_for_connection(&target.connection_id)?;
            let target_role = RoutedWriteTargetRole::for_index(index);
            let response = execute(provider, target, index, target_role).await?;
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
        record_storage_operation("create_table", self.storage.create_table(request)).await?;
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
        let provider = self.provider_for_connection(connection_id)?;
        record_storage_operation("create_table", provider.create_table(request)).await?;
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

    pub async fn resolve_storage_operation(
        &self,
        table_name: TableName,
    ) -> StorageResult<ResolvedStorageOperation> {
        let route = self.resolve_namespace_route_for_table(&table_name).await?;
        let table_info = if let Some(route) = route.as_ref() {
            let provider = self.provider_for_connection(&route.read_target.connection_id)?;
            Arc::new(
                record_storage_operation(
                    "get_table_info",
                    provider.get_table_info(&route.read_target.table_name),
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

        let info =
            record_storage_operation("get_table_info", self.storage.get_table_info(table_name))
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
            let provider = self.provider_for_connection(&route.read_target.connection_id)?;
            return record_storage_operation(
                "table_exists",
                provider.table_exists(&route.read_target.table_name),
            )
            .await;
        }
        record_storage_operation("table_exists", self.storage.table_exists(table_name)).await
    }

    pub async fn update_table_status(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()> {
        if let Some(route) = self.resolve_namespace_route_for_table(table_name).await? {
            let provider = self.provider_for_connection(&route.read_target.connection_id)?;
            record_storage_operation(
                "update_table_status",
                provider.update_table_status(&route.read_target.table_name, status),
            )
            .await?;
            self.invalidate_table_info_cache_for_targets(std::slice::from_ref(&route.read_target))
                .await;
            self.cache_query_runtime()
                .invalidate_table(table_name)
                .await?;
            return Ok(());
        }
        record_storage_operation(
            "update_table_status",
            self.storage.update_table_status(table_name, status),
        )
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
        record_storage_operation(
            "update_time_to_live",
            self.storage.update_time_to_live(request),
        )
        .await
    }

    pub async fn describe_time_to_live(
        &self,
        table_name: &TableName,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        record_storage_operation(
            "describe_time_to_live",
            self.storage.describe_time_to_live(table_name),
        )
        .await
    }

    pub async fn list_tables(
        &self,
        limit: Option<u32>,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<(Vec<StoredTableInfo>, Option<TableName>)> {
        let tables = record_storage_operation(
            "list_tables",
            self.storage
                .list_tables(limit.unwrap_or(10_000), exclusive_start_table_name),
        )
        .await?;
        let last_evaluated_table_name = if tables.len() >= limit.unwrap_or(1_000) as usize {
            tables.last().map(|t| t.table_name.clone())
        } else {
            None
        };
        Ok((tables, last_evaluated_table_name))
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
            let tables =
                record_storage_operation("list_tables", self.storage.list_tables(1_000, None))
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
            let provider = self.provider_for_connection(&route.read_target.connection_id)?;
            record_storage_operation(
                "delete_table",
                provider.delete_table(&route.read_target.table_name),
            )
            .await?;
            self.invalidate_table_info_cache_for_targets(std::slice::from_ref(&route.read_target))
                .await;
            self.cache_query_runtime()
                .invalidate_table(table_name)
                .await?;
            return Ok(());
        }
        record_storage_operation("delete_table", self.storage.delete_table(table_name)).await?;
        self.table_info_cache.write().await.remove(table_name);
        self.cache_query_runtime()
            .invalidate_table(table_name)
            .await?;
        Ok(())
    }

    #[must_use]
    pub fn stream_provider(&self) -> Arc<dyn StreamProvider> {
        Arc::clone(&self.storage) as Arc<dyn StreamProvider>
    }

    #[must_use]
    pub fn queue_provider(&self) -> Option<Arc<dyn queue_provider::QueueProvider>> {
        self.queue_provider.clone()
    }

    #[must_use]
    pub fn pubsub_provider(&self) -> Option<Arc<dyn pubsub_provider::PubsubProvider>> {
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
        let mut item = storage_types::single_table_entity::to_wire_item_fast(input.item)
            .map_err(|err| StorageError::internal(&err.to_string()))?;
        crate::updated_at_apply::stamp_wire_item_now(&mut item)?;
        self.put_item(PutItemInput {
            table_name: input.table_name,
            item: item.into(),
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
        let mut response =
            record_storage_operation("update_table", self.storage.update_table(provider_request))
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
        let _ = self.storage.run_job(name).await;
    }

    #[cfg(all(test, feature = "sqlite"))]
    pub(crate) fn route_resolver_for_tests(&self) -> Option<Arc<NamespaceRouteResolver>> {
        self.route_resolver.clone()
    }

    #[cfg(all(test, feature = "sqlite"))]
    pub(crate) fn default_storage_for_tests(&self) -> Arc<dyn DatabaseTrait> {
        Arc::clone(&self.storage)
    }
}

impl Drop for DatabaseManager {
    fn drop(&mut self) {
        if let Some(task) = self.cutover_watcher_task.take() {
            task.abort();
        }
    }
}
