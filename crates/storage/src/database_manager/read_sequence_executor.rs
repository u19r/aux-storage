use storage_provider::StorageProviderReadContext;
use storage_types::{
    KeyAttributes, QueryTableRequest, READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES,
    READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS, READ_SEQUENCE_DEFAULT_MAX_SEQUENCE_STEPS,
    READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS, READ_SEQUENCE_HARD_MAX_RESPONSE_BYTES,
    READ_SEQUENCE_HARD_MAX_ROOT_ITEMS, READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS,
    READ_SEQUENCE_HARD_MAX_TOTAL_READ_ITEMS, ReadSequenceConsistency, StorageError, StorageResult,
    TableName, TableNamespace, TryFromWireItem, WireItem, validate_expression_attribute_usage,
    validate_key_attributes_for_schema,
};

use crate::{
    QueryTableInput,
    database_manager::{DatabaseManager, ROUTED_DEFAULT_CONNECTION_ID, record_storage_operation},
    namespace_routing::NamespaceStorageMode,
};

/// Hard-bounded limits for an in-process dependent read sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InProcessReadSequenceLimits {
    max_operations: u32,
    max_total_read_items: u32,
    max_items_per_operation: u32,
    max_response_bytes: u32,
}

impl InProcessReadSequenceLimits {
    pub fn try_new(
        max_operations: u32,
        max_total_read_items: u32,
        max_items_per_operation: u32,
        max_response_bytes: u32,
    ) -> StorageResult<Self> {
        ensure_limit(
            "max_operations",
            max_operations,
            READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS,
        )?;
        ensure_limit(
            "max_total_read_items",
            max_total_read_items,
            READ_SEQUENCE_HARD_MAX_TOTAL_READ_ITEMS,
        )?;
        ensure_limit(
            "max_items_per_operation",
            max_items_per_operation,
            READ_SEQUENCE_HARD_MAX_ROOT_ITEMS,
        )?;
        ensure_limit(
            "max_response_bytes",
            max_response_bytes,
            READ_SEQUENCE_HARD_MAX_RESPONSE_BYTES,
        )?;
        Ok(Self {
            max_operations,
            max_total_read_items,
            max_items_per_operation,
            max_response_bytes,
        })
    }

    #[must_use]
    pub const fn max_operations(self) -> u32 {
        self.max_operations
    }

    #[must_use]
    pub const fn max_total_read_items(self) -> u32 {
        self.max_total_read_items
    }

    #[must_use]
    pub const fn max_items_per_operation(self) -> u32 {
        self.max_items_per_operation
    }

    #[must_use]
    pub const fn max_response_bytes(self) -> u32 {
        self.max_response_bytes
    }
}

impl Default for InProcessReadSequenceLimits {
    fn default() -> Self {
        Self {
            max_operations: READ_SEQUENCE_DEFAULT_MAX_SEQUENCE_STEPS,
            max_total_read_items: READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS,
            max_items_per_operation: READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS,
            max_response_bytes: READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES,
        }
    }
}

/// Accounting retained by an in-process executor, including cancelled or failed
/// reads.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct InProcessReadSequenceStats {
    operations_started: u32,
    operations_completed: u32,
    requested_items: u32,
    returned_items: u32,
    returned_bytes: u64,
}

impl InProcessReadSequenceStats {
    #[must_use]
    pub const fn operations_started(self) -> u32 {
        self.operations_started
    }

    #[must_use]
    pub const fn operations_completed(self) -> u32 {
        self.operations_completed
    }

    #[must_use]
    pub const fn requested_items(self) -> u32 {
        self.requested_items
    }

    #[must_use]
    pub const fn returned_items(self) -> u32 {
        self.returned_items
    }

    #[must_use]
    pub const fn returned_bytes(self) -> u64 {
        self.returned_bytes
    }
}

/// A wire-native, bounded sequence of dependent reads on one provider context.
///
/// Methods require `&mut self`, so one executor cannot create concurrent child
/// fanout. The caller may use a value returned by one operation to construct
/// the next operation without an HTTP hop or JSON serialization.
pub struct InProcessReadSequence<'manager> {
    manager: &'manager DatabaseManager,
    consistency: ReadSequenceConsistency,
    limits: InProcessReadSequenceLimits,
    stats: InProcessReadSequenceStats,
    connection_id: Option<String>,
    read_context: Option<Box<dyn StorageProviderReadContext>>,
}

impl DatabaseManager {
    pub fn read_sequence_executor(
        &self,
        consistency: ReadSequenceConsistency,
        limits: InProcessReadSequenceLimits,
    ) -> StorageResult<InProcessReadSequence<'_>> {
        ensure_consistency_supported(self, consistency)?;
        Ok(InProcessReadSequence {
            manager: self,
            consistency,
            limits,
            stats: InProcessReadSequenceStats::default(),
            connection_id: None,
            read_context: None,
        })
    }
}

impl InProcessReadSequence<'_> {
    #[must_use]
    pub const fn stats(&self) -> InProcessReadSequenceStats {
        self.stats
    }

    #[cfg(test)]
    pub(super) fn set_read_context_for_test(
        &mut self,
        connection_id: &str,
        read_context: Box<dyn StorageProviderReadContext>,
    ) {
        self.connection_id = Some(connection_id.to_string());
        self.read_context = Some(read_context);
    }

    pub async fn get_item<K>(
        &mut self,
        table_name: TableName,
        key: K,
    ) -> StorageResult<Option<WireItem>>
    where
        K: Into<KeyAttributes>,
    {
        self.ensure_operation_budget(1)?;
        let mut key = key.into();
        let table_info = self.manager.get_table_info_arc(&table_name).await?;
        validate_key_attributes_for_schema(&table_info.key_schema, &key)?;
        let target = self.prepare_target(&table_name).await?;
        if let Some(namespace) = target.shared_namespace.as_ref() {
            self.manager
                .request_rewriter
                .rewrite_key_for_shared_table(namespace, &mut key)?;
        }
        self.start_operation(1)?;
        self.ensure_context(&target.connection_id).await?;
        let consistent_read = self.consistency != ReadSequenceConsistency::Eventual;
        let mut item = record_storage_operation(
            "in_process_read_sequence_get_item",
            self.context()?
                .get_item(target.physical_table, key, consistent_read),
        )
        .await?;
        if let (Some(namespace), Some(item)) = (target.shared_namespace.as_ref(), item.as_mut()) {
            self.manager
                .request_rewriter
                .normalize_wire_item_from_shared_table(namespace, item)?;
        }
        self.complete_operation(
            u32::from(item.is_some()),
            item.as_ref().map_or(0, WireItem::payload_len) as u64,
        )?;
        Ok(item)
    }

    pub async fn get_item_decode<K, T>(
        &mut self,
        table_name: TableName,
        key: K,
    ) -> StorageResult<Option<T>>
    where
        K: Into<KeyAttributes>,
        T: TryFromWireItem,
    {
        self.get_item(table_name, key)
            .await?
            .as_ref()
            .map(T::try_from_wire_item)
            .transpose()
    }

    pub async fn query_table(
        &mut self,
        input: QueryTableInput,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let mut request: QueryTableRequest = input.into();
        let requested_items = self.bounded_query_limit(request.limit)?;
        self.ensure_operation_budget(requested_items)?;
        validate_expression_attribute_usage(
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
            std::iter::once(request.key_condition_expression.as_str()),
        )?;
        let logical_request = request.clone();
        let target = self.prepare_target(&request.table_name).await?;
        if let Some(namespace) = target.shared_namespace.as_ref() {
            self.manager
                .request_rewriter
                .rewrite_query_for_shared_table(namespace, &mut request)?;
            self.manager
                .rewrite_query_start_key_for_shared_table(namespace, &mut request)
                .await?;
        }
        request.table_name = target.physical_table;
        request.consistent_read = self.consistency != ReadSequenceConsistency::Eventual;
        request.limit = Some(requested_items);
        self.start_operation(requested_items)?;
        self.ensure_context(&target.connection_id).await?;
        let (mut items, mut last_evaluated_key) = record_storage_operation(
            "in_process_read_sequence_query_table",
            self.context()?.query_table(&request),
        )
        .await?;
        if items.len() > requested_items as usize {
            return Err(StorageError::internal(
                "read-sequence provider returned more query items than requested",
            ));
        }
        if let Some(namespace) = target.shared_namespace.as_ref() {
            crate::database_manager::normalize_wire_items_for_shared_table(
                &self.manager.request_rewriter,
                namespace,
                &mut items,
            )?;
            last_evaluated_key = self
                .manager
                .normalize_query_start_key_from_shared_table(
                    namespace,
                    &logical_request,
                    last_evaluated_key,
                )
                .await?;
        }
        let returned_bytes = items.iter().map(WireItem::payload_len).sum::<usize>() as u64;
        self.complete_operation(items.len() as u32, returned_bytes)?;
        Ok((items, last_evaluated_key))
    }

    pub async fn query_table_decode<T>(
        &mut self,
        input: QueryTableInput,
    ) -> StorageResult<(Vec<T>, Option<String>)>
    where
        T: TryFromWireItem,
    {
        let (items, token) = self.query_table(input).await?;
        Ok((
            items
                .iter()
                .map(T::try_from_wire_item)
                .collect::<StorageResult<Vec<_>>>()?,
            token,
        ))
    }

    async fn prepare_target(&self, logical_table: &TableName) -> StorageResult<ReadTarget> {
        let route = self
            .manager
            .resolve_namespace_route_for_table(logical_table)
            .await?;
        Ok(route.map_or_else(
            || ReadTarget {
                connection_id: ROUTED_DEFAULT_CONNECTION_ID.to_string(),
                physical_table: logical_table.clone(),
                shared_namespace: None,
            },
            |route| ReadTarget {
                connection_id: route.read_target.connection_id,
                physical_table: route.read_target.table_name,
                shared_namespace: (route.storage_mode == NamespaceStorageMode::SharedTable)
                    .then_some(route.namespace),
            },
        ))
    }

    async fn ensure_context(&mut self, connection_id: &str) -> StorageResult<()> {
        if let Some(existing) = self.connection_id.as_deref() {
            if existing != connection_id {
                return Err(StorageError::unsupported(
                    "an in-process read sequence cannot guarantee one snapshot across multiple \
                     storage connections",
                ));
            }
            return Ok(());
        }
        let provider = self
            .manager
            .provider_for_request_connection(connection_id)?;
        let context = provider
            .begin_read_sequence_read_context(self.consistency)
            .await?;
        self.connection_id = Some(connection_id.to_string());
        self.read_context = Some(context);
        Ok(())
    }

    fn context(&self) -> StorageResult<&dyn StorageProviderReadContext> {
        self.read_context.as_deref().ok_or_else(|| {
            StorageError::internal("in-process read sequence context was not initialized")
        })
    }

    fn bounded_query_limit(&self, requested: Option<u32>) -> StorageResult<u32> {
        let remaining = self
            .limits
            .max_total_read_items
            .checked_sub(self.stats.requested_items)
            .ok_or_else(total_read_budget_error)?;
        if remaining == 0 {
            return Err(total_read_budget_error());
        }
        Ok(requested
            .unwrap_or(self.limits.max_items_per_operation)
            .min(self.limits.max_items_per_operation)
            .min(remaining))
    }

    fn start_operation(&mut self, requested_items: u32) -> StorageResult<()> {
        self.ensure_operation_budget(requested_items)?;
        self.stats.operations_started += 1;
        self.stats.requested_items += requested_items;
        Ok(())
    }

    fn ensure_operation_budget(&self, requested_items: u32) -> StorageResult<()> {
        if self.stats.operations_started >= self.limits.max_operations {
            return Err(StorageError::validation(format!(
                "in-process read sequence operation limit exceeded: {}",
                self.limits.max_operations
            )));
        }
        if requested_items == 0 || requested_items > self.limits.max_items_per_operation {
            return Err(StorageError::validation(format!(
                "in-process read sequence operation item limit exceeded: {}",
                self.limits.max_items_per_operation
            )));
        }
        let total = self
            .stats
            .requested_items
            .checked_add(requested_items)
            .ok_or_else(total_read_budget_error)?;
        if total > self.limits.max_total_read_items {
            return Err(total_read_budget_error());
        }
        Ok(())
    }

    fn complete_operation(
        &mut self,
        returned_items: u32,
        returned_bytes: u64,
    ) -> StorageResult<()> {
        self.stats.operations_completed += 1;
        self.stats.returned_items = self.stats.returned_items.saturating_add(returned_items);
        self.stats.returned_bytes = self.stats.returned_bytes.saturating_add(returned_bytes);
        if self.stats.returned_bytes > u64::from(self.limits.max_response_bytes) {
            return Err(StorageError::validation(format!(
                "in-process read sequence response byte limit exceeded: {}",
                self.limits.max_response_bytes
            )));
        }
        Ok(())
    }
}

struct ReadTarget {
    connection_id: String,
    physical_table: TableName,
    shared_namespace: Option<TableNamespace>,
}

fn ensure_consistency_supported(
    manager: &DatabaseManager,
    consistency: ReadSequenceConsistency,
) -> StorageResult<()> {
    let capabilities = manager.read_sequence_capabilities();
    let supported = match consistency {
        ReadSequenceConsistency::Eventual => capabilities.eventual_reads,
        ReadSequenceConsistency::Strong => capabilities.strong_reads,
        ReadSequenceConsistency::Transactional => {
            capabilities.transactional_reads && capabilities.transactional_snapshots
        }
    };
    if supported {
        return Ok(());
    }
    Err(StorageError::unsupported(
        "in-process read sequence consistency is not supported by this backend",
    ))
}

fn ensure_limit(name: &str, value: u32, hard_max: u32) -> StorageResult<()> {
    if value == 0 || value > hard_max {
        return Err(StorageError::validation(format!(
            "in-process read sequence {name} must be between 1 and {hard_max}"
        )));
    }
    Ok(())
}

fn total_read_budget_error() -> StorageError {
    StorageError::validation("in-process read sequence total requested-item budget exceeded")
}
