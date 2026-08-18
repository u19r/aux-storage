use std::collections::HashMap;

use async_trait::async_trait;
use bg_jobs::BackgroundJobName;
use storage_types::{
    AllOld, BatchGetItemRequest, BatchGetWireItemResponse, BatchWriteItemEncodeRequest,
    BatchWriteItemRequest, BatchWriteItemResponse, CreateTableRequest, DeleteItemRequest,
    DurableBatchPointReadProof, DurableBatchPointReadProofEntry, DurableBatchPointReadRequest,
    DurablePointReadProof, DurablePointReadRequest, GuardedDeleteItemRequest,
    GuardedPutItemRequest, GuardedTransactWriteItemsRequest, GuardedUpdateItemRequest,
    ItemVersionedWireItem, KeyAttributes, KeySchemaElement, PutItemEncodeRequest, PutItemRequest,
    PutItemResponse, QueryTableRequest, ReadSequenceConsistency, ReplicationMutation,
    ScanTableRequest, SplitDynamoItem, StorageError, StorageResult, StoredTableInfo,
    StreamRetentionDuration, TableName, TableStatus, TransactWriteItemsEncodeRequest,
    UpdateItemRequest, UpdateItemResponse, WireItem, WriteRetryPolicy,
};

use crate::{
    AtomicItemReadModifyWriteRequest, AttributeValue, ChangeIndexMarker,
    ListChangeIndexMarkersRequest, ReadSequenceExecution, ReadSequenceExecutionBudget,
    StreamTrimDueMarker, StreamTrimState,
};

/// Provider-owned read context for operations that must share one backend
/// snapshot/read version.
#[async_trait]
pub trait StorageProviderReadContext: Send + Sync {
    /// Return and clear a backend signal that the current read-only attempt
    /// must be discarded and rebuilt from a fresh snapshot.  Providers which
    /// cannot classify retryable read failures keep the default `false`.
    fn take_retryable_read_failure(&self) -> bool {
        false
    }

    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>>;

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse>;

    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)>;
}

/// Trait for storage backends that can store `DynamoDB` table metadata
#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// Whether this provider can lower an eligible ReadSequence query→get
    /// edge to its native mapped-range primitive.  Callers can use this
    /// capability to avoid a speculative whole-plan attempt on providers
    /// which will necessarily fall back to serial reads.
    fn supports_read_sequence_mapped_range(&self) -> bool {
        false
    }

    /// Return and clear provider-pressure signals observed since the previous
    /// admitted-operation boundary.
    /// Providers which retry internally use this hook to tell the common
    /// admission controller that capacity was consumed even when the final
    /// operation succeeded.
    fn take_admission_pressure_signal(&self) -> bool {
        false
    }

    /// Attempt a backend whole-plan lowering. Unsupported shapes must return
    /// `Unsupported` so callers can execute the same validated plan through
    /// the ordinary DAG scheduler.
    async fn execute_read_sequence_plan(
        &self,
        _plan: &storage_types::ReadSequencePlan,
        _consistency: ReadSequenceConsistency,
        _continuation: Option<&str>,
    ) -> StorageResult<ReadSequenceExecution> {
        Ok(ReadSequenceExecution::unsupported())
    }

    /// Attempt an optimized execution with an explicit item frontier.  The
    /// default keeps providers which only implement the unbounded envelope on
    /// the ordinary DAG path; a provider must opt into bounded execution so a
    /// continuation cannot silently publish an over-budget page.
    async fn execute_read_sequence_plan_with_budget(
        &self,
        plan: &storage_types::ReadSequencePlan,
        consistency: ReadSequenceConsistency,
        continuation: Option<&str>,
        budget: ReadSequenceExecutionBudget,
    ) -> StorageResult<ReadSequenceExecution> {
        if budget.is_unbounded() {
            self.execute_read_sequence_plan(plan, consistency, continuation)
                .await
        } else {
            Ok(ReadSequenceExecution::Unsupported(
                crate::ReadSequenceUnsupportedReason::ParameterLimit,
            ))
        }
    }

    /// Backend-internal helper for a lowering that combines independent
    /// Get/BatchGet roots into one statement.  The ordinary whole-plan entry
    /// point remains the only API callers need; providers which do not have a
    /// multi-root lowering inherit the normal unsupported result.
    async fn execute_read_sequence_independent_roots(
        &self,
        _plan: &storage_types::ReadSequencePlan,
    ) -> StorageResult<ReadSequenceExecution> {
        Ok(ReadSequenceExecution::Unsupported(
            crate::ReadSequenceUnsupportedReason::OperationShape,
        ))
    }

    fn supports_guarded_writes(&self) -> bool {
        false
    }

    fn supports_guarded_transaction_writes(&self) -> bool {
        false
    }

    fn supports_custom_stream_duration(&self) -> bool {
        false
    }

    fn supports_change_index(&self) -> bool {
        false
    }

    async fn begin_read_sequence_read_context(
        &self,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<Box<dyn StorageProviderReadContext>> {
        let _ = consistency;
        Err(StorageError::unsupported(
            "read-sequence provider read contexts are not supported by this backend",
        ))
    }

    /// Initialize the storage backend
    async fn initialize_storage(&self) -> StorageResult<()>;

    /// Publish that service-level initialization has completed.
    ///
    /// Providers may use this lifecycle hook to release background work that
    /// was registered during backend initialization but must not run until the
    /// manager has created its system tables.  The default is a no-op so
    /// providers without deferred startup work remain unchanged.
    async fn complete_initialization(&self) -> StorageResult<()> {
        Ok(())
    }

    /// Check if a table exists
    async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool>;

    /// Create a new table record
    async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()>;

    /// Update table status
    async fn update_table_status(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()>;

    /// Get table information
    async fn get_table_info(&self, table_name: &TableName) -> StorageResult<StoredTableInfo>;

    async fn write_stream_trim_state(&self, _state: StreamTrimState) -> StorageResult<()> {
        Err(StorageError::unsupported_custom_stream_duration())
    }

    async fn list_due_stream_trim_markers(
        &self,
        _due_before: storage_types::TimestampMillis,
        _limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        Err(StorageError::unsupported_custom_stream_duration())
    }

    async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<ChangeIndexMarker>> {
        let _ = request;
        Err(StorageError::unsupported(
            "change index is not supported by this backend",
        ))
    }

    /// List all tables
    async fn list_tables(
        &self,
        limit: u32,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<Vec<StoredTableInfo>>;

    /// Delete a table
    async fn delete_table(&self, table_name: &TableName) -> StorageResult<()>;

    /// Create the actual data storage for a table (e.g., `SQLite` file,
    /// filesystem)
    async fn create_table_storage(
        &self,
        table_name: &TableName,
        request: &CreateTableRequest,
    ) -> StorageResult<()>;

    async fn atomic_item_read_modify_write(
        &self,
        _request: AtomicItemReadModifyWriteRequest,
    ) -> StorageResult<Vec<u8>> {
        Err(StorageError::unsupported(
            "atomic item read-modify-write is not supported by this provider",
        ))
    }

    /// Store an item in a table.
    async fn put_item(
        &self,
        table_name: TableName,
        item: HashMap<String, AttributeValue>,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
    ) -> StorageResult<PutItemResponse> {
        self.put_item_request(
            PutItemRequest::new(table_name, item)
                .with_condition_expression(condition_expression)
                .with_expression_attribute_names(expression_attribute_names)
                .with_expression_attribute_values(expression_attribute_values)
                .with_return_values(return_values),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_item_with_stream_ttl(
        &self,
        table_name: TableName,
        item: HashMap<String, AttributeValue>,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<PutItemResponse> {
        let mut request = PutItemRequest::new(table_name, item)
            .with_condition_expression(condition_expression)
            .with_expression_attribute_names(expression_attribute_names)
            .with_expression_attribute_values(expression_attribute_values)
            .with_return_values(return_values);
        request.aux_item_stream_ttl_hours = aux_item_stream_ttl_hours;
        self.put_item_request(request).await
    }

    async fn put_item_request(&self, request: PutItemRequest) -> StorageResult<PutItemResponse>;

    async fn put_item_request_with_retry(
        &self,
        request: PutItemRequest,
        policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        if policy.max_attempts() <= 1 {
            return self.put_item_request(request).await;
        }
        for _ in 1..policy.max_attempts() {
            match self.put_item_request(request.clone()).await {
                Ok(response) => return Ok(response),
                Err(error) if error.is_retryable_write() => {
                    tokio::time::sleep(policy.delay()).await;
                }
                Err(error) => return Err(error),
            }
        }
        self.put_item_request(request).await
    }

    /// Store a wire-encoded item in a table.
    ///
    /// Backends can override this to remain wire-native end-to-end on write
    /// paths. The default implementation preserves compatibility by decoding
    /// to an attribute map and delegating to `put_item`.
    async fn put_item_encode(
        &self,
        table_name: TableName,
        item: WireItem,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
    ) -> StorageResult<PutItemResponse> {
        let item = item.into_attribute_map()?;
        self.put_item(
            table_name,
            item,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_item_encode_with_stream_ttl(
        &self,
        table_name: TableName,
        item: WireItem,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<PutItemResponse> {
        if aux_item_stream_ttl_hours.is_some() {
            return Err(StorageError::unsupported_custom_stream_duration());
        }
        self.put_item_encode(
            table_name,
            item,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
        )
        .await
    }

    async fn put_item_encode_with_retry(
        &self,
        request: PutItemEncodeRequest,
        policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        if policy.max_attempts() <= 1 {
            if request.return_old_on_condition_failure {
                return self
                    .put_item_request(PutItemRequest {
                        table_name: request.table_name,
                        item: request.item.into_attribute_map()?,
                        indexers: request.indexers,
                        condition_expression: request.condition_expression,
                        expression_attribute_names: request.expression_attribute_names,
                        expression_attribute_values: request.expression_attribute_values,
                        expected: None,
                        conditional_operator: None,
                        return_values: request.return_values,
                        return_consumed_capacity: None,
                        return_item_collection_metrics: None,
                        return_values_on_condition_check_failure: Some("ALL_OLD".to_string()),
                        aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
                    })
                    .await;
            }
            return self
                .put_item_encode_with_stream_ttl(
                    request.table_name,
                    request.item,
                    request.condition_expression,
                    request.expression_attribute_names,
                    request.expression_attribute_values,
                    request.return_values,
                    request.aux_item_stream_ttl_hours,
                )
                .await;
        }
        for _ in 1..policy.max_attempts() {
            match self
                .put_item_encode_with_retry(request.clone(), WriteRetryPolicy::no_retry())
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) if error.is_retryable_write() => {
                    tokio::time::sleep(policy.delay()).await;
                }
                Err(error) => return Err(error),
            }
        }
        self.put_item_encode_with_retry(request, WriteRetryPolicy::no_retry())
            .await
    }

    /// Retrieve an item from a table
    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>>;

    async fn get_item_with_durable_proof(
        &self,
        request: DurablePointReadRequest,
    ) -> StorageResult<DurablePointReadProof> {
        let _ = request;
        Err(StorageError::unsupported(
            "durable point-read proofs are not supported by this backend",
        ))
    }

    async fn batch_get_item_with_durable_proofs(
        &self,
        request: DurableBatchPointReadRequest,
    ) -> StorageResult<DurableBatchPointReadProof> {
        let mut responses = HashMap::new();
        for (table_name, keys_and_attributes) in request.request_items {
            let mut table_proofs = Vec::with_capacity(keys_and_attributes.keys.len());
            for key in keys_and_attributes.keys {
                let proof = self
                    .get_item_with_durable_proof(DurablePointReadRequest {
                        table_name: table_name.clone(),
                        key: key.clone(),
                        consistent_read: keys_and_attributes.consistent_read.unwrap_or(false),
                    })
                    .await?;
                table_proofs.push(DurableBatchPointReadProofEntry { key, proof });
            }
            responses.insert(table_name, table_proofs);
        }

        Ok(DurableBatchPointReadProof {
            responses,
            unprocessed_keys: HashMap::new(),
        })
    }

    /// Delete an item from a table and optionally return the deleted item.
    async fn delete_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        self.delete_item_request(
            DeleteItemRequest::new(table_name, key)
                .with_condition_expression(condition_expression)
                .with_expression_attribute_names(expression_attribute_names)
                .with_expression_attribute_values(expression_attribute_values),
        )
        .await
    }

    async fn delete_item_with_stream_ttl(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let mut request = DeleteItemRequest::new(table_name, key)
            .with_condition_expression(condition_expression)
            .with_expression_attribute_names(expression_attribute_names)
            .with_expression_attribute_values(expression_attribute_values);
        request.aux_item_stream_ttl_hours = aux_item_stream_ttl_hours;
        self.delete_item_request(request).await
    }

    async fn delete_item_request(
        &self,
        request: DeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>;

    async fn guarded_put_item(
        &self,
        request: GuardedPutItemRequest,
    ) -> StorageResult<PutItemResponse> {
        let _ = request;
        Err(StorageError::unsupported(
            "guarded put is not supported by this backend",
        ))
    }

    async fn guarded_delete_item(
        &self,
        request: GuardedDeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let _ = request;
        Err(StorageError::unsupported(
            "guarded delete is not supported by this backend",
        ))
    }

    async fn guarded_update_item(
        &self,
        request: GuardedUpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        let _ = request;
        Err(StorageError::unsupported(
            "guarded update is not supported by this backend",
        ))
    }

    /// Scan a table and return items with pagination support
    async fn scan_table(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)>;

    /// Internal scan that returns present item images with their per-key stream
    /// versions for logical catchup/export.
    async fn scan_table_with_item_stream_versions(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<ItemVersionedWireItem>, Option<String>)> {
        let _ = request;
        Err(StorageError::unsupported(
            "versioned internal scan is not supported by this backend",
        ))
    }

    async fn export_logical_backfill_page(
        &self,
        request: storage_backfill::LogicalExportRequest,
    ) -> StorageResult<storage_backfill::LogicalExportPage> {
        let _ = request;
        Err(StorageError::unsupported(
            "logical backfill export is not supported by this backend",
        ))
    }

    async fn import_logical_backfill_chunk(
        &self,
        manifest: &storage_backfill::LogicalBackfillManifest,
        chunk: storage_backfill::LogicalBackfillChunk,
    ) -> StorageResult<storage_backfill::LogicalBackfillResult> {
        let _ = (manifest, chunk);
        Err(StorageError::unsupported(
            "logical backfill import is not supported by this backend",
        ))
    }

    async fn apply_resolved_sync_mutations(
        &self,
        metadata: storage_sync::SyncCommitMetadata,
        batch: storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<Vec<storage_sync::SyncMutationResponse>> {
        let _ = (metadata, batch);
        Err(StorageError::unsupported(
            "resolved sync mutation apply is not supported by this backend",
        ))
    }

    async fn last_resolved_sync_log_id(&self) -> StorageResult<Option<storage_sync::SyncLogId>> {
        Err(StorageError::unsupported(
            "resolved sync apply checkpoint is not supported by this backend",
        ))
    }

    async fn persist_resolved_sync_log_entry(
        &self,
        metadata: &storage_sync::SyncCommitMetadata,
        batch: &storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<()> {
        let _ = (metadata, batch);
        Err(StorageError::unsupported(
            "resolved sync log storage is not supported by this backend",
        ))
    }

    async fn get_resolved_sync_log_entry(
        &self,
        log_id: storage_sync::SyncLogId,
    ) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
        let _ = log_id;
        Err(StorageError::unsupported(
            "resolved sync log lookup is not supported by this backend",
        ))
    }

    async fn resolved_sync_log_entries_after(
        &self,
        log_id: Option<storage_sync::SyncLogId>,
        limit: usize,
    ) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
        let _ = (log_id, limit);
        Err(StorageError::unsupported(
            "resolved sync log scan is not supported by this backend",
        ))
    }

    /// Query a table based on key conditions and return items with pagination
    /// support
    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)>;

    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse>;

    /// Execute a batch write request using wire-encoded put payloads.
    ///
    /// Backends can override this to avoid map normalization on write puts.
    /// The default implementation preserves compatibility by mapping into the
    /// existing `BatchWriteItemRequest` shape.
    async fn batch_write_item_encode(
        &self,
        request: BatchWriteItemEncodeRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        let mapped = BatchWriteItemRequest::try_from(request)?;
        self.batch_write_item(mapped, should_write_to_stream).await
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse>;

    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse>;

    async fn transact_write_items(
        &self,
        request: storage_types::TransactWriteItemsRequest,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse>;

    async fn guarded_transact_write_items(
        &self,
        request: GuardedTransactWriteItemsRequest,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse> {
        let _ = request;
        Err(StorageError::unsupported(
            "guarded transaction writes are not supported by this backend",
        ))
    }

    /// Execute a transactional write request using wire-encoded put payloads.
    ///
    /// Backends can override this to avoid map normalization on transactional
    /// put operations. The default implementation preserves compatibility by
    /// mapping into the existing request shape.
    async fn transact_write_items_encode(
        &self,
        request: TransactWriteItemsEncodeRequest,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse> {
        let mapped = storage_types::TransactWriteItemsRequest::try_from(request)?;
        self.transact_write_items(mapped).await
    }

    async fn transact_write_items_encode_with_retry(
        &self,
        request: TransactWriteItemsEncodeRequest,
        policy: WriteRetryPolicy,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse> {
        if policy.max_attempts() <= 1 {
            return self.transact_write_items_encode(request).await;
        }
        for _ in 1..policy.max_attempts() {
            match self.transact_write_items_encode(request.clone()).await {
                Ok(response) => return Ok(response),
                Err(error) if error.is_retryable_write() => {
                    tokio::time::sleep(policy.delay()).await;
                }
                Err(error) => return Err(error),
            }
        }
        self.transact_write_items_encode(request).await
    }

    async fn update_table(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse>;

    async fn apply_replication_mutation(&self, mutation: ReplicationMutation) -> StorageResult<()> {
        let _ = mutation;
        Err(StorageError::internal(
            "replicated mutations are not supported by this backend",
        ))
    }

    fn replication_apply_parallelism_hint(&self) -> usize {
        1
    }

    async fn update_time_to_live(
        &self,
        request: storage_types::UpdateTimeToLiveRequest,
    ) -> StorageResult<storage_types::UpdateTimeToLiveResponse> {
        let _ = request;
        Err(StorageError::internal(
            "Time to live configuration is not supported by this backend",
        ))
    }

    async fn describe_time_to_live(
        &self,
        table_name: &TableName,
    ) -> StorageResult<storage_types::DescribeTimeToLiveResponse> {
        let _ = table_name;
        Err(StorageError::internal(
            "Time to live configuration is not supported by this backend",
        ))
    }

    /// Execute a named background job immediately (best-effort).
    ///
    /// Default implementation is a no-op so callers can safely invoke this
    /// without worrying if a backend supports ad-hoc execution.
    ///
    /// Use cases:
    /// - Test determinism: run the GSI update job right after a write so that
    ///   queries hitting a GSI observe the latest state without waiting for the
    ///   timer-driven background job.
    /// - On-demand maintenance hooks.
    ///
    /// Backends that support this should override and perform the work until
    /// no progress is made (idempotent catch-up semantics).
    async fn run_job(&self, _name: BackgroundJobName) -> StorageResult<()> {
        Ok(())
    }

    async fn split_item_into_key_and_attributes(
        &self,
        item: HashMap<String, AttributeValue>,
        table_name: &TableName,
    ) -> StorageResult<SplitDynamoItem> {
        let table_info = self.get_table_info(table_name).await?;

        split_item_into_key_and_attributes_sync(item, &table_info)
    }

    fn get_key_attributes(
        &self,
        item: &HashMap<String, AttributeValue>,
        key_schema: &[KeySchemaElement],
    ) -> StorageResult<KeyAttributes> {
        let mut key_attributes = KeyAttributes::with_capacity(key_schema.len());

        for (attr_name, attr_value) in item {
            let key_attribute = key_schema.iter().any(|i| *attr_name == i.attribute_name);

            if key_attribute {
                key_attributes.insert(attr_name.clone(), attr_value.clone());
            }
        }

        for table_schema_key in key_schema {
            if !key_attributes.contains_key(&table_schema_key.attribute_name) {
                return Err(StorageError::invalid_or_missing_key());
            }
        }

        Ok(key_attributes)
    }
}

pub fn split_item_into_key_and_attributes_sync(
    item: HashMap<String, AttributeValue>,
    table_info: &StoredTableInfo,
) -> StorageResult<SplitDynamoItem> {
    let all_attributes = item.clone();
    // Separate key attributes from non-key attributes
    let mut key_attributes = KeyAttributes::new();
    let mut non_key_attributes = HashMap::new();

    for (attr_name, attr_value) in item {
        let is_key = table_info
            .key_schema
            .iter()
            .any(|key| key.attribute_name == attr_name);
        if is_key {
            key_attributes.insert(attr_name, attr_value);
        } else {
            non_key_attributes.insert(attr_name, attr_value);
        }
    }

    for table_schema_key in &table_info.key_schema {
        if !key_attributes.contains_key(&table_schema_key.attribute_name) {
            return Err(StorageError::invalid_or_missing_key());
        }
    }

    Ok(SplitDynamoItem {
        key_attributes,
        all_attributes,
        non_key_attributes,
    })
}
