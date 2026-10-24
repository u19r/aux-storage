use std::collections::HashMap;

use async_trait::async_trait;
use storage_types::{
    BatchGetExecutionRequest, BatchGetExecutionResult, BatchGetItemRequest,
    BatchGetWireItemResponse, BatchWriteItemEncodeRequest, BatchWriteItemRequest,
    BatchWriteItemResponse, CreateTableRequest, DeleteItemRequest, DurablePointReadProof,
    DurablePointReadRequest, GuardedDeleteItemRequest, GuardedPutItemRequest,
    GuardedTransactWriteItemsRequest, GuardedUpdateItemRequest, KeyAttributes, KeySchemaElement,
    PutItemEncodeRequest, PutItemRequest, PutItemResponse, QueryTableRequest,
    ReadSequenceConsistency, ScanTableRequest, SplitDynamoItem, StorageError, StorageResult,
    StoredTableInfo, TableName, TableStatus, TransactGetItemsRequest, TransactGetItemsResponse,
    TransactWriteItemsEncodeRequest, UpdateItemRequest, UpdateItemResponse, WireItem,
    WriteRetryPolicy,
};

use super::{read_context_trait::StorageProviderReadContext, trait_defaults as defaults};
use crate::{
    AtomicItemReadModifyWriteRequest, AttributeValue, BatchWriteContexts, OrderedOperationContexts,
    PhysicalWrite, ReadSequenceExecution, ReadSequenceExecutionBudget, TtlWriteContext,
};

/// Trait for storage backends that can store `DynamoDB` table metadata
#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// Whether this provider can lower an eligible ReadSequence query→get
    /// edge to its native mapped-range primitive.  Callers can use this
    /// capability to avoid a speculative whole-plan attempt on providers
    /// which will necessarily fall back to serial reads.
    fn supports_read_sequence_mapped_range(&self) -> bool {
        defaults::supports_read_sequence_mapped_range()
    }

    /// Return and clear provider-pressure signals observed since the previous
    /// admitted-operation boundary.
    /// Providers which retry internally use this hook to tell the common
    /// admission controller that capacity was consumed even when the final
    /// operation succeeded.
    fn take_admission_pressure_signal(&self) -> bool {
        defaults::take_admission_pressure_signal()
    }

    /// Purge global stream-history replay state (effects, records, pointer
    /// tombstones) so a test wipe restores fresh-boot equivalence. Table
    /// delete deliberately keeps this history for incarnation fail-closed;
    /// only the wipe path calls this hook. Backends without SQL history
    /// tables keep the default no-op.
    async fn purge_stream_history_for_test(&self) -> StorageResult<()> {
        defaults::purge_stream_history_for_test()
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
        Ok(defaults::execute_read_sequence_plan())
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
        defaults::execute_read_sequence_plan_with_budget(
            self,
            plan,
            consistency,
            continuation,
            budget,
        )
        .await
    }

    /// Backend-internal helper for a lowering that combines independent
    /// Get/BatchGet roots into one statement.  The ordinary whole-plan entry
    /// point remains the only API callers need; providers which do not have a
    /// multi-root lowering inherit the normal unsupported result.
    async fn execute_read_sequence_independent_roots(
        &self,
        _plan: &storage_types::ReadSequencePlan,
    ) -> StorageResult<ReadSequenceExecution> {
        Ok(defaults::execute_read_sequence_independent_roots())
    }

    fn supports_guarded_writes(&self) -> bool {
        defaults::supports_guarded_writes()
    }

    fn supports_guarded_transaction_writes(&self) -> bool {
        defaults::supports_guarded_transaction_writes()
    }

    fn supports_change_index(&self) -> bool {
        defaults::supports_change_index()
    }

    async fn begin_read_sequence_read_context(
        &self,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<Box<dyn StorageProviderReadContext>> {
        let _ = consistency;
        defaults::unsupported_begin_read_sequence_read_context().await
    }

    /// Initialize the storage backend
    async fn initialize_storage(&self) -> StorageResult<()>;

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
        request: AtomicItemReadModifyWriteRequest,
    ) -> StorageResult<Vec<u8>> {
        defaults::atomic_item_read_modify_write(request).await
    }

    /// Store one item with the manager-derived physical write context.
    async fn put_item_request(
        &self,
        write: PhysicalWrite<PutItemRequest, TtlWriteContext>,
    ) -> StorageResult<PutItemResponse>;

    async fn put_item_request_with_retry(
        &self,
        write: PhysicalWrite<PutItemRequest, TtlWriteContext>,
        policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        defaults::put_item_request_with_retry(self, write, policy).await
    }

    /// Store one wire-encoded item with the manager-derived physical write
    /// context.
    async fn put_item_encode(
        &self,
        write: PhysicalWrite<PutItemEncodeRequest, TtlWriteContext>,
    ) -> StorageResult<PutItemResponse> {
        defaults::put_item_encode(self, write).await
    }

    async fn put_item_encode_with_retry(
        &self,
        write: PhysicalWrite<PutItemEncodeRequest, TtlWriteContext>,
        policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        defaults::put_item_encode_with_retry(self, write, policy).await
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
        defaults::get_item_with_durable_proof(request).await
    }

    /// Execute one BatchGet attempt. The response and any requested durable
    /// proofs are produced from the same provider snapshot.
    async fn execute_batch_get(
        &self,
        request: BatchGetExecutionRequest,
    ) -> StorageResult<BatchGetExecutionResult> {
        defaults::execute_batch_get(self, request).await
    }

    async fn delete_item_request(
        &self,
        write: PhysicalWrite<DeleteItemRequest, TtlWriteContext>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>;

    async fn guarded_put_item(
        &self,
        write: PhysicalWrite<GuardedPutItemRequest, TtlWriteContext>,
    ) -> StorageResult<PutItemResponse> {
        defaults::guarded_put_item(write).await
    }

    async fn guarded_delete_item(
        &self,
        write: PhysicalWrite<GuardedDeleteItemRequest, TtlWriteContext>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        defaults::guarded_delete_item(write).await
    }

    async fn guarded_update_item(
        &self,
        write: PhysicalWrite<GuardedUpdateItemRequest, TtlWriteContext>,
    ) -> StorageResult<UpdateItemResponse> {
        defaults::guarded_update_item(write).await
    }

    /// Scan a table and return items with pagination support
    async fn scan_table(&self, request: &ScanTableRequest) -> StorageResult<super::RangeReadPage>;

    /// Query a table based on key conditions and return items with pagination
    /// support
    async fn query_table(&self, request: &QueryTableRequest)
    -> StorageResult<super::RangeReadPage>;

    /// Reuse caller-resolved metadata for remote response decoding. Native
    /// providers retain authoritative catalog binding in their transaction
    /// through the default query path; this hint cannot replace that binding.
    async fn query_table_with_table_info(
        &self,
        request: &QueryTableRequest,
        _table_info: &StoredTableInfo,
    ) -> StorageResult<super::RangeReadPage> {
        self.query_table(request).await
    }

    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
        contexts: BatchWriteContexts,
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
        contexts: BatchWriteContexts,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        defaults::batch_write_item_encode(self, request, contexts, should_write_to_stream).await
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse>;

    /// Whether this provider can execute `TransactGetItems` in its native
    /// backend. Native providers must preserve the backend's atomic snapshot,
    /// ordering, and error contract; callers must not emulate this operation
    /// with sequential point reads.
    fn supports_native_transact_get_items(&self) -> bool {
        false
    }

    async fn transact_get_items(
        &self,
        _request: TransactGetItemsRequest,
    ) -> StorageResult<TransactGetItemsResponse> {
        Err(StorageError::unsupported(
            "native TransactGetItems is not supported by this storage provider",
        ))
    }

    async fn update_item(
        &self,
        write: PhysicalWrite<UpdateItemRequest, TtlWriteContext>,
    ) -> StorageResult<UpdateItemResponse>;

    async fn transact_write_items(
        &self,
        request: storage_types::TransactWriteItemsRequest,
        contexts: OrderedOperationContexts,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse>;

    async fn guarded_transact_write_items(
        &self,
        request: GuardedTransactWriteItemsRequest,
        contexts: OrderedOperationContexts,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse> {
        defaults::guarded_transact_write_items(request, contexts).await
    }

    /// Execute a transactional write request using wire-encoded put payloads.
    ///
    /// Backends can override this to avoid map normalization on transactional
    /// put operations. The default implementation preserves compatibility by
    /// mapping into the existing request shape.
    async fn transact_write_items_encode(
        &self,
        request: TransactWriteItemsEncodeRequest,
        contexts: OrderedOperationContexts,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse> {
        defaults::transact_write_items_encode(self, request, contexts).await
    }

    async fn transact_write_items_encode_with_retry(
        &self,
        request: TransactWriteItemsEncodeRequest,
        contexts: OrderedOperationContexts,
        policy: WriteRetryPolicy,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse> {
        defaults::transact_write_items_encode_with_retry(self, request, contexts, policy).await
    }

    async fn update_table(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse>;

    async fn update_time_to_live(
        &self,
        request: storage_types::UpdateTimeToLiveRequest,
    ) -> StorageResult<storage_types::UpdateTimeToLiveResponse> {
        defaults::update_time_to_live(request).await
    }

    async fn describe_time_to_live(
        &self,
        table_name: &TableName,
    ) -> StorageResult<storage_types::DescribeTimeToLiveResponse> {
        defaults::describe_time_to_live(table_name).await
    }

    async fn split_item_into_key_and_attributes(
        &self,
        item: HashMap<String, AttributeValue>,
        table_name: &TableName,
    ) -> StorageResult<SplitDynamoItem> {
        defaults::split_item_into_key_and_attributes(self, item, table_name).await
    }

    fn get_key_attributes(
        &self,
        item: &HashMap<String, AttributeValue>,
        key_schema: &[KeySchemaElement],
    ) -> StorageResult<KeyAttributes> {
        defaults::get_key_attributes(item, key_schema)
    }
}
