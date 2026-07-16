use crate::storage_ops::provider_impl::*;

#[async_trait]
impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> StorageProvider
    for SortedKvDbStorageProvider<S>
{
    async fn atomic_item_read_modify_write(
        &self,
        request: AtomicItemReadModifyWriteRequest,
    ) -> StorageResult<Vec<u8>> {
        self.atomic_item_read_modify_write_impl(request).await
    }

    fn supports_custom_stream_duration(&self) -> bool {
        true
    }

    fn supports_change_index(&self) -> bool {
        true
    }

    async fn begin_read_sequence_read_context(
        &self,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<Box<dyn StorageProviderReadContext>> {
        self.begin_read_sequence_read_context_impl(consistency)
            .await
    }

    async fn write_stream_trim_state(&self, state: StreamTrimState) -> StorageResult<()> {
        self.write_stream_trim_state_kv(state).await
    }

    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        self.list_due_stream_trim_markers_kv(due_before, limit)
            .await
    }

    async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<ChangeIndexMarker>> {
        self.list_change_index_markers_impl(request).await
    }

    async fn run_job(&self, name: BackgroundJobName) -> StorageResult<()> {
        self.run_job_impl(name).await
    }

    async fn initialize_storage(&self) -> StorageResult<()> {
        self.initialize_storage_impl().await
    }

    async fn apply_resolved_sync_mutations(
        &self,
        metadata: storage_sync::SyncCommitMetadata,
        batch: storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<Vec<storage_sync::SyncMutationResponse>> {
        crate::storage_ops::resolved_sync_apply::apply_resolved_sync_mutations(
            self, metadata, batch,
        )
        .await
    }

    async fn last_resolved_sync_log_id(&self) -> StorageResult<Option<storage_sync::SyncLogId>> {
        crate::storage_ops::resolved_sync_apply::last_resolved_sync_log_id(self).await
    }

    async fn persist_resolved_sync_log_entry(
        &self,
        metadata: &storage_sync::SyncCommitMetadata,
        batch: &storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<()> {
        crate::storage_ops::resolved_sync_apply::persist_resolved_sync_log_entry(
            self, metadata, batch,
        )
        .await
    }

    async fn get_resolved_sync_log_entry(
        &self,
        log_id: storage_sync::SyncLogId,
    ) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
        crate::storage_ops::resolved_sync_apply::get_resolved_sync_log_entry(self, log_id).await
    }

    async fn resolved_sync_log_entries_after(
        &self,
        log_id: Option<storage_sync::SyncLogId>,
        limit: usize,
    ) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
        crate::storage_ops::resolved_sync_apply::resolved_sync_log_entries_after(
            self, log_id, limit,
        )
        .await
    }

    async fn scan_table_with_item_stream_versions(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<ItemVersionedWireItem>, Option<String>)> {
        self.scan_table_with_item_stream_versions_impl(request)
            .await
    }

    async fn get_item_with_durable_proof(
        &self,
        request: DurablePointReadRequest,
    ) -> StorageResult<DurablePointReadProof> {
        self.get_item_with_durable_proof_impl(request).await
    }

    async fn export_logical_backfill_page(
        &self,
        request: storage_backfill::LogicalExportRequest,
    ) -> StorageResult<storage_backfill::LogicalExportPage> {
        self.export_logical_page_impl(request).await
    }

    async fn import_logical_backfill_chunk(
        &self,
        manifest: &storage_backfill::LogicalBackfillManifest,
        chunk: storage_backfill::LogicalBackfillChunk,
    ) -> StorageResult<storage_backfill::LogicalBackfillResult> {
        self.import_logical_chunk_impl(manifest, chunk).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "table_exists", table_name = %table_name))]
    async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        self.table_exists_impl(table_name).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "create_table", table_name = %request.table_name))]
    async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        self.create_table_impl(request).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "update_table_status", table_name = %table_name))]
    async fn update_table_status(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()> {
        self.update_table_status_impl(table_name, status).await
    }

    async fn get_table_info(&self, table_name: &TableName) -> StorageResult<StoredTableInfo> {
        self.get_table_info_impl(table_name).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "list_tables"))]
    async fn list_tables(
        &self,
        limit: u32,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<Vec<StoredTableInfo>> {
        self.list_tables_impl(limit, exclusive_start_table_name)
            .await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "delete_table", table_name = %table_name))]
    async fn delete_table(&self, table_name: &TableName) -> StorageResult<()> {
        self.delete_table_impl(table_name).await
    }

    async fn create_table_storage(
        &self,
        table_name: &TableName,
        request: &CreateTableRequest,
    ) -> StorageResult<()> {
        self.create_table_storage_impl(table_name, request).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "get_item", table_name = %table_name, ddb_read = true, items_returned = tracing::field::Empty, bytes_read = tracing::field::Empty))]
    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        self.get_item_impl(table_name, key, consistent_read).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "put_item", table_name = %request.table_name, ddb_write = true, items_updated = tracing::field::Empty, bytes_written = tracing::field::Empty))]
    async fn put_item_request(
        &self,
        request: storage_types::PutItemRequest,
    ) -> StorageResult<PutItemResponse> {
        self.put_item_with_stream_ttl_impl(put_item::PutItemStreamTtlRequest {
            table_name: request.table_name,
            item: request.item,
            condition_expression: request.condition_expression,
            expression_attribute_names: request.expression_attribute_names,
            expression_attribute_values: request.expression_attribute_values,
            return_values: request.return_values,
            return_old_on_condition_failure:
                storage_types::return_values_on_condition_check_failure_all_old(
                    request.return_values_on_condition_check_failure.as_ref(),
                ),
            aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
        })
        .await
    }

    async fn put_item_request_with_retry(
        &self,
        request: storage_types::PutItemRequest,
        policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        self.put_item_request_with_retry_impl(request, policy).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "put_item", table_name = %table_name, ddb_write = true, items_updated = tracing::field::Empty, bytes_written = tracing::field::Empty))]
    async fn put_item_encode(
        &self,
        table_name: TableName,
        item: WireItem,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
    ) -> StorageResult<PutItemResponse> {
        self.put_item_encode_impl(
            table_name,
            item,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            false,
        )
        .await
    }

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
        self.put_item_encode_with_stream_ttl_impl(put_item::PutItemStreamTtlRequest {
            table_name,
            item,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure: false,
            aux_item_stream_ttl_hours,
        })
        .await
    }

    async fn put_item_encode_with_retry(
        &self,
        request: PutItemEncodeRequest,
        policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        self.put_item_encode_with_retry_impl(request, policy).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "delete_item", table_name = %request.table_name, ddb_write = true, items_updated = tracing::field::Empty, bytes_written = tracing::field::Empty))]
    async fn delete_item_request(
        &self,
        request: storage_types::DeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        self.execute_delete_item(
            request.table_name,
            request.key,
            request.condition_expression,
            request.expression_attribute_names,
            request.expression_attribute_values,
            storage_types::return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            ),
            request.aux_item_stream_ttl_hours,
        )
        .await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "scan_table", table_name = %request.table_name, index_name = tracing::field::Empty, ddb_read = true, items_returned = tracing::field::Empty, bytes_read = tracing::field::Empty))]
    async fn scan_table(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.scan_table_impl(request).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "query_table", table_name = %request.table_name, index_name = tracing::field::Empty, ddb_read = true, items_returned = tracing::field::Empty, bytes_read = tracing::field::Empty))]
    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.query_table_impl(request).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "batch_write_item", ddb_write = true, items_updated = tracing::field::Empty, bytes_written = tracing::field::Empty))]
    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        self.batch_write_item_impl(request, should_write_to_stream)
            .await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "batch_write_item", ddb_write = true, items_updated = tracing::field::Empty, bytes_written = tracing::field::Empty))]
    async fn batch_write_item_encode(
        &self,
        request: BatchWriteItemEncodeRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        self.batch_write_item_encode_impl(request, should_write_to_stream)
            .await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "update_item", table_name = %request.table_name, ddb_write = true, items_updated = tracing::field::Empty, bytes_written = tracing::field::Empty))]
    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
        self.update_item_impl(request).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "batch_get_item", ddb_read = true, items_returned = tracing::field::Empty, bytes_read = tracing::field::Empty))]
    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        self.batch_get_item_impl(request).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "transact_write_items", ddb_write = true, items_updated = tracing::field::Empty, bytes_written = tracing::field::Empty))]
    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.transact_write_items_impl(request).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "transact_write_items", ddb_write = true, items_updated = tracing::field::Empty, bytes_written = tracing::field::Empty))]
    async fn transact_write_items_encode(
        &self,
        request: TransactWriteItemsEncodeRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.transact_write_items_encode_impl(request).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "transact_write_items", ddb_write = true, items_updated = tracing::field::Empty, bytes_written = tracing::field::Empty))]
    async fn transact_write_items_encode_with_retry(
        &self,
        request: TransactWriteItemsEncodeRequest,
        policy: WriteRetryPolicy,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.transact_write_items_encode_with_retry_impl(request, policy)
            .await
    }

    async fn update_table(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse> {
        self.update_table_impl(request).await
    }

    async fn apply_replication_mutation(&self, mutation: ReplicationMutation) -> StorageResult<()> {
        self.apply_replication_mutation_impl(mutation).await
    }

    fn replication_apply_parallelism_hint(&self) -> usize {
        REPLICATION_APPLY_PARALLELISM_HINT
    }

    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        self.update_time_to_live_impl(request).await
    }

    async fn describe_time_to_live(
        &self,
        table_name: &TableName,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        self.describe_time_to_live_impl(table_name).await
    }
}
