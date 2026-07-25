use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bg_jobs::BackgroundJobName;
use storage_backfill::{LogicalBackfillExport, LogicalBackfillImport};
use storage_common::{
    GSI_BACKFILL_JOB, GSI_UPDATE_JOB, STREAM_TRIM_JOB, TTL_SWEEP_JOB, apply_gsi_write_pressure,
    ttl::{TtlConfigRecord, ttl_gsi_name},
};
use storage_condition::parse_condition_expression;
use storage_provider::{
    CHANGE_INDEX_MARKER_RETENTION_MS, ChangeIndexMarker, ListChangeIndexMarkersRequest,
    StorageProvider, StorageProviderReadContext, StreamTrimDueMarker, StreamTrimState,
    StreamTrimStateWrite, plan_table_stream_duration, return_values_need_updated_fields,
};
use storage_types::{
    AllOld, AttributeDefinition, AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse,
    BatchWriteItemEncodeRequest, BatchWriteItemRequest, BatchWriteItemResponse, CreateTableRequest,
    DeleteGlobalSecondaryIndexAction, DeleteItemRequest, DurablePointReadProof,
    DurablePointReadRequest, GuardedDeleteItemRequest, GuardedPutItemRequest,
    GuardedTransactWriteItemsRequest, GuardedUpdateItemRequest, KeyAttributeType, KeyAttributes,
    PreparedBatchOperation, PutItemRequest, PutItemResponse, QueryTableRequest,
    ReadSequenceConsistency, ReplicationMutation, ScanTableRequest, StorageEnum, StorageError,
    StorageResult, StoredTableInfo, StreamRetentionDuration, TableName, TableStatus,
    TimeToLiveDescription, TimeToLiveStatus, TimestampMillis, TransactWriteItemsEncodeRequest,
    TransactWriteItemsRequest, TransactWriteItemsResponse, UpdateItemRequest, UpdateItemResponse,
    UpdateTimeToLiveRequest, UpdateTimeToLiveResponse, WireItem, WriteRequest,
};
use tracing::{Span, field, instrument};

use crate::{
    backends::sqlite::{
        delete_item_impl::DeleteItemInput, put_item_impl::PutItemInput,
        update_item_impl::UpdateItemInput,
    },
    billing_metrics::{
        WriteCostTally, attr_map_payload_bytes, record_read_cost, record_write_cost,
        serializable_payload_bytes,
    },
    helpers::MAX_SCAN_LIMIT,
};

pub(crate) fn record_read(items: usize, bytes: usize) {
    let span = Span::current();
    span.record("items_returned", items as u64);
    span.record("bytes_read", bytes as u64);
}

fn record_write(items: usize, bytes: usize) {
    let span = Span::current();
    span.record("items_updated", items as u64);
    span.record("bytes_written", bytes as u64);
}

fn current_ms_u64() -> u64 {
    u64::try_from(*TimestampMillis::now()).unwrap_or(0)
}

fn compute_items_bytes(items: &[HashMap<String, AttributeValue>]) -> StorageResult<usize> {
    let mut total = 0_usize;
    for item in items {
        total += storage_types::storage_serde::to_bytes(item)?.len();
    }
    Ok(total)
}

pub(crate) fn storage_error_to_rusqlite(err: &StorageError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            err.to_string(),
        )),
    )
}

#[expect(clippy::ref_option)]
pub(crate) fn parse_optional_condition(
    condition_expression: Option<String>,
    expression_attribute_names: &Option<HashMap<String, String>>,
    expression_attribute_values: &Option<HashMap<String, AttributeValue>>,
) -> StorageResult<Option<storage_condition::Condition>> {
    let Some(expr) = condition_expression else {
        return Ok(None);
    };
    let parsed = parse_condition_expression(
        &expr,
        expression_attribute_names.as_ref(),
        expression_attribute_values.as_ref(),
    )
    .map_err(|e| {
        tracing::warn!(error = %e, "condition parse failed");
        StorageEnum::ConditionalCheckFailed
    })?;
    Ok(Some(parsed))
}
use crate::{
    backends::{
        prepare_batch_operation,
        sqlite::{
            SQLiteStorageProvider,
            provider::SQLiteSnapshotConnectionLease,
            provider_table_lifecycle::{load_sqlite_table_scope_id, next_table_policy_version},
            stream_duration::write_stream_trim_state_tx,
        },
    },
    batch_write::{BatchWriteTxnState, execute_prepared_batch_operation},
    error_handler::map_sqlite_error,
    sql_statements,
    transaction_manager::with_transaction,
    utils::call_sqlite,
};

impl SQLiteStorageProvider {
    pub(crate) async fn trim_change_index_markers_older_than(
        &self,
        cutoff_created_at_ms: i64,
    ) -> StorageResult<usize> {
        call_sqlite(&self.connection, move |conn| {
            let (sql, params) =
                sql_statements::trim_change_index_markers_older_than(cutoff_created_at_ms);
            conn.execute(sql, params).map_err(map_sqlite_error)
        })
        .await
    }
}

struct SQLiteReadSequenceReadContext {
    provider: SQLiteStorageProvider,
    consistency: ReadSequenceConsistency,
    snapshot_lease: Option<SQLiteSnapshotConnectionLease>,
}

#[async_trait]
impl StorageProviderReadContext for SQLiteReadSequenceReadContext {
    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        self.ensure_supported()?;
        <SQLiteStorageProvider as StorageProvider>::get_item(
            &self.provider,
            table_name,
            key,
            consistent_read,
        )
        .await
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        self.ensure_supported()?;
        <SQLiteStorageProvider as StorageProvider>::batch_get_item(&self.provider, request).await
    }

    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.ensure_supported()?;
        <SQLiteStorageProvider as StorageProvider>::query_table(&self.provider, request).await
    }
}

impl SQLiteReadSequenceReadContext {
    fn ensure_supported(&self) -> StorageResult<()> {
        if self.consistency == ReadSequenceConsistency::Transactional
            && self.snapshot_lease.is_none()
        {
            return Err(sqlite_read_sequence_snapshot_unsupported());
        }
        Ok(())
    }
}

fn sqlite_read_sequence_snapshot_unsupported() -> StorageError {
    StorageError::unsupported(
        "sqlite read-sequence transactional contexts require a file-backed provider snapshot \
         connection",
    )
}

#[async_trait]
impl StorageProvider for SQLiteStorageProvider {
    fn supports_guarded_writes(&self) -> bool {
        true
    }

    fn supports_guarded_transaction_writes(&self) -> bool {
        true
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
        if consistency != ReadSequenceConsistency::Transactional {
            return Ok(Box::new(SQLiteReadSequenceReadContext {
                provider: self.clone(),
                consistency,
                snapshot_lease: None,
            }));
        }

        let Some(pool) = self.snapshot_connection_pool.as_ref() else {
            return Err(sqlite_read_sequence_snapshot_unsupported());
        };
        let snapshot_lease = pool.acquire().await?;
        let mut provider = self.clone();
        provider.connection = Arc::new(snapshot_lease.connection()?.clone());

        Ok(Box::new(SQLiteReadSequenceReadContext {
            provider,
            consistency,
            snapshot_lease: Some(snapshot_lease),
        }))
    }

    async fn write_stream_trim_state(&self, state: StreamTrimState) -> StorageResult<()> {
        self.write_stream_trim_state_sqlite(state).await
    }

    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        self.list_due_stream_trim_markers_sqlite(due_before, limit)
            .await
    }

    async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<ChangeIndexMarker>> {
        let after_versionstamp = request.after_versionstamp.unwrap_or_default();
        let limit = i64::try_from(request.limit)
            .map_err(|_| StorageError::validation("change index list limit exceeds i64"))?;
        call_sqlite(&self.connection, move |conn| {
            let (sql, params) =
                sql_statements::list_change_index_markers(request.slot, &after_versionstamp, limit);
            let mut stmt = conn.prepare(sql).map_err(map_sqlite_error)?;
            let rows = stmt
                .query_map(params, |row| {
                    let slot: i64 = row.get(0)?;
                    let versionstamp: String = row.get(1)?;
                    let table_id: String = row.get(2)?;
                    Ok((slot, versionstamp, table_id))
                })
                .map_err(map_sqlite_error)?;
            let mut markers = Vec::new();
            for row in rows {
                let (slot, versionstamp, table_id) = row.map_err(map_sqlite_error)?;
                markers.push(ChangeIndexMarker {
                    slot: u16::try_from(slot).map_err(|_| {
                        StorageError::internal("change index slot is outside u16 range")
                    })?,
                    versionstamp,
                    table_id: TableName::new(&table_id),
                });
            }
            Ok(markers)
        })
        .await
    }

    async fn initialize_storage(&self) -> StorageResult<()> {
        self.do_initialize_storage().await
    }

    async fn export_logical_backfill_page(
        &self,
        request: storage_backfill::LogicalExportRequest,
    ) -> StorageResult<storage_backfill::LogicalExportPage> {
        LogicalBackfillExport::export_logical_page(self, request).await
    }

    async fn import_logical_backfill_chunk(
        &self,
        manifest: &storage_backfill::LogicalBackfillManifest,
        chunk: storage_backfill::LogicalBackfillChunk,
    ) -> StorageResult<storage_backfill::LogicalBackfillResult> {
        LogicalBackfillImport::import_logical_chunk(self, manifest, chunk).await
    }

    async fn apply_resolved_sync_mutations(
        &self,
        metadata: storage_sync::SyncCommitMetadata,
        batch: storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<Vec<storage_sync::SyncMutationResponse>> {
        crate::backends::sqlite::logical_backfill_sync_store::apply_resolved_sync_mutations(
            self, metadata, batch,
        )
        .await
    }

    async fn last_resolved_sync_log_id(&self) -> StorageResult<Option<storage_sync::SyncLogId>> {
        crate::backends::sqlite::logical_backfill_sync_store::last_resolved_sync_log_id(self).await
    }

    async fn persist_resolved_sync_log_entry(
        &self,
        metadata: &storage_sync::SyncCommitMetadata,
        batch: &storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<()> {
        crate::backends::sqlite::logical_backfill_sync_store::persist_resolved_sync_log_entry(
            self, metadata, batch,
        )
        .await
    }

    async fn get_resolved_sync_log_entry(
        &self,
        log_id: storage_sync::SyncLogId,
    ) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
        crate::backends::sqlite::logical_backfill_sync_store::get_resolved_sync_log_entry(
            self, log_id,
        )
        .await
    }

    async fn resolved_sync_log_entries_after(
        &self,
        log_id: Option<storage_sync::SyncLogId>,
        limit: usize,
    ) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
        crate::backends::sqlite::logical_backfill_sync_store::resolved_sync_log_entries_after(
            self, log_id, limit,
        )
        .await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "table_exists", table_name = %table_name))]
    async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        self.do_table_exists(table_name).await
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "create_table", table_name = %request.table_name))]
    async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        self.do_create_table(request).await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "update_table_status",
            table_name = %table_name,
        )
    )]
    async fn update_table_status(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()> {
        self.do_update_table_status(table_name, status).await
    }

    async fn get_table_info(&self, table_name: &TableName) -> StorageResult<StoredTableInfo> {
        let table_info = self.get_table_info_cached_arc(table_name).await?;
        Ok((*table_info).clone())
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "list_tables"))]
    async fn list_tables(
        &self,
        limit: u32,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<Vec<StoredTableInfo>> {
        self.do_list_tables(limit, exclusive_start_table_name).await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "delete_table",
            table_name = %table_name,
        )
    )]
    async fn delete_table(&self, table_name: &TableName) -> StorageResult<()> {
        self.do_delete_table(table_name).await
    }

    async fn create_table_storage(
        &self,
        table_name: &TableName,
        request: &CreateTableRequest,
    ) -> StorageResult<()> {
        self.do_create_table_storage(table_name, request).await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "put_item",
            table_name = %table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn put_item_encode(
        &self,
        table_name: TableName,
        item: WireItem,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
    ) -> StorageResult<PutItemResponse> {
        self.put_item_encode_with_stream_ttl(
            table_name,
            item,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            None,
        )
        .await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "put_item",
            table_name = %request.table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn put_item_request(&self, request: PutItemRequest) -> StorageResult<PutItemResponse> {
        self.apply_gsi_write_pressure().await?;
        let bytes_written = compute_items_bytes(std::slice::from_ref(&request.item))?;
        let response = self.put_item_internal(request).await?;
        self.maybe_apply_immediate_gsi_updates().await?;
        record_write(1, bytes_written);
        record_write_cost("put_item", "put", 1, bytes_written as u64);
        Ok(response)
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
        self.apply_gsi_write_pressure().await?;
        let bytes_written = item.payload_len();
        let response = self
            .put_item_wire_internal(storage_types::PutItemEncodeRequest {
                table_name,
                item,
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
                return_values,
                return_old_on_condition_failure: false,
                aux_item_stream_ttl_hours,
            })
            .await?;
        self.maybe_apply_immediate_gsi_updates().await?;
        record_write(1, bytes_written);
        record_write_cost("put_item", "put", 1, bytes_written as u64);
        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "get_item",
            table_name = %table_name,
            ddb_read = true,
            items_returned = tracing::field::Empty,
            bytes_read = tracing::field::Empty,
        )
    )]
    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        _consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        if key.is_empty() {
            record_read(0, 0);
            return Ok(None);
        }

        let result = self.get_item_internal(table_name, key).await?;

        if let Some(ref item) = result {
            record_read(1, item.payload_len());
            record_read_cost("get_item", "get", 1, item.payload_len() as u64);
        } else {
            record_read(0, 0);
            record_read_cost("get_item", "get", 1, 0);
        }
        Ok(result)
    }

    async fn get_item_with_durable_proof(
        &self,
        request: DurablePointReadRequest,
    ) -> StorageResult<DurablePointReadProof> {
        if request.key.is_empty() {
            return Err(StorageError::invalid_or_missing_key());
        }

        call_sqlite(&self.connection, move |conn| {
            let sqlite = crate::utils::SqliteConn::Connection(conn);
            Self::do_get_item_with_durable_proof(&request, &sqlite)
        })
        .await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "delete_item",
            table_name = %request.table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn delete_item_request(
        &self,
        request: DeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        if request.key.is_empty() {
            record_write(0, 0);
            return Ok(None);
        }
        self.apply_gsi_write_pressure().await?;
        let key_bytes = attr_map_payload_bytes(&request.key);
        let result = self.delete_item_internal(request).await?;
        self.maybe_apply_immediate_gsi_updates().await?;
        record_write(usize::from(result.is_some()), 0);
        record_write_cost("delete_item", "delete", 1, key_bytes);
        Ok(result)
    }

    async fn guarded_put_item(
        &self,
        request: GuardedPutItemRequest,
    ) -> StorageResult<PutItemResponse> {
        self.apply_gsi_write_pressure().await?;
        let GuardedPutItemRequest {
            table_name,
            item,
            guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
        } = request;
        let condition = parse_optional_condition(
            condition_expression,
            &expression_attribute_names,
            &expression_attribute_values,
        )?;
        let table_info = self.get_table_info_internal(&table_name).await?;
        let key_attributes =
            StorageProvider::get_key_attributes(self, &item, &table_info.key_schema)?;
        let should_return_old = matches!(return_values, Some(AllOld::AllOld));
        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        let item_for_write = item.clone();
        let old_value = with_transaction(&self.connection, move |sqlite| {
            Self::validate_durable_guard(&table_name, &key_attributes, &guard, sqlite)?;
            Self::do_put_item(
                sqlite,
                PutItemInput {
                    table_name: &table_name,
                    item: &item_for_write,
                    condition: &condition,
                    immediate_gsi_consistency,
                    return_old_on_condition_failure: false,
                    replication: None,
                    item_stream_ttl_hours: None,
                },
            )
        })
        .await?;
        let attributes = if should_return_old { old_value } else { None };
        Ok(PutItemResponse {
            attributes: attributes.map(Into::into),
        })
    }

    async fn guarded_delete_item(
        &self,
        request: GuardedDeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        self.apply_gsi_write_pressure().await?;
        let GuardedDeleteItemRequest {
            table_name,
            key,
            guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        } = request;
        let condition = parse_optional_condition(
            condition_expression,
            &expression_attribute_names,
            &expression_attribute_values,
        )?;
        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        with_transaction(&self.connection, move |sqlite| {
            Self::validate_durable_guard(&table_name, &key, &guard, sqlite)?;
            Self::do_delete_item(
                sqlite,
                DeleteItemInput {
                    table_name: &table_name,
                    key: &key,
                    condition: &condition,
                    immediate_gsi_consistency,
                    return_old_on_condition_failure: false,
                    replication: None,
                    item_stream_ttl_hours: None,
                },
            )
        })
        .await
    }

    async fn guarded_update_item(
        &self,
        request: GuardedUpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        self.apply_gsi_write_pressure().await?;
        let GuardedUpdateItemRequest { request, guard } = request;
        let UpdateItemRequest {
            table_name,
            key,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            aux_item_stream_ttl_hours,
            ..
        } = request;
        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        let collect_response_fields = return_values_need_updated_fields(return_values.as_ref());
        let (old_item, new_item, response_fields) =
            with_transaction(&self.connection, move |sqlite| {
                let (operations, condition) = storage_provider::before_update_item_optional(
                    update_expression.as_deref(),
                    condition_expression.as_deref(),
                    expression_attribute_names.as_ref(),
                    expression_attribute_values.as_ref(),
                )?;
                let response_fields = if collect_response_fields {
                    {
                        operations
                            .iter()
                            .map(|operation| operation.field_name_arc())
                            .collect::<Vec<_>>()
                    }
                } else {
                    Default::default()
                };
                Self::validate_durable_guard(&table_name, &key, &guard, sqlite)?;
                Self::do_update_item(
                    sqlite,
                    UpdateItemInput {
                        operations: &operations,
                        condition: &condition,
                        table_name: &table_name,
                        key: &key,
                        immediate_gsi_consistency,
                        return_old_on_condition_failure: false,
                        item_stream_ttl_hours: aux_item_stream_ttl_hours,
                    },
                )
                .map(|(old_item, new_item)| (old_item, new_item, response_fields))
            })
            .await?;

        storage_provider::update_item_response(
            &response_fields,
            Some(old_item),
            Some(new_item),
            return_values.as_ref(),
        )
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "scan_table",
            table_name = %request.table_name,
            index_name = tracing::field::Empty,
            ddb_read = true,
            items_returned = tracing::field::Empty,
            bytes_read = tracing::field::Empty,
            req_limit = tracing::field::Empty,
        )
    )]
    async fn scan_table(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.do_scan_table(request).await
    }

    async fn scan_table_with_item_stream_versions(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<storage_types::ItemVersionedWireItem>, Option<String>)> {
        self.do_scan_table_with_item_stream_versions(request).await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "query_table",
            table_name = %request.table_name,
            index_name = tracing::field::Empty,
            ddb_read = true,
            items_returned = tracing::field::Empty,
            bytes_read = tracing::field::Empty,
            req_limit = tracing::field::Empty,
            scan_forward = tracing::field::Empty,
        )
    )]
    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.do_query_table(request).await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "batch_write_item",
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
            table_count = tracing::field::Empty,
            total_requests = tracing::field::Empty,
            stream = tracing::field::Empty,
        )
    )]
    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        self.apply_gsi_write_pressure().await?;
        let total_reqs: usize = request.request_items.values().map(Vec::len).sum();
        let mut requested_tally = WriteCostTally::default();
        for write_requests in request.request_items.values() {
            for write_request in write_requests {
                requested_tally.record_write_request(write_request);
            }
        }
        let span = Span::current();
        span.record("table_count", request.request_items.len() as u64);
        span.record("total_requests", total_reqs as u64);
        span.record("stream", field::display(should_write_to_stream));
        // Prepare all operations with item splitting outside the transaction
        let mut prepared_operations = Vec::new();

        for (table_name, write_requests) in request.request_items {
            let table_info = self.get_table_info(&table_name).await?;

            for write_request in write_requests {
                let prepared_op = prepare_batch_operation(&table_info, write_request)?;
                prepared_operations.push(prepared_op);
            }
        }

        let (unprocessed_items, applied_items, applied_bytes) =
            with_transaction(&self.connection, move |sqlite| {
                let mut unprocessed_by_table: HashMap<TableName, Vec<WriteRequest>> =
                    HashMap::new();
                let mut applied_items = 0usize;
                let mut applied_bytes = 0usize;
                let mut batch_state = BatchWriteTxnState::default();

                for operation in &prepared_operations {
                    match execute_prepared_batch_operation(
                        sqlite,
                        operation,
                        &mut batch_state,
                        should_write_to_stream,
                    ) {
                        Ok(()) => match operation {
                            PreparedBatchOperation::Put { full_item, .. } => {
                                applied_items += 1;
                                applied_bytes +=
                                    compute_items_bytes(std::slice::from_ref(full_item))?;
                            }
                            PreparedBatchOperation::Delete { .. } => {
                                applied_items += 1;
                            }
                        },
                        Err(_e) => match operation {
                            PreparedBatchOperation::Put {
                                table_name,
                                write_request,
                                ..
                            }
                            | PreparedBatchOperation::Delete {
                                table_name,
                                write_request,
                                ..
                            } => {
                                unprocessed_by_table
                                    .entry(table_name.clone())
                                    .or_default()
                                    .push(write_request.clone());
                            }
                        },
                    }
                }

                Ok::<_, StorageError>((unprocessed_by_table, applied_items, applied_bytes))
            })
            .await?;

        let response = BatchWriteItemResponse {
            unprocessed_items: if unprocessed_items.is_empty() {
                None
            } else {
                Some(unprocessed_items)
            },
            item_collection_metrics: None,
            consumed_capacity: None,
        };

        self.maybe_apply_immediate_gsi_updates().await?;
        record_write(applied_items, applied_bytes);
        let mut unprocessed_tally = WriteCostTally::default();
        if let Some(unprocessed_items) = response.unprocessed_items.as_ref() {
            for write_requests in unprocessed_items.values() {
                for write_request in write_requests {
                    unprocessed_tally.record_write_request(write_request);
                }
            }
        }
        requested_tally
            .subtract(&unprocessed_tally)
            .emit("batch_write_item");

        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "batch_write_item",
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
            table_count = tracing::field::Empty,
            total_requests = tracing::field::Empty,
            stream = tracing::field::Empty,
        )
    )]
    async fn batch_write_item_encode(
        &self,
        request: BatchWriteItemEncodeRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        let mapped = BatchWriteItemRequest::try_from(request)?;
        self.batch_write_item(mapped, should_write_to_stream).await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "update_item",
            table_name = %request.table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
        self.apply_gsi_write_pressure().await?;
        let billed_bytes = serializable_payload_bytes(&request);
        let response = self.update_item_internal(request).await?;
        self.maybe_apply_immediate_gsi_updates().await?;
        record_write(1, 0);
        record_write_cost("update_item", "update", 1, billed_bytes);
        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "batch_get_item",
            ddb_read = true,
            items_returned = tracing::field::Empty,
            bytes_read = tracing::field::Empty,
            table_count = tracing::field::Empty,
            total_keys = tracing::field::Empty,
        )
    )]
    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        self.do_batch_get_item(request).await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "transact_write_items",
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
            action_count = tracing::field::Empty,
        )
    )]
    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.apply_gsi_write_pressure().await?;
        Span::current().record("action_count", request.transact_items.len() as u64);
        let mut billed_tally = WriteCostTally::default();
        for item in &request.transact_items {
            billed_tally.record_transact_item(item);
        }

        let mut total_items_updated = 0usize;
        let mut total_bytes_written = 0usize;

        for item in &request.transact_items {
            if item.put.is_some() || item.delete.is_some() || item.update.is_some() {
                total_items_updated += 1;
            }
            if let Some(put_request) = &item.put {
                total_bytes_written +=
                    compute_items_bytes(std::slice::from_ref(&put_request.item))?;
            }
        }

        let response = self.do_transact_write_items(request).await?;
        self.maybe_apply_immediate_gsi_updates().await?;
        record_write(total_items_updated, total_bytes_written);
        billed_tally.emit("transact_write_items");
        Ok(response)
    }

    async fn guarded_transact_write_items(
        &self,
        request: GuardedTransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.apply_gsi_write_pressure().await?;
        Span::current().record("action_count", request.request.transact_items.len() as u64);
        let mut billed_tally = WriteCostTally::default();
        for item in &request.request.transact_items {
            billed_tally.record_transact_item(item);
        }

        let mut total_items_updated = 0usize;
        let mut total_bytes_written = 0usize;

        for item in &request.request.transact_items {
            if item.put.is_some() || item.delete.is_some() || item.update.is_some() {
                total_items_updated += 1;
            }
            if let Some(put_request) = &item.put {
                total_bytes_written +=
                    compute_items_bytes(std::slice::from_ref(&put_request.item))?;
            }
        }

        let response = self.do_guarded_transact_write_items(request).await?;
        self.maybe_apply_immediate_gsi_updates().await?;
        record_write(total_items_updated, total_bytes_written);
        billed_tally.emit("guarded_transact_write_items");
        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "transact_write_items",
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
            action_count = tracing::field::Empty,
        )
    )]
    async fn transact_write_items_encode(
        &self,
        request: TransactWriteItemsEncodeRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.apply_gsi_write_pressure().await?;
        Span::current().record("action_count", request.transact_items.len() as u64);
        let mut billed_tally = WriteCostTally::default();
        for item in &request.transact_items {
            billed_tally.record_transact_encode_item(item);
        }

        let mut total_items_updated = 0usize;
        let mut total_bytes_written = 0usize;

        for item in &request.transact_items {
            if item.put.is_some() || item.delete.is_some() || item.update.is_some() {
                total_items_updated += 1;
            }
            if let Some(put_request) = &item.put {
                total_bytes_written += put_request.item.payload_len();
            }
        }

        let response = self.do_transact_write_items_encode(request).await?;
        self.maybe_apply_immediate_gsi_updates().await?;
        record_write(total_items_updated, total_bytes_written);
        billed_tally.emit("transact_write_items");
        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "update_table",
            table = %request.table_name,
            gsi_updates = request
                .global_secondary_index_updates
                .as_ref()
                .map_or(0, Vec::len),
            has_stream_spec = request.stream_specification.is_some(),
        )
    )]
    async fn update_table(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse> {
        let table_name = request.table_name.clone();
        let mut table_info = self.get_table_info(&table_name).await?;

        // Transition to UPDATING
        self.update_table_status(&table_name, TableStatus::Updating)
            .await?;

        // Apply StreamSpecification update if present
        if let Some(spec) = request.stream_specification.clone() {
            table_info.stream_specification = Some(spec.clone());
            let table_name_clone = table_name.clone();
            let spec_json = serde_json::to_string(&table_info.stream_specification).ok();
            call_sqlite(&self.connection, move |conn| {
                let (sql, params) =
                    sql_statements::update_stream_specification(&table_name_clone, &spec_json);
                conn.execute(sql, params).map_err(map_sqlite_error)
            })
            .await?;
        }

        if let Some(deletion_protection_enabled) = request.deletion_protection_enabled {
            table_info.deletion_protection_enabled = deletion_protection_enabled;
            let table_name_clone = table_name.clone();
            call_sqlite(&self.connection, move |conn| {
                let (sql, params) = sql_statements::update_deletion_protection(
                    &table_name_clone,
                    deletion_protection_enabled,
                );
                conn.execute(sql, params).map_err(map_sqlite_error)
            })
            .await?;
        }

        if request.aux_stream_duration_hours.is_some()
            || request.aux_default_item_stream_duration_hours.is_some()
        {
            if let Some(table_stream_duration) = request.aux_stream_duration_hours {
                table_info.table_stream_duration = table_stream_duration;
            }
            if let Some(default_item_stream_duration) =
                request.aux_default_item_stream_duration_hours
            {
                table_info.default_item_stream_duration = default_item_stream_duration;
            }
            let table_name_clone = table_name.clone();
            let table_stream_duration = table_info.table_stream_duration;
            let default_item_stream_duration = table_info.default_item_stream_duration;
            let table_scope_id = load_sqlite_table_scope_id(self, &table_name).await?;
            let policy_version = next_table_policy_version(self, &table_scope_id).await?;
            let table_duration_plan = plan_table_stream_duration(
                table_name.clone(),
                table_scope_id,
                policy_version,
                table_stream_duration,
                default_item_stream_duration,
                TimestampMillis::now(),
            );
            call_sqlite(&self.connection, move |conn| {
                let tx = conn.transaction().map_err(map_sqlite_error)?;
                let (sql, params) = sql_statements::update_stream_durations(
                    &table_name_clone,
                    table_stream_duration,
                    default_item_stream_duration,
                );
                tx.execute(sql, params).map_err(map_sqlite_error)?;
                write_stream_trim_state_tx(
                    &tx,
                    StreamTrimStateWrite {
                        state: table_duration_plan.trim_state,
                        next_marker: table_duration_plan.due_marker,
                    },
                )?;
                tx.commit().map_err(map_sqlite_error)?;
                Ok(())
            })
            .await?;
        }

        if let Some(gsi_updates) = request.global_secondary_index_updates.clone() {
            crate::gsi_lifecycle::process_gsi_updates(
                self,
                &mut table_info,
                &table_name,
                gsi_updates,
            )
            .await?;
        }

        // Back to ACTIVE
        self.update_table_status(&table_name, TableStatus::Active)
            .await?;

        let resp = storage_types::UpdateTableResponse {
            table_description: storage_types::TableDescription {
                table_name: table_info.table_name.clone(),
                table_status: TableStatus::Active,
                created_at: table_info.created_at.into(),
                attribute_definitions: table_info.attribute_definitions.clone(),
                key_schema: table_info.key_schema.clone(),
                table_size_bytes: table_info.table_size_bytes,
                item_count: table_info.item_count,
                table_arn: format!(
                    "arn:aws:dynamodb:us-east-1:123456789012:table/{}",
                    table_info.table_name
                ),
                replicas: None,
                multi_region_consistency: None,
                billing_mode_summary: Some(storage_types::BillingModeSummary {
                    billing_mode: Some(storage_types::BillingMode::PayPerRequest),
                    last_update_to_pay_per_request_date_time: None,
                }),
                global_secondary_indexes: table_info.global_secondary_indexes.clone().map(
                    |indexes| {
                        indexes
                            .into_iter()
                            .map(|index| storage_types::GlobalSecondaryIndexDescription {
                                index_name: index.index_name,
                                key_schema: index.key_schema,
                                projection: index.projection,
                                index_status: None,
                                backfilling: None,
                                provisioned_throughput: None,
                                index_size_bytes: None,
                                item_count: None,
                                index_arn: None,
                            })
                            .collect()
                    },
                ),
                local_secondary_indexes: None,
                provisioned_throughput: None,
                stream_specification: table_info.stream_specification.clone(),
                latest_stream_arn: None,
                latest_stream_label: None,
                deletion_protection_enabled: table_info.deletion_protection_enabled,
            },
        };

        Ok(resp)
    }

    async fn apply_replication_mutation(&self, mutation: ReplicationMutation) -> StorageResult<()> {
        let table_name = mutation.table_name.clone();
        let metadata = mutation.metadata.clone();

        if let Some(new_image) = mutation.new_image {
            let bytes_written = compute_items_bytes(std::slice::from_ref(&new_image))?;
            let immediate_gsi_consistency = self.immediate_gsi_consistency;
            with_transaction(&self.connection, move |sqlite| {
                crate::SQLiteStorageProvider::do_put_item(
                    sqlite,
                    PutItemInput {
                        table_name: &table_name,
                        item: &new_image,
                        condition: &None,
                        immediate_gsi_consistency,
                        return_old_on_condition_failure: false,
                        replication: Some(&metadata),
                        item_stream_ttl_hours: None,
                    },
                )
                .map(|_| ())
            })
            .await?;
            self.maybe_apply_immediate_gsi_updates().await?;
            record_write(1, bytes_written);
            return Ok(());
        }

        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        with_transaction(&self.connection, move |sqlite| {
            crate::SQLiteStorageProvider::do_delete_item(
                sqlite,
                DeleteItemInput {
                    table_name: &table_name,
                    key: &mutation.key,
                    condition: &None,
                    immediate_gsi_consistency,
                    return_old_on_condition_failure: false,
                    replication: Some(&metadata),
                    item_stream_ttl_hours: None,
                },
            )
            .map(|_| ())
        })
        .await?;
        self.maybe_apply_immediate_gsi_updates().await?;
        record_write(1, 0);
        Ok(())
    }

    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        let UpdateTimeToLiveRequest {
            table_name,
            mut time_to_live_specification,
        } = request;

        let mut table_info = self.get_table_info(&table_name).await?;
        let enabled = time_to_live_specification.enabled;
        let attribute_name = time_to_live_specification.attribute_name.clone();
        let existing_config = self.load_ttl_config(&table_name).await?;

        if enabled {
            if attribute_name.trim().is_empty() {
                return Err(StorageError::validation(
                    "Time to live attribute name must not be empty",
                ));
            }

            if let Some(config) = existing_config.as_ref() {
                if matches!(
                    config.status,
                    TimeToLiveStatus::Enabling | TimeToLiveStatus::Disabling
                ) {
                    return Err(StorageError::validation(
                        "Time to live configuration update in progress; retry later",
                    ));
                }

                if config.status == TimeToLiveStatus::Enabled {
                    if config.attribute_name == attribute_name {
                        return Ok(UpdateTimeToLiveResponse {
                            time_to_live_specification,
                        });
                    }

                    return Err(StorageError::validation(
                        "Disable time to live before changing attribute name",
                    ));
                }
            }

            ensure_attribute_definition(&mut table_info, &attribute_name, KeyAttributeType::N)?;

            persist_attribute_definitions(self, &table_name, &table_info).await?;
            self.invalidate_table_info_cache(&table_name).await;

            self.create_ttl_index_table(&table_name).await?;

            let gsi_name = ttl_gsi_name(&table_name);
            let mut config = TtlConfigRecord::new(
                attribute_name.clone(),
                &gsi_name,
                TimeToLiveStatus::Enabling,
            );
            config.touch();
            self.save_ttl_config(&table_name, &config).await?;

            self.backfill_ttl_index(&table_name, &table_info, &attribute_name)
                .await?;
            config.status = TimeToLiveStatus::Enabled;
            config.touch();
            self.save_ttl_config(&table_name, &config).await?;

            Ok(UpdateTimeToLiveResponse {
                time_to_live_specification,
            })
        } else {
            if let Some(config) = existing_config {
                let delete = DeleteGlobalSecondaryIndexAction {
                    index_name: config.gsi_name(),
                };
                crate::gsi_lifecycle::apply_gsi_delete(self, &mut table_info, &table_name, delete)
                    .await?;
                self.invalidate_table_info_cache(&table_name).await;

                self.drop_ttl_index_table(&table_name).await?;

                self.delete_ttl_config(&table_name).await?;
                time_to_live_specification.attribute_name = config.attribute_name;
            }

            time_to_live_specification.enabled = false;
            Ok(UpdateTimeToLiveResponse {
                time_to_live_specification,
            })
        }
    }

    async fn describe_time_to_live(
        &self,
        table_name: &TableName,
    ) -> StorageResult<storage_types::DescribeTimeToLiveResponse> {
        let _ = self.get_table_info(table_name).await?;

        let description = match self.load_ttl_config(table_name).await? {
            Some(config) => TimeToLiveDescription {
                attribute_name: Some(config.attribute_name),
                time_to_live_status: config.status,
            },
            None => TimeToLiveDescription {
                attribute_name: None,
                time_to_live_status: TimeToLiveStatus::Disabled,
            },
        };

        Ok(storage_types::DescribeTimeToLiveResponse {
            time_to_live_description: Some(description),
        })
    }

    async fn run_job(&self, name: BackgroundJobName) -> StorageResult<()> {
        match name {
            GSI_UPDATE_JOB => {
                if self.immediate_gsi_consistency {
                    return Ok(());
                }
                // Repeatedly process GSI update batches until queue drained.
                loop {
                    let progressed = self
                        .process_gsi_updates()
                        .await
                        .map_err(|e| StorageError::internal(&e.to_string()))?;
                    if !progressed {
                        break;
                    }
                }
            }
            TTL_SWEEP_JOB => loop {
                let cutoff_created_at_ms = TimestampMillis::now()
                    .timestamp_millis()
                    .saturating_sub(CHANGE_INDEX_MARKER_RETENTION_MS);
                self.trim_change_index_markers_older_than(cutoff_created_at_ms)
                    .await?;
                let progressed = self.run_ttl_sweep().await?;
                if !progressed {
                    break;
                }
            },
            STREAM_TRIM_JOB => loop {
                let progressed = self.run_stream_trim().await?;
                if !progressed {
                    break;
                }
            },
            GSI_BACKFILL_JOB => {
                // Drain backfill phases (may move from Backfilling -> CatchingUp -> Done)
                loop {
                    let progressed = self
                        .process_gsi_backfills()
                        .await
                        .map_err(|e| StorageError::internal(&e.to_string()))?;
                    if !progressed {
                        break;
                    }
                }
            }
            _ => {
                // Unknown job; ignore for forward compatibility.
            }
        }
        Ok(())
    }
}

impl SQLiteStorageProvider {
    async fn apply_gsi_write_pressure(&self) -> StorageResult<()> {
        apply_gsi_write_pressure(
            self.immediate_gsi_consistency,
            &self.gsi_propagation_governor,
            current_ms_u64(),
        )
        .await
    }

    async fn maybe_apply_immediate_gsi_updates(&self) -> StorageResult<()> {
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn get_item_map<K>(
        &self,
        table_name: TableName,
        key: K,
        consistent_read: bool,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>
    where
        K: Into<KeyAttributes>,
    {
        let item =
            <Self as StorageProvider>::get_item(self, table_name, key.into(), consistent_read)
                .await?;
        item.map(WireItem::into_attribute_map).transpose()
    }

    #[cfg(test)]
    pub(crate) async fn scan_table_maps(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, last_evaluated_key) =
            <Self as StorageProvider>::scan_table(self, request).await?;
        let mut decoded = Vec::with_capacity(items.len());
        for item in items {
            decoded.push(item.into_attribute_map()?);
        }
        Ok((decoded, last_evaluated_key))
    }

    #[cfg(test)]
    pub async fn scan_table(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        self.scan_table_maps(request).await
    }

    #[cfg(test)]
    pub async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, last_evaluated_key) =
            <Self as StorageProvider>::query_table(self, request).await?;
        let mut decoded = Vec::with_capacity(items.len());
        for item in items {
            decoded.push(item.into_attribute_map()?);
        }
        Ok((decoded, last_evaluated_key))
    }

    #[cfg(test)]
    pub async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<storage_types::BatchGetItemResponse> {
        let response = <Self as StorageProvider>::batch_get_item(self, request).await?;
        decode_batch_get_response_to_maps(response)
    }

    async fn backfill_ttl_index(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        ttl_attribute: &str,
    ) -> StorageResult<()> {
        use storage_common::ttl::{
            normalize_ttl_seconds, ttl_index_key_token_for_item, ttl_value_from_item,
        };
        use storage_types::ScanTableRequest;

        let mut exclusive_start_key: Option<String> = None;
        let ttl_table = crate::naming::physical_ttl_index_table_name(table_name);
        let ttl_attr = ttl_attribute.to_string();
        let table_info_clone = table_info.clone();
        let table_for_scan = table_name.clone();

        loop {
            let (wire_items, lek) = <Self as StorageProvider>::scan_table(
                self,
                &ScanTableRequest {
                    table_name: table_for_scan.clone(),
                    index_name: None,
                    limit: Some(MAX_SCAN_LIMIT),
                    exclusive_start_key: exclusive_start_key.clone(),
                    consistent_read: true,
                },
            )
            .await?;

            if wire_items.is_empty() {
                break;
            }

            let ttl_table_clone = ttl_table.clone();
            let table_info_for_insert = table_info_clone.clone();
            let ttl_attr_clone = ttl_attr.clone();
            with_transaction(&self.connection, move |sqlite| {
                for wire_item in &wire_items {
                    let item = wire_item.to_attribute_map()?;
                    let Some(ttl_value) = ttl_value_from_item(&item, &ttl_attr_clone) else {
                        continue;
                    };
                    let normalized = i64::try_from(normalize_ttl_seconds(ttl_value))
                        .map_err(|_| StorageError::internal("sqlite ttl normalize overflow"))?;
                    let token = ttl_index_key_token_for_item(&table_info_for_insert, &item)?;
                    let sql = format!(
                        "INSERT OR REPLACE INTO \"{ttl_table_clone}\" (ttl_value, key_token) \
                         VALUES (?1, ?2)"
                    );
                    sqlite
                        .execute(&sql, rusqlite::params![normalized as i64, token])
                        .map_err(map_sqlite_error)?;
                }
                Ok::<(), StorageError>(())
            })
            .await?;

            exclusive_start_key = lek;
            if exclusive_start_key.is_none() {
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
fn decode_batch_get_response_to_maps(
    response: BatchGetWireItemResponse,
) -> StorageResult<storage_types::BatchGetItemResponse> {
    let responses = if let Some(table_items) = response.responses {
        let mut decoded = HashMap::with_capacity(table_items.len());
        for (table, items) in table_items {
            let mut table_rows = Vec::with_capacity(items.len());
            for item in items {
                table_rows.push(item.into_attribute_map()?.into());
            }
            decoded.insert(table, table_rows);
        }
        Some(decoded)
    } else {
        None
    };

    Ok(storage_types::BatchGetItemResponse {
        responses,
        unprocessed_keys: response.unprocessed_keys,
        consumed_capacity: response.consumed_capacity,
    })
}

async fn persist_attribute_definitions(
    provider: &SQLiteStorageProvider,
    table_name: &TableName,
    table_info: &StoredTableInfo,
) -> StorageResult<()> {
    let defs_json = serde_json::to_string(&table_info.attribute_definitions)
        .map_err(|e| StorageError::internal(&format!("serialize defs: {e}")))?;
    let table_clone = table_name.clone();
    call_sqlite(&provider.connection, move |conn| {
        let (sql, params) = sql_statements::update_attribute_definitions(&table_clone, &defs_json);
        conn.execute(sql, params).map_err(map_sqlite_error)
    })
    .await?;
    Ok(())
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "Matches DynamoDB parity helpers that may become fallible"
)]
fn ensure_attribute_definition(
    table_info: &mut StoredTableInfo,
    attribute_name: &str,
    attribute_type: KeyAttributeType,
) -> StorageResult<()> {
    if !table_info
        .attribute_definitions
        .iter()
        .any(|def| def.attribute_name == attribute_name)
    {
        table_info.attribute_definitions.push(AttributeDefinition {
            attribute_name: attribute_name.to_string(),
            attribute_type,
        });
    }
    Ok(())
}

// run_read_operation removed in favor of unified execute_unified_read path.
