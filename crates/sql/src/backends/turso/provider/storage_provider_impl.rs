use std::{collections::HashMap, sync::LazyLock};

use async_trait::async_trait;
use storage_backfill::{LogicalBackfillExport, LogicalBackfillImport};
use storage_common::{
    GSI_UPDATE_JOB, TTL_SWEEP_JOB, apply_gsi_write_pressure as apply_shared_gsi_write_pressure,
    normalize_limit as calc_limit,
};
use storage_condition::{evaluate_condition, parse_condition_expression};
use storage_provider::{
    CHANGE_INDEX_MARKER_RETENTION_MS, ChangeIndexMarker, ListChangeIndexMarkersRequest,
    StorageProvider, StorageProviderReadContext, StreamDurationTrimBackend,
    StreamDurationTrimConfig, StreamDurationTrimPageRequest, StreamDurationTrimPageResult,
    StreamDurationTrimWorker, StreamTrimDueMarker, StreamTrimScope, StreamTrimScopeBoundaries,
    StreamTrimState, StreamTrimStateWrite, apply_bound_update_operations, before_update_item,
    before_update_item_optional, plan_table_stream_duration, return_values_need_updated_fields,
    split_item_into_key_and_attributes_sync, update_item_response,
};
use storage_types::{
    AllOld, AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse, BatchWriteItemRequest,
    BatchWriteItemResponse, CreateTableRequest, DeleteItemRequest, DurableAbsenceProof,
    DurableItemRevision, DurablePointReadProof, DurablePointReadRequest, GuardedDeleteItemRequest,
    GuardedPutItemRequest, GuardedUpdateItemRequest, ItemVersionedWireItem, KeyAttributes,
    PreparedBatchOperation, PutItemRequest, PutItemResponse, QueryTableRequest,
    ReadSequenceConsistency, ReplicationMutation, ScanTableRequest, StorageError, StorageResult,
    StoredTableInfo, TableName, TableStatus, TimestampMillis, TransactWriteItem,
    TransactWriteItemsRequest, TransactWriteItemsResponse, UpdateItemRequest, UpdateItemResponse,
    WireItem,
};
use turso::{Connection as TursoConnection, Value as TursoValue};

use crate::{
    backends::{
        prepare_batch_operation,
        turso::{
            provider::{
                TursoDeleteItemInput, TursoSqlConnection, TursoStorageProvider, gsi_table_name,
                map_turso_error, option_string_to_value, row_to_table_info, value_to_i64,
                value_to_string,
            },
            sql_statements,
        },
    },
    constants::{DEFAULT_QUERY_LIMIT, DEFAULT_SCAN_LIMIT, MAX_QUERY_LIMIT, MAX_SCAN_LIMIT},
    errors::missing_index_error,
    helpers::decode_exclusive_start,
    parse_conditions::parse_key_condition_expression,
    provider_core::{
        table_lifecycle::{prepare_table_metadata, validate_create_table_request},
        transaction::{
            TransactionKeyPreflight, all_old, conditional_check_failed_reason,
            preflight_transact_item_key_with_table_info, transact_item_table_name,
            transaction_canceled_for_indexed_reasons, transaction_canceled_for_item_error_with_len,
            transaction_canceled_for_preflights, transaction_canceled_for_reason,
            transaction_cancellation_reason_at, validate_no_duplicate_transact_item_keys,
            validate_transact_key, validate_transact_put_item_key,
        },
        write::plan_update_from_existing_item,
    },
    sql_builder::build_sql_query,
    utils::{SqliteTableRowidMode, build_gsi_creation_sqls, build_table_creation_sql},
};

fn condition_item_ref(
    old_item: Option<&HashMap<String, AttributeValue>>,
) -> &HashMap<String, AttributeValue> {
    static EMPTY_ITEM: LazyLock<HashMap<String, AttributeValue>> = LazyLock::new(HashMap::new);
    old_item.unwrap_or(&EMPTY_ITEM)
}

fn current_ms_u64() -> u64 {
    u64::try_from(*TimestampMillis::now()).unwrap_or(0)
}

async fn apply_gsi_write_pressure(provider: &TursoStorageProvider) -> StorageResult<()> {
    apply_shared_gsi_write_pressure(
        provider.immediate_gsi_consistency,
        &provider.gsi_propagation_governor,
        current_ms_u64(),
    )
    .await
}

mod batch_transaction;
mod item_writes;
mod lifecycle;
mod query;
mod table_writes;
mod transaction_helpers;
mod ttl;

#[async_trait]
impl StorageProvider for TursoStorageProvider {
    fn supports_guarded_writes(&self) -> bool {
        self.supports_guarded_writes_operation()
    }

    fn supports_custom_stream_duration(&self) -> bool {
        self.supports_custom_stream_duration_operation()
    }

    fn supports_change_index(&self) -> bool {
        self.supports_change_index_operation()
    }

    async fn begin_read_sequence_read_context(
        &self,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<Box<dyn StorageProviderReadContext>> {
        self.begin_read_sequence_read_context_operation(consistency)
            .await
    }

    async fn write_stream_trim_state(
        &self,
        state: storage_provider::StreamTrimState,
    ) -> StorageResult<()> {
        self.write_stream_trim_state_operation(state).await
    }

    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<storage_provider::StreamTrimDueMarker>> {
        self.list_due_stream_trim_markers_operation(due_before, limit)
            .await
    }

    async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<ChangeIndexMarker>> {
        self.list_change_index_markers_operation(request).await
    }

    async fn initialize_storage(&self) -> StorageResult<()> {
        self.initialize_storage_operation().await
    }

    async fn export_logical_backfill_page(
        &self,
        request: storage_backfill::LogicalExportRequest,
    ) -> StorageResult<storage_backfill::LogicalExportPage> {
        self.export_logical_backfill_page_operation(request).await
    }

    async fn import_logical_backfill_chunk(
        &self,
        manifest: &storage_backfill::LogicalBackfillManifest,
        chunk: storage_backfill::LogicalBackfillChunk,
    ) -> StorageResult<storage_backfill::LogicalBackfillResult> {
        self.import_logical_backfill_chunk_operation(manifest, chunk)
            .await
    }

    async fn apply_resolved_sync_mutations(
        &self,
        metadata: storage_sync::SyncCommitMetadata,
        batch: storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<Vec<storage_sync::SyncMutationResponse>> {
        self.apply_resolved_sync_mutations_operation(metadata, batch)
            .await
    }

    async fn last_resolved_sync_log_id(&self) -> StorageResult<Option<storage_sync::SyncLogId>> {
        self.last_resolved_sync_log_id_operation().await
    }

    async fn persist_resolved_sync_log_entry(
        &self,
        metadata: &storage_sync::SyncCommitMetadata,
        batch: &storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<()> {
        self.persist_resolved_sync_log_entry_operation(metadata, batch)
            .await
    }

    async fn get_resolved_sync_log_entry(
        &self,
        log_id: storage_sync::SyncLogId,
    ) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
        self.get_resolved_sync_log_entry_operation(log_id).await
    }

    async fn resolved_sync_log_entries_after(
        &self,
        log_id: Option<storage_sync::SyncLogId>,
        limit: usize,
    ) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
        self.resolved_sync_log_entries_after_operation(log_id, limit)
            .await
    }

    async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        self.table_exists_operation(table_name).await
    }

    async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        self.create_table_operation(request).await
    }

    async fn update_table_status(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()> {
        let conn = self.connect().await?;
        let status: String = (&status).into();
        let _ = self
            .execute(
                &conn,
                sql_statements::update_table_status(),
                vec![
                    TursoValue::Text(status),
                    TursoValue::Text(table_name.to_string()),
                ],
            )
            .await?;
        self.invalidate_table_cache(table_name).await;
        Ok(())
    }

    async fn get_table_info(&self, table_name: &TableName) -> StorageResult<StoredTableInfo> {
        self.load_table_info_cached(table_name).await
    }

    async fn list_tables(
        &self,
        limit: u32,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<Vec<StoredTableInfo>> {
        let conn = self.connect().await?;
        let rows = if let Some(start_name) = exclusive_start_table_name.map(|name| name.to_string())
        {
            self.query_rows(
                &conn,
                sql_statements::list_tables_after(),
                vec![
                    TursoValue::Text(start_name),
                    TursoValue::Integer(i64::from(limit)),
                ],
            )
            .await?
        } else {
            self.query_rows(
                &conn,
                sql_statements::list_all_tables(),
                vec![TursoValue::Integer(i64::from(limit))],
            )
            .await?
        };

        rows.into_iter()
            .map(|row| row_to_table_info(&row))
            .collect()
    }

    async fn delete_table(&self, table_name: &TableName) -> StorageResult<()> {
        self.delete_table_operation(table_name).await
    }

    async fn create_table_storage(
        &self,
        _table_name: &TableName,
        _request: &CreateTableRequest,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn put_item_request(&self, request: PutItemRequest) -> StorageResult<PutItemResponse> {
        self.put_item_request_operation(request).await
    }

    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        _consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        let table_info = self.get_table_info(&table_name).await?;
        let conn = self.connect().await?;
        let item = self.get_item_map_by_key(&conn, &table_info, &key).await?;
        item.map(|map| WireItem::from_attribute_map(&map))
            .transpose()
    }

    async fn scan_table_with_item_stream_versions(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<ItemVersionedWireItem>, Option<String>)> {
        let table_info = self.get_table_info(&request.table_name).await?;
        let conn = self.connect().await?;
        let (items, next_cursor) = self.scan_table(request).await?;
        let mut versioned = Vec::with_capacity(items.len());
        for item in items {
            let item_map = item.to_attribute_map()?;
            let split = split_item_into_key_and_attributes_sync(item_map, &table_info)?;
            let revision = self
                .get_item_revision(&conn, &request.table_name, &split.key_attributes)
                .await?;
            versioned.push(ItemVersionedWireItem {
                item,
                item_stream_version: storage_types::ItemStreamVersion::try_from(revision)?,
            });
        }
        Ok((versioned, next_cursor))
    }

    async fn get_item_with_durable_proof(
        &self,
        request: DurablePointReadRequest,
    ) -> StorageResult<DurablePointReadProof> {
        let table_info = self.get_table_info(&request.table_name).await?;
        let conn = self.connect().await?;
        let item = self
            .get_item_map_by_key(&conn, &table_info, &request.key)
            .await?;
        let revision = self
            .get_item_revision(&conn, &request.table_name, &request.key)
            .await?;

        Ok(match item {
            Some(item) => DurablePointReadProof::Present {
                item: Box::new(WireItem::from_attribute_map(&item)?),
                revision: DurableItemRevision::new(revision.to_be_bytes().to_vec()),
            },
            None => DurablePointReadProof::Absent {
                proof: DurableAbsenceProof::new(revision.to_be_bytes().to_vec()),
            },
        })
    }

    async fn delete_item_request(
        &self,
        request: DeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        self.delete_item_request_operation(request).await
    }

    async fn guarded_put_item(
        &self,
        request: GuardedPutItemRequest,
    ) -> StorageResult<PutItemResponse> {
        self.guarded_put_item_operation(request).await
    }

    async fn guarded_delete_item(
        &self,
        request: GuardedDeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        self.guarded_delete_item_operation(request).await
    }

    async fn apply_replication_mutation(&self, mutation: ReplicationMutation) -> StorageResult<()> {
        self.apply_replication_mutation_operation(mutation).await
    }

    async fn scan_table(
        &self,
        request: &storage_types::ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.scan_table_operation(request).await
    }

    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let conn = self.connect().await?;
        self.query_table_with_connection(&conn, request).await
    }

    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        self.batch_write_item_operation(request, should_write_to_stream)
            .await
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        let conn = self.connect().await?;
        self.batch_get_item_with_connection(&conn, request).await
    }

    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
        self.update_item_operation(request).await
    }

    async fn guarded_update_item(
        &self,
        request: GuardedUpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        self.guarded_update_item_operation(request).await
    }

    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.transact_write_items_operation(request).await
    }

    async fn update_table(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse> {
        self.update_table_operation(request).await
    }

    async fn update_time_to_live(
        &self,
        _request: storage_types::UpdateTimeToLiveRequest,
    ) -> StorageResult<storage_types::UpdateTimeToLiveResponse> {
        Err(StorageError::internal(
            "time to live configuration is not implemented for turso backend",
        ))
    }

    async fn describe_time_to_live(
        &self,
        table_name: &TableName,
    ) -> StorageResult<storage_types::DescribeTimeToLiveResponse> {
        let _ = self.get_table_info(table_name).await?;
        Ok(storage_types::DescribeTimeToLiveResponse {
            time_to_live_description: None,
        })
    }

    async fn run_job(&self, name: bg_jobs::BackgroundJobName) -> StorageResult<()> {
        if name == GSI_UPDATE_JOB {
            if self.immediate_gsi_consistency {
                return Ok(());
            }
            loop {
                let progressed = self.process_gsi_updates().await?;
                if !progressed {
                    break;
                }
            }
        } else if name == TTL_SWEEP_JOB {
            let cutoff_created_at_ms = TimestampMillis::now()
                .timestamp_millis()
                .saturating_sub(CHANGE_INDEX_MARKER_RETENTION_MS);
            self.trim_change_index_markers_older_than(cutoff_created_at_ms)
                .await?;
            let _ = ttl::run_custom_stream_trim_once(self).await?;
        }
        Ok(())
    }
}
