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
    StorageProvider, StreamDurationTrimBackend, StreamDurationTrimConfig,
    StreamDurationTrimPageRequest, StreamDurationTrimPageResult, StreamDurationTrimWorker,
    StreamTrimDueMarker, StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimState,
    StreamTrimStateWrite, apply_bound_update_operations, before_update_item,
    before_update_item_optional, plan_table_stream_duration, return_values_need_updated_fields,
    split_item_into_key_and_attributes_sync, update_item_response,
};
use storage_types::{
    AllOld, AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse, BatchWriteItemRequest,
    BatchWriteItemResponse, CreateTableRequest, DurableAbsenceProof, DurableItemRevision,
    DurablePointReadProof, DurablePointReadRequest, GuardedDeleteItemRequest,
    GuardedPutItemRequest, GuardedUpdateItemRequest, ItemVersionedWireItem, KeyAttributes,
    PreparedBatchOperation, PutItemResponse, QueryTableRequest, ReplicationMutation,
    ScanTableRequest, StorageError, StorageResult, StoredTableInfo, TableName, TableStatus,
    TimestampMillis, TransactWriteItem, TransactWriteItemsRequest, TransactWriteItemsResponse,
    UpdateItemRequest, UpdateItemResponse, WireItem,
};
use turso::Value as TursoValue;

use crate::{
    backends::{
        prepare_batch_operation,
        turso::{
            provider::{
                TursoStorageProvider, gsi_table_name, option_string_to_value, row_to_table_info,
                value_to_i64, value_to_string,
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

async fn run_custom_stream_trim_once(provider: &TursoStorageProvider) -> StorageResult<bool> {
    let stats = StreamDurationTrimWorker::new(
        provider.clone(),
        StreamDurationTrimConfig {
            marker_page_size: 250,
            stream_page_size: 1_000,
        },
    )
    .run_due_page(TimestampMillis::now(), TimestampMillis::now())
    .await?;
    Ok(stats.did_work())
}

impl TursoStorageProvider {
    pub(crate) async fn trim_change_index_markers_older_than(
        &self,
        cutoff_created_at_ms: i64,
    ) -> StorageResult<usize> {
        let conn = self.connect().await?;
        let deleted_markers = self
            .execute(
                &conn,
                sql_statements::trim_change_index_markers_older_than(),
                vec![TursoValue::Integer(cutoff_created_at_ms)],
            )
            .await?;
        usize::try_from(deleted_markers)
            .map_err(|_| StorageError::internal("turso deleted marker count exceeds usize"))
    }
}

#[async_trait]
impl StorageProvider for TursoStorageProvider {
    fn supports_guarded_writes(&self) -> bool {
        true
    }

    fn supports_custom_stream_duration(&self) -> bool {
        true
    }

    fn supports_change_index(&self) -> bool {
        true
    }

    async fn write_stream_trim_state(
        &self,
        state: storage_provider::StreamTrimState,
    ) -> StorageResult<()> {
        let conn = self.connect().await?;
        self.write_stream_trim_state(
            &conn,
            storage_provider::StreamTrimStateWrite {
                state,
                next_marker: None,
            },
        )
        .await
    }

    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<storage_provider::StreamTrimDueMarker>> {
        let conn = self.connect().await?;
        self.list_due_stream_trim_markers(&conn, due_before, limit)
            .await
    }

    async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<ChangeIndexMarker>> {
        let conn = self.connect().await?;
        let limit = i64::try_from(request.limit)
            .map_err(|_| StorageError::validation("change index list limit exceeds i64"))?;
        let rows = self
            .query_rows(
                &conn,
                sql_statements::list_change_index_markers(),
                vec![
                    TursoValue::Integer(i64::from(request.slot)),
                    TursoValue::Text(request.after_versionstamp.unwrap_or_default()),
                    TursoValue::Integer(limit),
                ],
            )
            .await?;
        rows.into_iter()
            .map(|row| {
                let slot = row
                    .get("slot")
                    .ok_or_else(|| StorageError::internal("change index row missing slot"))
                    .and_then(value_to_i64)
                    .and_then(|slot| {
                        u16::try_from(slot).map_err(|_| {
                            StorageError::internal("change index slot is outside u16 range")
                        })
                    })?;
                let versionstamp = row
                    .get("versionstamp")
                    .ok_or_else(|| StorageError::internal("change index row missing versionstamp"))
                    .and_then(value_to_string)?;
                let table_id = row
                    .get("table_id")
                    .ok_or_else(|| StorageError::internal("change index row missing table_id"))
                    .and_then(value_to_string)?;
                Ok(ChangeIndexMarker {
                    slot,
                    versionstamp,
                    table_id: TableName::new(&table_id),
                })
            })
            .collect()
    }

    async fn initialize_storage(&self) -> StorageResult<()> {
        let _ddl_guard = self.ddl_lock.lock().await;
        let this = self.clone();
        self.with_exclusive_transaction(true, |conn| {
            let this = this.clone();
            Box::pin(async move {
                let _ = this
                    .execute(conn, sql_statements::create_tables_table(), Vec::new())
                    .await?;
                let columns = this
                    .query_rows(conn, "PRAGMA table_info(tables)", Vec::new())
                    .await?;
                let has_column = |column_name: &str| {
                    columns.iter().any(|row| {
                        row.get("name").is_some_and(
                            |value| matches!(value, TursoValue::Text(name) if name == column_name),
                        )
                    })
                };
                if !has_column("deletion_protection_enabled") {
                    let _ = this
                        .execute(
                            conn,
                            sql_statements::add_deletion_protection_column(),
                            Vec::new(),
                        )
                        .await?;
                }
                if !has_column("table_stream_duration_hours") {
                    let _ = this
                        .execute(
                            conn,
                            sql_statements::add_table_stream_duration_column(),
                            Vec::new(),
                        )
                        .await?;
                }
                if !has_column("default_item_stream_duration_hours") {
                    let _ = this
                        .execute(
                            conn,
                            sql_statements::add_default_item_stream_duration_column(),
                            Vec::new(),
                        )
                        .await?;
                }
                let _ = this
                    .execute(
                        conn,
                        sql_statements::create_gsi_backfill_table(),
                        Vec::new(),
                    )
                    .await?;
                let _ = this
                    .execute(conn, sql_statements::create_ttl_config_table(), Vec::new())
                    .await?;
                let _ = this
                    .execute(
                        conn,
                        sql_statements::create_item_revisions_table(),
                        Vec::new(),
                    )
                    .await?;
                this.initialize_stream_duration_tables(conn).await?;
                Ok(())
            })
        })
        .await
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
        crate::backends::turso::logical_backfill::apply_resolved_sync_mutations(
            self, metadata, batch,
        )
        .await
    }

    async fn last_resolved_sync_log_id(&self) -> StorageResult<Option<storage_sync::SyncLogId>> {
        crate::backends::turso::logical_backfill::last_resolved_sync_log_id(self).await
    }

    async fn persist_resolved_sync_log_entry(
        &self,
        metadata: &storage_sync::SyncCommitMetadata,
        batch: &storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<()> {
        crate::backends::turso::logical_backfill::persist_resolved_sync_log_entry(
            self, metadata, batch,
        )
        .await
    }

    async fn get_resolved_sync_log_entry(
        &self,
        log_id: storage_sync::SyncLogId,
    ) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
        crate::backends::turso::logical_backfill::get_resolved_sync_log_entry(self, log_id).await
    }

    async fn resolved_sync_log_entries_after(
        &self,
        log_id: Option<storage_sync::SyncLogId>,
        limit: usize,
    ) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
        crate::backends::turso::logical_backfill::resolved_sync_log_entries_after(
            self, log_id, limit,
        )
        .await
    }

    async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        let conn = self.connect().await?;
        let rows = self
            .query_rows(
                &conn,
                sql_statements::table_exists(),
                vec![TursoValue::Text(table_name.to_string())],
            )
            .await?;
        let count = rows
            .first()
            .and_then(|row| row.get("count"))
            .map(value_to_i64)
            .transpose()?
            .unwrap_or_default();
        Ok(count > 0)
    }

    async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        validate_create_table_request(request)?;
        let table_name = request.table_name.clone();
        let _ddl_guard = self.ddl_lock.lock().await;

        let metadata = prepare_table_metadata(request)?;
        let stored_gsis = request
            .global_secondary_indexes
            .clone()
            .map(|indexes| indexes.into_iter().map(Into::into).collect::<Vec<_>>());

        let table_name_for_tx = table_name.clone();
        let this = self.clone();
        self.with_exclusive_transaction(true, |conn| {
            let this = this.clone();
            let metadata = metadata.clone();
            let request = request.clone();
            let stored_gsis = stored_gsis.clone();
            let table_name_for_tx = table_name_for_tx.clone();
            Box::pin(async move {
                if this.table_exists_conn(conn, &table_name_for_tx).await? {
                    return Err(StorageError::table_already_exists(&table_name_for_tx));
                }

                let table_id = uuid::Uuid::now_v7().to_string();
                let table_duration_plan = plan_table_stream_duration(
                    table_name_for_tx.clone(),
                    format!("turso-table:{table_id}"),
                    1,
                    metadata.table_stream_duration,
                    metadata.default_item_stream_duration,
                    metadata.created_at,
                );
                let insert_sql = sql_statements::insert_table();
                let insert_params = vec![
                    TursoValue::Text(table_id),
                    TursoValue::Text(table_name_for_tx.to_string()),
                    TursoValue::Text("CREATING".to_string()),
                    TursoValue::Integer(*metadata.created_at),
                    TursoValue::Text(metadata.attribute_definitions_json),
                    TursoValue::Text(metadata.key_schema_json),
                    option_string_to_value(metadata.global_secondary_indexes_json),
                    TursoValue::Integer(0),
                    TursoValue::Integer(0),
                    option_string_to_value(metadata.stream_specification_json),
                    TursoValue::Integer(if metadata.deletion_protection_enabled {
                        1
                    } else {
                        0
                    }),
                    TursoValue::Integer(metadata.table_stream_duration.as_hours_wire_value()),
                    TursoValue::Integer(
                        metadata.default_item_stream_duration.as_hours_wire_value(),
                    ),
                ];
                let _ = this.execute(conn, insert_sql, insert_params).await?;
                this.write_stream_trim_state(
                    conn,
                    storage_provider::StreamTrimStateWrite {
                        state: table_duration_plan.trim_state,
                        next_marker: table_duration_plan.due_marker,
                    },
                )
                .await?;

                let rowid_mode = SqliteTableRowidMode::WithRowid;
                // TODO: Enable after Turso releases support for WITHOUT ROWID.
                // let rowid_mode = SqliteTableRowidMode::WithoutRowid;

                let create_sql = build_table_creation_sql(
                    &request.table_name,
                    &request.attribute_definitions,
                    &request.key_schema,
                    stored_gsis.as_deref(),
                    rowid_mode,
                );
                let _ = this.execute(conn, &create_sql, Vec::new()).await?;

                if let Some(gsis) = stored_gsis.as_ref() {
                    for sql in build_gsi_creation_sqls(
                        &request.table_name,
                        &request.attribute_definitions,
                        &request.key_schema,
                        gsis,
                        rowid_mode,
                    ) {
                        let _ = this.execute(conn, &sql, Vec::new()).await?;
                    }
                }

                let _ = this
                    .execute(
                        conn,
                        sql_statements::update_table_status(),
                        vec![
                            TursoValue::Text("ACTIVE".to_string()),
                            TursoValue::Text(table_name_for_tx.to_string()),
                        ],
                    )
                    .await?;

                Ok(())
            })
        })
        .await?;

        self.invalidate_table_cache(&table_name).await;
        Ok(())
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
        let _ddl_guard = self.ddl_lock.lock().await;
        let table_name_clone = table_name.clone();
        let this = self.clone();

        self.with_exclusive_transaction(true, |conn| {
            let this = this.clone();
            let table_name_clone = table_name_clone.clone();
            Box::pin(async move {
                let table_info = this
                    .load_table_info_uncached(conn, &table_name_clone)
                    .await?;
                if table_info.deletion_protection_enabled {
                    return Err(StorageError::deletion_protection_enabled(&table_name_clone));
                }

                let _ = this
                    .execute(
                        conn,
                        sql_statements::delete_table_metadata(),
                        vec![TursoValue::Text(table_name_clone.to_string())],
                    )
                    .await?;
                let _ = this
                    .execute(
                        conn,
                        &sql_statements::drop_table(&table_name_clone.sanitized_name()),
                        Vec::new(),
                    )
                    .await?;

                if let Some(gsis) = table_info.global_secondary_indexes.as_ref() {
                    for gsi in gsis {
                        let gsi_table = gsi_table_name(&table_info.table_name, &gsi.index_name);
                        let _ = this
                            .execute(
                                conn,
                                &sql_statements::drop_named_table(&gsi_table),
                                Vec::new(),
                            )
                            .await?;
                    }
                }
                Ok(())
            })
        })
        .await?;

        self.invalidate_table_cache(table_name).await;
        Ok(())
    }

    async fn create_table_storage(
        &self,
        _table_name: &TableName,
        _request: &CreateTableRequest,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn put_item(
        &self,
        table_name: TableName,
        item: HashMap<String, AttributeValue>,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
    ) -> StorageResult<PutItemResponse> {
        self.put_item_with_stream_ttl(
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

    async fn put_item_with_stream_ttl(
        &self,
        table_name: TableName,
        item: HashMap<String, AttributeValue>,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
        aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
    ) -> StorageResult<PutItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let table_info = self.get_table_info(&table_name).await?;
        let condition = self
            .parse_condition(
                condition_expression,
                &expression_attribute_names,
                &expression_attribute_values,
            )
            .await?;
        let this = self.clone();

        let old_item = self
            .with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let item = item.clone();
                let condition = condition.clone();
                Box::pin(async move {
                    this.put_item_txn(
                        conn,
                        &table_info,
                        &item,
                        condition.as_ref(),
                        aux_item_stream_ttl_hours,
                    )
                    .await
                })
            })
            .await?;

        let attributes = if matches!(return_values, Some(AllOld::AllOld)) {
            old_item
        } else {
            None
        };

        Ok(PutItemResponse {
            attributes: attributes.map(Into::into),
        })
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

    async fn delete_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        self.delete_item_with_stream_ttl(
            table_name,
            key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            None,
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
        aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        apply_gsi_write_pressure(self).await?;
        let table_info = self.get_table_info(&table_name).await?;
        let condition = self
            .parse_condition(
                condition_expression,
                &expression_attribute_names,
                &expression_attribute_values,
            )
            .await?;
        let this = self.clone();

        self.with_transaction(true, |conn| {
            let this = this.clone();
            let table_info = table_info.clone();
            let key = key.clone();
            let condition = condition.clone();
            Box::pin(async move {
                this.delete_item_txn_with_replication(
                    conn,
                    &table_info,
                    &key,
                    condition.as_ref(),
                    None,
                    aux_item_stream_ttl_hours,
                )
                .await
            })
        })
        .await
    }

    async fn guarded_put_item(
        &self,
        request: GuardedPutItemRequest,
    ) -> StorageResult<PutItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let GuardedPutItemRequest {
            table_name,
            item,
            guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
        } = request;
        let table_info = self.get_table_info(&table_name).await?;
        let key_attributes =
            StorageProvider::get_key_attributes(self, &item, &table_info.key_schema)?;
        let condition = self
            .parse_condition(
                condition_expression,
                &expression_attribute_names,
                &expression_attribute_values,
            )
            .await?;
        let this = self.clone();

        let old_item = self
            .with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let item = item.clone();
                let guard = guard.clone();
                let key_attributes = key_attributes.clone();
                let condition = condition.clone();
                Box::pin(async move {
                    this.validate_durable_guard(
                        conn,
                        &table_info.table_name,
                        &key_attributes,
                        &guard,
                    )
                    .await?;
                    this.put_item_txn(conn, &table_info, &item, condition.as_ref(), None)
                        .await
                })
            })
            .await?;

        let attributes = if matches!(return_values, Some(AllOld::AllOld)) {
            old_item
        } else {
            None
        };

        Ok(PutItemResponse {
            attributes: attributes.map(Into::into),
        })
    }

    async fn guarded_delete_item(
        &self,
        request: GuardedDeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        apply_gsi_write_pressure(self).await?;
        let GuardedDeleteItemRequest {
            table_name,
            key,
            guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        } = request;
        let table_info = self.get_table_info(&table_name).await?;
        let condition = self
            .parse_condition(
                condition_expression,
                &expression_attribute_names,
                &expression_attribute_values,
            )
            .await?;
        let this = self.clone();

        self.with_transaction(true, |conn| {
            let this = this.clone();
            let table_info = table_info.clone();
            let key = key.clone();
            let guard = guard.clone();
            let condition = condition.clone();
            Box::pin(async move {
                this.validate_durable_guard(conn, &table_info.table_name, &key, &guard)
                    .await?;
                this.delete_item_txn_with_replication(
                    conn,
                    &table_info,
                    &key,
                    condition.as_ref(),
                    None,
                    None,
                )
                .await
            })
        })
        .await
    }

    async fn apply_replication_mutation(&self, mutation: ReplicationMutation) -> StorageResult<()> {
        let table_info = self.get_table_info(&mutation.table_name).await?;
        let metadata = mutation.metadata.clone();
        let this = self.clone();
        if let Some(new_image) = mutation.new_image {
            self.with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let new_image = new_image.clone();
                let metadata = metadata.clone();
                Box::pin(async move {
                    let split =
                        split_item_into_key_and_attributes_sync(new_image.clone(), &table_info)?;
                    let old_item = this
                        .get_item_map_by_key(conn, &table_info, &split.key_attributes)
                        .await?;
                    this.overwrite_item_txn_with_replication(
                        conn,
                        &table_info,
                        &new_image,
                        old_item.as_ref(),
                        Some(&metadata),
                        None,
                    )
                    .await
                })
            })
            .await?;
            return Ok(());
        }

        self.with_transaction(true, |conn| {
            let this = this.clone();
            let table_info = table_info.clone();
            let key = mutation.key.clone();
            let metadata = metadata.clone();
            Box::pin(async move {
                this.delete_item_txn_with_replication(
                    conn,
                    &table_info,
                    &key,
                    None,
                    Some(&metadata),
                    None,
                )
                .await
                .map(|_| ())
            })
        })
        .await
    }

    async fn scan_table(
        &self,
        request: &storage_types::ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let table_info = self.get_table_info(&request.table_name).await?;
        let effective_limit = calc_limit(request.limit, DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT)?;
        let exclusive_start_key = decode_exclusive_start(
            &request.exclusive_start_key,
            &table_info,
            &request.index_name,
        )?;

        let (table_name_safe, key_schema, table_key_schema_for_index, origin_gsi) =
            if let Some(index_name) = &request.index_name {
                let gsi = table_info
                    .global_secondary_indexes
                    .as_ref()
                    .and_then(|indexes| {
                        indexes.iter().find(|index| &index.index_name == index_name)
                    })
                    .ok_or_else(|| missing_index_error(&table_info, index_name))?;
                (
                    gsi_table_name(&table_info.table_name, index_name),
                    gsi.key_schema.clone(),
                    Some(table_info.key_schema.as_slice()),
                    true,
                )
            } else {
                (
                    format!("table_{}", table_info.table_name.sanitized_name()),
                    table_info.key_schema.clone(),
                    None,
                    false,
                )
            };

        let (sql, values) = build_sql_query(
            &table_name_safe,
            &key_schema,
            None,
            exclusive_start_key,
            effective_limit,
            Some(true),
            table_key_schema_for_index,
        )?;

        let conn = self.connect().await?;
        let rows = self
            .query_row_set(
                &conn,
                &sql,
                values.into_iter().map(TursoValue::Text).collect(),
            )
            .await?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let wire = if origin_gsi {
                self.build_wire_item_from_gsi_row_view(row, &table_info, &key_schema)
                    .await?
            } else {
                self.build_wire_item_from_main_row_view(row, &table_info)
                    .await?
            };
            items.push(wire);
        }

        let has_more = items.len() > effective_limit as usize;
        if has_more {
            items.pop();
        }

        let last_evaluated_key = if has_more {
            items
                .last()
                .map(|item| item.last_evaluated_key(&table_info, &request.index_name))
                .transpose()?
                .flatten()
        } else {
            None
        };

        Ok((items, last_evaluated_key))
    }

    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let table_info = self.get_table_info(&request.table_name).await?;
        let effective_limit = calc_limit(request.limit, DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT)?;
        let exclusive_start_key = decode_exclusive_start(
            &request.exclusive_start_key,
            &table_info,
            &request.index_name,
        )?;

        let (table_name_safe, key_schema, table_key_schema_for_index, origin_gsi) =
            if let Some(index_name) = &request.index_name {
                let gsi = table_info
                    .global_secondary_indexes
                    .as_ref()
                    .and_then(|indexes| {
                        indexes.iter().find(|index| &index.index_name == index_name)
                    })
                    .ok_or_else(|| missing_index_error(&table_info, index_name))?;
                (
                    gsi_table_name(&table_info.table_name, index_name),
                    gsi.key_schema.clone(),
                    Some(table_info.key_schema.as_slice()),
                    true,
                )
            } else {
                (
                    format!("table_{}", table_info.table_name.sanitized_name()),
                    table_info.key_schema.clone(),
                    None,
                    false,
                )
            };

        let conditions = parse_key_condition_expression(
            &request.key_condition_expression,
            &key_schema,
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
        )?;

        let (sql, values) = build_sql_query(
            &table_name_safe,
            &key_schema,
            Some(conditions),
            exclusive_start_key,
            effective_limit,
            request.scan_index_forward,
            table_key_schema_for_index,
        )?;

        let conn = self.connect().await?;
        let rows = self
            .query_row_set(
                &conn,
                &sql,
                values.into_iter().map(TursoValue::Text).collect(),
            )
            .await?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let wire = if origin_gsi {
                self.build_wire_item_from_gsi_row_view(row, &table_info, &key_schema)
                    .await?
            } else {
                self.build_wire_item_from_main_row_view(row, &table_info)
                    .await?
            };
            items.push(wire);
        }

        let has_more = items.len() > effective_limit as usize;
        if has_more {
            items.pop();
        }

        let last_evaluated_key = if has_more {
            items
                .last()
                .map(|item| item.last_evaluated_key(&table_info, &request.index_name))
                .transpose()?
                .flatten()
        } else {
            None
        };

        Ok((items, last_evaluated_key))
    }

    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
        _should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let mut prepared_ops: Vec<PreparedBatchOperation> = Vec::new();
        for (table_name, writes) in request.request_items {
            let table_info = self.get_table_info(&table_name).await?;
            for write in writes {
                prepared_ops.push(prepare_batch_operation(&table_info, write)?);
            }
        }

        let this = self.clone();
        self.with_transaction(true, move |conn| {
            let this = this.clone();
            let prepared_ops = prepared_ops.clone();
            Box::pin(async move {
                this.execute_prepared_batch_operations(conn, &prepared_ops)
                    .await
            })
        })
        .await?;

        Ok(BatchWriteItemResponse {
            unprocessed_items: None,
            item_collection_metrics: None,
            consumed_capacity: None,
        })
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        let mut responses = HashMap::new();
        for (table_name, keys_and_attributes) in request.request_items {
            let mut table_items = Vec::new();
            for key in keys_and_attributes.keys {
                if let Some(item) = self.get_item(table_name.clone(), key, false).await? {
                    table_items.push(item);
                }
            }
            responses.insert(table_name, table_items);
        }

        Ok(BatchGetWireItemResponse {
            responses: Some(responses),
            unprocessed_keys: None,
            consumed_capacity: None,
        })
    }

    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let table_info = self.get_table_info(&request.table_name).await?;
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
        let this = self.clone();
        let collect_response_fields = return_values_need_updated_fields(return_values.as_ref());

        let (old_item, new_item, response_fields) = self
            .with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let key = key.clone();
                let update_expression = update_expression.clone();
                let condition_expression = condition_expression.clone();
                let expression_attribute_names = expression_attribute_names.clone();
                let expression_attribute_values = expression_attribute_values.clone();
                Box::pin(async move {
                    let (operations, condition) = before_update_item_optional(
                        update_expression.as_deref(),
                        condition_expression.as_deref(),
                        expression_attribute_names.as_ref(),
                        expression_attribute_values.as_ref(),
                    )?;
                    let response_fields = if collect_response_fields {
                        operations
                            .iter()
                            .map(|operation| operation.field_name_arc())
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    let existing_item = this.get_item_map_by_key(conn, &table_info, &key).await?;
                    let (item_to_update, updated_item) = plan_update_from_existing_item(
                        existing_item,
                        &key,
                        &operations,
                        condition.as_ref(),
                    )?;

                    this.overwrite_item_txn(
                        conn,
                        &table_info,
                        &updated_item,
                        Some(&item_to_update),
                        aux_item_stream_ttl_hours,
                    )
                    .await?;

                    Ok((item_to_update, updated_item, response_fields))
                })
            })
            .await?;

        let response = update_item_response(
            &response_fields,
            Some(old_item),
            Some(new_item),
            return_values.as_ref(),
        )?;

        self.invalidate_table_cache(&table_name).await;
        Ok(response)
    }

    async fn guarded_update_item(
        &self,
        request: GuardedUpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let GuardedUpdateItemRequest { request, guard } = request;
        let table_info = self.get_table_info(&request.table_name).await?;
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
        let this = self.clone();
        let collect_response_fields = return_values_need_updated_fields(return_values.as_ref());

        let (old_item, new_item, response_fields) = self
            .with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let key = key.clone();
                let guard = guard.clone();
                let update_expression = update_expression.clone();
                let condition_expression = condition_expression.clone();
                let expression_attribute_names = expression_attribute_names.clone();
                let expression_attribute_values = expression_attribute_values.clone();
                Box::pin(async move {
                    let (operations, condition) = before_update_item_optional(
                        update_expression.as_deref(),
                        condition_expression.as_deref(),
                        expression_attribute_names.as_ref(),
                        expression_attribute_values.as_ref(),
                    )?;
                    let response_fields = if collect_response_fields {
                        operations
                            .iter()
                            .map(|operation| operation.field_name_arc())
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    this.validate_durable_guard(conn, &table_info.table_name, &key, &guard)
                        .await?;
                    let existing_item = this.get_item_map_by_key(conn, &table_info, &key).await?;
                    let (item_to_update, updated_item) = plan_update_from_existing_item(
                        existing_item,
                        &key,
                        &operations,
                        condition.as_ref(),
                    )?;

                    this.overwrite_item_txn(
                        conn,
                        &table_info,
                        &updated_item,
                        Some(&item_to_update),
                        aux_item_stream_ttl_hours,
                    )
                    .await?;

                    Ok((item_to_update, updated_item, response_fields))
                })
            })
            .await?;

        let response = update_item_response(
            &response_fields,
            Some(old_item),
            Some(new_item),
            return_values.as_ref(),
        )?;

        self.invalidate_table_cache(&table_name).await;
        Ok(response)
    }

    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        apply_gsi_write_pressure(self).await?;
        let this = self.clone();
        self.with_transaction(true, |conn| {
            let this = this.clone();
            let request = request.clone();
            Box::pin(async move {
                let mut preflights = Vec::with_capacity(request.transact_items.len());
                for item in &request.transact_items {
                    preflights.push(this.preflight_transact_item_key(item).await?);
                }
                if let Some(error) = transaction_canceled_for_preflights(&preflights) {
                    return Err(error);
                }
                validate_no_duplicate_transact_item_keys(&preflights)?;

                let item_count = request.transact_items.len();
                let mut cancellation_reasons = vec![None; item_count];
                for (index, item) in request.transact_items.into_iter().enumerate() {
                    let result = async {
                        if let Some(put) = item.put {
                            let table_info = this.get_table_info(&put.table_name).await?;
                            validate_transact_put_item_key(&table_info, &put.item)?;
                            let old_item = if put.condition_expression.is_some() {
                                let split_item = split_item_into_key_and_attributes_sync(
                                    put.item.clone(),
                                    &table_info,
                                )?;
                                this.get_item_map_by_key(
                                    conn,
                                    &table_info,
                                    &split_item.key_attributes,
                                )
                                .await?
                            } else {
                                None
                            };
                            let condition = this
                                .parse_condition(
                                    put.condition_expression.clone(),
                                    &put.expression_attribute_names,
                                    &put.expression_attribute_values,
                                )
                                .await?;
                            if let Some(condition) = condition.as_ref()
                                && !evaluate_condition(
                                    condition_item_ref(old_item.as_ref()),
                                    condition,
                                )
                            {
                                return Err(transaction_canceled_for_reason(
                                    index,
                                    conditional_check_failed_reason(
                                        all_old(
                                            put.return_values_on_condition_check_failure.as_ref(),
                                        )
                                        .then_some(old_item.as_ref())
                                        .flatten(),
                                    )?,
                                ));
                            }
                            let _ = this
                                .put_item_txn(
                                    conn,
                                    &table_info,
                                    &put.item,
                                    None,
                                    put.aux_item_stream_ttl_hours,
                                )
                                .await?;
                        }

                        if let Some(delete) = item.delete {
                            let table_info = this.get_table_info(&delete.table_name).await?;
                            validate_transact_key(&table_info, &delete.key)?;
                            let old_item = if delete.condition_expression.is_some() {
                                this.get_item_map_by_key(conn, &table_info, &delete.key)
                                    .await?
                            } else {
                                None
                            };
                            let condition = this
                                .parse_condition(
                                    delete.condition_expression.clone(),
                                    &delete.expression_attribute_names,
                                    &delete.expression_attribute_values,
                                )
                                .await?;
                            if let Some(condition) = condition.as_ref()
                                && !evaluate_condition(
                                    condition_item_ref(old_item.as_ref()),
                                    condition,
                                )
                            {
                                return Err(transaction_canceled_for_reason(
                                    index,
                                    conditional_check_failed_reason(
                                        all_old(
                                            delete
                                                .return_values_on_condition_check_failure
                                                .as_ref(),
                                        )
                                        .then_some(old_item.as_ref())
                                        .flatten(),
                                    )?,
                                ));
                            }
                            let _ = this
                                .delete_item_txn_with_replication(
                                    conn,
                                    &table_info,
                                    &delete.key,
                                    None,
                                    None,
                                    delete.aux_item_stream_ttl_hours,
                                )
                                .await?;
                        }

                        if let Some(update) = item.update {
                            let table_info = this.get_table_info(&update.table_name).await?;
                            let (operations, condition) = before_update_item(
                                update.update_expression.as_str(),
                                update.condition_expression.as_deref(),
                                update.expression_attribute_names.as_ref(),
                                update.expression_attribute_values.as_ref(),
                            )?;
                            let existing_item = this
                                .get_item_map_by_key(conn, &table_info, &update.key)
                                .await?;

                            if let Some(condition) = condition.as_ref()
                                && !evaluate_condition(
                                    condition_item_ref(existing_item.as_ref()),
                                    condition,
                                )
                            {
                                return Err(transaction_canceled_for_reason(
                                    index,
                                    conditional_check_failed_reason(
                                        all_old(
                                            update
                                                .return_values_on_condition_check_failure
                                                .as_ref(),
                                        )
                                        .then_some(existing_item.as_ref())
                                        .flatten(),
                                    )?,
                                ));
                            }

                            let item_to_update =
                                existing_item.unwrap_or_else(|| update.key.to_attribute_map());
                            let updated_item =
                                apply_bound_update_operations(item_to_update, &operations)?;
                            let _ = this
                                .put_item_txn(
                                    conn,
                                    &table_info,
                                    &updated_item,
                                    None,
                                    update.aux_item_stream_ttl_hours,
                                )
                                .await?;
                        }

                        if let Some(condition_check) = item.condition_check {
                            let table_info =
                                this.get_table_info(&condition_check.table_name).await?;
                            validate_transact_key(&table_info, &condition_check.key)?;
                            let existing = this
                                .get_item_map_by_key(conn, &table_info, &condition_check.key)
                                .await?;
                            let parsed = parse_condition_expression(
                                &condition_check.condition_expression,
                                condition_check.expression_attribute_names.as_ref(),
                                condition_check.expression_attribute_values.as_ref(),
                            )
                            .map_err(StorageError::validation)?;
                            if !evaluate_condition(condition_item_ref(existing.as_ref()), &parsed) {
                                return Err(transaction_canceled_for_reason(
                                    index,
                                    conditional_check_failed_reason(
                                        all_old(
                                            condition_check
                                                .return_values_on_condition_check_failure
                                                .as_ref(),
                                        )
                                        .then_some(existing.as_ref())
                                        .flatten(),
                                    )?,
                                ));
                            }
                        }
                        Ok(())
                    }
                    .await;
                    if let Err(error) = result {
                        let error =
                            transaction_canceled_for_item_error_with_len(index, item_count, error);
                        let Some(reason) = transaction_cancellation_reason_at(&error, index) else {
                            return Err(error);
                        };
                        cancellation_reasons[index] = Some(reason);
                    }
                }
                if let Some(error) = transaction_canceled_for_indexed_reasons(cancellation_reasons)
                {
                    return Err(error);
                }

                Ok(TransactWriteItemsResponse {
                    consumed_capacity: None,
                    item_collection_metrics: None,
                })
            })
        })
        .await
    }

    async fn update_table(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse> {
        let mut table_info = self.get_table_info(&request.table_name).await?;
        if let Some(deletion_protection_enabled) = request.deletion_protection_enabled {
            let conn = self.connect().await?;
            let _ = self
                .execute(
                    &conn,
                    sql_statements::update_deletion_protection(),
                    vec![
                        TursoValue::Integer(if deletion_protection_enabled { 1 } else { 0 }),
                        TursoValue::Text(request.table_name.to_string()),
                    ],
                )
                .await?;
            self.invalidate_table_cache(&request.table_name).await;
            table_info.deletion_protection_enabled = deletion_protection_enabled;
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
            let table_name = request.table_name.clone();
            let this = self.clone();
            let table_stream_duration = table_info.table_stream_duration;
            let default_item_stream_duration = table_info.default_item_stream_duration;
            self.with_exclusive_transaction(true, |conn| {
                let this = this.clone();
                let table_name = table_name.clone();
                Box::pin(async move {
                    let table_scope_id = this.load_table_scope_id(conn, &table_name).await?;
                    let policy_version = this
                        .next_table_policy_version(conn, &table_scope_id)
                        .await?;
                    let table_duration_plan = plan_table_stream_duration(
                        table_name.clone(),
                        table_scope_id,
                        policy_version,
                        table_stream_duration,
                        default_item_stream_duration,
                        TimestampMillis::now(),
                    );
                    let _ = this
                        .execute(
                            conn,
                            sql_statements::update_stream_durations(),
                            vec![
                                TursoValue::Integer(table_stream_duration.as_hours_wire_value()),
                                TursoValue::Integer(
                                    default_item_stream_duration.as_hours_wire_value(),
                                ),
                                TursoValue::Text(table_name.to_string()),
                            ],
                        )
                        .await?;
                    this.write_stream_trim_state(
                        conn,
                        storage_provider::StreamTrimStateWrite {
                            state: table_duration_plan.trim_state,
                            next_marker: table_duration_plan.due_marker,
                        },
                    )
                    .await?;
                    Ok(())
                })
            })
            .await?;
            self.invalidate_table_cache(&request.table_name).await;
        }

        Ok(storage_types::UpdateTableResponse {
            table_description: storage_types::TableDescription {
                table_name: table_info.table_name.clone(),
                table_status: table_info.table_status,
                created_at: table_info.created_at.into(),
                attribute_definitions: table_info.attribute_definitions,
                key_schema: table_info.key_schema,
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
                global_secondary_indexes: table_info.global_secondary_indexes.map(|indexes| {
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
                }),
                local_secondary_indexes: None,
                provisioned_throughput: None,
                stream_specification: table_info.stream_specification,
                latest_stream_arn: None,
                latest_stream_label: None,
                deletion_protection_enabled: table_info.deletion_protection_enabled,
            },
        })
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
            let _ = run_custom_stream_trim_once(self).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl StreamDurationTrimBackend for TursoStorageProvider {
    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        let conn = self.connect().await?;
        self.list_due_stream_trim_markers(&conn, due_before, limit)
            .await
    }

    async fn load_stream_trim_state(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>> {
        let conn = self.connect().await?;
        self.load_stream_trim_state_by_scope(&conn, scope).await
    }

    async fn load_stream_trim_boundaries(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<StreamTrimScopeBoundaries> {
        let conn = self.connect().await?;
        self.load_stream_trim_boundaries(&conn, scope).await
    }

    async fn trim_table_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.trim_stream_page(request).await
    }

    async fn trim_item_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.trim_stream_page(request).await
    }

    async fn finish_stream_trim_marker(
        &self,
        marker: StreamTrimDueMarker,
        write: Option<StreamTrimStateWrite>,
    ) -> StorageResult<()> {
        self.finish_stream_trim_marker(marker, write).await
    }
}

impl TursoStorageProvider {
    async fn preflight_transact_item_key(
        &self,
        item: &TransactWriteItem,
    ) -> StorageResult<TransactionKeyPreflight> {
        let Some(table_name) = transact_item_table_name(item) else {
            return Ok(TransactionKeyPreflight::default());
        };
        let table_info = self.get_table_info(table_name).await?;
        preflight_transact_item_key_with_table_info(item, &table_info)
    }

    async fn execute_prepared_batch_operations<C>(
        &self,
        conn: &C,
        prepared_ops: &[PreparedBatchOperation],
    ) -> StorageResult<()>
    where
        C: crate::backends::turso::provider::core::TursoSqlConnection + ?Sized,
    {
        for prepared_op in prepared_ops {
            match prepared_op {
                PreparedBatchOperation::Put {
                    table_info,
                    full_item,
                    aux_item_stream_ttl_hours,
                    ..
                } => {
                    let _ = self
                        .put_item_txn(
                            conn,
                            table_info,
                            full_item,
                            None,
                            *aux_item_stream_ttl_hours,
                        )
                        .await?;
                }
                PreparedBatchOperation::Delete {
                    table_info,
                    key,
                    aux_item_stream_ttl_hours,
                    ..
                } => {
                    let _ = self
                        .delete_item_txn_with_replication(
                            conn,
                            table_info,
                            key,
                            None,
                            None,
                            *aux_item_stream_ttl_hours,
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }
}
