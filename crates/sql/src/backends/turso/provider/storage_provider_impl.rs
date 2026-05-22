use std::collections::HashMap;

use async_trait::async_trait;
use storage_backfill::{LogicalBackfillExport, LogicalBackfillImport};
use storage_common::{GSI_UPDATE_JOB, normalize_limit as calc_limit};
use storage_condition::{evaluate_condition, parse_condition_expression};
use storage_provider::{
    StorageProvider, apply_bound_update_operations, before_update_item,
    return_values_need_updated_fields, split_item_into_key_and_attributes_sync,
    update_item_response,
};
use storage_types::{
    AllOld, AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse, BatchWriteItemRequest,
    BatchWriteItemResponse, CreateTableRequest, DurableAbsenceProof, DurableItemRevision,
    DurablePointReadProof, DurablePointReadRequest, GuardedDeleteItemRequest,
    GuardedPutItemRequest, GuardedUpdateItemRequest, ItemVersionedWireItem, KeyAttributes,
    PreparedBatchOperation, PutItemResponse, QueryTableRequest, ReplicationMutation,
    ScanTableRequest, StorageEnum, StorageError, StorageResult, StoredTableInfo, TableName,
    TableStatus, TransactWriteItem, TransactWriteItemsRequest, TransactWriteItemsResponse,
    UpdateItemRequest, UpdateItemResponse, WireItem,
};
use turso::Value as TursoValue;

use crate::{
    backends::{
        prepare_batch_operation,
        turso::{
            provider::{
                TursoStorageProvider, gsi_table_name, option_string_to_value, row_to_table_info,
                value_to_i64,
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
            transaction_canceled_for_item_error_with_len, transaction_canceled_for_preflights,
            transaction_canceled_for_reason, validate_no_duplicate_transact_item_keys,
            validate_transact_key, validate_transact_put_item_key,
        },
        write::plan_update_from_existing_item,
    },
    sql_builder::build_sql_query,
    utils::{SqliteTableRowidMode, build_gsi_creation_sqls, build_table_creation_sql},
};

#[async_trait]
impl StorageProvider for TursoStorageProvider {
    fn supports_guarded_writes(&self) -> bool {
        true
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

                let insert_sql = sql_statements::insert_table();
                let insert_params = vec![
                    TursoValue::Text(uuid::Uuid::now_v7().to_string()),
                    TursoValue::Text(table_name_for_tx.to_string()),
                    TursoValue::Text("CREATING".to_string()),
                    TursoValue::Integer(*metadata.created_at),
                    TursoValue::Text(metadata.attribute_definitions_json),
                    TursoValue::Text(metadata.key_schema_json),
                    option_string_to_value(metadata.global_secondary_indexes_json),
                    TursoValue::Integer(0),
                    TursoValue::Integer(0),
                    option_string_to_value(metadata.stream_specification_json),
                ];
                let _ = this.execute(conn, insert_sql, insert_params).await?;

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
                    this.put_item_txn(conn, &table_info, &item, condition.as_ref())
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
                this.delete_item_txn(conn, &table_info, &key, condition.as_ref())
                    .await
            })
        })
        .await
    }

    async fn guarded_put_item(
        &self,
        request: GuardedPutItemRequest,
    ) -> StorageResult<PutItemResponse> {
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
                    this.put_item_txn(conn, &table_info, &item, condition.as_ref())
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
                this.delete_item_txn(conn, &table_info, &key, condition.as_ref())
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
        let table_info = self.get_table_info(&request.table_name).await?;
        let UpdateItemRequest {
            table_name,
            key,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
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
                    let (operations, condition) = before_update_item(
                        &update_expression,
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
                    let (operations, condition) = before_update_item(
                        &update_expression,
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
                            if let Some(condition) = condition.as_ref() {
                                let condition_item = old_item.clone().unwrap_or_default();
                                if !evaluate_condition(&condition_item, condition) {
                                    return Err(transaction_canceled_for_reason(
                                        index,
                                        conditional_check_failed_reason(
                                            all_old(
                                                put.return_values_on_condition_check_failure
                                                    .as_ref(),
                                            )
                                            .then_some(old_item.as_ref())
                                            .flatten(),
                                        )?,
                                    ));
                                }
                            }
                            let _ = this
                                .put_item_txn(conn, &table_info, &put.item, None)
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
                            if let Some(condition) = condition.as_ref() {
                                let condition_item = old_item.clone().unwrap_or_default();
                                if !evaluate_condition(&condition_item, condition) {
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
                            }
                            let _ = this
                                .delete_item_txn(conn, &table_info, &delete.key, None)
                                .await?;
                        }

                        if let Some(update) = item.update {
                            let table_info = this.get_table_info(&update.table_name).await?;
                            let (operations, condition) = before_update_item(
                                &update.update_expression,
                                update.condition_expression.as_deref(),
                                update.expression_attribute_names.as_ref(),
                                update.expression_attribute_values.as_ref(),
                            )?;
                            let existing_item = this
                                .get_item_map_by_key(conn, &table_info, &update.key)
                                .await?;
                            let item_to_update =
                                existing_item.unwrap_or_else(|| update.key.to_attribute_map());

                            if let Some(condition) = condition.as_ref()
                                && !evaluate_condition(&item_to_update, condition)
                            {
                                return Err(StorageEnum::ConditionalCheckFailed.into());
                            }

                            let updated_item =
                                apply_bound_update_operations(item_to_update, &operations)?;
                            let _ = this
                                .put_item_txn(conn, &table_info, &updated_item, None)
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
                            let condition_item = existing.clone().unwrap_or_default();
                            if !evaluate_condition(&condition_item, &parsed) {
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
                        return Err(transaction_canceled_for_item_error_with_len(
                            index, item_count, error,
                        ));
                    }
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
        let table_info = self.get_table_info(&request.table_name).await?;

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
        _table_name: &TableName,
    ) -> StorageResult<storage_types::DescribeTimeToLiveResponse> {
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
        }
        Ok(())
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
                    ..
                } => {
                    let _ = self.put_item_txn(conn, table_info, full_item, None).await?;
                }
                PreparedBatchOperation::Delete {
                    table_info, key, ..
                } => {
                    let _ = self.delete_item_txn(conn, table_info, key, None).await?;
                }
            }
        }

        Ok(())
    }
}
