use std::collections::HashMap;
#[cfg(test)]
use std::time::Instant;

use async_trait::async_trait;
use storage_backfill::{LogicalBackfillExport, LogicalBackfillImport};
#[cfg(test)]
use storage_common::provider_perf;
use storage_common::{
    GSI_UPDATE_JOB, TTL_SWEEP_JOB, normalize_limit as calc_limit,
    ttl::{TtlConfigRecord, ttl_gsi_name},
};
use storage_condition::{Condition, evaluate_condition, parse_condition_expression};
use storage_provider::{StorageProvider, split_item_into_key_and_attributes_sync};
use storage_types::{
    AllOld, BatchGetItemRequest, BatchGetWireItemResponse, BatchWriteItemEncodeRequest,
    BatchWriteItemRequest, BatchWriteItemResponse, CreateTableRequest, DurableAbsenceProof,
    DurableItemRevision, DurablePointReadProof, DurablePointReadRequest, GuardedDeleteItemRequest,
    GuardedPutItemRequest, GuardedUpdateItemRequest, ItemVersionedWireItem, KeyAttributes,
    KeysAndAttributes, PutItemResponse, QueryTableRequest, ReplicationMutation, ScanTableRequest,
    StorageEnum, StorageError, StorageResult, StoredTableInfo, StreamItemId, StreamName, TableName,
    TableStatus, TimeToLiveDescription, TimeToLiveStatus, UpdateItemRequest, UpdateItemResponse,
    UpdateTimeToLiveRequest, UpdateTimeToLiveResponse, WireItem, WriteRequest,
};
use stream_provider::{CursorName, CursorPosition, StreamDataType, StreamItem, StreamProvider};
use tokio_postgres::types::ToSql;

use crate::{
    backends::postgres::{
        PostgresStorageProvider, physical_names, record_read, record_write, sql_statements,
        stream_helpers::PostgresWriteStreamEntriesInput,
    },
    billing_metrics::{
        WriteCostTally, attr_map_payload_bytes, record_read_cost, record_write_cost,
        wire_items_payload_bytes,
    },
    errors::missing_index_error,
    helpers::{
        DEFAULT_QUERY_LIMIT, DEFAULT_SCAN_LIMIT, MAX_QUERY_LIMIT, MAX_SCAN_LIMIT,
        decode_exclusive_start,
    },
};

#[async_trait]
impl StorageProvider for PostgresStorageProvider {
    fn supports_guarded_writes(&self) -> bool {
        true
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
        crate::backends::postgres::logical_backfill::apply_resolved_sync_mutations(
            self, metadata, batch,
        )
        .await
    }

    async fn last_resolved_sync_log_id(&self) -> StorageResult<Option<storage_sync::SyncLogId>> {
        crate::backends::postgres::logical_backfill::last_resolved_sync_log_id(self).await
    }

    async fn persist_resolved_sync_log_entry(
        &self,
        metadata: &storage_sync::SyncCommitMetadata,
        batch: &storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<()> {
        crate::backends::postgres::logical_backfill::persist_resolved_sync_log_entry(
            self, metadata, batch,
        )
        .await
    }

    async fn get_resolved_sync_log_entry(
        &self,
        log_id: storage_sync::SyncLogId,
    ) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
        crate::backends::postgres::logical_backfill::get_resolved_sync_log_entry(self, log_id).await
    }

    async fn resolved_sync_log_entries_after(
        &self,
        log_id: Option<storage_sync::SyncLogId>,
        limit: usize,
    ) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
        crate::backends::postgres::logical_backfill::resolved_sync_log_entries_after(
            self, log_id, limit,
        )
        .await
    }

    async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        self.do_table_exists(table_name).await
    }

    async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        self.do_create_table(request).await
    }

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

    async fn list_tables(
        &self,
        limit: u32,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<Vec<StoredTableInfo>> {
        self.do_list_tables(limit, exclusive_start_table_name).await
    }

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

    async fn put_item(
        &self,
        table_name: TableName,
        item: HashMap<String, storage_provider::AttributeValue>,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, storage_provider::AttributeValue>>,
        return_values: Option<AllOld>,
    ) -> StorageResult<PutItemResponse> {
        let bytes_written = attr_map_payload_bytes(&item);
        let response = self
            .retry_postgres_conflicts("put_item", || {
                let table_name = table_name.clone();
                let item = item.clone();
                let condition_expression = condition_expression.clone();
                let expression_attribute_names = expression_attribute_names.clone();
                let expression_attribute_values = expression_attribute_values.clone();
                let return_values = return_values.clone();
                async move {
                    let condition = crate::storage_provider::parse_optional_condition(
                        condition_expression,
                        &expression_attribute_names,
                        &expression_attribute_values,
                    )?;

                    if item.is_empty() {
                        return Err(StorageError::validation("Item is empty"));
                    }

                    let table_info = self.get_table_info_cached_arc(&table_name).await?;
                    let split_item = split_item_into_key_and_attributes_sync(item, &table_info)?;
                    let key_bindings = Self::key_column_bindings_for_schema(
                        &table_info,
                        &table_info.key_schema,
                        &split_item.key_attributes,
                        None,
                    )?;
                    let mut client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    #[cfg(test)]
                    let tx_begin_started = Instant::now();
                    let transaction = client.transaction().await.map_err(|err| {
                        Self::map_postgres_write_error("start put_item transaction", err)
                    })?;
                    #[cfg(test)]
                    provider_perf::record("postgres", "tx_begin", tx_begin_started.elapsed());
                    let key_absence_condition =
                        is_key_absence_condition(condition.as_ref(), &table_info);
                    let old_item = if key_absence_condition {
                        None
                    } else {
                        #[cfg(test)]
                        let main_read_started = Instant::now();
                        let old_item = self
                            .get_item_with_client(
                                &transaction,
                                &table_name,
                                &split_item.key_attributes,
                                &table_info,
                            )
                            .await?;
                        #[cfg(test)]
                        provider_perf::record(
                            "postgres",
                            "sql_query_main_row",
                            main_read_started.elapsed(),
                        );
                        old_item
                    };

                    if !key_absence_condition && let Some(condition) = &condition {
                        let old_item_for_condition = old_item
                            .as_ref()
                            .map(WireItem::to_attribute_map)
                            .transpose()?
                            .unwrap_or_default();
                        if !evaluate_condition(&old_item_for_condition, condition) {
                            return Err(StorageEnum::ConditionalCheckFailed.into());
                        }
                    }

                    let attributes_blob = if split_item.non_key_attributes.is_empty() {
                        "{}".to_string()
                    } else {
                        serde_json::to_string(&split_item.non_key_attributes).map_err(|err| {
                            Self::map_postgres_error("serialize non-key attributes", err)
                        })?
                    };

                    let table_name_safe = table_name.sanitized_name();
                    let key_columns = key_bindings
                        .iter()
                        .map(|binding| binding.column.clone())
                        .collect::<Vec<_>>();
                    let columns_sql = key_columns
                        .iter()
                        .cloned()
                        .chain(std::iter::once("attributes_blob".to_string()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let mut value_placeholders = key_bindings
                        .iter()
                        .enumerate()
                        .map(|(idx, binding)| {
                            Self::postgres_placeholder_for_type(idx + 1, &binding.attribute_type)
                        })
                        .collect::<Vec<_>>();
                    value_placeholders.push(format!("${}", key_bindings.len() + 1));
                    let values_placeholders = value_placeholders.join(", ");
                    let conflict_target = key_columns.join(", ");
                    let assignments = key_columns
                        .iter()
                        .cloned()
                        .chain(std::iter::once("attributes_blob".to_string()))
                        .map(|column| format!("{column} = EXCLUDED.{column}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let sql = if key_absence_condition {
                        sql_statements::insert_main_row_returning(
                            &table_name_safe,
                            &columns_sql,
                            &values_placeholders,
                        )
                    } else {
                        sql_statements::upsert_main_row_returning(
                            &table_name_safe,
                            &columns_sql,
                            &values_placeholders,
                            &conflict_target,
                            &assignments,
                        )
                    };

                    let mut bind_values = key_bindings
                        .iter()
                        .map(|binding| binding.value.clone())
                        .collect::<Vec<_>>();
                    bind_values.push(attributes_blob);
                    #[cfg(test)]
                    let main_write_started = Instant::now();
                    let revision_key_json = split_item
                        .key_attributes
                        .canonical_dynamo_json()
                        .map_err(|err| {
                            StorageError::validation(format!(
                                "revision key must be Dynamo JSON encodable: {err}"
                            ))
                        })?;
                    let revision_sql = sql_statements::bump_item_revision_with_placeholders(
                        &format!("${}", bind_values.len() + 1),
                        &format!("${}", bind_values.len() + 2),
                    );
                    let combined_sql = sql_statements::dml_ctes(&[sql, revision_sql]);
                    let table_name_value = table_name.to_string();
                    let mut combined_bind_values = bind_values;
                    combined_bind_values.push(table_name_value);
                    combined_bind_values.push(revision_key_json);
                    let params: Vec<&(dyn ToSql + Sync)> = combined_bind_values
                        .iter()
                        .map(|value| value as &(dyn ToSql + Sync))
                        .collect();
                    let main_write_result = transaction.execute(&combined_sql, &params).await;
                    if let Err(err) = main_write_result {
                        if key_absence_condition && Self::is_postgres_constraint_error(&err) {
                            return Err(StorageEnum::ConditionalCheckFailed.into());
                        }
                        return Err(Self::map_postgres_write_error("put_item execute", err));
                    }
                    #[cfg(test)]
                    provider_perf::record(
                        "postgres",
                        "sql_execute_main_upsert",
                        main_write_started.elapsed(),
                    );
                    #[cfg(test)]
                    provider_perf::record(
                        "postgres",
                        "sql_execute_revision",
                        std::time::Duration::ZERO,
                    );

                    let old_item_for_ttl = old_item
                        .as_ref()
                        .map(WireItem::to_attribute_map)
                        .transpose()?;
                    if self.immediate_gsi_consistency {
                        self.apply_gsi_entries_for_item_change_with_client(
                            &transaction,
                            &table_name,
                            &table_info,
                            old_item_for_ttl.as_ref(),
                            Some(&split_item.all_attributes),
                        )
                        .await?;
                    }
                    #[cfg(test)]
                    let ttl_started = Instant::now();
                    self.sync_ttl_index_entries_with_client(
                        &transaction,
                        &table_info,
                        old_item_for_ttl.as_ref(),
                        Some(&split_item.all_attributes),
                    )
                    .await?;
                    #[cfg(test)]
                    provider_perf::record("postgres", "ttl_sync", ttl_started.elapsed());
                    let item_stream_version = storage_types::ItemStreamVersion::try_from(
                        Self::get_item_revision_with_client(
                            &transaction,
                            &table_name,
                            &split_item.key_attributes,
                        )
                        .await?,
                    )?;
                    #[cfg(test)]
                    let stream_started = Instant::now();
                    self.write_stream_entries_for_item_with_client(
                        &transaction,
                        &table_info,
                        &split_item.all_attributes,
                        PostgresWriteStreamEntriesInput {
                            old_item: old_item_for_ttl.as_ref(),
                            is_deleted: false,
                            item_stream_version,
                            replication: None,
                        },
                    )
                    .await?;
                    #[cfg(test)]
                    provider_perf::record("postgres", "stream_write", stream_started.elapsed());
                    #[cfg(test)]
                    let commit_started = Instant::now();
                    transaction.commit().await.map_err(|err| {
                        Self::map_postgres_write_error("commit put_item transaction", err)
                    })?;
                    #[cfg(test)]
                    provider_perf::record("postgres", "tx_commit", commit_started.elapsed());

                    let attributes = if matches!(return_values, Some(AllOld::AllOld)) {
                        old_item.map(WireItem::into_attribute_map).transpose()?
                    } else {
                        None
                    };
                    Ok(PutItemResponse {
                        attributes: attributes.map(Into::into),
                    })
                }
            })
            .await?;
        record_write(1, bytes_written as usize);
        record_write_cost("put_item", "put", 1, bytes_written);
        Ok(response)
    }

    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        _consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        let table_info = self.get_table_info_cached_arc(&table_name).await?;
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        let result = self
            .get_item_with_client(&client, &table_name, &key, &table_info)
            .await?;
        let bytes_read = result.as_ref().map_or(0, |item| item.payload_len());
        record_read(usize::from(result.is_some()), bytes_read);
        record_read_cost("get_item", "get", 1, bytes_read as u64);
        Ok(result)
    }

    async fn scan_table_with_item_stream_versions(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<ItemVersionedWireItem>, Option<String>)> {
        let table_info = self.get_table_info_cached_arc(&request.table_name).await?;
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        let (items, next_cursor) = self.scan_table(request).await?;
        let mut versioned = Vec::with_capacity(items.len());
        for item in items {
            let item_map = item.to_attribute_map()?;
            let split = split_item_into_key_and_attributes_sync(item_map, &table_info)?;
            let revision = Self::get_item_revision_with_client(
                &client,
                &request.table_name,
                &split.key_attributes,
            )
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
        let table_info = self.get_table_info_cached_arc(&request.table_name).await?;
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        let item = self
            .get_item_with_client(&client, &request.table_name, &request.key, &table_info)
            .await?;
        let revision =
            Self::get_item_revision_with_client(&client, &request.table_name, &request.key).await?;

        Ok(match item {
            Some(item) => DurablePointReadProof::Present {
                item: Box::new(item),
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
        expression_attribute_values: Option<HashMap<String, storage_provider::AttributeValue>>,
    ) -> StorageResult<Option<HashMap<String, storage_provider::AttributeValue>>> {
        let key_bytes = attr_map_payload_bytes(&key);
        let result = self
            .retry_postgres_conflicts("delete_item", || {
                let table_name = table_name.clone();
                let key = key.clone();
                let condition_expression = condition_expression.clone();
                let expression_attribute_names = expression_attribute_names.clone();
                let expression_attribute_values = expression_attribute_values.clone();
                async move {
                    let condition = crate::storage_provider::parse_optional_condition(
                        condition_expression,
                        &expression_attribute_names,
                        &expression_attribute_values,
                    )?;

                    let table_info = self.get_table_info_cached_arc(&table_name).await?;
                    let key_attributes = key.clone();
                    let mut client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    let transaction = client.transaction().await.map_err(|err| {
                        Self::map_postgres_write_error("start delete_item transaction", err)
                    })?;
                    let old_item = self
                        .get_item_with_client(
                            &transaction,
                            &table_name,
                            &key_attributes,
                            &table_info,
                        )
                        .await?;

                    if let Some(condition) = &condition {
                        let old_item_for_condition = old_item
                            .as_ref()
                            .map(WireItem::to_attribute_map)
                            .transpose()?
                            .unwrap_or_default();
                        if !evaluate_condition(&old_item_for_condition, condition) {
                            return Err(StorageEnum::ConditionalCheckFailed.into());
                        }
                    }

                    let Some(old_item) = old_item else {
                        return Ok(None);
                    };

                    let key_bindings = Self::key_column_bindings_for_schema(
                        &table_info,
                        &table_info.key_schema,
                        &key_attributes,
                        None,
                    )?;
                    let mut bind_values = Vec::with_capacity(key_bindings.len());
                    let where_sql =
                        Self::where_clause_for_bindings(&key_bindings, &mut bind_values);
                    let table_name_safe = table_name.sanitized_name();
                    let sql = sql_statements::delete_main_row(&table_name_safe, &where_sql);
                    let params: Vec<&(dyn ToSql + Sync)> = bind_values
                        .iter()
                        .map(|value| value as &(dyn ToSql + Sync))
                        .collect();
                    transaction.execute(&sql, &params).await.map_err(|err| {
                        Self::map_postgres_write_error("delete_item execute", err)
                    })?;
                    let item_stream_version = storage_types::ItemStreamVersion::try_from(
                        Self::bump_item_revision_with_client(
                            &transaction,
                            &table_name,
                            &key_attributes,
                        )
                        .await?,
                    )?;
                    let old_map = old_item.into_attribute_map()?;
                    if self.immediate_gsi_consistency {
                        self.delete_gsi_entries_for_item_with_client(
                            &transaction,
                            &table_name,
                            &table_info,
                            &old_map,
                        )
                        .await?;
                    }
                    self.sync_ttl_index_entries_with_client(
                        &transaction,
                        &table_info,
                        Some(&old_map),
                        None,
                    )
                    .await?;
                    self.write_stream_entries_for_item_with_client(
                        &transaction,
                        &table_info,
                        &old_map,
                        PostgresWriteStreamEntriesInput {
                            old_item: Some(&old_map),
                            is_deleted: true,
                            item_stream_version,
                            replication: None,
                        },
                    )
                    .await?;
                    transaction.commit().await.map_err(|err| {
                        Self::map_postgres_write_error("commit delete_item transaction", err)
                    })?;
                    Ok(Some(old_map))
                }
            })
            .await?;
        record_write(usize::from(result.is_some()), 0);
        record_write_cost("delete_item", "delete", 1, key_bytes);
        Ok(result)
    }

    async fn guarded_put_item(
        &self,
        request: GuardedPutItemRequest,
    ) -> StorageResult<PutItemResponse> {
        self.do_guarded_put_item(request).await
    }

    async fn guarded_delete_item(
        &self,
        request: GuardedDeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, storage_provider::AttributeValue>>> {
        self.do_guarded_delete_item(request).await
    }

    async fn apply_replication_mutation(&self, mutation: ReplicationMutation) -> StorageResult<()> {
        self.do_apply_replication_mutation(mutation).await
    }

    fn replication_apply_parallelism_hint(&self) -> usize {
        self.do_replication_apply_parallelism_hint()
    }

    async fn scan_table(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        if request.consistent_read && request.index_name.is_some() {
            return Err(StorageError::validation(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }

        let table_info = self.get_table_info_cached_arc(&request.table_name).await?;
        let effective_limit = calc_limit(request.limit, DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT)?;
        let exclusive_start_key = decode_exclusive_start(
            &request.exclusive_start_key,
            &table_info,
            &request.index_name,
        )?;

        let (physical_name, primary_key_schema, secondary_key_schema): (
            String,
            Vec<storage_types::KeySchemaElement>,
            Option<Vec<storage_types::KeySchemaElement>>,
        ) = if let Some(index_name) = &request.index_name {
            let Some(gsis) = &table_info.global_secondary_indexes else {
                return Err(missing_index_error(&table_info, index_name));
            };
            let Some(gsi) = gsis.iter().find(|gsi| gsi.index_name == *index_name) else {
                return Err(missing_index_error(&table_info, index_name));
            };
            (
                physical_names::physical_gsi_table_name(&request.table_name, index_name),
                gsi.key_schema.clone(),
                Some(table_info.key_schema.clone()),
            )
        } else {
            (
                physical_names::physical_table_name(&request.table_name),
                table_info.key_schema.clone(),
                None,
            )
        };

        let ordered_columns = Self::ordered_key_columns_for_origin(
            &table_info,
            &primary_key_schema,
            secondary_key_schema.as_deref(),
        )?;
        let mut where_clauses = Vec::new();
        let mut bind_values = Vec::new();
        if let Some(start_key) = exclusive_start_key.as_ref()
            && let Some(predicate) = Self::build_exclusive_start_predicate(
                &ordered_columns,
                start_key,
                true,
                &mut bind_values,
            )?
        {
            where_clauses.push(format!("({predicate})"));
        }

        let (items, has_more) = self
            .load_paginated_wire_items(
                &physical_name,
                &table_info,
                &primary_key_schema,
                secondary_key_schema.as_deref(),
                &where_clauses,
                &bind_values,
                true,
                effective_limit,
            )
            .await?;

        let last_evaluated_key = if has_more {
            items
                .last()
                .map(|item| item.last_evaluated_key(&table_info, &request.index_name))
                .transpose()?
                .flatten()
        } else {
            None
        };

        let bytes_read = wire_items_payload_bytes(&items);
        record_read(items.len(), bytes_read as usize);
        record_read_cost("scan_table", "scan", 1, bytes_read);
        Ok((items, last_evaluated_key))
    }

    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        if request.consistent_read && request.index_name.is_some() {
            return Err(StorageError::validation(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }

        let table_info = self.get_table_info_cached_arc(&request.table_name).await?;
        let effective_limit = calc_limit(request.limit, DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT)?;
        let scan_forward = request.scan_index_forward.unwrap_or(true);
        let exclusive_start_key = decode_exclusive_start(
            &request.exclusive_start_key,
            &table_info,
            &request.index_name,
        )?;

        let (physical_name, primary_key_schema, secondary_key_schema): (
            String,
            Vec<storage_types::KeySchemaElement>,
            Option<Vec<storage_types::KeySchemaElement>>,
        ) = if let Some(index_name) = &request.index_name {
            let Some(gsis) = &table_info.global_secondary_indexes else {
                return Err(missing_index_error(&table_info, index_name));
            };
            let Some(gsi) = gsis.iter().find(|gsi| gsi.index_name == *index_name) else {
                return Err(missing_index_error(&table_info, index_name));
            };
            (
                physical_names::physical_gsi_table_name(&request.table_name, index_name),
                gsi.key_schema.clone(),
                Some(table_info.key_schema.clone()),
            )
        } else {
            (
                physical_names::physical_table_name(&request.table_name),
                table_info.key_schema.clone(),
                None,
            )
        };

        let parsed_key_condition = parse_condition_expression(
            &request.key_condition_expression,
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
        )
        .map_err(|err| {
            StorageError::validation(format!("Invalid key condition expression: {err}"))
        })?;
        let key_attribute_types =
            Self::key_attribute_types_map_for_schema(&table_info, &primary_key_schema)?;
        let ordered_columns = Self::ordered_key_columns_for_origin(
            &table_info,
            &primary_key_schema,
            secondary_key_schema.as_deref(),
        )?;
        let mut where_clauses = Vec::new();
        let mut bind_values = Vec::new();
        where_clauses.push(Self::compile_key_condition_sql(
            &parsed_key_condition,
            &key_attribute_types,
            &mut bind_values,
        )?);
        if let Some(start_key) = exclusive_start_key.as_ref()
            && let Some(predicate) = Self::build_exclusive_start_predicate(
                &ordered_columns,
                start_key,
                scan_forward,
                &mut bind_values,
            )?
        {
            where_clauses.push(format!("({predicate})"));
        }

        let (items, has_more) = self
            .load_paginated_wire_items(
                &physical_name,
                &table_info,
                &primary_key_schema,
                secondary_key_schema.as_deref(),
                &where_clauses,
                &bind_values,
                scan_forward,
                effective_limit,
            )
            .await?;

        let last_evaluated_key = if has_more {
            items
                .last()
                .map(|item| item.last_evaluated_key(&table_info, &request.index_name))
                .transpose()?
                .flatten()
        } else {
            None
        };

        let bytes_read = wire_items_payload_bytes(&items);
        record_read(items.len(), bytes_read as usize);
        record_read_cost("query_table", "query", 1, bytes_read);
        Ok((items, last_evaluated_key))
    }

    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
        _should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        let mut billed_tally = WriteCostTally::default();
        for write_requests in request.request_items.values() {
            for write_request in write_requests {
                billed_tally.record_write_request(write_request);
            }
        }
        let response = self
            .retry_postgres_conflicts("batch_write_item", || {
                let request = request.clone();
                async move {
                    let mut client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    let tx = client.transaction().await.map_err(|err| {
                        Self::map_postgres_write_error("start batch_write_item transaction", err)
                    })?;

                    for (table_name, write_requests) in request.request_items {
                        self.get_table_info_cached_arc(&table_name).await?;

                        for write_request in write_requests {
                            match write_request {
                                WriteRequest {
                                    put_request: Some(put_request),
                                    delete_request: None,
                                } => {
                                    self.transact_put_with_client(
                                        &tx,
                                        storage_types::TransactPutRequest {
                                            table_name: table_name.clone(),
                                            item: put_request.item,
                                            condition_expression: None,
                                            expression_attribute_names: None,
                                            expression_attribute_values: None,
                                            return_values_on_condition_check_failure: None,
                                        },
                                        None,
                                    )
                                    .await?;
                                }
                                WriteRequest {
                                    put_request: None,
                                    delete_request: Some(delete_request),
                                } => {
                                    self.transact_delete_with_client(
                                        &tx,
                                        storage_types::TransactDeleteRequest {
                                            table_name: table_name.clone(),
                                            key: delete_request.key,
                                            condition_expression: None,
                                            expression_attribute_names: None,
                                            expression_attribute_values: None,
                                            return_values_on_condition_check_failure: None,
                                        },
                                        None,
                                    )
                                    .await?;
                                }
                                _ => {
                                    return Err(StorageError::validation(
                                        "Each WriteRequest must contain exactly one of PutRequest \
                                         or DeleteRequest",
                                    ));
                                }
                            }
                        }
                    }

                    tx.commit().await.map_err(|err| {
                        Self::map_postgres_write_error("commit batch_write_item transaction", err)
                    })?;

                    Ok(BatchWriteItemResponse {
                        unprocessed_items: None,
                        item_collection_metrics: None,
                        consumed_capacity: None,
                    })
                }
            })
            .await?;
        let applied_items = billed_tally.put_ops + billed_tally.delete_ops;
        let applied_bytes = billed_tally.put_bytes + billed_tally.delete_bytes;
        record_write(applied_items, applied_bytes as usize);
        billed_tally.emit("batch_write_item");
        Ok(response)
    }

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
    ) -> StorageResult<BatchGetWireItemResponse> {
        let total_requested_keys: usize = request
            .request_items
            .values()
            .map(|item| item.keys.len())
            .sum();
        let mut total_items_returned = 0usize;
        let mut total_bytes_read = 0usize;
        let mut responses: HashMap<TableName, Vec<WireItem>> = HashMap::new();
        let mut unprocessed_keys: HashMap<TableName, KeysAndAttributes> = HashMap::new();

        for (table_name, keys_and_attributes) in request.request_items {
            if keys_and_attributes.keys.is_empty() {
                continue;
            }

            let table_info = match self.get_table_info_cached_arc(&table_name).await {
                Ok(info) => info,
                Err(err) if matches!(err.as_ref(), StorageEnum::TableNotFound { .. }) => {
                    return Err(err);
                }
                Err(_) => {
                    unprocessed_keys.insert(table_name.clone(), keys_and_attributes);
                    continue;
                }
            };

            let _consistent_read = keys_and_attributes.consistent_read.unwrap_or(true);
            let key_columns = table_info
                .key_schema
                .iter()
                .map(|key| {
                    Ok((
                        key.attribute_name.clone(),
                        Self::sanitize_column_name(&key.attribute_name),
                        Self::key_attribute_type(&table_info, &key.attribute_name)?,
                    ))
                })
                .collect::<StorageResult<Vec<(String, String, storage_types::KeyAttributeType)>>>(
                )?;
            if key_columns.is_empty() {
                unprocessed_keys.insert(table_name.clone(), keys_and_attributes);
                continue;
            }
            let mut select_projection = Vec::with_capacity(key_columns.len() + 1);
            for (_, column_name, attribute_type) in &key_columns {
                if matches!(attribute_type, storage_types::KeyAttributeType::N) {
                    select_projection.push(format!("{column_name}::TEXT AS {column_name}"));
                } else {
                    select_projection.push(column_name.clone());
                }
            }
            select_projection.push("attributes_blob".to_string());
            let select_projection = select_projection.join(", ");

            let mut bind_values =
                Vec::with_capacity(key_columns.len() * keys_and_attributes.keys.len());
            let sql = if key_columns.len() == 1 {
                let (attribute_name, column_name, attribute_type) = &key_columns[0];
                let mut placeholders = Vec::with_capacity(keys_and_attributes.keys.len());
                for key in &keys_and_attributes.keys {
                    let key_attributes = key.clone();
                    let value = key_attributes
                        .get(attribute_name)
                        .ok_or_else(StorageError::invalid_or_missing_key)?;
                    bind_values.push(Self::scalar_key_value(value, attribute_name)?);
                    placeholders.push(Self::postgres_placeholder_for_type(
                        bind_values.len(),
                        attribute_type,
                    ));
                }
                sql_statements::batch_get_single_key(
                    &select_projection,
                    &physical_names::physical_table_name(&table_name),
                    column_name,
                    &placeholders.join(", "),
                )
            } else {
                let tuple_columns = key_columns
                    .iter()
                    .map(|(_, column_name, _)| column_name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut tuple_predicates = Vec::with_capacity(keys_and_attributes.keys.len());
                for key in &keys_and_attributes.keys {
                    let key_attributes = key.clone();
                    let mut tuple_placeholders = Vec::with_capacity(key_columns.len());
                    for (attribute_name, _, attribute_type) in &key_columns {
                        let value = key_attributes
                            .get(attribute_name)
                            .ok_or_else(StorageError::invalid_or_missing_key)?;
                        bind_values.push(Self::scalar_key_value(value, attribute_name)?);
                        tuple_placeholders.push(Self::postgres_placeholder_for_type(
                            bind_values.len(),
                            attribute_type,
                        ));
                    }
                    tuple_predicates.push(format!("({})", tuple_placeholders.join(", ")));
                }
                sql_statements::batch_get_composite_key(
                    &select_projection,
                    &physical_names::physical_table_name(&table_name),
                    &tuple_columns,
                    &tuple_predicates.join(", "),
                )
            };

            let params: Vec<&(dyn ToSql + Sync)> = bind_values
                .iter()
                .map(|value| value as &(dyn ToSql + Sync))
                .collect();
            let client = match self.pool.get().await {
                Ok(client) => client,
                Err(_) => {
                    unprocessed_keys.insert(table_name.clone(), keys_and_attributes);
                    continue;
                }
            };
            match client.query(&sql, &params).await {
                Ok(rows) => {
                    let mut table_items = Vec::with_capacity(rows.len());
                    for row in rows {
                        table_items.push(Self::row_to_wire_item(&row, &table_info)?);
                    }
                    total_items_returned += table_items.len();
                    total_bytes_read += wire_items_payload_bytes(&table_items) as usize;
                    if !table_items.is_empty() {
                        responses.insert(table_name, table_items);
                    }
                }
                Err(_) => {
                    unprocessed_keys.insert(table_name.clone(), keys_and_attributes);
                }
            }
        }

        let response = BatchGetWireItemResponse {
            responses: if responses.is_empty() {
                None
            } else {
                Some(responses)
            },
            unprocessed_keys: if unprocessed_keys.is_empty() {
                None
            } else {
                Some(unprocessed_keys)
            },
            consumed_capacity: None,
        };
        record_read(total_items_returned, total_bytes_read);
        record_read_cost(
            "batch_get_item",
            "get",
            total_requested_keys,
            total_bytes_read as u64,
        );
        Ok(response)
    }

    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
        self.do_update_item(request).await
    }

    async fn guarded_update_item(
        &self,
        request: GuardedUpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        self.do_guarded_update_item(request).await
    }

    async fn transact_write_items(
        &self,
        request: storage_types::TransactWriteItemsRequest,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse> {
        self.do_transact_write_items(request).await
    }

    async fn update_table(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse> {
        self.do_update_table(request).await
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

            if !table_info
                .attribute_definitions
                .iter()
                .any(|def| def.attribute_name == attribute_name)
            {
                table_info
                    .attribute_definitions
                    .push(storage_types::AttributeDefinition {
                        attribute_name: attribute_name.clone(),
                        attribute_type: storage_types::KeyAttributeType::N,
                    });
                let definitions_json = serde_json::to_string(&table_info.attribute_definitions)
                    .map_err(|err| {
                        Self::map_postgres_error("serialize ttl attribute definitions", err)
                    })?;
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(Self::map_postgres_client_acquire_error)?;
                client
                    .execute(
                        sql_statements::update_attribute_definitions(),
                        &[&definitions_json, &table_name.as_ref()],
                    )
                    .await
                    .map_err(|err| {
                        Self::map_postgres_error("persist ttl attribute definitions", err)
                    })?;
            }

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
            self.invalidate_table_info_cache(&table_name).await;

            Ok(UpdateTimeToLiveResponse {
                time_to_live_specification,
            })
        } else {
            if let Some(config) = existing_config {
                self.drop_ttl_index_table(&table_name).await?;
                self.delete_ttl_config(&table_name).await?;
                time_to_live_specification.attribute_name = config.attribute_name;
            }
            time_to_live_specification.enabled = false;
            self.invalidate_table_info_cache(&table_name).await;
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

    async fn run_job(&self, name: bg_jobs::BackgroundJobName) -> StorageResult<()> {
        match name {
            GSI_UPDATE_JOB => {
                if self.immediate_gsi_consistency {
                    return Ok(());
                }
                loop {
                    let progressed = self.process_gsi_updates().await?;
                    if !progressed {
                        break;
                    }
                }
                Ok(())
            }
            TTL_SWEEP_JOB => {
                loop {
                    let progressed = self.run_ttl_sweep_once().await?;
                    if !progressed {
                        break;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl PostgresStorageProvider {
    async fn ensure_gsi_update_cursor(
        &self,
        stream_name: &StreamName,
        cursor_name: &CursorName,
    ) -> StorageResult<Option<StreamItemId>> {
        let mut cursor_position = self
            .get_cursor(stream_name.clone(), cursor_name.clone())
            .await
            .map_err(|err| {
                StorageError::internal(&format!("postgres get gsi cursor failed: {err}"))
            })?
            .map(|cursor| cursor.position);

        if cursor_position.is_none() {
            self.create_cursor(
                stream_name.clone(),
                cursor_name.clone(),
                CursorPosition::Head,
            )
            .await
            .map_err(|err| {
                StorageError::internal(&format!("postgres create gsi cursor failed: {err}"))
            })?;
            cursor_position = self
                .get_cursor(stream_name.clone(), cursor_name.clone())
                .await
                .map_err(|err| {
                    StorageError::internal(&format!("postgres reload gsi cursor failed: {err}"))
                })?
                .map(|cursor| cursor.position);
        }

        Ok(cursor_position)
    }

    pub(super) async fn process_gsi_updates(&self) -> StorageResult<bool> {
        let cursor_name: CursorName = "gsi-update-cursor".to_string().into();
        let stream_name = StreamName::system_table_stream();
        let mut cursor_position = self
            .ensure_gsi_update_cursor(&stream_name, &cursor_name)
            .await?;
        let mut did_work = false;
        let mut table_infos: HashMap<TableName, Option<StoredTableInfo>> = HashMap::new();

        loop {
            let records_result = self
                .get_items_from_pointer_stream(
                    stream_name.clone(),
                    cursor_position,
                    Some(crate::constants::GSI_UPDATE_STREAM_FETCH_LIMIT),
                )
                .await
                .map_err(|err| {
                    StorageError::internal(&format!("postgres gsi stream read failed: {err}"))
                })?;

            let had_more = records_result.last_evaluated_key.is_some();
            let last_item = records_result.last_evaluated_key.or_else(|| {
                records_result
                    .records
                    .last()
                    .map(|(pointer, _)| pointer.stream_item_id)
            });
            let records = records_result.records;

            if records.is_empty() {
                return Ok(did_work);
            }

            let mut client = self
                .pool
                .get()
                .await
                .map_err(Self::map_postgres_client_acquire_error)?;
            let transaction = client.transaction().await.map_err(|err| {
                Self::map_postgres_write_error("start postgres gsi update transaction", err)
            })?;

            for (pointer, stream_items) in records {
                let filtered_info = if let Some(cached) = table_infos.get(&pointer.table_name) {
                    cached.clone()
                } else {
                    let loaded = self
                        .get_table_info(&pointer.table_name)
                        .await
                        .ok()
                        .and_then(|info| postgres_user_gsi_table_info(&info));
                    table_infos.insert(pointer.table_name.clone(), loaded.clone());
                    loaded
                };
                let Some(table_info) = filtered_info.as_ref() else {
                    continue;
                };

                let (old_item, new_item) = postgres_gsi_images(&stream_items);
                if old_item.is_some() || new_item.is_some() {
                    self.apply_gsi_entries_for_item_change_with_client(
                        &transaction,
                        &pointer.table_name,
                        table_info,
                        old_item.as_ref(),
                        new_item.as_ref(),
                    )
                    .await?;
                    did_work = true;
                }
            }

            transaction.commit().await.map_err(|err| {
                Self::map_postgres_write_error("commit postgres gsi update transaction", err)
            })?;

            let Some(last_item) = last_item else {
                return Ok(did_work);
            };
            self.advance_cursor(stream_name.clone(), cursor_name.clone(), last_item)
                .await
                .map_err(|err| {
                    StorageError::internal(&format!("postgres advance gsi cursor failed: {err}"))
                })?;
            cursor_position = Some(last_item);

            if !had_more {
                return Ok(did_work);
            }
        }
    }
}

fn postgres_user_gsi_table_info(table_info: &StoredTableInfo) -> Option<StoredTableInfo> {
    let mut filtered = table_info.clone();
    filtered.global_secondary_indexes = table_info.global_secondary_indexes.as_ref().map(|gsis| {
        gsis.iter()
            .filter(|gsi| !storage_common::ttl::is_ttl_index(&gsi.index_name))
            .cloned()
            .collect::<Vec<_>>()
    });
    filtered
        .global_secondary_indexes
        .as_ref()
        .filter(|gsis| !gsis.is_empty())?;
    Some(filtered)
}

type PostgresGsiImage = Option<HashMap<String, storage_provider::AttributeValue>>;

fn postgres_gsi_images(stream_items: &[StreamItem]) -> (PostgresGsiImage, PostgresGsiImage) {
    let Some(first) = stream_items.first() else {
        return (None, None);
    };

    if first.data_type == StreamDataType::DeleteMarker {
        let old_item = stream_items
            .last()
            .and_then(|item| storage_types::storage_serde::from_bytes(&item.data).ok());
        return (old_item, None);
    }

    let new_item = storage_types::storage_serde::from_bytes(&first.data).ok();
    let old_item = stream_items
        .get(1)
        .filter(|item| item.data_type != StreamDataType::DeleteMarker)
        .and_then(|item| storage_types::storage_serde::from_bytes(&item.data).ok());
    (old_item, new_item)
}

fn is_key_absence_condition(condition: Option<&Condition>, table_info: &StoredTableInfo) -> bool {
    let Some(Condition::NotExists { field }) = condition else {
        return false;
    };
    table_info
        .key_schema
        .iter()
        .any(|key| key.key_type == storage_types::KeyType::Hash && key.attribute_name == *field)
}
