use crate::backends::turso::provider::storage_provider_impl::*;

struct TursoReadSequenceReadContext {
    provider: TursoStorageProvider,
    connection: tokio::sync::Mutex<Option<tokio::sync::OwnedMutexGuard<TursoConnection>>>,
}

#[async_trait]
impl StorageProviderReadContext for TursoReadSequenceReadContext {
    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        let _ = consistent_read;
        let database_call = metrics_facade::begin_database_call("read_sequence.get_item");
        let table_info = self.provider.get_table_info(&table_name).await?;
        let guard = self.connection.lock().await;
        let conn = guard.as_ref().ok_or_else(turso_read_context_closed)?;
        let item = self
            .provider
            .get_item_map_by_key(conn, &table_info, &key)
            .await?;
        let result = item
            .map(|map| WireItem::from_attribute_map(&map))
            .transpose();
        drop(database_call);
        result
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        let database_call = metrics_facade::begin_database_call("read_sequence.batch_get_item");
        let guard = self.connection.lock().await;
        let conn = guard.as_ref().ok_or_else(turso_read_context_closed)?;
        let result = self
            .provider
            .batch_get_item_with_connection(conn, request)
            .await;
        drop(database_call);
        result
    }

    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let database_call = metrics_facade::begin_database_call("read_sequence.query_table");
        let guard = self.connection.lock().await;
        let conn = guard.as_ref().ok_or_else(turso_read_context_closed)?;
        let result = self
            .provider
            .query_table_with_connection(conn, request)
            .await;
        drop(database_call);
        result
    }
}

impl Drop for TursoReadSequenceReadContext {
    fn drop(&mut self) {
        let Ok(mut guard) = self.connection.try_lock() else {
            return;
        };
        let Some(connection) = guard.take() else {
            return;
        };
        tokio::spawn(async move {
            let _ = connection.execute("ROLLBACK", ()).await;
        });
    }
}

fn turso_read_context_closed() -> StorageError {
    StorageError::internal("turso read-sequence read context is closed")
}

impl TursoStorageProvider {
    pub(crate) fn supports_guarded_writes_operation(&self) -> bool {
        true
    }

    pub(crate) fn supports_custom_stream_duration_operation(&self) -> bool {
        true
    }

    pub(crate) fn supports_change_index_operation(&self) -> bool {
        true
    }

    pub(crate) async fn begin_read_sequence_read_context_operation(
        &self,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<Box<dyn StorageProviderReadContext>> {
        if consistency != ReadSequenceConsistency::Transactional {
            return Err(StorageError::unsupported(
                "turso read-sequence provider contexts are only used for transactional reads",
            ));
        }
        let conn = self.connect().await?;
        conn.execute("BEGIN", ()).await.map_err(map_turso_error)?;
        Ok(Box::new(TursoReadSequenceReadContext {
            provider: self.clone(),
            connection: tokio::sync::Mutex::new(Some(conn)),
        }))
    }

    pub(crate) async fn write_stream_trim_state_operation(
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

    pub(crate) async fn list_due_stream_trim_markers_operation(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<storage_provider::StreamTrimDueMarker>> {
        let conn = self.connect().await?;
        self.list_due_stream_trim_markers(&conn, due_before, limit)
            .await
    }

    pub(crate) async fn list_change_index_markers_operation(
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

    pub(crate) async fn initialize_storage_operation(&self) -> StorageResult<()> {
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

    pub(crate) async fn export_logical_backfill_page_operation(
        &self,
        request: storage_backfill::LogicalExportRequest,
    ) -> StorageResult<storage_backfill::LogicalExportPage> {
        LogicalBackfillExport::export_logical_page(self, request).await
    }

    pub(crate) async fn import_logical_backfill_chunk_operation(
        &self,
        manifest: &storage_backfill::LogicalBackfillManifest,
        chunk: storage_backfill::LogicalBackfillChunk,
    ) -> StorageResult<storage_backfill::LogicalBackfillResult> {
        LogicalBackfillImport::import_logical_chunk(self, manifest, chunk).await
    }

    pub(crate) async fn apply_resolved_sync_mutations_operation(
        &self,
        metadata: storage_sync::SyncCommitMetadata,
        batch: storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<Vec<storage_sync::SyncMutationResponse>> {
        crate::backends::turso::logical_backfill::apply_resolved_sync_mutations(
            self, metadata, batch,
        )
        .await
    }

    pub(crate) async fn last_resolved_sync_log_id_operation(
        &self,
    ) -> StorageResult<Option<storage_sync::SyncLogId>> {
        crate::backends::turso::logical_backfill::last_resolved_sync_log_id(self).await
    }

    pub(crate) async fn persist_resolved_sync_log_entry_operation(
        &self,
        metadata: &storage_sync::SyncCommitMetadata,
        batch: &storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<()> {
        crate::backends::turso::logical_backfill::persist_resolved_sync_log_entry(
            self, metadata, batch,
        )
        .await
    }

    pub(crate) async fn get_resolved_sync_log_entry_operation(
        &self,
        log_id: storage_sync::SyncLogId,
    ) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
        crate::backends::turso::logical_backfill::get_resolved_sync_log_entry(self, log_id).await
    }

    pub(crate) async fn resolved_sync_log_entries_after_operation(
        &self,
        log_id: Option<storage_sync::SyncLogId>,
        limit: usize,
    ) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
        crate::backends::turso::logical_backfill::resolved_sync_log_entries_after(
            self, log_id, limit,
        )
        .await
    }

    pub(crate) async fn table_exists_operation(
        &self,
        table_name: &TableName,
    ) -> StorageResult<bool> {
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
}
