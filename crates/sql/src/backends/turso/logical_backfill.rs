use std::collections::HashMap;

use storage_backfill::{
    LogicalBackfillChunk, LogicalBackfillDomain, LogicalBackfillExport, LogicalBackfillImport,
    LogicalBackfillRecord, LogicalBackfillResult, LogicalBackfillTombstone, LogicalExportPage,
    LogicalExportRequest, LogicalImportApplyCase, LogicalImportApplyDecision,
    LogicalImportRecordKind, plan_logical_import_apply, validate_logical_chunk_for_manifest,
};
use storage_provider::{StorageProvider, split_item_into_key_and_attributes_sync};
use storage_types::{
    AttributeValue, ItemStreamVersion, KeyAttributes, ScanTableRequest, StorageError,
    StorageResult, TableName,
};
use turso::Value as TursoValue;

pub(crate) use super::logical_backfill_sync::{
    apply_resolved_sync_mutations, get_resolved_sync_log_entry, last_resolved_sync_log_id,
    persist_resolved_sync_log_entry, resolved_sync_log_entries_after,
};
pub(super) use super::logical_backfill_values::{
    payload_i64, payload_optional_i64, payload_optional_string, payload_string, row_blob, row_i64,
    row_optional_i64, row_optional_text, row_text, unchecked_checksum,
};
use super::{
    TursoStorageProvider,
    logical_backfill_gsi::{export_gsi_records, import_gsi_record},
    logical_backfill_metadata::{import_table_metadata_record, table_metadata_record},
    logical_backfill_stream::{export_stream_records, import_stream_record},
    logical_backfill_values::log_component_i64,
    provider::{TursoWriteStreamEntriesInput, build_key_where_clause},
    sql_statements,
};

#[async_trait::async_trait]
impl LogicalBackfillExport for TursoStorageProvider {
    async fn export_logical_page(
        &self,
        request: LogicalExportRequest,
    ) -> Result<LogicalExportPage, StorageError> {
        match request.domain {
            LogicalBackfillDomain::TableMetadata => export_table_metadata(self, request).await,
            LogicalBackfillDomain::ItemRecords => export_item_records(self, request).await,
            LogicalBackfillDomain::DurableRevisions => {
                export_durable_revisions(self, request).await
            }
            LogicalBackfillDomain::TtlRecords => export_empty_domain(request),
            LogicalBackfillDomain::StreamRecords => export_stream_records(self, request).await,
            LogicalBackfillDomain::GsiRecords => export_gsi_records(self, request).await,
            LogicalBackfillDomain::Tombstones
            | LogicalBackfillDomain::StorageControlPlane
            | LogicalBackfillDomain::BackgroundJobs
            | LogicalBackfillDomain::SyncControlPlane => export_empty_domain(request),
        }
    }
}

#[async_trait::async_trait]
impl LogicalBackfillImport for TursoStorageProvider {
    async fn import_logical_chunk(
        &self,
        manifest: &storage_backfill::LogicalBackfillManifest,
        chunk: LogicalBackfillChunk,
    ) -> Result<LogicalBackfillResult, StorageError> {
        validate_logical_chunk_for_manifest(manifest, &chunk).map_err(|error| {
            StorageError::validation(format!("logical chunk rejected: {error}"))
        })?;
        import_logical_records(self, chunk.records).await?;
        Ok(LogicalBackfillResult::ChunkImported)
    }
}

async fn export_table_metadata(
    provider: &TursoStorageProvider,
    request: LogicalExportRequest,
) -> StorageResult<LogicalExportPage> {
    let records = if let Some(table_name) = request.table_name {
        vec![table_metadata_record(
            provider
                .get_table_info(&TableName::new(&table_name))
                .await?,
        )?]
    } else {
        provider
            .list_tables(request.limit, None)
            .await?
            .into_iter()
            .map(table_metadata_record)
            .collect::<StorageResult<Vec<_>>>()?
    };
    Ok(LogicalExportPage {
        domain: LogicalBackfillDomain::TableMetadata,
        records,
        next_cursor: None,
        checksum: unchecked_checksum()?,
    })
}

async fn export_item_records(
    provider: &TursoStorageProvider,
    request: LogicalExportRequest,
) -> StorageResult<LogicalExportPage> {
    let table_name = request
        .table_name
        .as_deref()
        .map(storage_types::TableName::new)
        .ok_or_else(|| StorageError::validation("item export requires table_name"))?;
    let scan = ScanTableRequest {
        table_name: table_name.clone(),
        index_name: None,
        limit: Some(request.limit),
        exclusive_start_key: request.cursor,
        consistent_read: true,
    };
    let (items, next_cursor) = provider.scan_table_with_item_stream_versions(&scan).await?;
    let table_info = provider.load_table_info_cached(&table_name).await?;
    let mut records = Vec::with_capacity(items.len());
    for versioned in items {
        let item = versioned.item.to_attribute_map()?;
        let key_attributes = provider.get_key_attributes(&item, &table_info.key_schema)?;
        records.push(LogicalBackfillRecord::PresentItem {
            table_name: table_name.as_ref().to_string(),
            key_json: key_attributes.canonical_dynamo_json().map_err(|error| {
                StorageError::validation(format!("logical export key encoding failed: {error}"))
            })?,
            item_json: serde_json::to_string(&item)?,
            indexers: versioned.indexers,
            item_stream_version: versioned.item_stream_version,
        });
    }
    Ok(LogicalExportPage {
        domain: LogicalBackfillDomain::ItemRecords,
        records,
        next_cursor,
        checksum: unchecked_checksum()?,
    })
}

async fn export_durable_revisions(
    provider: &TursoStorageProvider,
    request: LogicalExportRequest,
) -> StorageResult<LogicalExportPage> {
    let conn = provider.connect().await?;
    let limit = i64::from(request.limit);
    let (sql, params) = if let Some(table_name) = request.table_name {
        (
            "SELECT table_name, key_json, revision FROM item_revisions WHERE table_name = ?1 \
             ORDER BY table_name, key_json LIMIT ?2",
            vec![TursoValue::Text(table_name), TursoValue::Integer(limit)],
        )
    } else {
        (
            "SELECT table_name, key_json, revision FROM item_revisions ORDER BY table_name, \
             key_json LIMIT ?1",
            vec![TursoValue::Integer(limit)],
        )
    };
    let rows = provider.query_rows(&conn, sql, params).await?;
    let records = rows
        .iter()
        .map(durable_revision_record_from_row)
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(LogicalExportPage {
        domain: LogicalBackfillDomain::DurableRevisions,
        records,
        next_cursor: None,
        checksum: unchecked_checksum()?,
    })
}

fn export_empty_domain(request: LogicalExportRequest) -> StorageResult<LogicalExportPage> {
    Ok(LogicalExportPage {
        domain: request.domain,
        records: Vec::new(),
        next_cursor: None,
        checksum: unchecked_checksum()?,
    })
}

async fn import_logical_records(
    provider: &TursoStorageProvider,
    records: Vec<LogicalBackfillRecord>,
) -> StorageResult<()> {
    let this = provider.clone();
    this.with_exclusive_transaction(true, |conn| {
        let this = this.clone();
        let records = records.clone();
        Box::pin(async move {
            for record in records {
                match record {
                    LogicalBackfillRecord::PresentItem {
                        table_name,
                        item_json,
                        indexers,
                        item_stream_version,
                        key_json: _,
                    } => {
                        import_present_item(
                            &this,
                            conn,
                            &table_name,
                            &item_json,
                            &indexers,
                            item_stream_version,
                        )
                        .await?;
                    }
                    LogicalBackfillRecord::Tombstone(tombstone) => {
                        import_tombstone(&this, conn, tombstone).await?;
                    }
                    LogicalBackfillRecord::DomainRecord {
                        domain,
                        payload_json,
                        ..
                    } => match domain {
                        LogicalBackfillDomain::TableMetadata => {
                            import_table_metadata_record(&this, conn, &payload_json).await?;
                        }
                        LogicalBackfillDomain::DurableRevisions => {
                            import_durable_revision_record(&this, conn, &payload_json).await?;
                        }
                        LogicalBackfillDomain::StreamRecords => {
                            import_stream_record(&this, conn, &payload_json).await?;
                        }
                        LogicalBackfillDomain::GsiRecords => {
                            import_gsi_record(&this, conn, &payload_json).await?;
                        }
                        LogicalBackfillDomain::ItemRecords
                        | LogicalBackfillDomain::Tombstones
                        | LogicalBackfillDomain::TtlRecords
                        | LogicalBackfillDomain::StorageControlPlane
                        | LogicalBackfillDomain::BackgroundJobs
                        | LogicalBackfillDomain::SyncControlPlane => {
                            return Err(StorageError::validation(format!(
                                "turso logical import received unexpected domain record for \
                                 {domain:?}"
                            )));
                        }
                    },
                    LogicalBackfillRecord::StreamRecord { .. } => {
                        return Err(StorageError::validation(
                            "turso logical import currently imports stream rows through domain \
                             records",
                        ));
                    }
                }
            }
            Ok(())
        })
    })
    .await
}

async fn import_present_item<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    table_name: &str,
    item_json: &str,
    indexers: &[String],
    item_stream_version: ItemStreamVersion,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let table_name = TableName::new(&table_name);
    let table_info = provider.load_table_info_cached(&table_name).await?;
    let item = serde_json::from_str::<HashMap<String, AttributeValue>>(item_json)?;
    let split = split_item_into_key_and_attributes_sync(item, &table_info)?;
    let (old_item, old_indexers) = provider
        .get_item_map_with_indexers_by_key(conn, &table_info, &split.key_attributes)
        .await?
        .map_or_else(
            || (None, Vec::new()),
            |(item, indexers)| (Some(item), indexers),
        );
    let current_version =
        current_item_stream_version(provider, conn, &table_name, &split.key_attributes).await?;
    let decision = plan_logical_import_apply(LogicalImportApplyCase::new(
        current_version,
        item_stream_version,
        LogicalImportRecordKind::PresentItem,
    ));
    if !matches!(decision, LogicalImportApplyDecision::ApplyPresentItem) {
        return Ok(());
    }

    provider
        .upsert_main_row(
            conn,
            &table_info,
            &split.key_attributes,
            &split.all_attributes,
            &split.non_key_attributes,
            Some(indexers),
        )
        .await?;
    set_item_revision(
        provider,
        conn,
        &table_name,
        &split.key_attributes,
        item_stream_version,
    )
    .await?;
    provider
        .write_stream_entries_for_item_change(
            conn,
            &table_info,
            &split.all_attributes,
            TursoWriteStreamEntriesInput {
                old_item: old_item.as_ref(),
                indexers,
                old_indexers: old_item.as_ref().map(|_| old_indexers.as_slice()),
                is_deleted: false,
                item_stream_version,
                replication: None,
            },
        )
        .await?;
    if provider.immediate_gsi_consistency {
        provider
            .apply_gsi_rows_for_item_change(
                conn,
                &table_info,
                old_item.as_ref(),
                Some(&split.all_attributes),
                indexers,
            )
            .await?;
    }
    Ok(())
}

async fn import_tombstone<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    tombstone: LogicalBackfillTombstone,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let table_name = TableName::new(&tombstone.table_name);
    let table_info = provider.load_table_info_cached(&table_name).await?;
    let key = serde_json::from_str::<KeyAttributes>(&tombstone.key_json)?;
    let (old_item, old_indexers) = provider
        .get_item_map_with_indexers_by_key(conn, &table_info, &key)
        .await?
        .map_or_else(
            || (None, Vec::new()),
            |(item, indexers)| (Some(item), indexers),
        );
    let current_version = current_item_stream_version(provider, conn, &table_name, &key).await?;
    let decision = plan_logical_import_apply(LogicalImportApplyCase::new(
        current_version,
        tombstone.item_stream_version,
        LogicalImportRecordKind::Tombstone,
    ));
    if !matches!(decision, LogicalImportApplyDecision::ApplyTombstone) {
        return Ok(());
    }

    delete_main_row(provider, conn, &table_info, &key).await?;
    set_item_revision(
        provider,
        conn,
        &table_name,
        &key,
        tombstone.item_stream_version,
    )
    .await?;
    provider
        .write_stream_entries_for_item_change(
            conn,
            &table_info,
            &key.to_attribute_map(),
            TursoWriteStreamEntriesInput {
                old_item: old_item.as_ref(),
                indexers: &[],
                old_indexers: old_item.as_ref().map(|_| old_indexers.as_slice()),
                is_deleted: true,
                item_stream_version: tombstone.item_stream_version,
                replication: None,
            },
        )
        .await?;
    if provider.immediate_gsi_consistency {
        provider
            .apply_gsi_rows_for_item_change(conn, &table_info, old_item.as_ref(), None, &[])
            .await?;
    }
    Ok(())
}

async fn current_item_stream_version<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    table_name: &TableName,
    key: &KeyAttributes,
) -> StorageResult<Option<ItemStreamVersion>>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let revision = provider.get_item_revision(conn, table_name, key).await?;
    if revision == 0 {
        Ok(None)
    } else {
        Ok(Some(ItemStreamVersion::try_from(revision)?))
    }
}

async fn import_durable_revision_record<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    payload_json: &str,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let payload: serde_json::Value = serde_json::from_str(payload_json)?;
    let table_name = payload_string(&payload, "table_name")?;
    let key_json = payload_string(&payload, "key_json")?;
    let revision = payload_i64(&payload, "revision")?;
    let _ = provider
        .execute(
            conn,
            r"INSERT INTO item_revisions (table_name, key_json, revision)
              VALUES (?1, ?2, ?3)
              ON CONFLICT(table_name, key_json)
              DO UPDATE SET revision = excluded.revision",
            vec![
                TursoValue::Text(table_name),
                TursoValue::Text(key_json),
                TursoValue::Integer(revision),
            ],
        )
        .await?;
    Ok(())
}

fn durable_revision_record_from_row(
    row: &HashMap<String, TursoValue>,
) -> StorageResult<LogicalBackfillRecord> {
    let table_name = row_text(row, "table_name")?.to_string();
    let key_json = row_text(row, "key_json")?.to_string();
    let revision = row
        .get("revision")
        .map(super::provider::value_to_i64)
        .transpose()?
        .ok_or_else(|| StorageError::internal("missing durable revision column"))?;
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::DurableRevisions,
        record_key_json: serde_json::json!({
            "table_name": table_name,
            "key_json": key_json,
        })
        .to_string(),
        payload_json: serde_json::json!({
            "table_name": table_name,
            "key_json": key_json,
            "revision": revision,
        })
        .to_string(),
    })
}

async fn set_item_revision<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    table_name: &storage_types::TableName,
    key: &KeyAttributes,
    version: storage_types::ItemStreamVersion,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let revision = log_component_i64(version.get(), "item stream version")?;
    let key_json = super::provider::canonical_revision_key(key)?;
    let _ = provider
        .execute(
            conn,
            r"INSERT INTO item_revisions (table_name, key_json, revision)
              VALUES (?1, ?2, ?3)
              ON CONFLICT(table_name, key_json)
              DO UPDATE SET revision = excluded.revision",
            vec![
                TursoValue::Text(table_name.to_string()),
                TursoValue::Text(key_json),
                TursoValue::Integer(revision),
            ],
        )
        .await?;
    Ok(())
}

async fn delete_main_row<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    table_info: &storage_types::StoredTableInfo,
    key: &KeyAttributes,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let table_name_safe = table_info.table_name.sanitized_name();
    let (where_clause, params) = build_key_where_clause(key, &table_info.key_schema)?;
    let sql = sql_statements::delete_main_row(&table_name_safe, &where_clause);
    let _ = provider.execute(conn, &sql, params).await?;
    Ok(())
}
