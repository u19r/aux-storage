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

pub(crate) use super::logical_backfill_sync::{
    apply_resolved_sync_mutations, get_resolved_sync_log_entry, last_resolved_sync_log_id,
    persist_resolved_sync_log_entry, resolved_sync_log_entries_after,
};
pub(super) use super::logical_backfill_values::{
    payload_i64, payload_optional_i64, payload_optional_string, payload_string, unchecked_checksum,
};
use super::{
    PostgresStorageProvider,
    logical_backfill_gsi::{export_gsi_records, import_gsi_record},
    logical_backfill_metadata::{
        durable_revision_record_from_row, import_durable_revision_record,
        import_table_metadata_record, import_ttl_record, table_metadata_record, ttl_record,
    },
    logical_backfill_stream::{export_stream_records, import_stream_record},
    logical_backfill_sync_store::{delete_main_row, set_item_revision, upsert_main_row},
    stream_helpers::PostgresWriteStreamEntriesInput,
};

#[async_trait::async_trait]
impl LogicalBackfillExport for PostgresStorageProvider {
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
            LogicalBackfillDomain::TtlRecords => export_ttl_records(self, request).await,
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
impl LogicalBackfillImport for PostgresStorageProvider {
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
    provider: &PostgresStorageProvider,
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
    provider: &PostgresStorageProvider,
    request: LogicalExportRequest,
) -> StorageResult<LogicalExportPage> {
    let table_name = request
        .table_name
        .as_deref()
        .map(TableName::new)
        .ok_or_else(|| StorageError::validation("item export requires table_name"))?;
    let (items, next_cursor) = provider
        .scan_table_with_item_stream_versions(&ScanTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            limit: Some(request.limit),
            exclusive_start_key: request.cursor,
            consistent_read: true,
        })
        .await?;
    let table_info = provider.get_table_info(&table_name).await?;
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
    provider: &PostgresStorageProvider,
    request: LogicalExportRequest,
) -> StorageResult<LogicalExportPage> {
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    let limit = i64::from(request.limit);
    let rows = if let Some(table_name) = request.table_name {
        client
            .query(
                "SELECT table_name, key_json, revision FROM item_revisions WHERE table_name = $1 \
                 ORDER BY table_name, key_json LIMIT $2",
                &[&table_name, &limit],
            )
            .await
    } else {
        client
            .query(
                "SELECT table_name, key_json, revision FROM item_revisions ORDER BY table_name, \
                 key_json LIMIT $1",
                &[&limit],
            )
            .await
    }
    .map_err(|err| PostgresStorageProvider::map_postgres_error("export item revisions", err))?;
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

async fn export_ttl_records(
    provider: &PostgresStorageProvider,
    request: LogicalExportRequest,
) -> StorageResult<LogicalExportPage> {
    let mut records = Vec::new();
    for (table_name, config) in provider.list_ttl_configs().await? {
        if request
            .table_name
            .as_ref()
            .is_some_and(|filter| filter != table_name.as_ref())
        {
            continue;
        }
        records.push(ttl_record(table_name, config)?);
        if records.len() >= request.limit as usize {
            break;
        }
    }
    Ok(LogicalExportPage {
        domain: LogicalBackfillDomain::TtlRecords,
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
    provider: &PostgresStorageProvider,
    records: Vec<LogicalBackfillRecord>,
) -> StorageResult<()> {
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
                    provider,
                    &table_name,
                    &item_json,
                    &indexers,
                    item_stream_version,
                )
                .await?;
            }
            LogicalBackfillRecord::Tombstone(tombstone) => {
                import_tombstone(provider, tombstone).await?;
            }
            LogicalBackfillRecord::DomainRecord {
                domain,
                payload_json,
                ..
            } => match domain {
                LogicalBackfillDomain::TableMetadata => {
                    import_table_metadata_record(provider, &payload_json).await?;
                }
                LogicalBackfillDomain::DurableRevisions => {
                    import_durable_revision_record(provider, &payload_json).await?;
                }
                LogicalBackfillDomain::TtlRecords => {
                    import_ttl_record(provider, &payload_json).await?;
                }
                LogicalBackfillDomain::StreamRecords => {
                    import_stream_record(provider, &payload_json).await?;
                }
                LogicalBackfillDomain::GsiRecords => {
                    import_gsi_record(provider, &payload_json).await?;
                }
                LogicalBackfillDomain::ItemRecords
                | LogicalBackfillDomain::Tombstones
                | LogicalBackfillDomain::StorageControlPlane
                | LogicalBackfillDomain::BackgroundJobs
                | LogicalBackfillDomain::SyncControlPlane => {
                    return Err(StorageError::validation(format!(
                        "postgres logical import received unexpected domain record for {domain:?}"
                    )));
                }
            },
            LogicalBackfillRecord::StreamRecord { .. } => {
                return Err(StorageError::validation(
                    "postgres logical import currently imports stream rows through domain records",
                ));
            }
        }
    }
    Ok(())
}

async fn import_present_item(
    provider: &PostgresStorageProvider,
    table_name: &str,
    item_json: &str,
    indexers: &[String],
    item_stream_version: ItemStreamVersion,
) -> StorageResult<()> {
    provider
        .retry_postgres_conflicts("import_logical_present_item", || async move {
            let table_name = TableName::new(&table_name);
            let mut client = provider
                .pool
                .get()
                .await
                .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
            let transaction = client.transaction().await.map_err(|err| {
                PostgresStorageProvider::map_postgres_write_error(
                    "start logical present import transaction",
                    err,
                )
            })?;
            let table_info = provider.get_table_info_cached_arc(&table_name).await?;
            let item = serde_json::from_str::<HashMap<String, AttributeValue>>(item_json)?;
            let split = split_item_into_key_and_attributes_sync(item, table_info.as_ref())?;
            let old_item = provider
                .get_item_with_indexers_with_client(
                    &transaction,
                    &table_name,
                    &split.key_attributes,
                    table_info.as_ref(),
                )
                .await?;
            let old_item_map = old_item
                .as_ref()
                .map(|item| item.item.to_attribute_map())
                .transpose()?;
            let current_version =
                current_item_stream_version(&transaction, &table_name, &split.key_attributes)
                    .await?;
            let decision = plan_logical_import_apply(LogicalImportApplyCase::new(
                current_version,
                item_stream_version,
                LogicalImportRecordKind::PresentItem,
            ));
            if !matches!(decision, LogicalImportApplyDecision::ApplyPresentItem) {
                transaction.commit().await.map_err(|err| {
                    PostgresStorageProvider::map_postgres_write_error(
                        "commit stale logical present import",
                        err,
                    )
                })?;
                return Ok(());
            }
            upsert_main_row(
                provider,
                &transaction,
                table_info.as_ref(),
                &split.key_attributes,
                &split.all_attributes,
                &split.non_key_attributes,
                indexers,
            )
            .await?;
            set_item_revision(
                provider,
                &transaction,
                &table_name,
                &split.key_attributes,
                item_stream_version,
            )
            .await?;
            if provider.immediate_gsi_consistency {
                provider
                    .apply_gsi_entries_for_item_change_with_client(
                        &transaction,
                        &table_name,
                        table_info.as_ref(),
                        old_item_map.as_ref(),
                        Some(&split.all_attributes),
                        indexers,
                    )
                    .await?;
            }
            provider
                .sync_ttl_index_entries_with_client(
                    &transaction,
                    table_info.as_ref(),
                    old_item_map.as_ref(),
                    Some(&split.all_attributes),
                )
                .await?;
            provider
                .write_stream_entries_for_item_with_client(
                    &transaction,
                    table_info.as_ref(),
                    &split.all_attributes,
                    PostgresWriteStreamEntriesInput {
                        old_item: old_item_map.as_ref(),
                        indexers,
                        old_indexers: old_item.as_ref().map(|item| item.indexers.as_slice()),
                        is_deleted: false,
                        item_stream_version,
                        replication: None,
                    },
                )
                .await?;
            transaction.commit().await.map_err(|err| {
                PostgresStorageProvider::map_postgres_write_error(
                    "commit logical present import",
                    err,
                )
            })?;
            Ok(())
        })
        .await
}

async fn import_tombstone(
    provider: &PostgresStorageProvider,
    tombstone: LogicalBackfillTombstone,
) -> StorageResult<()> {
    provider
        .retry_postgres_conflicts("import_logical_tombstone", || {
            let tombstone = tombstone.clone();
            async move {
                let table_name = TableName::new(&tombstone.table_name);
                let mut client = provider
                    .pool
                    .get()
                    .await
                    .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
                let transaction = client.transaction().await.map_err(|err| {
                    PostgresStorageProvider::map_postgres_write_error(
                        "start logical tombstone import transaction",
                        err,
                    )
                })?;
                let table_info = provider.get_table_info_cached_arc(&table_name).await?;
                let key = serde_json::from_str::<KeyAttributes>(&tombstone.key_json)?;
                let old_item = provider
                    .get_item_with_indexers_with_client(
                        &transaction,
                        &table_name,
                        &key,
                        table_info.as_ref(),
                    )
                    .await?;
                let old_item_map = old_item
                    .as_ref()
                    .map(|item| item.item.to_attribute_map())
                    .transpose()?;
                let current_version =
                    current_item_stream_version(&transaction, &table_name, &key).await?;
                let decision = plan_logical_import_apply(LogicalImportApplyCase::new(
                    current_version,
                    tombstone.item_stream_version,
                    LogicalImportRecordKind::Tombstone,
                ));
                if !matches!(decision, LogicalImportApplyDecision::ApplyTombstone) {
                    transaction.commit().await.map_err(|err| {
                        PostgresStorageProvider::map_postgres_write_error(
                            "commit stale logical tombstone import",
                            err,
                        )
                    })?;
                    return Ok(());
                }
                delete_main_row(provider, &transaction, table_info.as_ref(), &key).await?;
                set_item_revision(
                    provider,
                    &transaction,
                    &table_name,
                    &key,
                    tombstone.item_stream_version,
                )
                .await?;
                if provider.immediate_gsi_consistency {
                    provider
                        .apply_gsi_entries_for_item_change_with_client(
                            &transaction,
                            &table_name,
                            table_info.as_ref(),
                            old_item_map.as_ref(),
                            None,
                            &[],
                        )
                        .await?;
                }
                provider
                    .sync_ttl_index_entries_with_client(
                        &transaction,
                        table_info.as_ref(),
                        old_item_map.as_ref(),
                        None,
                    )
                    .await?;
                provider
                    .write_stream_entries_for_item_with_client(
                        &transaction,
                        table_info.as_ref(),
                        &key.to_attribute_map(),
                        PostgresWriteStreamEntriesInput {
                            old_item: old_item_map.as_ref(),
                            indexers: &[],
                            old_indexers: old_item.as_ref().map(|item| item.indexers.as_slice()),
                            is_deleted: true,
                            item_stream_version: tombstone.item_stream_version,
                            replication: None,
                        },
                    )
                    .await?;
                transaction.commit().await.map_err(|err| {
                    PostgresStorageProvider::map_postgres_write_error(
                        "commit logical tombstone import",
                        err,
                    )
                })?;
                Ok(())
            }
        })
        .await
}

async fn current_item_stream_version<C>(
    client: &C,
    table_name: &TableName,
    key: &KeyAttributes,
) -> StorageResult<Option<ItemStreamVersion>>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let revision =
        PostgresStorageProvider::get_item_revision_with_client(client, table_name, key).await?;
    if revision == 0 {
        Ok(None)
    } else {
        Ok(Some(ItemStreamVersion::try_from(revision)?))
    }
}
