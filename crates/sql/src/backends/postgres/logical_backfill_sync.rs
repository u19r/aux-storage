use std::collections::HashMap;

use storage_provider::split_item_into_key_and_attributes_sync;
use storage_types::{AttributeValue, KeyAttributes, StorageError, StorageResult};

use super::{
    PostgresStorageProvider,
    logical_backfill_sync_store::{
        delete_main_row, ensure_sync_apply_markers_table, ensure_sync_log_entries_table,
        insert_sync_apply_marker, old_item_map, resolved_sync_log_entry_from_row,
        set_item_revision, sync_apply_marker_exists, upsert_main_row,
    },
    logical_backfill_values::{log_component_i64, log_component_u64},
    stream_helpers::PostgresWriteStreamEntriesInput,
};

pub(crate) async fn apply_resolved_sync_mutations(
    provider: &PostgresStorageProvider,
    metadata: storage_sync::SyncCommitMetadata,
    batch: storage_sync::ResolvedSyncMutationBatch,
) -> StorageResult<Vec<storage_sync::SyncMutationResponse>> {
    provider
        .retry_postgres_conflicts("apply_resolved_sync_mutations", || {
            let metadata = metadata.clone();
            let batch = batch.clone();
            async move {
                let mut client = provider
                    .pool
                    .get()
                    .await
                    .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
                let transaction = client.transaction().await.map_err(|err| {
                    PostgresStorageProvider::map_postgres_write_error(
                        "start resolved sync apply transaction",
                        err,
                    )
                })?;
                ensure_sync_apply_markers_table(provider, &transaction).await?;
                let mut responses = Vec::with_capacity(batch.mutations.len());

                for mutation in batch.mutations {
                    let response = match mutation {
                        storage_sync::ResolvedSyncMutation::Put(mutation) => {
                            let mutation_id = mutation.mutation_id.as_str();
                            if sync_apply_marker_exists(provider, &transaction, mutation_id).await?
                            {
                                responses.push(mutation.response.clone());
                                continue;
                            }
                            let table_info = provider
                                .get_table_info_cached_arc(&mutation.table_name)
                                .await?;
                            let item = serde_json::from_str::<HashMap<String, AttributeValue>>(
                                &mutation.item_json,
                            )?;
                            let old_item = old_item_map(mutation.old_item_json.as_deref())?;
                            let split_item =
                                split_item_into_key_and_attributes_sync(item, table_info.as_ref())?;
                            upsert_main_row(
                                provider,
                                &transaction,
                                table_info.as_ref(),
                                &split_item.key_attributes,
                                &split_item.all_attributes,
                                &split_item.non_key_attributes,
                                &mutation.indexers,
                            )
                            .await?;
                            set_item_revision(
                                provider,
                                &transaction,
                                &mutation.table_name,
                                &split_item.key_attributes,
                                mutation.target_item_stream_version,
                            )
                            .await?;
                            if provider.immediate_gsi_consistency {
                                provider
                                    .apply_gsi_entries_for_item_change_with_client(
                                        &transaction,
                                        &mutation.table_name,
                                        table_info.as_ref(),
                                        old_item.as_ref(),
                                        Some(&split_item.all_attributes),
                                        &mutation.indexers,
                                    )
                                    .await?;
                            }
                            provider
                                .sync_ttl_index_entries_with_client(
                                    &transaction,
                                    table_info.as_ref(),
                                    old_item.as_ref(),
                                    Some(&split_item.all_attributes),
                                )
                                .await?;
                            provider
                                .write_stream_entries_for_item_with_client(
                                    &transaction,
                                    table_info.as_ref(),
                                    &split_item.all_attributes,
                                    PostgresWriteStreamEntriesInput {
                                        old_item: old_item.as_ref(),
                                        indexers: &mutation.indexers,
                                        old_indexers: mutation.old_indexers.as_deref(),
                                        is_deleted: false,
                                        item_stream_version: mutation.target_item_stream_version,
                                        replication: None,
                                    },
                                )
                                .await?;
                            insert_sync_apply_marker(
                                provider,
                                &transaction,
                                mutation_id,
                                metadata.log_id,
                                &metadata,
                            )
                            .await?;
                            mutation.response
                        }
                        storage_sync::ResolvedSyncMutation::Delete(mutation) => {
                            let mutation_id = mutation.mutation_id.as_str();
                            if sync_apply_marker_exists(provider, &transaction, mutation_id).await?
                            {
                                responses.push(mutation.response.clone());
                                continue;
                            }
                            let table_info = provider
                                .get_table_info_cached_arc(&mutation.table_name)
                                .await?;
                            let key = serde_json::from_str::<HashMap<String, AttributeValue>>(
                                &mutation.key_json,
                            )?;
                            let key_attributes = KeyAttributes::from(key);
                            let old_item = old_item_map(mutation.old_item_json.as_deref())?;
                            delete_main_row(
                                provider,
                                &transaction,
                                table_info.as_ref(),
                                &key_attributes,
                            )
                            .await?;
                            set_item_revision(
                                provider,
                                &transaction,
                                &mutation.table_name,
                                &key_attributes,
                                mutation.target_item_stream_version,
                            )
                            .await?;
                            if provider.immediate_gsi_consistency {
                                provider
                                    .apply_gsi_entries_for_item_change_with_client(
                                        &transaction,
                                        &mutation.table_name,
                                        table_info.as_ref(),
                                        old_item.as_ref(),
                                        None,
                                        &[],
                                    )
                                    .await?;
                            }
                            provider
                                .sync_ttl_index_entries_with_client(
                                    &transaction,
                                    table_info.as_ref(),
                                    old_item.as_ref(),
                                    None,
                                )
                                .await?;
                            provider
                                .write_stream_entries_for_item_with_client(
                                    &transaction,
                                    table_info.as_ref(),
                                    &key_attributes.to_attribute_map(),
                                    PostgresWriteStreamEntriesInput {
                                        old_item: old_item.as_ref(),
                                        indexers: &[],
                                        old_indexers: mutation.old_indexers.as_deref(),
                                        is_deleted: true,
                                        item_stream_version: mutation.target_item_stream_version,
                                        replication: None,
                                    },
                                )
                                .await?;
                            insert_sync_apply_marker(
                                provider,
                                &transaction,
                                mutation_id,
                                metadata.log_id,
                                &metadata,
                            )
                            .await?;
                            mutation.response
                        }
                        storage_sync::ResolvedSyncMutation::CreateTable(_)
                        | storage_sync::ResolvedSyncMutation::UpdateTable(_)
                        | storage_sync::ResolvedSyncMutation::DeleteTable(_)
                        | storage_sync::ResolvedSyncMutation::UpdateTimeToLive(_) => {
                            return Err(StorageError::internal(
                                "lifecycle sync mutations must be applied by DatabaseManager",
                            ));
                        }
                    };
                    responses.push(response);
                }
                insert_sync_apply_marker(
                    provider,
                    &transaction,
                    "__last_applied__",
                    metadata.log_id,
                    &metadata,
                )
                .await?;
                transaction.commit().await.map_err(|err| {
                    PostgresStorageProvider::map_postgres_write_error(
                        "commit resolved sync apply transaction",
                        err,
                    )
                })?;
                Ok(responses)
            }
        })
        .await
}

pub(crate) async fn last_resolved_sync_log_id(
    provider: &PostgresStorageProvider,
) -> StorageResult<Option<storage_sync::SyncLogId>> {
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    ensure_sync_apply_markers_table(provider, &client).await?;
    let row = client
        .query_opt(
            "SELECT term, log_index FROM sys_sync_apply_markers WHERE marker_key = $1",
            &[&"__last_applied__"],
        )
        .await
        .map_err(|err| PostgresStorageProvider::map_postgres_error("read sync marker", err))?;
    row.map(|row| {
        Ok(storage_sync::SyncLogId::new(
            log_component_u64(row.try_get::<_, i64>(0), "term")?,
            log_component_u64(row.try_get::<_, i64>(1), "index")?,
        ))
    })
    .transpose()
}

pub(crate) async fn persist_resolved_sync_log_entry(
    provider: &PostgresStorageProvider,
    metadata: &storage_sync::SyncCommitMetadata,
    batch: &storage_sync::ResolvedSyncMutationBatch,
) -> StorageResult<()> {
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    ensure_sync_log_entries_table(provider, &client).await?;
    client
        .execute(
            r"INSERT INTO sys_sync_log_entries (
                term, log_index, metadata_json, batch_json
            )
              VALUES ($1, $2, $3, $4)
              ON CONFLICT(term, log_index)
              DO UPDATE SET
                metadata_json = excluded.metadata_json,
                batch_json = excluded.batch_json",
            &[
                &log_component_i64(metadata.log_id.term, "term")?,
                &log_component_i64(metadata.log_id.index, "index")?,
                &serde_json::to_string(metadata)?,
                &serde_json::to_string(batch)?,
            ],
        )
        .await
        .map_err(|err| PostgresStorageProvider::map_postgres_write_error("write sync log", err))?;
    Ok(())
}

pub(crate) async fn get_resolved_sync_log_entry(
    provider: &PostgresStorageProvider,
    log_id: storage_sync::SyncLogId,
) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    ensure_sync_log_entries_table(provider, &client).await?;
    let row = client
        .query_opt(
            "SELECT metadata_json, batch_json FROM sys_sync_log_entries WHERE term = $1 AND \
             log_index = $2",
            &[
                &log_component_i64(log_id.term, "term")?,
                &log_component_i64(log_id.index, "index")?,
            ],
        )
        .await
        .map_err(|err| PostgresStorageProvider::map_postgres_error("read sync log", err))?;
    row.as_ref()
        .map(resolved_sync_log_entry_from_row)
        .transpose()
}

pub(crate) async fn resolved_sync_log_entries_after(
    provider: &PostgresStorageProvider,
    log_id: Option<storage_sync::SyncLogId>,
    limit: usize,
) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    ensure_sync_log_entries_table(provider, &client).await?;
    let limit = i64::try_from(limit).map_err(|_| {
        StorageError::validation("sync log scan limit does not fit postgres integer")
    })?;
    let (after_term, after_index) = match log_id {
        Some(log_id) => (
            log_component_i64(log_id.term, "term")?,
            log_component_i64(log_id.index, "index")?,
        ),
        None => (0, 0),
    };
    let rows = client
        .query(
            r"SELECT metadata_json, batch_json
              FROM sys_sync_log_entries
              WHERE term > $1 OR (term = $1 AND log_index > $2)
              ORDER BY term ASC, log_index ASC
              LIMIT $3",
            &[&after_term, &after_index, &limit],
        )
        .await
        .map_err(|err| PostgresStorageProvider::map_postgres_error("scan sync log", err))?;
    rows.iter().map(resolved_sync_log_entry_from_row).collect()
}
