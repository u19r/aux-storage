use std::collections::HashMap;

use storage_provider::split_item_into_key_and_attributes_sync;
use storage_types::{AttributeValue, KeyAttributes, StorageError, StorageResult};
use turso::Value as TursoValue;

use super::{
    TursoStorageProvider,
    logical_backfill_values::{log_component_i64, log_component_u64, row_text},
    provider::{TursoWriteStreamEntriesInput, build_key_where_clause},
    sql_statements,
};

pub(crate) async fn apply_resolved_sync_mutations(
    provider: &TursoStorageProvider,
    metadata: storage_sync::SyncCommitMetadata,
    batch: storage_sync::ResolvedSyncMutationBatch,
) -> StorageResult<Vec<storage_sync::SyncMutationResponse>> {
    let this = provider.clone();
    this.with_exclusive_transaction(true, |conn| {
        let this = this.clone();
        let metadata = metadata.clone();
        let batch = batch.clone();
        Box::pin(async move {
            ensure_sync_apply_markers_table(&this, conn).await?;
            let mut responses = Vec::with_capacity(batch.mutations.len());

            for mutation in batch.mutations {
                let response = match mutation {
                    storage_sync::ResolvedSyncMutation::Put(mutation) => {
                        let mutation_id = mutation.mutation_id.as_str();
                        if sync_apply_marker_exists(&this, conn, mutation_id).await? {
                            responses.push(mutation.response.clone());
                            continue;
                        }
                        let table_info = this.load_table_info_cached(&mutation.table_name).await?;
                        let item = serde_json::from_str::<HashMap<String, AttributeValue>>(
                            &mutation.item_json,
                        )?;
                        let old_item = old_item_map(mutation.old_item_json.as_deref())?;
                        let split_item =
                            split_item_into_key_and_attributes_sync(item, &table_info)?;
                        this.upsert_main_row(
                            conn,
                            &table_info,
                            &split_item.key_attributes,
                            &split_item.all_attributes,
                            &split_item.non_key_attributes,
                            Some(&mutation.indexers),
                        )
                        .await?;
                        set_item_revision(
                            &this,
                            conn,
                            &mutation.table_name,
                            &split_item.key_attributes,
                            mutation.target_item_stream_version,
                        )
                        .await?;
                        this.write_stream_entries_for_item_change(
                            conn,
                            &table_info,
                            &split_item.all_attributes,
                            TursoWriteStreamEntriesInput {
                                old_item: old_item.as_ref(),
                                indexers: &mutation.indexers,
                                old_indexers: mutation.old_indexers.as_deref(),
                                is_deleted: false,
                                item_stream_version: mutation.target_item_stream_version,
                                replication: None,
                            },
                        )
                        .await?;
                        if this.immediate_gsi_consistency {
                            this.apply_gsi_rows_for_item_change(
                                conn,
                                &table_info,
                                old_item.as_ref(),
                                Some(&split_item.all_attributes),
                                &mutation.indexers,
                            )
                            .await?;
                        }
                        insert_sync_apply_marker(
                            &this,
                            conn,
                            mutation_id,
                            metadata.log_id,
                            &metadata,
                        )
                        .await?;
                        mutation.response
                    }
                    storage_sync::ResolvedSyncMutation::Delete(mutation) => {
                        let mutation_id = mutation.mutation_id.as_str();
                        if sync_apply_marker_exists(&this, conn, mutation_id).await? {
                            responses.push(mutation.response.clone());
                            continue;
                        }
                        let table_info = this.load_table_info_cached(&mutation.table_name).await?;
                        let key = serde_json::from_str::<HashMap<String, AttributeValue>>(
                            &mutation.key_json,
                        )?;
                        let key_attributes = KeyAttributes::from(key);
                        let old_item = old_item_map(mutation.old_item_json.as_deref())?;
                        delete_main_row(&this, conn, &table_info, &key_attributes).await?;
                        set_item_revision(
                            &this,
                            conn,
                            &mutation.table_name,
                            &key_attributes,
                            mutation.target_item_stream_version,
                        )
                        .await?;
                        this.write_stream_entries_for_item_change(
                            conn,
                            &table_info,
                            &key_attributes.to_attribute_map(),
                            TursoWriteStreamEntriesInput {
                                old_item: old_item.as_ref(),
                                indexers: &[],
                                old_indexers: mutation.old_indexers.as_deref(),
                                is_deleted: true,
                                item_stream_version: mutation.target_item_stream_version,
                                replication: None,
                            },
                        )
                        .await?;
                        if this.immediate_gsi_consistency {
                            this.apply_gsi_rows_for_item_change(
                                conn,
                                &table_info,
                                old_item.as_ref(),
                                None,
                                &[],
                            )
                            .await?;
                        }
                        insert_sync_apply_marker(
                            &this,
                            conn,
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

            insert_sync_apply_marker(&this, conn, "__last_applied__", metadata.log_id, &metadata)
                .await?;
            Ok(responses)
        })
    })
    .await
}

pub(crate) async fn last_resolved_sync_log_id(
    provider: &TursoStorageProvider,
) -> StorageResult<Option<storage_sync::SyncLogId>> {
    let conn = provider.connect().await?;
    ensure_sync_apply_markers_table(provider, &conn).await?;
    let rows = provider
        .query_rows(
            &conn,
            "SELECT term, log_index FROM sys_sync_apply_markers WHERE marker_key = ?1",
            vec![TursoValue::Text("__last_applied__".to_string())],
        )
        .await?;
    rows.first()
        .map(|row| {
            Ok(storage_sync::SyncLogId::new(
                log_component_u64(row.get("term"), "term")?,
                log_component_u64(row.get("log_index"), "index")?,
            ))
        })
        .transpose()
}

pub(crate) async fn persist_resolved_sync_log_entry(
    provider: &TursoStorageProvider,
    metadata: &storage_sync::SyncCommitMetadata,
    batch: &storage_sync::ResolvedSyncMutationBatch,
) -> StorageResult<()> {
    let conn = provider.connect().await?;
    ensure_sync_log_entries_table(provider, &conn).await?;
    let _ = provider
        .execute(
            &conn,
            r"INSERT INTO sys_sync_log_entries (
                term, log_index, metadata_json, batch_json
            )
              VALUES (?1, ?2, ?3, ?4)
              ON CONFLICT(term, log_index)
              DO UPDATE SET
                metadata_json = excluded.metadata_json,
                batch_json = excluded.batch_json",
            vec![
                TursoValue::Integer(log_component_i64(metadata.log_id.term, "term")?),
                TursoValue::Integer(log_component_i64(metadata.log_id.index, "index")?),
                TursoValue::Text(serde_json::to_string(metadata)?),
                TursoValue::Text(serde_json::to_string(batch)?),
            ],
        )
        .await?;
    Ok(())
}

pub(crate) async fn get_resolved_sync_log_entry(
    provider: &TursoStorageProvider,
    log_id: storage_sync::SyncLogId,
) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
    let conn = provider.connect().await?;
    ensure_sync_log_entries_table(provider, &conn).await?;
    let rows = provider
        .query_rows(
            &conn,
            "SELECT metadata_json, batch_json FROM sys_sync_log_entries WHERE term = ?1 AND \
             log_index = ?2",
            vec![
                TursoValue::Integer(log_component_i64(log_id.term, "term")?),
                TursoValue::Integer(log_component_i64(log_id.index, "index")?),
            ],
        )
        .await?;
    rows.first()
        .map(resolved_sync_log_entry_from_row)
        .transpose()
}

pub(crate) async fn resolved_sync_log_entries_after(
    provider: &TursoStorageProvider,
    log_id: Option<storage_sync::SyncLogId>,
    limit: usize,
) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
    let conn = provider.connect().await?;
    ensure_sync_log_entries_table(provider, &conn).await?;
    let limit = i64::try_from(limit)
        .map_err(|_| StorageError::validation("sync log scan limit does not fit turso integer"))?;
    let (after_term, after_index) = match log_id {
        Some(log_id) => (
            log_component_i64(log_id.term, "term")?,
            log_component_i64(log_id.index, "index")?,
        ),
        None => (0, 0),
    };
    let rows = provider
        .query_rows(
            &conn,
            r"SELECT metadata_json, batch_json
              FROM sys_sync_log_entries
              WHERE term > ?1 OR (term = ?1 AND log_index > ?2)
              ORDER BY term ASC, log_index ASC
              LIMIT ?3",
            vec![
                TursoValue::Integer(after_term),
                TursoValue::Integer(after_index),
                TursoValue::Integer(limit),
            ],
        )
        .await?;
    rows.iter().map(resolved_sync_log_entry_from_row).collect()
}

async fn ensure_sync_apply_markers_table<C>(
    provider: &TursoStorageProvider,
    conn: &C,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let _ = provider
        .execute(
            conn,
            r"CREATE TABLE IF NOT EXISTS sys_sync_apply_markers (
                marker_key TEXT PRIMARY KEY,
                term INTEGER NOT NULL,
                log_index INTEGER NOT NULL,
                applied_at INTEGER NOT NULL,
                leader_node_id TEXT NOT NULL
            )",
            Vec::new(),
        )
        .await?;
    Ok(())
}

async fn ensure_sync_log_entries_table<C>(
    provider: &TursoStorageProvider,
    conn: &C,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let _ = provider
        .execute(
            conn,
            r"CREATE TABLE IF NOT EXISTS sys_sync_log_entries (
                term INTEGER NOT NULL,
                log_index INTEGER NOT NULL,
                metadata_json TEXT NOT NULL,
                batch_json TEXT NOT NULL,
                PRIMARY KEY (term, log_index)
            )",
            Vec::new(),
        )
        .await?;
    Ok(())
}

async fn sync_apply_marker_exists<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    mutation_id: &str,
) -> StorageResult<bool>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let marker_key = sync_mutation_marker_key(mutation_id);
    let rows = provider
        .query_rows(
            conn,
            "SELECT COUNT(*) AS count FROM sys_sync_apply_markers WHERE marker_key = ?1",
            vec![TursoValue::Text(marker_key)],
        )
        .await?;
    let count = rows
        .first()
        .and_then(|row| row.get("count"))
        .map(super::provider::value_to_i64)
        .transpose()?
        .unwrap_or_default();
    Ok(count > 0)
}

async fn insert_sync_apply_marker<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    marker: &str,
    log_id: storage_sync::SyncLogId,
    metadata: &storage_sync::SyncCommitMetadata,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let marker_key = if marker == "__last_applied__" {
        marker.to_string()
    } else {
        sync_mutation_marker_key(marker)
    };
    let _ = provider
        .execute(
            conn,
            r"INSERT INTO sys_sync_apply_markers (
                marker_key, term, log_index, applied_at, leader_node_id
            )
              VALUES (?1, ?2, ?3, ?4, ?5)
              ON CONFLICT(marker_key)
              DO UPDATE SET
                term = excluded.term,
                log_index = excluded.log_index,
                applied_at = excluded.applied_at,
                leader_node_id = excluded.leader_node_id",
            vec![
                TursoValue::Text(marker_key),
                TursoValue::Integer(log_component_i64(log_id.term, "term")?),
                TursoValue::Integer(log_component_i64(log_id.index, "index")?),
                TursoValue::Integer(metadata.committed_at.timestamp_millis()),
                TursoValue::Text(metadata.leader_node_id.clone()),
            ],
        )
        .await?;
    Ok(())
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

fn old_item_map(value: Option<&str>) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
    value
        .map(serde_json::from_str::<HashMap<String, AttributeValue>>)
        .transpose()
        .map_err(Into::into)
}

fn sync_mutation_marker_key(mutation_id: &str) -> String {
    format!("mutation#{mutation_id}")
}

fn resolved_sync_log_entry_from_row(
    row: &HashMap<String, TursoValue>,
) -> StorageResult<storage_sync::ResolvedSyncLogEntry> {
    let metadata_json = row_text(row, "metadata_json")?;
    let batch_json = row_text(row, "batch_json")?;
    Ok(storage_sync::ResolvedSyncLogEntry::new(
        serde_json::from_str(metadata_json)?,
        serde_json::from_str(batch_json)?,
    ))
}
