use storage_types::{AttributeValue, StorageError, StorageResult};

use super::{
    SQLiteStorageProvider,
    logical_backfill_import::{ImportPresentItemInput, OldItemSource, SyncImportContext},
};
use crate::{
    error_handler::map_sqlite_error,
    transaction_manager::with_transaction,
    utils::{SqliteConn, call_sqlite},
};

pub(crate) async fn apply_resolved_sync_mutations(
    provider: &SQLiteStorageProvider,
    metadata: storage_sync::SyncCommitMetadata,
    batch: storage_sync::ResolvedSyncMutationBatch,
) -> StorageResult<Vec<storage_sync::SyncMutationResponse>> {
    let immediate_gsi_consistency = provider.immediate_gsi_consistency;
    with_transaction(&provider.connection, move |sqlite| {
        let mut responses = Vec::with_capacity(batch.mutations.len());
        let mut context = SyncImportContext::default();
        ensure_sync_apply_markers_table(sqlite)?;
        let term = log_component_i64(metadata.log_id.term, "term")?;
        let index = log_component_i64(metadata.log_id.index, "index")?;
        for mutation in batch.mutations {
            let response = match mutation {
                storage_sync::ResolvedSyncMutation::Put(mutation) => {
                    let mutation_id = mutation.mutation_id.as_str();
                    if sync_apply_marker_exists(sqlite, mutation_id)? {
                        responses.push(mutation.response.clone());
                        continue;
                    }
                    let item = serde_json::from_str::<
                        std::collections::HashMap<String, AttributeValue>,
                    >(&mutation.item_json)?;
                    SQLiteStorageProvider::import_present_item_with_context(
                        &mutation.table_name,
                        ImportPresentItemInput {
                            item,
                            indexers: &mutation.indexers,
                            old_item_source: OldItemSource::Resolved {
                                item_json: mutation.old_item_json.as_deref(),
                                indexers: mutation.old_indexers.as_deref(),
                            },
                            item_stream_version: mutation.target_item_stream_version,
                            immediate_gsi_consistency,
                        },
                        &mut context,
                        sqlite,
                    )?;
                    insert_sync_mutation_apply_marker(sqlite, mutation_id, term, index, &metadata)?;
                    mutation.response
                }
                storage_sync::ResolvedSyncMutation::Delete(mutation) => {
                    let mutation_id = mutation.mutation_id.as_str();
                    if sync_apply_marker_exists(sqlite, mutation_id)? {
                        responses.push(mutation.response.clone());
                        continue;
                    }
                    SQLiteStorageProvider::import_tombstone_with_context(
                        storage_backfill::LogicalBackfillTombstone {
                            table_name: mutation.table_name.to_string(),
                            key_json: mutation.key_json,
                            item_stream_version: mutation.target_item_stream_version,
                        },
                        OldItemSource::Resolved {
                            item_json: mutation.old_item_json.as_deref(),
                            indexers: mutation.old_indexers.as_deref(),
                        },
                        immediate_gsi_consistency,
                        &mut context,
                        sqlite,
                    )?;
                    insert_sync_mutation_apply_marker(sqlite, mutation_id, term, index, &metadata)?;
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
        insert_last_applied_marker(sqlite, term, index, &metadata)?;
        Ok(responses)
    })
    .await
}

pub(crate) async fn last_resolved_sync_log_id(
    provider: &SQLiteStorageProvider,
) -> StorageResult<Option<storage_sync::SyncLogId>> {
    call_sqlite(&provider.connection, move |sqlite| {
        ensure_sync_apply_markers_table(&SqliteConn::Connection(sqlite))?;
        let result = sqlite.query_row(
            "SELECT term, log_index FROM sys_sync_apply_markers WHERE marker_key = ?1",
            ["__last_applied__"],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        );
        match result {
            Ok((term, index)) => Ok(Some(storage_sync::SyncLogId::new(
                u64::try_from(term)
                    .map_err(|_| StorageError::validation("sync apply term cannot be negative"))?,
                u64::try_from(index)
                    .map_err(|_| StorageError::validation("sync apply index cannot be negative"))?,
            ))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(map_sqlite_error(error)),
        }
    })
    .await
}

pub(crate) async fn persist_resolved_sync_log_entry(
    provider: &SQLiteStorageProvider,
    metadata: &storage_sync::SyncCommitMetadata,
    batch: &storage_sync::ResolvedSyncMutationBatch,
) -> StorageResult<()> {
    let metadata = metadata.clone();
    let batch = batch.clone();
    call_sqlite(&provider.connection, move |sqlite| {
        ensure_sync_log_entries_table(&SqliteConn::Connection(sqlite))?;
        sqlite
            .execute(
                r"INSERT INTO sys_sync_log_entries (
                    term, log_index, metadata_json, batch_json
                )
                  VALUES (?1, ?2, ?3, ?4)
                  ON CONFLICT(term, log_index)
                  DO UPDATE SET
                    metadata_json = excluded.metadata_json,
                    batch_json = excluded.batch_json",
                (
                    log_component_i64(metadata.log_id.term, "term")?,
                    log_component_i64(metadata.log_id.index, "index")?,
                    serde_json::to_string(&metadata)?,
                    serde_json::to_string(&batch)?,
                ),
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    })
    .await
}

pub(crate) async fn get_resolved_sync_log_entry(
    provider: &SQLiteStorageProvider,
    log_id: storage_sync::SyncLogId,
) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
    call_sqlite(&provider.connection, move |sqlite| {
        ensure_sync_log_entries_table(&SqliteConn::Connection(sqlite))?;
        let result = sqlite.query_row(
            "SELECT metadata_json, batch_json FROM sys_sync_log_entries WHERE term = ?1 AND \
             log_index = ?2",
            (
                log_component_i64(log_id.term, "term")?,
                log_component_i64(log_id.index, "index")?,
            ),
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        );
        let (metadata_json, batch_json) = match result {
            Ok(row) => row,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(error) => return Err(map_sqlite_error(error)),
        };
        let metadata = serde_json::from_str::<storage_sync::SyncCommitMetadata>(&metadata_json)?;
        let batch = serde_json::from_str::<storage_sync::ResolvedSyncMutationBatch>(&batch_json)?;
        Ok(Some(storage_sync::ResolvedSyncLogEntry::new(
            metadata, batch,
        )))
    })
    .await
}

pub(crate) async fn resolved_sync_log_entries_after(
    provider: &SQLiteStorageProvider,
    log_id: Option<storage_sync::SyncLogId>,
    limit: usize,
) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
    let limit = i64::try_from(limit)
        .map_err(|_| StorageError::validation("sync log scan limit does not fit sqlite integer"))?;
    call_sqlite(&provider.connection, move |sqlite| {
        ensure_sync_log_entries_table(&SqliteConn::Connection(sqlite))?;
        let (after_term, after_index) = match log_id {
            Some(log_id) => (
                log_component_i64(log_id.term, "term")?,
                log_component_i64(log_id.index, "index")?,
            ),
            None => (0, 0),
        };
        let mut stmt = sqlite
            .prepare(
                r"SELECT metadata_json, batch_json
                  FROM sys_sync_log_entries
                  WHERE term > ?1 OR (term = ?1 AND log_index > ?2)
                  ORDER BY term ASC, log_index ASC
                  LIMIT ?3",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map((after_term, after_index, limit), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(map_sqlite_error)?;
        let mut entries = Vec::new();
        for row in rows {
            let (metadata_json, batch_json) = row.map_err(map_sqlite_error)?;
            let metadata =
                serde_json::from_str::<storage_sync::SyncCommitMetadata>(&metadata_json)?;
            let batch =
                serde_json::from_str::<storage_sync::ResolvedSyncMutationBatch>(&batch_json)?;
            entries.push(storage_sync::ResolvedSyncLogEntry::new(metadata, batch));
        }
        Ok(entries)
    })
    .await
}

fn ensure_sync_apply_markers_table(sqlite: &SqliteConn<'_>) -> StorageResult<()> {
    sqlite
        .execute(
            r"CREATE TABLE IF NOT EXISTS sys_sync_apply_markers (
                marker_key TEXT PRIMARY KEY,
                term INTEGER NOT NULL,
                log_index INTEGER NOT NULL,
                applied_at INTEGER NOT NULL,
                leader_node_id TEXT NOT NULL
            )",
            [],
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

fn ensure_sync_log_entries_table(sqlite: &SqliteConn<'_>) -> StorageResult<()> {
    sqlite
        .execute(
            r"CREATE TABLE IF NOT EXISTS sys_sync_log_entries (
                term INTEGER NOT NULL,
                log_index INTEGER NOT NULL,
                metadata_json TEXT NOT NULL,
                batch_json TEXT NOT NULL,
                PRIMARY KEY (term, log_index)
            )",
            [],
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

fn sync_apply_marker_exists(sqlite: &SqliteConn<'_>, mutation_id: &str) -> StorageResult<bool> {
    let count: i64 = sqlite
        .query_row(
            "SELECT COUNT(*) FROM sys_sync_apply_markers WHERE marker_key = 'mutation#' || ?1",
            [mutation_id],
            |row| row.get(0),
        )
        .map_err(map_sqlite_error)?;
    Ok(count > 0)
}

fn insert_sync_mutation_apply_marker(
    sqlite: &SqliteConn<'_>,
    mutation_id: &str,
    term: i64,
    index: i64,
    metadata: &storage_sync::SyncCommitMetadata,
) -> StorageResult<()> {
    sqlite
        .execute(
            r"INSERT INTO sys_sync_apply_markers (
                marker_key, term, log_index, applied_at, leader_node_id
            )
              VALUES ('mutation#' || ?1, ?2, ?3, ?4, ?5)
              ON CONFLICT(marker_key)
              DO UPDATE SET
                term = excluded.term,
                log_index = excluded.log_index,
                applied_at = excluded.applied_at,
                leader_node_id = excluded.leader_node_id",
            (
                mutation_id,
                term,
                index,
                metadata.committed_at.timestamp_millis(),
                metadata.leader_node_id.as_str(),
            ),
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

fn insert_last_applied_marker(
    sqlite: &SqliteConn<'_>,
    term: i64,
    index: i64,
    metadata: &storage_sync::SyncCommitMetadata,
) -> StorageResult<()> {
    sqlite
        .execute(
            r"INSERT INTO sys_sync_apply_markers (
                marker_key, term, log_index, applied_at, leader_node_id
            )
              VALUES ('__last_applied__', ?1, ?2, ?3, ?4)
              ON CONFLICT(marker_key)
              DO UPDATE SET
                term = excluded.term,
                log_index = excluded.log_index,
                applied_at = excluded.applied_at,
                leader_node_id = excluded.leader_node_id",
            (
                term,
                index,
                metadata.committed_at.timestamp_millis(),
                metadata.leader_node_id.as_str(),
            ),
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

fn log_component_i64(value: u64, label: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| {
        StorageError::validation(format!(
            "sync apply log {label} does not fit sqlite integer"
        ))
    })
}
