use std::collections::HashMap;

use storage_provider::{
    StreamDurationTrimPageRequest, StreamDurationTrimPageResult, StreamTrimBoundary,
    StreamTrimDueMarker, StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimState,
    StreamTrimStateWrite, plan_validated_item_stream_duration,
};
use storage_types::{
    ItemKey, KeyAttributes, StorageError, StorageResult, StoredTableInfo, StreamItemId, StreamName,
    StreamRetentionDuration, TableName, TimestampMillis,
};
use turso::Value as TursoValue;

use crate::backends::turso::provider::{
    TursoSqlConnection, TursoStorageProvider, row_required_blob, row_required_i64,
    row_required_text,
};

const ITEM_KEY_HASH_PREFIX: &str = "turso-key:";

pub(super) struct TursoStreamPointerIndexEntry<'a> {
    pub(super) table_name: &'a TableName,
    pub(super) item_stream_name: &'a str,
    pub(super) item_stream_version: storage_types::ItemStreamVersion,
    pub(super) table_stream_item_id: StreamItemId,
    pub(super) system_stream_item_id: StreamItemId,
    pub(super) created_at: TimestampMillis,
}

#[derive(Clone)]
struct TursoStreamTrimRow {
    item_id: String,
    version: storage_types::ItemStreamVersion,
    created_at: TimestampMillis,
}

fn item_stream_policy_version(
    requested_retention: StreamRetentionDuration,
    table_retention: StreamRetentionDuration,
) -> u64 {
    (u64::from(duration_policy_code(requested_retention)) << 32)
        | u64::from(duration_policy_code(
            StreamRetentionDuration::effective_item_retention(table_retention, requested_retention),
        ))
}

fn duration_policy_code(duration: StreamRetentionDuration) -> u32 {
    match duration {
        StreamRetentionDuration::Forever => u32::MAX,
        StreamRetentionDuration::FiniteHours(hours) => u32::from(hours),
    }
}

fn item_stream_key_hash(stream_name: &StreamName) -> String {
    let digest = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, stream_name.as_ref())
        .as_hyphenated()
        .to_string();
    let mut key_hash = String::with_capacity(ITEM_KEY_HASH_PREFIX.len() + digest.len());
    key_hash.push_str(ITEM_KEY_HASH_PREFIX);
    key_hash.push_str(&digest);
    key_hash
}

fn decode_stream_trim_state_blob(blob: &[u8]) -> StorageResult<StreamTrimState> {
    storage_types::storage_serde::from_bytes::<StreamTrimState>(blob).map_err(|error| {
        StorageError::internal(&format!("decode stream trim state failed: {error}"))
    })
}

fn stream_name_for_trim_scope(scope: &StreamTrimScope) -> String {
    match scope.kind {
        storage_provider::StreamTrimScopeKind::Table => {
            String::from(&StreamName::table_stream(&scope.table_name))
        }
        storage_provider::StreamTrimScopeKind::Item => scope.scope_id.clone(),
    }
}

fn stream_item_id_from_text(value: &str) -> StorageResult<StreamItemId> {
    value
        .parse()
        .map_err(|error| StorageError::internal(&format!("decode stream item id failed: {error}")))
}

fn turso_stream_trim_row(row: &HashMap<String, TursoValue>) -> StorageResult<TursoStreamTrimRow> {
    let item_id = row_required_text(row, "item_id")?;
    let item_id_value = stream_item_id_from_text(&item_id)?;
    Ok(TursoStreamTrimRow {
        item_id,
        version: storage_types::ItemStreamVersion::from(item_id_value),
        created_at: TimestampMillis::from_timestamp(row_required_i64(row, "created_at")?),
    })
}

impl TursoStorageProvider {
    pub(crate) async fn initialize_stream_duration_tables<C>(&self, conn: &C) -> StorageResult<()>
    where C: TursoSqlConnection + ?Sized {
        for sql in [
            r"CREATE TABLE IF NOT EXISTS sys_stream_trim_state (
                scope_id TEXT PRIMARY KEY,
                state_blob BLOB NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            r"CREATE TABLE IF NOT EXISTS sys_stream_trim_due_markers (
                due_bucket INTEGER NOT NULL,
                scope_id TEXT NOT NULL,
                policy_version INTEGER NOT NULL,
                marker_blob BLOB NOT NULL,
                PRIMARY KEY (due_bucket, scope_id, policy_version)
            )",
            r"CREATE INDEX IF NOT EXISTS idx_stream_trim_due_markers_scope
                ON sys_stream_trim_due_markers(scope_id)",
            r"CREATE TABLE IF NOT EXISTS sys_stream_pointer_index (
                table_name TEXT NOT NULL,
                item_stream_name TEXT NOT NULL,
                item_stream_version TEXT NOT NULL,
                table_stream_item_id TEXT NOT NULL,
                system_stream_item_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (
                    table_name, item_stream_name, item_stream_version, table_stream_item_id
                )
            )",
            r"CREATE INDEX IF NOT EXISTS idx_stream_pointer_index_item
                ON sys_stream_pointer_index(item_stream_name, item_stream_version)",
            r"CREATE INDEX IF NOT EXISTS idx_stream_pointer_index_table_pointer
                ON sys_stream_pointer_index(table_name, table_stream_item_id)",
        ] {
            let _ = self.execute(conn, sql, Vec::new()).await?;
        }
        Ok(())
    }

    pub(crate) async fn load_table_scope_id<C>(
        &self,
        conn: &C,
        table_name: &TableName,
    ) -> StorageResult<String>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let rows = self
            .query_rows(
                conn,
                "SELECT id FROM tables WHERE table_name = ?1",
                vec![TursoValue::Text(table_name.to_string())],
            )
            .await?;
        let Some(row) = rows.first() else {
            return Err(StorageError::table_not_found(table_name));
        };
        Ok(format!("turso-table:{}", row_required_text(row, "id")?))
    }

    pub(crate) async fn next_table_policy_version<C>(
        &self,
        conn: &C,
        table_scope_id: &str,
    ) -> StorageResult<u64>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let rows = self
            .query_rows(
                conn,
                "SELECT state_blob FROM sys_stream_trim_state WHERE scope_id = ?1",
                vec![TursoValue::Text(table_scope_id.to_string())],
            )
            .await?;
        let current = rows
            .first()
            .map(|row| {
                let blob = row_required_blob(row, "state_blob")?;
                decode_stream_trim_state_blob(&blob)
            })
            .transpose()?;
        Ok(current
            .and_then(|state| state.policy_version.checked_add(1))
            .unwrap_or(1))
    }

    pub(crate) async fn load_stream_trim_state_by_scope<C>(
        &self,
        conn: &C,
        scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let rows = self
            .query_rows(
                conn,
                "SELECT state_blob FROM sys_stream_trim_state WHERE scope_id = ?1",
                vec![TursoValue::Text(scope.scope_id.clone())],
            )
            .await?;
        rows.first()
            .map(|row| {
                let blob = row_required_blob(row, "state_blob")?;
                decode_stream_trim_state_blob(&blob)
            })
            .transpose()
    }

    pub(crate) async fn load_stream_trim_boundaries<C>(
        &self,
        conn: &C,
        scope: &StreamTrimScope,
    ) -> StorageResult<StreamTrimScopeBoundaries>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let stream_name = stream_name_for_trim_scope(scope);
        let latest_item_id = match scope.kind {
            storage_provider::StreamTrimScopeKind::Table => None,
            storage_provider::StreamTrimScopeKind::Item => {
                self.latest_stream_item_id(conn, &stream_name).await?
            }
        };
        let retained_table_pointer_boundary = match scope.kind {
            storage_provider::StreamTrimScopeKind::Table => None,
            storage_provider::StreamTrimScopeKind::Item => {
                self.retained_item_pointer_boundary(conn, &stream_name)
                    .await?
            }
        };
        Ok(StreamTrimScopeBoundaries {
            latest_item_id,
            protected_boundary: None,
            retained_table_pointer_boundary,
        })
    }

    pub(crate) async fn apply_item_stream_duration<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        key_attributes: &KeyAttributes,
        requested_retention: Option<StreamRetentionDuration>,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let Some(retention) = requested_retention else {
            return Ok(());
        };
        let item_key = ItemKey::from_key_schema(
            table_info.table_name.clone(),
            &table_info.key_schema,
            key_attributes,
        )
        .map_err(|error| {
            StorageError::validation(format!("custom item stream TTL key failed: {error}"))
        })?;
        let item_stream = StreamName::table_item_stream(&table_info.table_name, &item_key)
            .map_err(|error| {
                StorageError::validation(format!("custom item stream TTL scope failed: {error}"))
            })?;
        let item_scope_id = String::from(&item_stream);
        let item_key_hash = item_stream_key_hash(&item_stream);
        let plan = plan_validated_item_stream_duration(
            table_info.table_name.clone(),
            item_scope_id,
            item_key_hash,
            item_stream_policy_version(retention, table_info.table_stream_duration),
            retention,
            table_info.table_stream_duration,
            TimestampMillis::now(),
        );
        self.write_stream_trim_state(
            conn,
            StreamTrimStateWrite {
                state: plan.trim_state,
                next_marker: plan.due_marker,
            },
        )
        .await
    }

    pub(crate) async fn write_stream_trim_state<C>(
        &self,
        conn: &C,
        write: StreamTrimStateWrite,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let state = write.state;
        let scope_id = state.scope.scope_id.clone();
        let state_blob = storage_types::storage_serde::to_bytes(&state)?;
        let _ = self
            .execute(
                conn,
                r"INSERT INTO sys_stream_trim_state (scope_id, state_blob, updated_at)
                  VALUES (?1, ?2, ?3)
                  ON CONFLICT(scope_id) DO UPDATE SET
                      state_blob = excluded.state_blob,
                      updated_at = excluded.updated_at",
                vec![
                    TursoValue::Text(scope_id),
                    TursoValue::Blob(state_blob),
                    TursoValue::Integer(*state.updated_at),
                ],
            )
            .await?;
        if let Some(marker) = write.next_marker {
            self.upsert_stream_trim_due_marker(conn, marker).await?;
        }
        Ok(())
    }

    pub(super) async fn insert_stream_pointer_index<C>(
        &self,
        conn: &C,
        entry: TursoStreamPointerIndexEntry<'_>,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let _ = self
            .execute(
                conn,
                r"INSERT OR REPLACE INTO sys_stream_pointer_index (
                      table_name,
                      item_stream_name,
                      item_stream_version,
                      table_stream_item_id,
                      system_stream_item_id,
                      created_at
                )
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                vec![
                    TursoValue::Text(entry.table_name.to_string()),
                    TursoValue::Text(entry.item_stream_name.to_owned()),
                    TursoValue::Text(StreamItemId::from(entry.item_stream_version).to_string()),
                    TursoValue::Text(entry.table_stream_item_id.to_string()),
                    TursoValue::Text(entry.system_stream_item_id.to_string()),
                    TursoValue::Integer(entry.created_at.timestamp_millis()),
                ],
            )
            .await?;
        Ok(())
    }

    async fn upsert_stream_trim_due_marker<C>(
        &self,
        conn: &C,
        marker: StreamTrimDueMarker,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let policy_version = i64::try_from(marker.policy_version).map_err(|error| {
            StorageError::validation(format!("stream trim policy version is too large: {error}"))
        })?;
        let marker_blob = storage_types::storage_serde::to_bytes(&marker)?;
        let _ = self
            .execute(
                conn,
                r"INSERT INTO sys_stream_trim_due_markers (
                      due_bucket, scope_id, policy_version, marker_blob
                  )
                  VALUES (?1, ?2, ?3, ?4)
                  ON CONFLICT(due_bucket, scope_id, policy_version) DO UPDATE SET
                      marker_blob = excluded.marker_blob",
                vec![
                    TursoValue::Integer(*marker.due_bucket),
                    TursoValue::Text(marker.scope.scope_id),
                    TursoValue::Integer(policy_version),
                    TursoValue::Blob(marker_blob),
                ],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn list_due_stream_trim_markers<C>(
        &self,
        conn: &C,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let limit = i64::try_from(limit).map_err(|error| {
            StorageError::validation(format!("stream trim marker limit is too large: {error}"))
        })?;
        let rows = self
            .query_rows(
                conn,
                r"SELECT marker_blob
                  FROM sys_stream_trim_due_markers
                  WHERE due_bucket <= ?1
                  ORDER BY due_bucket ASC, scope_id ASC, policy_version ASC
                  LIMIT ?2",
                vec![TursoValue::Integer(*due_before), TursoValue::Integer(limit)],
            )
            .await?;
        rows.iter()
            .map(|row| {
                let blob = row_required_blob(row, "marker_blob")?;
                storage_types::storage_serde::from_bytes::<StreamTrimDueMarker>(&blob).map_err(
                    |error| {
                        StorageError::internal(&format!(
                            "decode stream trim due marker failed: {error}"
                        ))
                    },
                )
            })
            .collect()
    }

    pub(crate) async fn trim_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        let this = self.clone();
        self.with_transaction(true, |conn| {
            let this = this.clone();
            let request = request.clone();
            Box::pin(async move {
                let stream_name = stream_name_for_trim_scope(&request.scope);
                let rows = this
                    .stream_rows_to_trim(conn, &request, &stream_name)
                    .await?;
                let deleted_rows = rows.len();
                for row in rows {
                    let item_id = row.item_id;
                    let _ = this
                        .execute(
                            conn,
                            "DELETE FROM sys_stream_items WHERE stream_name = ?1 AND item_id = ?2",
                            vec![
                                TursoValue::Text(stream_name.clone()),
                                TursoValue::Text(item_id.clone()),
                            ],
                        )
                        .await?;
                    if matches!(
                        request.scope.kind,
                        storage_provider::StreamTrimScopeKind::Table
                    ) {
                        let _ = this
                            .execute(
                                conn,
                                "DELETE FROM sys_stream_pointer_index
                                 WHERE table_name = ?1 AND table_stream_item_id = ?2",
                                vec![
                                    TursoValue::Text(request.scope.table_name.to_string()),
                                    TursoValue::Text(item_id),
                                ],
                            )
                            .await?;
                    }
                }
                let first_remaining = this.first_remaining_stream_row(conn, &stream_name).await?;
                Ok(StreamDurationTrimPageResult {
                    deleted_rows,
                    first_remaining_version: first_remaining.as_ref().map(|row| row.version),
                    first_remaining_timestamp: first_remaining.map(|row| row.created_at),
                })
            })
        })
        .await
    }

    pub(crate) async fn finish_stream_trim_marker(
        &self,
        marker: StreamTrimDueMarker,
        write: Option<StreamTrimStateWrite>,
    ) -> StorageResult<()> {
        let this = self.clone();
        self.with_transaction(true, |conn| {
            let this = this.clone();
            let marker = marker.clone();
            let write = write.clone();
            Box::pin(async move {
                let policy_version = i64::try_from(marker.policy_version).map_err(|error| {
                    StorageError::validation(format!(
                        "stream trim policy version is too large: {error}"
                    ))
                })?;
                let _ = this
                    .execute(
                        conn,
                        "DELETE FROM sys_stream_trim_due_markers
                         WHERE due_bucket = ?1 AND scope_id = ?2 AND policy_version = ?3",
                        vec![
                            TursoValue::Integer(*marker.due_bucket),
                            TursoValue::Text(marker.scope.scope_id),
                            TursoValue::Integer(policy_version),
                        ],
                    )
                    .await?;
                if let Some(write) = write {
                    this.write_stream_trim_state(conn, write).await?;
                }
                Ok(())
            })
        })
        .await
    }

    async fn latest_stream_item_id<C>(
        &self,
        conn: &C,
        stream_name: &str,
    ) -> StorageResult<Option<StreamItemId>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let rows = self
            .query_rows(
                conn,
                "SELECT item_id FROM sys_stream_items
                 WHERE stream_name = ?1
                 ORDER BY item_id DESC
                 LIMIT 1",
                vec![TursoValue::Text(stream_name.to_string())],
            )
            .await?;
        rows.first()
            .map(|row| {
                let item_id = row_required_text(row, "item_id")?;
                stream_item_id_from_text(&item_id)
            })
            .transpose()
    }

    async fn retained_item_pointer_boundary<C>(
        &self,
        conn: &C,
        item_stream_name: &str,
    ) -> StorageResult<Option<StreamTrimBoundary>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let rows = self
            .query_rows(
                conn,
                "SELECT item_stream_version FROM sys_stream_pointer_index
                 WHERE item_stream_name = ?1
                 ORDER BY item_stream_version ASC
                 LIMIT 1",
                vec![TursoValue::Text(item_stream_name.to_string())],
            )
            .await?;
        rows.first()
            .map(|row| {
                let version = row_required_text(row, "item_stream_version")?;
                let version = version.parse::<u64>().map_err(|error| {
                    StorageError::internal(&format!(
                        "decode pointer item stream version failed: {error}"
                    ))
                })?;
                Ok(StreamTrimBoundary {
                    item_id: StreamItemId::from(storage_types::ItemStreamVersion::new(version)),
                })
            })
            .transpose()
    }

    async fn stream_rows_to_trim<C>(
        &self,
        conn: &C,
        request: &StreamDurationTrimPageRequest,
        stream_name: &str,
    ) -> StorageResult<Vec<TursoStreamTrimRow>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let page_limit = i64::try_from(request.page_limit).map_err(|error| {
            StorageError::validation(format!("stream trim page limit is too large: {error}"))
        })?;
        let rows = if let Some(max_deleted_id) = request.max_deleted_item_id {
            self.query_rows(
                conn,
                "SELECT item_id, created_at
                 FROM sys_stream_items
                 WHERE stream_name = ?1 AND created_at <= ?2 AND item_id <= ?3
                 ORDER BY item_id ASC
                 LIMIT ?4",
                vec![
                    TursoValue::Text(stream_name.to_string()),
                    TursoValue::Integer(*request.cutoff_timestamp),
                    TursoValue::Text(max_deleted_id.to_string()),
                    TursoValue::Integer(page_limit),
                ],
            )
            .await?
        } else {
            self.query_rows(
                conn,
                "SELECT item_id, created_at
                 FROM sys_stream_items
                 WHERE stream_name = ?1 AND created_at <= ?2
                 ORDER BY item_id ASC
                 LIMIT ?3",
                vec![
                    TursoValue::Text(stream_name.to_string()),
                    TursoValue::Integer(*request.cutoff_timestamp),
                    TursoValue::Integer(page_limit),
                ],
            )
            .await?
        };
        rows.iter().map(turso_stream_trim_row).collect()
    }

    async fn first_remaining_stream_row<C>(
        &self,
        conn: &C,
        stream_name: &str,
    ) -> StorageResult<Option<TursoStreamTrimRow>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let rows = self
            .query_rows(
                conn,
                "SELECT item_id, created_at FROM sys_stream_items
                 WHERE stream_name = ?1
                 ORDER BY item_id ASC
                 LIMIT 1",
                vec![TursoValue::Text(stream_name.to_string())],
            )
            .await?;
        rows.first().map(turso_stream_trim_row).transpose()
    }
}
