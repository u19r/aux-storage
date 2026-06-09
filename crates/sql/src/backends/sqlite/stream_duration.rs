use async_trait::async_trait;
use storage_provider::{
    StreamDurationTrimBackend, StreamDurationTrimPageRequest, StreamDurationTrimPageResult,
    StreamTrimBoundary, StreamTrimDueMarker, StreamTrimScope, StreamTrimScopeBoundaries,
    StreamTrimState, StreamTrimStateWrite, plan_validated_item_stream_duration,
};
use storage_types::{
    ItemKey, ItemStreamVersion, KeyAttributes, StorageError, StorageResult, StoredTableInfo,
    StreamItemId, StreamName, StreamRetentionDuration, TimestampMillis,
};

use crate::{
    backends::sqlite::SQLiteStorageProvider,
    error_handler::map_sqlite_error,
    utils::{call_sqlite, call_sqlite_raw},
};

const ITEM_KEY_HASH_PREFIX: &str = "sqlite-key:";

const CREATE_TRIM_STATE_TABLE: &str = r"CREATE TABLE IF NOT EXISTS sys_stream_trim_state (
    scope_id TEXT PRIMARY KEY,
    state_blob BLOB NOT NULL,
    updated_at INTEGER NOT NULL
)";

const CREATE_TRIM_DUE_MARKERS_TABLE: &str = r"CREATE TABLE IF NOT EXISTS sys_stream_trim_due_markers (
    due_bucket INTEGER NOT NULL,
    scope_id TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    marker_blob BLOB NOT NULL,
    PRIMARY KEY (due_bucket, scope_id, policy_version)
)";

const CREATE_TRIM_DUE_MARKERS_SCOPE_INDEX: &str = "CREATE INDEX IF NOT EXISTS \
                                                   idx_stream_trim_due_markers_scope ON \
                                                   sys_stream_trim_due_markers(scope_id)";

const CREATE_STREAM_POINTER_INDEX_TABLE: &str = r"CREATE TABLE IF NOT EXISTS sys_stream_pointer_index (
    table_name TEXT NOT NULL,
    item_stream_name TEXT NOT NULL,
    item_stream_version TEXT NOT NULL,
    table_stream_item_id TEXT NOT NULL,
    system_stream_item_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (table_name, item_stream_name, item_stream_version, table_stream_item_id)
)";

const CREATE_STREAM_POINTER_INDEX_ITEM_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_stream_pointer_index_item ON \
     sys_stream_pointer_index(item_stream_name, item_stream_version)";

const CREATE_STREAM_POINTER_INDEX_TABLE_POINTER_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_stream_pointer_index_table_pointer ON \
     sys_stream_pointer_index(table_name, table_stream_item_id)";

impl SQLiteStorageProvider {
    pub(crate) async fn initialize_stream_duration_tables(&self) -> StorageResult<()> {
        call_sqlite_raw(&self.connection, |conn| {
            conn.execute(CREATE_TRIM_STATE_TABLE, [])?;
            conn.execute(CREATE_TRIM_DUE_MARKERS_TABLE, [])?;
            conn.execute(CREATE_TRIM_DUE_MARKERS_SCOPE_INDEX, [])?;
            conn.execute(CREATE_STREAM_POINTER_INDEX_TABLE, [])?;
            conn.execute(CREATE_STREAM_POINTER_INDEX_ITEM_INDEX, [])?;
            conn.execute(CREATE_STREAM_POINTER_INDEX_TABLE_POINTER_INDEX, [])?;
            Ok(())
        })
        .await
    }

    pub(crate) fn insert_stream_pointer_index_tx(
        sqlite: &crate::utils::SqliteConn<'_>,
        table_name: &storage_types::TableName,
        item_stream: &StreamName,
        item_stream_version: ItemStreamVersion,
        table_stream_item_id: StreamItemId,
        system_stream_item_id: StreamItemId,
        created_at: TimestampMillis,
    ) -> StorageResult<()> {
        let item_stream_name = String::from(item_stream);
        sqlite
            .execute(
                r"INSERT OR REPLACE INTO sys_stream_pointer_index (
                      table_name, item_stream_name, item_stream_version, table_stream_item_id,
                      system_stream_item_id, created_at
                  )
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    table_name.as_ref(),
                    item_stream_name,
                    item_stream_version.to_string(),
                    table_stream_item_id.to_string(),
                    system_stream_item_id.to_string(),
                    *created_at,
                ],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    pub(crate) async fn load_stream_trim_state_by_scope(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>> {
        let scope_id = scope.scope_id.clone();
        call_sqlite(&self.connection, move |conn| {
            let result = conn.query_row(
                "SELECT state_blob FROM sys_stream_trim_state WHERE scope_id = ?1",
                [scope_id],
                |row| row.get::<_, Vec<u8>>(0),
            );
            match result {
                Ok(blob) => decode_state_blob(&blob).map(Some),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(err) => Err(map_sqlite_error(err)),
            }
        })
        .await
    }

    pub(crate) async fn write_stream_trim_state_sqlite(
        &self,
        state: StreamTrimState,
    ) -> StorageResult<()> {
        let scope_id = state.scope.scope_id.clone();
        let updated_at = *state.updated_at;
        let state_blob = storage_types::storage_serde::to_bytes(&state)?;
        call_sqlite(&self.connection, move |conn| {
            conn.execute(
                r"INSERT INTO sys_stream_trim_state (scope_id, state_blob, updated_at)
                  VALUES (?1, ?2, ?3)
                  ON CONFLICT(scope_id) DO UPDATE SET
                      state_blob = excluded.state_blob,
                      updated_at = excluded.updated_at",
                rusqlite::params![scope_id, state_blob, updated_at],
            )
            .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    }

    pub(crate) fn apply_item_stream_duration_tx(
        sqlite: &crate::utils::SqliteConn<'_>,
        table_info: &StoredTableInfo,
        key_attributes: &KeyAttributes,
        requested_retention: Option<StreamRetentionDuration>,
    ) -> StorageResult<()> {
        let Some(retention) = requested_retention else {
            return Ok(());
        };
        let item_key = ItemKey::from_key_schema(
            table_info.table_name.clone(),
            &table_info.key_schema,
            key_attributes,
        )
        .map_err(|err| {
            StorageError::validation(format!("custom item stream TTL key failed: {err}"))
        })?;
        let item_stream = StreamName::table_item_stream(&table_info.table_name, &item_key)
            .map_err(|err| {
                StorageError::validation(format!("custom item stream TTL scope failed: {err}"))
            })?;
        let item_scope_id = String::from(&item_stream);
        let item_key_hash = item_stream_key_hash(&item_stream);
        let plan = plan_validated_item_stream_duration(
            table_info.table_name.clone(),
            item_scope_id.clone(),
            item_key_hash,
            item_stream_policy_version(retention, table_info.table_stream_duration),
            retention,
            table_info.table_stream_duration,
            TimestampMillis::now(),
        );
        write_stream_trim_state_sqlite_conn(
            sqlite,
            StreamTrimStateWrite {
                state: plan.trim_state,
                next_marker: plan.due_marker,
            },
        )?;
        Ok(())
    }

    pub(crate) async fn list_due_stream_trim_markers_sqlite(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        let limit = i64::try_from(limit).map_err(|err| {
            StorageError::validation(format!("stream trim marker page limit is too large: {err}"))
        })?;
        call_sqlite(&self.connection, move |conn| {
            let mut stmt = conn
                .prepare(
                    r"SELECT marker_blob
                      FROM sys_stream_trim_due_markers
                      WHERE due_bucket <= ?1
                      ORDER BY due_bucket ASC, scope_id ASC, policy_version ASC
                      LIMIT ?2",
                )
                .map_err(map_sqlite_error)?;
            let rows = stmt
                .query_map(rusqlite::params![*due_before, limit], |row| {
                    row.get::<_, Vec<u8>>(0)
                })
                .map_err(map_sqlite_error)?;
            let mut markers = Vec::new();
            for row in rows {
                let blob = row.map_err(map_sqlite_error)?;
                markers.push(decode_marker_blob(&blob)?);
            }
            Ok(markers)
        })
        .await
    }

    async fn load_stream_trim_boundaries_sqlite(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<StreamTrimScopeBoundaries> {
        let scope = scope.clone();
        let protected_floor = self.oldest_protected_replication_cursor().await?;
        call_sqlite(&self.connection, move |conn| {
            let stream_name = stream_name_for_scope(&scope);
            let latest_item_id = match scope.kind {
                storage_provider::StreamTrimScopeKind::Table => None,
                storage_provider::StreamTrimScopeKind::Item => {
                    latest_stream_item_id(conn, &stream_name)?
                }
            };
            let protected_boundary = match scope.kind {
                storage_provider::StreamTrimScopeKind::Table => {
                    protected_table_pointer_boundary(conn, &scope.table_name, protected_floor)?
                }
                storage_provider::StreamTrimScopeKind::Item => None,
            };
            let retained_table_pointer_boundary = match scope.kind {
                storage_provider::StreamTrimScopeKind::Table => None,
                storage_provider::StreamTrimScopeKind::Item => {
                    retained_item_pointer_boundary(conn, &stream_name)?
                }
            };
            Ok(StreamTrimScopeBoundaries {
                latest_item_id,
                protected_boundary,
                retained_table_pointer_boundary,
            })
        })
        .await
    }

    async fn trim_stream_page_sqlite(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        call_sqlite(&self.connection, move |conn| {
            let stream_name = stream_name_for_scope(&request.scope);
            let page_limit = i64::try_from(request.page_limit).map_err(|err| {
                StorageError::validation(format!("stream trim page limit is too large: {err}"))
            })?;
            let max_deleted_id = request.max_deleted_item_id.map(|id| id.to_string());
            let rows = stream_rows_to_trim(
                conn,
                &stream_name,
                request.cutoff_timestamp,
                max_deleted_id.as_deref(),
                page_limit,
            )?;
            let deleted_rows = rows.len();
            for row in rows {
                conn.execute(
                    "DELETE FROM sys_stream_items WHERE stream_name = ?1 AND item_id = ?2",
                    rusqlite::params![stream_name, row.item_id],
                )
                .map_err(map_sqlite_error)?;
                if matches!(
                    request.scope.kind,
                    storage_provider::StreamTrimScopeKind::Table
                ) {
                    conn.execute(
                        "DELETE FROM sys_stream_pointer_index
                         WHERE table_name = ?1 AND table_stream_item_id = ?2",
                        rusqlite::params![request.scope.table_name.as_ref(), row.item_id],
                    )
                    .map_err(map_sqlite_error)?;
                }
            }
            let first_remaining = first_remaining_stream_row(conn, &stream_name)?;
            Ok(StreamDurationTrimPageResult {
                deleted_rows,
                first_remaining_version: first_remaining.as_ref().map(|row| row.version),
                first_remaining_timestamp: first_remaining.map(|row| row.created_at),
            })
        })
        .await
    }

    #[cfg(test)]
    pub(crate) async fn upsert_stream_trim_due_marker(
        &self,
        marker: StreamTrimDueMarker,
    ) -> StorageResult<()> {
        let due_bucket = *marker.due_bucket;
        let scope_id = marker.scope.scope_id.clone();
        let policy_version = i64::try_from(marker.policy_version).map_err(|err| {
            StorageError::validation(format!("stream trim policy version is too large: {err}"))
        })?;
        let marker_blob = storage_types::storage_serde::to_bytes(&marker)?;
        call_sqlite(&self.connection, move |conn| {
            conn.execute(
                r"INSERT INTO sys_stream_trim_due_markers (
                      due_bucket, scope_id, policy_version, marker_blob
                  )
                  VALUES (?1, ?2, ?3, ?4)
                  ON CONFLICT(due_bucket, scope_id, policy_version) DO UPDATE SET
                      marker_blob = excluded.marker_blob",
                rusqlite::params![due_bucket, scope_id, policy_version, marker_blob],
            )
            .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    }
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

#[derive(Clone)]
struct StreamTrimRow {
    item_id: String,
    version: ItemStreamVersion,
    created_at: TimestampMillis,
}

fn stream_name_for_scope(scope: &StreamTrimScope) -> String {
    match scope.kind {
        storage_provider::StreamTrimScopeKind::Table => {
            String::from(&StreamName::table_stream(&scope.table_name))
        }
        storage_provider::StreamTrimScopeKind::Item => scope.scope_id.clone(),
    }
}

fn latest_stream_item_id(
    conn: &rusqlite::Connection,
    stream_name: &str,
) -> StorageResult<Option<StreamItemId>> {
    let result = conn.query_row(
        "SELECT item_id FROM sys_stream_items WHERE stream_name = ?1 ORDER BY item_id DESC LIMIT 1",
        [stream_name],
        |row| row.get::<_, String>(0),
    );
    match result {
        Ok(item_id) => stream_item_id_from_hex(&item_id).map(Some),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(map_sqlite_error(err)),
    }
}

fn retained_item_pointer_boundary(
    conn: &rusqlite::Connection,
    item_stream_name: &str,
) -> StorageResult<Option<StreamTrimBoundary>> {
    let result = conn.query_row(
        "SELECT item_stream_version FROM sys_stream_pointer_index
         WHERE item_stream_name = ?1
         ORDER BY item_stream_version ASC
         LIMIT 1",
        [item_stream_name],
        |row| row.get::<_, String>(0),
    );
    match result {
        Ok(version) => {
            let version = version.parse::<u64>().map_err(|err| {
                StorageError::internal(&format!("decode pointer item stream version failed: {err}"))
            })?;
            Ok(Some(StreamTrimBoundary {
                item_id: StreamItemId::from(ItemStreamVersion::new(version)),
            }))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(map_sqlite_error(err)),
    }
}

fn protected_table_pointer_boundary(
    conn: &rusqlite::Connection,
    table_name: &storage_types::TableName,
    protected_floor: Option<StreamItemId>,
) -> StorageResult<Option<StreamTrimBoundary>> {
    let Some(protected_floor) = protected_floor else {
        return Ok(None);
    };
    let result = conn.query_row(
        "SELECT table_stream_item_id FROM sys_stream_pointer_index
         WHERE table_name = ?1 AND system_stream_item_id >= ?2
         ORDER BY system_stream_item_id ASC
         LIMIT 1",
        rusqlite::params![table_name.as_ref(), protected_floor.to_string()],
        |row| row.get::<_, String>(0),
    );
    match result {
        Ok(item_id) => {
            stream_item_id_from_hex(&item_id).map(|item_id| Some(StreamTrimBoundary { item_id }))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(map_sqlite_error(err)),
    }
}

fn stream_rows_to_trim(
    conn: &rusqlite::Connection,
    stream_name: &str,
    cutoff_timestamp: TimestampMillis,
    max_deleted_id: Option<&str>,
    page_limit: i64,
) -> StorageResult<Vec<StreamTrimRow>> {
    let mut rows = Vec::new();
    let sql_without_version = r"SELECT item_id, created_at
        FROM sys_stream_items
        WHERE stream_name = ?1 AND created_at <= ?2
        ORDER BY item_id ASC
        LIMIT ?3";
    let sql_with_version = r"SELECT item_id, created_at
        FROM sys_stream_items
        WHERE stream_name = ?1 AND created_at <= ?2 AND item_id <= ?3
        ORDER BY item_id ASC
        LIMIT ?4";
    if let Some(max_deleted_id) = max_deleted_id {
        let mut stmt = conn.prepare(sql_with_version).map_err(map_sqlite_error)?;
        let mapped = stmt
            .query_map(
                rusqlite::params![stream_name, *cutoff_timestamp, max_deleted_id, page_limit],
                stream_trim_row_from_sql,
            )
            .map_err(map_sqlite_error)?;
        for row in mapped {
            rows.push(row.map_err(map_sqlite_error)?);
        }
    } else {
        let mut stmt = conn
            .prepare(sql_without_version)
            .map_err(map_sqlite_error)?;
        let mapped = stmt
            .query_map(
                rusqlite::params![stream_name, *cutoff_timestamp, page_limit],
                stream_trim_row_from_sql,
            )
            .map_err(map_sqlite_error)?;
        for row in mapped {
            rows.push(row.map_err(map_sqlite_error)?);
        }
    }
    Ok(rows)
}

fn first_remaining_stream_row(
    conn: &rusqlite::Connection,
    stream_name: &str,
) -> StorageResult<Option<StreamTrimRow>> {
    let result = conn.query_row(
        "SELECT item_id, created_at FROM sys_stream_items
         WHERE stream_name = ?1
         ORDER BY item_id ASC
         LIMIT 1",
        [stream_name],
        stream_trim_row_from_sql,
    );
    match result {
        Ok(row) => Ok(Some(row)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(map_sqlite_error(err)),
    }
}

fn stream_trim_row_from_sql(row: &rusqlite::Row<'_>) -> rusqlite::Result<StreamTrimRow> {
    let item_id = row.get::<_, String>(0)?;
    let created_at = row.get::<_, i64>(1)?;
    let stream_item_id = stream_item_id_from_hex(&item_id).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            )),
        )
    })?;
    Ok(StreamTrimRow {
        item_id,
        version: ItemStreamVersion::from(stream_item_id),
        created_at: TimestampMillis::from_timestamp(created_at),
    })
}

fn stream_item_id_from_hex(value: &str) -> StorageResult<StreamItemId> {
    value
        .parse()
        .map_err(|err| StorageError::internal(&format!("decode stream item id failed: {err}")))
}

#[async_trait]
impl StreamDurationTrimBackend for SQLiteStorageProvider {
    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        self.list_due_stream_trim_markers_sqlite(due_before, limit)
            .await
    }

    async fn load_stream_trim_state(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>> {
        self.load_stream_trim_state_by_scope(scope).await
    }

    async fn load_stream_trim_boundaries(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<StreamTrimScopeBoundaries> {
        self.load_stream_trim_boundaries_sqlite(scope).await
    }

    async fn trim_table_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.trim_stream_page_sqlite(request).await
    }

    async fn trim_item_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.trim_stream_page_sqlite(request).await
    }

    async fn finish_stream_trim_marker(
        &self,
        marker: StreamTrimDueMarker,
        write: Option<StreamTrimStateWrite>,
    ) -> StorageResult<()> {
        let due_bucket = *marker.due_bucket;
        let scope_id = marker.scope.scope_id.clone();
        let policy_version = i64::try_from(marker.policy_version).map_err(|err| {
            StorageError::validation(format!("stream trim policy version is too large: {err}"))
        })?;
        let prepared_write = write.map(prepare_state_write).transpose()?;
        call_sqlite(&self.connection, move |conn| {
            let tx = conn.transaction().map_err(map_sqlite_error)?;
            tx.execute(
                "DELETE FROM sys_stream_trim_due_markers
                 WHERE due_bucket = ?1 AND scope_id = ?2 AND policy_version = ?3",
                rusqlite::params![due_bucket, scope_id, policy_version],
            )
            .map_err(map_sqlite_error)?;

            if let Some(write) = prepared_write {
                write.apply(&tx)?;
            }

            tx.commit().map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    }
}

struct PreparedStateWrite {
    scope_id: String,
    state_blob: Vec<u8>,
    updated_at: i64,
    next_marker: Option<PreparedDueMarker>,
}

impl PreparedStateWrite {
    fn apply(self, conn: &rusqlite::Connection) -> StorageResult<()> {
        conn.execute(
            r"INSERT INTO sys_stream_trim_state (scope_id, state_blob, updated_at)
              VALUES (?1, ?2, ?3)
              ON CONFLICT(scope_id) DO UPDATE SET
                  state_blob = excluded.state_blob,
                  updated_at = excluded.updated_at",
            rusqlite::params![self.scope_id, self.state_blob, self.updated_at],
        )
        .map_err(map_sqlite_error)?;

        if let Some(marker) = self.next_marker {
            marker.apply(conn)?;
        }
        Ok(())
    }
}

pub(crate) fn write_stream_trim_state_tx(
    tx: &rusqlite::Transaction<'_>,
    write: StreamTrimStateWrite,
) -> StorageResult<()> {
    prepare_state_write(write)?.apply(tx)
}

pub(crate) fn write_stream_trim_state_sqlite_conn(
    sqlite: &crate::utils::SqliteConn<'_>,
    write: StreamTrimStateWrite,
) -> StorageResult<()> {
    prepare_state_write(write)?.apply(sqlite)
}

struct PreparedDueMarker {
    due_bucket: i64,
    scope_id: String,
    policy_version: i64,
    marker_blob: Vec<u8>,
}

impl PreparedDueMarker {
    fn apply(self, conn: &rusqlite::Connection) -> StorageResult<()> {
        conn.execute(
            r"INSERT INTO sys_stream_trim_due_markers (
                  due_bucket, scope_id, policy_version, marker_blob
              )
              VALUES (?1, ?2, ?3, ?4)
              ON CONFLICT(due_bucket, scope_id, policy_version) DO UPDATE SET
                  marker_blob = excluded.marker_blob",
            rusqlite::params![
                self.due_bucket,
                self.scope_id,
                self.policy_version,
                self.marker_blob
            ],
        )
        .map_err(map_sqlite_error)?;
        Ok(())
    }
}

fn prepare_state_write(write: StreamTrimStateWrite) -> StorageResult<PreparedStateWrite> {
    let state = write.state;
    let scope_id = state.scope.scope_id.clone();
    let updated_at = *state.updated_at;
    let state_blob = storage_types::storage_serde::to_bytes(&state)?;
    let next_marker = write.next_marker.map(prepare_due_marker).transpose()?;
    Ok(PreparedStateWrite {
        scope_id,
        state_blob,
        updated_at,
        next_marker,
    })
}

fn prepare_due_marker(marker: StreamTrimDueMarker) -> StorageResult<PreparedDueMarker> {
    let due_bucket = *marker.due_bucket;
    let scope_id = marker.scope.scope_id.clone();
    let policy_version = i64::try_from(marker.policy_version).map_err(|err| {
        StorageError::validation(format!("stream trim policy version is too large: {err}"))
    })?;
    let marker_blob = storage_types::storage_serde::to_bytes(&marker)?;
    Ok(PreparedDueMarker {
        due_bucket,
        scope_id,
        policy_version,
        marker_blob,
    })
}

fn decode_state_blob(blob: &[u8]) -> StorageResult<StreamTrimState> {
    storage_types::storage_serde::from_bytes(blob)
        .map_err(|err| StorageError::internal(&format!("decode stream trim state failed: {err}")))
}

fn decode_marker_blob(blob: &[u8]) -> StorageResult<StreamTrimDueMarker> {
    storage_types::storage_serde::from_bytes(blob)
        .map_err(|err| StorageError::internal(&format!("decode stream trim marker failed: {err}")))
}
