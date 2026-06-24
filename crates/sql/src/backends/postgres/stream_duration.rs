use async_trait::async_trait;
use deadpool_postgres::GenericClient;
use storage_provider::{
    StreamDurationTrimBackend, StreamDurationTrimConfig, StreamDurationTrimPageRequest,
    StreamDurationTrimPageResult, StreamDurationTrimWorker, StreamTrimBoundary,
    StreamTrimDueMarker, StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimState,
    StreamTrimStateWrite, plan_validated_item_stream_duration,
};
use storage_types::{
    ItemKey, ItemStreamVersion, KeyAttributes, StorageError, StorageResult, StoredTableInfo,
    StreamItemId, StreamName, StreamRetentionDuration, TimestampMillis,
};

use crate::backends::postgres::PostgresStorageProvider;

const ITEM_KEY_HASH_PREFIX: &str = "postgres-key:";

impl PostgresStorageProvider {
    pub(super) async fn initialize_stream_duration_tables(&self) -> StorageResult<()> {
        let client = self.acquire_client("initialize_stream_duration").await?;
        client
            .batch_execute(CREATE_STREAM_DURATION_TABLES)
            .await
            .map_err(|err| Self::map_postgres_error("initialize stream duration tables", err))?;
        Ok(())
    }

    pub(super) async fn run_custom_stream_trim_once(&self) -> StorageResult<bool> {
        let worker = StreamDurationTrimWorker::new(
            self.clone(),
            StreamDurationTrimConfig {
                marker_page_size: 250,
                stream_page_size: 1_000,
            },
        );
        Ok(worker
            .run_due_page(TimestampMillis::now(), TimestampMillis::now())
            .await?
            .did_work())
    }

    pub(super) async fn load_postgres_table_scope_id(
        &self,
        table_name: &storage_types::TableName,
    ) -> StorageResult<String> {
        let client = self.acquire_client("load_postgres_table_scope_id").await?;
        let table_name_value = table_name.to_string();
        let row = client
            .query_opt(
                "SELECT id FROM tables WHERE table_name = $1",
                &[&table_name_value],
            )
            .await
            .map_err(|err| Self::map_postgres_error("load postgres table scope id", err))?;
        let Some(row) = row else {
            return Err(StorageError::table_not_found(table_name_value.as_str()));
        };
        let table_id: String = row
            .try_get(0)
            .map_err(|err| Self::map_postgres_error("decode postgres table scope id", err))?;
        Ok(postgres_table_scope_id(&table_id))
    }

    pub(super) async fn next_postgres_table_policy_version(
        &self,
        table_scope_id: &str,
    ) -> StorageResult<u64> {
        let scope = StreamTrimScope::table(table_scope_id, storage_types::TableName::new(""));
        let current = self.load_stream_trim_state(&scope).await?;
        Ok(current
            .and_then(|state| state.policy_version.checked_add(1))
            .unwrap_or(1))
    }

    pub(super) async fn write_stream_trim_state_with_client<C: GenericClient + Sync>(
        client: &C,
        write: StreamTrimStateWrite,
    ) -> StorageResult<()> {
        let prepared = prepare_state_write(write)?;
        apply_prepared_state_write(client, prepared).await
    }

    pub(super) async fn apply_item_stream_duration_with_client<C: GenericClient + Sync>(
        client: &C,
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
            item_scope_id,
            item_key_hash,
            item_stream_policy_version(retention, table_info.table_stream_duration),
            retention,
            table_info.table_stream_duration,
            TimestampMillis::now(),
        );
        Self::write_stream_trim_state_with_client(
            client,
            StreamTrimStateWrite {
                state: plan.trim_state,
                next_marker: plan.due_marker,
            },
        )
        .await
    }

    pub(super) async fn insert_stream_pointer_index_with_client<C: GenericClient + Sync>(
        client: &C,
        table_name: &storage_types::TableName,
        item_stream: &StreamName,
        item_stream_version: ItemStreamVersion,
        table_stream_item_id: StreamItemId,
        system_stream_item_id: StreamItemId,
        created_at: TimestampMillis,
    ) -> StorageResult<()> {
        let item_stream_name = Self::encode_stream_name(item_stream);
        let table_name_value = table_name.to_string();
        let item_stream_version_value = item_stream_version.to_string();
        let table_stream_item_id_value = table_stream_item_id.to_string();
        let system_stream_item_id_value = system_stream_item_id.to_string();
        let created_at_ms = *created_at;
        client
            .execute(
                r"INSERT INTO sys_stream_pointer_index (
                      table_name, item_stream_name, item_stream_version, table_stream_item_id,
                      system_stream_item_id, created_at
                  )
                  VALUES ($1, $2, $3, $4, $5, $6)
                  ON CONFLICT(table_name, item_stream_name, item_stream_version, table_stream_item_id)
                  DO UPDATE SET
                      system_stream_item_id = excluded.system_stream_item_id,
                      created_at = excluded.created_at",
                &[
                    &table_name_value,
                    &item_stream_name,
                    &item_stream_version_value,
                    &table_stream_item_id_value,
                    &system_stream_item_id_value,
                    &created_at_ms,
                ],
            )
            .await
            .map_err(|err| Self::map_postgres_write_error("insert stream pointer index", err))?;
        Ok(())
    }
}

const CREATE_STREAM_DURATION_TABLES: &str = r"CREATE TABLE IF NOT EXISTS sys_stream_trim_state (
    scope_id TEXT PRIMARY KEY,
    state_blob BYTEA NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS sys_stream_trim_due_markers (
    due_bucket BIGINT NOT NULL,
    scope_id TEXT NOT NULL,
    policy_version BIGINT NOT NULL,
    marker_blob BYTEA NOT NULL,
    PRIMARY KEY (due_bucket, scope_id, policy_version)
);
CREATE INDEX IF NOT EXISTS idx_stream_trim_due_markers_scope
    ON sys_stream_trim_due_markers(scope_id);
CREATE TABLE IF NOT EXISTS sys_stream_pointer_index (
    table_name TEXT NOT NULL,
    item_stream_name TEXT NOT NULL,
    item_stream_version TEXT NOT NULL,
    table_stream_item_id TEXT NOT NULL,
    system_stream_item_id TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (table_name, item_stream_name, item_stream_version, table_stream_item_id)
);
CREATE INDEX IF NOT EXISTS idx_stream_pointer_index_item
    ON sys_stream_pointer_index(item_stream_name, item_stream_version);
CREATE INDEX IF NOT EXISTS idx_stream_pointer_index_table_pointer
    ON sys_stream_pointer_index(table_name, table_stream_item_id);";

pub(super) fn postgres_table_scope_id(table_id: &str) -> String {
    format!("postgres-table:{table_id}")
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

struct PreparedStateWrite {
    scope_id: String,
    state_blob: Vec<u8>,
    updated_at: i64,
    next_marker: Option<PreparedDueMarker>,
}

struct PreparedDueMarker {
    due_bucket: i64,
    scope_id: String,
    policy_version: i64,
    marker_blob: Vec<u8>,
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

async fn apply_prepared_state_write<C: GenericClient + Sync>(
    client: &C,
    write: PreparedStateWrite,
) -> StorageResult<()> {
    client
        .execute(
            r"INSERT INTO sys_stream_trim_state (scope_id, state_blob, updated_at)
              VALUES ($1, $2, $3)
              ON CONFLICT(scope_id) DO UPDATE SET
                  state_blob = excluded.state_blob,
                  updated_at = excluded.updated_at",
            &[&write.scope_id, &write.state_blob, &write.updated_at],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("write stream trim state", err)
        })?;

    if let Some(marker) = write.next_marker {
        apply_due_marker(client, marker).await?;
    }
    Ok(())
}

async fn apply_due_marker<C: GenericClient + Sync>(
    client: &C,
    marker: PreparedDueMarker,
) -> StorageResult<()> {
    client
        .execute(
            r"INSERT INTO sys_stream_trim_due_markers (
                  due_bucket, scope_id, policy_version, marker_blob
              )
              VALUES ($1, $2, $3, $4)
              ON CONFLICT(due_bucket, scope_id, policy_version) DO UPDATE SET
                  marker_blob = excluded.marker_blob",
            &[
                &marker.due_bucket,
                &marker.scope_id,
                &marker.policy_version,
                &marker.marker_blob,
            ],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("write stream trim due marker", err)
        })?;
    Ok(())
}

fn decode_state_blob(blob: &[u8]) -> StorageResult<StreamTrimState> {
    storage_types::storage_serde::from_bytes(blob)
        .map_err(|err| StorageError::internal(&format!("decode stream trim state failed: {err}")))
}

fn decode_marker_blob(blob: &[u8]) -> StorageResult<StreamTrimDueMarker> {
    storage_types::storage_serde::from_bytes(blob)
        .map_err(|err| StorageError::internal(&format!("decode stream trim marker failed: {err}")))
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
            PostgresStorageProvider::encode_stream_name(&StreamName::table_stream(
                &scope.table_name,
            ))
        }
        storage_provider::StreamTrimScopeKind::Item => {
            PostgresStorageProvider::encode_stream_name(&StreamName::from(scope.scope_id.clone()))
        }
    }
}

fn stream_item_id_from_hex(value: &str) -> StorageResult<StreamItemId> {
    value
        .parse()
        .map_err(|err| StorageError::internal(&format!("decode stream item id failed: {err}")))
}

fn stream_trim_row(row: &tokio_postgres::Row) -> StorageResult<StreamTrimRow> {
    let item_id: String = row
        .try_get("item_id")
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode trim item_id", err))?;
    let created_at: i64 = row.try_get("created_at").map_err(|err| {
        PostgresStorageProvider::map_postgres_error("decode trim created_at", err)
    })?;
    let stream_item_id = stream_item_id_from_hex(&item_id)?;
    Ok(StreamTrimRow {
        item_id,
        version: ItemStreamVersion::from(stream_item_id),
        created_at: TimestampMillis::from_timestamp(created_at),
    })
}

#[async_trait]
impl StreamDurationTrimBackend for PostgresStorageProvider {
    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        let limit = i64::try_from(limit).map_err(|err| {
            StorageError::validation(format!("stream trim marker page limit is too large: {err}"))
        })?;
        let client = self.acquire_client("list_due_stream_trim_markers").await?;
        let rows = client
            .query(
                r"SELECT marker_blob
                  FROM sys_stream_trim_due_markers
                  WHERE due_bucket <= $1
                  ORDER BY due_bucket ASC, scope_id ASC, policy_version ASC
                  LIMIT $2",
                &[&*due_before, &limit],
            )
            .await
            .map_err(|err| Self::map_postgres_error("list due stream trim markers", err))?;
        rows.into_iter()
            .map(|row| {
                let blob: Vec<u8> = row.try_get(0).map_err(|err| {
                    Self::map_postgres_error("decode stream trim marker blob", err)
                })?;
                decode_marker_blob(&blob)
            })
            .collect()
    }

    async fn load_stream_trim_state(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>> {
        let client = self.acquire_client("load_stream_trim_state").await?;
        let row = client
            .query_opt(
                "SELECT state_blob FROM sys_stream_trim_state WHERE scope_id = $1",
                &[&scope.scope_id],
            )
            .await
            .map_err(|err| Self::map_postgres_error("load stream trim state", err))?;
        row.map(|row| {
            let blob: Vec<u8> = row
                .try_get(0)
                .map_err(|err| Self::map_postgres_error("decode stream trim state blob", err))?;
            decode_state_blob(&blob)
        })
        .transpose()
    }

    async fn load_stream_trim_boundaries(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<StreamTrimScopeBoundaries> {
        let client = self.acquire_client("load_stream_trim_boundaries").await?;
        let stream_name = stream_name_for_scope(scope);
        let latest_item_id = match scope.kind {
            storage_provider::StreamTrimScopeKind::Table => None,
            storage_provider::StreamTrimScopeKind::Item => {
                let row = client
                    .query_opt(
                        "SELECT item_id FROM sys_stream_items WHERE stream_name = $1 ORDER BY \
                         item_id DESC LIMIT 1",
                        &[&stream_name],
                    )
                    .await
                    .map_err(|err| Self::map_postgres_error("latest stream item id", err))?;
                row.map(|row| {
                    let item_id: String = row.try_get(0).map_err(|err| {
                        Self::map_postgres_error("decode latest stream item id", err)
                    })?;
                    stream_item_id_from_hex(&item_id)
                })
                .transpose()?
            }
        };
        let protected_boundary = match scope.kind {
            storage_provider::StreamTrimScopeKind::Table => {
                let protected_floor: Option<StreamItemId> = None;
                if let Some(protected_floor) = protected_floor {
                    let floor = protected_floor.to_string();
                    let row = client
                        .query_opt(
                            r"SELECT table_stream_item_id FROM sys_stream_pointer_index
                              WHERE table_name = $1 AND system_stream_item_id >= $2
                              ORDER BY system_stream_item_id ASC
                              LIMIT 1",
                            &[&scope.table_name.to_string(), &floor],
                        )
                        .await
                        .map_err(|err| {
                            Self::map_postgres_error("protected table pointer boundary", err)
                        })?;
                    row.map(|row| {
                        let item_id: String = row.try_get(0).map_err(|err| {
                            Self::map_postgres_error("decode protected table pointer boundary", err)
                        })?;
                        stream_item_id_from_hex(&item_id)
                            .map(|item_id| StreamTrimBoundary { item_id })
                    })
                    .transpose()?
                } else {
                    None
                }
            }
            storage_provider::StreamTrimScopeKind::Item => None,
        };
        let retained_table_pointer_boundary = match scope.kind {
            storage_provider::StreamTrimScopeKind::Table => None,
            storage_provider::StreamTrimScopeKind::Item => {
                let row = client
                    .query_opt(
                        r"SELECT item_stream_version FROM sys_stream_pointer_index
                          WHERE item_stream_name = $1
                          ORDER BY item_stream_version ASC
                          LIMIT 1",
                        &[&scope.scope_id],
                    )
                    .await
                    .map_err(|err| {
                        Self::map_postgres_error("retained item pointer boundary", err)
                    })?;
                row.map(|row| -> StorageResult<StreamTrimBoundary> {
                    let version: String = row.try_get(0).map_err(|err| {
                        Self::map_postgres_error("decode retained item pointer boundary", err)
                    })?;
                    let version = version.parse::<u64>().map_err(|err| {
                        StorageError::internal(&format!(
                            "decode pointer item stream version failed: {err}"
                        ))
                    })?;
                    Ok(StreamTrimBoundary {
                        item_id: StreamItemId::from(ItemStreamVersion::new(version)),
                    })
                })
                .transpose()?
            }
        };
        Ok(StreamTrimScopeBoundaries {
            latest_item_id,
            protected_boundary,
            retained_table_pointer_boundary,
        })
    }

    async fn trim_table_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        trim_stream_page(self, request).await
    }

    async fn trim_item_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        trim_stream_page(self, request).await
    }

    async fn finish_stream_trim_marker(
        &self,
        marker: StreamTrimDueMarker,
        write: Option<StreamTrimStateWrite>,
    ) -> StorageResult<()> {
        let mut client = self.acquire_client("finish_stream_trim_marker").await?;
        let transaction = self
            .begin_transaction(
                &mut client,
                "finish_stream_trim_marker",
                "start finish stream trim transaction",
            )
            .await?;
        let due_bucket = *marker.due_bucket;
        let policy_version = i64::try_from(marker.policy_version).map_err(|err| {
            StorageError::validation(format!("stream trim policy version is too large: {err}"))
        })?;
        transaction
            .execute(
                "DELETE FROM sys_stream_trim_due_markers
                 WHERE due_bucket = $1 AND scope_id = $2 AND policy_version = $3",
                &[&due_bucket, &marker.scope.scope_id, &policy_version],
            )
            .await
            .map_err(|err| Self::map_postgres_write_error("finish stream trim marker", err))?;
        if let Some(write) = write {
            Self::write_stream_trim_state_with_client(&transaction, write).await?;
        }
        transaction.commit().await.map_err(|err| {
            Self::map_postgres_write_error("commit finish stream trim marker", err)
        })?;
        Ok(())
    }
}

async fn trim_stream_page(
    provider: &PostgresStorageProvider,
    request: StreamDurationTrimPageRequest,
) -> StorageResult<StreamDurationTrimPageResult> {
    let mut client = provider.acquire_client("trim_stream_page").await?;
    let transaction = provider
        .begin_transaction(
            &mut client,
            "trim_stream_page",
            "start stream trim transaction",
        )
        .await?;
    let stream_name = stream_name_for_scope(&request.scope);
    let page_limit = i64::try_from(request.page_limit).map_err(|err| {
        StorageError::validation(format!("stream trim page limit is too large: {err}"))
    })?;
    let rows = if let Some(max_deleted_id) = request.max_deleted_item_id {
        let max_deleted_id = max_deleted_id.to_string();
        transaction
            .query(
                r"SELECT item_id, created_at
                  FROM sys_stream_items
                  WHERE stream_name = $1 AND created_at <= $2 AND item_id <= $3
                  ORDER BY item_id ASC
                  LIMIT $4",
                &[
                    &stream_name,
                    &*request.cutoff_timestamp,
                    &max_deleted_id,
                    &page_limit,
                ],
            )
            .await
    } else {
        transaction
            .query(
                r"SELECT item_id, created_at
                  FROM sys_stream_items
                  WHERE stream_name = $1 AND created_at <= $2
                  ORDER BY item_id ASC
                  LIMIT $3",
                &[&stream_name, &*request.cutoff_timestamp, &page_limit],
            )
            .await
    }
    .map_err(|err| PostgresStorageProvider::map_postgres_error("stream rows to trim", err))?;

    let mut trim_rows = Vec::with_capacity(rows.len());
    for row in rows {
        trim_rows.push(stream_trim_row(&row)?);
    }
    let deleted_rows = trim_rows.len();
    for row in trim_rows {
        transaction
            .execute(
                "DELETE FROM sys_stream_items WHERE stream_name = $1 AND item_id = $2",
                &[&stream_name, &row.item_id],
            )
            .await
            .map_err(|err| {
                PostgresStorageProvider::map_postgres_write_error("trim stream item", err)
            })?;
        if matches!(
            request.scope.kind,
            storage_provider::StreamTrimScopeKind::Table
        ) {
            transaction
                .execute(
                    "DELETE FROM sys_stream_pointer_index
                     WHERE table_name = $1 AND table_stream_item_id = $2",
                    &[&request.scope.table_name.to_string(), &row.item_id],
                )
                .await
                .map_err(|err| {
                    PostgresStorageProvider::map_postgres_write_error(
                        "trim stream pointer index",
                        err,
                    )
                })?;
        }
    }
    let first_remaining = transaction
        .query_opt(
            "SELECT item_id, created_at FROM sys_stream_items
             WHERE stream_name = $1
             ORDER BY item_id ASC
             LIMIT 1",
            &[&stream_name],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_error("first remaining stream row", err)
        })?
        .map(|row| stream_trim_row(&row))
        .transpose()?;
    transaction.commit().await.map_err(|err| {
        PostgresStorageProvider::map_postgres_write_error("commit trim stream page", err)
    })?;
    Ok(StreamDurationTrimPageResult {
        deleted_rows,
        first_remaining_version: first_remaining.as_ref().map(|row| row.version),
        first_remaining_timestamp: first_remaining.map(|row| row.created_at),
    })
}
