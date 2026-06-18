use std::collections::HashMap;

use async_trait::async_trait;
use storage_types::{
    DurationSeconds, StorageEnum, StorageError, StorageResult, StreamItemId, StreamName,
    TimestampMillis, UserStreamName,
};
use stream_provider::{
    CursorName, CursorPage, CursorPosition, StoredStreamPointer, Stream, StreamCursor,
    StreamDataType, StreamError, StreamItem, StreamPage, StreamProvider, StreamResult,
};
use turso::Value as TursoValue;
use uuid::Uuid;

use crate::backends::turso::{
    provider::{
        TursoSqlConnection, TursoStorageProvider, row_required_i64, row_required_text, value_to_i64,
    },
    sql_statements,
};

const ITEM_VERSIONED_STREAM_FORMAT_KEY: &str = "item_versioned_stream";
const ITEM_VERSIONED_STREAM_FORMAT_VERSION: i64 = 1;
const MARKER_STREAM_ALREADY_EXISTS: &str = "turso_stream_already_exists";
const MARKER_STREAM_NOT_FOUND: &str = "turso_stream_not_found";
const MARKER_CURSOR_ALREADY_EXISTS: &str = "turso_cursor_already_exists";
const MARKER_CURSOR_NOT_FOUND: &str = "turso_cursor_not_found";
const MARKER_CURSOR_TARGET_ITEM_NOT_FOUND: &str = "turso_cursor_target_item_not_found";

#[async_trait]
impl StreamProvider for TursoStorageProvider {
    async fn initialize_stream(&self) -> StreamResult<()> {
        let _ddl_guard = self.ddl_lock.lock().await;
        let this = self.clone();
        self.with_exclusive_transaction(true, move |conn| {
            let this = this.clone();
            Box::pin(async move {
                let _ = this
                    .execute(
                        conn,
                        sql_statements::create_user_streams_table(),
                        Vec::new(),
                    )
                    .await?;
                let _ = this
                    .execute(
                        conn,
                        sql_statements::create_stream_items_table(),
                        Vec::new(),
                    )
                    .await?;
                let _ = this
                    .execute(
                        conn,
                        sql_statements::create_stream_cursors_table(),
                        Vec::new(),
                    )
                    .await?;
                let _ = this
                    .execute(
                        conn,
                        sql_statements::create_change_index_table(),
                        Vec::new(),
                    )
                    .await?;
                let _ = this
                    .execute(
                        conn,
                        sql_statements::create_stream_format_metadata_table(),
                        Vec::new(),
                    )
                    .await?;
                this.ensure_item_versioned_stream_format_metadata(conn)
                    .await?;
                let _ = this
                    .execute(
                        conn,
                        sql_statements::create_change_index_created_at_index(),
                        Vec::new(),
                    )
                    .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| map_stream_storage_error("initialize_stream", error))
    }

    async fn create_stream(
        &self,
        stream_name: UserStreamName,
        ttl_seconds: Option<DurationSeconds>,
        _partitioning_mode: ::stream_provider::StreamPartitioningMode,
    ) -> StreamResult<StreamName> {
        let stream_name_for_error = stream_name.clone();
        let internal_id: StreamName = Uuid::now_v7().to_string().into();
        let now = TimestampMillis::now();
        let this = self.clone();

        let result = self
            .with_transaction(true, move |conn| {
                let this = this.clone();
                let stream_name = stream_name.clone();
                let internal_id = internal_id.clone();
                Box::pin(async move {
                    let stream_name_value = stream_name.to_string();
                    let internal_id_value: String = (&internal_id).into();
                    let ttl_value = ttl_seconds.as_ref().map_or(TursoValue::Null, |ttl| {
                        TursoValue::Integer(i64::from(**ttl))
                    });

                    let affected = this
                        .execute(
                            conn,
                            sql_statements::insert_user_stream(),
                            vec![
                                TursoValue::Text(stream_name_value),
                                TursoValue::Text(internal_id_value),
                                ttl_value,
                                TursoValue::Integer(*now),
                                TursoValue::Integer(*now),
                            ],
                        )
                        .await?;

                    if affected == 0 {
                        return Err(marker_error(MARKER_STREAM_ALREADY_EXISTS));
                    }

                    Ok(internal_id)
                })
            })
            .await;

        match result {
            Ok(stream_id) => Ok(stream_id),
            Err(error) if is_validation_marker(&error, MARKER_STREAM_ALREADY_EXISTS) => Err(
                StreamError::stream_already_exists(stream_name_for_error.to_string()),
            ),
            Err(error) => Err(map_stream_storage_error("create_stream", error)),
        }
    }

    async fn delete_stream(&self, stream_name: UserStreamName) -> StreamResult<()> {
        let stream_name_for_error = stream_name.clone();
        let this = self.clone();
        let result = self
            .with_transaction(true, move |conn| {
                let this = this.clone();
                let stream_name = stream_name.clone();
                Box::pin(async move {
                    let stream_rows = this
                        .query_rows(
                            conn,
                            sql_statements::get_stream_internal_id(),
                            vec![TursoValue::Text(stream_name.to_string())],
                        )
                        .await?;

                    let Some(row) = stream_rows.first() else {
                        return Err(marker_error(MARKER_STREAM_NOT_FOUND));
                    };

                    let internal_id = row_required_text(row, "internal_id")?;

                    let _ = this
                        .execute(
                            conn,
                            sql_statements::delete_stream_cursors(),
                            vec![TursoValue::Text(internal_id.clone())],
                        )
                        .await?;
                    let _ = this
                        .execute(
                            conn,
                            sql_statements::delete_stream_items(),
                            vec![TursoValue::Text(internal_id)],
                        )
                        .await?;
                    let _ = this
                        .execute(
                            conn,
                            sql_statements::delete_user_stream(),
                            vec![TursoValue::Text(stream_name.to_string())],
                        )
                        .await?;

                    Ok(())
                })
            })
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(error) if is_validation_marker(&error, MARKER_STREAM_NOT_FOUND) => Err(
                StreamError::stream_not_found(stream_name_for_error.to_string()),
            ),
            Err(error) => Err(map_stream_storage_error("delete_stream", error)),
        }
    }

    async fn get_stream(&self, stream_name: UserStreamName) -> StreamResult<Option<Stream>> {
        let conn = self
            .connect()
            .await
            .map_err(|error| map_stream_storage_error("get_stream connect", error))?;

        let rows = self
            .query_rows(
                &conn,
                sql_statements::get_stream(),
                vec![TursoValue::Text(stream_name.to_string())],
            )
            .await
            .map_err(|error| map_stream_storage_error("get_stream query", error))?;

        let Some(row) = rows.first() else {
            return Ok(None);
        };

        let name = row_required_text(row, "stream_name")
            .map_err(|error| map_stream_storage_error("decode stream_name", error))?;
        let internal_id = row_required_text(row, "internal_id")
            .map_err(|error| map_stream_storage_error("decode internal_id", error))?;
        let created_at = row_required_i64(row, "created_at")
            .map_err(|error| map_stream_storage_error("decode created_at", error))?;

        let ttl_seconds = match row.get("ttl_seconds") {
            None | Some(TursoValue::Null) => None,
            Some(value) => {
                let raw = value_to_i64(value)
                    .map_err(|error| map_stream_storage_error("decode ttl_seconds", error))?;
                u32::try_from(raw).ok().map(Into::into)
            }
        };

        Ok(Some(Stream {
            name: name.into(),
            internal_id: internal_id.into(),
            ttl_seconds,
            partitioning_mode: ::stream_provider::StreamPartitioningMode::Single,
            created_at: created_at.into(),
        }))
    }

    async fn append_item(
        &self,
        stream_name: StreamName,
        item_data: &[u8],
        _partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId> {
        let item_id = StreamItemId::from(Uuid::now_v7());
        let created_at = TimestampMillis::now();
        let stream_name_value: String = (&stream_name).into();
        let payload = item_data.to_vec();
        let this = self.clone();

        self.with_transaction(true, move |conn| {
            let this = this.clone();
            let item_id = item_id;
            let payload = payload.clone();
            let stream_name_value = stream_name_value.clone();
            Box::pin(async move {
                let _ = this
                    .execute(
                        conn,
                        sql_statements::insert_stream_entry(),
                        vec![
                            TursoValue::Text(stream_name_value),
                            TursoValue::Text(item_id.to_string()),
                            TursoValue::Blob(payload),
                            TursoValue::Integer(*created_at),
                            TursoValue::Integer(StreamDataType::Text as i64),
                        ],
                    )
                    .await?;

                Ok(item_id)
            })
        })
        .await
        .map_err(|error| map_stream_storage_error("append_item", error))
    }

    async fn read_forward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        if limit == 0 || limit > 10_000 {
            return Err(StreamError::validation("Limit must be between 1 and 10000"));
        }

        let conn = self
            .connect()
            .await
            .map_err(|error| map_stream_storage_error("read_forward connect", error))?;

        let stream_name_value: String = (&stream_name).into();
        let start_item_id = exclusive_start_key.unwrap_or_else(|| StreamItemId::from(Uuid::nil()));

        let rows = self
            .query_rows(
                &conn,
                sql_statements::read_stream_forward(),
                vec![
                    TursoValue::Text(stream_name_value.clone()),
                    TursoValue::Text(stream_name_value),
                    TursoValue::Text(start_item_id.to_string()),
                    TursoValue::Integer(i64::from(limit + 1)),
                ],
            )
            .await
            .map_err(|error| map_stream_storage_error("read_forward query", error))?;
        let mut items = parse_stream_items(rows)?;

        let has_more = items.len() > limit as usize;
        if has_more {
            items.truncate(limit as usize);
        }

        let last_evaluated_key = items.last().map(|item| item.id);

        Ok(StreamPage {
            items,
            last_evaluated_key,
            has_more,
        })
    }

    async fn read_backward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        if limit == 0 || limit > 1_000 {
            return Err(StreamError::validation("Limit must be between 1 and 1000"));
        }

        let conn = self
            .connect()
            .await
            .map_err(|error| map_stream_storage_error("read_backward connect", error))?;

        let stream_name_value: String = (&stream_name).into();
        let start_item_id = exclusive_start_key.unwrap_or_else(|| StreamItemId::from(Uuid::max()));

        let rows = self
            .query_rows(
                &conn,
                sql_statements::read_stream_backward(),
                vec![
                    TursoValue::Text(stream_name_value.clone()),
                    TursoValue::Text(stream_name_value),
                    TursoValue::Text(start_item_id.to_string()),
                    TursoValue::Integer(i64::from(limit + 1)),
                ],
            )
            .await
            .map_err(|error| map_stream_storage_error("read_backward query", error))?;
        let mut items = parse_stream_items(rows)?;

        let has_more = items.len() > limit as usize;
        if has_more {
            items.truncate(limit as usize);
        }

        let last_evaluated_key = items.last().map(|item| item.id);

        Ok(StreamPage {
            items,
            last_evaluated_key,
            has_more,
        })
    }

    async fn create_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        position: CursorPosition,
    ) -> StreamResult<()> {
        let cursor_name_for_error = cursor_name.clone();
        let stream_name_value: String = (&stream_name).into();
        let created_at = TimestampMillis::now();
        let this = self.clone();

        let result = self
            .with_transaction(true, move |conn| {
                let this = this.clone();
                let stream_name_value = stream_name_value.clone();
                let cursor_name = cursor_name.clone();
                Box::pin(async move {
                    let cursor_rows = this
                        .query_rows(
                            conn,
                            sql_statements::check_cursor_exists(),
                            vec![
                                TursoValue::Text(cursor_name.to_string()),
                                TursoValue::Text(stream_name_value.clone()),
                            ],
                        )
                        .await?;

                    if !cursor_rows.is_empty() {
                        return Err(marker_error(MARKER_CURSOR_ALREADY_EXISTS));
                    }

                    let position_id = match position {
                        CursorPosition::Tail => {
                            let latest_rows = this
                                .query_rows(
                                    conn,
                                    sql_statements::read_stream_backward(),
                                    vec![
                                        TursoValue::Text(stream_name_value.clone()),
                                        TursoValue::Text(stream_name_value.clone()),
                                        TursoValue::Text(
                                            StreamItemId::from(Uuid::max()).to_string(),
                                        ),
                                        TursoValue::Integer(1),
                                    ],
                                )
                                .await?;

                            let latest_items = parse_stream_items(latest_rows)
                                .map_err(|error| StorageError::internal(&error.to_string()))?;
                            match latest_items.first() {
                                Some(item) => item.id,
                                None => StreamItemId::from(Uuid::now_v7()),
                            }
                        }
                        CursorPosition::Head => StreamItemId::from(Uuid::nil()),
                    };

                    let _ = this
                        .execute(
                            conn,
                            sql_statements::insert_cursor(),
                            vec![
                                TursoValue::Text(cursor_name.to_string()),
                                TursoValue::Text(stream_name_value),
                                TursoValue::Text(position_id.to_string()),
                                TursoValue::Integer(*created_at),
                            ],
                        )
                        .await?;

                    Ok(())
                })
            })
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(error) if is_validation_marker(&error, MARKER_CURSOR_ALREADY_EXISTS) => Err(
                StreamError::cursor_already_exists(cursor_name_for_error.to_string()),
            ),
            Err(error) => Err(map_stream_storage_error("create_cursor", error)),
        }
    }

    async fn delete_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<()> {
        let cursor_name_for_error = cursor_name.clone();
        let stream_name_value: String = (&stream_name).into();
        let this = self.clone();

        let result = self
            .with_transaction(true, move |conn| {
                let this = this.clone();
                let stream_name_value = stream_name_value.clone();
                let cursor_name = cursor_name.clone();
                Box::pin(async move {
                    let affected = this
                        .execute(
                            conn,
                            sql_statements::delete_cursor(),
                            vec![
                                TursoValue::Text(cursor_name.to_string()),
                                TursoValue::Text(stream_name_value),
                            ],
                        )
                        .await?;

                    if affected == 0 {
                        return Err(marker_error(MARKER_CURSOR_NOT_FOUND));
                    }

                    Ok(())
                })
            })
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(error) if is_validation_marker(&error, MARKER_CURSOR_NOT_FOUND) => Err(
                StreamError::cursor_not_found(cursor_name_for_error.to_string()),
            ),
            Err(error) => Err(map_stream_storage_error("delete_cursor", error)),
        }
    }

    async fn read_from_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        limit: u32,
    ) -> StreamResult<CursorPage> {
        if limit == 0 || limit > 1_000 {
            return Err(StreamError::validation("Limit must be between 1 and 1000"));
        }

        let conn = self
            .connect()
            .await
            .map_err(|error| map_stream_storage_error("read_from_cursor connect", error))?;

        let stream_name_value: String = (&stream_name).into();
        let cursor_rows = self
            .query_rows(
                &conn,
                sql_statements::get_cursor_position(),
                vec![
                    TursoValue::Text(cursor_name.to_string()),
                    TursoValue::Text(stream_name_value.clone()),
                ],
            )
            .await
            .map_err(|error| map_stream_storage_error("read_from_cursor lookup", error))?;

        let Some(cursor_row) = cursor_rows.first() else {
            return Err(StreamError::cursor_not_found(cursor_name.to_string()));
        };

        let cursor_position_text = row_required_text(cursor_row, "position")
            .map_err(|error| map_stream_storage_error("decode cursor position", error))?;
        let cursor_position = cursor_position_text
            .parse::<StreamItemId>()
            .map_err(|_| StreamError::validation("Invalid cursor position ID"))?;

        let rows = self
            .query_rows(
                &conn,
                sql_statements::read_stream_forward(),
                vec![
                    TursoValue::Text(stream_name_value.clone()),
                    TursoValue::Text(stream_name_value),
                    TursoValue::Text(cursor_position.to_string()),
                    TursoValue::Integer(i64::from(limit + 1)),
                ],
            )
            .await
            .map_err(|error| map_stream_storage_error("read_from_cursor query", error))?;
        let mut items = parse_stream_items(rows)?;

        let has_more = items.len() > limit as usize;
        if has_more {
            items.truncate(limit as usize);
        }

        Ok(CursorPage {
            items,
            cursor_position,
            has_more,
        })
    }

    async fn advance_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        to_item_id: StreamItemId,
    ) -> StreamResult<()> {
        let cursor_name_for_error = cursor_name.clone();
        let stream_name_value: String = (&stream_name).into();
        let this = self.clone();

        let result = self
            .with_transaction(true, move |conn| {
                let this = this.clone();
                let stream_name_value = stream_name_value.clone();
                let cursor_name = cursor_name.clone();
                Box::pin(async move {
                    let cursor_rows = this
                        .query_rows(
                            conn,
                            sql_statements::check_cursor_exists(),
                            vec![
                                TursoValue::Text(cursor_name.to_string()),
                                TursoValue::Text(stream_name_value.clone()),
                            ],
                        )
                        .await?;
                    if cursor_rows.is_empty() {
                        return Err(marker_error(MARKER_CURSOR_NOT_FOUND));
                    }

                    let item_rows = this
                        .query_rows(
                            conn,
                            sql_statements::check_stream_item_exists(),
                            vec![
                                TursoValue::Text(to_item_id.to_string()),
                                TursoValue::Text(stream_name_value.clone()),
                            ],
                        )
                        .await?;
                    if item_rows.is_empty() {
                        return Err(marker_error(MARKER_CURSOR_TARGET_ITEM_NOT_FOUND));
                    }

                    let _ = this
                        .execute(
                            conn,
                            sql_statements::advance_cursor_position(),
                            vec![
                                TursoValue::Text(to_item_id.to_string()),
                                TursoValue::Text(cursor_name.to_string()),
                                TursoValue::Text(stream_name_value),
                            ],
                        )
                        .await?;

                    Ok(())
                })
            })
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(error) if is_validation_marker(&error, MARKER_CURSOR_NOT_FOUND) => Err(
                StreamError::cursor_not_found(cursor_name_for_error.to_string()),
            ),
            Err(error) if is_validation_marker(&error, MARKER_CURSOR_TARGET_ITEM_NOT_FOUND) => {
                Err(StreamError::validation("Target item not found in stream"))
            }
            Err(error) => Err(map_stream_storage_error("advance_cursor", error)),
        }
    }

    async fn get_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>> {
        let conn = self
            .connect()
            .await
            .map_err(|error| map_stream_storage_error("get_cursor connect", error))?;

        let stream_name_value: String = (&stream_name).into();
        let rows = self
            .query_rows(
                &conn,
                sql_statements::get_cursor(),
                vec![
                    TursoValue::Text(stream_name_value.clone()),
                    TursoValue::Text(cursor_name.to_string()),
                    TursoValue::Text(stream_name_value),
                ],
            )
            .await
            .map_err(|error| map_stream_storage_error("get_cursor query", error))?;

        let Some(row) = rows.first() else {
            return Ok(None);
        };

        let name = row_required_text(row, "cursor_name")
            .map_err(|error| map_stream_storage_error("decode cursor_name", error))?;
        let stream_name = row_required_text(row, "stream_name")
            .map_err(|error| map_stream_storage_error("decode stream_name", error))?;
        let position = row_required_text(row, "position")
            .map_err(|error| map_stream_storage_error("decode position", error))?;
        let created_at = row_required_i64(row, "created_at")
            .map_err(|error| map_stream_storage_error("decode created_at", error))?;

        let position = position
            .parse::<StreamItemId>()
            .map_err(|_| StreamError::validation("Invalid cursor position ID"))?;

        Ok(Some(StreamCursor {
            name: CursorName::new(&name),
            stream_name: stream_name.into(),
            position,
            created_at: created_at.into(),
        }))
    }

    async fn start_cleanup_task(&self, _parallelism: usize) -> StreamResult<()> {
        Ok(())
    }

    async fn stop_cleanup_task(&self) -> StreamResult<()> {
        Ok(())
    }

    async fn cleanup_expired_items(&self) -> StreamResult<u64> {
        Ok(0)
    }
}

impl TursoStorageProvider {
    async fn ensure_item_versioned_stream_format_metadata<C>(
        &self,
        conn: &C,
    ) -> storage_types::StorageResult<()>
    where
        C: TursoSqlConnection + Sync,
    {
        let rows = self
            .query_rows(
                conn,
                sql_statements::get_stream_format_version(),
                vec![TursoValue::Text(
                    ITEM_VERSIONED_STREAM_FORMAT_KEY.to_string(),
                )],
            )
            .await?;
        let format_version = rows
            .first()
            .map(|row| row_required_i64(row, "format_version"))
            .transpose()?;

        match format_version {
            Some(ITEM_VERSIONED_STREAM_FORMAT_VERSION) => {
                if !self.stream_items_exist(conn).await? {
                    Ok(())
                } else {
                    self.reject_old_pointer_payloads(conn).await
                }
            }
            Some(version) => {
                tracing::warn!(
                    backend = "turso",
                    reason = "incompatible_format_metadata",
                    format_version = version,
                    expected_format_version = ITEM_VERSIONED_STREAM_FORMAT_VERSION,
                    "stream format startup rejected unsupported state"
                );
                Err(StorageError::unsupported(&format!(
                    "unsupported stream format metadata version {version}; expected \
                     item-versioned stream format version {ITEM_VERSIONED_STREAM_FORMAT_VERSION}"
                )))
            }
            None => {
                if self.stream_items_exist(conn).await? {
                    tracing::warn!(
                        backend = "turso",
                        reason = "missing_format_metadata",
                        "stream format startup rejected unsupported state"
                    );
                    return Err(StorageError::unsupported(
                        "item-versioned streams require empty stream tables or stream format \
                         metadata; in-place upgrade from old stream rows is unsupported",
                    ));
                }

                let _ = self
                    .execute(
                        conn,
                        sql_statements::upsert_stream_format_version(),
                        vec![
                            TursoValue::Text(ITEM_VERSIONED_STREAM_FORMAT_KEY.to_string()),
                            TursoValue::Integer(ITEM_VERSIONED_STREAM_FORMAT_VERSION),
                        ],
                    )
                    .await?;
                Ok(())
            }
        }
    }

    async fn stream_items_exist<C>(&self, conn: &C) -> StorageResult<bool>
    where C: TursoSqlConnection + Sync {
        self.query_rows(conn, sql_statements::stream_items_exist(), Vec::new())
            .await
            .map(|rows| !rows.is_empty())
    }

    async fn reject_old_pointer_payloads<C>(&self, conn: &C) -> StorageResult<()>
    where C: TursoSqlConnection + Sync {
        let rows = self
            .query_rows(
                conn,
                sql_statements::list_stream_pointer_payloads(),
                vec![TursoValue::Integer(StreamDataType::StreamPointer as i64)],
            )
            .await?;

        for row in rows {
            let stream_name = row_required_text(&row, "stream_name")?;
            let item_id = row_required_text(&row, "item_id")?;
            let data = row_required_blob(&row, "data")?;
            if let Err(err) = storage_types::storage_serde::from_bytes::<StoredStreamPointer>(&data)
            {
                tracing::warn!(
                    backend = "turso",
                    reason = "old_format_pointer_payload",
                    stream_name,
                    item_id,
                    "stream format startup rejected unsupported state"
                );
                return Err(StorageError::unsupported(&format!(
                    "item-versioned streams cannot start with old-format stream pointer payload \
                     at stream {stream_name} item {item_id}: {err}"
                )));
            }
        }

        Ok(())
    }
}

fn parse_stream_item_row(row: &HashMap<String, TursoValue>) -> StreamResult<StreamItem> {
    let id = row_required_text(row, "item_id")
        .map_err(|error| map_stream_storage_error("decode item_id", error))?;
    let stream_name = row_required_text(row, "stream_name")
        .map_err(|error| map_stream_storage_error("decode stream_name", error))?;
    let created_at = row_required_i64(row, "created_at")
        .map_err(|error| map_stream_storage_error("decode created_at", error))?;
    let data_type_raw = row_required_i64(row, "data_type")
        .map_err(|error| map_stream_storage_error("decode data_type", error))?;

    let data_type = i32::try_from(data_type_raw)
        .ok()
        .map(StreamDataType::from)
        .unwrap_or(StreamDataType::Text);

    let data = match row.get("data") {
        Some(TursoValue::Blob(value)) => value.clone(),
        Some(TursoValue::Text(value)) => value.as_bytes().to_vec(),
        Some(TursoValue::Null) => Vec::new(),
        Some(TursoValue::Integer(_)) | Some(TursoValue::Real(_)) => {
            return Err(StreamError::validation("Invalid stream item data type"));
        }
        None => return Err(StreamError::internal("Missing stream item data column")),
    };

    let parsed_id = id
        .parse::<StreamItemId>()
        .map_err(|_| StreamError::validation("Invalid stream item ID"))?;

    Ok(StreamItem {
        id: parsed_id,
        stream_name: Some(stream_name.into()),
        data,
        data_type,
        created_at: created_at.into(),
    })
}

fn parse_stream_items(rows: Vec<HashMap<String, TursoValue>>) -> StreamResult<Vec<StreamItem>> {
    rows.iter().map(parse_stream_item_row).collect()
}

fn row_required_blob(row: &HashMap<String, TursoValue>, column: &str) -> StorageResult<Vec<u8>> {
    match row.get(column) {
        Some(TursoValue::Blob(value)) => Ok(value.clone()),
        Some(TursoValue::Text(value)) => Ok(value.as_bytes().to_vec()),
        Some(TursoValue::Null) => Ok(Vec::new()),
        Some(TursoValue::Integer(_)) | Some(TursoValue::Real(_)) => Err(StorageError::internal(
            "stream pointer payload is not bytes",
        )),
        None => Err(StorageError::internal(&format!(
            "missing column '{column}'"
        ))),
    }
}

fn marker_error(marker: &str) -> StorageError {
    StorageError::validation(marker)
}

fn is_validation_marker(error: &StorageError, marker: &str) -> bool {
    matches!(error.as_ref(), StorageEnum::Validation { message } if message == marker)
}

fn map_stream_storage_error(context: &str, error: StorageError) -> StreamError {
    if matches!(error.as_ref(), StorageEnum::Unsupported { .. }) {
        return StreamError::from(error);
    }
    StreamError::internal(format!("{context} failed: {error}"))
}
