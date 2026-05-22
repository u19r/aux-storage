use async_trait::async_trait;
use storage_types::{StreamName, TimestampMillis, UserStreamName};
use stream_provider::{
    CursorName, CursorPage, CursorPosition, StoredStreamPointer, Stream, StreamCursor,
    StreamDataType, StreamError, StreamPage, StreamProvider, StreamResult,
};
use uuid::Uuid;

use crate::backends::postgres::{PostgresStorageProvider, sql_statements};

#[async_trait]
impl StreamProvider for PostgresStorageProvider {
    async fn initialize_stream(&self) -> StreamResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
        client
            .batch_execute(sql_statements::create_stream_tables())
            .await
            .map_err(|err| Self::map_stream_error("initialize stream tables", err))?;
        client
            .batch_execute(sql_statements::create_stream_format_metadata_table())
            .await
            .map_err(|err| Self::map_stream_error("initialize stream format metadata", err))?;
        ensure_postgres_item_versioned_stream_format_metadata(&client)
            .await
            .map_err(StreamError::from)?;
        Ok(())
    }

    async fn create_stream(
        &self,
        stream_name: UserStreamName,
        ttl_seconds: Option<storage_types::DurationSeconds>,
        _partitioning_mode: ::stream_provider::StreamPartitioningMode,
    ) -> StreamResult<StreamName> {
        let internal_id: StreamName = Uuid::now_v7().to_string().into();
        let now = TimestampMillis::now();
        let ttl = ttl_seconds.map(|value| i64::from(*value));
        self.retry_postgres_stream_conflicts("create_stream", || {
            let stream_name = stream_name.clone();
            let internal_id = internal_id.clone();
            async move {
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
                let exists = client
                    .query_opt(
                        sql_statements::check_user_stream_exists(),
                        &[&stream_name.as_str()],
                    )
                    .await
                    .map_err(|err| Self::map_stream_error("check stream exists", err))?
                    .is_some();
                if exists {
                    return Err(StreamError::stream_already_exists(stream_name.to_string()));
                }

                let internal_id_value: String = (&internal_id).into();
                client
                    .execute(
                        sql_statements::insert_user_stream(),
                        &[
                            &stream_name.as_str(),
                            &internal_id_value,
                            &ttl,
                            &*now,
                            &*now,
                        ],
                    )
                    .await
                    .map_err(|err| Self::map_stream_error("insert stream", err))?;
                Ok(internal_id)
            }
        })
        .await
    }

    async fn delete_stream(&self, stream_name: UserStreamName) -> StreamResult<()> {
        self.retry_postgres_stream_conflicts("delete_stream", || {
            let stream_name = stream_name.clone();
            async move {
                let mut client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
                let txn = client.transaction().await.map_err(|err| {
                    Self::map_stream_error("start delete stream transaction", err)
                })?;

                let stream_id_row = txn
                    .query_opt(
                        sql_statements::get_stream_internal_id(),
                        &[&stream_name.as_str()],
                    )
                    .await
                    .map_err(|err| Self::map_stream_error("lookup stream id", err))?;
                let Some(stream_id_row) = stream_id_row else {
                    return Err(StreamError::stream_not_found(stream_name.to_string()));
                };
                let internal_id: String = stream_id_row
                    .try_get("internal_id")
                    .map_err(|err| Self::map_stream_error("decode stream id", err))?;
                let internal_stream = StreamName::from(internal_id.clone());
                let internal_stream_value = Self::encode_stream_name(&internal_stream);

                txn.execute(
                    sql_statements::delete_stream_cursors(),
                    &[&internal_stream_value],
                )
                .await
                .map_err(|err| Self::map_stream_error("delete stream cursors", err))?;
                txn.execute(
                    sql_statements::delete_stream_items(),
                    &[&internal_stream_value],
                )
                .await
                .map_err(|err| Self::map_stream_error("delete stream items", err))?;
                txn.execute(
                    sql_statements::delete_user_stream(),
                    &[&stream_name.as_str()],
                )
                .await
                .map_err(|err| Self::map_stream_error("delete stream metadata", err))?;
                txn.commit().await.map_err(|err| {
                    Self::map_stream_error("commit delete stream transaction", err)
                })?;
                Ok(())
            }
        })
        .await
    }

    async fn get_stream(&self, stream_name: UserStreamName) -> StreamResult<Option<Stream>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
        let row = client
            .query_opt(sql_statements::get_stream(), &[&stream_name.as_str()])
            .await
            .map_err(|err| Self::map_stream_error("get stream", err))?;
        let Some(row) = row else {
            return Ok(None);
        };

        let stream_name: String = row
            .try_get("stream_name")
            .map_err(|err| Self::map_stream_error("decode stream_name", err))?;
        let internal_id: String = row
            .try_get("internal_id")
            .map_err(|err| Self::map_stream_error("decode internal_id", err))?;
        let ttl_seconds: Option<i64> = row
            .try_get("ttl_seconds")
            .map_err(|err| Self::map_stream_error("decode ttl_seconds", err))?;
        let created_at: i64 = row
            .try_get("created_at")
            .map_err(|err| Self::map_stream_error("decode created_at", err))?;

        Ok(Some(Stream {
            name: stream_name.into(),
            internal_id: internal_id.into(),
            ttl_seconds: ttl_seconds
                .and_then(|seconds| u32::try_from(seconds).ok())
                .map(Into::into),
            partitioning_mode: ::stream_provider::StreamPartitioningMode::Single,
            created_at: created_at.into(),
        }))
    }

    async fn append_item(
        &self,
        stream_name: StreamName,
        item_data: &[u8],
        _partition_key: Option<&str>,
    ) -> StreamResult<storage_types::StreamItemId> {
        let item_id = storage_types::StreamItemId::from(Uuid::now_v7());
        let now = TimestampMillis::now();
        let stream_name_value = Self::encode_stream_name(&stream_name);
        let item_id_value = item_id.to_string();
        let payload = item_data.to_vec();
        self.retry_postgres_stream_conflicts("append_item", || {
            let stream_name_value = stream_name_value.clone();
            let item_id_value = item_id_value.clone();
            let payload = payload.clone();
            async move {
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
                client
                    .execute(
                        sql_statements::insert_stream_entry(),
                        &[
                            &stream_name_value,
                            &item_id_value,
                            &payload,
                            &*now,
                            &(StreamDataType::Text as i32),
                        ],
                    )
                    .await
                    .map_err(|err| Self::map_stream_error("append stream item", err))?;
                Ok(item_id)
            }
        })
        .await
    }

    async fn read_forward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<storage_types::StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        if limit == 0 || limit > 10_000 {
            return Err(StreamError::validation("Limit must be between 1 and 10000"));
        }
        let start = exclusive_start_key
            .map(|item_id| item_id.increment().to_string())
            .unwrap_or_else(|| storage_types::StreamItemId::from(Uuid::nil()).to_string());
        let stream_name_value = Self::encode_stream_name(&stream_name);
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
        let rows = client
            .query(
                sql_statements::read_stream_forward(),
                &[&stream_name_value, &start, &(i64::from(limit) + 1)],
            )
            .await
            .map_err(|err| Self::map_stream_error("read stream forward", err))?;

        let mut items = rows
            .iter()
            .map(|row| Self::parse_stream_item_row(row, &stream_name))
            .collect::<StreamResult<Vec<_>>>()?;
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
        exclusive_start_key: Option<storage_types::StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        if limit == 0 || limit > 1_000 {
            return Err(StreamError::validation("Limit must be between 1 and 1000"));
        }
        let start = exclusive_start_key
            .map(|item_id| item_id.to_string())
            .unwrap_or_else(|| storage_types::StreamItemId::from(Uuid::max()).to_string());
        let stream_name_value = Self::encode_stream_name(&stream_name);
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
        let rows = client
            .query(
                sql_statements::read_stream_backward(),
                &[&stream_name_value, &start, &(i64::from(limit) + 1)],
            )
            .await
            .map_err(|err| Self::map_stream_error("read stream backward", err))?;
        let mut items = rows
            .iter()
            .map(|row| Self::parse_stream_item_row(row, &stream_name))
            .collect::<StreamResult<Vec<_>>>()?;
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
        let stream_name_value = Self::encode_stream_name(&stream_name);
        self.retry_postgres_stream_conflicts("create_cursor", || {
            let stream_name_value = stream_name_value.clone();
            let cursor_name = cursor_name.clone();
            async move {
                let now = TimestampMillis::now();
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
                let exists = client
                    .query_opt(
                        sql_statements::check_cursor_exists(),
                        &[&cursor_name.as_str(), &stream_name_value],
                    )
                    .await
                    .map_err(|err| Self::map_stream_error("check cursor exists", err))?
                    .is_some();
                if exists {
                    return Err(StreamError::cursor_already_exists(cursor_name.to_string()));
                }

                let position_id = match position {
                    CursorPosition::Tail => {
                        let latest = client
                            .query_opt(
                                sql_statements::get_latest_stream_item(),
                                &[&stream_name_value],
                            )
                            .await
                            .map_err(|err| Self::map_stream_error("read tail item", err))?;
                        if let Some(row) = latest {
                            let id: String = row.try_get("item_id").map_err(|err| {
                                Self::map_stream_error("decode tail item id", err)
                            })?;
                            Self::parse_stream_item_id(&id, "item_id")?
                        } else {
                            storage_types::StreamItemId::from(Uuid::now_v7())
                        }
                    }
                    CursorPosition::Head => storage_types::StreamItemId::from(Uuid::nil()),
                };

                client
                    .execute(
                        sql_statements::insert_cursor(),
                        &[
                            &cursor_name.as_str(),
                            &stream_name_value,
                            &position_id.to_string(),
                            &*now,
                        ],
                    )
                    .await
                    .map_err(|err| Self::map_stream_error("insert cursor", err))?;
                Ok(())
            }
        })
        .await
    }

    async fn delete_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<()> {
        self.retry_postgres_stream_conflicts("delete_cursor", || {
            let stream_name_value = Self::encode_stream_name(&stream_name);
            let cursor_name = cursor_name.clone();
            async move {
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
                let affected = client
                    .execute(
                        sql_statements::delete_cursor(),
                        &[&cursor_name.as_str(), &stream_name_value],
                    )
                    .await
                    .map_err(|err| Self::map_stream_error("delete cursor", err))?;
                if affected == 0 {
                    return Err(StreamError::cursor_not_found(cursor_name.to_string()));
                }
                Ok(())
            }
        })
        .await
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
        let stream_name_value = Self::encode_stream_name(&stream_name);
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
        let cursor_row = client
            .query_opt(
                sql_statements::get_cursor_position(),
                &[&cursor_name.as_str(), &stream_name_value],
            )
            .await
            .map_err(|err| Self::map_stream_error("get cursor position", err))?;
        let Some(cursor_row) = cursor_row else {
            return Err(StreamError::cursor_not_found(cursor_name.to_string()));
        };
        let cursor_position_raw: String = cursor_row
            .try_get("position")
            .map_err(|err| Self::map_stream_error("decode cursor position", err))?;
        let cursor_position = Self::parse_stream_item_id(&cursor_position_raw, "position")?;

        let rows = client
            .query(
                sql_statements::read_stream_forward(),
                &[
                    &stream_name_value,
                    &cursor_position.to_string(),
                    &(i64::from(limit) + 1),
                ],
            )
            .await
            .map_err(|err| Self::map_stream_error("read from cursor", err))?;
        let mut items = rows
            .iter()
            .map(|row| Self::parse_stream_item_row(row, &stream_name))
            .collect::<StreamResult<Vec<_>>>()?;
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
        to_item_id: storage_types::StreamItemId,
    ) -> StreamResult<()> {
        self.retry_postgres_stream_conflicts("advance_cursor", || {
            let stream_name_value = Self::encode_stream_name(&stream_name);
            let cursor_name = cursor_name.clone();
            async move {
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
                let cursor_exists = client
                    .query_opt(
                        sql_statements::check_cursor_exists(),
                        &[&cursor_name.as_str(), &stream_name_value],
                    )
                    .await
                    .map_err(|err| Self::map_stream_error("check cursor exists", err))?
                    .is_some();
                if !cursor_exists {
                    return Err(StreamError::cursor_not_found(cursor_name.to_string()));
                }

                let item_exists = client
                    .query_opt(
                        sql_statements::check_stream_item_exists(),
                        &[&to_item_id.to_string(), &stream_name_value],
                    )
                    .await
                    .map_err(|err| Self::map_stream_error("check stream item exists", err))?
                    .is_some();
                if !item_exists {
                    return Err(StreamError::validation("Target item not found in stream"));
                }

                client
                    .execute(
                        sql_statements::advance_cursor_position(),
                        &[
                            &to_item_id.to_string(),
                            &cursor_name.as_str(),
                            &stream_name_value,
                        ],
                    )
                    .await
                    .map_err(|err| Self::map_stream_error("advance cursor", err))?;
                Ok(())
            }
        })
        .await
    }

    async fn get_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>> {
        let stream_name_value = Self::encode_stream_name(&stream_name);
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_stream_error("acquire postgres client", err))?;
        let row = client
            .query_opt(
                sql_statements::get_cursor(),
                &[&cursor_name.as_str(), &stream_name_value],
            )
            .await
            .map_err(|err| Self::map_stream_error("get cursor", err))?;
        let Some(row) = row else {
            return Ok(None);
        };

        let name: String = row
            .try_get("cursor_name")
            .map_err(|err| Self::map_stream_error("decode cursor name", err))?;
        let stream_name: String = row
            .try_get("stream_name")
            .map_err(|err| Self::map_stream_error("decode cursor stream_name", err))?;
        let position: String = row
            .try_get("position")
            .map_err(|err| Self::map_stream_error("decode cursor position", err))?;
        let created_at: i64 = row
            .try_get("created_at")
            .map_err(|err| Self::map_stream_error("decode cursor created_at", err))?;

        Ok(Some(StreamCursor {
            name: CursorName::new(&name),
            stream_name: Self::decode_stream_name(&stream_name),
            position: Self::parse_stream_item_id(&position, "position")?,
            created_at: created_at.into(),
        }))
    }

    async fn start_cleanup_task(&self, _interval_seconds: usize) -> StreamResult<()> {
        Ok(())
    }

    async fn stop_cleanup_task(&self) -> StreamResult<()> {
        Ok(())
    }

    async fn cleanup_expired_items(&self) -> StreamResult<u64> {
        Ok(0)
    }
}

async fn ensure_postgres_item_versioned_stream_format_metadata(
    client: &tokio_postgres::Client,
) -> storage_types::StorageResult<()> {
    let format_version = client
        .query_opt(
            sql_statements::get_stream_format_version(),
            &[&sql_statements::item_versioned_stream_format_key()],
        )
        .await
        .map_err(|err| {
            storage_types::StorageError::internal(&format!("read stream format metadata: {err}"))
        })?
        .map(|row| row.get::<_, i64>(0));

    match format_version {
        Some(version) if version == sql_statements::item_versioned_stream_format_version() => {
            reject_old_postgres_pointer_payloads(client).await
        }
        Some(version) => {
            tracing::warn!(
                backend = "postgres",
                reason = "incompatible_format_metadata",
                format_version = version,
                expected_format_version = sql_statements::item_versioned_stream_format_version(),
                "stream format startup rejected unsupported state"
            );
            Err(storage_types::StorageError::unsupported(&format!(
                "unsupported stream format metadata version {version}; expected item-versioned \
                 stream format version {}",
                sql_statements::item_versioned_stream_format_version()
            )))
        }
        None => {
            let stream_items = client
                .query_one(sql_statements::count_stream_items(), &[])
                .await
                .map_err(|err| {
                    storage_types::StorageError::internal(&format!("count stream items: {err}"))
                })?
                .get::<_, i64>(0);
            if stream_items > 0 {
                tracing::warn!(
                    backend = "postgres",
                    reason = "missing_format_metadata",
                    stream_items,
                    "stream format startup rejected unsupported state"
                );
                return Err(storage_types::StorageError::unsupported(
                    "item-versioned streams require empty stream tables or stream format \
                     metadata; in-place upgrade from old stream rows is unsupported",
                ));
            }

            client
                .execute(
                    sql_statements::upsert_stream_format_version(),
                    &[
                        &sql_statements::item_versioned_stream_format_key(),
                        &sql_statements::item_versioned_stream_format_version(),
                    ],
                )
                .await
                .map_err(|err| {
                    storage_types::StorageError::internal(&format!(
                        "write stream format metadata: {err}"
                    ))
                })?;
            Ok(())
        }
    }
}

async fn reject_old_postgres_pointer_payloads(
    client: &tokio_postgres::Client,
) -> storage_types::StorageResult<()> {
    let rows = client
        .query(
            sql_statements::list_stream_pointer_payloads(),
            &[&(StreamDataType::StreamPointer as i32)],
        )
        .await
        .map_err(|err| {
            storage_types::StorageError::internal(&format!("read stream pointer payloads: {err}"))
        })?;

    for row in rows {
        let stream_name = row.get::<_, String>(0);
        let item_id = row.get::<_, String>(1);
        let data = row.get::<_, Vec<u8>>(2);
        if let Err(err) = storage_types::storage_serde::from_bytes::<StoredStreamPointer>(&data) {
            tracing::warn!(
                backend = "postgres",
                reason = "old_format_pointer_payload",
                stream_name,
                item_id,
                "stream format startup rejected unsupported state"
            );
            return Err(storage_types::StorageError::unsupported(&format!(
                "item-versioned streams cannot start with old-format stream pointer payload at \
                 stream {stream_name} item {item_id}: {err}"
            )));
        }
    }

    Ok(())
}
