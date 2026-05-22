use ::stream_provider::{
    CursorName, CursorPage, CursorPosition, StoredStreamPointer, Stream, StreamCursor,
    StreamDataType, StreamError, StreamItem, StreamPage, StreamPartitioningMode, StreamProvider,
    StreamResult,
};
use async_trait::async_trait;
use rusqlite::OptionalExtension as _;
use storage_types::{DurationSeconds, StreamItemId, StreamName, TimestampMillis, UserStreamName};
use uuid::Uuid;

use crate::{
    backends::sqlite::SQLiteStorageProvider,
    sql_statements::{self, insert_stream_entry},
    transaction_manager::with_transaction,
    utils::{call_sqlite, call_sqlite_raw},
};

const ITEM_VERSIONED_STREAM_FORMAT_VERSION: i64 = 1;

#[async_trait]
impl StreamProvider for SQLiteStorageProvider {
    async fn initialize_stream(&self) -> StreamResult<()> {
        let result = call_sqlite(
            &self.connection,
            move |conn: &mut rusqlite::Connection| -> storage_types::StorageResult<()> {
                let (sql, params) = sql_statements::create_user_streams_table();
                conn.execute(sql, params)
                    .map_err(crate::error_handler::map_sqlite_error)?;

                let (sql, params) = sql_statements::create_stream_items_table();
                conn.execute(sql, params)
                    .map_err(crate::error_handler::map_sqlite_error)?;

                let (sql, params) = sql_statements::create_stream_cursors_table();
                conn.execute(sql, params)
                    .map_err(crate::error_handler::map_sqlite_error)?;

                let (sql, params) = sql_statements::create_stream_format_metadata_table();
                conn.execute(sql, params)
                    .map_err(crate::error_handler::map_sqlite_error)?;

                ensure_sqlite_ordered_stream_format_metadata(conn)?;

                let (sql, params) = sql_statements::create_stream_items_internal_time_index();
                conn.execute(sql, params)
                    .map_err(crate::error_handler::map_sqlite_error)?;

                let (sql, params) = sql_statements::create_stream_cursors_internal_index();
                conn.execute(sql, params)
                    .map_err(crate::error_handler::map_sqlite_error)?;

                Ok(())
            },
        )
        .await;

        result.map_err(StreamError::from)
    }

    async fn create_stream(
        &self,
        user_stream_name: UserStreamName,
        ttl_seconds: Option<DurationSeconds>,
        _partitioning_mode: StreamPartitioningMode,
    ) -> StreamResult<StreamName> {
        let internal_id: StreamName = Uuid::now_v7().to_string().into();
        let internal_id_for_return = internal_id.clone();
        let created_at = TimestampMillis::now();
        let stream_name_for_error = user_stream_name.to_string();

        let result = call_sqlite_raw(
            &self.connection,
            move |conn: &mut rusqlite::Connection| -> Result<bool, rusqlite::Error> {
                let (sql, params) = sql_statements::check_stream_exists(&user_stream_name);
                let exists: bool = conn
                    .query_row(sql, params, |_| Ok(true))
                    .optional()?
                    .is_some();

                if exists {
                    return Ok(false);
                }

                let (sql, params) = sql_statements::insert_new_stream(
                    &user_stream_name,
                    &internal_id,
                    ttl_seconds.as_ref(),
                    &created_at,
                    &created_at,
                );
                conn.execute(sql, params)?;

                Ok(true)
            },
        )
        .await;

        match result {
            Ok(true) => Ok(StreamName::new(&internal_id_for_return)),
            Ok(false) => Err(StreamError::stream_already_exists(stream_name_for_error)),
            Err(e) => Err(StreamError::internal(format!("create stream failed: {e}"))),
        }
    }

    async fn delete_stream(&self, user_stream_name: UserStreamName) -> StreamResult<()> {
        let stream_name_for_error = user_stream_name.clone();

        let result = with_transaction(&self.connection, move |sqlite| {
            let (sql, params) = sql_statements::get_stream_internal_id(&user_stream_name);
            let mut stmt = sqlite
                .prepare(sql)
                .map_err(crate::error_handler::map_sqlite_error)?;

            let stream_name: StreamName = stmt
                .query_row(params, |row| {
                    let internal_id: String = row.get(0)?;
                    Ok(internal_id)
                })
                .map_err(crate::error_handler::map_sqlite_error)?
                .into();

            drop(stmt);

            let (sql, params) = sql_statements::delete_stream_cursors(&stream_name);
            sqlite
                .execute(sql, params)
                .map_err(crate::error_handler::map_sqlite_error)?;

            let (sql, params) = sql_statements::delete_stream_items(&stream_name);
            sqlite
                .execute(sql, params)
                .map_err(crate::error_handler::map_sqlite_error)?;

            let (sql, params) = sql_statements::delete_user_stream(&user_stream_name);
            let affected = sqlite
                .execute(sql, params)
                .map_err(crate::error_handler::map_sqlite_error)?;

            Ok::<bool, storage_types::StorageError>(affected > 0)
        })
        .await;

        match result {
            Ok(true) => Ok(()),
            Ok(false) => Err(StreamError::stream_not_found(
                stream_name_for_error.to_string(),
            )),
            Err(e) => Err(StreamError::internal(format!("delete stream failed: {e}"))),
        }
    }

    async fn get_stream(&self, user_stream_name: UserStreamName) -> StreamResult<Option<Stream>> {
        let result = call_sqlite_raw(
            &self.connection,
            move |conn: &mut rusqlite::Connection| -> Result<Option<Stream>, rusqlite::Error> {
                let (sql, params) = sql_statements::get_stream_info(&user_stream_name);
                let mut stmt = conn.prepare(sql)?;

                let stream: Option<Stream> = stmt
                    .query_row(params, |row| {
                        let name: String = row.get(0)?;
                        let internal_id: String = row.get(1)?;
                        let ttl_seconds: Option<u32> = row.get(2)?;
                        let created_at = TimestampMillis::from(row.get::<_, i64>(3)?);

                        Ok(Stream {
                            name: name.into(),
                            internal_id: internal_id.into(),
                            ttl_seconds: ttl_seconds.map(Into::into),
                            partitioning_mode: StreamPartitioningMode::Single,
                            created_at,
                        })
                    })
                    .optional()?;

                Ok(stream)
            },
        )
        .await;

        result.map_err(|e| StreamError::internal(format!("get stream failed: {e}")))
    }

    async fn append_item(
        &self,
        stream_name: StreamName,
        item_data: &[u8],
        _partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId> {
        let stream_name_for_error = stream_name.clone();
        let item_data = item_data.to_vec();

        let item_id = StreamItemId::from(Uuid::now_v7());
        let created_at = TimestampMillis::now();

        let result = call_sqlite_raw(
            &self.connection,
            move |conn: &mut rusqlite::Connection| -> Result<Option<StreamItemId>, rusqlite::Error> {
                    let (sql, params) = insert_stream_entry(
                        &stream_name,
                        &item_id,
                        &item_data,
                        &created_at,
                        StreamDataType::Text,
                    );
                    conn.execute(sql, params)?;

                    Ok(Some(item_id))
                },
            )
            .await;

        match result {
            Ok(Some(item_id)) => Ok(item_id),
            Ok(None) => Err(StreamError::stream_not_found(stream_name_for_error)),
            Err(e) => Err(StreamError::internal(format!(
                "append stream item failed: {e}"
            ))),
        }
    }

    async fn read_forward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        let stream_name_for_error = stream_name.clone();

        if limit == 0 || limit > 10000 {
            return Err(StreamError::validation("Limit must be between 1 and 10000"));
        }

        let result = call_sqlite_raw(
            &self.connection,
            move |conn: &mut rusqlite::Connection| -> Result<Option<StreamPage>, rusqlite::Error> {
                let starting_item_id = match exclusive_start_key {
                    Some(item_id) => item_id.increment(),
                    None => StreamItemId::from(Uuid::nil()),
                };

                let items = {
                    let (sql, params) = sql_statements::read_stream_from_position(
                        &stream_name,
                        &starting_item_id,
                        limit,
                        stream_provider::ReadDirection::Forward,
                    );
                    conn.prepare(&sql)?
                        .query_map(params, parse_stream_item_row)?
                        .collect::<Result<Vec<_>, _>>()?
                };

                let has_more = items.len() > limit as usize;
                let mut items = items;
                if has_more {
                    items.truncate(limit as usize);
                }

                let last_evaluated_key = items.last().map(|i| i.id);

                Ok(Some(StreamPage {
                    items,
                    last_evaluated_key,
                    has_more,
                }))
            },
        )
        .await;

        match result {
            Ok(Some(page)) => Ok(page),
            Ok(None) => Err(StreamError::stream_not_found(stream_name_for_error)),
            Err(e) => Err(StreamError::internal(format!(
                "read stream forward failed: {e}"
            ))),
        }
    }

    async fn read_backward(
        &self,
        stream_name: StreamName,
        from_item_id: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        let stream_name_for_error = stream_name.clone();
        let starting_item_id = match from_item_id {
            Some(item_id) => item_id,
            None => StreamItemId::from(Uuid::max()),
        };

        if limit == 0 || limit > 1000 {
            return Err(StreamError::validation("Limit must be between 1 and 1000"));
        }

        let result = call_sqlite_raw(
            &self.connection,
            move |conn: &mut rusqlite::Connection| -> Result<Option<StreamPage>, rusqlite::Error> {
                let (sql, params) = sql_statements::read_stream_from_position(
                    &stream_name,
                    &starting_item_id,
                    limit,
                    stream_provider::ReadDirection::Backward,
                );

                let items = conn
                    .prepare(&sql)?
                    .query_map(params, parse_stream_item_row)?
                    .collect::<Result<Vec<_>, _>>()?;

                let has_more = items.len() > limit as usize;
                let mut items = items;
                if has_more {
                    items.truncate(limit as usize);
                }

                let last_evaluated_key = items.last().map(|i| i.id);

                Ok(Some(StreamPage {
                    items,
                    last_evaluated_key,
                    has_more,
                }))
            },
        )
        .await;

        match result {
            Ok(Some(page)) => Ok(page),
            Ok(None) => Err(StreamError::stream_not_found(stream_name_for_error)),
            Err(e) => Err(StreamError::internal(format!(
                "read stream backward failed: {e}"
            ))),
        }
    }

    async fn create_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        position: CursorPosition,
    ) -> StreamResult<()> {
        let stream_name_for_error = stream_name.clone();
        let cursor_name_for_error = cursor_name.clone();
        let created_at = TimestampMillis::now();

        let result = call_sqlite_raw(
            &self.connection,
            move |conn: &mut rusqlite::Connection| -> Result<Result<(), &'static str>, rusqlite::Error> {
                    let (sql, params) = sql_statements::check_cursor_exists(&cursor_name, &stream_name);
                    let cursor_exists: bool = conn
                        .query_row(
                            sql,
                            params,
                            |_| Ok(true),
                        )
                        .optional()?
                        .is_some();

                    if cursor_exists {
                        return Ok(Err("Cursor already exists"));
                    }

                    let position_id: StreamItemId = match position {
                        CursorPosition::Tail => {
                            // Position cursor at newest item so that reading returns only items
                            // added after cursor creation
                            let (sql, params) = sql_statements::get_latest_item_for_cursor(&stream_name);
                            conn.query_row(
                                sql,
                                params,
                                |row| {
                                    let id: String = row.get(0)?;
                                    parse_stream_item_id(&id, "item_id")
                                },
                            )
                            .optional()?
                            .unwrap_or_else(|| StreamItemId::from(Uuid::now_v7())) // Use new UUID if no items
                        },
                        CursorPosition::Head => StreamItemId::from(Uuid::nil()),
                    };

                    let (sql, params) = sql_statements::insert_cursor(
                        &cursor_name,
                        &stream_name,
                        &position_id,
                        &created_at,
                    );
                    conn.execute(sql, params)?;

                    Ok(Ok(()))
                },
            )
            .await;

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err("Stream not found")) => {
                Err(StreamError::stream_not_found(stream_name_for_error))
            }
            Ok(Err("Cursor already exists")) => Err(StreamError::cursor_already_exists(
                cursor_name_for_error.to_string(),
            )),
            Ok(Err(_)) => Err(StreamError::internal(
                "Unknown error during cursor creation".to_string(),
            )),
            Err(e) => Err(StreamError::internal(format!("create cursor failed: {e}"))),
        }
    }

    async fn delete_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<()> {
        let cursor_name_for_error = cursor_name.clone();

        let result = call_sqlite_raw(
            &self.connection,
            move |conn: &mut rusqlite::Connection| -> Result<bool, rusqlite::Error> {
                let (sql, params) = sql_statements::delete_cursor(&cursor_name, &stream_name);
                let affected = conn.execute(sql, params)?;

                Ok(affected > 0)
            },
        )
        .await;

        match result {
            Ok(true) => Ok(()),
            Ok(false) => Err(StreamError::cursor_not_found(
                cursor_name_for_error.to_string(),
            )),
            Err(e) => Err(StreamError::internal(format!("delete cursor failed: {e}"))),
        }
    }

    async fn read_from_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        limit: u32,
    ) -> StreamResult<CursorPage> {
        let stream_name_for_error = stream_name.clone();
        let cursor_name_for_error = cursor_name.clone();

        if limit == 0 || limit > 1000 {
            return Err(StreamError::validation("Limit must be between 1 and 1000"));
        }

        let result = call_sqlite_raw(
            &self.connection,
            move |conn: &mut rusqlite::Connection| -> Result<Result<CursorPage, &'static str>, rusqlite::Error> {
                    let (sql, params) = sql_statements::get_cursor_position(&cursor_name, &stream_name);
                    let cursor_position: Option<String> = conn
                        .query_row(
                            sql,
                            params,
                            |row| row.get(0),
                        )
                        .optional()?;

                    let Some(cursor_position) = cursor_position else {
                        return Ok(Err("Cursor not found"));
                    };

                    let starting_item_id: StreamItemId = cursor_position.parse::<StreamItemId>()
                        .unwrap_or_else(|_| StreamItemId::from(Uuid::nil()));

                    let (sql, params) = sql_statements::read_stream_from_position(&stream_name, &starting_item_id, limit, stream_provider::ReadDirection::Forward);

                    let items = conn.prepare(&sql)?
                        .query_map(params, |row| {
                            parse_stream_item_row(row)
                        })?
                        .collect::<Result<Vec<_>, _>>()?;

                    let has_more = items.len() > limit as usize;
                    let mut items = items;
                    if has_more {
                        items.truncate(limit as usize);
                    }

                    let Ok(current_position) = cursor_position.parse::<StreamItemId>() else {
                        return Ok(Err("Invalid cursor position ID"));
                    };

                    Ok(Ok(CursorPage {
                        items,
                        cursor_position: current_position,
                        has_more,
                    }))
                },
            )
            .await;

        match result {
            Ok(Ok(page)) => Ok(page),
            Ok(Err("Stream not found")) => {
                Err(StreamError::stream_not_found(stream_name_for_error))
            }
            Ok(Err("Cursor not found")) => Err(StreamError::cursor_not_found(
                cursor_name_for_error.to_string(),
            )),
            Ok(Err("Invalid cursor position UUID")) => Err(StreamError::validation(
                "Invalid cursor position UUID".to_string(),
            )),
            Ok(Err(_)) => Err(StreamError::internal(
                "Unknown error during cursor read".to_string(),
            )),
            Err(e) => Err(StreamError::internal(format!(
                "read from cursor failed: {e}"
            ))),
        }
    }

    async fn advance_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        to_item_id: StreamItemId,
    ) -> StreamResult<()> {
        let stream_name_for_error = stream_name.clone();
        let cursor_name_for_error = cursor_name.clone();

        let result = call_sqlite_raw(
            &self.connection,
            move |conn: &mut rusqlite::Connection| -> Result<Result<bool, &'static str>, rusqlite::Error> {
                    let (sql, params) = sql_statements::check_cursor_exists_for_advance(&cursor_name, &stream_name);
                    let cursor_exists: bool = conn
                        .query_row(
                            sql,
                            params,
                            |_| Ok(true),
                        )
                        .optional()?
                        .is_some();

                    if !cursor_exists {
                        return Ok(Ok(false));
                    }

                    let (sql, params) = sql_statements::check_item_exists_for_advance(&to_item_id, &stream_name);
                    let item_exists: bool = conn
                        .query_row(
                            sql,
                            params,
                            |_| Ok(true),
                        )
                        .optional()?
                        .is_some();

                    if !item_exists {
                        return Ok(Err("Item not found in stream"));
                    }

                    let (sql, params) = sql_statements::advance_cursor_position(
                        &to_item_id,
                        &cursor_name,
                        &stream_name,
                    );
                    conn.execute(sql, params)?;

                    Ok(Ok(true))
                },
            )
            .await;

        match result {
            Ok(Ok(true)) => Ok(()),
            Ok(Ok(false)) => Err(StreamError::cursor_not_found(
                cursor_name_for_error.to_string(),
            )),
            Ok(Err("Stream not found")) => {
                Err(StreamError::stream_not_found(stream_name_for_error))
            }
            Ok(Err("Item not found in stream")) => {
                Err(StreamError::validation("Target item not found in stream"))
            }
            Ok(Err(_)) => Err(StreamError::internal(
                "Unknown error during cursor advance".to_string(),
            )),
            Err(e) => Err(StreamError::internal(format!("advance cursor failed: {e}"))),
        }
    }

    async fn get_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>> {
        let result = call_sqlite_raw(&self.connection, move |conn: &mut rusqlite::Connection| -> Result<Option<StreamCursor>, rusqlite::Error> {

                let (sql, params) = sql_statements::get_cursor_details(&stream_name, &cursor_name);
                let mut stmt = conn.prepare(sql)?;

                let cursor = stmt.query_row(params, |row| {
                    let name: String = row.get(0)?;
                    let stream: String = row.get(1)?;
                    let position: String = row.get(2)?;
                    let created_at = TimestampMillis::from(row.get::<_, i64>(3)?);

                    Ok(StreamCursor {
                        name: CursorName::new(&name),
                        stream_name: stream.into(),
                        position: parse_stream_item_id(&position, "position")?,
                        created_at,
                    })
                }).optional()?;

                Ok(cursor)
            })
            .await;

        result.map_err(|e| StreamError::internal(format!("get cursor failed: {e}")))
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

fn ensure_sqlite_ordered_stream_format_metadata(
    conn: &rusqlite::Connection,
) -> storage_types::StorageResult<()> {
    let (sql, params) = sql_statements::get_stream_format_version();
    let format_version: Option<i64> = conn
        .query_row(sql, params, |row| row.get(0))
        .optional()
        .map_err(crate::error_handler::map_sqlite_error)?;

    match format_version {
        Some(ITEM_VERSIONED_STREAM_FORMAT_VERSION) => reject_old_sqlite_pointer_payloads(conn),
        Some(version) => {
            tracing::warn!(
                backend = "sqlite",
                reason = "incompatible_format_metadata",
                format_version = version,
                expected_format_version = ITEM_VERSIONED_STREAM_FORMAT_VERSION,
                "stream format startup rejected unsupported state"
            );
            Err(storage_types::StorageError::unsupported(&format!(
                "unsupported stream format metadata version {version}; expected item-versioned \
                 stream format version {ITEM_VERSIONED_STREAM_FORMAT_VERSION}"
            )))
        }
        None => {
            let (sql, params) = sql_statements::count_stream_items();
            let stream_items: i64 = conn
                .query_row(sql, params, |row| row.get(0))
                .map_err(crate::error_handler::map_sqlite_error)?;
            if stream_items > 0 {
                tracing::warn!(
                    backend = "sqlite",
                    reason = "missing_format_metadata",
                    stream_items,
                    "stream format startup rejected unsupported state"
                );
                return Err(storage_types::StorageError::unsupported(
                    "item-versioned streams require empty stream tables or stream format \
                     metadata; in-place upgrade from old stream rows is unsupported",
                ));
            }

            let (sql, params) = sql_statements::upsert_stream_format_version();
            conn.execute(sql, params)
                .map_err(crate::error_handler::map_sqlite_error)?;
            Ok(())
        }
    }
}

fn reject_old_sqlite_pointer_payloads(
    conn: &rusqlite::Connection,
) -> storage_types::StorageResult<()> {
    let (sql, params) = sql_statements::list_stream_pointer_payloads();
    let mut stmt = conn
        .prepare(sql)
        .map_err(crate::error_handler::map_sqlite_error)?;
    let mut rows = stmt
        .query(params)
        .map_err(crate::error_handler::map_sqlite_error)?;

    while let Some(row) = rows
        .next()
        .map_err(crate::error_handler::map_sqlite_error)?
    {
        let stream_name: String = row.get(0).map_err(crate::error_handler::map_sqlite_error)?;
        let item_id: String = row.get(1).map_err(crate::error_handler::map_sqlite_error)?;
        let data: Vec<u8> = row.get(2).map_err(crate::error_handler::map_sqlite_error)?;
        if let Err(err) = storage_types::storage_serde::from_bytes::<StoredStreamPointer>(&data) {
            tracing::warn!(
                backend = "sqlite",
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

fn parse_stream_item_row(row: &rusqlite::Row) -> rusqlite::Result<StreamItem> {
    let id: String = row.get(0)?;
    let stream_name: String = row.get(1)?;
    let data: Vec<u8> = row.get(2)?;
    let created_at = TimestampMillis::from(row.get::<_, i64>(3)?);
    let data_type: StreamDataType = row.get::<_, i32>(4)?.into();

    Ok(StreamItem {
        id: parse_stream_item_id(&id, "item_id")?,
        stream_name: Some(stream_name.into()),
        data,
        data_type,
        created_at,
    })
}

fn parse_stream_item_id(value: &str, column: &str) -> rusqlite::Result<StreamItemId> {
    value.parse::<StreamItemId>().map_err(|_| {
        rusqlite::Error::InvalidColumnType(0, column.to_string(), rusqlite::types::Type::Text)
    })
}

// fn encode_page_token(uuidv7: &Uuid) -> String {
//     let str = uuidv7.to_string();
//     let bytes = str.as_bytes();
//     URL_SAFE.encode(bytes)
// }

// #[expect(unused)]
// fn decode_page_token(token: &str) -> StreamResult<String> {
//     let token = URL_SAFE
//         .decode(token)
//         .map_err(|_e| StreamError::validation("Invalid page
// token".to_string()))?;

//     Ok(String::from_utf8_lossy(&token).to_string())
//}
