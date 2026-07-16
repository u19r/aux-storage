use std::collections::HashMap;

use async_trait::async_trait;
use queue_provider::{
    MessageId, MessageResponse, Queue, QueueError, QueueMessage, QueueMessageCounts, QueueProvider,
    QueueResult, QueueValidationKind, ReceiptHandle, VisibleQueueMessage,
};
use rusqlite::OptionalExtension as _;
use storage_types::{DurationSeconds, TimestampMillis};
use uuid::Uuid;

use crate::{
    backends::sqlite::SQLiteStorageProvider,
    error_handler::map_sqlite_error,
    sql_statements,
    utils::{call_sqlite, sql_row_to_queue_message},
};

async fn call_sqlite_queue<F, R>(
    connection: &tokio_rusqlite::Connection,
    function: F,
) -> QueueResult<R>
where
    F: FnOnce(&mut rusqlite::Connection) -> QueueResult<R> + Send + 'static,
    R: Send + 'static,
{
    connection
        .call(move |conn| function(conn).map_err(|err| tokio_rusqlite::Error::Other(Box::new(err))))
        .await
        .map_err(map_tokio_rusqlite_queue_error)
}

fn map_tokio_rusqlite_queue_error(err: tokio_rusqlite::Error) -> QueueError {
    match err {
        tokio_rusqlite::Error::Other(err) => match err.downcast::<QueueError>() {
            Ok(err) => *err,
            Err(err) => QueueError::StorageError(storage_types::StorageError::internal(&format!(
                "sqlite queue call failed: {err}"
            ))),
        },
        tokio_rusqlite::Error::Rusqlite(err) => QueueError::StorageError(map_sqlite_error(err)),
        other => QueueError::StorageError(storage_types::StorageError::internal(&format!(
            "sqlite queue call failed: {other}"
        ))),
    }
}

#[async_trait]
impl QueueProvider for SQLiteStorageProvider {
    async fn initialize(&self) -> QueueResult<()> {
        call_sqlite(&self.connection, move |conn| {
            let (sql, params) = sql_statements::create_queues_table();
            conn.execute(sql, params).map_err(map_sqlite_error)?;

            let (sql, params) = sql_statements::create_messages_table();
            conn.execute(sql, params).map_err(map_sqlite_error)?;

            let (sql, params) = sql_statements::create_messages_queue_visibility_index();
            conn.execute(sql, params).map_err(map_sqlite_error)?;

            let (sql, params) = sql_statements::create_messages_queue_receipt_index();
            conn.execute(sql, params).map_err(map_sqlite_error)?;

            Ok(())
        })
        .await
        .map_err(Into::into)
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue> {
        let attributes_json = serde_json::to_string(&queue.attributes)?;

        call_sqlite_queue(&self.connection, move |conn| {
            let existing: Option<(String, String)> = conn
                .query_row(
                    "SELECT queue_url, attributes FROM sys_queues WHERE queue_name = ?1",
                    [queue.queue_name.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(map_sqlite_error)?;
            if let Some((existing_url, existing_attributes_json)) = existing {
                let existing_attributes: HashMap<String, String> =
                    serde_json::from_str(&existing_attributes_json)?;
                if existing_url == queue.queue_url && existing_attributes == queue.attributes {
                    return Ok(queue);
                }
                return Err(QueueError::queue_already_exists(queue.queue_name.clone()));
            }

            let (sql, params) = sql_statements::insert_or_replace_queue(
                &queue.queue_name,
                &queue.queue_url,
                &attributes_json,
                &queue.created_at,
            );

            conn.execute(sql, params).map_err(map_sqlite_error)?;

            Ok(queue)
        })
        .await
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        let queue_url = queue_url.to_string();
        call_sqlite(&self.connection, move |conn| {
            let (sql, params) = sql_statements::get_queue(&queue_url);

            let queue_url_clone = queue_url.clone();
            let queue_opt = conn
                .query_row(sql, params, move |row| {
                    let queue_name: String = row.get(0)?;
                    let attributes_json: String = row.get(1)?;
                    let created_at: i64 = row.get(2)?;
                    let created_at: TimestampMillis = created_at.into();

                    let attributes: HashMap<String, String> =
                        serde_json::from_str(&attributes_json).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?;

                    Ok(Queue {
                        queue_name,
                        queue_url: queue_url_clone.clone(),
                        attributes,
                        created_at,
                    })
                })
                .optional()
                .map_err(map_sqlite_error)?;

            Ok(queue_opt)
        })
        .await
        .map_err(Into::into)
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        let queue_name = queue_name.to_string();
        call_sqlite(&self.connection, move |conn| {
            let (sql, params) = sql_statements::get_queue_by_name(&queue_name);

            let queue_name_clone = queue_name.clone();
            let queue_opt = conn
                .query_row(sql, params, move |row| {
                    let queue_url: String = row.get(0)?;
                    let attributes_json: String = row.get(1)?;
                    let created_at: i64 = row.get(2)?;
                    let attributes: HashMap<String, String> =
                        serde_json::from_str(&attributes_json).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?;

                    Ok(Queue {
                        queue_name: queue_name_clone.clone(),
                        queue_url,
                        attributes,
                        created_at: created_at.into(),
                    })
                })
                .optional()
                .map_err(map_sqlite_error)?;

            Ok(queue_opt)
        })
        .await
        .map_err(Into::into)
    }

    async fn list_queues(&self, queue_name_prefix: Option<&str>) -> QueueResult<Vec<Queue>> {
        let prefix = queue_name_prefix.map(ToString::to_string);
        call_sqlite(&self.connection, move |conn| {
            let (sql, params) = sql_statements::list_queues(prefix.as_deref());
            let mut stmt = conn.prepare(sql).map_err(map_sqlite_error)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(params), |row| {
                    let queue_name: String = row.get(0)?;
                    let queue_url: String = row.get(1)?;
                    let attributes_json: String = row.get(2)?;
                    let created_at: i64 = row.get(3)?;
                    let attributes: HashMap<String, String> =
                        serde_json::from_str(&attributes_json).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?;
                    Ok(Queue {
                        queue_name,
                        queue_url,
                        attributes,
                        created_at: created_at.into(),
                    })
                })
                .map_err(map_sqlite_error)?;

            let mut queues = Vec::new();
            for row in rows {
                queues.push(row.map_err(map_sqlite_error)?);
            }
            Ok(queues)
        })
        .await
        .map_err(Into::into)
    }

    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()> {
        let queue_url = queue_url.to_string();
        call_sqlite_queue(&self.connection, move |conn| {
            let txn = conn.transaction().map_err(map_sqlite_error)?;
            let queue_name: Option<String> = txn
                .query_row(
                    "SELECT queue_name FROM sys_queues WHERE queue_url = ?1",
                    [queue_url.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(map_sqlite_error)?;
            let Some(queue_name) = queue_name else {
                return Err(QueueError::table_not_found(queue_url));
            };
            txn.execute(
                "DELETE FROM sys_messages WHERE queue_name = ?1",
                [queue_name.as_str()],
            )
            .map_err(map_sqlite_error)?;
            txn.execute(
                "DELETE FROM sys_queues WHERE queue_url = ?1",
                [queue_url.as_str()],
            )
            .map_err(map_sqlite_error)?;
            txn.commit().map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    }

    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()> {
        let queue_url = queue_url.to_string();
        call_sqlite_queue(&self.connection, move |conn| {
            let queue_name: Option<String> = conn
                .query_row(
                    "SELECT queue_name FROM sys_queues WHERE queue_url = ?1",
                    [queue_url.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(map_sqlite_error)?;
            let Some(queue_name) = queue_name else {
                return Err(QueueError::table_not_found(queue_url));
            };
            let (sql, params) = sql_statements::purge_queue(&queue_name);
            conn.execute(sql, params).map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    }

    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: HashMap<String, String>,
    ) -> QueueResult<()> {
        let queue_url = queue_url.to_string();
        let attributes_json = serde_json::to_string(&attributes)?;
        call_sqlite_queue(&self.connection, move |conn| {
            let (sql, params) = sql_statements::set_queue_attributes(&queue_url, &attributes_json);
            let updated = conn.execute(sql, params).map_err(map_sqlite_error)?;
            if updated == 0 {
                return Err(QueueError::table_not_found(queue_url));
            }
            Ok(())
        })
        .await
    }

    async fn get_queue_message_counts(&self, queue_url: &str) -> QueueResult<QueueMessageCounts> {
        let queue_name = queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();

        call_sqlite_queue(&self.connection, move |conn| {
            let now = *TimestampMillis::now();
            let visible = conn
                .query_row(
                    "SELECT COUNT(*) FROM sys_messages WHERE queue_name = ?1 AND \
                     visibility_timestamp <= ?2",
                    rusqlite::params![queue_name.as_str(), now],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(map_sqlite_error)?
                .try_into()
                .map_err(|err| {
                    QueueError::internal_with_detail(
                        queue_provider::QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                        err,
                    )
                })?;
            let not_visible = conn
                .query_row(
                    "SELECT COUNT(*) FROM sys_messages WHERE queue_name = ?1 AND \
                     visibility_timestamp > ?2 AND receipt_handle IS NOT NULL",
                    rusqlite::params![queue_name.as_str(), now],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(map_sqlite_error)?
                .try_into()
                .map_err(|err| {
                    QueueError::internal_with_detail(
                        queue_provider::QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                        err,
                    )
                })?;
            let delayed = conn
                .query_row(
                    "SELECT COUNT(*) FROM sys_messages WHERE queue_name = ?1 AND \
                     visibility_timestamp > ?2 AND receipt_handle IS NULL",
                    rusqlite::params![queue_name.as_str(), now],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(map_sqlite_error)?
                .try_into()
                .map_err(|err| {
                    QueueError::internal_with_detail(
                        queue_provider::QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                        err,
                    )
                })?;

            Ok(QueueMessageCounts {
                visible,
                not_visible,
                delayed,
            })
        })
        .await
    }

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        let mut stored_message = message;
        if stored_message.message_id == MessageId::default() {
            stored_message.message_id = MessageId::random();
        }
        let message_id = stored_message.message_id;

        let message_attributes_json = stored_message
            .message_attributes
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        // Extract queue name from queue URL (assuming format like "/queue/queue-name")
        let queue_name = stored_message
            .queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();

        let visibility_timestamp = stored_message.visibility_timestamp.unwrap_or_default();

        call_sqlite_queue(&self.connection, move |conn| {
            let (sql, params) = sql_statements::send_message(
                &stored_message.message_id,
                &queue_name,
                &stored_message.body,
                message_attributes_json.as_deref(),
                &visibility_timestamp,
                &stored_message.created_at,
            );

            conn.execute(sql, params).map_err(map_sqlite_error)?;
            Ok(())
        })
        .await?;

        Ok(message_id)
    }

    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        _wait_time_seconds: DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>> {
        // Extract queue name from queue URL
        let queue_name = queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();

        let next_visible = visibility_timeout.time_from_now();
        let queue_url = queue_url.to_string();

        call_sqlite_queue(&self.connection, move |conn| {
            let timestamp = TimestampMillis::now();
            let (sql, params) =
                sql_statements::receive_messages(&queue_name, &timestamp, max_messages);

            let mut stmt = conn.prepare(sql).map_err(map_sqlite_error)?;

            let rows = stmt
                .query_map(params, |row: &rusqlite::Row<'_>| {
                    sql_row_to_queue_message(&queue_url, row)
                })
                .map_err(map_sqlite_error)?;

            let mut messages = Vec::new();

            for row_result in rows {
                let queue_message = row_result.map_err(map_sqlite_error)?;
                let visible = VisibleQueueMessage::new(queue_message);

                let next_receipt_handle = ReceiptHandle::new(*next_visible, Uuid::now_v7());

                let (sql, params) = sql_statements::update_message_visibility(
                    &next_visible,
                    &next_receipt_handle,
                    &visible.as_message().message_id,
                    &queue_name,
                    visible.as_message().receipt_handle.as_ref(),
                );

                let updated_rows = conn.execute(sql, params).map_err(map_sqlite_error)?;

                if updated_rows > 0 {
                    let invisible = visible.into_invisible(next_receipt_handle, next_visible);
                    if let Some(message) = invisible.into_message_response() {
                        messages.push(message);
                    }
                }
            }

            Ok(messages)
        })
        .await
    }

    async fn delete_message(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
    ) -> QueueResult<()> {
        let queue_name = queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();

        call_sqlite_queue(&self.connection, move |conn| {
            let (sql, params) = sql_statements::delete_message(&queue_name, &receipt_handle);

            let deleted_rows = conn.execute(sql, params).map_err(map_sqlite_error)?;

            if deleted_rows == 0 {
                return Err(QueueError::validation(
                    QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
                ));
            }

            Ok(())
        })
        .await
    }

    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        visibility_timeout: DurationSeconds,
    ) -> QueueResult<()> {
        let queue_name = queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();

        let new_visibility_timestamp = visibility_timeout.time_from_now();

        call_sqlite_queue(&self.connection, move |conn| {
            let (sql, params) = sql_statements::change_message_visibility(
                &new_visibility_timestamp,
                &queue_name,
                &receipt_handle,
            );

            let updated_rows = conn.execute(sql, params).map_err(map_sqlite_error)?;

            if updated_rows == 0 {
                return Err(QueueError::validation(
                    QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
                ));
            }

            Ok(())
        })
        .await
    }

    async fn update_message_snapshot_checkpoint(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()> {
        let queue_name = queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();

        call_sqlite_queue(&self.connection, move |conn| {
            let (sql, params) = sql_statements::update_message_checkpoint(
                &checkpoint_data,
                &queue_name,
                &receipt_handle,
            );

            let updated_rows = conn.execute(sql, params).map_err(map_sqlite_error)?;

            if updated_rows == 0 {
                return Err(QueueError::validation(QueueValidationKind::MessageNotFound));
            }

            Ok(())
        })
        .await
    }
}
