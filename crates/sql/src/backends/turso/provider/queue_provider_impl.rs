use std::collections::HashMap;

use async_trait::async_trait;
use queue_provider::{
    MessageId, MessageResponse, Queue, QueueError, QueueMessage, QueueMessageCounts, QueueProvider,
    QueueResult, QueueValidationKind, ReceiptHandle, VisibleQueueMessage,
};
use storage_types::{DurationSeconds, TimestampMillis};
use turso::Value as TursoValue;

use crate::backends::turso::{
    provider::{
        TursoStorageProvider, option_string_to_value, row_optional_text, row_required_i64,
        row_required_text,
    },
    sql_statements,
};

#[async_trait]
impl QueueProvider for TursoStorageProvider {
    async fn initialize(&self) -> QueueResult<()> {
        let _ddl_guard = self.ddl_lock.lock().await;
        let this = self.clone();
        self.with_exclusive_transaction(true, |conn| {
            let this = this.clone();
            Box::pin(async move {
                let sql = sql_statements::create_queues_table();
                let _ = this.execute(conn, sql, Vec::new()).await?;

                let sql = sql_statements::create_messages_table();
                let _ = this.execute(conn, sql, Vec::new()).await?;

                let sql = sql_statements::create_messages_queue_visibility_index();
                let _ = this.execute(conn, sql, Vec::new()).await?;

                let sql = sql_statements::create_messages_queue_receipt_index();
                let _ = this.execute(conn, sql, Vec::new()).await?;

                Ok(())
            })
        })
        .await
        .map_err(QueueError::from)
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<()> {
        let conn = self.connect().await.map_err(QueueError::from)?;
        let attributes_json = serde_json::to_string(&queue.attributes)?;
        let created_at = *queue.created_at;
        let sql = sql_statements::insert_or_replace_queue();
        let params = vec![
            TursoValue::Text(queue.queue_name),
            TursoValue::Text(queue.queue_url),
            TursoValue::Text(attributes_json),
            TursoValue::Integer(created_at),
        ];
        let _ = self
            .execute(&conn, sql, params)
            .await
            .map_err(QueueError::from)?;
        Ok(())
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        let conn = self.connect().await.map_err(QueueError::from)?;
        let sql = sql_statements::get_queue();
        let rows = self
            .query_rows(&conn, sql, vec![TursoValue::Text(queue_url.to_string())])
            .await
            .map_err(QueueError::from)?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };

        let queue_name = row_required_text(&row, "queue_name").map_err(QueueError::from)?;
        let attributes_raw = row_required_text(&row, "attributes").map_err(QueueError::from)?;
        let created_at = row_required_i64(&row, "created_at").map_err(QueueError::from)?;

        let attributes = serde_json::from_str::<HashMap<String, String>>(&attributes_raw)?;
        Ok(Some(Queue {
            queue_name,
            queue_url: queue_url.to_string(),
            attributes,
            created_at: created_at.into(),
        }))
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        let conn = self.connect().await.map_err(QueueError::from)?;
        let sql = sql_statements::get_queue_by_name();
        let rows = self
            .query_rows(&conn, sql, vec![TursoValue::Text(queue_name.to_string())])
            .await
            .map_err(QueueError::from)?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        let queue_url = row_required_text(&row, "queue_url").map_err(QueueError::from)?;
        let attributes_raw = row_required_text(&row, "attributes").map_err(QueueError::from)?;
        let created_at = row_required_i64(&row, "created_at").map_err(QueueError::from)?;
        let attributes = serde_json::from_str::<HashMap<String, String>>(&attributes_raw)?;
        Ok(Some(Queue {
            queue_name: queue_name.to_string(),
            queue_url,
            attributes,
            created_at: created_at.into(),
        }))
    }

    async fn list_queues(&self, queue_name_prefix: Option<&str>) -> QueueResult<Vec<Queue>> {
        let conn = self.connect().await.map_err(QueueError::from)?;
        let (sql, params) = if let Some(prefix) = queue_name_prefix {
            (
                sql_statements::list_queues_with_prefix(),
                vec![TursoValue::Text(format!("{prefix}%"))],
            )
        } else {
            (sql_statements::list_all_queues(), Vec::new())
        };
        let rows = self
            .query_rows(&conn, sql, params)
            .await
            .map_err(QueueError::from)?;
        let mut queues = Vec::with_capacity(rows.len());
        for row in rows {
            let attributes_raw = row_required_text(&row, "attributes").map_err(QueueError::from)?;
            queues.push(Queue {
                queue_name: row_required_text(&row, "queue_name").map_err(QueueError::from)?,
                queue_url: row_required_text(&row, "queue_url").map_err(QueueError::from)?,
                attributes: serde_json::from_str::<HashMap<String, String>>(&attributes_raw)?,
                created_at: row_required_i64(&row, "created_at")
                    .map_err(QueueError::from)?
                    .into(),
            });
        }
        Ok(queues)
    }

    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()> {
        let queue_name = queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();
        let conn = self.connect().await.map_err(QueueError::from)?;
        let _ = self
            .execute(
                &conn,
                sql_statements::delete_messages_for_queue(),
                vec![TursoValue::Text(queue_name)],
            )
            .await
            .map_err(QueueError::from)?;
        let _ = self
            .execute(
                &conn,
                sql_statements::delete_queue(),
                vec![TursoValue::Text(queue_url.to_string())],
            )
            .await
            .map_err(QueueError::from)?;
        Ok(())
    }

    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()> {
        let queue_name = queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();
        let conn = self.connect().await.map_err(QueueError::from)?;
        let _ = self
            .execute(
                &conn,
                sql_statements::delete_messages_for_queue(),
                vec![TursoValue::Text(queue_name)],
            )
            .await
            .map_err(QueueError::from)?;
        Ok(())
    }

    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: HashMap<String, String>,
    ) -> QueueResult<()> {
        let conn = self.connect().await.map_err(QueueError::from)?;
        let _ = self
            .execute(
                &conn,
                sql_statements::set_queue_attributes(),
                vec![
                    TursoValue::Text(serde_json::to_string(&attributes)?),
                    TursoValue::Text(queue_url.to_string()),
                ],
            )
            .await
            .map_err(QueueError::from)?;
        Ok(())
    }

    async fn get_queue_message_counts(&self, queue_url: &str) -> QueueResult<QueueMessageCounts> {
        let queue_name = queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();
        let now = *TimestampMillis::now();
        let conn = self.connect().await.map_err(QueueError::from)?;

        let visible_rows = self
            .query_rows(
                &conn,
                "SELECT COUNT(*) AS count FROM sys_messages WHERE queue_name = ?1 AND \
                 visibility_timestamp <= ?2",
                vec![
                    TursoValue::Text(queue_name.clone()),
                    TursoValue::Integer(now),
                ],
            )
            .await
            .map_err(QueueError::from)?;
        let visible = turso_count(visible_rows)?;

        let not_visible_rows = self
            .query_rows(
                &conn,
                "SELECT COUNT(*) AS count FROM sys_messages WHERE queue_name = ?1 AND \
                 visibility_timestamp > ?2 AND receipt_handle IS NOT NULL",
                vec![
                    TursoValue::Text(queue_name.clone()),
                    TursoValue::Integer(now),
                ],
            )
            .await
            .map_err(QueueError::from)?;
        let not_visible = turso_count(not_visible_rows)?;

        let delayed_rows = self
            .query_rows(
                &conn,
                "SELECT COUNT(*) AS count FROM sys_messages WHERE queue_name = ?1 AND \
                 visibility_timestamp > ?2 AND receipt_handle IS NULL",
                vec![TursoValue::Text(queue_name), TursoValue::Integer(now)],
            )
            .await
            .map_err(QueueError::from)?;
        let delayed = turso_count(delayed_rows)?;

        Ok(QueueMessageCounts {
            visible,
            not_visible,
            delayed,
        })
    }

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        let conn = self.connect().await.map_err(QueueError::from)?;
        let queue = self
            .get_queue(&message.queue_url)
            .await?
            .ok_or_else(|| QueueError::table_not_found(message.queue_url.clone()))?;
        let message_id = if message.message_id == MessageId::default() {
            MessageId::random()
        } else {
            message.message_id
        };

        let message_attributes_json = message
            .message_attributes
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        let visibility_timestamp = message
            .visibility_timestamp
            .map(|value| *value)
            .unwrap_or_else(|| *TimestampMillis::now());
        let created_at = *message.created_at;

        let sql = sql_statements::send_message();
        let params = vec![
            TursoValue::Text(message_id.to_string()),
            TursoValue::Text(queue.queue_name),
            TursoValue::Text(message.body),
            option_string_to_value(message_attributes_json),
            TursoValue::Integer(visibility_timestamp),
            TursoValue::Integer(created_at),
        ];
        let _ = self
            .execute(&conn, sql, params)
            .await
            .map_err(QueueError::from)?;
        Ok(message_id)
    }

    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        _wait_time_seconds: DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>> {
        let queue = self
            .get_queue(queue_url)
            .await?
            .ok_or_else(|| QueueError::table_not_found(queue_url.to_string()))?;
        let queue_name = queue.queue_name.clone();
        let queue_url = queue_url.to_string();
        let this = self.clone();

        let now = TimestampMillis::now();
        let next_visible = visibility_timeout.time_from_now();

        self.with_transaction(true, |conn| {
            let this = this.clone();
            let queue_name = queue_name.clone();
            let queue_url = queue_url.clone();
            Box::pin(async move {
                let sql = sql_statements::receive_messages();

                let rows = this
                    .query_rows(
                        conn,
                        sql,
                        vec![
                            TursoValue::Text(queue_name.clone()),
                            TursoValue::Integer(*now),
                            TursoValue::Integer(i64::from(max_messages)),
                        ],
                    )
                    .await?;

                let mut responses = Vec::with_capacity(rows.len());
                for row in rows {
                    let message_id = row_required_text(&row, "message_id")?;
                    let receipt_handle_raw = uuid::Uuid::now_v7().to_string();
                    let receipt_handle = ReceiptHandle::from(receipt_handle_raw.as_str());

                    let update_sql = sql_statements::claim_message();

                    let _ = this
                        .execute(
                            conn,
                            update_sql,
                            vec![
                                TursoValue::Integer(*next_visible),
                                TursoValue::Text(receipt_handle.to_string()),
                                TursoValue::Text(message_id.clone()),
                                TursoValue::Text(queue_name.clone()),
                                row.get("receipt_handle")
                                    .cloned()
                                    .unwrap_or(TursoValue::Null),
                            ],
                        )
                        .await?;

                    let body = row_required_text(&row, "body")?;
                    let created_at = row_required_i64(&row, "created_at")?;
                    let message_attributes = row_optional_text(&row, "message_attributes")?
                        .and_then(|json| serde_json::from_str(&json).ok());

                    let queue_message = QueueMessage {
                        message_id: MessageId::from(message_id.as_str()),
                        queue_url: queue_url.clone(),
                        body,
                        message_attributes,
                        receipt_handle: None,
                        created_at: created_at.into(),
                        visibility_timestamp: Some(next_visible),
                    };
                    let visible = VisibleQueueMessage::new(queue_message);
                    let invisible = visible.into_invisible(receipt_handle, next_visible);
                    if let Some(message) = invisible.into_message_response() {
                        responses.push(message);
                    }
                }

                Ok(responses)
            })
        })
        .await
        .map_err(QueueError::from)
    }

    async fn delete_message(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
    ) -> QueueResult<()> {
        let queue = self
            .get_queue(queue_url)
            .await?
            .ok_or_else(|| QueueError::table_not_found(queue_url.to_string()))?;

        let conn = self.connect().await.map_err(QueueError::from)?;
        let sql = sql_statements::delete_message();
        let _ = self
            .execute(
                &conn,
                sql,
                vec![
                    TursoValue::Text(queue.queue_name),
                    TursoValue::Text(receipt_handle.to_string()),
                ],
            )
            .await
            .map_err(QueueError::from)?;
        Ok(())
    }

    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        visibility_timeout: DurationSeconds,
    ) -> QueueResult<()> {
        let queue = self
            .get_queue(queue_url)
            .await?
            .ok_or_else(|| QueueError::table_not_found(queue_url.to_string()))?;

        let conn = self.connect().await.map_err(QueueError::from)?;
        let new_visibility = visibility_timeout.time_from_now();
        let sql = sql_statements::change_message_visibility();
        let _ = self
            .execute(
                &conn,
                sql,
                vec![
                    TursoValue::Integer(*new_visibility),
                    TursoValue::Text(queue.queue_name),
                    TursoValue::Text(receipt_handle.to_string()),
                ],
            )
            .await
            .map_err(QueueError::from)?;

        Ok(())
    }

    async fn update_message_snapshot_checkpoint(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()> {
        let queue = self
            .get_queue(queue_url)
            .await?
            .ok_or_else(|| QueueError::table_not_found(queue_url.to_string()))?;

        let conn = self.connect().await.map_err(QueueError::from)?;
        let sql = sql_statements::update_message_checkpoint();
        let _ = self
            .execute(
                &conn,
                sql,
                vec![
                    TursoValue::Text(checkpoint_data),
                    TursoValue::Text(queue.queue_name),
                    TursoValue::Text(receipt_handle.to_string()),
                ],
            )
            .await
            .map_err(QueueError::from)?;

        Ok(())
    }
}

fn turso_count(rows: Vec<HashMap<String, TursoValue>>) -> QueueResult<u64> {
    let count = rows
        .first()
        .ok_or_else(|| {
            QueueError::internal(
                queue_provider::QueueInternalKind::InvalidMessageVisibilityKeyFormat,
            )
        })
        .and_then(|row| row_required_i64(row, "count").map_err(QueueError::from))?;
    count.try_into().map_err(|err| {
        QueueError::internal_with_detail(
            queue_provider::QueueInternalKind::InvalidMessageVisibilityKeyFormat,
            err,
        )
    })
}
