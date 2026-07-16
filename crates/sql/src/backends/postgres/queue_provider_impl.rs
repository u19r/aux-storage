use std::collections::HashMap;

use async_trait::async_trait;
use queue_provider::{
    MessageId, MessageResponse, Queue, QueueError, QueueMessage, QueueMessageCounts, QueueProvider,
    QueueResult, QueueValidationKind, ReceiptHandle, VisibleQueueMessage,
};
use storage_types::TimestampMillis;
use uuid::Uuid;

use crate::backends::postgres::{PostgresStorageProvider, sql_statements};

#[async_trait]
impl QueueProvider for PostgresStorageProvider {
    async fn initialize(&self) -> QueueResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
        client
            .batch_execute(sql_statements::create_queue_tables())
            .await
            .map_err(|err| Self::map_queue_error("initialize queue tables", err))?;
        Ok(())
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue> {
        self.retry_postgres_queue_conflicts("create_queue", || {
            let queue = queue.clone();
            async move {
                let attributes_json = serde_json::to_string(&queue.attributes)?;
                let created_at = *queue.created_at;
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
                client
                    .execute(
                        sql_statements::create_queue(),
                        &[
                            &queue.queue_name,
                            &queue.queue_url,
                            &attributes_json,
                            &created_at,
                        ],
                    )
                    .await
                    .map_err(|err| Self::map_queue_error("create queue", err))?;
                Ok(())
            }
        })
        .await?;
        Ok(queue)
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        let queue_url = queue_url.to_string();
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
        let row = client
            .query_opt(sql_statements::get_queue(), &[&queue_url])
            .await
            .map_err(|err| Self::map_queue_error("get queue", err))?;
        let Some(row) = row else {
            return Ok(None);
        };

        let queue_name: String = row
            .try_get("queue_name")
            .map_err(|err| Self::map_queue_error("decode queue_name", err))?;
        let attributes_json: String = row
            .try_get("attributes")
            .map_err(|err| Self::map_queue_error("decode queue attributes", err))?;
        let created_at: i64 = row
            .try_get("created_at")
            .map_err(|err| Self::map_queue_error("decode queue created_at", err))?;
        let attributes = serde_json::from_str(&attributes_json)?;

        Ok(Some(Queue {
            queue_name,
            queue_url,
            attributes,
            created_at: created_at.into(),
        }))
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        let queue_name = queue_name.to_string();
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
        let row = client
            .query_opt(sql_statements::get_queue_by_name(), &[&queue_name])
            .await
            .map_err(|err| Self::map_queue_error("get queue by name", err))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let queue_url: String = row
            .try_get("queue_url")
            .map_err(|err| Self::map_queue_error("decode queue_url", err))?;
        let attributes_json: String = row
            .try_get("attributes")
            .map_err(|err| Self::map_queue_error("decode queue attributes", err))?;
        let created_at: i64 = row
            .try_get("created_at")
            .map_err(|err| Self::map_queue_error("decode queue created_at", err))?;
        let attributes = serde_json::from_str(&attributes_json)?;
        Ok(Some(Queue {
            queue_name,
            queue_url,
            attributes,
            created_at: created_at.into(),
        }))
    }

    async fn list_queues(&self, queue_name_prefix: Option<&str>) -> QueueResult<Vec<Queue>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
        let rows = if let Some(prefix) = queue_name_prefix {
            client
                .query(
                    sql_statements::list_queues_with_prefix(),
                    &[&format!("{prefix}%")],
                )
                .await
        } else {
            client.query(sql_statements::list_all_queues(), &[]).await
        }
        .map_err(|err| Self::map_queue_error("list queues", err))?;
        let mut queues = Vec::with_capacity(rows.len());
        for row in rows {
            let attributes_json: String = row
                .try_get("attributes")
                .map_err(|err| Self::map_queue_error("decode queue attributes", err))?;
            queues.push(Queue {
                queue_name: row
                    .try_get("queue_name")
                    .map_err(|err| Self::map_queue_error("decode queue_name", err))?,
                queue_url: row
                    .try_get("queue_url")
                    .map_err(|err| Self::map_queue_error("decode queue_url", err))?,
                attributes: serde_json::from_str(&attributes_json)?,
                created_at: row
                    .try_get::<_, i64>("created_at")
                    .map_err(|err| Self::map_queue_error("decode queue created_at", err))?
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
        client
            .execute(sql_statements::delete_messages_for_queue(), &[&queue_name])
            .await
            .map_err(|err| Self::map_queue_error("delete queue messages", err))?;
        client
            .execute(sql_statements::delete_queue(), &[&queue_url])
            .await
            .map_err(|err| Self::map_queue_error("delete queue", err))?;
        Ok(())
    }

    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()> {
        let queue_name = queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
        client
            .execute(sql_statements::delete_messages_for_queue(), &[&queue_name])
            .await
            .map_err(|err| Self::map_queue_error("purge queue", err))?;
        Ok(())
    }

    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: HashMap<String, String>,
    ) -> QueueResult<()> {
        let attributes_json = serde_json::to_string(&attributes)?;
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
        client
            .execute(
                sql_statements::set_queue_attributes(),
                &[&attributes_json, &queue_url],
            )
            .await
            .map_err(|err| Self::map_queue_error("set queue attributes", err))?;
        Ok(())
    }

    async fn get_queue_message_counts(&self, queue_url: &str) -> QueueResult<QueueMessageCounts> {
        let queue_name = queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?
            .to_string();
        let now = *TimestampMillis::now();
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;

        let visible = query_count(
            client
                .query_one(
                    "SELECT COUNT(*) FROM sys_messages WHERE queue_name = $1 AND \
                     visibility_timestamp <= $2",
                    &[&queue_name, &now],
                )
                .await
                .map_err(|err| Self::map_queue_error("count visible queue messages", err))?
                .try_get(0)
                .map_err(|err| Self::map_queue_error("decode visible queue message count", err))?,
        )?;
        let not_visible = query_count(
            client
                .query_one(
                    "SELECT COUNT(*) FROM sys_messages WHERE queue_name = $1 AND \
                     visibility_timestamp > $2 AND receipt_handle IS NOT NULL",
                    &[&queue_name, &now],
                )
                .await
                .map_err(|err| Self::map_queue_error("count invisible queue messages", err))?
                .try_get(0)
                .map_err(|err| {
                    Self::map_queue_error("decode invisible queue message count", err)
                })?,
        )?;
        let delayed = query_count(
            client
                .query_one(
                    "SELECT COUNT(*) FROM sys_messages WHERE queue_name = $1 AND \
                     visibility_timestamp > $2 AND receipt_handle IS NULL",
                    &[&queue_name, &now],
                )
                .await
                .map_err(|err| Self::map_queue_error("count delayed queue messages", err))?
                .try_get(0)
                .map_err(|err| Self::map_queue_error("decode delayed queue message count", err))?,
        )?;

        Ok(QueueMessageCounts {
            visible,
            not_visible,
            delayed,
        })
    }

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        self.retry_postgres_queue_conflicts("send_message", || {
            let message = message.clone();
            async move {
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

                let queue_name = stored_message
                    .queue_url
                    .split('/')
                    .next_back()
                    .ok_or_else(|| {
                        QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat)
                    })?
                    .to_string();
                let visibility_timestamp = stored_message.visibility_timestamp.unwrap_or_default();
                let created_at = *stored_message.created_at;

                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
                client
                    .execute(
                        sql_statements::send_message(),
                        &[
                            &message_id.to_string(),
                            &queue_name,
                            &stored_message.body,
                            &message_attributes_json,
                            &*visibility_timestamp,
                            &created_at,
                        ],
                    )
                    .await
                    .map_err(|err| Self::map_queue_error("send message", err))?;
                Ok(message_id)
            }
        })
        .await
    }

    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: storage_types::DurationSeconds,
        _wait_time_seconds: storage_types::DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>> {
        self.retry_postgres_queue_conflicts("receive_messages", || {
            let queue_url = queue_url.to_string();
            async move {
                let queue_name = queue_url
                    .split('/')
                    .next_back()
                    .ok_or_else(|| {
                        QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat)
                    })?
                    .to_string();
                let next_visible = visibility_timeout.time_from_now();
                let now = TimestampMillis::now();
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
                let rows = client
                    .query(
                        sql_statements::receive_and_claim_messages(),
                        &[&queue_name, &*now, &i64::from(max_messages), &*next_visible],
                    )
                    .await
                    .map_err(|err| Self::map_queue_error("receive and claim messages", err))?;

                let mut messages = Vec::new();
                for row in rows {
                    let message_id_raw: String = row
                        .try_get("message_id")
                        .map_err(|err| Self::map_queue_error("decode message_id", err))?;
                    let body: String = row
                        .try_get("body")
                        .map_err(|err| Self::map_queue_error("decode message body", err))?;
                    let message_attributes_raw: Option<String> = row
                        .try_get("message_attributes")
                        .map_err(|err| Self::map_queue_error("decode message attributes", err))?;
                    let created_at: i64 = row
                        .try_get("created_at")
                        .map_err(|err| Self::map_queue_error("decode message created_at", err))?;
                    let current_receipt: Option<String> =
                        row.try_get("receipt_handle").map_err(|err| {
                            Self::map_queue_error("decode message receipt_handle", err)
                        })?;

                    let message_id = Self::parse_message_id(&message_id_raw, "message_id")?;
                    let queue_message = QueueMessage {
                        message_id,
                        queue_url: queue_url.clone(),
                        body,
                        message_attributes: Self::parse_queue_message_attributes(
                            message_attributes_raw,
                        )?,
                        receipt_handle: current_receipt.as_deref().map(ReceiptHandle::from),
                        created_at: created_at.into(),
                        visibility_timestamp: None,
                    };
                    let next_receipt_handle = current_receipt
                        .as_deref()
                        .map(ReceiptHandle::from)
                        .unwrap_or_else(|| ReceiptHandle::new(*next_visible, Uuid::now_v7()));

                    let visible = VisibleQueueMessage::new(queue_message);
                    let invisible = visible.into_invisible(next_receipt_handle, next_visible);
                    if let Some(message) = invisible.into_message_response() {
                        messages.push(message);
                    }
                }

                Ok(messages)
            }
        })
        .await
    }

    async fn delete_message(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
    ) -> QueueResult<()> {
        self.retry_postgres_queue_conflicts("delete_message", || {
            let queue_url = queue_url.to_string();
            let receipt_handle = receipt_handle.clone();
            async move {
                let queue_name = queue_url
                    .split('/')
                    .next_back()
                    .ok_or_else(|| {
                        QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat)
                    })?
                    .to_string();
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
                let deleted_rows = client
                    .execute(
                        sql_statements::delete_message(),
                        &[&queue_name, &receipt_handle.0.as_str()],
                    )
                    .await
                    .map_err(|err| Self::map_queue_error("delete message", err))?;
                if deleted_rows == 0 {
                    return Err(QueueError::validation(
                        QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
                    ));
                }
                Ok(())
            }
        })
        .await
    }

    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        visibility_timeout: storage_types::DurationSeconds,
    ) -> QueueResult<()> {
        self.retry_postgres_queue_conflicts("change_message_visibility", || {
            let queue_url = queue_url.to_string();
            let receipt_handle = receipt_handle.clone();
            async move {
                let queue_name = queue_url
                    .split('/')
                    .next_back()
                    .ok_or_else(|| {
                        QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat)
                    })?
                    .to_string();
                let new_visibility = visibility_timeout.time_from_now();
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
                let updated_rows = client
                    .execute(
                        sql_statements::change_message_visibility(),
                        &[&*new_visibility, &queue_name, &receipt_handle.0.as_str()],
                    )
                    .await
                    .map_err(|err| Self::map_queue_error("change message visibility", err))?;
                if updated_rows == 0 {
                    return Err(QueueError::validation(
                        QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
                    ));
                }
                Ok(())
            }
        })
        .await
    }

    async fn update_message_snapshot_checkpoint(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()> {
        self.retry_postgres_queue_conflicts("update_message_snapshot_checkpoint", || {
            let queue_url = queue_url.to_string();
            let receipt_handle = receipt_handle.clone();
            let checkpoint_data = checkpoint_data.clone();
            async move {
                let queue_name = queue_url
                    .split('/')
                    .next_back()
                    .ok_or_else(|| {
                        QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat)
                    })?
                    .to_string();
                let client = self
                    .pool
                    .get()
                    .await
                    .map_err(|err| Self::map_queue_error("acquire postgres client", err))?;
                let updated_rows = client
                    .execute(
                        sql_statements::update_message_checkpoint(),
                        &[&checkpoint_data, &queue_name, &receipt_handle.0.as_str()],
                    )
                    .await
                    .map_err(|err| Self::map_queue_error("update message checkpoint", err))?;
                if updated_rows == 0 {
                    return Err(QueueError::validation(QueueValidationKind::MessageNotFound));
                }
                Ok(())
            }
        })
        .await
    }
}

fn query_count(count: i64) -> QueueResult<u64> {
    count.try_into().map_err(|err| {
        QueueError::internal_with_detail(
            queue_provider::QueueInternalKind::InvalidMessageVisibilityKeyFormat,
            err,
        )
    })
}
