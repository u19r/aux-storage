use std::{sync::Arc, time::Duration};

use queue_provider::{
    BatchResultErrorEntry, ChangeMessageVisibilityBatchRequest,
    ChangeMessageVisibilityBatchResponse, ChangeMessageVisibilityBatchResultEntry,
    ChangeMessageVisibilityRequest, CreateQueueRequest, CreateQueueResponse,
    DeleteMessageBatchRequest, DeleteMessageBatchResponse, DeleteMessageBatchResultEntry,
    DeleteMessageRequest, DeleteQueueRequest, DeleteQueueResponse, GetQueueAttributesRequest,
    GetQueueAttributesResponse, GetQueueUrlRequest, GetQueueUrlResponse, ListQueuesRequest,
    ListQueuesResponse, MessageId, MessageResponse, PurgeQueueRequest, PurgeQueueResponse, Queue,
    QueueError, QueueMessage, QueueMessageCounts, QueueResult, ReceiptHandle,
    ReceiveMessageRequest, ReceiveMessageResponse, SendMessageBatchRequest,
    SendMessageBatchResponse, SendMessageBatchResultEntry, SendMessageRequest, SendMessageResponse,
    SetQueueAttributesRequest, SetQueueAttributesResponse, UpdateMessageSnapshotRequest,
};
use storage_types::{DurationSeconds, StorageEnum, TimestampMillis};
use tokio::time::Instant;
use tracing::instrument;

use crate::{
    QueueProvider,
    constants::{
        DEFAULT_DELAY_SECONDS, DEFAULT_MAXIMUM_MESSAGE_SIZE, DEFAULT_MESSAGE_RETENTION_PERIOD,
        DEFAULT_RECEIVE_WAIT_TIME_SECONDS, DEFAULT_VISIBILITY_TIMEOUT_SECS,
        EMPTY_RECEIVE_POLL_INTERVAL, MAX_RECEIVE_WAIT_TIME_SECS, RECEIVE_CONFLICT_RETRY_ATTEMPTS,
        RECEIVE_CONFLICT_RETRY_DELAY,
    },
    operation_metrics::record_queue_operation,
};

pub struct QueueManager {
    storage: Arc<dyn QueueProvider>,
}

impl std::fmt::Debug for QueueManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueManager")
            .field("queue", &"Arc<dyn QueueProvider>")
            .finish()
    }
}

impl QueueManager {
    pub fn new(storage: Arc<dyn QueueProvider>) -> Self {
        Self { storage }
    }

    #[instrument(skip(self), fields(feature = "queue", queue_name = tracing::field::Empty))]
    pub async fn create_queue(
        &self,
        request: CreateQueueRequest,
    ) -> QueueResult<CreateQueueResponse> {
        request.validate()?;
        let queue_url = request.queue_name.clone();

        self.create_queue_with_url(request, queue_url).await
    }

    #[instrument(skip(self), fields(feature = "queue", queue_name = tracing::field::Empty))]
    pub async fn create_queue_with_url(
        &self,
        request: CreateQueueRequest,
        queue_url: String,
    ) -> QueueResult<CreateQueueResponse> {
        request.validate()?;
        Queue::validate_url(&queue_url)?;
        let queue_name = request.queue_name;

        tracing::Span::current().record("queue_name", &queue_name);

        let queue = Queue {
            queue_name,
            queue_url: queue_url.clone(),
            attributes: request.attributes.unwrap_or_default(),
            created_at: TimestampMillis::now(),
        };

        self.storage.create_queue(queue).await.map_err(|e| {
            tracing::error!(error = %e, "queue.create.failed");
            e
        })?;

        Ok(CreateQueueResponse { queue_url })
    }

    pub async fn delete_queue(
        &self,
        request: DeleteQueueRequest,
    ) -> QueueResult<DeleteQueueResponse> {
        request.validate()?;
        self.storage.delete_queue(&request.queue_url).await?;
        Ok(DeleteQueueResponse::default())
    }

    pub async fn list_queues(&self, request: ListQueuesRequest) -> QueueResult<ListQueuesResponse> {
        request.validate()?;
        let queues = self
            .storage
            .list_queues(request.queue_name_prefix.as_deref())
            .await?;
        Ok(ListQueuesResponse {
            queue_urls: queues.into_iter().map(|queue| queue.queue_url).collect(),
        })
    }

    pub async fn get_queue_url(
        &self,
        request: GetQueueUrlRequest,
    ) -> QueueResult<GetQueueUrlResponse> {
        request.validate()?;
        let queue = self.storage.get_queue_by_name(&request.queue_name).await?;
        let queue = queue.ok_or_else(|| queue_provider::QueueError::ResourceNotFound {
            resource_type: "queue",
            resource_id: request.queue_name.clone(),
        })?;
        Ok(GetQueueUrlResponse {
            queue_url: queue.queue_url,
        })
    }

    pub async fn get_queue_attributes(
        &self,
        request: GetQueueAttributesRequest,
    ) -> QueueResult<GetQueueAttributesResponse> {
        request.validate()?;
        let queue = self.storage.get_queue(&request.queue_url).await?;
        let queue = queue.ok_or_else(|| queue_provider::QueueError::ResourceNotFound {
            resource_type: "queue",
            resource_id: request.queue_url.clone(),
        })?;
        let counts = self
            .storage
            .get_queue_message_counts(&request.queue_url)
            .await?;
        Ok(GetQueueAttributesResponse {
            attributes: selected_queue_attributes(
                queue.attributes,
                counts,
                request.attribute_names.as_deref(),
            ),
        })
    }

    pub async fn set_queue_attributes(
        &self,
        request: SetQueueAttributesRequest,
    ) -> QueueResult<SetQueueAttributesResponse> {
        request.validate()?;
        self.storage
            .set_queue_attributes(&request.queue_url, request.attributes)
            .await?;
        Ok(SetQueueAttributesResponse::default())
    }

    pub async fn purge_queue(&self, request: PurgeQueueRequest) -> QueueResult<PurgeQueueResponse> {
        request.validate()?;
        self.storage.purge_queue(&request.queue_url).await?;
        Ok(PurgeQueueResponse::default())
    }

    #[instrument(skip(self, request), fields(feature = "queue", queue_url = tracing::field::Empty, delay_seconds = tracing::field::Empty, message_body_size = tracing::field::Empty, message_id = tracing::field::Empty))]
    pub async fn send_message(
        &self,
        request: SendMessageRequest,
    ) -> QueueResult<SendMessageResponse> {
        request.validate()?;
        let now = TimestampMillis::now();

        tracing::Span::current().record("queue_url", &request.queue_url);
        tracing::Span::current().record(
            "delay_seconds",
            i64::from(request.delay_seconds.unwrap_or(0)),
        );
        tracing::Span::current().record("message_body_size", request.message_body.len());

        let delay_seconds: DurationSeconds =
            DurationSeconds::from(request.delay_seconds.unwrap_or(0));

        let visibility_timestamp = Some(now + delay_seconds);
        let md5_of_message_attributes = request
            .message_attributes
            .as_ref()
            .and_then(queue_provider::md5_of_message_attributes);

        let message = QueueMessage {
            message_id: MessageId::default(),
            queue_url: request.queue_url,
            body: request.message_body.clone(),
            message_attributes: request.message_attributes,
            receipt_handle: None,
            created_at: now,
            visibility_timestamp,
        };

        let assigned_id =
            record_queue_operation("send_message", self.storage.send_message(message))
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, "queue.message.send.failed");
                    e
                })?;

        tracing::Span::current().record("message_id", assigned_id.to_string());

        let md5_hash = format!("{:x}", md5::compute(request.message_body.as_bytes()));

        Ok(SendMessageResponse {
            message_id: assigned_id,
            md5_of_body: md5_hash,
            md5_of_message_attributes,
        })
    }

    pub async fn send_message_batch(
        &self,
        request: SendMessageBatchRequest,
    ) -> QueueResult<SendMessageBatchResponse> {
        request.validate()?;
        let now = TimestampMillis::now();
        let mut entry_ids = Vec::with_capacity(request.entries.len());
        let mut response_metadata = Vec::with_capacity(request.entries.len());
        let mut messages = Vec::with_capacity(request.entries.len());
        for entry in request.entries {
            let delay_seconds = DurationSeconds::from(entry.delay_seconds.unwrap_or(0));
            let visibility_timestamp = Some(now + delay_seconds);
            let md5_of_message_attributes = entry
                .message_attributes
                .as_ref()
                .and_then(queue_provider::md5_of_message_attributes);
            let md5_of_body = format!("{:x}", md5::compute(entry.message_body.as_bytes()));
            messages.push(QueueMessage {
                message_id: MessageId::default(),
                queue_url: request.queue_url.clone(),
                body: entry.message_body,
                message_attributes: entry.message_attributes,
                receipt_handle: None,
                created_at: now,
                visibility_timestamp,
            });
            entry_ids.push(entry.id);
            response_metadata.push((md5_of_body, md5_of_message_attributes));
        }

        let send_results =
            record_queue_operation("send_message_batch", self.storage.send_messages(messages))
                .await?;

        let mut successful = Vec::with_capacity(send_results.len());
        let mut failed = Vec::new();
        for ((id, metadata), result) in entry_ids
            .into_iter()
            .zip(response_metadata)
            .zip(send_results)
        {
            match result {
                Ok(message_id) => successful.push(SendMessageBatchResultEntry {
                    id,
                    message_id,
                    md5_of_message_body: metadata.0,
                    md5_of_message_attributes: metadata.1,
                }),
                Err(error) => failed.push(batch_error_entry(id, &error)),
            }
        }

        Ok(SendMessageBatchResponse { successful, failed })
    }

    #[instrument(skip(self), fields(feature = "queue", queue_url = tracing::field::Empty, max_messages = tracing::field::Empty, visibility_timeout = tracing::field::Empty, messages_received = tracing::field::Empty))]
    pub async fn receive_message(
        &self,
        request: ReceiveMessageRequest,
    ) -> QueueResult<ReceiveMessageResponse> {
        request.validate()?;
        let max_messages = request.max_number_of_messages.unwrap_or(1);
        let visibility_timeout = request
            .visibility_timeout
            .unwrap_or(DEFAULT_VISIBILITY_TIMEOUT_SECS);
        let wait_time_seconds = request
            .wait_time_seconds
            .unwrap_or(0)
            .min(MAX_RECEIVE_WAIT_TIME_SECS);

        tracing::Span::current().record("queue_url", &request.queue_url);
        tracing::Span::current().record("max_messages", i64::from(max_messages));
        tracing::Span::current().record("visibility_timeout", i64::from(visibility_timeout));

        let messages = self
            .receive_messages_with_wait_budget(
                &request.queue_url,
                max_messages,
                visibility_timeout.into(),
                wait_time_seconds,
            )
            .await?;

        tracing::Span::current().record("messages_received", messages.len());
        Ok(ReceiveMessageResponse { messages })
    }

    pub async fn delete_message(&self, request: DeleteMessageRequest) -> QueueResult<()> {
        request.validate()?;
        let receipt_handle = request.receipt_handle;
        record_queue_operation(
            "delete_message",
            self.storage
                .delete_message(&request.queue_url, receipt_handle.as_str().into()),
        )
        .await
        .map_err(|error| receipt_handle_error_with_value(error, &receipt_handle))
    }

    pub async fn delete_message_batch(
        &self,
        request: DeleteMessageBatchRequest,
    ) -> QueueResult<DeleteMessageBatchResponse> {
        request.validate()?;
        let mut successful = Vec::with_capacity(request.entries.len());
        let mut failed = Vec::new();

        let entries = request.entries;
        let receipt_handles = entries
            .iter()
            .map(|entry| entry.receipt_handle.as_str().into())
            .collect();
        let results = record_queue_operation(
            "delete_message_batch",
            self.storage
                .delete_messages(&request.queue_url, receipt_handles),
        )
        .await;

        match results {
            Ok(results) => {
                for (entry, result) in entries.into_iter().zip(results) {
                    match result.map_err(|error| {
                        receipt_handle_error_with_value(error, &entry.receipt_handle)
                    }) {
                        Ok(()) => successful.push(DeleteMessageBatchResultEntry { id: entry.id }),
                        Err(error) => failed.push(batch_error_entry(entry.id, &error)),
                    }
                }
            }
            Err(error) => {
                for entry in entries {
                    failed.push(batch_error_entry(entry.id, &error));
                }
            }
        }

        Ok(DeleteMessageBatchResponse { successful, failed })
    }

    pub async fn change_message_visibility(
        &self,
        request: ChangeMessageVisibilityRequest,
    ) -> QueueResult<()> {
        request.validate()?;
        record_queue_operation(
            "change_message_visibility",
            self.storage.change_message_visibility(
                &request.queue_url,
                request.receipt_handle.as_str().into(),
                request.visibility_timeout.into(),
            ),
        )
        .await
        .map_err(|error| receipt_handle_error_with_value(error, &request.receipt_handle))
    }

    pub async fn change_message_visibility_batch(
        &self,
        request: ChangeMessageVisibilityBatchRequest,
    ) -> QueueResult<ChangeMessageVisibilityBatchResponse> {
        request.validate()?;
        let mut successful = Vec::with_capacity(request.entries.len());
        let mut failed = Vec::new();

        let entries = request.entries;
        let visibility_entries = entries
            .iter()
            .map(|entry| {
                (
                    entry.receipt_handle.as_str().into(),
                    entry.visibility_timeout.into(),
                )
            })
            .collect();
        let results = record_queue_operation(
            "change_message_visibility_batch",
            self.storage
                .change_message_visibilities(&request.queue_url, visibility_entries),
        )
        .await;

        match results {
            Ok(results) => {
                for (entry, result) in entries.into_iter().zip(results) {
                    match result.map_err(|error| {
                        receipt_handle_error_with_value(error, &entry.receipt_handle)
                    }) {
                        Ok(()) => {
                            successful
                                .push(ChangeMessageVisibilityBatchResultEntry { id: entry.id });
                        }
                        Err(error) => failed.push(batch_error_entry(entry.id, &error)),
                    }
                }
            }
            Err(error) => {
                for entry in entries {
                    failed.push(batch_error_entry(entry.id, &error));
                }
            }
        }

        Ok(ChangeMessageVisibilityBatchResponse { successful, failed })
    }

    pub async fn update_message_snapshot(
        &self,
        request: UpdateMessageSnapshotRequest,
    ) -> QueueResult<()> {
        // For the new implementation, we need to extract receipt handle from the
        // request Since UpdateMessageSnapshotRequest still uses message_id,
        // we'll create a receipt handle

        self.storage
            .update_message_snapshot_checkpoint(
                &request.queue_url,
                request.receipt_handle.as_str().into(),
                request.checkpoint_data,
            )
            .await
    }

    async fn receive_messages_with_wait_budget(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        wait_time_seconds: u32,
    ) -> QueueResult<Vec<MessageResponse>> {
        let deadline = receive_wait_deadline(wait_time_seconds);
        let mut provider_wait_time_seconds = wait_time_seconds;
        let mut conflict_retries_remaining = RECEIVE_CONFLICT_RETRY_ATTEMPTS;

        loop {
            let messages = match self
                .receive_messages_once(
                    queue_url,
                    max_messages,
                    visibility_timeout,
                    provider_wait_time_seconds.into(),
                )
                .await
            {
                Ok(messages) => messages,
                Err(error) if is_transaction_conflict(&error) && conflict_retries_remaining > 0 => {
                    conflict_retries_remaining -= 1;
                    tokio::time::sleep(RECEIVE_CONFLICT_RETRY_DELAY).await;
                    provider_wait_time_seconds = 0;
                    continue;
                }
                Err(error) => return Err(error),
            };

            if !messages.is_empty() || wait_budget_exhausted(deadline) {
                return Ok(messages);
            }

            tokio::time::sleep(remaining_wait_time(deadline).min(EMPTY_RECEIVE_POLL_INTERVAL))
                .await;
            provider_wait_time_seconds = 0;
        }
    }

    async fn receive_messages_once(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        wait_time_seconds: DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>> {
        record_queue_operation(
            "receive_message",
            self.storage.receive_messages(
                queue_url,
                max_messages,
                visibility_timeout,
                wait_time_seconds,
            ),
        )
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "queue.messages.receive.failed");
            error
        })
    }
}

fn batch_error_entry(id: String, error: &QueueError) -> BatchResultErrorEntry {
    BatchResultErrorEntry {
        id,
        sender_fault: true,
        code: error.aws_query_error_type().to_string(),
        message: error.aws_query_message(),
    }
}

fn receipt_handle_error_with_value(
    error: QueueError,
    receipt_handle: &ReceiptHandle,
) -> QueueError {
    match error {
        QueueError::Validation {
            kind:
                queue_provider::QueueValidationKind::MessageNotFound
                | queue_provider::QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
            ..
        } => QueueError::validation_with_detail(
            queue_provider::QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
            format!(
                "The input receipt handle \"{}\" is not a valid receipt handle.",
                receipt_handle
            ),
        ),
        other => other,
    }
}

fn is_transaction_conflict(error: &QueueError) -> bool {
    matches!(
        error,
        QueueError::StorageError(storage_types::StorageError::Base(
            StorageEnum::TransactionConflict { .. }
        )) | QueueError::TransactWrite(storage_types::StorageError::Base(
            StorageEnum::TransactionConflict { .. }
        ))
    )
}

fn selected_queue_attributes(
    mut stored: std::collections::HashMap<String, String>,
    counts: QueueMessageCounts,
    requested: Option<&[String]>,
) -> std::collections::HashMap<String, String> {
    stored
        .entry("DelaySeconds".to_string())
        .or_insert_with(|| DEFAULT_DELAY_SECONDS.to_string());
    stored
        .entry("VisibilityTimeout".to_string())
        .or_insert_with(|| DEFAULT_VISIBILITY_TIMEOUT_SECS.to_string());
    stored
        .entry("MaximumMessageSize".to_string())
        .or_insert_with(|| DEFAULT_MAXIMUM_MESSAGE_SIZE.to_string());
    stored
        .entry("MessageRetentionPeriod".to_string())
        .or_insert_with(|| DEFAULT_MESSAGE_RETENTION_PERIOD.to_string());
    stored
        .entry("ReceiveMessageWaitTimeSeconds".to_string())
        .or_insert_with(|| DEFAULT_RECEIVE_WAIT_TIME_SECONDS.to_string());
    stored.insert(
        "ApproximateNumberOfMessages".to_string(),
        counts.visible.to_string(),
    );
    stored.insert(
        "ApproximateNumberOfMessagesNotVisible".to_string(),
        counts.not_visible.to_string(),
    );
    stored.insert(
        "ApproximateNumberOfMessagesDelayed".to_string(),
        counts.delayed.to_string(),
    );

    let Some(requested) = requested else {
        return stored;
    };
    if requested.iter().any(|name| name == "All") {
        return stored;
    }

    requested
        .iter()
        .filter_map(|name| stored.get(name).map(|value| (name.clone(), value.clone())))
        .collect()
}

fn receive_wait_deadline(wait_time_seconds: u32) -> Option<Instant> {
    if wait_time_seconds == 0 {
        return None;
    }

    Some(Instant::now() + Duration::from_secs(u64::from(wait_time_seconds)))
}

fn wait_budget_exhausted(deadline: Option<Instant>) -> bool {
    deadline.is_none_or(|deadline| Instant::now() >= deadline)
}

fn remaining_wait_time(deadline: Option<Instant>) -> Duration {
    deadline
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
        .unwrap_or_default()
}
