use std::{sync::Arc, time::Duration};

use queue_provider::{
    BatchResultErrorEntry, ChangeMessageVisibilityBatchRequest,
    ChangeMessageVisibilityBatchResponse, ChangeMessageVisibilityBatchResultEntry,
    ChangeMessageVisibilityRequest, CreateQueueRequest, CreateQueueResponse,
    DeleteMessageBatchRequest, DeleteMessageBatchResponse, DeleteMessageBatchResultEntry,
    DeleteMessageRequest, DeleteQueueRequest, DeleteQueueResponse, GetQueueAttributesRequest,
    GetQueueAttributesResponse, GetQueueUrlRequest, GetQueueUrlResponse, IntoValidatedQueueRequest,
    ListQueuesRequest, ListQueuesResponse, MessageResponse, PublishMessageRequest,
    PurgeQueueRequest, PurgeQueueResponse, Queue, QueueError, QueueMessage, QueueMessageCounts,
    QueueOperations, QueueResult, ReceiptHandle, ReceiveMessageRequest, ReceiveMessageResponse,
    SendMessageBatchRequest, SendMessageBatchResponse, SendMessageBatchResultEntry,
    SendMessageRequest, SendMessageResponse, SetQueueAttributesRequest, SetQueueAttributesResponse,
    UpdateMessageSnapshotRequest,
};
use storage_types::{DurationSeconds, StorageEnum, TimestampMillis};
use tokio::time::Instant;
use tracing::instrument;

use crate::{
    constants::{
        DEFAULT_DELAY_SECONDS, DEFAULT_MAXIMUM_MESSAGE_SIZE, DEFAULT_MESSAGE_RETENTION_PERIOD,
        DEFAULT_RECEIVE_WAIT_TIME_SECONDS, DEFAULT_VISIBILITY_TIMEOUT_SECS,
        EMPTY_RECEIVE_POLL_INTERVAL, MAX_RECEIVE_WAIT_TIME_SECS, RECEIVE_CONFLICT_RETRY_ATTEMPTS,
        RECEIVE_CONFLICT_RETRY_DELAY,
    },
    operation_metrics::record_queue_operation,
};

pub struct QueueManager {
    storage: Arc<dyn QueueOperations>,
}

impl std::fmt::Debug for QueueManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueManager")
            .field("queue", &"Arc<dyn QueueOperations>")
            .finish()
    }
}

impl QueueManager {
    pub fn new(storage: Arc<dyn QueueOperations>) -> Self {
        Self { storage }
    }

    /// Return and clear pressure signals from the queue backend so the
    /// enclosing storage admission permit can observe consumed capacity.
    #[must_use]
    pub fn take_admission_pressure_signal(&self) -> bool {
        self.storage.take_admission_pressure_signal()
    }

    #[instrument(skip_all, fields(feature = "queue"))]
    pub async fn create_queue(
        &self,
        request: impl IntoValidatedQueueRequest<CreateQueueRequest>,
    ) -> QueueResult<CreateQueueResponse> {
        let request = request.into_validated()?;
        let queue_url = request.queue_name.clone();

        self.create_queue_with_url(request, queue_url).await
    }

    #[instrument(skip_all, fields(feature = "queue"))]
    pub async fn create_queue_with_url(
        &self,
        request: impl IntoValidatedQueueRequest<CreateQueueRequest>,
        queue_url: String,
    ) -> QueueResult<CreateQueueResponse> {
        let request = request.into_validated()?.into_inner();
        Queue::validate_url(&queue_url)?;
        let queue_name = request.queue_name;

        let queue = Queue {
            queue_name,
            queue_url: queue_url.clone(),
            attributes: request.attributes.unwrap_or_default(),
            created_at: TimestampMillis::now(),
        };

        let queue = self.storage.create_queue(queue).await.inspect_err(|_| {
            tracing::error!("queue.create.failed");
        })?;

        Ok(CreateQueueResponse {
            queue_url: queue.queue_url,
        })
    }

    pub async fn delete_queue(
        &self,
        request: impl IntoValidatedQueueRequest<DeleteQueueRequest>,
    ) -> QueueResult<DeleteQueueResponse> {
        let request = request.into_validated()?.into_inner();
        self.storage.delete_queue(&request.queue_url).await?;
        Ok(DeleteQueueResponse::default())
    }

    pub async fn list_queues(
        &self,
        request: impl IntoValidatedQueueRequest<ListQueuesRequest>,
    ) -> QueueResult<ListQueuesResponse> {
        let request = request.into_validated()?.into_inner();
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
        request: impl IntoValidatedQueueRequest<GetQueueUrlRequest>,
    ) -> QueueResult<GetQueueUrlResponse> {
        let request = request.into_validated()?.into_inner();
        let queue_url = self
            .storage
            .get_queue_url_by_name(&request.queue_name)
            .await?;
        let queue_url = queue_url.ok_or_else(|| queue_provider::QueueError::ResourceNotFound {
            resource_type: "queue",
            resource_id: request.queue_name.clone(),
        })?;
        Ok(GetQueueUrlResponse { queue_url })
    }

    pub async fn get_queue_attributes(
        &self,
        request: impl IntoValidatedQueueRequest<GetQueueAttributesRequest>,
    ) -> QueueResult<GetQueueAttributesResponse> {
        let request = request.into_validated()?.into_inner();
        let should_count = request.attribute_names.as_ref().is_none_or(|names| {
            names.iter().any(|name| {
                matches!(
                    name.as_str(),
                    "All"
                        | "ApproximateNumberOfMessages"
                        | "ApproximateNumberOfMessagesNotVisible"
                        | "ApproximateNumberOfMessagesDelayed"
                )
            })
        });
        let queue = if should_count {
            self.storage
                .get_queue_with_message_counts(&request.queue_url)
                .await?
        } else {
            self.storage
                .get_queue(&request.queue_url)
                .await?
                .map(|queue| (queue, QueueMessageCounts::default()))
        };
        let (queue, counts) =
            queue.ok_or_else(|| queue_provider::QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: request.queue_url.clone(),
            })?;
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
        request: impl IntoValidatedQueueRequest<SetQueueAttributesRequest>,
    ) -> QueueResult<SetQueueAttributesResponse> {
        let request = request.into_validated()?.into_inner();
        self.storage
            .set_queue_attributes(&request.queue_url, request.attributes)
            .await?;
        Ok(SetQueueAttributesResponse::default())
    }

    pub async fn purge_queue(
        &self,
        request: impl IntoValidatedQueueRequest<PurgeQueueRequest>,
    ) -> QueueResult<PurgeQueueResponse> {
        let request = request.into_validated()?.into_inner();
        self.storage.purge_queue(&request.queue_url).await?;
        Ok(PurgeQueueResponse::default())
    }

    /// Publish one stable identity without disturbing an existing pending
    /// delivery.
    #[instrument(skip_all, fields(feature = "queue"))]
    pub async fn publish_message(
        &self,
        request: impl IntoValidatedQueueRequest<PublishMessageRequest>,
    ) -> QueueResult<SendMessageResponse> {
        let request = request.into_validated()?.into_inner();
        let mut message =
            queue_provider::queue_message_for_send(request.message, TimestampMillis::now());
        message.message_id = request.message_id;
        let mut response = Self::prepare_message_integrity(&message);
        response.message_id =
            record_queue_operation("publish_message", self.storage.publish_message(message))
                .await?;
        Ok(response)
    }

    fn prepare_message_integrity(message: &QueueMessage) -> SendMessageResponse {
        SendMessageResponse {
            message_id: message.message_id,
            md5_of_body: format!("{:x}", md5::compute(message.body.as_bytes())),
            md5_of_message_attributes: message
                .message_attributes
                .as_ref()
                .and_then(queue_provider::md5_of_message_attributes),
        }
    }

    #[instrument(skip_all, fields(feature = "queue", delay_seconds = tracing::field::Empty, message_body_size = tracing::field::Empty))]
    pub async fn send_message(
        &self,
        request: impl IntoValidatedQueueRequest<SendMessageRequest>,
    ) -> QueueResult<SendMessageResponse> {
        let request = request.into_validated()?.into_inner();
        let now = TimestampMillis::now();

        tracing::Span::current().record(
            "delay_seconds",
            i64::from(request.delay_seconds.unwrap_or(0)),
        );
        tracing::Span::current().record("message_body_size", request.message_body.len());

        let message = queue_provider::queue_message_for_send(request, now);
        let mut response = Self::prepare_message_integrity(&message);
        let assigned_id =
            record_queue_operation("send_message", self.storage.send_message(message))
                .await
                .inspect_err(|_| {
                    tracing::error!("queue.message.send.failed");
                })?;

        response.message_id = assigned_id;
        Ok(response)
    }

    pub async fn send_message_batch(
        &self,
        request: impl IntoValidatedQueueRequest<SendMessageBatchRequest>,
    ) -> QueueResult<SendMessageBatchResponse> {
        let request = request.into_validated()?.into_inner();
        let now = TimestampMillis::now();
        let mut entry_ids = Vec::with_capacity(request.entries.len());
        let mut response_metadata = Vec::with_capacity(request.entries.len());
        let mut messages = Vec::with_capacity(request.entries.len());
        for entry in request.entries {
            let md5_of_message_attributes = entry
                .message_attributes
                .as_ref()
                .and_then(queue_provider::md5_of_message_attributes);
            let md5_of_body = format!("{:x}", md5::compute(entry.message_body.as_bytes()));
            let entry_id = entry.id.clone();
            messages.push(queue_provider::queue_message_for_send(
                SendMessageRequest {
                    queue_url: request.queue_url.clone(),
                    message_body: entry.message_body,
                    delay_seconds: entry.delay_seconds,
                    message_attributes: entry.message_attributes,
                },
                now,
            ));
            entry_ids.push(entry_id);
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

    #[instrument(skip_all, fields(feature = "queue", max_messages = tracing::field::Empty, visibility_timeout = tracing::field::Empty, messages_received = tracing::field::Empty))]
    pub async fn receive_message(
        &self,
        request: impl IntoValidatedQueueRequest<ReceiveMessageRequest>,
    ) -> QueueResult<ReceiveMessageResponse> {
        let request = request.into_validated()?.into_inner();
        let max_messages = request
            .max_number_of_messages
            .unwrap_or(queue_provider::DEFAULT_RECEIVE_MAX_MESSAGES);
        let visibility_timeout = request
            .visibility_timeout
            .unwrap_or(DEFAULT_VISIBILITY_TIMEOUT_SECS);
        let wait_time_seconds = request
            .wait_time_seconds
            .unwrap_or(queue_provider::DEFAULT_RECEIVE_WAIT_TIME_SECONDS)
            .min(MAX_RECEIVE_WAIT_TIME_SECS);

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

    pub async fn delete_message(
        &self,
        request: impl IntoValidatedQueueRequest<DeleteMessageRequest>,
    ) -> QueueResult<()> {
        let request = request.into_validated()?.into_inner();
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
        request: impl IntoValidatedQueueRequest<DeleteMessageBatchRequest>,
    ) -> QueueResult<DeleteMessageBatchResponse> {
        let request = request.into_validated()?.into_inner();
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
        request: impl IntoValidatedQueueRequest<ChangeMessageVisibilityRequest>,
    ) -> QueueResult<()> {
        let request = request.into_validated()?.into_inner();
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
        request: impl IntoValidatedQueueRequest<ChangeMessageVisibilityBatchRequest>,
    ) -> QueueResult<ChangeMessageVisibilityBatchResponse> {
        let request = request.into_validated()?.into_inner();
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
        let UpdateMessageSnapshotRequest {
            queue_url,
            receipt_handle,
            checkpoint_data,
        } = request;
        self.storage
            .update_message_snapshot_checkpoint(
                &queue_url,
                ReceiptHandle(receipt_handle),
                checkpoint_data,
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
        .inspect_err(|_| {
            tracing::error!("queue.messages.receive.failed");
        })
    }
}

fn batch_error_entry(id: String, error: &QueueError) -> BatchResultErrorEntry {
    BatchResultErrorEntry {
        id,
        sender_fault: error.is_sender_fault(),
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
    matches!(error, QueueError::StorageError(error) | QueueError::TransactWrite(error)
        if matches!(error.base_error(), Some(StorageEnum::TransactionConflict { .. })))
}

fn selected_queue_attributes(
    mut stored: std::collections::HashMap<String, String>,
    counts: QueueMessageCounts,
    requested: Option<&[String]>,
) -> std::collections::HashMap<String, String> {
    let is_requested = |name: &str| {
        requested.is_none_or(|names| names.iter().any(|value| value == "All" || value == name))
    };
    stored.retain(|name, _| is_requested(name));
    for (name, value) in [
        ("DelaySeconds", DEFAULT_DELAY_SECONDS),
        ("VisibilityTimeout", DEFAULT_VISIBILITY_TIMEOUT_SECS),
        ("MaximumMessageSize", DEFAULT_MAXIMUM_MESSAGE_SIZE),
        ("MessageRetentionPeriod", DEFAULT_MESSAGE_RETENTION_PERIOD),
        (
            "ReceiveMessageWaitTimeSeconds",
            DEFAULT_RECEIVE_WAIT_TIME_SECONDS,
        ),
    ] {
        if is_requested(name) {
            stored
                .entry(name.to_string())
                .or_insert_with(|| value.to_string());
        }
    }
    for (name, value) in [
        ("ApproximateNumberOfMessages", counts.visible),
        ("ApproximateNumberOfMessagesNotVisible", counts.not_visible),
        ("ApproximateNumberOfMessagesDelayed", counts.delayed),
    ] {
        if is_requested(name) {
            stored.insert(name.to_string(), value.to_string());
        }
    }
    stored
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

#[cfg(test)]
mod batch_error_tests;

#[cfg(test)]
mod attribute_projection_tests;
