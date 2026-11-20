use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc, time::Duration};

use queue_provider::{
    BatchResultErrorEntry, ChangeMessageVisibilityBatchRequest,
    ChangeMessageVisibilityBatchResponse, ChangeMessageVisibilityBatchResultEntry,
    ChangeMessageVisibilityRequest, CreateQueueRequest, CreateQueueResponse,
    DeleteMessageBatchRequest, DeleteMessageBatchResponse, DeleteMessageBatchResultEntry,
    DeleteMessageRequest, DeleteQueueRequest, DeleteQueueResponse, GetQueueAttributesRequest,
    GetQueueAttributesResponse, GetQueueUrlRequest, GetQueueUrlResponse,
    ListDeadLetterSourceQueuesRequest, ListDeadLetterSourceQueuesResponse, ListQueueTagsRequest,
    ListQueueTagsResponse, ListQueuesRequest, ListQueuesResponse, MessageResponse,
    PublishMessageRequest, PurgeQueueRequest, PurgeQueueResponse, Queue, QueueArn, QueueError,
    QueueMessage, QueueMessageCounts, QueueOperations, QueueResult, ReceiptHandle,
    ReceiveMessageRequest, ReceiveMessageResponse, ReceivedMessages, SendMessageBatchRequest,
    SendMessageBatchResponse, SendMessageBatchResultEntry, SendMessageRequest, SendMessageResponse,
    SetQueueAttributesRequest, SetQueueAttributesResponse, TagQueueRequest, UntagQueueRequest,
    UpdateMessageSnapshotRequest, ValidatedQueueRequest, validated_u32,
};
use storage_types::{DurationSeconds, TimestampMillis};
use tokio::time::Instant;
use tracing::instrument;

use super::{
    receive_wait::{Observed, QueueWaitState, ReceiveWaiters, instant_now},
    wake_handle::QueueWakeHandle,
};
use crate::{
    constants::{
        DEFAULT_MAX_WAITING_RECEIVES, EMPTY_CLAIM_BACKOFF, MAX_RECEIVE_WAIT_TIME_SECS,
        RECEIVE_PROBE_INTERVAL,
    },
    operation_metrics::record_queue_operation,
};

pub struct QueueManager {
    storage: Arc<dyn QueueOperations>,
    /// Partition, region and account of queue ARNs (review C2).
    pub(crate) identity: storage_types::MessagingIdentity,
    /// Shared by managers of one process so that a commit through any of
    /// them wakes the waiting receives of the others.
    waiters: Arc<ReceiveWaiters>,
    /// The public base of queue URLs; `dispatch` requires it.
    pub(crate) endpoint: Option<QueueEndpoint>,
}

/// The public base URL of the SQS route, such as `https://aux.example/queue`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueEndpoint {
    pub public_base_url: String,
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
        let wake = storage.queue_wake_board();
        Self {
            storage,
            identity: storage_types::MessagingIdentity::default(),
            waiters: Arc::new(ReceiveWaiters::new(
                DEFAULT_MAX_WAITING_RECEIVES,
                RECEIVE_PROBE_INTERVAL,
                wake,
            )),
            endpoint: None,
        }
    }

    /// Queue URLs become `{public_base_url}/{account}/{name}`.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: QueueEndpoint) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// The URL of `queue_name` under the configured endpoint and account.
    #[must_use]
    pub fn queue_url(&self, queue_name: &str) -> Option<String> {
        self.endpoint.as_ref().map(|endpoint| {
            format!(
                "{}/{}/{queue_name}",
                endpoint.public_base_url.trim_end_matches('/'),
                self.identity.account_id
            )
        })
    }

    /// The URL that a response gives for a queue that storage reported as
    /// `stored` (a URL or a name): built from this node's endpoint and the
    /// name when one is configured, else `stored` as it is (a remote
    /// provider's URL or a bare name).
    fn public_queue_url(&self, stored: String) -> String {
        self.queue_url(queue_provider::queue_name_of(&stored))
            .unwrap_or(stored)
    }

    /// The partition, region and account of the `QueueArn` attribute and of
    /// the dead-letter targets that `RedrivePolicy` accepts.
    #[must_use]
    pub fn with_messaging_identity(mut self, identity: storage_types::MessagingIdentity) -> Self {
        self.identity = identity;
        self
    }

    /// The waiting receives of this manager, for another domain that commits
    /// messages into its queues on the same store (an SNS delivery). Take it
    /// after `with_max_waiting_receives` and `with_receive_probe_interval`:
    /// those replace the waiter set.
    #[must_use]
    pub fn wake_handle(&self) -> QueueWakeHandle {
        QueueWakeHandle::new(Arc::clone(&self.waiters))
    }

    /// Bound the receives that wait at the same time. A receive over the bound
    /// returns its empty result at once, which a long poll may do.
    #[must_use]
    pub fn with_max_waiting_receives(mut self, max_waiting: usize) -> Self {
        self.waiters = Arc::new(ReceiveWaiters::new(
            max_waiting,
            self.waiters.probe_interval(),
            self.waiters.wake_board(),
        ));
        self
    }

    /// Change how long one probe observation of a queue stays fresh. It
    /// bounds the delay for messages that another process commits.
    #[must_use]
    pub fn with_receive_probe_interval(mut self, interval: Duration) -> Self {
        self.waiters = Arc::new(ReceiveWaiters::new(
            self.waiters.max_waiting(),
            interval,
            self.waiters.wake_board(),
        ));
        self
    }

    /// Receives of this process that are waiting now.
    #[must_use]
    pub fn waiting_receives(&self) -> usize {
        self.waiters.waiting()
    }

    /// Create a queue. With a `QueueEndpoint` its URL is
    /// `{base}/{account}/{name}`; without one the URL is the name.
    #[instrument(skip_all, fields(feature = "queue"))]
    pub async fn create_queue(
        &self,
        request: CreateQueueRequest,
    ) -> QueueResult<CreateQueueResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        let queue_url = self
            .queue_url(&request.queue_name)
            .unwrap_or_else(|| request.queue_name.clone());
        Queue::validate_url(&queue_url)?;
        let attributes = request.attributes.unwrap_or_default();
        queue_provider::check_redrive_identity(&attributes, &self.identity)?;
        let now = TimestampMillis::now();
        let queue = Queue {
            queue_name: request.queue_name,
            queue_url,
            attributes,
            created_at: now,
            updated_at: now,
            tags: request.tags.unwrap_or_default(),
        };

        let queue = self.storage.create_queue(queue).await.inspect_err(|_| {
            tracing::error!("queue.create.failed");
        })?;

        Ok(CreateQueueResponse {
            queue_url: self.public_queue_url(queue.queue_url),
        })
    }

    pub async fn delete_queue(
        &self,
        request: DeleteQueueRequest,
    ) -> QueueResult<DeleteQueueResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        self.storage.delete_queue(&request.queue_url).await?;
        self.waiters.forget(&request.queue_url);
        Ok(DeleteQueueResponse::default())
    }

    /// One page of queue URLs in name order. `NextToken` is returned only
    /// when `MaxResults` was given and more queues remain (AWS documentation).
    pub async fn list_queues(&self, request: ListQueuesRequest) -> QueueResult<ListQueuesResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        let prefix = request.queue_name_prefix.as_deref();
        let after = page_token_name(request.next_token.as_deref(), prefix)?;
        let limit = request
            .max_results
            .and_then(|max| NonZeroU32::new(validated_u32(max)))
            .unwrap_or(queue_provider::MAX_LIST_QUEUES_RESULTS);
        let (queues, last) = self
            .storage
            .list_queue_page(prefix, after.as_deref(), limit)
            .await?;
        Ok(ListQueuesResponse {
            queue_urls: queues
                .into_iter()
                .map(|queue| self.public_queue_url(queue.queue_url))
                .collect(),
            next_token: last
                .filter(|_| request.max_results.is_some())
                .map(|name| queue_provider::encode_page_token(&name)),
        })
    }

    /// `TagQueue`: add or replace tags under the shared tag rule.
    pub async fn tag_queue(&self, request: TagQueueRequest) -> QueueResult<()> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        self.storage
            .change_queue_tags(&request.queue_url, request.tags, Vec::new())
            .await
    }

    /// `UntagQueue`: an unknown key is not an error.
    pub async fn untag_queue(&self, request: UntagQueueRequest) -> QueueResult<()> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        self.storage
            .change_queue_tags(&request.queue_url, BTreeMap::new(), request.tag_keys)
            .await
    }

    /// `ListQueueTags`: one catalog read.
    pub async fn list_queue_tags(
        &self,
        request: ListQueueTagsRequest,
    ) -> QueueResult<ListQueueTagsResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        let tags = self
            .storage
            .list_queue_tags(&request.queue_url)
            .await?
            .ok_or_else(|| queue_not_found(&request.queue_url))?;
        Ok(ListQueueTagsResponse { tags })
    }

    pub async fn get_queue_url(
        &self,
        request: GetQueueUrlRequest,
    ) -> QueueResult<GetQueueUrlResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        // Not probed: another owner account names no queue of this deployment.
        if request
            .queue_owner_aws_account_id
            .as_ref()
            .is_some_and(|account| *account != self.identity.account_id)
        {
            return Err(queue_not_found(&request.queue_name));
        }
        let queue_url = self
            .storage
            .get_queue_url_by_name(&request.queue_name)
            .await?;
        let queue_url = queue_url.ok_or_else(|| queue_provider::QueueError::ResourceNotFound {
            resource_type: "queue",
            resource_id: request.queue_name.clone(),
        })?;
        Ok(GetQueueUrlResponse {
            queue_url: self.public_queue_url(queue_url),
        })
    }

    pub async fn get_queue_attributes(
        &self,
        request: GetQueueAttributesRequest,
    ) -> QueueResult<GetQueueAttributesResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        let names = request.attribute_names.as_deref().unwrap_or_default();
        // Probe 7: no names select no attributes, after one catalog read
        // that proves the queue exists; only count names add the count scan.
        let should_count = names.iter().any(|name| {
            matches!(
                name.as_str(),
                "All"
                    | "ApproximateNumberOfMessages"
                    | "ApproximateNumberOfMessagesNotVisible"
                    | "ApproximateNumberOfMessagesDelayed"
            )
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
        let (queue, counts) = queue.ok_or_else(|| queue_not_found(&request.queue_url))?;
        let arn = QueueArn::of(&self.identity, &queue.queue_name).to_string();
        let computed = [
            ("QueueArn", arn),
            ("CreatedTimestamp", epoch_seconds(queue.created_at)),
            ("LastModifiedTimestamp", epoch_seconds(queue.updated_at)),
        ];
        let mut attributes = selected_queue_attributes(queue.attributes, counts, names);
        // A remote provider reports its own `QueueArn`; it is kept.
        for (name, value) in computed {
            if is_requested_attribute(names, name) {
                attributes.entry(name.to_owned()).or_insert(value);
            }
        }
        Ok(GetQueueAttributesResponse { attributes })
    }

    pub async fn set_queue_attributes(
        &self,
        request: SetQueueAttributesRequest,
    ) -> QueueResult<SetQueueAttributesResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        queue_provider::check_redrive_identity(&request.attributes, &self.identity)?;
        self.storage
            .set_queue_attributes(&request.queue_url, request.attributes)
            .await?;
        self.waiters.forget(&request.queue_url);
        Ok(SetQueueAttributesResponse::default())
    }

    /// The queues whose redrive policy names this queue (SQS
    /// `ListDeadLetterSourceQueues`). Each call reads a bounded part of the
    /// catalog; `NextToken` continues after the last row that it read.
    pub async fn list_dead_letter_source_queues(
        &self,
        request: ListDeadLetterSourceQueuesRequest,
    ) -> QueueResult<ListDeadLetterSourceQueuesResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        let max_results = request
            .max_results
            .and_then(|max| NonZeroU32::new(validated_u32(max)))
            .unwrap_or(queue_provider::MAX_DEAD_LETTER_SOURCE_RESULTS);
        let after = page_token_name(request.next_token.as_deref(), None)?;
        let mut response = self
            .storage
            .list_dead_letter_source_queues(&request.queue_url, after.as_deref(), max_results)
            .await?
            .ok_or_else(|| queue_not_found(&request.queue_url))?;
        response.next_token = response
            .next_token
            .map(|name| queue_provider::encode_page_token(&name));
        response.queue_urls = std::mem::take(&mut response.queue_urls)
            .into_iter()
            .map(|stored| self.public_queue_url(stored))
            .collect();
        Ok(response)
    }

    pub async fn purge_queue(&self, request: PurgeQueueRequest) -> QueueResult<PurgeQueueResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        self.storage.purge_queue(&request.queue_url).await?;
        self.waiters.forget(&request.queue_url);
        Ok(PurgeQueueResponse::default())
    }

    /// Publish one stable identity without disturbing an existing pending
    /// delivery.
    #[instrument(skip_all, fields(feature = "queue"))]
    pub async fn publish_message(
        &self,
        request: PublishMessageRequest,
    ) -> QueueResult<SendMessageResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        let queue_url = request.message.queue_url.clone();
        let delay = message_delay(request.message.delay_seconds);
        let mut message =
            queue_provider::queue_message_for_send(request.message, TimestampMillis::now());
        message.message_id = request.message_id;
        let mut response = Self::prepare_message_integrity(&message);
        response.message_id =
            record_queue_operation("publish_message", self.storage.publish_message(message))
                .await?;
        self.waiters.signal(&queue_url, delay);
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
            md5_of_message_system_attributes: message
                .trace_header
                .as_deref()
                .map(queue_provider::md5_of_trace_header),
        }
    }

    #[instrument(skip_all, fields(feature = "queue", delay_seconds = tracing::field::Empty, message_body_size = tracing::field::Empty))]
    pub async fn send_message(
        &self,
        request: SendMessageRequest,
    ) -> QueueResult<SendMessageResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        let now = TimestampMillis::now();

        tracing::Span::current().record("delay_seconds", request.delay_seconds.unwrap_or(0));
        tracing::Span::current().record("message_body_size", request.message_body.len());

        let queue_url = request.queue_url.clone();
        let delay = message_delay(request.delay_seconds);
        let message = queue_provider::queue_message_for_send(request, now);
        let mut response = Self::prepare_message_integrity(&message);
        let assigned_id =
            record_queue_operation("send_message", self.storage.send_message(message))
                .await
                .inspect_err(|_| {
                    tracing::error!("queue.message.send.failed");
                })?;

        self.waiters.signal(&queue_url, delay);
        response.message_id = assigned_id;
        Ok(response)
    }

    pub async fn send_message_batch(
        &self,
        request: SendMessageBatchRequest,
    ) -> QueueResult<SendMessageBatchResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        let now = TimestampMillis::now();
        let mut entry_ids = Vec::with_capacity(request.entries.len());
        let mut response_metadata = Vec::with_capacity(request.entries.len());
        let mut messages = Vec::with_capacity(request.entries.len());
        let mut delays = Vec::with_capacity(request.entries.len());
        let mut failed = Vec::new();
        for (index, entry) in request.entries.into_iter().enumerate() {
            // A malformed entry fails alone; the others are sent.
            if let Err(error) = entry.validate(index + 1) {
                failed.push(batch_error_entry(entry.id, &error));
                continue;
            }
            delays.push(message_delay(entry.delay_seconds));
            let message = queue_provider::queue_message_for_send(
                SendMessageRequest {
                    queue_url: request.queue_url.clone(),
                    message_body: entry.message_body,
                    delay_seconds: entry.delay_seconds,
                    message_attributes: entry.message_attributes,
                    message_system_attributes: entry.message_system_attributes,
                    message_deduplication_id: None,
                },
                now,
            );
            response_metadata.push(Self::prepare_message_integrity(&message));
            entry_ids.push(entry.id);
            messages.push(message);
        }

        let send_results = if messages.is_empty() {
            Vec::new()
        } else {
            record_queue_operation("send_message_batch", self.storage.send_messages(messages))
                .await?
        };

        let mut successful = Vec::with_capacity(send_results.len());
        let mut earliest_delay = None::<Duration>;
        for (((id, integrity), result), delay) in entry_ids
            .into_iter()
            .zip(response_metadata)
            .zip(send_results)
            .zip(delays)
        {
            match result {
                Ok(message_id) => {
                    earliest_delay = Some(earliest_delay.map_or(delay, |other| other.min(delay)));
                    successful.push(SendMessageBatchResultEntry {
                        id,
                        message_id,
                        md5_of_message_body: integrity.md5_of_body,
                        md5_of_message_attributes: integrity.md5_of_message_attributes,
                        md5_of_message_system_attributes: integrity
                            .md5_of_message_system_attributes,
                    });
                }
                Err(error) => failed.push(batch_error_entry(id, &error)),
            }
        }
        if let Some(delay) = earliest_delay {
            self.waiters.signal(&request.queue_url, delay);
        }

        Ok(SendMessageBatchResponse { successful, failed })
    }

    pub async fn receive_message(
        &self,
        request: ReceiveMessageRequest,
    ) -> QueueResult<ReceiveMessageResponse> {
        self.receive_message_with_wait_hook(request, || {}).await
    }

    /// Receive and call `on_wait` once when the receive starts to wait, so
    /// that a host can release capacity that it holds only for active work.
    #[instrument(skip_all, fields(feature = "queue", max_messages = tracing::field::Empty, visibility_timeout = tracing::field::Empty, messages_received = tracing::field::Empty))]
    pub async fn receive_message_with_wait_hook(
        &self,
        request: ReceiveMessageRequest,
        on_wait: impl FnOnce(),
    ) -> QueueResult<ReceiveMessageResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        let max_messages = request
            .max_number_of_messages
            .map_or(queue_provider::DEFAULT_RECEIVE_MAX_MESSAGES, validated_u32);
        // Omitted values take the queue attribute observed by storage.
        let visibility_timeout = request
            .visibility_timeout
            .map(|seconds| DurationSeconds::from(validated_u32(seconds)));
        let wait_time_seconds = request
            .wait_time_seconds
            .map(|seconds| validated_u32(seconds).min(MAX_RECEIVE_WAIT_TIME_SECS));

        tracing::Span::current().record("max_messages", i64::from(max_messages));
        if let Some(visibility_timeout) = request.visibility_timeout {
            tracing::Span::current().record("visibility_timeout", visibility_timeout);
        }

        let messages = if self.storage.waits_with_local_probe() {
            self.receive_with_local_wait(
                &request.queue_url,
                max_messages,
                visibility_timeout,
                wait_time_seconds,
                on_wait,
            )
            .await?
        } else {
            self.claim_messages(
                &request.queue_url,
                max_messages,
                visibility_timeout,
                wait_time_seconds.map(DurationSeconds::from),
            )
            .await?
            .messages
        };

        let messages = project_received_messages(
            messages,
            request.attribute_names.as_deref(),
            request.message_attribute_names.as_deref(),
        );
        tracing::Span::current().record("messages_received", messages.len());
        Ok(ReceiveMessageResponse { messages })
    }

    pub async fn delete_message(&self, request: DeleteMessageRequest) -> QueueResult<()> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
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
        let request = ValidatedQueueRequest::new(request)?.into_inner();
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
        .await?;

        for (entry, result) in entries.into_iter().zip(results) {
            match result
                .map_err(|error| receipt_handle_error_with_value(error, &entry.receipt_handle))
            {
                Ok(()) => successful.push(DeleteMessageBatchResultEntry { id: entry.id }),
                Err(error) => failed.push(batch_error_entry(entry.id, &error)),
            }
        }

        Ok(DeleteMessageBatchResponse { successful, failed })
    }

    pub async fn change_message_visibility(
        &self,
        request: ChangeMessageVisibilityRequest,
    ) -> QueueResult<()> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        record_queue_operation(
            "change_message_visibility",
            self.storage.change_message_visibility(
                &request.queue_url,
                request.receipt_handle.as_str().into(),
                validated_u32(request.visibility_timeout).into(),
            ),
        )
        .await
        .map_err(|error| receipt_handle_error_with_value(error, &request.receipt_handle))?;
        self.waiters.signal(
            &request.queue_url,
            message_delay(Some(request.visibility_timeout)),
        );
        Ok(())
    }

    pub async fn change_message_visibility_batch(
        &self,
        request: ChangeMessageVisibilityBatchRequest,
    ) -> QueueResult<ChangeMessageVisibilityBatchResponse> {
        let request = ValidatedQueueRequest::new(request)?.into_inner();
        let mut successful = Vec::with_capacity(request.entries.len());
        let mut failed = Vec::new();

        let entries = request.entries;
        let visibility_entries = entries
            .iter()
            .map(|entry| {
                (
                    entry.receipt_handle.as_str().into(),
                    validated_u32(entry.visibility_timeout).into(),
                )
            })
            .collect();
        let results = record_queue_operation(
            "change_message_visibility_batch",
            self.storage
                .change_message_visibilities(&request.queue_url, visibility_entries),
        )
        .await?;

        let mut earliest_delay = None::<Duration>;
        for (entry, result) in entries.into_iter().zip(results) {
            match result
                .map_err(|error| receipt_handle_error_with_value(error, &entry.receipt_handle))
            {
                Ok(()) => {
                    let delay = message_delay(Some(entry.visibility_timeout));
                    earliest_delay = Some(earliest_delay.map_or(delay, |other| other.min(delay)));
                    successful.push(ChangeMessageVisibilityBatchResultEntry { id: entry.id });
                }
                Err(error) => failed.push(batch_error_entry(entry.id, &error)),
            }
        }
        if let Some(delay) = earliest_delay {
            self.waiters.signal(&request.queue_url, delay);
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

    /// Receive from a store that this process probes while it waits.
    ///
    /// A write (claim) attempt opens only when this process believes a
    /// message is due: a fresh observation or this request's own probe or
    /// claim (of any age) says so, a local commit signalled it, or the latest
    /// claim filled its page. Otherwise the queue's shared
    /// read-only probe decides; it also supplies the queue's wait default. A
    /// short poll can therefore return empty for a message that another node
    /// committed within the latest probe interval.
    async fn receive_with_local_wait(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: Option<DurationSeconds>,
        requested_wait: Option<u32>,
        on_wait: impl FnOnce(),
    ) -> QueueResult<Vec<MessageResponse>> {
        let started = instant_now();
        let interval = self.waiters.probe_interval();
        let queue = self.waiters.queue(queue_url);
        // Subscribe before the first read so that no later commit is missed.
        let mut observed = queue.subscribe();
        let first = *observed.borrow_and_update();
        let mut wake = queue.subscribe_wake();
        let mut mark = wake.as_mut().map(|receiver| *receiver.borrow_and_update());
        // The start of the latest probe or claim that this request ran; its
        // result counts whatever its age (a slow store still delivers).
        let mut own = None;
        if first.needs_probe(started, interval, mark) {
            own = self.probe_observed(&queue, queue_url).await?;
        }
        let observation = *observed.borrow_and_update();
        mark = queue.wake_mark();
        let now = instant_now();
        if observation.is_gone(now, interval, mark) {
            return Err(queue_not_found(queue_url));
        }
        let mut wait_time = requested_wait.or(observation.wait_time_seconds(now, interval, mark));
        if observation.is_due(now, interval, own)
            && let Some((claimed, received)) = self
                .claim_gated(&queue, queue_url, max_messages, visibility_timeout, own)
                .await?
        {
            own = Some(claimed);
            wait_time = wait_time.or(Some(received.wait_time_seconds));
            if !received.messages.is_empty() {
                return Ok(received.messages);
            }
        }
        let wait_time = match wait_time {
            Some(seconds) => seconds,
            None => queue
                .current()
                .wait_time_seconds(instant_now(), interval, queue.wake_mark())
                .unwrap_or_default(),
        };
        if wait_time == 0 {
            return Ok(Vec::new());
        }
        let deadline = started + Duration::from_secs(u64::from(wait_time));
        // Over the bound the receive has already probed and claimed once.
        let Some(_waiting) = self.waiters.begin_waiting() else {
            return Ok(Vec::new());
        };
        let _interest = queue.register_wake();
        on_wait();
        loop {
            let observation = *observed.borrow_and_update();
            let mark = wake.as_mut().map(|receiver| *receiver.borrow_and_update());
            let now = instant_now();
            if observation.is_gone(now, interval, mark) {
                return Err(queue_not_found(queue_url));
            }
            if observation.is_due(now, interval, own) {
                // A claim that is empty but not drained records a short
                // jittered backoff as its next due time, so the loop does not
                // spin.
                if let Some((claimed, received)) = self
                    .claim_gated(&queue, queue_url, max_messages, visibility_timeout, own)
                    .await?
                {
                    own = Some(claimed);
                    if !received.messages.is_empty() {
                        return Ok(received.messages);
                    }
                }
                if instant_now() >= deadline {
                    return Ok(Vec::new());
                }
                continue;
            }
            if now >= deadline {
                return Ok(Vec::new());
            }
            if observation.needs_probe(now, interval, mark) {
                own = self.probe_observed(&queue, queue_url).await?.or(own);
                continue;
            }
            tokio::select! {
                () = tokio::time::sleep_until(observation.wake_at(deadline, interval, mark)) => {}
                () = wake_changed(&mut wake) => {}
                changed = observed.changed() => {
                    changed.map_err(|_| {
                        QueueError::from(storage_types::StorageError::internal(
                            "queue wait state closed",
                        ))
                    })?;
                }
            }
        }
    }

    /// Claim when the queue is still due. Outside a deep queue one claim runs
    /// at a time and the others reuse its observation; a deep queue lets
    /// claims run in parallel. `own` is the start of the caller's latest
    /// probe or claim. `None` means the queue was no longer due; otherwise
    /// the claim's start and result.
    async fn claim_gated(
        &self,
        queue: &QueueWaitState,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: Option<DurationSeconds>,
        own: Option<Instant>,
    ) -> QueueResult<Option<(Instant, ReceivedMessages)>> {
        let interval = self.waiters.probe_interval();
        let gate = queue.claim_lock().lock().await;
        let observation = queue.current();
        let now = instant_now();
        if !observation.is_due(now, interval, own) {
            return Ok(None);
        }
        if observation.is_deep(now, interval, own) {
            drop(gate);
        }
        self.claim_observed(queue, queue_url, max_messages, visibility_timeout)
            .await
            .map(Some)
    }

    /// Claim and record what the claim observed (decisions D15 and D16): a
    /// claim that took messages and left more due marks the queue deep; an
    /// empty claim that left due messages is due again after a backoff; a
    /// drained claim keeps the next due time that it reports. Returns the
    /// claim's start with its result.
    async fn claim_observed(
        &self,
        queue: &QueueWaitState,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: Option<DurationSeconds>,
    ) -> QueueResult<(Instant, ReceivedMessages)> {
        let start = queue.start();
        let started = QueueWaitState::started_at(start);
        let received = self
            .claim_messages(queue_url, max_messages, visibility_timeout, None)
            .await;
        match &received {
            Ok(received) => queue.record(
                start,
                Observed::Claim {
                    took: !received.messages.is_empty(),
                    drained: received.drained,
                    next_due: received.next_due_in.map(|due_in| started + due_in),
                    backoff: empty_claim_backoff(started),
                    wait_time_seconds: received.wait_time_seconds,
                },
            ),
            Err(QueueError::ResourceNotFound {
                resource_type: "queue",
                ..
            }) => queue.record(start, Observed::Gone),
            Err(_) => {}
        }
        received.map(|received| (started, received))
    }

    /// Refresh a stale observation with one read-only probe per queue. A new
    /// probe starts no earlier than one interval after the previous one
    /// completed; waiters that queue behind it reuse its result as a hint.
    /// Returns the probe's start when this caller ran the probe.
    async fn probe_observed(
        &self,
        queue: &QueueWaitState,
        queue_url: &str,
    ) -> QueueResult<Option<Instant>> {
        let interval = self.waiters.probe_interval();
        let _probe = queue.probe_lock().lock().await;
        if !queue
            .current()
            .needs_probe(instant_now(), interval, queue.wake_mark())
        {
            return Ok(None);
        }
        let start = queue.start();
        let started = QueueWaitState::started_at(start);
        let probe = record_queue_operation("probe_receive", self.storage.probe_receive(queue_url))
            .await
            .inspect_err(|_| {
                tracing::error!("queue.messages.probe.failed");
            })?;
        queue.record(
            start,
            match probe {
                Some(probe) => Observed::Probe {
                    next_due: probe.next_due_in.map(|due_in| started + due_in),
                    wait_time_seconds: probe.wait_time_seconds,
                },
                None => Observed::Gone,
            },
        );
        Ok(Some(started))
    }

    /// One claim. The provider owns conflict retries; a claim that loses
    /// them returns an empty, undrained receive.
    async fn claim_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: Option<DurationSeconds>,
        wait_time_seconds: Option<DurationSeconds>,
    ) -> QueueResult<ReceivedMessages> {
        let received = record_queue_operation(
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
        })?;
        // A dead-letter move is a send to the target queue (design D10).
        if let Some(target) = received.moved_to.as_deref() {
            self.waiters.signal(target, Duration::ZERO);
        }
        Ok(received)
    }
}

/// A short jittered pause after an empty claim that left due messages; the
/// claim's own duration supplies the jitter so that concurrent waiters do not
/// retry together.
fn empty_claim_backoff(started: Instant) -> Duration {
    let base = EMPTY_CLAIM_BACKOFF.as_micros() as u64;
    let jitter = u64::from(started.elapsed().subsec_nanos());
    Duration::from_micros(base + jitter % base.max(1))
}

/// The queue name in a listing `NextToken` (probe 7): it decodes and starts
/// with the listing prefix, else 400 "Invalid or expired next token.".
fn page_token_name(token: Option<&str>, prefix: Option<&str>) -> QueueResult<Option<String>> {
    let Some(token) = token else {
        return Ok(None);
    };
    queue_provider::decode_page_token(token)
        .filter(|name| prefix.is_none_or(|prefix| name.starts_with(prefix)))
        .map(Some)
        .ok_or_else(|| {
            QueueError::validation_with_detail(
                queue_provider::QueueValidationKind::InvalidParameterValue,
                queue_provider::INVALID_NEXT_TOKEN_MESSAGE,
            )
        })
}

pub(crate) fn queue_not_found(queue_url: &str) -> QueueError {
    QueueError::ResourceNotFound {
        resource_type: "queue",
        resource_id: queue_url.to_owned(),
    }
}

/// Delay from a commit until its message can be due.
fn message_delay(seconds: Option<i64>) -> Duration {
    Duration::from_secs(u64::from(seconds.map(validated_u32).unwrap_or_default()))
}

pub(crate) fn batch_error_entry(id: String, error: &QueueError) -> BatchResultErrorEntry {
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
            detail: None,
        } => QueueError::validation_with_detail(
            queue_provider::QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
            queue_provider::receipt_handle_invalid_message(receipt_handle),
        ),
        other => other,
    }
}

fn project_received_messages(
    mut messages: Vec<queue_provider::MessageResponse>,
    attribute_names: Option<&[String]>,
    message_attribute_names: Option<&[String]>,
) -> Vec<queue_provider::MessageResponse> {
    for message in &mut messages {
        message.attributes =
            project_named_attributes(message.attributes.take(), attribute_names, false);
        message.message_attributes = project_named_attributes(
            message.message_attributes.take(),
            message_attribute_names,
            true,
        );
        message.md5_of_message_attributes = message
            .message_attributes
            .as_ref()
            .and_then(queue_provider::md5_of_message_attributes);
    }
    messages
}

fn project_named_attributes<T>(
    attributes: Option<std::collections::HashMap<String, T>>,
    requested: Option<&[String]>,
    allow_prefix: bool,
) -> Option<std::collections::HashMap<String, T>> {
    let attributes = attributes?;
    let requested = requested.filter(|names| !names.is_empty())?;
    let requested_all = requested
        .iter()
        .any(|name| name == "All" || allow_prefix && name == ".*");
    let projected = attributes
        .into_iter()
        .filter(|(name, _)| {
            requested_all
                || requested.iter().any(|requested| {
                    requested == name
                        || (allow_prefix
                            && requested
                                .strip_suffix('*')
                                .is_some_and(|prefix| name.starts_with(prefix)))
                })
        })
        .collect::<std::collections::HashMap<_, _>>();
    (!projected.is_empty()).then_some(projected)
}

/// SQS timestamps are whole seconds since the epoch.
fn epoch_seconds(timestamp: TimestampMillis) -> String {
    (timestamp.timestamp_millis() / 1_000).to_string()
}

fn is_requested_attribute(requested: &[String], name: &str) -> bool {
    requested
        .iter()
        .any(|value| value == "All" || value == name)
}

pub(crate) fn selected_queue_attributes(
    mut stored: std::collections::HashMap<String, String>,
    counts: QueueMessageCounts,
    requested: &[String],
) -> std::collections::HashMap<String, String> {
    let is_requested = |name: &str| is_requested_attribute(requested, name);
    stored.retain(|name, _| is_requested(name));
    for (name, value) in queue_provider::QUEUE_ATTRIBUTE_DEFAULTS {
        if is_requested(name) {
            stored
                .entry(name.to_owned())
                .or_insert_with(|| value.to_owned());
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

/// Resolves when the slot mark changes; never without a board.
async fn wake_changed(
    wake: &mut Option<tokio::sync::watch::Receiver<queue_provider::QueueWakeMark>>,
) {
    if let Some(receiver) = wake
        && receiver.changed().await.is_ok()
    {
        return;
    }
    std::future::pending::<()>().await;
}
