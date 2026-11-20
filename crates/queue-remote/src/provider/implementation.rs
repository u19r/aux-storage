use std::time::Duration;

use async_trait::async_trait;
use aws_sigv4_signing::{AwsRequestSigner, AwsStaticCredentials, CredentialSource, SignableBody};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Uri};
use http_request::{HttpRequestDispatchStatus, reqwest::Client};
use queue_provider::{
    BatchResultErrorEntry, ChangeMessageVisibilityBatchRequest,
    ChangeMessageVisibilityBatchRequestEntry, ChangeMessageVisibilityBatchResponse,
    ChangeMessageVisibilityRequest, CreateQueueRequest, CreateQueueResponse,
    DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, DeleteMessageBatchResponse,
    DeleteMessageRequest, DeleteQueueRequest, GetQueueAttributesRequest,
    GetQueueAttributesResponse, GetQueueUrlRequest, GetQueueUrlResponse,
    ListDeadLetterSourceQueuesRequest, ListDeadLetterSourceQueuesResponse, ListQueueTagsRequest,
    ListQueueTagsResponse, ListQueuesRequest, ListQueuesResponse, MessageId, PublishMessageRequest,
    PurgeQueueRequest, Queue, QueueError, QueueInternalKind, QueueMessage, QueueMessageCounts,
    QueueOperations, QueueResult, QueueValidationKind, ReceiptHandle, ReceiveMessageRequest,
    ReceiveMessageResponse, SQS_NON_EXISTENT_QUEUE_ERROR_TYPE, SendMessageBatchRequest,
    SendMessageBatchRequestEntry, SendMessageBatchResponse, SendMessageRequest,
    SendMessageResponse, SetQueueAttributesRequest, TagQueueRequest, UntagQueueRequest,
    sqs_json_error_type,
};
use serde::{Serialize, de::DeserializeOwned};
use tracing::instrument;

use super::replay_policy::transport_failure;
use crate::{RemoteCredentialStrategy, RemoteQueueSettings, RemoteTimeoutOverrides};

/// Idle pooled connections close before common peer idle closes (hyper 30 s
/// header read, load balancers 60 s), so a write rarely meets a connection
/// its peer already closed, which answers an unknown outcome.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone)]
struct EndpointState {
    url: String,
    uri: Uri,
}

pub struct RemoteQueueProvider {
    client: Client,
    endpoints: Vec<EndpointState>,
    signer: Option<AwsRequestSigner>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct RemoteErrorBody {
    #[serde(rename = "__type")]
    error_type: Option<String>,
    message: Option<String>,
}

impl RemoteQueueProvider {
    #[instrument(name = "remote_queue.init", skip(settings), fields(feature = "queue"))]
    pub async fn new(settings: RemoteQueueSettings) -> QueueResult<Self> {
        settings.validate()?;
        let client = build_client(settings.timeouts.as_ref())?;
        let endpoints = build_endpoints(&settings)?;
        let signer = if settings.sigv4.enabled {
            let region = settings.region.clone().ok_or_else(|| {
                QueueError::internal_with_detail(
                    QueueInternalKind::RemoteBackendNotImplemented,
                    "remote queue SigV4 requires region",
                )
            })?;
            Some(
                AwsRequestSigner::new(
                    &region,
                    signer_credentials(&settings.credentials),
                    &settings.sigv4.service_name,
                )
                .map_err(|err| {
                    QueueError::internal_with_detail(
                        QueueInternalKind::RemoteBackendNotImplemented,
                        format!("initialize queue signer: {err}"),
                    )
                })?,
            )
        } else {
            None
        };

        Ok(Self {
            client,
            endpoints,
            signer,
        })
    }

    fn endpoint(&self) -> QueueResult<&EndpointState> {
        self.endpoints.first().ok_or_else(|| {
            QueueError::internal_with_detail(
                QueueInternalKind::RemoteBackendNotImplemented,
                "remote queue has no configured endpoints",
            )
        })
    }

    async fn invoke<Request, Response>(
        &self,
        target: &str,
        request: &Request,
    ) -> QueueResult<Response>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        let bytes = self.invoke_bytes(target, request).await?;
        serde_json::from_slice(&bytes).map_err(QueueError::from)
    }

    async fn invoke_bytes<Request>(&self, target: &str, request: &Request) -> QueueResult<Bytes>
    where Request: Serialize + ?Sized {
        let endpoint = self.endpoint()?;
        let body = serde_json::to_vec(request)?;
        let headers = self.build_headers(endpoint, target, &body).await?;
        let response = self
            .client
            .post(endpoint.url.as_str())
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|err| {
                let err = err.without_url();
                tracing::warn!(operation = target, error = %err, "remote queue request failed");
                transport_failure(
                    target,
                    http_request::HttpRequestError::from(err).dispatch_status(),
                )
            })?;
        let status = response.status();
        // The server answered with a status: the request was sent.
        let bytes = response.bytes().await.map_err(|err| {
            tracing::warn!(
                operation = target,
                error = %err.without_url(),
                "remote queue response body read failed"
            );
            transport_failure(target, HttpRequestDispatchStatus::PossiblyDispatched)
        })?;
        if !status.is_success() {
            return Err(classify_remote_error(status.as_u16(), &bytes));
        }
        Ok(bytes)
    }

    async fn build_headers(
        &self,
        endpoint: &EndpointState,
        target: &str,
        body: &[u8],
    ) -> QueueResult<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-amz-json-1.0"),
        );
        headers.insert(
            "x-amz-target",
            HeaderValue::from_str(target)
                .map_err(|_| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?,
        );
        let Some(signer) = &self.signer else {
            return Ok(headers);
        };
        signer
            .sign_request("POST", &endpoint.uri, &headers, SignableBody::Bytes(body))
            .await
            .map_err(|err| {
                QueueError::internal_with_detail(
                    QueueInternalKind::RemoteBackendNotImplemented,
                    format!("remote queue signing failed: {err}"),
                )
            })
    }

    async fn ensure_queue_matches(&self, queue: Queue) -> QueueResult<Queue> {
        let Some(mut existing_queue) =
            QueueOperations::get_queue_by_name(self, &queue.queue_name).await?
        else {
            return Err(QueueError::internal_with_detail(
                QueueInternalKind::RemoteBackendNotImplemented,
                format!(
                    "queue reported as existing but could not be fetched by name: {}",
                    queue.queue_name
                ),
            ));
        };

        let attribute_updates =
            queue_attribute_updates(&existing_queue.attributes, &queue.attributes);
        if !attribute_updates.is_empty() {
            QueueOperations::set_queue_attributes(
                self,
                &existing_queue.queue_url,
                attribute_updates,
            )
            .await?;
            existing_queue.attributes = queue.attributes;
        }
        Ok(existing_queue)
    }
}

#[async_trait]
impl QueueOperations for RemoteQueueProvider {
    fn waits_with_local_probe(&self) -> bool {
        false
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue> {
        match self
            .invoke::<_, CreateQueueResponse>(
                "AmazonSQS.CreateQueue",
                &CreateQueueRequest {
                    queue_name: queue.queue_name.clone(),
                    attributes: Some(queue.attributes.clone()),
                    tags: (!queue.tags.is_empty()).then(|| queue.tags.clone()),
                },
            )
            .await
        {
            Ok(response) => Ok(Queue {
                queue_name: queue.queue_name,
                queue_url: response.queue_url,
                attributes: queue.attributes,
                created_at: queue.created_at,
                ..Default::default()
            }),
            Err(QueueError::ResourceExists { .. }) => self.ensure_queue_matches(queue).await,
            Err(err) => Err(err),
        }
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        Ok(self
            .get_remote_queue(queue_url)
            .await?
            .map(|(queue, _)| queue))
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        let Some(queue_url) = QueueOperations::get_queue_url_by_name(self, queue_name).await?
        else {
            return Ok(None);
        };
        QueueOperations::get_queue(self, &queue_url).await
    }

    async fn get_queue_url_by_name(&self, queue_name: &str) -> QueueResult<Option<String>> {
        let response = self
            .invoke::<_, GetQueueUrlResponse>(
                "AmazonSQS.GetQueueUrl",
                &GetQueueUrlRequest {
                    queue_name: queue_name.to_string(),
                    ..Default::default()
                },
            )
            .await;
        match response {
            Ok(response) => Ok(Some(response.queue_url)),
            Err(QueueError::ResourceNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// The remote server owns the catalog page; its token is the encoded
    /// last name.
    async fn list_queue_page(
        &self,
        prefix: Option<&str>,
        after: Option<&str>,
        limit: std::num::NonZeroU32,
    ) -> QueueResult<(Vec<Queue>, Option<String>)> {
        let response: ListQueuesResponse = self
            .invoke(
                "AmazonSQS.ListQueues",
                &ListQueuesRequest {
                    queue_name_prefix: prefix.map(ToString::to_string),
                    max_results: Some(limit.get().into()),
                    next_token: after.map(queue_provider::encode_page_token),
                },
            )
            .await?;
        let mut queues = Vec::with_capacity(response.queue_urls.len());
        for queue_url in response.queue_urls {
            queues.push(Queue {
                queue_name: queue_name_from_url(&queue_url)?,
                queue_url,
                attributes: Default::default(),
                created_at: storage_types::TimestampMillis::now(),
                ..Default::default()
            });
        }
        let last = response
            .next_token
            .as_deref()
            .and_then(queue_provider::decode_page_token);
        Ok((queues, last))
    }

    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()> {
        self.invoke_bytes(
            "AmazonSQS.DeleteQueue",
            &DeleteQueueRequest {
                queue_url: queue_url.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    /// One `TagQueue` or `UntagQueue` call; the manager sends one kind.
    async fn change_queue_tags(
        &self,
        queue_url: &str,
        added: std::collections::BTreeMap<String, String>,
        removed: Vec<String>,
    ) -> QueueResult<()> {
        if !added.is_empty() {
            let request = TagQueueRequest {
                queue_url: queue_url.to_owned(),
                tags: added,
            };
            self.invoke_bytes("AmazonSQS.TagQueue", &request).await?;
        }
        if !removed.is_empty() {
            let request = UntagQueueRequest {
                queue_url: queue_url.to_owned(),
                tag_keys: removed,
            };
            self.invoke_bytes("AmazonSQS.UntagQueue", &request).await?;
        }
        Ok(())
    }

    async fn list_queue_tags(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<std::collections::BTreeMap<String, String>>> {
        let response: QueueResult<ListQueueTagsResponse> = self
            .invoke(
                "AmazonSQS.ListQueueTags",
                &ListQueueTagsRequest {
                    queue_url: queue_url.to_owned(),
                },
            )
            .await;
        match response {
            Ok(response) => Ok(Some(response.tags)),
            Err(QueueError::ResourceNotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()> {
        self.invoke_bytes(
            "AmazonSQS.PurgeQueue",
            &PurgeQueueRequest {
                queue_url: queue_url.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn get_queue_with_message_counts(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, queue_provider::QueueMessageCounts)>> {
        self.get_remote_queue(queue_url).await
    }

    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: std::collections::HashMap<String, String>,
    ) -> QueueResult<()> {
        self.invoke_bytes(
            "AmazonSQS.SetQueueAttributes",
            &SetQueueAttributesRequest {
                queue_url: queue_url.to_string(),
                attributes,
            },
        )
        .await?;
        Ok(())
    }

    /// The remote server owns the catalog scan and its token.
    async fn list_dead_letter_source_queues(
        &self,
        queue_url: &str,
        exclusive_start: Option<&str>,
        max_results: std::num::NonZeroU32,
    ) -> QueueResult<Option<ListDeadLetterSourceQueuesResponse>> {
        let response: QueueResult<ListDeadLetterSourceQueuesResponse> = self
            .invoke(
                "AmazonSQS.ListDeadLetterSourceQueues",
                &ListDeadLetterSourceQueuesRequest {
                    queue_url: queue_url.to_owned(),
                    max_results: Some(max_results.get().into()),
                    next_token: exclusive_start.map(queue_provider::encode_page_token),
                },
            )
            .await;
        match response {
            Ok(mut response) => {
                response.next_token = response
                    .next_token
                    .as_deref()
                    .and_then(queue_provider::decode_page_token);
                Ok(Some(response))
            }
            Err(QueueError::ResourceNotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn publish_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        let response: SendMessageResponse = self
            .invoke(
                "AmazonSQS.PublishMessage",
                &PublishMessageRequest {
                    message_id: message.message_id,
                    message: SendMessageRequest {
                        queue_url: message.queue_url,
                        message_body: message.body,
                        delay_seconds: message
                            .visibility_timestamp
                            .map(|timestamp| delay_seconds_from_timestamp(timestamp).into()),
                        message_attributes: message.message_attributes,
                        message_system_attributes: trace_header_attributes(message.trace_header),
                        ..Default::default()
                    },
                },
            )
            .await?;
        Ok(response.message_id)
    }

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        let response: SendMessageResponse = self
            .invoke(
                "AmazonSQS.SendMessage",
                &SendMessageRequest {
                    queue_url: message.queue_url,
                    message_body: message.body,
                    delay_seconds: message
                        .visibility_timestamp
                        .map(|timestamp| delay_seconds_from_timestamp(timestamp).into()),
                    message_attributes: message.message_attributes,
                    message_system_attributes: trace_header_attributes(message.trace_header),
                    ..Default::default()
                },
            )
            .await?;
        Ok(response.message_id)
    }

    async fn send_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<QueueResult<MessageId>>> {
        let Some(first) = messages.first() else {
            return Ok(Vec::new());
        };
        let queue_url = first.queue_url.clone();
        if messages
            .iter()
            .any(|message| message.queue_url != queue_url)
        {
            return Err(QueueError::validation_with_detail(
                QueueValidationKind::InvalidParameterValue,
                "send_messages requires one queue URL",
            ));
        }
        let entry_count = messages.len();
        let entries = messages
            .into_iter()
            .enumerate()
            .map(|(index, message)| SendMessageBatchRequestEntry {
                id: index.to_string(),
                message_body: message.body,
                delay_seconds: message
                    .visibility_timestamp
                    .map(|timestamp| delay_seconds_from_timestamp(timestamp).into()),
                message_attributes: message.message_attributes,
                message_system_attributes: trace_header_attributes(message.trace_header),
                ..Default::default()
            })
            .collect();
        let response: SendMessageBatchResponse = self
            .invoke(
                "AmazonSQS.SendMessageBatch",
                &SendMessageBatchRequest { queue_url, entries },
            )
            .await?;
        ordered_batch_results(
            entry_count,
            response
                .successful
                .into_iter()
                .map(|entry| (entry.id, entry.message_id)),
            response.failed,
        )
    }

    /// The remote server applies queue defaults for omitted values and performs
    /// the long poll.
    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: Option<storage_types::DurationSeconds>,
        wait_time_seconds: Option<storage_types::DurationSeconds>,
    ) -> QueueResult<queue_provider::ReceivedMessages> {
        let response: ReceiveMessageResponse = self
            .invoke(
                "AmazonSQS.ReceiveMessage",
                &ReceiveMessageRequest {
                    queue_url: queue_url.to_string(),
                    max_number_of_messages: Some(max_messages.into()),
                    visibility_timeout: visibility_timeout.map(|timeout| (*timeout).into()),
                    wait_time_seconds: wait_time_seconds.map(|wait| (*wait).into()),
                    attribute_names: Some(vec![
                        "SentTimestamp".into(),
                        "ApproximateReceiveCount".into(),
                        queue_provider::TRACE_HEADER_ATTRIBUTE.into(),
                    ]),
                    message_attribute_names: Some(vec!["All".into()]),
                },
            )
            .await?;
        Ok(queue_provider::ReceivedMessages {
            messages: response.messages,
            wait_time_seconds: 0,
            drained: true,
            next_due_in: None,
            // The remote server wakes its own waiters after a move.
            moved_to: None,
        })
    }

    async fn probe_receive(
        &self,
        _queue_url: &str,
    ) -> QueueResult<Option<queue_provider::ReceiveProbe>> {
        Err(QueueError::internal_with_detail(
            QueueInternalKind::RemoteBackendNotImplemented,
            "the remote queue server waits for messages; it has no local probe",
        ))
    }

    async fn delete_message(&self, queue_url: &str, receipt: ReceiptHandle) -> QueueResult<()> {
        self.invoke_bytes(
            "AmazonSQS.DeleteMessage",
            &DeleteMessageRequest {
                queue_url: queue_url.to_string(),
                receipt_handle: receipt,
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_messages(
        &self,
        queue_url: &str,
        receipts: Vec<ReceiptHandle>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        if receipts.is_empty() {
            return Ok(Vec::new());
        }
        let entry_count = receipts.len();
        let entries = receipts
            .into_iter()
            .enumerate()
            .map(|(index, receipt_handle)| DeleteMessageBatchRequestEntry {
                id: index.to_string(),
                receipt_handle,
            })
            .collect();
        let response: DeleteMessageBatchResponse = self
            .invoke(
                "AmazonSQS.DeleteMessageBatch",
                &DeleteMessageBatchRequest {
                    queue_url: queue_url.to_string(),
                    entries,
                },
            )
            .await?;
        ordered_batch_results(
            entry_count,
            response.successful.into_iter().map(|entry| (entry.id, ())),
            response.failed,
        )
    }

    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt: ReceiptHandle,
        visibility_timeout: storage_types::DurationSeconds,
    ) -> QueueResult<()> {
        self.invoke_bytes(
            "AmazonSQS.ChangeMessageVisibility",
            &ChangeMessageVisibilityRequest {
                queue_url: queue_url.to_string(),
                receipt_handle: receipt,
                visibility_timeout: (*visibility_timeout).into(),
            },
        )
        .await?;
        Ok(())
    }

    async fn change_message_visibilities(
        &self,
        queue_url: &str,
        entries: Vec<(ReceiptHandle, storage_types::DurationSeconds)>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let entry_count = entries.len();
        let entries = entries
            .into_iter()
            .enumerate()
            .map(|(index, (receipt_handle, visibility_timeout))| {
                ChangeMessageVisibilityBatchRequestEntry {
                    id: index.to_string(),
                    receipt_handle,
                    visibility_timeout: (*visibility_timeout).into(),
                }
            })
            .collect();
        let response: ChangeMessageVisibilityBatchResponse = self
            .invoke(
                "AmazonSQS.ChangeMessageVisibilityBatch",
                &ChangeMessageVisibilityBatchRequest {
                    queue_url: queue_url.to_string(),
                    entries,
                },
            )
            .await?;
        ordered_batch_results(
            entry_count,
            response.successful.into_iter().map(|entry| (entry.id, ())),
            response.failed,
        )
    }

    async fn update_message_snapshot_checkpoint(
        &self,
        _queue_url: &str,
        _receipt: ReceiptHandle,
        _checkpoint_data: String,
    ) -> QueueResult<()> {
        Ok(())
    }
}

/// The stored trace header as the `MessageSystemAttributes` member.
fn trace_header_attributes(
    trace_header: Option<String>,
) -> Option<std::collections::HashMap<String, queue_provider::MessageAttributeValue>> {
    trace_header.map(|value| {
        std::collections::HashMap::from([(
            queue_provider::TRACE_HEADER_ATTRIBUTE.to_owned(),
            queue_provider::MessageAttributeValue {
                data_type: "String".to_owned(),
                string_value: Some(value),
                binary_value: None,
            },
        )])
    })
}

fn delay_seconds_from_timestamp(visibility: storage_types::TimestampMillis) -> u32 {
    let now = storage_types::TimestampMillis::now().timestamp_millis();
    let delay_ms = visibility.timestamp_millis().saturating_sub(now).max(0);
    u32::try_from(delay_ms / 1_000).unwrap_or(u32::MAX)
}

fn ordered_batch_results<T>(
    entry_count: usize,
    successful: impl IntoIterator<Item = (String, T)>,
    failed: Vec<BatchResultErrorEntry>,
) -> QueueResult<Vec<QueueResult<T>>> {
    let mut results = std::iter::repeat_with(|| None)
        .take(entry_count)
        .collect::<Vec<Option<QueueResult<T>>>>();
    for (id, value) in successful {
        insert_batch_result(&mut results, id, Ok(value))?;
    }
    for failure in failed {
        let BatchResultErrorEntry {
            id,
            sender_fault,
            code,
            message,
        } = failure;
        insert_batch_result(
            &mut results,
            id,
            Err(QueueError::batch_entry(sender_fault, code, message)),
        )?;
    }
    results
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.ok_or_else(|| malformed_batch_response(format!("missing entry {index}")))
        })
        .collect()
}

fn insert_batch_result<T>(
    results: &mut [Option<QueueResult<T>>],
    id: String,
    result: QueueResult<T>,
) -> QueueResult<()> {
    let index = id
        .parse::<usize>()
        .map_err(|_| malformed_batch_response(format!("invalid entry id {id}")))?;
    let slot = results
        .get_mut(index)
        .ok_or_else(|| malformed_batch_response(format!("unknown entry id {id}")))?;
    if slot.is_some() {
        return Err(malformed_batch_response(format!("duplicate entry id {id}")));
    }
    *slot = Some(result);
    Ok(())
}

fn malformed_batch_response(detail: impl std::fmt::Display) -> QueueError {
    QueueError::internal_with_detail(
        QueueInternalKind::RemoteBackendNotImplemented,
        format!("malformed remote batch response: {detail}"),
    )
}

fn signer_credentials(strategy: &RemoteCredentialStrategy) -> CredentialSource {
    match strategy {
        RemoteCredentialStrategy::DefaultChain => CredentialSource::DefaultChain,
        RemoteCredentialStrategy::Static(creds) => CredentialSource::Static(AwsStaticCredentials {
            access_key_id: creds.access_key_id.clone(),
            secret_access_key: creds.secret_access_key.clone(),
            session_token: creds.session_token.clone(),
        }),
    }
}

fn build_client(timeouts: Option<&RemoteTimeoutOverrides>) -> QueueResult<Client> {
    let default_timeouts = RemoteTimeoutOverrides::default();
    let timeouts = timeouts.unwrap_or(&default_timeouts);
    if let (Some(connect), Some(request)) =
        (timeouts.connect_timeout_ms, timeouts.request_timeout_ms)
        && connect >= request
    {
        return Err(QueueError::validation_with_detail(
            QueueValidationKind::InvalidParameterValue,
            format!(
                "connect_timeout_ms ({connect}) must be less than request_timeout_ms ({request})"
            ),
        ));
    }
    let mut builder = Client::builder()
        .connect_timeout(timeouts.connect_timeout())
        .pool_idle_timeout(POOL_IDLE_TIMEOUT);
    if let Some(request_timeout) = timeouts.request_timeout() {
        builder = builder.timeout(request_timeout);
    }
    builder.build().map_err(|err| {
        QueueError::internal_with_detail(
            QueueInternalKind::RemoteBackendNotImplemented,
            format!("build remote queue http client: {err}"),
        )
    })
}

fn build_endpoints(settings: &RemoteQueueSettings) -> QueueResult<Vec<EndpointState>> {
    let mut endpoints = Vec::with_capacity(settings.endpoint_urls.len());
    for raw in &settings.endpoint_urls {
        let trimmed = raw.trim();
        let normalized = if trimmed.contains("://") {
            trimmed.to_string()
        } else if settings.tls {
            format!("https://{trimmed}")
        } else {
            format!("http://{trimmed}")
        };
        let uri: Uri = normalized.parse().map_err(|_| {
            QueueError::internal_with_detail(
                QueueInternalKind::RemoteBackendNotImplemented,
                format!("invalid remote queue endpoint: {normalized}"),
            )
        })?;
        endpoints.push(EndpointState {
            url: normalized,
            uri,
        });
    }
    Ok(endpoints)
}

impl RemoteQueueProvider {
    /// One `GetQueueAttributes All` call. The remote owns the computed values:
    /// its timestamps become `created_at` and `updated_at`, its counts are
    /// the counts, and its `QueueArn` stays an attribute that the manager
    /// reports as it is (review finding 7).
    async fn get_remote_queue(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
        let response = self
            .invoke::<_, GetQueueAttributesResponse>(
                "AmazonSQS.GetQueueAttributes",
                &GetQueueAttributesRequest {
                    queue_url: queue_url.to_string(),
                    attribute_names: Some(vec!["All".to_owned()]),
                },
            )
            .await;
        let mut attributes = match response {
            Ok(response) => response.attributes,
            Err(QueueError::ResourceNotFound { .. }) => return Ok(None),
            Err(err) => return Err(err),
        };
        let mut seconds = |name: &str| {
            attributes
                .remove(name)
                .and_then(|value| value.parse::<i64>().ok())
                .map(|seconds| {
                    storage_types::TimestampMillis::from_timestamp(seconds.saturating_mul(1_000))
                })
                .unwrap_or_default()
        };
        let created_at = seconds("CreatedTimestamp");
        let updated_at = seconds("LastModifiedTimestamp");
        let mut count = |name: &str| {
            attributes
                .remove(name)
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default()
        };
        let counts = QueueMessageCounts {
            visible: count("ApproximateNumberOfMessages"),
            not_visible: count("ApproximateNumberOfMessagesNotVisible"),
            delayed: count("ApproximateNumberOfMessagesDelayed"),
        };
        let queue = Queue {
            queue_name: queue_name_from_url(queue_url)?,
            queue_url: queue_url.to_string(),
            attributes,
            created_at,
            updated_at,
            ..Default::default()
        };
        Ok(Some((queue, counts)))
    }
}

fn queue_name_from_url(queue_url: &str) -> QueueResult<String> {
    match queue_provider::queue_name_of(queue_url) {
        "" => Err(QueueError::validation(
            QueueValidationKind::InvalidQueueUrlFormat,
        )),
        name => Ok(name.to_owned()),
    }
}

pub(crate) fn classify_remote_error(status: u16, body: &[u8]) -> QueueError {
    let parsed: RemoteErrorBody = serde_json::from_slice(body).unwrap_or_default();
    let message = parsed
        .message
        .unwrap_or_else(|| String::from_utf8_lossy(body).to_string());
    let error_type = parsed.error_type.unwrap_or_default();
    if status == 400
        && (error_type.contains("QueueNameExists") || error_type.contains("QueueAlreadyExists"))
    {
        return QueueError::ResourceExists {
            resource_type: "queue",
            resource_id: message,
        };
    }
    let is_json_queue_not_found =
        error_type == sqs_json_error_type(SQS_NON_EXISTENT_QUEUE_ERROR_TYPE);
    if status == 404 || error_type.contains("NonExistentQueue") || is_json_queue_not_found {
        return QueueError::ResourceNotFound {
            resource_type: "queue",
            resource_id: message,
        };
    }
    if status == 400 && error_type.contains("ReceiptHandle") {
        return QueueError::validation_with_detail(
            QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
            message,
        );
    }
    if status == 400 {
        return QueueError::validation_with_detail(
            QueueValidationKind::InvalidParameterValue,
            message,
        );
    }
    QueueError::internal_with_detail(QueueInternalKind::RemoteBackendNotImplemented, message)
}

pub(crate) fn queue_attribute_updates(
    existing: &std::collections::HashMap<String, String>,
    desired: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    desired
        .iter()
        .filter(|(key, desired_value)| existing.get(*key) != Some(*desired_value))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
