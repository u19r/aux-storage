use std::time::Duration;

use async_trait::async_trait;
use aws_sigv4_signing::{AwsRequestSigner, AwsStaticCredentials, CredentialSource, SignableBody};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Uri};
use http_request::reqwest::Client;
use queue_provider::{
    BatchResultErrorEntry, ChangeMessageVisibilityBatchRequest,
    ChangeMessageVisibilityBatchRequestEntry, ChangeMessageVisibilityBatchResponse,
    ChangeMessageVisibilityRequest, CreateQueueRequest, CreateQueueResponse,
    DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, DeleteMessageBatchResponse,
    DeleteMessageRequest, DeleteQueueRequest, GetQueueAttributesRequest,
    GetQueueAttributesResponse, GetQueueUrlRequest, GetQueueUrlResponse, ListQueuesRequest,
    ListQueuesResponse, MessageId, PublishMessageRequest, PurgeQueueRequest, Queue, QueueError,
    QueueInternalKind, QueueMessage, QueueOperations, QueueProvider, QueueResult,
    QueueValidationKind, ReceiptHandle, ReceiveMessageRequest, ReceiveMessageResponse,
    RemoteCredentialStrategy, RemoteQueueSettings, SQS_NON_EXISTENT_QUEUE_ERROR_TYPE,
    SendMessageBatchRequest, SendMessageBatchRequestEntry, SendMessageBatchResponse,
    SendMessageRequest, SendMessageResponse, SetQueueAttributesRequest, sqs_json_error_type,
};
use serde::{Serialize, de::DeserializeOwned};
use tracing::instrument;

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
                QueueError::internal_with_detail(
                    QueueInternalKind::RemoteBackendNotImplemented,
                    format!("remote queue transport: {err}"),
                )
            })?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(|err| {
            QueueError::internal_with_detail(
                QueueInternalKind::RemoteBackendNotImplemented,
                format!("remote queue body read: {err}"),
            )
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
            QueueProvider::get_queue_by_name(self, &queue.queue_name).await?
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
            QueueProvider::set_queue_attributes(self, &existing_queue.queue_url, attribute_updates)
                .await?;
            existing_queue.attributes = queue.attributes;
        }
        Ok(existing_queue)
    }
}

#[async_trait]
impl QueueProvider for RemoteQueueProvider {
    async fn initialize(&self) -> QueueResult<()> {
        Ok(())
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue> {
        match self
            .invoke::<_, CreateQueueResponse>(
                "AmazonSQS.CreateQueue",
                &CreateQueueRequest {
                    queue_name: queue.queue_name.clone(),
                    attributes: Some(queue.attributes.clone()),
                },
            )
            .await
        {
            Ok(response) => Ok(Queue {
                queue_name: queue.queue_name,
                queue_url: response.queue_url,
                attributes: queue.attributes,
                created_at: queue.created_at,
            }),
            Err(QueueError::ResourceExists { .. }) => self.ensure_queue_matches(queue).await,
            Err(err) => Err(err),
        }
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        let response = self
            .invoke::<_, GetQueueAttributesResponse>(
                "AmazonSQS.GetQueueAttributes",
                &GetQueueAttributesRequest {
                    queue_url: queue_url.to_string(),
                    attribute_names: None,
                },
            )
            .await;
        match response {
            Ok(attributes) => Ok(Some(Queue {
                queue_name: queue_name_from_url(queue_url)?,
                queue_url: queue_url.to_string(),
                attributes: attributes.attributes,
                created_at: storage_types::TimestampMillis::now(),
            })),
            Err(QueueError::ResourceNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        let Some(queue_url) = QueueProvider::get_queue_url_by_name(self, queue_name).await? else {
            return Ok(None);
        };
        QueueProvider::get_queue(self, &queue_url).await
    }

    async fn get_queue_url_by_name(&self, queue_name: &str) -> QueueResult<Option<String>> {
        let response = self
            .invoke::<_, GetQueueUrlResponse>(
                "AmazonSQS.GetQueueUrl",
                &GetQueueUrlRequest {
                    queue_name: queue_name.to_string(),
                },
            )
            .await;
        match response {
            Ok(response) => Ok(Some(response.queue_url)),
            Err(QueueError::ResourceNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn list_queues(&self, queue_name_prefix: Option<&str>) -> QueueResult<Vec<Queue>> {
        let response: ListQueuesResponse = self
            .invoke(
                "AmazonSQS.ListQueues",
                &ListQueuesRequest {
                    queue_name_prefix: queue_name_prefix.map(ToString::to_string),
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
            });
        }
        Ok(queues)
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
                            .map(delay_seconds_from_timestamp),
                        message_attributes: message.message_attributes,
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
                        .map(delay_seconds_from_timestamp),
                    message_attributes: message.message_attributes,
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
                    .map(delay_seconds_from_timestamp),
                message_attributes: message.message_attributes,
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

    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: storage_types::DurationSeconds,
        wait_time_seconds: storage_types::DurationSeconds,
    ) -> QueueResult<Vec<queue_provider::MessageResponse>> {
        let response: ReceiveMessageResponse = self
            .invoke(
                "AmazonSQS.ReceiveMessage",
                &ReceiveMessageRequest {
                    queue_url: queue_url.to_string(),
                    max_number_of_messages: Some(max_messages),
                    visibility_timeout: Some(*visibility_timeout),
                    wait_time_seconds: Some(*wait_time_seconds),
                    attribute_names: Some(vec![
                        "SentTimestamp".into(),
                        "ApproximateReceiveCount".into(),
                    ]),
                    message_attribute_names: Some(vec!["All".into()]),
                },
            )
            .await?;
        Ok(response.messages)
    }

    async fn delete_message(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
    ) -> QueueResult<()> {
        self.invoke_bytes(
            "AmazonSQS.DeleteMessage",
            &DeleteMessageRequest {
                queue_url: queue_url.to_string(),
                receipt_handle,
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_messages(
        &self,
        queue_url: &str,
        receipt_handles: Vec<ReceiptHandle>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        if receipt_handles.is_empty() {
            return Ok(Vec::new());
        }
        let entry_count = receipt_handles.len();
        let entries = receipt_handles
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
        receipt_handle: ReceiptHandle,
        visibility_timeout: storage_types::DurationSeconds,
    ) -> QueueResult<()> {
        self.invoke_bytes(
            "AmazonSQS.ChangeMessageVisibility",
            &ChangeMessageVisibilityRequest {
                queue_url: queue_url.to_string(),
                receipt_handle,
                visibility_timeout: *visibility_timeout,
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
                    visibility_timeout: *visibility_timeout,
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
        _receipt_handle: ReceiptHandle,
        _checkpoint_data: String,
    ) -> QueueResult<()> {
        Ok(())
    }
}

#[async_trait]
impl QueueOperations for RemoteQueueProvider {
    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue> {
        QueueProvider::create_queue(self, queue).await
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        QueueProvider::get_queue(self, queue_url).await
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        QueueProvider::get_queue_by_name(self, queue_name).await
    }

    async fn get_queue_url_by_name(&self, queue_name: &str) -> QueueResult<Option<String>> {
        QueueProvider::get_queue_url_by_name(self, queue_name).await
    }

    async fn list_queues(&self, prefix: Option<&str>) -> QueueResult<Vec<Queue>> {
        QueueProvider::list_queues(self, prefix).await
    }

    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()> {
        QueueProvider::delete_queue(self, queue_url).await
    }

    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()> {
        QueueProvider::purge_queue(self, queue_url).await
    }

    async fn get_queue_with_message_counts(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, queue_provider::QueueMessageCounts)>> {
        QueueProvider::get_queue_with_message_counts(self, queue_url).await
    }

    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: std::collections::HashMap<String, String>,
    ) -> QueueResult<()> {
        QueueProvider::set_queue_attributes(self, queue_url, attributes).await
    }

    async fn publish_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        QueueProvider::publish_message(self, message).await
    }

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        QueueProvider::send_message(self, message).await
    }

    async fn send_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<QueueResult<MessageId>>> {
        QueueProvider::send_messages(self, messages).await
    }

    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: storage_types::DurationSeconds,
        wait_time_seconds: storage_types::DurationSeconds,
    ) -> QueueResult<Vec<queue_provider::MessageResponse>> {
        QueueProvider::receive_messages(
            self,
            queue_url,
            max_messages,
            visibility_timeout,
            wait_time_seconds,
        )
        .await
    }

    async fn delete_message(&self, queue_url: &str, receipt: ReceiptHandle) -> QueueResult<()> {
        QueueProvider::delete_message(self, queue_url, receipt).await
    }

    async fn delete_messages(
        &self,
        queue_url: &str,
        receipts: Vec<ReceiptHandle>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        QueueProvider::delete_messages(self, queue_url, receipts).await
    }

    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt: ReceiptHandle,
        visibility_timeout: storage_types::DurationSeconds,
    ) -> QueueResult<()> {
        QueueProvider::change_message_visibility(self, queue_url, receipt, visibility_timeout).await
    }

    async fn change_message_visibilities(
        &self,
        queue_url: &str,
        entries: Vec<(ReceiptHandle, storage_types::DurationSeconds)>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        QueueProvider::change_message_visibilities(self, queue_url, entries).await
    }

    async fn update_message_snapshot_checkpoint(
        &self,
        queue_url: &str,
        receipt: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()> {
        QueueProvider::update_message_snapshot_checkpoint(self, queue_url, receipt, checkpoint_data)
            .await
    }
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

fn build_client(timeouts: Option<&queue_provider::RemoteTimeoutOverrides>) -> QueueResult<Client> {
    let mut builder = Client::builder();
    if let Some(overrides) = timeouts {
        if let Some(connect_ms) = overrides.connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(connect_ms));
        }
        if let Some(request_ms) = overrides.request_timeout_ms {
            builder = builder.timeout(Duration::from_millis(request_ms));
        }
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

fn queue_name_from_url(queue_url: &str) -> QueueResult<String> {
    queue_url
        .split('/')
        .next_back()
        .map(ToString::to_string)
        .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))
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
