use std::time::Duration;

use async_trait::async_trait;
use aws_sigv4_signing::{AwsRequestSigner, AwsStaticCredentials, CredentialSource, SignableBody};
use http::{HeaderMap, HeaderValue, Uri};
use http_request::reqwest::Client;
use queue_provider::{
    ChangeMessageVisibilityRequest, CreateQueueRequest, CreateQueueResponse, DeleteMessageRequest,
    DeleteQueueRequest, GetQueueAttributesRequest, GetQueueAttributesResponse, GetQueueUrlRequest,
    GetQueueUrlResponse, ListQueuesRequest, ListQueuesResponse, MessageId, PurgeQueueRequest,
    Queue, QueueError, QueueInternalKind, QueueMessage, QueueProvider, QueueResult,
    QueueValidationKind, ReceiptHandle, ReceiveMessageRequest, ReceiveMessageResponse,
    RemoteCredentialStrategy, RemoteQueueSettings, SendMessageRequest, SendMessageResponse,
    SetQueueAttributesRequest,
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

    async fn invoke_bytes<Request>(&self, target: &str, request: &Request) -> QueueResult<Vec<u8>>
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
        Ok(bytes.to_vec())
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

    async fn ensure_queue_matches(&self, queue: Queue) -> QueueResult<()> {
        let Some(existing_queue) = self.get_queue_by_name(&queue.queue_name).await? else {
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
        if attribute_updates.is_empty() {
            return Ok(());
        }

        self.set_queue_attributes(&existing_queue.queue_url, attribute_updates)
            .await
    }
}

#[async_trait]
impl QueueProvider for RemoteQueueProvider {
    async fn initialize(&self) -> QueueResult<()> {
        Ok(())
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<()> {
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
            Ok(_) => Ok(()),
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
        let response = self
            .invoke::<_, GetQueueUrlResponse>(
                "AmazonSQS.GetQueueUrl",
                &GetQueueUrlRequest {
                    queue_name: queue_name.to_string(),
                },
            )
            .await;
        match response {
            Ok(queue) => self.get_queue(&queue.queue_url).await,
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

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        let response: SendMessageResponse = self
            .invoke(
                "AmazonSQS.SendMessage",
                &SendMessageRequest {
                    queue_url: message.queue_url,
                    message_body: message.body,
                    delay_seconds: message.visibility_timestamp.map(|visibility| {
                        let now = storage_types::TimestampMillis::now().timestamp_millis();
                        visibility.timestamp_millis().saturating_sub(now) as u32 / 1000
                    }),
                    message_attributes: message.message_attributes,
                },
            )
            .await?;
        Ok(response.message_id)
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
                    attribute_names: None,
                    message_attribute_names: None,
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

    async fn update_message_snapshot_checkpoint(
        &self,
        _queue_url: &str,
        _receipt_handle: ReceiptHandle,
        _checkpoint_data: String,
    ) -> QueueResult<()> {
        Ok(())
    }
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
    if status == 404 || error_type.contains("NonExistentQueue") {
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
            QueueValidationKind::MessageNotFound,
            if error_type.is_empty() {
                message
            } else {
                error_type
            },
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
