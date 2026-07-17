use std::{collections::HashMap, sync::Arc, time::Duration};

use http_request::{HttpClient, StatusCode};
use pubsub_provider::{
    ClaimDeliveryRecordsRequest, ConfirmSubscriptionRequest, ConfirmSubscriptionResponse,
    CreateTopicRequest, CreateTopicResponse, DeliveryRecord, DeliveryRecordId, DeliveryRecordKind,
    DeliveryStatus, DeliveryTarget, GetSubscriptionAttributesRequest,
    GetSubscriptionAttributesResponse, GetTopicAttributesRequest, GetTopicAttributesResponse,
    ListSubscriptionsRequest, ListSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse,
    PublishRequest, PublishResponse, PubsubError, PubsubMessageId, PubsubProvider, PubsubResult,
    PubsubValidationKind, SetSubscriptionAttributesRequest, SetTopicAttributesRequest,
    SubscribeRequest, SubscribeResponse, Subscription, SubscriptionProtocol, TopicArn, TopicName,
};
use queue::QueueManager;
use queue_provider::{MessageAttributeValue, SendMessageRequest};
use storage_types::TimestampMillis;
use stream_provider::{
    SubscriptionDestination, SubscriptionMessage, SubscriptionMessageSender,
    SubscriptionSendOutcome,
};

use crate::{
    notification::{
        ConfirmationRenderContext, NotificationRenderContext, PubsubNotificationSigner,
        confirmation_body, confirmation_headers, notification_body, notification_headers,
    },
    protocol::{PubsubAction, PubsubSuccess, SubscriptionView},
};

#[derive(Default)]
pub struct PubsubManagerBuilder {
    provider: Option<Arc<dyn PubsubProvider>>,
    queue_manager: Option<Arc<QueueManager>>,
    subscription_message_sender: Option<Arc<dyn SubscriptionMessageSender>>,
    notification_signer: Option<Arc<dyn PubsubNotificationSigner>>,
    http_client: Option<HttpClient>,
    delivery_config: PubsubDeliveryConfig,
}

impl PubsubManagerBuilder {
    #[must_use]
    pub fn provider(mut self, provider: Arc<dyn PubsubProvider>) -> Self {
        self.provider = Some(provider);
        self
    }

    #[must_use]
    pub fn queue_manager(mut self, queue_manager: Arc<QueueManager>) -> Self {
        self.queue_manager = Some(queue_manager);
        self
    }

    #[must_use]
    pub fn subscription_message_sender(
        mut self,
        sender: Arc<dyn SubscriptionMessageSender>,
    ) -> Self {
        self.subscription_message_sender = Some(sender);
        self
    }

    #[must_use]
    pub fn notification_signer(mut self, signer: Arc<dyn PubsubNotificationSigner>) -> Self {
        self.notification_signer = Some(signer);
        self
    }

    #[must_use]
    pub fn http_client(mut self, http_client: HttpClient) -> Self {
        self.http_client = Some(http_client);
        self
    }

    #[must_use]
    pub fn delivery_config(mut self, delivery_config: PubsubDeliveryConfig) -> Self {
        self.delivery_config = delivery_config;
        self
    }

    pub fn build(self) -> PubsubResult<PubsubManager> {
        let Some(provider) = self.provider else {
            return Err(PubsubError::validation_with_detail(
                PubsubValidationKind::UnsupportedAttribute,
                "pubsub provider is required",
            ));
        };
        Ok(PubsubManager {
            provider,
            queue_manager: self.queue_manager,
            subscription_message_sender: self.subscription_message_sender,
            notification_signer: self.notification_signer,
            http_client: match self.http_client {
                Some(http_client) => Some(http_client),
                None => Some(HttpClient::new().map_err(PubsubError::storage)?),
            },
            delivery_config: self.delivery_config,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PubsubDeliveryConfig {
    pub worker_id: String,
    pub lease_duration: Duration,
    pub max_attempts: u32,
    pub retry_base_delay: Duration,
    pub request_timeout: Duration,
    pub subscribe_url_base: Option<String>,
    pub unsubscribe_url_base: Option<String>,
    pub signing_cert_url: Option<String>,
}

impl Default for PubsubDeliveryConfig {
    fn default() -> Self {
        Self {
            worker_id: "pubsub-worker".to_string(),
            lease_duration: Duration::from_secs(30),
            max_attempts: 5,
            retry_base_delay: Duration::from_secs(1),
            request_timeout: Duration::from_secs(10),
            subscribe_url_base: None,
            unsubscribe_url_base: None,
            signing_cert_url: None,
        }
    }
}

pub struct PubsubManager {
    provider: Arc<dyn PubsubProvider>,
    queue_manager: Option<Arc<QueueManager>>,
    subscription_message_sender: Option<Arc<dyn SubscriptionMessageSender>>,
    notification_signer: Option<Arc<dyn PubsubNotificationSigner>>,
    http_client: Option<HttpClient>,
    delivery_config: PubsubDeliveryConfig,
}

impl PubsubManager {
    #[must_use]
    pub fn builder() -> PubsubManagerBuilder {
        PubsubManagerBuilder::default()
    }

    #[must_use]
    pub fn new(provider: Arc<dyn PubsubProvider>) -> Self {
        Self {
            provider,
            queue_manager: None,
            subscription_message_sender: None,
            notification_signer: None,
            http_client: None,
            delivery_config: PubsubDeliveryConfig::default(),
        }
    }

    pub async fn create_topic(&self, name: impl Into<String>) -> PubsubResult<CreateTopicResponse> {
        let topic = self
            .provider
            .create_topic(CreateTopicRequest {
                name: TopicName::new(name.into())?,
                attributes: HashMap::new(),
            })
            .await?;
        Ok(CreateTopicResponse {
            topic_arn: topic.topic_arn,
        })
    }

    pub async fn delete_topic(&self, topic_arn: TopicArn) -> PubsubResult<()> {
        self.provider.delete_topic(&topic_arn).await
    }

    pub async fn list_topics(
        &self,
        request: ListTopicsRequest,
    ) -> PubsubResult<ListTopicsResponse> {
        self.provider.list_topics(request).await
    }

    pub async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse> {
        self.provider.get_topic_attributes(request).await
    }

    pub async fn set_topic_attributes(
        &self,
        request: SetTopicAttributesRequest,
    ) -> PubsubResult<()> {
        validate_topic_attributes(request.attributes.keys())?;
        self.provider.set_topic_attributes(request).await?;
        Ok(())
    }

    pub async fn subscribe(&self, request: SubscribeRequest) -> PubsubResult<SubscribeResponse> {
        request.protocol.validate_endpoint(&request.endpoint)?;
        if matches!(request.protocol, SubscriptionProtocol::Queue) && self.queue_manager.is_none() {
            return Err(PubsubError::validation_with_detail(
                PubsubValidationKind::UnsupportedProtocol,
                "sqs",
            ));
        }
        let subscription = self.provider.create_subscription(request).await?;
        if let Some(token) = subscription.confirmation.token() {
            self.record_confirmation_delivery(&subscription, token)
                .await?;
        }
        let subscription_arn = if subscription.confirmation.pending_confirmation() {
            "pending confirmation".to_string()
        } else {
            subscription.subscription_arn.to_string()
        };
        Ok(SubscribeResponse { subscription_arn })
    }

    pub async fn confirm_subscription(
        &self,
        request: ConfirmSubscriptionRequest,
    ) -> PubsubResult<ConfirmSubscriptionResponse> {
        self.provider.confirm_subscription(request).await
    }

    pub async fn unsubscribe(
        &self,
        subscription_arn: pubsub_provider::SubscriptionArn,
    ) -> PubsubResult<()> {
        self.provider.delete_subscription(&subscription_arn).await
    }

    pub async fn list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> PubsubResult<ListSubscriptionsResponse> {
        self.provider.list_subscriptions(request).await
    }

    pub async fn get_subscription_attributes(
        &self,
        request: GetSubscriptionAttributesRequest,
    ) -> PubsubResult<GetSubscriptionAttributesResponse> {
        self.provider.get_subscription_attributes(request).await
    }

    pub async fn set_subscription_attributes(
        &self,
        request: SetSubscriptionAttributesRequest,
    ) -> PubsubResult<()> {
        validate_subscription_attributes(request.attributes.keys())?;
        self.provider.set_subscription_attributes(request).await?;
        Ok(())
    }

    pub async fn publish(&self, request: PublishRequest) -> PubsubResult<PublishResponse> {
        if request.message.is_empty() {
            return Err(PubsubError::validation(PubsubValidationKind::EmptyMessage));
        }
        let message_id = PubsubMessageId::new();
        self.provider
            .accept_publish(
                request,
                message_id.clone(),
                self.subscription_message_sender.is_some(),
            )
            .await?;
        Ok(PublishResponse { message_id })
    }

    pub async fn process_due_deliveries(&self, limit: usize) -> PubsubResult<usize> {
        self.provider.materialize_publish_intents(limit).await?;
        let now = TimestampMillis::now();
        let lease_expires_at = TimestampMillis::from_timestamp(
            now.timestamp_millis()
                .saturating_add(duration_millis_i64(self.delivery_config.lease_duration)),
        );
        let records = self
            .provider
            .claim_delivery_records(ClaimDeliveryRecordsRequest {
                owner: self.delivery_config.worker_id.clone(),
                now,
                lease_expires_at,
                limit,
            })
            .await?
            .records;
        let count = records.len();
        for record in records {
            self.process_delivery_record(record).await?;
        }
        Ok(count)
    }

    pub async fn execute_query_action(&self, action: PubsubAction) -> PubsubResult<PubsubSuccess> {
        match action {
            PubsubAction::CreateTopic { name, attributes } => {
                let topic = self
                    .provider
                    .create_topic(CreateTopicRequest { name, attributes })
                    .await?;
                Ok(PubsubSuccess::CreateTopic {
                    topic_arn: topic.topic_arn,
                })
            }
            PubsubAction::DeleteTopic { topic_arn } => {
                self.delete_topic(topic_arn).await?;
                Ok(PubsubSuccess::DeleteTopic)
            }
            PubsubAction::GetTopicAttributes(request) => {
                let response = self.get_topic_attributes(request).await?;
                Ok(PubsubSuccess::GetTopicAttributes {
                    attributes: response.attributes,
                })
            }
            PubsubAction::SetTopicAttributes(request) => {
                self.set_topic_attributes(request).await?;
                Ok(PubsubSuccess::SetTopicAttributes)
            }
            PubsubAction::ListTopics(request) => {
                let response = self.list_topics(request).await?;
                Ok(PubsubSuccess::ListTopics {
                    topic_arns: response
                        .topics
                        .into_iter()
                        .map(|topic| topic.topic_arn)
                        .collect(),
                })
            }
            PubsubAction::Subscribe(request) => {
                let response = self.subscribe(request).await?;
                Ok(PubsubSuccess::Subscribe {
                    subscription_arn: response.subscription_arn,
                })
            }
            PubsubAction::ConfirmSubscription(request) => {
                let response = self.confirm_subscription(request).await?;
                Ok(PubsubSuccess::ConfirmSubscription {
                    subscription_arn: response.subscription_arn,
                })
            }
            PubsubAction::Unsubscribe { subscription_arn } => {
                self.unsubscribe(subscription_arn).await?;
                Ok(PubsubSuccess::Unsubscribe)
            }
            PubsubAction::GetSubscriptionAttributes(request) => {
                let response = self.get_subscription_attributes(request).await?;
                Ok(PubsubSuccess::GetSubscriptionAttributes {
                    attributes: response.attributes,
                })
            }
            PubsubAction::SetSubscriptionAttributes(request) => {
                self.set_subscription_attributes(request).await?;
                Ok(PubsubSuccess::SetSubscriptionAttributes)
            }
            PubsubAction::ListSubscriptions(request)
            | PubsubAction::ListSubscriptionsByTopic(request) => {
                let response = self.list_subscriptions(request).await?;
                Ok(PubsubSuccess::ListSubscriptions {
                    subscriptions: response
                        .subscriptions
                        .into_iter()
                        .map(SubscriptionView::from)
                        .collect(),
                })
            }
            PubsubAction::Publish(request) => {
                let response = self.publish(request).await?;
                Ok(PubsubSuccess::Publish {
                    message_id: response.message_id,
                })
            }
        }
    }

    async fn record_confirmation_delivery(
        &self,
        subscription: &Subscription,
        token: &str,
    ) -> PubsubResult<()> {
        let now = TimestampMillis::now();
        let message_id = PubsubMessageId::new();
        self.provider
            .put_delivery_record(DeliveryRecord {
                id: DeliveryRecordId(format!(
                    "{}:{}:confirmation",
                    subscription.subscription_arn, message_id
                )),
                kind: DeliveryRecordKind::SubscriptionConfirmation {
                    token: token.to_string(),
                },
                message_id,
                subscription_arn: subscription.subscription_arn.clone(),
                subscription: None,
                message_body: None,
                subject: None,
                message_attributes: HashMap::new(),
                target: DeliveryTarget::BuiltIn,
                status: DeliveryStatus::Pending,
                attempts: 0,
                next_attempt_at: None,
                lease_owner: None,
                lease_expires_at: None,
                last_error: None,
                created_at: now,
                updated_at: now,
            })
            .await
    }

    async fn process_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        let subscription = match record.subscription.clone() {
            Some(subscription) => subscription,
            None => {
                let Some(subscription) = self
                    .provider
                    .get_subscription(&record.subscription_arn)
                    .await?
                else {
                    self.update_delivery(
                        record,
                        DeliveryStatus::Failed,
                        Some("subscription not found".to_string()),
                    )
                    .await?;
                    return Ok(());
                };
                subscription
            }
        };
        if let DeliveryRecordKind::SubscriptionConfirmation { token } = record.kind.clone() {
            self.process_confirmation_delivery(record, subscription, token)
                .await?;
            return Ok(());
        }
        let Some(message_body) = record.message_body.clone() else {
            self.update_delivery(
                record,
                DeliveryStatus::Failed,
                Some("message body missing".to_string()),
            )
            .await?;
            return Ok(());
        };
        let request = PublishRequest {
            topic_arn: subscription.topic_arn.clone(),
            message: message_body,
            subject: record.subject.clone(),
            message_attributes: record.message_attributes.clone(),
        };

        match record.target {
            DeliveryTarget::CustomSender => {
                self.process_custom_delivery(record, subscription, request)
                    .await
            }
            DeliveryTarget::BuiltIn => {
                self.process_builtin_delivery(record, subscription, request)
                    .await
            }
        }
    }

    async fn process_custom_delivery(
        &self,
        record: DeliveryRecord,
        subscription: Subscription,
        request: PublishRequest,
    ) -> PubsubResult<()> {
        let Some(sender) = self.subscription_message_sender.as_ref() else {
            self.schedule_or_fail(record, "custom sender is not configured")
                .await?;
            return Ok(());
        };
        let destination = subscription_destination(&subscription);
        let message = SubscriptionMessage::new(
            subscription.subscription_arn.to_string(),
            record.message_id.to_string(),
            destination,
            request.message.as_bytes().to_vec(),
        )
        .with_attributes(request.message_attributes);
        match sender.send_subscription_message(message).await {
            Ok(SubscriptionSendOutcome::Delivered) => {
                self.update_delivery(record, DeliveryStatus::Delivered, None)
                    .await
            }
            Ok(SubscriptionSendOutcome::AcceptedForDelivery) => {
                self.update_delivery(record, DeliveryStatus::AcceptedByCustomSender, None)
                    .await
            }
            Err(error) => self.schedule_or_fail(record, &error.to_string()).await,
        }
    }

    async fn process_builtin_delivery(
        &self,
        record: DeliveryRecord,
        subscription: Subscription,
        request: PublishRequest,
    ) -> PubsubResult<()> {
        match subscription.protocol {
            SubscriptionProtocol::Queue => {
                let Some(queue_manager) = self.queue_manager.as_ref() else {
                    self.schedule_or_fail(record, "queue manager is not configured")
                        .await?;
                    return Ok(());
                };
                let message_body = if subscription.raw_message_delivery {
                    request.message.clone()
                } else {
                    self.pubsub_notification_body(&request, &record.message_id, &subscription)?
                };
                let message_attributes = if subscription.raw_message_delivery {
                    queue_message_attributes(&request.message_attributes)
                } else {
                    None
                };
                match queue_manager
                    .send_message(SendMessageRequest {
                        queue_url: subscription.endpoint,
                        message_body,
                        delay_seconds: None,
                        message_attributes,
                    })
                    .await
                {
                    Ok(_) => {
                        self.update_delivery(record, DeliveryStatus::Delivered, None)
                            .await
                    }
                    Err(error) => self.schedule_or_fail(record, &error.to_string()).await,
                }
            }
            SubscriptionProtocol::Http | SubscriptionProtocol::Https => {
                let body =
                    self.pubsub_notification_body(&request, &record.message_id, &subscription)?;
                let Some(http_client) = self.http_client.as_ref() else {
                    self.schedule_or_fail(record, "HTTP client is not configured")
                        .await?;
                    return Ok(());
                };
                let headers = notification_headers(&record.message_id, &subscription, &request)?;
                match http_client
                    .post(subscription.endpoint.as_str())
                    .headers(headers)
                    .timeout(self.delivery_config.request_timeout)
                    .body(body)
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => {
                        self.update_delivery(record, DeliveryStatus::Delivered, None)
                            .await
                    }
                    Ok(response) => {
                        let status = response.status();
                        if is_retryable_status(status) {
                            self.schedule_or_fail(record, &format!("HTTP delivery status {status}"))
                                .await
                        } else {
                            self.update_delivery(
                                record,
                                DeliveryStatus::Failed,
                                Some(format!("HTTP delivery status {status}")),
                            )
                            .await
                        }
                    }
                    Err(error) => self.schedule_or_fail(record, &error.to_string()).await,
                }
            }
        }
    }

    async fn process_confirmation_delivery(
        &self,
        record: DeliveryRecord,
        subscription: Subscription,
        token: String,
    ) -> PubsubResult<()> {
        if !matches!(
            subscription.protocol,
            SubscriptionProtocol::Http | SubscriptionProtocol::Https
        ) {
            self.update_delivery(
                record,
                DeliveryStatus::Failed,
                Some("subscription confirmation is only supported for HTTP endpoints".to_string()),
            )
            .await?;
            return Ok(());
        }
        if !subscription.confirmation.pending_confirmation() {
            self.update_delivery(record, DeliveryStatus::Delivered, None)
                .await?;
            return Ok(());
        }
        let Some(http_client) = self.http_client.as_ref() else {
            self.schedule_or_fail(record, "HTTP client is not configured")
                .await?;
            return Ok(());
        };
        let body = confirmation_body(ConfirmationRenderContext {
            topic_arn: &subscription.topic_arn,
            message_id: &record.message_id,
            token: &token,
            delivery_config: &self.delivery_config,
            signer: self.notification_signer.as_deref(),
        })?;
        let headers = confirmation_headers(&record.message_id, &subscription.topic_arn)?;
        match http_client
            .post(subscription.endpoint.as_str())
            .headers(headers)
            .timeout(self.delivery_config.request_timeout)
            .body(body)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                self.update_delivery(record, DeliveryStatus::Delivered, None)
                    .await
            }
            Ok(response) => {
                let status = response.status();
                if is_retryable_status(status) {
                    self.schedule_or_fail(record, &format!("HTTP delivery status {status}"))
                        .await
                } else {
                    self.update_delivery(
                        record,
                        DeliveryStatus::Failed,
                        Some(format!("HTTP delivery status {status}")),
                    )
                    .await
                }
            }
            Err(error) => self.schedule_or_fail(record, &error.to_string()).await,
        }
    }

    async fn schedule_or_fail(&self, mut record: DeliveryRecord, error: &str) -> PubsubResult<()> {
        let attempts = record.attempts.saturating_add(1);
        if attempts >= self.delivery_config.max_attempts {
            self.update_delivery_record(record, DeliveryStatus::Failed, Some(error.to_string()))
                .await
        } else {
            record.attempts = attempts;
            record.status = DeliveryStatus::RetryScheduled;
            let now = TimestampMillis::now();
            record.next_attempt_at = Some(TimestampMillis::from_timestamp(
                now.timestamp_millis()
                    .saturating_add(self.retry_delay_millis(attempts)),
            ));
            record.lease_owner = None;
            record.lease_expires_at = None;
            record.last_error = Some(error.to_string());
            record.updated_at = now;
            self.provider.update_delivery_record(record).await
        }
    }

    async fn update_delivery(
        &self,
        record: DeliveryRecord,
        status: DeliveryStatus,
        last_error: Option<String>,
    ) -> PubsubResult<()> {
        self.update_delivery_record(record, status, last_error)
            .await
    }

    async fn update_delivery_record(
        &self,
        mut record: DeliveryRecord,
        status: DeliveryStatus,
        last_error: Option<String>,
    ) -> PubsubResult<()> {
        record.attempts = record.attempts.saturating_add(1);
        record.status = status;
        record.next_attempt_at = None;
        record.lease_owner = None;
        record.lease_expires_at = None;
        record.last_error = last_error;
        record.updated_at = TimestampMillis::now();
        self.provider.update_delivery_record(record).await
    }

    fn retry_delay_millis(&self, attempts: u32) -> i64 {
        let exponent = attempts.saturating_sub(1).min(10);
        duration_millis_i64(self.delivery_config.retry_base_delay)
            .saturating_mul(2_i64.saturating_pow(exponent))
    }

    fn pubsub_notification_body(
        &self,
        request: &PublishRequest,
        message_id: &PubsubMessageId,
        subscription: &Subscription,
    ) -> PubsubResult<String> {
        notification_body(NotificationRenderContext {
            request,
            message_id,
            subscription,
            delivery_config: &self.delivery_config,
            signer: self.notification_signer.as_deref(),
        })
    }
}

fn validate_topic_attributes<'a>(keys: impl Iterator<Item = &'a String>) -> PubsubResult<()> {
    for key in keys {
        if key != "DisplayName" {
            return Err(PubsubError::validation_with_detail(
                PubsubValidationKind::UnsupportedAttribute,
                key,
            ));
        }
    }
    Ok(())
}

fn validate_subscription_attributes<'a>(
    keys: impl Iterator<Item = &'a String>,
) -> PubsubResult<()> {
    for key in keys {
        if key != "RawMessageDelivery" {
            return Err(PubsubError::validation_with_detail(
                PubsubValidationKind::UnsupportedAttribute,
                key,
            ));
        }
    }
    Ok(())
}

fn queue_message_attributes(
    attributes: &HashMap<String, String>,
) -> Option<HashMap<String, MessageAttributeValue>> {
    if attributes.is_empty() {
        return None;
    }
    Some(
        attributes
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    MessageAttributeValue {
                        string_value: Some(value.clone()),
                        binary_value: None,
                        data_type: "String".to_string(),
                    },
                )
            })
            .collect(),
    )
}

fn subscription_destination(subscription: &Subscription) -> SubscriptionDestination {
    SubscriptionDestination::new(
        subscription.protocol.as_str(),
        subscription.endpoint.clone(),
    )
    .with_extra_json(subscription.extra_json.clone())
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}
