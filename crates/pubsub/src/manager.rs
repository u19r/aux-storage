use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures_util::{StreamExt as _, stream};
use http_request::HttpClient;
use pubsub_provider::{
    ClaimDeliveryRecordsRequest, ConfirmSubscriptionRequest, ConfirmSubscriptionResponse,
    CreateTopicRequest, CreateTopicResponse, DeliveryClaimLimit, DeliveryRecord,
    GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse, GetTopicAttributesRequest,
    GetTopicAttributesResponse, ListSubscriptionsRequest, ListSubscriptionsResponse,
    ListTopicsRequest, ListTopicsResponse, PublishRequest, PublishResponse, PubsubError,
    PubsubMessageId, PubsubProvider, PubsubResult, PubsubValidationKind,
    SetSubscriptionAttributesRequest, SetTopicAttributesRequest, SubscribeRequest,
    SubscribeResponse, SubscriptionProtocol, TopicArn, TopicName,
};
use queue::QueueManager;
use storage_types::TimestampMillis;
use stream_provider::SubscriptionMessageSender;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    notification::PubsubNotificationSigner,
    protocol::{PubsubAction, PubsubSuccess, SubscriptionView},
};

#[async_trait]
pub trait PubsubDeliveryAdmission: Send + Sync {
    async fn acquire(&self) -> PubsubResult<Box<dyn PubsubDeliveryPermit>>;
}

pub trait PubsubDeliveryPermit: Send {}

struct BoundedDeliveryAdmission {
    semaphore: Arc<Semaphore>,
}

struct BoundedDeliveryPermit {
    _permit: OwnedSemaphorePermit,
}

impl PubsubDeliveryPermit for BoundedDeliveryPermit {}

#[async_trait]
impl PubsubDeliveryAdmission for BoundedDeliveryAdmission {
    async fn acquire(&self) -> PubsubResult<Box<dyn PubsubDeliveryPermit>> {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .map(|permit| {
                Box::new(BoundedDeliveryPermit { _permit: permit }) as Box<dyn PubsubDeliveryPermit>
            })
            .map_err(|_| PubsubError::storage("pubsub delivery admission is closed"))
    }
}

fn default_delivery_admission() -> Arc<dyn PubsubDeliveryAdmission> {
    Arc::new(BoundedDeliveryAdmission {
        semaphore: Arc::new(Semaphore::new(1)),
    })
}

#[derive(Default)]
pub struct PubsubManagerBuilder {
    provider: Option<Arc<dyn PubsubProvider>>,
    queue_manager: Option<Arc<QueueManager>>,
    subscription_message_sender: Option<Arc<dyn SubscriptionMessageSender>>,
    notification_signer: Option<Arc<dyn PubsubNotificationSigner>>,
    http_client: Option<HttpClient>,
    delivery_admission: Option<Arc<dyn PubsubDeliveryAdmission>>,
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

    #[must_use]
    pub fn delivery_admission(mut self, admission: Arc<dyn PubsubDeliveryAdmission>) -> Self {
        self.delivery_admission = Some(admission);
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
            delivery_admission: self
                .delivery_admission
                .unwrap_or_else(default_delivery_admission),
            delivery_config: self.delivery_config,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PubsubDeliveryConfig {
    pub worker_id: String,
    pub lease_duration: Duration,
    /// Maximum number of claimed deliveries dispatched concurrently.
    ///
    /// The claim lease must cover the slowest in-flight delivery window. A
    /// value of zero is treated as one to keep configuration fail-safe.
    pub delivery_concurrency: usize,
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
            delivery_concurrency: 8,
            max_attempts: 5,
            retry_base_delay: Duration::from_secs(1),
            request_timeout: Duration::from_secs(10),
            subscribe_url_base: None,
            unsubscribe_url_base: None,
            signing_cert_url: None,
        }
    }
}

#[derive(Clone)]
pub struct PubsubManager {
    pub(super) provider: Arc<dyn PubsubProvider>,
    pub(super) queue_manager: Option<Arc<QueueManager>>,
    pub(super) subscription_message_sender: Option<Arc<dyn SubscriptionMessageSender>>,
    pub(super) notification_signer: Option<Arc<dyn PubsubNotificationSigner>>,
    pub(super) http_client: Option<HttpClient>,
    delivery_admission: Arc<dyn PubsubDeliveryAdmission>,
    pub(super) delivery_config: PubsubDeliveryConfig,
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
            delivery_admission: default_delivery_admission(),
            delivery_config: PubsubDeliveryConfig::default(),
        }
    }

    /// Return and clear pressure signals from the pubsub backend so the
    /// enclosing storage admission permit can observe consumed capacity.
    #[must_use]
    pub fn take_admission_pressure_signal(&self) -> bool {
        self.provider.take_admission_pressure_signal()
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
        Ok(SubscribeResponse::from(&subscription))
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
        let delivery_limit = DeliveryClaimLimit::try_from(limit)?;
        let permit = self.delivery_admission.acquire().await?;
        let manager = self.clone();
        use tracing::{Instrument as _, instrument::WithSubscriber as _};
        let settlement = async move {
            let _permit = permit;
            manager
                .execute_due_deliveries((limit, delivery_limit))
                .await
        };
        match tokio::spawn(
            metrics_facade::inherit_request_cost(settlement)
                .instrument(tracing::Span::current())
                .with_current_subscriber(),
        )
        .await
        {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(_) => Err(PubsubError::storage("pubsub delivery settlement stopped")),
        }
    }

    async fn execute_due_deliveries(
        &self,
        (limit, delivery_limit): (usize, DeliveryClaimLimit),
    ) -> PubsubResult<usize> {
        self.provider.materialize_publish_intents(limit).await?;
        let now = TimestampMillis::now();
        let lease_expires_at = self.delivery_lease_deadline(now);
        let records = self
            .provider
            .claim_delivery_records(ClaimDeliveryRecordsRequest {
                owner: self.delivery_config.worker_id.clone(),
                now,
                lease_expires_at,
                limit: delivery_limit,
            })
            .await?
            .records;
        let count = records.len();
        let concurrency = self.delivery_config.delivery_concurrency.max(1);
        let mut deliveries = stream::iter(records)
            .map(|record| self.process_delivery_record(record))
            .buffered(concurrency);
        let mut first_error = None;
        while let Some(result) = deliveries.next().await {
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(count), Err)
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

    async fn process_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        let outcome = self.deliver_claimed_record(record).await?;
        self.provider.update_delivery_record(outcome).await
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

pub(super) fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}
