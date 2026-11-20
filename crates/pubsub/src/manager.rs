use std::{collections::HashMap, sync::Arc};

use http_request::HttpClient;
use pubsub_provider::{
    AskMode, ClaimTerms, ConfirmSubscriptionRequest, ConfirmSubscriptionResponse,
    CreateTopicRequest, CreateTopicResponse, DeliveryClaimRequest, DestinationAsk,
    DestinationClaim, GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse,
    GetTopicAttributesRequest, GetTopicAttributesResponse, ListSubscriptionsRequest,
    ListSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse, ProtocolRejection,
    PublishBatchEntry, PublishBatchResult, PublishRequest, PublishResponse, PubsubError,
    PubsubMessageId, PubsubProvider, PubsubResult, PubsubValidationKind,
    SetSubscriptionAttributesRequest, SetTopicAttributesRequest, SubscribeRequest,
    SubscribeResponse, Subscription, SubscriptionAttributes, SubscriptionProtocol, SweepCursor,
    Topic, TopicArn, TopicName,
};
use queue::QueueWakeHandle;
use storage_types::MessagingIdentity;
use stream_provider::SubscriptionMessageSender;

use crate::{
    delivery_config::PubsubDeliveryConfig,
    delivery_runner::DeliveryWake,
    notification::PubsubNotificationSigner,
    protocol::{PublishBatchFailure, PubsubAction, PubsubSuccess, SubscriptionView},
};

#[derive(Default)]
pub struct PubsubManagerBuilder {
    provider: Option<Arc<dyn PubsubProvider>>,
    queue_wake: Option<QueueWakeHandle>,
    subscription_message_sender: Option<Arc<dyn SubscriptionMessageSender>>,
    notification_signer: Option<Arc<dyn PubsubNotificationSigner>>,
    http_client: Option<HttpClient>,
    delivery_config: PubsubDeliveryConfig,
    delivery_wake: Option<DeliveryWake>,
    identity: MessagingIdentity,
}

impl PubsubManagerBuilder {
    /// The partition, region, and account of topic ARNs and SQS endpoints.
    #[must_use]
    pub fn messaging_identity(mut self, identity: MessagingIdentity) -> Self {
        self.identity = identity;
        self
    }

    #[must_use]
    pub fn provider(mut self, provider: Arc<dyn PubsubProvider>) -> Self {
        self.provider = Some(provider);
        self
    }

    /// The waiting receives of this process's queue service. `sqs`
    /// subscriptions need it (design 8.6); a delivery that commits into a
    /// queue wakes them.
    #[must_use]
    pub fn queue_wake(mut self, queue_wake: QueueWakeHandle) -> Self {
        self.queue_wake = Some(queue_wake);
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

    /// The wake that a publish on this manager sends to the runner of this
    /// process (design 4.4). The route manager and the delivery manager of
    /// one process share it.
    #[must_use]
    pub fn delivery_wake(mut self, wake: DeliveryWake) -> Self {
        self.delivery_wake = Some(wake);
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
            queue_wake: self.queue_wake,
            subscription_message_sender: self.subscription_message_sender,
            notification_signer: self.notification_signer,
            http_client: match self.http_client {
                Some(http_client) => Some(http_client),
                None => Some(HttpClient::new().map_err(PubsubError::storage)?),
            },
            delivery_config: self.delivery_config,
            delivery_wake: self.delivery_wake.unwrap_or_default(),
            identity: self.identity,
        })
    }
}

#[derive(Clone)]
pub struct PubsubManager {
    pub(super) provider: Arc<dyn PubsubProvider>,
    pub(super) queue_wake: Option<QueueWakeHandle>,
    pub(super) subscription_message_sender: Option<Arc<dyn SubscriptionMessageSender>>,
    pub(super) notification_signer: Option<Arc<dyn PubsubNotificationSigner>>,
    pub(super) http_client: Option<HttpClient>,
    pub(super) delivery_config: PubsubDeliveryConfig,
    delivery_wake: DeliveryWake,
    pub(super) identity: MessagingIdentity,
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
            queue_wake: None,
            subscription_message_sender: None,
            notification_signer: None,
            http_client: None,
            delivery_config: PubsubDeliveryConfig::default(),
            delivery_wake: DeliveryWake::default(),
            identity: MessagingIdentity::default(),
        }
    }

    pub async fn create_topic(&self, name: impl Into<String>) -> PubsubResult<CreateTopicResponse> {
        let topic = self
            .create_topic_with(CreateTopicRequest {
                name: TopicName::new(name.into())?,
                attributes: HashMap::new(),
                tags: std::collections::BTreeMap::new(),
            })
            .await?;
        Ok(CreateTopicResponse {
            topic_arn: topic.topic_arn,
        })
    }

    /// One attribute rule for create and set (design 8.3); tags are stored.
    pub async fn create_topic_with(&self, request: CreateTopicRequest) -> PubsubResult<Topic> {
        self.provider
            .create_topic(Topic::try_new(request, &self.identity)?)
            .await
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
        Topic::validate_attribute_update(&request.attributes)?;
        self.provider.set_topic_attributes(request).await?;
        Ok(())
    }

    /// `sqs` needs the queue service in this process (design 8.6). The
    /// endpoint and every attribute are validated before the store is read.
    pub async fn subscribe(&self, request: SubscribeRequest) -> PubsubResult<SubscribeResponse> {
        let subscription = self.subscribe_subscription(request).await?;
        Ok(SubscribeResponse::from(&subscription))
    }

    async fn subscribe_subscription(
        &self,
        request: SubscribeRequest,
    ) -> PubsubResult<Subscription> {
        if matches!(request.protocol, SubscriptionProtocol::Queue) && self.queue_wake.is_none() {
            return Err(ProtocolRejection::NotImplemented.error("sqs"));
        }
        let subscription = Subscription::try_new(request, &self.identity)?;
        self.provider.create_subscription(subscription).await
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
        self.provider.unsubscribe(&subscription_arn).await
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
        let attributes = SubscriptionAttributes::parse(&request.attributes, &self.identity)?;
        self.provider
            .set_subscription_attributes(&request.subscription_arn, attributes)
            .await?;
        Ok(())
    }

    pub async fn publish(&self, request: PublishRequest) -> PubsubResult<PublishResponse> {
        let message_id = PubsubMessageId::new();
        self.provider
            .accept_publish(
                request,
                message_id.clone(),
                self.subscription_message_sender.is_some(),
            )
            .await?;
        self.delivery_wake.notify();
        Ok(PublishResponse { message_id })
    }

    /// Accept every valid entry in one Durable attempt (W3 design 8); an
    /// invalid entry is a failed member.
    pub async fn publish_batch(
        &self,
        topic_arn: TopicArn,
        entries: Vec<PublishBatchEntry>,
    ) -> PubsubResult<Vec<PublishBatchResult>> {
        let results = self
            .provider
            .accept_publish_batch(
                topic_arn,
                entries,
                self.subscription_message_sender.is_some(),
            )
            .await?;
        if results
            .iter()
            .any(|result| matches!(result, PublishBatchResult::Successful { .. }))
        {
            self.delivery_wake.notify();
        }
        Ok(results)
    }

    /// The wake that publishes on this manager send.
    #[must_use]
    pub fn delivery_wake(&self) -> DeliveryWake {
        self.delivery_wake.clone()
    }

    #[must_use]
    pub fn delivery_config(&self) -> &PubsubDeliveryConfig {
        &self.delivery_config
    }

    /// One deterministic pass for tests and scenario steps: materialize,
    /// find the due destinations with a full sweep, claim up to `limit`
    /// records, then attempt and settle each one with the runner's
    /// functions. A record that fails to settle does not stop the others;
    /// the first error is returned.
    pub async fn process_due_deliveries(&self, limit: usize) -> PubsubResult<usize> {
        if limit == 0 {
            return Err(PubsubError::storage("delivery pass limit must be positive"));
        }
        let batch = pubsub_provider::CLAIM_BATCH_LIMIT as usize;
        loop {
            let report = self.provider.materialize_publish_intents(batch).await?;
            if let Some(error) = report.first_error {
                return Err(error);
            }
            if report.chunks == 0 {
                break;
            }
        }
        let mut count = 0;
        let mut first_error = None;
        while count < limit {
            let asks = self.due_asks((limit - count).min(batch)).await?;
            if asks.is_empty() {
                break;
            }
            let claims = self.claim_asks(asks).await?;
            let claimed: Vec<_> = claims.into_iter().flat_map(|claim| claim.claimed).collect();
            if claimed.is_empty() {
                break;
            }
            count += claimed.len();
            // Records for one queue of this store go in one attempt (E1).
            let (batches, others) = self.split_local(claimed);
            let local = futures_util::future::join_all(
                batches
                    .into_iter()
                    .map(|batch| self.deliver_local_batch(batch.queue, batch.items)),
            );
            let remote = futures_util::future::join_all(
                others
                    .into_iter()
                    .map(|claimed| self.deliver_and_settle(claimed)),
            );
            let (local, remote) = futures_util::future::join(local, remote).await;
            if let Some(error) = local
                .into_iter()
                .find_map(Result::err)
                .or_else(|| remote.into_iter().find_map(Result::err))
            {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(count), Err)
    }

    /// Asks for the due destinations of a full sweep, at most `want`
    /// records in all.
    async fn due_asks(&self, want: usize) -> PubsubResult<Vec<DestinationAsk>> {
        let now = self.provider.now();
        let mut cursor = SweepCursor::from_start(None);
        let mut asks = Vec::new();
        let mut left = want;
        loop {
            let page = self
                .provider
                .sweep_destination_heads(cursor, std::num::NonZeroU32::MIN.saturating_add(255))
                .await?;
            for head in page.heads.into_iter().filter(|head| head.head_due <= now) {
                let Some(take) = std::num::NonZeroU32::new(u32::try_from(left).unwrap_or(0)) else {
                    return Ok(asks);
                };
                left = left.saturating_sub(take.get() as usize);
                asks.push(DestinationAsk {
                    destination: head.destination,
                    take,
                    mode: AskMode::Due,
                });
            }
            let Some(next) = page.next else {
                return Ok(asks);
            };
            cursor = next;
        }
    }

    /// Claim the asked records for this node's worker.
    pub(crate) async fn claim_asks(
        &self,
        asks: Vec<DestinationAsk>,
    ) -> PubsubResult<Vec<DestinationClaim>> {
        let now = self.provider.now();
        self.provider
            .claim_deliveries(DeliveryClaimRequest {
                terms: ClaimTerms {
                    owner: self.delivery_config.worker_id.clone(),
                    now,
                    lease_until: self.delivery_config.timing.lease_deadline(now),
                },
                asks,
            })
            .await
    }

    pub async fn execute_query_action(&self, action: PubsubAction) -> PubsubResult<PubsubSuccess> {
        match action {
            PubsubAction::CreateTopic(request) => {
                let topic = self.create_topic_with(request).await?;
                Ok(PubsubSuccess::CreateTopic {
                    topic_arn: topic.topic_arn,
                })
            }
            PubsubAction::TagResource { resource_arn, tags } => {
                self.provider
                    .change_topic_tags(&resource_arn, tags, Vec::new())
                    .await?;
                Ok(PubsubSuccess::TagResource)
            }
            PubsubAction::UntagResource {
                resource_arn,
                tag_keys,
            } => {
                self.provider
                    .change_topic_tags(&resource_arn, std::collections::BTreeMap::new(), tag_keys)
                    .await?;
                Ok(PubsubSuccess::UntagResource)
            }
            PubsubAction::ListTagsForResource { resource_arn } => {
                let topic = self
                    .provider
                    .get_topic(&resource_arn)
                    .await?
                    .ok_or_else(|| {
                        PubsubError::tagged_resource_not_found(resource_arn.to_string())
                    })?;
                Ok(PubsubSuccess::ListTagsForResource { tags: topic.tags })
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
                    next_token: response.next_token,
                })
            }
            PubsubAction::Subscribe {
                request,
                return_subscription_arn,
            } => {
                let subscription = self.subscribe_subscription(request).await?;
                let subscription_arn = if return_subscription_arn {
                    subscription.subscription_arn.to_string()
                } else {
                    SubscribeResponse::from(&subscription).subscription_arn
                };
                Ok(PubsubSuccess::Subscribe { subscription_arn })
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
                let by_topic = request.topic_arn.is_some();
                let response = self.list_subscriptions(request).await?;
                Ok(PubsubSuccess::ListSubscriptions {
                    by_topic,
                    subscriptions: response
                        .subscriptions
                        .into_iter()
                        .map(SubscriptionView::from)
                        .collect(),
                    next_token: response.next_token,
                })
            }
            PubsubAction::Publish(request) => {
                let response = self.publish(request).await?;
                Ok(PubsubSuccess::Publish {
                    message_id: response.message_id,
                })
            }
            PubsubAction::PublishBatch { topic_arn, entries } => {
                let mut successful = Vec::new();
                let mut failed = Vec::new();
                for result in self.publish_batch(topic_arn, entries).await? {
                    match result {
                        PublishBatchResult::Successful { id, message_id } => {
                            successful.push((id, message_id));
                        }
                        PublishBatchResult::Failed { id, error } => {
                            failed.push(PublishBatchFailure {
                                id,
                                code: error.aws_query_error_type(),
                                message: error.aws_query_message(),
                                sender_fault: error.aws_query_status_code() < 500,
                            });
                        }
                    }
                }
                Ok(PubsubSuccess::PublishBatch { successful, failed })
            }
        }
    }
}
