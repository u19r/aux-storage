use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use async_trait::async_trait;
use storage_types::TimestampMillis;

use crate::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse, GetTopicAttributesRequest,
    GetTopicAttributesResponse, ListSubscriptionsRequest, ListSubscriptionsResponse,
    ListTopicsRequest, ListTopicsResponse, PubsubError, PubsubInternalKind, PubsubProvider,
    PubsubResult, PubsubValidationKind, SetSubscriptionAttributesRequest,
    SetTopicAttributesRequest, SubscribeRequest, Subscription, SubscriptionArn,
    SubscriptionConfirmation, Topic, TopicArn,
};

#[derive(Debug, Default)]
pub struct InMemoryPubsubProvider {
    state: Mutex<InMemoryPubsubState>,
}

#[derive(Debug, Default)]
struct InMemoryPubsubState {
    topics: HashMap<TopicArn, Topic>,
    subscriptions: HashMap<SubscriptionArn, Subscription>,
    deliveries: HashMap<String, DeliveryRecord>,
}

#[async_trait]
impl PubsubProvider for InMemoryPubsubProvider {
    async fn initialize(&self) -> PubsubResult<()> {
        Ok(())
    }

    async fn create_topic(&self, request: CreateTopicRequest) -> PubsubResult<Topic> {
        let mut state = self.state()?;
        if let Some(topic) = state
            .topics
            .values()
            .find(|topic| topic.name == request.name)
            .cloned()
        {
            return Ok(topic);
        }
        let topic = Topic {
            topic_arn: TopicArn::compose("aws", "us-east-1", "000000000000", &request.name),
            name: request.name,
            display_name: request.attributes.get("DisplayName").cloned(),
            created_at: TimestampMillis::now(),
        };
        state.topics.insert(topic.topic_arn.clone(), topic.clone());
        Ok(topic)
    }

    async fn delete_topic(&self, topic_arn: &TopicArn) -> PubsubResult<()> {
        let mut state = self.state()?;
        state.topics.remove(topic_arn);
        state
            .subscriptions
            .retain(|_, subscription| &subscription.topic_arn != topic_arn);
        Ok(())
    }

    async fn get_topic(&self, topic_arn: &TopicArn) -> PubsubResult<Option<Topic>> {
        Ok(self.state()?.topics.get(topic_arn).cloned())
    }

    async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse> {
        let state = self.state()?;
        let Some(topic) = state.topics.get(&request.topic_arn) else {
            return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
        };
        let subscriptions = state
            .subscriptions
            .values()
            .filter(|subscription| subscription.topic_arn == request.topic_arn);
        let (confirmed, pending) =
            subscriptions.fold((0usize, 0usize), |(confirmed, pending), subscription| {
                if subscription.confirmation.pending_confirmation() {
                    (confirmed, pending + 1)
                } else {
                    (confirmed + 1, pending)
                }
            });
        Ok(GetTopicAttributesResponse {
            attributes: topic.attributes(confirmed, pending),
        })
    }

    async fn set_topic_attributes(
        &self,
        request: SetTopicAttributesRequest,
    ) -> PubsubResult<Topic> {
        let mut state = self.state()?;
        let Some(topic) = state.topics.get_mut(&request.topic_arn) else {
            return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
        };
        if let Some(value) = request.attributes.get("DisplayName") {
            topic.display_name = Some(value.clone());
        }
        Ok(topic.clone())
    }

    async fn list_topics(&self, _request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse> {
        let mut topics = self.state()?.topics.values().cloned().collect::<Vec<_>>();
        topics.sort_by(|left, right| left.topic_arn.as_str().cmp(right.topic_arn.as_str()));
        Ok(ListTopicsResponse {
            topics,
            next_token: None,
        })
    }

    async fn create_subscription(&self, request: SubscribeRequest) -> PubsubResult<Subscription> {
        let mut state = self.state()?;
        if !state.topics.contains_key(&request.topic_arn) {
            return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
        }
        if let Some(subscription) = state
            .subscriptions
            .values()
            .find(|subscription| {
                subscription.topic_arn == request.topic_arn
                    && subscription.protocol == request.protocol
                    && subscription.endpoint == request.endpoint
            })
            .cloned()
        {
            return Ok(subscription);
        }
        let raw_message_delivery = request
            .attributes
            .get("RawMessageDelivery")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let subscription = Subscription {
            subscription_arn: SubscriptionArn::compose(&request.topic_arn),
            topic_arn: request.topic_arn,
            protocol: request.protocol,
            endpoint: request.endpoint,
            raw_message_delivery,
            confirmation: request.protocol.subscription_confirmation(),
            extra_json: request.extra_json,
            created_at: TimestampMillis::now(),
        };
        state
            .subscriptions
            .insert(subscription.subscription_arn.clone(), subscription.clone());
        Ok(subscription)
    }

    async fn confirm_subscription(
        &self,
        request: ConfirmSubscriptionRequest,
    ) -> PubsubResult<ConfirmSubscriptionResponse> {
        let mut state = self.state()?;
        let Some(subscription) = state.subscriptions.values_mut().find(|subscription| {
            subscription.topic_arn == request.topic_arn
                && subscription.confirmation.token() == Some(request.token.as_str())
        }) else {
            return Err(PubsubError::validation(PubsubValidationKind::InvalidToken));
        };
        subscription.confirmation = SubscriptionConfirmation::Confirmed;
        Ok(ConfirmSubscriptionResponse {
            subscription_arn: subscription.subscription_arn.clone(),
        })
    }

    async fn delete_subscription(&self, subscription_arn: &SubscriptionArn) -> PubsubResult<()> {
        self.state()?.subscriptions.remove(subscription_arn);
        Ok(())
    }

    async fn get_subscription(
        &self,
        subscription_arn: &SubscriptionArn,
    ) -> PubsubResult<Option<Subscription>> {
        Ok(self.state()?.subscriptions.get(subscription_arn).cloned())
    }

    async fn set_subscription_attributes(
        &self,
        request: SetSubscriptionAttributesRequest,
    ) -> PubsubResult<Subscription> {
        let mut state = self.state()?;
        let Some(subscription) = state.subscriptions.get_mut(&request.subscription_arn) else {
            return Err(PubsubError::subscription_not_found(
                request.subscription_arn.to_string(),
            ));
        };
        if let Some(value) = request.attributes.get("RawMessageDelivery") {
            subscription.raw_message_delivery = value.eq_ignore_ascii_case("true");
        }
        Ok(subscription.clone())
    }

    async fn get_subscription_attributes(
        &self,
        request: GetSubscriptionAttributesRequest,
    ) -> PubsubResult<GetSubscriptionAttributesResponse> {
        let state = self.state()?;
        let Some(subscription) = state.subscriptions.get(&request.subscription_arn) else {
            return Err(PubsubError::subscription_not_found(
                request.subscription_arn.to_string(),
            ));
        };
        Ok(GetSubscriptionAttributesResponse {
            attributes: subscription.attributes(),
        })
    }

    async fn list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> PubsubResult<ListSubscriptionsResponse> {
        let mut subscriptions = self
            .state()?
            .subscriptions
            .values()
            .filter(|subscription| {
                request
                    .topic_arn
                    .as_ref()
                    .is_none_or(|topic_arn| &subscription.topic_arn == topic_arn)
            })
            .cloned()
            .collect::<Vec<_>>();
        subscriptions.sort_by(|left, right| {
            left.subscription_arn
                .as_str()
                .cmp(right.subscription_arn.as_str())
        });
        Ok(ListSubscriptionsResponse {
            subscriptions,
            next_token: None,
        })
    }

    async fn put_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        self.state()?.deliveries.insert(record.id.0.clone(), record);
        Ok(())
    }

    async fn claim_delivery_records(
        &self,
        request: ClaimDeliveryRecordsRequest,
    ) -> PubsubResult<ClaimDeliveryRecordsResponse> {
        let mut state = self.state()?;
        let mut records = state
            .deliveries
            .values_mut()
            .filter(|record| record.is_claimable(request.now))
            .take(request.limit)
            .map(|record| {
                record.lease_owner = Some(request.owner.clone());
                record.lease_expires_at = Some(request.lease_expires_at);
                record.updated_at = request.now;
                record.clone()
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.id.0.cmp(&right.id.0));
        Ok(ClaimDeliveryRecordsResponse { records })
    }

    async fn update_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        self.state()?.deliveries.insert(record.id.0.clone(), record);
        Ok(())
    }

    async fn get_delivery_record(
        &self,
        record_id: &DeliveryRecordId,
    ) -> PubsubResult<Option<DeliveryRecord>> {
        Ok(self.state()?.deliveries.get(&record_id.0).cloned())
    }
}

impl InMemoryPubsubProvider {
    fn state(&self) -> PubsubResult<MutexGuard<'_, InMemoryPubsubState>> {
        self.state
            .lock()
            .map_err(|_| PubsubError::internal(PubsubInternalKind::LockPoisoned))
    }
}
