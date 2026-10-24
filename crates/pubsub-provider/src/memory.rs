use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use async_trait::async_trait;
use storage_types::TimestampMillis;

use crate::{
    AcceptedPublication, ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse,
    ClaimedPublishChunk, ConfirmSubscriptionRequest, ConfirmSubscriptionResponse, DeliveryRecord,
    DeliveryRecordId, DeliveryRecordUpdate, GetTopicAttributesRequest, GetTopicAttributesResponse,
    ListSubscriptionsRequest, ListSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse,
    PublishChunkClaimRequest, PublishChunkCommit, PublishIntent, PubsubAttributeRepository,
    PubsubDeliveryRepository, PubsubError, PubsubInternalKind, PubsubPublicationRepository,
    PubsubPublishAcceptanceRepository, PubsubResult, PubsubRuntimeRepository,
    PubsubSubscriptionRepository, PubsubTopicRepository, PubsubValidationKind,
    SetSubscriptionAttributesRequest, SetTopicAttributesRequest, Subscription,
    SubscriptionAcceptanceInput, SubscriptionArn, SubscriptionConfirmation, Topic, TopicArn,
};

#[derive(Default)]
pub struct InMemoryPubsubProvider {
    state: Mutex<InMemoryPubsubState>,
}

#[derive(Default)]
struct InMemoryPubsubState {
    topics: HashMap<TopicArn, Topic>,
    subscriptions: HashMap<SubscriptionArn, Subscription>,
    deliveries: HashMap<String, DeliveryRecord>,
    snapshot_chunks: HashMap<(String, u32), Vec<Subscription>>,
    publish_intents: HashMap<String, PublishIntent>,
}

#[async_trait]
impl PubsubPublicationRepository for InMemoryPubsubProvider {
    async fn persist_accepted_publication(
        &self,
        publication: AcceptedPublication,
    ) -> PubsubResult<()> {
        let mut state = self.state()?;
        let AcceptedPublication { snapshot, intent } = publication;
        let generation = snapshot.root.generation.to_string();
        if let Some(stored) = state
            .publish_intents
            .get(&intent.acceptance.message_id.to_string())
        {
            return if stored == &intent {
                Ok(())
            } else {
                Err(PubsubError::storage(
                    "publish message id already belongs to another intent",
                ))
            };
        }
        for (index, subscriptions) in snapshot.chunks.iter().enumerate() {
            let index =
                u32::try_from(index).map_err(|error| PubsubError::storage(error.to_string()))?;
            if state
                .snapshot_chunks
                .get(&(generation.clone(), index))
                .is_some_and(|stored| stored != subscriptions)
            {
                return Err(PubsubError::storage(
                    "publish snapshot generation already contains different subscriptions",
                ));
            }
        }
        for (index, subscriptions) in snapshot.chunks.into_iter().enumerate() {
            let index =
                u32::try_from(index).map_err(|error| PubsubError::storage(error.to_string()))?;
            state
                .snapshot_chunks
                .insert((generation.clone(), index), subscriptions);
        }
        state
            .publish_intents
            .insert(intent.acceptance.message_id.to_string(), intent);
        Ok(())
    }

    async fn claim_publish_chunks(
        &self,
        request: PublishChunkClaimRequest,
    ) -> PubsubResult<Vec<ClaimedPublishChunk>> {
        let mut state = self.state()?;
        let mut keys = state.publish_intents.keys().cloned().collect::<Vec<_>>();
        keys.sort_unstable();
        let limit = usize::from(request.limit);
        let mut claims = Vec::with_capacity(limit.min(keys.len()));
        for key in keys.into_iter().take(limit) {
            let Some(mut intent) = state.publish_intents.remove(&key) else {
                continue;
            };
            if intent
                .lease_expires_at
                .is_some_and(|lease_expires_at| lease_expires_at > request.now)
            {
                state.publish_intents.insert(key, intent);
                continue;
            }
            intent.lease_expires_at = Some(TimestampMillis::from_timestamp(
                request
                    .now
                    .timestamp_millis()
                    .saturating_add(crate::PUBLISH_INTENT_LEASE_MILLIS),
            ));
            let chunk_key = (intent.snapshot.generation.to_string(), intent.next_chunk);
            let subscriptions = if intent.snapshot.chunk_count == 0 {
                Vec::new()
            } else {
                state
                    .snapshot_chunks
                    .get(&chunk_key)
                    .cloned()
                    .ok_or_else(|| {
                        PubsubError::storage("in-memory publish snapshot chunk is missing")
                    })?
            };
            state.publish_intents.insert(key, intent.clone());
            claims.push(ClaimedPublishChunk {
                intent,
                subscriptions,
            });
        }
        Ok(claims)
    }

    async fn commit_publish_chunk(&self, commit: PublishChunkCommit) -> PubsubResult<()> {
        let mut state = self.state()?;
        let intent_key = commit.claimed_intent.acceptance.message_id.to_string();
        let Some(current) = state.publish_intents.get(&intent_key) else {
            return Ok(());
        };
        if current.lease_expires_at != commit.claimed_intent.lease_expires_at
            || current.next_chunk != commit.claimed_intent.next_chunk
        {
            return Err(PubsubError::storage(
                "in-memory publish intent claim was lost",
            ));
        }
        for record in commit.records {
            state
                .deliveries
                .entry(record.id.0.clone())
                .or_insert(record);
        }
        match commit.next_intent {
            Some(intent) => {
                state.publish_intents.insert(intent_key, intent);
            }
            None => {
                state.publish_intents.remove(&intent_key);
                let generation = commit.claimed_intent.snapshot.generation.to_string();
                state
                    .snapshot_chunks
                    .retain(|(stored_generation, _), _| stored_generation != &generation);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl PubsubRuntimeRepository for InMemoryPubsubProvider {
    async fn initialize_pubsub(&self) -> PubsubResult<()> {
        Ok(())
    }
}

#[async_trait]
impl PubsubPublishAcceptanceRepository for InMemoryPubsubProvider {}

#[async_trait]
impl PubsubAttributeRepository for InMemoryPubsubProvider {
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
}

#[async_trait]
impl PubsubTopicRepository for InMemoryPubsubProvider {
    async fn create_topic(&self, topic: Topic) -> PubsubResult<Topic> {
        let mut state = self.state()?;
        if let Some(topic) = state
            .topics
            .values()
            .find(|stored| stored.name == topic.name)
            .cloned()
        {
            return Ok(topic);
        }
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
}

#[async_trait]
impl PubsubSubscriptionRepository for InMemoryPubsubProvider {
    async fn create_subscription(
        &self,
        input: SubscriptionAcceptanceInput,
    ) -> PubsubResult<Subscription> {
        let (proposed, message_id) = input.into_parts();
        let mut state = self.state()?;
        if !state.topics.contains_key(&proposed.topic_arn) {
            return Err(PubsubError::topic_not_found(proposed.topic_arn.to_string()));
        }
        let accepted_at = proposed.created_at;
        let subscription = state
            .subscriptions
            .values()
            .find(|stored| {
                stored.topic_arn == proposed.topic_arn
                    && stored.protocol == proposed.protocol
                    && stored.endpoint == proposed.endpoint
            })
            .cloned()
            .unwrap_or(proposed);
        let confirmation = DeliveryRecord::pending_confirmation_at(
            &subscription,
            message_id.as_ref(),
            accepted_at,
        )?;
        state
            .subscriptions
            .entry(subscription.subscription_arn.clone())
            .or_insert_with(|| subscription.clone());
        if let Some(record) = confirmation {
            state
                .deliveries
                .entry(record.id.0.clone())
                .or_insert(record);
        }
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
}

#[async_trait]
impl PubsubDeliveryRepository for InMemoryPubsubProvider {
    async fn put_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        record.validate_storage_range()?;
        self.state()?
            .deliveries
            .entry(record.id.0.clone())
            .or_insert(record);
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
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.id.0.cmp(&right.id.0));
        records.truncate(usize::from(request.limit));
        let generations = records
            .iter()
            .map(|record| {
                record
                    .claim_generation
                    .checked_add(1)
                    .ok_or_else(|| PubsubError::storage("delivery claim generation overflow"))
            })
            .collect::<PubsubResult<Vec<_>>>()?;
        let records = records
            .into_iter()
            .zip(generations)
            .map(|(record, claim_generation)| {
                record.claim_generation = claim_generation;
                record.lease_owner = Some(request.owner.clone());
                record.lease_expires_at = Some(request.lease_expires_at);
                record.updated_at = request.now;
                Ok(record.clone())
            })
            .collect::<PubsubResult<Vec<_>>>()?;
        Ok(ClaimDeliveryRecordsResponse { records })
    }

    async fn update_delivery_record(&self, update: DeliveryRecordUpdate) -> PubsubResult<()> {
        let mut state = self.state()?;
        let Some(record) = state.deliveries.get_mut(&update.record_id.0) else {
            return Err(PubsubError::delivery_claim_lost(update.record_id.0));
        };
        if update.expected_claim_generation == 0
            || record.claim_generation != update.expected_claim_generation
            || record.lease_owner.as_deref() != Some(update.expected_lease_owner.as_str())
        {
            return Err(PubsubError::delivery_claim_lost(update.record_id.0));
        }
        record.status = update.status;
        record.attempts = update.attempts;
        record.next_attempt_at = update.next_attempt_at;
        record.lease_owner = None;
        record.lease_expires_at = None;
        record.last_error = update.last_error;
        record.updated_at = update.updated_at;
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
