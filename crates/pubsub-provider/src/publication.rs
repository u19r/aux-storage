use crate::{
    ClaimedPublishChunk, DeliveryPayload, DeliveryRecordId, DeliveryRecordKind,
    DeliveryRetryPolicy, DeliveryTarget, DestinationKey, NewDelivery, PublishAcceptanceInput,
    PublishChunkCommit, PublishIntent, PubsubError, PubsubResult, Subscription,
    SubscriptionProtocol, SubscriptionSnapshot, SubscriptionSnapshotGeneration,
    SubscriptionSnapshotRoot, TopicArn,
};

impl SubscriptionSnapshot {
    pub fn with_generation(
        topic_arn: TopicArn,
        mut subscriptions: Vec<Subscription>,
        chunk_size: usize,
        generation: SubscriptionSnapshotGeneration,
    ) -> PubsubResult<Self> {
        if chunk_size == 0 {
            return Err(PubsubError::storage(
                "subscription snapshot chunk size must be greater than zero",
            ));
        }

        subscriptions.sort_by(|left, right| {
            left.subscription_arn
                .as_str()
                .cmp(right.subscription_arn.as_str())
        });
        let chunk_count = subscriptions.len().div_ceil(chunk_size);
        let root = SubscriptionSnapshotRoot {
            topic_arn,
            generation,
            chunk_count: u32::try_from(chunk_count)
                .map_err(|error| PubsubError::storage(error.to_string()))?,
        };
        let chunks = subscriptions
            .chunks(chunk_size)
            .map(<[Subscription]>::to_vec)
            .collect();
        Ok(Self { root, chunks })
    }
}

impl PublishIntent {
    #[must_use]
    pub const fn new(
        acceptance: PublishAcceptanceInput,
        snapshot: SubscriptionSnapshotRoot,
    ) -> Self {
        Self {
            acceptance,
            snapshot,
            next_chunk: 0,
            lease_expires_at: None,
        }
    }

    /// The deliveries of one chunk: confirmed subscriptions whose filter
    /// policy accepts the message (design 3.5). Also returns how many the
    /// filter excluded.
    #[must_use]
    pub fn deliveries(&self, subscriptions: Vec<Subscription>) -> (Vec<NewDelivery>, u32) {
        let mut filtered_out = 0_u32;
        let deliveries = subscriptions
            .into_iter()
            .filter(|subscription| !subscription.confirmation.pending_confirmation())
            .filter(|subscription| {
                let accepted = subscription.accepts(&self.acceptance.request);
                filtered_out += u32::from(!accepted);
                accepted
            })
            .map(|subscription| self.delivery_for(subscription))
            .collect();
        (deliveries, filtered_out)
    }

    fn delivery_for(&self, subscription: Subscription) -> NewDelivery {
        let accepted_at = self.acceptance.accepted_at;
        let is_http = matches!(
            subscription.protocol,
            SubscriptionProtocol::Http | SubscriptionProtocol::Https
        );
        let target = if self.acceptance.custom_sender_enabled && is_http {
            DeliveryTarget::CustomSender
        } else {
            DeliveryTarget::BuiltIn
        };
        let request = &self.acceptance.request;
        let message_id = &self.acceptance.message_id;
        let payload = DeliveryPayload {
            id: DeliveryRecordId(format!("{}:{message_id}", subscription.subscription_arn)),
            kind: DeliveryRecordKind::Notification,
            message_id: message_id.clone(),
            destination: DestinationKey::of_endpoint(subscription.protocol, &subscription.endpoint),
            message_body: Some(request.message.clone()),
            subject: request.subject.clone(),
            message_attributes: request.message_attributes.clone(),
            target,
            created_at: accepted_at,
            subscription: None,
        };
        let deadline =
            DeliveryRetryPolicy::for_protocol(subscription.protocol).deadline(accepted_at);
        let state = payload.first_state(subscription.subscription_arn.clone(), deadline);
        (
            DeliveryPayload {
                subscription: Some(subscription),
                ..payload
            },
            state,
        )
    }
}

impl TryFrom<ClaimedPublishChunk> for PublishChunkCommit {
    type Error = storage_types::StorageError;

    fn try_from(chunk: ClaimedPublishChunk) -> Result<Self, Self::Error> {
        let (records, filtered_out) = chunk.intent.deliveries(chunk.subscriptions);
        let next_intent = chunk.intent.next_chunk()?;
        Ok(PublishChunkCommit {
            claimed_intent: chunk.intent,
            records,
            filtered_out,
            next_intent,
        })
    }
}
