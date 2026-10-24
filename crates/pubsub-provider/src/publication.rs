use crate::{
    ClaimedPublishChunk, DeliveryRecord, PendingNotificationContext, PublishAcceptanceInput,
    PublishChunkCommit, PublishIntent, PubsubError, PubsubResult, Subscription,
    SubscriptionSnapshot, SubscriptionSnapshotGeneration, SubscriptionSnapshotRoot, TopicArn,
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

    #[must_use]
    pub fn delivery_records(&self, subscriptions: Vec<Subscription>) -> Vec<DeliveryRecord> {
        let context = PendingNotificationContext::new(
            self.acceptance.accepted_at,
            self.acceptance.custom_sender_enabled,
        );
        subscriptions
            .into_iter()
            .filter(|subscription| !subscription.confirmation.pending_confirmation())
            .map(|subscription| {
                DeliveryRecord::pending_notification_at(
                    &self.acceptance.request,
                    &self.acceptance.message_id,
                    &subscription,
                    context,
                )
            })
            .collect()
    }
}

impl TryFrom<ClaimedPublishChunk> for PublishChunkCommit {
    type Error = storage_types::StorageError;

    fn try_from(chunk: ClaimedPublishChunk) -> Result<Self, Self::Error> {
        let records = chunk.intent.delivery_records(chunk.subscriptions);
        let next_intent = chunk.intent.next_chunk()?;
        Ok(PublishChunkCommit {
            claimed_intent: chunk.intent,
            records,
            next_intent,
        })
    }
}
