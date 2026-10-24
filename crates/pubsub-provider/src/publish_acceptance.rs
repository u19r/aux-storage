use async_trait::async_trait;

use crate::{
    ListSubscriptionsRequest, PublishAcceptanceInput, PublishRequest, PubsubError, PubsubMessageId,
    PubsubPublicationRepository, PubsubResult, PubsubSubscriptionRepository, PubsubTopicRepository,
    SubscriptionSnapshot,
};

const SUBSCRIPTION_SNAPSHOT_CHUNK_SIZE: usize = 10;

#[async_trait]
pub trait PubsubPublishAcceptanceRepository:
    PubsubPublicationRepository + PubsubSubscriptionRepository + PubsubTopicRepository + Send + Sync
{
    fn publish_acceptance_started(&self, _message_id: &PubsubMessageId) {}

    fn publish_acceptance_finished(&self, _message_id: &PubsubMessageId, _accepted: bool) {}

    async fn persist_publish_acceptance(
        &self,
        request: PublishRequest,
        message_id: PubsubMessageId,
        custom_sender_enabled: bool,
    ) -> PubsubResult<()> {
        self.publish_acceptance_started(&message_id);
        let result = self
            .persist_publish_generation(request, message_id.clone(), custom_sender_enabled)
            .await;
        self.publish_acceptance_finished(&message_id, result.is_ok());
        result
    }

    async fn persist_publish_generation(
        &self,
        request: PublishRequest,
        message_id: PubsubMessageId,
        custom_sender_enabled: bool,
    ) -> PubsubResult<()> {
        if PubsubTopicRepository::get_topic(self, &request.topic_arn)
            .await?
            .is_none()
        {
            return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
        }
        let topic_arn = request.topic_arn.clone();
        let subscriptions = PubsubSubscriptionRepository::list_subscriptions(
            self,
            ListSubscriptionsRequest {
                topic_arn: Some(topic_arn.clone()),
                next_token: None,
            },
        )
        .await?
        .subscriptions;
        let acceptance =
            PublishAcceptanceInput::capture(request, message_id, custom_sender_enabled);
        let snapshot = SubscriptionSnapshot::with_generation(
            topic_arn,
            subscriptions,
            SUBSCRIPTION_SNAPSHOT_CHUNK_SIZE,
            acceptance.snapshot_generation,
        )?;
        self.persist_publish(snapshot, acceptance).await
    }
}
