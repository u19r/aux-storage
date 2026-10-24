use async_trait::async_trait;

use crate::{
    GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse, GetTopicAttributesRequest,
    GetTopicAttributesResponse, ListSubscriptionsRequest, PubsubError, PubsubResult,
    PubsubSubscriptionRepository, PubsubTopicRepository,
};

#[async_trait]
pub trait PubsubAttributeRepository:
    PubsubTopicRepository + PubsubSubscriptionRepository + Send + Sync
{
    async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse> {
        let Some(topic) = self.get_topic(&request.topic_arn).await? else {
            return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
        };
        let subscriptions = self
            .list_subscriptions(ListSubscriptionsRequest {
                topic_arn: Some(topic.topic_arn.clone()),
                next_token: None,
            })
            .await?
            .subscriptions;
        let pending = subscriptions
            .iter()
            .filter(|subscription| subscription.confirmation.pending_confirmation())
            .count();
        let confirmed = subscriptions.len().saturating_sub(pending);
        Ok(GetTopicAttributesResponse {
            attributes: topic.attributes(confirmed, pending),
        })
    }

    async fn get_subscription_attributes(
        &self,
        request: GetSubscriptionAttributesRequest,
    ) -> PubsubResult<GetSubscriptionAttributesResponse> {
        let Some(subscription) = self.get_subscription(&request.subscription_arn).await? else {
            return Err(PubsubError::subscription_not_found(
                request.subscription_arn.to_string(),
            ));
        };
        Ok(GetSubscriptionAttributesResponse {
            attributes: subscription.attributes(),
        })
    }
}
