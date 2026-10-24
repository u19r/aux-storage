use async_trait::async_trait;

use crate::{
    ConfirmSubscriptionRequest, ConfirmSubscriptionResponse, ListSubscriptionsRequest,
    ListSubscriptionsResponse, PubsubResult, SetSubscriptionAttributesRequest, Subscription,
    SubscriptionAcceptanceInput, SubscriptionArn,
};

#[async_trait]
pub trait PubsubSubscriptionRepository: Send + Sync {
    async fn create_subscription(
        &self,
        input: SubscriptionAcceptanceInput,
    ) -> PubsubResult<Subscription>;

    async fn confirm_subscription(
        &self,
        request: ConfirmSubscriptionRequest,
    ) -> PubsubResult<ConfirmSubscriptionResponse>;

    async fn delete_subscription(&self, subscription_arn: &SubscriptionArn) -> PubsubResult<()>;

    async fn get_subscription(
        &self,
        subscription_arn: &SubscriptionArn,
    ) -> PubsubResult<Option<Subscription>>;

    async fn set_subscription_attributes(
        &self,
        request: SetSubscriptionAttributesRequest,
    ) -> PubsubResult<Subscription>;

    async fn list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> PubsubResult<ListSubscriptionsResponse>;
}
