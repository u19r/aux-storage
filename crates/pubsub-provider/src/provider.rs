use async_trait::async_trait;

use crate::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse, GetTopicAttributesRequest,
    GetTopicAttributesResponse, ListSubscriptionsRequest, ListSubscriptionsResponse,
    ListTopicsRequest, ListTopicsResponse, PublishRequest, PubsubError, PubsubMessageId,
    PubsubResult, SetSubscriptionAttributesRequest,
    SetTopicAttributesRequest, SubscribeRequest, Subscription, SubscriptionArn, Topic, TopicArn,
};

#[async_trait]
pub trait PubsubProvider: Send + Sync {
    async fn initialize(&self) -> PubsubResult<()>;

    async fn create_topic(&self, request: CreateTopicRequest) -> PubsubResult<Topic>;

    async fn delete_topic(&self, topic_arn: &TopicArn) -> PubsubResult<()>;

    async fn get_topic(&self, topic_arn: &TopicArn) -> PubsubResult<Option<Topic>>;

    async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse>;

    async fn set_topic_attributes(&self, request: SetTopicAttributesRequest)
    -> PubsubResult<Topic>;

    async fn list_topics(&self, request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse>;

    async fn create_subscription(&self, request: SubscribeRequest) -> PubsubResult<Subscription>;

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

    async fn get_subscription_attributes(
        &self,
        request: GetSubscriptionAttributesRequest,
    ) -> PubsubResult<GetSubscriptionAttributesResponse>;

    async fn list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> PubsubResult<ListSubscriptionsResponse>;

    async fn accept_publish(
        &self,
        request: PublishRequest,
        message_id: PubsubMessageId,
        custom_sender_enabled: bool,
    ) -> PubsubResult<()> {
        if self.get_topic(&request.topic_arn).await?.is_none() {
            return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
        }
        let records = self
            .list_subscriptions(ListSubscriptionsRequest {
                topic_arn: Some(request.topic_arn.clone()),
                next_token: None,
            })
            .await?
            .subscriptions
            .into_iter()
            .filter(|subscription| !subscription.confirmation.pending_confirmation())
            .map(|subscription| {
                DeliveryRecord::pending_notification(
                    &request,
                    &message_id,
                    &subscription,
                    custom_sender_enabled,
                )
            })
            .collect();
        self.put_delivery_records(records).await
    }

    async fn materialize_publish_intents(&self, _limit: usize) -> PubsubResult<usize> {
        Ok(0)
    }

    async fn put_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()>;

    async fn put_delivery_records(&self, records: Vec<DeliveryRecord>) -> PubsubResult<()> {
        for record in records {
            self.put_delivery_record(record).await?;
        }
        Ok(())
    }

    async fn claim_delivery_records(
        &self,
        request: ClaimDeliveryRecordsRequest,
    ) -> PubsubResult<ClaimDeliveryRecordsResponse>;

    async fn update_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()>;

    async fn get_delivery_record(
        &self,
        record_id: &DeliveryRecordId,
    ) -> PubsubResult<Option<DeliveryRecord>>;
}
