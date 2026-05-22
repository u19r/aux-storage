use async_trait::async_trait;

use crate::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse, GetTopicAttributesRequest,
    GetTopicAttributesResponse, ListSubscriptionsRequest, ListSubscriptionsResponse,
    ListTopicsRequest, ListTopicsResponse, PubsubResult, SetSubscriptionAttributesRequest,
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
