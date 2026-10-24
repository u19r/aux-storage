use async_trait::async_trait;
use storage_types::TimestampMillis;

use crate::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordUpdate, GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse,
    GetTopicAttributesRequest, GetTopicAttributesResponse, ListSubscriptionsRequest,
    ListSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse, PublishChunkClaimLimit,
    PublishChunkClaimRequest, PublishChunkCommit, PublishRequest, PubsubAttributeRepository,
    PubsubDeliveryRepository, PubsubMessageId, PubsubPublicationRepository,
    PubsubPublishAcceptanceRepository, PubsubResult, PubsubRuntimeRepository,
    PubsubSubscriptionRepository, PubsubTopicRepository, SetSubscriptionAttributesRequest,
    SetTopicAttributesRequest, SubscribeRequest, Subscription, SubscriptionAcceptanceInput,
    SubscriptionArn, Topic, TopicArn,
};

#[async_trait]
pub trait PubsubProvider: Send + Sync {
    fn take_admission_pressure_signal(&self) -> bool;

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
    ) -> PubsubResult<()>;

    async fn materialize_publish_intents(&self, limit: usize) -> PubsubResult<usize>;

    async fn materialize_publish_chunks(
        &self,
        request: PublishChunkClaimRequest,
    ) -> PubsubResult<usize>;

    async fn put_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()>;

    async fn put_delivery_records(&self, records: Vec<DeliveryRecord>) -> PubsubResult<()>;

    async fn claim_delivery_records(
        &self,
        request: ClaimDeliveryRecordsRequest,
    ) -> PubsubResult<ClaimDeliveryRecordsResponse>;

    async fn update_delivery_record(&self, update: DeliveryRecordUpdate) -> PubsubResult<()>;

    async fn get_delivery_record(
        &self,
        record_id: &DeliveryRecordId,
    ) -> PubsubResult<Option<DeliveryRecord>>;
}

#[async_trait]
impl<T> PubsubProvider for T
where T: PubsubAttributeRepository
        + PubsubDeliveryRepository
        + PubsubPublicationRepository
        + PubsubPublishAcceptanceRepository
        + PubsubRuntimeRepository
        + PubsubSubscriptionRepository
        + PubsubTopicRepository
        + Send
        + Sync
{
    fn take_admission_pressure_signal(&self) -> bool {
        PubsubRuntimeRepository::take_admission_pressure_signal(self)
    }

    async fn initialize(&self) -> PubsubResult<()> {
        PubsubRuntimeRepository::initialize_pubsub(self).await
    }

    async fn create_topic(&self, request: CreateTopicRequest) -> PubsubResult<Topic> {
        PubsubTopicRepository::create_topic(self, Topic::from(request)).await
    }

    async fn delete_topic(&self, topic_arn: &TopicArn) -> PubsubResult<()> {
        PubsubTopicRepository::delete_topic(self, topic_arn).await
    }

    async fn get_topic(&self, topic_arn: &TopicArn) -> PubsubResult<Option<Topic>> {
        PubsubTopicRepository::get_topic(self, topic_arn).await
    }

    async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse> {
        PubsubAttributeRepository::get_topic_attributes(self, request).await
    }

    async fn set_topic_attributes(
        &self,
        request: SetTopicAttributesRequest,
    ) -> PubsubResult<Topic> {
        PubsubTopicRepository::set_topic_attributes(self, request).await
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse> {
        PubsubTopicRepository::list_topics(self, request).await
    }

    async fn create_subscription(&self, request: SubscribeRequest) -> PubsubResult<Subscription> {
        let input = SubscriptionAcceptanceInput::capture(Subscription::from(request))?;
        PubsubSubscriptionRepository::create_subscription(self, input).await
    }

    async fn confirm_subscription(
        &self,
        request: ConfirmSubscriptionRequest,
    ) -> PubsubResult<ConfirmSubscriptionResponse> {
        PubsubSubscriptionRepository::confirm_subscription(self, request).await
    }

    async fn delete_subscription(&self, subscription_arn: &SubscriptionArn) -> PubsubResult<()> {
        PubsubSubscriptionRepository::delete_subscription(self, subscription_arn).await
    }

    async fn get_subscription(
        &self,
        subscription_arn: &SubscriptionArn,
    ) -> PubsubResult<Option<Subscription>> {
        PubsubSubscriptionRepository::get_subscription(self, subscription_arn).await
    }

    async fn set_subscription_attributes(
        &self,
        request: SetSubscriptionAttributesRequest,
    ) -> PubsubResult<Subscription> {
        PubsubSubscriptionRepository::set_subscription_attributes(self, request).await
    }

    async fn get_subscription_attributes(
        &self,
        request: GetSubscriptionAttributesRequest,
    ) -> PubsubResult<GetSubscriptionAttributesResponse> {
        PubsubAttributeRepository::get_subscription_attributes(self, request).await
    }

    async fn list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> PubsubResult<ListSubscriptionsResponse> {
        PubsubSubscriptionRepository::list_subscriptions(self, request).await
    }

    async fn accept_publish(
        &self,
        request: PublishRequest,
        message_id: PubsubMessageId,
        custom_sender_enabled: bool,
    ) -> PubsubResult<()> {
        PubsubPublishAcceptanceRepository::persist_publish_acceptance(
            self,
            request,
            message_id,
            custom_sender_enabled,
        )
        .await
    }

    async fn materialize_publish_intents(&self, limit: usize) -> PubsubResult<usize> {
        PubsubProvider::materialize_publish_chunks(
            self,
            PublishChunkClaimRequest {
                limit: PublishChunkClaimLimit::try_from(limit)?,
                now: TimestampMillis::now(),
            },
        )
        .await
    }

    async fn materialize_publish_chunks(
        &self,
        request: PublishChunkClaimRequest,
    ) -> PubsubResult<usize> {
        let mut materialized = 0usize;
        for chunk in PubsubPublicationRepository::claim_publish_chunks(self, request).await? {
            let commit = PublishChunkCommit::try_from(chunk)?;
            materialized = materialized.saturating_add(commit.records.len());
            PubsubPublicationRepository::commit_publish_chunk(self, commit).await?;
        }
        Ok(materialized)
    }

    async fn put_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        PubsubDeliveryRepository::put_delivery_record(self, record).await
    }

    async fn put_delivery_records(&self, records: Vec<DeliveryRecord>) -> PubsubResult<()> {
        PubsubDeliveryRepository::put_delivery_records(self, records).await
    }

    async fn claim_delivery_records(
        &self,
        request: ClaimDeliveryRecordsRequest,
    ) -> PubsubResult<ClaimDeliveryRecordsResponse> {
        PubsubDeliveryRepository::claim_delivery_records(self, request).await
    }

    async fn update_delivery_record(&self, update: DeliveryRecordUpdate) -> PubsubResult<()> {
        PubsubDeliveryRepository::update_delivery_record(self, update).await
    }

    async fn get_delivery_record(
        &self,
        record_id: &DeliveryRecordId,
    ) -> PubsubResult<Option<DeliveryRecord>> {
        PubsubDeliveryRepository::get_delivery_record(self, record_id).await
    }
}
