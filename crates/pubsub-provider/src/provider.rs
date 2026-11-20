use std::collections::BTreeMap;

use async_trait::async_trait;
use storage_types::TimestampMillis;

use crate::{
    ConfirmSubscriptionRequest, ConfirmSubscriptionResponse, DeliveryRecordId,
    GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse, GetTopicAttributesRequest,
    GetTopicAttributesResponse, ListSubscriptionsRequest, ListSubscriptionsResponse,
    ListTopicsRequest, ListTopicsResponse, PublishChunkClaimLimit, PublishChunkClaimRequest,
    PublishChunkCommit, PublishRequest, PubsubMessageId, PubsubPort, PubsubResult,
    SetTopicAttributesRequest, SettleReport, Subscription, SubscriptionAcceptanceInput,
    SubscriptionArn, SubscriptionAttributes, Topic, TopicArn, attribute_operations,
    publication_operations, subscription_operations, topic_operations,
};

/// The object-safe facade of the pubsub domain over one store. Requests are
/// validated by the caller (`Topic::try_new`, `Subscription::try_new`,
/// `SubscriptionAttributes::parse`), which owns the messaging identity.
/// What one materialization pass committed. The runner continues while
/// chunks commit, also when a chunk wrote no record (review D3).
#[derive(Debug, Default)]
pub struct MaterializeReport {
    pub chunks: usize,
    pub records: usize,
    /// The destination and due time of each delivery written: the runner
    /// adds them to its candidate set (W3S plan 5.7).
    pub written: Vec<(crate::DestinationKey, TimestampMillis)>,
    pub failed_chunks: usize,
    pub first_error: Option<crate::PubsubError>,
}

#[async_trait]
pub trait PubsubProvider: Send + Sync {
    /// The store clock; delivery times use it, so tests can move it.
    fn now(&self) -> TimestampMillis;

    async fn initialize(&self) -> PubsubResult<()>;

    async fn create_topic(&self, topic: Topic) -> PubsubResult<Topic>;

    async fn delete_topic(&self, topic_arn: &TopicArn) -> PubsubResult<()>;

    async fn get_topic(&self, topic_arn: &TopicArn) -> PubsubResult<Option<Topic>>;

    async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse>;

    async fn set_topic_attributes(&self, request: SetTopicAttributesRequest)
    -> PubsubResult<Topic>;

    async fn list_topics(&self, request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse>;

    /// `TagResource` (add or replace) and `UntagResource` on a topic.
    async fn change_topic_tags(
        &self,
        topic_arn: &TopicArn,
        added: BTreeMap<String, String>,
        removed: Vec<String>,
    ) -> PubsubResult<Topic>;

    async fn create_subscription(&self, subscription: Subscription) -> PubsubResult<Subscription>;

    async fn confirm_subscription(
        &self,
        request: ConfirmSubscriptionRequest,
    ) -> PubsubResult<ConfirmSubscriptionResponse>;

    async fn delete_subscription(&self, subscription_arn: &SubscriptionArn) -> PubsubResult<()>;

    /// The customer `Unsubscribe`: `delete_subscription`, except that a
    /// subscription pending confirmation is refused.
    async fn unsubscribe(&self, subscription_arn: &SubscriptionArn) -> PubsubResult<()>;

    async fn get_subscription(
        &self,
        subscription_arn: &SubscriptionArn,
    ) -> PubsubResult<Option<Subscription>>;

    async fn set_subscription_attributes(
        &self,
        subscription_arn: &SubscriptionArn,
        attributes: SubscriptionAttributes,
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

    /// One Durable attempt for every valid entry (W3 design 8).
    async fn accept_publish_batch(
        &self,
        topic_arn: TopicArn,
        entries: Vec<crate::PublishBatchEntry>,
        custom_sender_enabled: bool,
    ) -> PubsubResult<Vec<crate::PublishBatchResult>>;

    async fn materialize_publish_intents(&self, limit: usize) -> PubsubResult<MaterializeReport>;

    async fn materialize_publish_chunks(
        &self,
        request: PublishChunkClaimRequest,
    ) -> PubsubResult<MaterializeReport>;

    /// The read-only probe of the publish-intent due index (design 4.2).
    async fn next_publish_due(&self) -> PubsubResult<Option<TimestampMillis>>;

    /// Claim the due records of each ask (W3S plan 5.6).
    async fn claim_deliveries(
        &self,
        request: crate::DeliveryClaimRequest,
    ) -> PubsubResult<Vec<crate::DestinationClaim>>;

    /// One page of destination heads after `after`, read-only.
    async fn sweep_destination_heads(
        &self,
        after: crate::SweepCursor,
        limit: std::num::NonZeroU32,
    ) -> PubsubResult<crate::SweepPage>;

    /// One turn of the stale pending-subscription sweep: one page of the
    /// pending index after `cursor`, and the delete of each entry still
    /// pending `PENDING_SUBSCRIPTION_LIFETIME` after it was created.
    async fn sweep_stale_subscriptions(
        &self,
        cursor: Option<crate::PendingSubscriptionCursor>,
        limit: std::num::NonZeroU32,
    ) -> PubsubResult<crate::StaleSubscriptionSweep>;

    /// The state and payload of one delivery (audit reads only).
    async fn get_delivery(
        &self,
        id: &DeliveryRecordId,
    ) -> PubsubResult<Option<(crate::DeliveryState, crate::DeliveryPayload)>>;

    /// Settle one claimed delivery under its fence.
    async fn settle_delivery(&self, input: crate::SettleInput) -> PubsubResult<SettleReport>;

    /// Deliver claimed deliveries to a queue on the same store in one
    /// transaction.
    async fn deliver_to_local_queue(
        &self,
        delivery: crate::LocalDelivery,
    ) -> PubsubResult<crate::LocalOutcome>;
}

#[async_trait]
impl<T: PubsubPort> PubsubProvider for T {
    fn now(&self) -> TimestampMillis {
        storage_types::Clock::now(self)
    }

    async fn initialize(&self) -> PubsubResult<()> {
        // Initialized family stores already hold the pubsub schema or key
        // format.
        Ok(())
    }

    async fn create_topic(&self, topic: Topic) -> PubsubResult<Topic> {
        topic_operations::create_topic(self, topic).await
    }

    async fn delete_topic(&self, topic_arn: &TopicArn) -> PubsubResult<()> {
        topic_operations::delete_topic(self, topic_arn).await
    }

    async fn get_topic(&self, topic_arn: &TopicArn) -> PubsubResult<Option<Topic>> {
        topic_operations::get_topic(self, topic_arn).await
    }

    async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse> {
        attribute_operations::get_topic_attributes(self, request).await
    }

    async fn set_topic_attributes(
        &self,
        request: SetTopicAttributesRequest,
    ) -> PubsubResult<Topic> {
        topic_operations::set_topic_attributes(self, request).await
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse> {
        topic_operations::list_topics(self, request).await
    }

    async fn change_topic_tags(
        &self,
        topic_arn: &TopicArn,
        added: BTreeMap<String, String>,
        removed: Vec<String>,
    ) -> PubsubResult<Topic> {
        crate::topic_operations::change_topic_tags(self, topic_arn, added, removed).await
    }

    /// The creation time comes from the store clock: it is the accepted time
    /// of the confirmation delivery.
    async fn create_subscription(
        &self,
        mut subscription: Subscription,
    ) -> PubsubResult<Subscription> {
        subscription.created_at = storage_types::Clock::now(self);
        let input = SubscriptionAcceptanceInput::capture(subscription)?;
        subscription_operations::create_subscription(self, input).await
    }

    async fn confirm_subscription(
        &self,
        request: ConfirmSubscriptionRequest,
    ) -> PubsubResult<ConfirmSubscriptionResponse> {
        subscription_operations::confirm_subscription(self, request).await
    }

    async fn delete_subscription(&self, subscription_arn: &SubscriptionArn) -> PubsubResult<()> {
        subscription_operations::delete_subscription(self, subscription_arn).await
    }

    async fn unsubscribe(&self, subscription_arn: &SubscriptionArn) -> PubsubResult<()> {
        subscription_operations::unsubscribe(self, subscription_arn).await
    }

    async fn get_subscription(
        &self,
        subscription_arn: &SubscriptionArn,
    ) -> PubsubResult<Option<Subscription>> {
        subscription_operations::get_subscription(self, subscription_arn).await
    }

    async fn set_subscription_attributes(
        &self,
        subscription_arn: &SubscriptionArn,
        attributes: SubscriptionAttributes,
    ) -> PubsubResult<Subscription> {
        subscription_operations::set_subscription_attributes(self, subscription_arn, attributes)
            .await
    }

    async fn get_subscription_attributes(
        &self,
        request: GetSubscriptionAttributesRequest,
    ) -> PubsubResult<GetSubscriptionAttributesResponse> {
        attribute_operations::get_subscription_attributes(self, request).await
    }

    async fn list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> PubsubResult<ListSubscriptionsResponse> {
        subscription_operations::list_subscriptions(self, request).await
    }

    /// The request is canonicalized and checked before the attempt.
    async fn accept_publish(
        &self,
        mut request: PublishRequest,
        message_id: PubsubMessageId,
        custom_sender_enabled: bool,
    ) -> PubsubResult<()> {
        request.validate()?;
        crate::accept_publish_on_port(self, request, message_id, custom_sender_enabled).await
    }

    async fn accept_publish_batch(
        &self,
        topic_arn: TopicArn,
        entries: Vec<crate::PublishBatchEntry>,
        custom_sender_enabled: bool,
    ) -> PubsubResult<Vec<crate::PublishBatchResult>> {
        crate::accept_publish_batch_on_port(self, topic_arn, entries, custom_sender_enabled).await
    }

    async fn materialize_publish_intents(&self, limit: usize) -> PubsubResult<MaterializeReport> {
        PubsubProvider::materialize_publish_chunks(
            self,
            PublishChunkClaimRequest {
                limit: PublishChunkClaimLimit::try_from(limit)?,
                now: storage_types::Clock::now(self),
            },
        )
        .await
    }

    async fn materialize_publish_chunks(
        &self,
        request: PublishChunkClaimRequest,
    ) -> PubsubResult<MaterializeReport> {
        let mut report = MaterializeReport::default();
        for chunk in publication_operations::claim_publish_chunks(self, request).await? {
            let commit = PublishChunkCommit::try_from(chunk)?;
            let records = commit.records.len();
            let written: Vec<_> = commit
                .records
                .iter()
                .map(|(_, state)| (state.destination.clone(), state.due_at))
                .collect();
            let filtered_out = commit.filtered_out as usize;
            // One failed chunk does not keep the others leased (review D8):
            // its lease returns it; the rest are committed.
            match publication_operations::commit_publish_chunk(self, commit).await {
                Ok(()) => {
                    report.chunks += 1;
                    report.records += records;
                    report.written.extend(written);
                    storage_common::telemetry::record_provider_count(
                        "pubsub",
                        "delivery_filtered_out",
                        filtered_out,
                    );
                }
                Err(error) => {
                    report.failed_chunks += 1;
                    report.first_error.get_or_insert(error);
                }
            }
        }
        Ok(report)
    }

    async fn next_publish_due(&self) -> PubsubResult<Option<TimestampMillis>> {
        publication_operations::next_publish_due(self).await
    }

    async fn claim_deliveries(
        &self,
        request: crate::DeliveryClaimRequest,
    ) -> PubsubResult<Vec<crate::DestinationClaim>> {
        crate::delivery_claim::claim_deliveries(self, request).await
    }

    async fn sweep_destination_heads(
        &self,
        after: crate::SweepCursor,
        limit: std::num::NonZeroU32,
    ) -> PubsubResult<crate::SweepPage> {
        crate::delivery_claim::sweep_destination_heads(self, after, limit).await
    }

    async fn sweep_stale_subscriptions(
        &self,
        cursor: Option<crate::PendingSubscriptionCursor>,
        limit: std::num::NonZeroU32,
    ) -> PubsubResult<crate::StaleSubscriptionSweep> {
        crate::stale_subscription_sweep::sweep_stale_subscriptions(self, cursor, limit)
            .await
            .map_err(crate::PubsubError::from)
    }

    async fn get_delivery(
        &self,
        id: &DeliveryRecordId,
    ) -> PubsubResult<Option<(crate::DeliveryState, crate::DeliveryPayload)>> {
        crate::delivery_claim::get_delivery(self, id).await
    }

    async fn settle_delivery(&self, input: crate::SettleInput) -> PubsubResult<SettleReport> {
        crate::delivery_settlement::settle_delivery(self, input).await
    }

    async fn deliver_to_local_queue(
        &self,
        delivery: crate::LocalDelivery,
    ) -> PubsubResult<crate::LocalOutcome> {
        crate::delivery_settlement::deliver_to_local_queue(self, delivery).await
    }
}
