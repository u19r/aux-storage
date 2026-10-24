//! Shared pubsub decisions over one SQL or KV native attempt.

use std::{future::Future, num::NonZeroU32};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage_types::{AttemptMode, StorageError, StorageResult, TimestampMillis};
use uuid::Uuid;

use crate::{
    AcceptedPublication, DeliveryRecord, DeliveryRecordId, PublishIntent, PubsubMessageId,
    Subscription, SubscriptionArn, SubscriptionProtocol, Topic, TopicArn,
};

/// Subscriptions bind this identity so deleting and recreating a topic cannot
/// make an old subscription visible under the new topic.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TopicRecord {
    pub topic: Topic,
    pub incarnation: TopicIncarnation,
}

impl TopicRecord {
    pub fn new(topic: Topic) -> Self {
        Self {
            topic,
            incarnation: TopicIncarnation::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct TopicIncarnation(Uuid);

impl TopicIncarnation {
    fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl TryFrom<&[u8]> for TopicIncarnation {
    type Error = StorageError;

    fn try_from(bytes: &[u8]) -> StorageResult<Self> {
        Uuid::from_slice(bytes)
            .map(Self)
            .map_err(|_| StorageError::internal("topic incarnation is invalid"))
    }
}

pub struct TopicPage {
    pub rows: Vec<TopicRecord>,
    pub more: bool,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SubscriptionRecord {
    pub subscription: Subscription,
    pub topic_incarnation: TopicIncarnation,
}

pub struct SubscriptionPage {
    pub rows: Vec<SubscriptionRecord>,
    pub more: bool,
}

pub struct PublishCandidate {
    pub intent: PublishIntent,
    pub subscriptions: Vec<Subscription>,
}

pub struct SubscriptionIdentity<'a> {
    pub topic_incarnation: TopicIncarnation,
    pub protocol: SubscriptionProtocol,
    pub endpoint: &'a str,
}

impl SubscriptionIdentity<'_> {
    pub fn endpoint_digest(&self) -> [u8; 32] {
        digest_bytes(self.endpoint.as_bytes())
    }
}

impl SubscriptionRecord {
    pub fn validate_revision(&self, previous: &Self) -> StorageResult<()> {
        if self.topic_incarnation != previous.topic_incarnation
            || self.subscription.subscription_arn != previous.subscription.subscription_arn
            || self.subscription.topic_arn != previous.subscription.topic_arn
            || self.subscription.protocol != previous.subscription.protocol
            || self.subscription.endpoint != previous.subscription.endpoint
        {
            return Err(StorageError::internal(
                "subscription identity changed during update",
            ));
        }
        Ok(())
    }
}

pub fn digest_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub struct SubscriptionPageRequest<'a> {
    pub topic_incarnation: Option<TopicIncarnation>,
    pub after: Option<&'a SubscriptionArn>,
    pub limit: NonZeroU32,
}

pub trait PubsubAttempt: Send {
    fn read_topics(
        &mut self,
        arns: Vec<TopicArn>,
    ) -> impl Future<Output = StorageResult<Vec<Option<TopicRecord>>>> + Send;

    fn write_topic(
        &mut self,
        record: TopicRecord,
        expected: Option<TopicIncarnation>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn delete_topic(
        &mut self,
        record: TopicRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn read_topic_page(
        &mut self,
        after: Option<&TopicArn>,
        limit: NonZeroU32,
    ) -> impl Future<Output = StorageResult<TopicPage>> + Send;

    fn read_subscription(
        &mut self,
        arn: &SubscriptionArn,
    ) -> impl Future<Output = StorageResult<Option<SubscriptionRecord>>> + Send;

    fn find_subscription_by_identity(
        &mut self,
        identity: SubscriptionIdentity<'_>,
    ) -> impl Future<Output = StorageResult<Option<SubscriptionRecord>>> + Send;

    fn find_subscription_by_token(
        &mut self,
        topic_incarnation: TopicIncarnation,
        token: &str,
    ) -> impl Future<Output = StorageResult<Option<SubscriptionRecord>>> + Send;

    fn write_subscription(
        &mut self,
        record: SubscriptionRecord,
        expected: Option<SubscriptionRecord>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn delete_subscription(
        &mut self,
        record: SubscriptionRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn read_subscription_page(
        &mut self,
        request: SubscriptionPageRequest<'_>,
    ) -> impl Future<Output = StorageResult<SubscriptionPage>> + Send;

    fn read_publish_intent(
        &mut self,
        message_id: &PubsubMessageId,
    ) -> impl Future<Output = StorageResult<Option<PublishIntent>>> + Send;

    /// Read the exact current chunk of one accepted publication; never select
    /// another publication while applying captured work.
    fn read_publish_candidate(
        &mut self,
        message_id: &PubsubMessageId,
    ) -> impl Future<Output = StorageResult<Option<PublishCandidate>>> + Send;

    fn write_publication(
        &mut self,
        publication: AcceptedPublication,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn read_due_publish_chunks(
        &mut self,
        now: TimestampMillis,
        limit: NonZeroU32,
    ) -> impl Future<Output = StorageResult<Vec<PublishCandidate>>> + Send;

    fn replace_publish_intent(
        &mut self,
        expected: PublishIntent,
        next: Option<PublishIntent>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn replace_publish_intents(
        &mut self,
        replacements: Vec<(PublishIntent, PublishIntent)>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn write_delivery_records(
        &mut self,
        records: Vec<DeliveryRecord>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn read_deliveries(
        &mut self,
        ids: Vec<DeliveryRecordId>,
    ) -> impl Future<Output = StorageResult<Vec<Option<DeliveryRecord>>>> + Send;

    fn read_due_deliveries(
        &mut self,
        now: TimestampMillis,
        limit: NonZeroU32,
    ) -> impl Future<Output = StorageResult<Vec<DeliveryRecord>>> + Send;

    fn replace_deliveries(
        &mut self,
        replacements: Vec<(DeliveryRecord, DeliveryRecord)>,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

pub async fn read_topic<A: PubsubAttempt>(
    attempt: &mut A,
    arn: &TopicArn,
) -> StorageResult<Option<TopicRecord>> {
    attempt
        .read_topics(vec![arn.clone()])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| StorageError::internal("topic batch omitted its requested row"))
}

pub trait PubsubPort: storage_types::Clock + Send + Sync {
    type Attempt<'native>: PubsubAttempt
    where Self: 'native;

    fn with_pubsub_attempt<R, F>(
        &self,
        mode: AttemptMode,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static;
}
