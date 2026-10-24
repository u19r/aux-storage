//! One SQL native attempt for shared pubsub decisions.

use std::future::Future;

use futures::future::BoxFuture;
use pubsub_provider::{
    AcceptedPublication, DeliveryRecord, DeliveryRecordId, PublishCandidate, PublishIntent,
    PubsubAttempt, PubsubMessageId, PubsubPort, SubscriptionArn, SubscriptionIdentity,
    SubscriptionPage, SubscriptionPageRequest, SubscriptionRecord, TopicArn, TopicIncarnation,
    TopicPage, TopicRecord,
};
use storage_types::{AttemptMode, StorageResult};

use crate::{
    SqlAttempt, SqlBackend, SqlLayout, SqlStore, catalog::SqlCatalogAttempt, pubsub_delivery,
    pubsub_publication, pubsub_subscription, pubsub_topic,
};

impl<A: SqlAttempt, L: SqlLayout> PubsubAttempt for SqlCatalogAttempt<'_, A, L> {
    fn read_topics(
        &mut self,
        arns: Vec<TopicArn>,
    ) -> impl Future<Output = StorageResult<Vec<Option<TopicRecord>>>> + Send {
        pubsub_topic::read_topics(self.attempt, self.engine, self.protection, arns)
    }

    fn write_topic(
        &mut self,
        record: TopicRecord,
        expected: Option<TopicIncarnation>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_topic::write_topic(self.attempt, self.engine, record, expected)
    }

    fn delete_topic(
        &mut self,
        record: TopicRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_topic::delete_topic(self.attempt, self.engine, record)
    }

    fn read_topic_page(
        &mut self,
        after: Option<&TopicArn>,
        limit: std::num::NonZeroU32,
    ) -> impl Future<Output = StorageResult<TopicPage>> + Send {
        pubsub_topic::read_topic_page(self.attempt, self.engine, after, limit)
    }

    fn read_subscription(
        &mut self,
        arn: &SubscriptionArn,
    ) -> impl Future<Output = StorageResult<Option<SubscriptionRecord>>> + Send {
        pubsub_subscription::read_subscription(self.attempt, self.engine, self.protection, arn)
    }

    fn find_subscription_by_identity(
        &mut self,
        identity: SubscriptionIdentity<'_>,
    ) -> impl Future<Output = StorageResult<Option<SubscriptionRecord>>> + Send {
        pubsub_subscription::find_subscription_by_identity(
            self.attempt,
            self.engine,
            self.protection,
            identity,
        )
    }

    fn find_subscription_by_token(
        &mut self,
        incarnation: TopicIncarnation,
        token: &str,
    ) -> impl Future<Output = StorageResult<Option<SubscriptionRecord>>> + Send {
        pubsub_subscription::find_subscription_by_token(
            self.attempt,
            self.engine,
            self.protection,
            incarnation,
            token,
        )
    }

    fn write_subscription(
        &mut self,
        record: SubscriptionRecord,
        expected: Option<SubscriptionRecord>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_subscription::write_subscription(self.attempt, self.engine, record, expected)
    }

    fn delete_subscription(
        &mut self,
        record: SubscriptionRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_subscription::delete_subscription(self.attempt, self.engine, record)
    }

    fn read_subscription_page(
        &mut self,
        request: SubscriptionPageRequest<'_>,
    ) -> impl Future<Output = StorageResult<SubscriptionPage>> + Send {
        pubsub_subscription::read_subscription_page(self.attempt, self.engine, request)
    }

    fn read_publish_intent(
        &mut self,
        id: &PubsubMessageId,
    ) -> impl Future<Output = StorageResult<Option<PublishIntent>>> + Send {
        pubsub_publication::read_publish_intent(self.attempt, self.engine, self.protection, id)
    }

    fn read_publish_candidate(
        &mut self,
        message_id: &PubsubMessageId,
    ) -> impl Future<Output = StorageResult<Option<PublishCandidate>>> + Send {
        pubsub_publication::read_publish_candidate(
            self.attempt,
            self.engine,
            self.protection,
            message_id,
        )
    }

    fn write_publication(
        &mut self,
        publication: AcceptedPublication,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_publication::write_publication(self.attempt, self.engine, publication)
    }

    fn read_due_publish_chunks(
        &mut self,
        now: storage_types::TimestampMillis,
        limit: std::num::NonZeroU32,
    ) -> impl Future<Output = StorageResult<Vec<PublishCandidate>>> + Send {
        pubsub_publication::read_due_publish_chunks(self.attempt, self.engine, now, limit)
    }

    fn replace_publish_intent(
        &mut self,
        expected: PublishIntent,
        next: Option<PublishIntent>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_publication::replace_publish_intent(self.attempt, self.engine, expected, next)
    }

    fn replace_publish_intents(
        &mut self,
        replacements: Vec<(PublishIntent, PublishIntent)>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_publication::replace_publish_intents(self.attempt, self.engine, replacements)
    }

    fn write_delivery_records(
        &mut self,
        records: Vec<DeliveryRecord>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_delivery::write_delivery_records(self.attempt, self.engine, records)
    }

    fn read_deliveries(
        &mut self,
        ids: Vec<DeliveryRecordId>,
    ) -> impl Future<Output = StorageResult<Vec<Option<DeliveryRecord>>>> + Send {
        pubsub_delivery::read_deliveries(self.attempt, self.engine, self.protection, ids)
    }

    fn read_due_deliveries(
        &mut self,
        now: storage_types::TimestampMillis,
        limit: std::num::NonZeroU32,
    ) -> impl Future<Output = StorageResult<Vec<DeliveryRecord>>> + Send {
        pubsub_delivery::read_due_deliveries(self.attempt, self.engine, now, limit)
    }

    fn replace_deliveries(
        &mut self,
        replacements: Vec<(DeliveryRecord, DeliveryRecord)>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_delivery::replace_deliveries(self.attempt, self.engine, replacements)
    }
}

impl<B: SqlBackend + 'static> PubsubPort for SqlStore<B> {
    type Attempt<'native>
        = SqlCatalogAttempt<'native, B::Attempt>
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
            + 'static,
    {
        let engine = self.backend.engine();
        self.backend.with_attempt(mode, move |native| {
            Box::pin(async move {
                let mut attempt = SqlCatalogAttempt::new(native, engine, mode.read_protection());
                operation(&mut attempt).await
            })
        })
    }
}
