//! One SQL native attempt for shared pubsub decisions.

use std::{future::Future, sync::Arc};

use futures::future::BoxFuture;
use pubsub_provider::{
    AcceptedPublication, ClaimRows, ClaimScan, DeliveryFence, DeliveryLocator, DeliveryPayload,
    DeliveryRecordId, DeliveryState, PayloadRow, PendingSubscriptionCursor,
    PendingSubscriptionPage, PublishCandidate, PublishIntent, PubsubAttempt, PubsubPort,
    RecordObservations, RecordReads, StateChange, StateRow, SubscriptionArn, SubscriptionIdentity,
    SubscriptionPage, SubscriptionPageRequest, SubscriptionRecord, SweepCursor, SweepPage,
    TopicArn, TopicIncarnation, TopicPage, TopicRecord,
};
use storage_types::{AttemptRequest, ReadProtection, StorageResult};

use crate::{
    SqlAttempt, SqlBackend, SqlLayout, SqlStore,
    catalog::{SqlCatalogAttempt, SqlDynamoCount},
    pubsub_publication, pubsub_schedule, pubsub_subscription, pubsub_topic,
    queue_wake_publish::{StagedWakes, publish_after, store_staged},
};

impl<A: SqlAttempt, L: SqlLayout> PubsubAttempt for SqlCatalogAttempt<'_, A, L> {
    fn default_protection(&self) -> ReadProtection {
        self.protection
    }

    fn read_topics(
        &mut self,
        arns: Vec<TopicArn>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<TopicRecord>>>> + Send {
        pubsub_topic::read_topics(self.attempt, self.engine, protection, arns)
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
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Option<SubscriptionRecord>>> + Send {
        pubsub_subscription::read_subscription(self.attempt, self.engine, protection, arn)
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

    fn read_pending_subscription_page(
        &mut self,
        after: Option<&PendingSubscriptionCursor>,
        limit: std::num::NonZeroU32,
    ) -> impl Future<Output = StorageResult<PendingSubscriptionPage>> + Send {
        pubsub_subscription::read_pending_subscription_page(self.attempt, self.engine, after, limit)
    }

    fn write_publication(
        &mut self,
        publication: AcceptedPublication,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        let staged = pubsub_publication::publication_statements(self.engine, publication).and_then(
            |statements| {
                statements.into_iter().try_for_each(|statement| {
                    self.stage_pubsub_write(statement, SqlDynamoCount::ExactlyOne)
                })
            },
        );
        std::future::ready(staged)
    }

    fn read_due_publish_chunks(
        &mut self,
        request: &pubsub_provider::PublishDueRequest,
    ) -> impl Future<Output = StorageResult<Vec<pubsub_provider::DueRow<PublishCandidate>>>> + Send
    {
        pubsub_publication::read_due_publish_chunks(self.attempt, self.engine, request)
    }

    fn move_publish_due(
        &mut self,
        row: &pubsub_provider::UndecodableRow,
        until: storage_types::TimestampMillis,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_publication::move_publish_due(self.attempt, self.engine, row, until)
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
        replacements: Vec<pubsub_provider::PublishIntentClaim>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_publication::replace_publish_intents(self.attempt, self.engine, replacements)
    }

    fn store_new_deliveries(
        &mut self,
        rows: Vec<(DeliveryPayload, DeliveryState)>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_schedule::store_new_deliveries(self.attempt, self.engine, rows)
    }

    fn scan_claimable(
        &mut self,
        scan: &ClaimScan,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<ClaimRows>>> + Send {
        pubsub_schedule::scan_claimable(self.attempt, self.engine, scan)
    }

    fn store_claimed_states(
        &mut self,
        claims: Vec<(DeliveryState, DeliveryState)>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_schedule::store_claimed_states(self.attempt, self.engine, claims)
    }

    fn store_delivery_states(
        &mut self,
        changes: Vec<StateChange>,
    ) -> impl Future<Output = StorageResult<Vec<bool>>> + Send {
        pubsub_schedule::store_delivery_states(self.attempt, self.engine, changes)
    }

    fn delete_deliveries(
        &mut self,
        fences: Vec<DeliveryFence>,
    ) -> impl Future<Output = StorageResult<Vec<bool>>> + Send {
        pubsub_schedule::delete_deliveries(self.attempt, self.engine, fences)
    }

    fn load_records(
        &mut self,
        reads: RecordReads<'_>,
    ) -> impl Future<Output = StorageResult<RecordObservations>> + Send {
        pubsub_schedule::load_records(self.attempt, self.engine, reads)
    }

    fn stage_delivery_deletes(&mut self, fences: Vec<DeliveryFence>) -> StorageResult<()> {
        for fence in &fences {
            let [state, payload] = pubsub_schedule::delivery_delete_statements(self.engine, fence)?;
            self.stage_pubsub_write(state, SqlDynamoCount::ExactlyOne)?;
            // The payload goes only when no state row of the record is left.
            self.stage_pubsub_write(payload, SqlDynamoCount::AtMostRows(1))?;
        }
        Ok(())
    }

    fn move_undecodable_state(
        &mut self,
        locator: &DeliveryLocator,
        until: storage_types::TimestampMillis,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_schedule::move_undecodable_state(self.attempt, self.engine, locator, until)
    }

    fn delete_state_at(
        &mut self,
        locator: &DeliveryLocator,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_schedule::delete_state_at(self.attempt, self.engine, locator)
    }

    fn scan_destination_heads(
        &mut self,
        after: &SweepCursor,
        limit: std::num::NonZeroU32,
    ) -> impl Future<Output = StorageResult<SweepPage>> + Send {
        pubsub_schedule::scan_destination_heads(self.attempt, self.engine, after, limit)
    }

    fn load_delivery(
        &mut self,
        id: &DeliveryRecordId,
    ) -> impl Future<Output = StorageResult<Option<(StateRow, PayloadRow)>>> + Send {
        pubsub_schedule::load_delivery(self.attempt, self.engine, id)
    }

    fn read_next_publish_due(
        &mut self,
    ) -> impl Future<Output = StorageResult<Option<storage_types::TimestampMillis>>> + Send {
        pubsub_publication::read_next_publish_due(self.attempt, self.engine)
    }
}

impl<B: SqlBackend + 'static> PubsubPort for SqlStore<B> {
    type Attempt<'native>
        = SqlCatalogAttempt<'native, B::Attempt>
    where Self: 'native;

    fn with_attempt<R, F>(
        &self,
        request: AttemptRequest,
        step: F,
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
        let board = self.queue_wake.clone();
        let backend = Arc::clone(&self.backend);
        let staged = StagedWakes::default();
        let body_staged = Arc::clone(&staged);
        let attempt = async move {
            backend
                .with_attempt(request, move |native| {
                    Box::pin(async move {
                        let mut attempt =
                            SqlCatalogAttempt::new_complete_write(native, engine, request);
                        let result = step(&mut attempt).await?;
                        // Staged pubsub and queue writes (W3 C8): with COMMIT
                        // on Postgres, one batch before the driver's commit
                        // elsewhere.
                        if !attempt.dynamo_writes.is_empty() {
                            attempt.finish_dynamo_statements().await?;
                        }
                        store_staged(&body_staged, attempt.queue_wakes.take());
                        Ok(result)
                    })
                })
                .await
        };
        self.admission
            .settle(request, publish_after(board, staged, attempt))
    }
}
