//! One KV native attempt for shared pubsub decisions.

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
    KvAttempt, KvBackend, KvStore, catalog::KvCatalogAttempt, pubsub_publication, pubsub_schedule,
    pubsub_subscription, pubsub_topic,
};

impl<A: KvAttempt> PubsubAttempt for KvCatalogAttempt<'_, A> {
    fn default_protection(&self) -> ReadProtection {
        self.protection
    }

    fn read_topics(
        &mut self,
        arns: Vec<TopicArn>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<TopicRecord>>>> + Send {
        pubsub_topic::read_topics(self.attempt, protection, arns)
    }

    fn write_topic(
        &mut self,
        record: TopicRecord,
        expected: Option<TopicIncarnation>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_topic::write_topic(self.attempt, record, expected)
    }

    fn delete_topic(
        &mut self,
        record: TopicRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_topic::delete_topic(self.attempt, record)
    }

    fn read_topic_page(
        &mut self,
        after: Option<&TopicArn>,
        limit: std::num::NonZeroU32,
    ) -> impl Future<Output = StorageResult<TopicPage>> + Send {
        pubsub_topic::read_topic_page(self.attempt, after, limit)
    }

    fn read_subscription(
        &mut self,
        arn: &SubscriptionArn,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Option<SubscriptionRecord>>> + Send {
        pubsub_subscription::read_subscription(self.attempt, protection, arn)
    }

    fn find_subscription_by_identity(
        &mut self,
        identity: SubscriptionIdentity<'_>,
    ) -> impl Future<Output = StorageResult<Option<SubscriptionRecord>>> + Send {
        pubsub_subscription::find_subscription_by_identity(self.attempt, self.protection, identity)
    }

    fn find_subscription_by_token(
        &mut self,
        incarnation: TopicIncarnation,
        token: &str,
    ) -> impl Future<Output = StorageResult<Option<SubscriptionRecord>>> + Send {
        pubsub_subscription::find_subscription_by_token(
            self.attempt,
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
        pubsub_subscription::write_subscription(self.attempt, record, expected)
    }

    fn delete_subscription(
        &mut self,
        record: SubscriptionRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_subscription::delete_subscription(self.attempt, record)
    }

    fn read_subscription_page(
        &mut self,
        request: SubscriptionPageRequest<'_>,
    ) -> impl Future<Output = StorageResult<SubscriptionPage>> + Send {
        pubsub_subscription::read_subscription_page(self.attempt, request)
    }

    fn read_pending_subscription_page(
        &mut self,
        after: Option<&PendingSubscriptionCursor>,
        limit: std::num::NonZeroU32,
    ) -> impl Future<Output = StorageResult<PendingSubscriptionPage>> + Send {
        pubsub_subscription::read_pending_subscription_page(self.attempt, after, limit)
    }

    fn write_publication(
        &mut self,
        publication: AcceptedPublication,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_publication::write_publication(self.attempt, publication)
    }

    fn read_due_publish_chunks(
        &mut self,
        request: &pubsub_provider::PublishDueRequest,
    ) -> impl Future<Output = StorageResult<Vec<pubsub_provider::DueRow<PublishCandidate>>>> + Send
    {
        pubsub_publication::read_due_publish_chunks(self.attempt, request)
    }

    fn move_publish_due(
        &mut self,
        row: &pubsub_provider::UndecodableRow,
        until: storage_types::TimestampMillis,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_publication::move_publish_due(self.attempt, row, until)
    }

    fn replace_publish_intent(
        &mut self,
        expected: PublishIntent,
        next: Option<PublishIntent>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_publication::replace_publish_intent(self.attempt, expected, next)
    }

    fn replace_publish_intents(
        &mut self,
        replacements: Vec<pubsub_provider::PublishIntentClaim>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_publication::replace_publish_intents(self.attempt, replacements)
    }

    fn store_new_deliveries(
        &mut self,
        rows: Vec<(DeliveryPayload, DeliveryState)>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_schedule::store_new_deliveries(self.attempt, rows)
    }

    fn scan_claimable(
        &mut self,
        scan: &ClaimScan,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<ClaimRows>>> + Send {
        pubsub_schedule::scan_claimable(self.attempt, scan)
    }

    fn store_claimed_states(
        &mut self,
        claims: Vec<(DeliveryState, DeliveryState)>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_schedule::store_claimed_states(self.attempt, claims)
    }

    fn store_delivery_states(
        &mut self,
        changes: Vec<StateChange>,
    ) -> impl Future<Output = StorageResult<Vec<bool>>> + Send {
        pubsub_schedule::store_delivery_states(self.attempt, changes)
    }

    fn delete_deliveries(
        &mut self,
        fences: Vec<DeliveryFence>,
    ) -> impl Future<Output = StorageResult<Vec<bool>>> + Send {
        pubsub_schedule::delete_deliveries(self.attempt, fences)
    }

    async fn load_records(&mut self, reads: RecordReads<'_>) -> StorageResult<RecordObservations> {
        let queue_name = reads.queue_name;
        let message_ids = reads.message_ids;
        let hints = self.queue_catalog_hints.clone();
        // The hint only chooses extra keys of this wave; the row the
        // operation uses is the one read in it (W8-E-e).
        let hinted = match (&hints, queue_name) {
            (Some((hints, now)), Some(name)) if !message_ids.is_empty() => {
                hints.get(name, *now).map(|record| record.incarnation)
            }
            _ => None,
        };
        let (observed, present) =
            pubsub_schedule::load_records(&mut *self.attempt, reads, hinted).await?;
        if let (Some((hints, now)), Some(name)) = (&hints, queue_name) {
            hints.observe(name, observed.queue.as_ref(), *now);
        }
        self.queue_absence = match (hinted, observed.queue.as_ref()) {
            (Some(incarnation), Some(record)) if record.incarnation == incarnation => {
                Some(crate::queue::CheckedAbsence {
                    incarnation,
                    present: message_ids.iter().copied().zip(present).collect(),
                })
            }
            _ => None,
        };
        Ok(observed)
    }

    fn stage_delivery_deletes(&mut self, fences: Vec<DeliveryFence>) -> StorageResult<()> {
        pubsub_schedule::stage_delivery_deletes(self.attempt, fences)
    }

    fn move_undecodable_state(
        &mut self,
        locator: &DeliveryLocator,
        until: storage_types::TimestampMillis,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_schedule::move_undecodable_state(self.attempt, locator, until)
    }

    fn delete_state_at(
        &mut self,
        locator: &DeliveryLocator,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        pubsub_schedule::delete_state_at(self.attempt, locator)
    }

    fn scan_destination_heads(
        &mut self,
        after: &SweepCursor,
        limit: std::num::NonZeroU32,
    ) -> impl Future<Output = StorageResult<SweepPage>> + Send {
        pubsub_schedule::scan_destination_heads(self.attempt, after, limit)
    }

    fn load_delivery(
        &mut self,
        id: &DeliveryRecordId,
    ) -> impl Future<Output = StorageResult<Option<(StateRow, PayloadRow)>>> + Send {
        pubsub_schedule::load_delivery(self.attempt, self.protection, id)
    }

    fn read_next_publish_due(
        &mut self,
    ) -> impl Future<Output = StorageResult<Option<storage_types::TimestampMillis>>> + Send {
        pubsub_publication::read_next_publish_due(self.attempt)
    }
}

impl<B: KvBackend + 'static> PubsubPort for KvStore<B> {
    type Attempt<'native>
        = KvCatalogAttempt<'native, B::Attempt>
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
        let board = self.queue_wake.clone();
        let hints = Arc::clone(&self.queue_catalog_hints);
        let clock = Arc::clone(&self.clock);
        // The board is told after commit and before the permit is released.
        let attempt = crate::queue::run_and_publish(
            Arc::clone(&self.backend),
            board,
            move |backend, slots| async move {
                backend
                    .with_attempt(request, move |native| {
                        Box::pin(async move {
                            let mut attempt =
                                KvCatalogAttempt::new(native, request.mode.read_protection());
                            attempt.queue_catalog_hints = Some((hints, clock.now()));
                            let result = step(&mut attempt).await;
                            // As the queue attempt: a hinted row that no read
                            // verified is read before the attempt returns. No
                            // pubsub path calls `read_queue_for_messages`
                            // today, so this is `Ok(true)` with no I/O.
                            match (result, attempt.verify_pending_queue_catalog().await) {
                                (_, Ok(false)) => Err(crate::queue::stale_queue_catalog()),
                                (Ok(result), Ok(true)) => {
                                    crate::queue::store_wake_slots(
                                        &slots,
                                        attempt.queue_wakes.take(),
                                    );
                                    Ok(result)
                                }
                                (Ok(_), Err(error)) | (Err(error), _) => Err(error),
                            }
                        })
                    })
                    .await
            },
        );
        self.admission.settle(request, attempt)
    }
}
