//! Shared pubsub decisions over one SQL or KV native attempt.

use std::{future::Future, num::NonZeroU32, sync::Arc};

use futures::future::BoxFuture;
use queue_provider::{MessageId, QueueCatalogRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage_types::{
    AttemptPlan, AttemptRequest, AttemptTry, ReadProtection, StorageError, StorageResult,
    TimestampMillis,
};
use uuid::Uuid;

use crate::{
    AcceptedPublication, ClaimRows, ClaimScan, DeliveryFence, DeliveryLocator, DeliveryPayload,
    DeliveryRecordId, DeliveryState, PayloadRow, PublishIntent, StateChange, StateRow,
    Subscription, SubscriptionArn, SubscriptionProtocol, SweepCursor, SweepPage, Topic, TopicArn,
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
        Self(storage_types::ambient::uuid_now_v7())
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
    /// The due time of the index row that the claim read (SQL `due_at`, KV
    /// due key). It is the only due source: after a poison move it differs
    /// from `intent.due_at()` (review W3S finding 1).
    pub due_at: TimestampMillis,
}

/// One claim of a due intent: the index row at `scanned_due` is replaced by
/// the row of `claimed`.
pub struct PublishIntentClaim {
    pub scanned_due: TimestampMillis,
    pub previous: PublishIntent,
    pub claimed: PublishIntent,
}

/// A due row that the backend could not decode or whose stored columns
/// disagree (review D4): its key and its current due time. No payload, so
/// it can be logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndecodableRow {
    pub key: String,
    pub due_at: TimestampMillis,
    pub reason: &'static str,
}

/// One row of a due-index read: a decoded value, or a typed observation of
/// a row that cannot be decoded. The provider owner decides what to do.
pub enum DueRow<T> {
    Decoded(T),
    Undecodable(UndecodableRow),
}

/// One publish-chunk claim read (design 7.2, review D8): SQL ignores the
/// window and slot; an ordered-key store scans `window` due entries at
/// `Snapshot` and reads only the chosen intents with conflict protection.
#[derive(Clone, Copy, Debug)]
pub struct PublishDueRequest {
    pub now: TimestampMillis,
    pub limit: NonZeroU32,
    pub window: NonZeroU32,
    pub slot: storage_types::ClaimSlot,
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

/// The position of one pending-subscription index entry. The backend defines
/// the bytes (KV: the token-index key after its family prefix; SQL: the token
/// text) and reads the entries after it. Stored in the sweep job checkpoint.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingSubscriptionCursor(pub Vec<u8>);

/// One pending subscription as the pending index holds it.
pub struct PendingSubscription {
    pub cursor: PendingSubscriptionCursor,
    pub subscription_arn: SubscriptionArn,
    pub created_at: TimestampMillis,
}

/// At most `limit` entries; `more` when another entry may follow the last.
pub struct PendingSubscriptionPage {
    pub rows: Vec<PendingSubscription>,
    pub more: bool,
}

/// Independent typed records that one attempt reads in ONE native wave
/// (W3 C8): the backend sends every entry in one round trip (one KV point
/// batch, one SQL pipelined batch or statement batch). Each entry keeps its
/// own protection. An empty entry is not read.
#[derive(Default)]
pub struct RecordReads<'a> {
    /// Delivery states, read with `Conflict`. The answer says, for each
    /// fence in input order, whether the stored state still holds it (R4).
    pub fences: &'a [DeliveryFence],
    /// One queue catalog row by name, read at `Snapshot`: the incarnation
    /// is the fence of every message write (queue decision D1).
    pub queue_name: Option<&'a str>,
    /// Ids of the queue messages this attempt may insert into `queue_name`,
    /// in any order. A backend may read their absence in this wave when it
    /// knows the queue's incarnation in advance (a node-local hint) and use
    /// that answer for its own insert check (W8-E-e). A backend may ignore it.
    pub message_ids: &'a [MessageId],
}

/// The answer of `PubsubAttempt::load_records`.
pub struct RecordObservations {
    /// One entry for each fence of the request, in its order.
    pub held: storage_types::Exact<bool>,
    /// The catalog row of `queue_name`; `None` when it was not asked or is
    /// missing.
    pub queue: Option<QueueCatalogRecord>,
}

pub trait PubsubAttempt: Send {
    /// The protection that the attempt mode selects: `Conflict` for a write
    /// attempt, `Snapshot` for a read-only one.
    fn default_protection(&self) -> ReadProtection;

    fn read_topics(
        &mut self,
        arns: Vec<TopicArn>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<TopicRecord>>>> + Send;

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
        protection: ReadProtection,
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

    /// Pending (unconfirmed) subscriptions after `after` in the backend's
    /// pending-index order, read at `Snapshot`, at most `limit` of them.
    fn read_pending_subscription_page(
        &mut self,
        after: Option<&PendingSubscriptionCursor>,
        limit: NonZeroU32,
    ) -> impl Future<Output = StorageResult<PendingSubscriptionPage>> + Send;

    fn write_publication(
        &mut self,
        publication: AcceptedPublication,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn read_due_publish_chunks(
        &mut self,
        request: &PublishDueRequest,
    ) -> impl Future<Output = StorageResult<Vec<DueRow<PublishCandidate>>>> + Send;

    /// Move the due entry of an undecodable intent from `row.due_at` to
    /// `until`, so later intents are claimed. The intent bytes stay.
    fn move_publish_due(
        &mut self,
        row: &UndecodableRow,
        until: TimestampMillis,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// A conditional write: the stored intent must equal `expected`, else
    /// the attempt fails with a conflict.
    fn replace_publish_intent(
        &mut self,
        expected: PublishIntent,
        next: Option<PublishIntent>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Claims of intents read by `read_due_publish_chunks` in this attempt.
    fn replace_publish_intents(
        &mut self,
        replacements: Vec<PublishIntentClaim>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Insert payload and state rows; an existing id is a conflict (R1).
    fn store_new_deliveries(
        &mut self,
        rows: Vec<(DeliveryPayload, DeliveryState)>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// The due rows of each ask with their payloads, read with claim
    /// protection, and a `next_due` hint per ask (one statement batch or
    /// two read waves).
    fn scan_claimable(
        &mut self,
        scan: &ClaimScan,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<ClaimRows>>> + Send;

    /// Write the claimed state of rows that this attempt read with claim
    /// protection: `(previous, next)` pairs.
    fn store_claimed_states(
        &mut self,
        claims: Vec<(DeliveryState, DeliveryState)>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Fenced state writes; `false` where the fence does not hold (R4).
    fn store_delivery_states(
        &mut self,
        changes: Vec<StateChange>,
    ) -> impl Future<Output = StorageResult<Vec<bool>>> + Send;

    /// Fenced terminal deletes of state and payload, in the caller's order;
    /// `false` where the fence does not hold.
    fn delete_deliveries(
        &mut self,
        fences: Vec<DeliveryFence>,
    ) -> impl Future<Output = StorageResult<Vec<bool>>> + Send;

    /// Read every entry of `reads` in one native wave (W3 C8).
    fn load_records(
        &mut self,
        reads: RecordReads<'_>,
    ) -> impl Future<Output = StorageResult<RecordObservations>> + Send;

    /// Stage the terminal delete of state and payload for each fence that
    /// `load_records` read as held in THIS attempt. No I/O: the deletes go
    /// with the commit. A fence that no longer holds at commit is a native
    /// conflict (the state row was read with `Conflict`).
    fn stage_delivery_deletes(&mut self, fences: Vec<DeliveryFence>) -> StorageResult<()>;

    /// Move a state whose state or payload does not decode to `until`; the
    /// stored bytes stay.
    fn move_undecodable_state(
        &mut self,
        locator: &DeliveryLocator,
        until: TimestampMillis,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Delete the state row at `locator` (it must still be there, else a
    /// conflict) and the record's payload when no state row is left.
    fn delete_state_at(
        &mut self,
        locator: &DeliveryLocator,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// The oldest record of each destination after the cursor, read-only.
    fn scan_destination_heads(
        &mut self,
        after: &SweepCursor,
        limit: NonZeroU32,
    ) -> impl Future<Output = StorageResult<SweepPage>> + Send;

    /// The state and payload of one delivery (audit reads only).
    fn load_delivery(
        &mut self,
        id: &DeliveryRecordId,
    ) -> impl Future<Output = StorageResult<Option<(StateRow, PayloadRow)>>> + Send;

    /// The least due time of the publish-intent due index, read at
    /// `Snapshot`.
    fn read_next_publish_due(
        &mut self,
    ) -> impl Future<Output = StorageResult<Option<TimestampMillis>>> + Send;
}

/// One topic with the attempt's own protection.
pub async fn read_topic<A: PubsubAttempt>(
    attempt: &mut A,
    arn: &TopicArn,
) -> StorageResult<Option<TopicRecord>> {
    let protection = attempt.default_protection();
    read_topic_at(attempt, arn, protection).await
}

/// One topic with an explicit protection: a delivery settle reads it at
/// `Snapshot`, so a failing delivery does not lock the topic (design 7.5).
pub async fn read_topic_at<A: PubsubAttempt>(
    attempt: &mut A,
    arn: &TopicArn,
    protection: ReadProtection,
) -> StorageResult<Option<TopicRecord>> {
    let [topic] = attempt
        .read_topics(vec![arn.clone()], protection)
        .await?
        .into_array::<1>()?;
    Ok(topic)
}

/// One attempt can act on pubsub rows and on queue rows of the same store, so
/// an SNS delivery to a local queue is one native transaction (W3 design 5.1).
pub trait PubsubPort:
    storage_types::Clock + storage_types::AttemptDeadlineSource + Send + Sync
{
    type Attempt<'native>: PubsubAttempt + queue_provider::QueueAttempt
    where Self: 'native;

    /// One native attempt under this store value's admission lane.
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
            + 'static;

    /// Retry known aborts with a fresh try; return an unknown outcome
    /// unchanged. Never overridden.
    fn run<I, R, S>(
        &self,
        plan: AttemptPlan,
        input: Arc<I>,
        step: S,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        I: Send + Sync + 'static,
        R: Send + 'static,
        S: for<'native, 'borrow> Fn(
                &'borrow mut Self::Attempt<'native>,
                &'borrow I,
                AttemptTry,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Copy
            + Send
            + Sync
            + 'static,
    {
        storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            plan,
            move |attempt_request, attempt_try| {
                let input = Arc::clone(&input);
                self.with_attempt(attempt_request, move |attempt| {
                    Box::pin(async move { step(attempt, &input, attempt_try).await })
                })
            },
        )
    }
}
