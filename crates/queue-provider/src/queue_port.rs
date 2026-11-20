//! Native-attempt capabilities lent to shared queue decisions.

use std::{future::Future, num::NonZeroU32, sync::Arc};

use futures::future::BoxFuture;
use storage_types::{
    AttemptPlan, AttemptRequest, AttemptTry, Clock, ReadProtection, StorageResult, TimestampMillis,
};

use crate::{
    MessageId, QueueCatalogPage, QueueCatalogRecord, QueueCatalogRequest, QueueCleanupPage,
    QueueCleanupRequest, QueueDueBatch, QueueDueMark, QueueDueRequest, QueueIncarnation,
    QueueMessageCounts, QueueMessageRecord, QueueMessageState, QueueWakeBoard, QueueWakeSlot,
    ReceiptHandle,
};

pub trait QueueAttempt: Send {
    /// Message operations read the catalog with `Snapshot`: the incarnation
    /// is their fence. Catalog changes read it with `Conflict`.
    fn read_queue_by_name(
        &mut self,
        name: &str,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Option<QueueCatalogRecord>>> + Send;

    /// The catalog row of a message operation, read without a lock (decision
    /// D1). A backend may answer from a node-local hint and read the row with
    /// its next message read; it reads the row before the attempt returns and
    /// fails the attempt with a transaction conflict when the row differs
    /// (W7 P7). Only callers inside `retry_native_conflicts` use it.
    fn read_queue_for_messages(
        &mut self,
        name: &str,
    ) -> impl Future<Output = StorageResult<Option<QueueCatalogRecord>>> + Send {
        self.read_queue_by_name(name, ReadProtection::Snapshot)
    }

    /// The rows of `names` in input order.
    fn read_queues_by_name(
        &mut self,
        names: Vec<String>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<QueueCatalogRecord>>>> + Send;

    fn read_queue_page(
        &mut self,
        request: &QueueCatalogRequest,
    ) -> impl Future<Output = StorageResult<QueueCatalogPage>> + Send;

    fn write_queue_record(
        &mut self,
        record: QueueCatalogRecord,
        expected: Option<QueueIncarnation>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn delete_queue_record(
        &mut self,
        record: QueueCatalogRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Read the rows that `request` selects with claim protection, in
    /// `(visible_at, id)` order or in any order after a lock re-check, plus
    /// the observations that `request` turns into `drained` and `next_due`.
    fn read_due_messages(
        &mut self,
        request: &QueueDueRequest,
    ) -> impl Future<Output = StorageResult<QueueDueBatch>> + Send;

    /// The first `limit` visibility marks of this incarnation in
    /// `(visible_at, id)` order, read without a conflict dependency.
    fn read_due_head(
        &mut self,
        incarnation: QueueIncarnation,
        limit: NonZeroU32,
    ) -> impl Future<Output = StorageResult<Vec<QueueDueMark>>> + Send;

    /// States of the messages that hold `receipts`, in input order. No body is
    /// read.
    fn read_messages_by_receipt(
        &mut self,
        incarnation: QueueIncarnation,
        receipts: Vec<ReceiptHandle>,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<QueueMessageState>>>> + Send;

    /// Protected point reads of state and body (publication identity), in
    /// input order, explicit missing rows; one read call.
    fn read_messages_by_id(
        &mut self,
        incarnation: QueueIncarnation,
        ids: Vec<MessageId>,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<QueueMessageRecord>>>> + Send;

    /// Protected point reads of states only, in input order.
    fn read_message_states_by_id(
        &mut self,
        incarnation: QueueIncarnation,
        ids: Vec<MessageId>,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<QueueMessageState>>>> + Send;

    /// Writes the body and the state of new messages. Each message must be
    /// absent (a transaction conflict otherwise). A message deleted earlier in
    /// this attempt counts as absent.
    fn insert_messages(
        &mut self,
        records: Vec<QueueMessageRecord>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// `(new, previous)`: replaces each state. `previous` must be a state that
    /// this attempt read with conflict protection (`read_due_messages`,
    /// `read_messages_by_receipt`, `read_messages_by_id`,
    /// `read_message_states_by_id`, `read_message_cleanup_page`); a backend may
    /// write without reading again (KV) or compare in its statement (SQL). A
    /// batch must not repeat a message (a transaction conflict). The body is
    /// not read or written.
    fn write_message_states(
        &mut self,
        changes: Vec<(QueueMessageState, QueueMessageState)>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Deletes state and body of each message. Each state must be one that this
    /// attempt read with conflict protection (as for `write_message_states`). A
    /// batch must not repeat a message. The body is not read.
    fn delete_messages(
        &mut self,
        states: Vec<QueueMessageState>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Count each of the regions `visible_at <= captured_at` and
    /// `visible_at > captured_at` up to `scan_limit` entries.
    fn count_queue_messages(
        &mut self,
        incarnation: QueueIncarnation,
        captured_at: TimestampMillis,
        scan_limit: NonZeroU32,
    ) -> impl Future<Output = StorageResult<QueueMessageCounts>> + Send;

    fn read_message_cleanup_page(
        &mut self,
        request: &QueueCleanupRequest,
    ) -> impl Future<Output = StorageResult<QueueCleanupPage>> + Send;

    /// Stage a wake of `slot` for this attempt; no I/O. A store shared by
    /// processes writes it natively in this attempt (FoundationDB); every
    /// store publishes the staged slots to its board after commit.
    fn stage_queue_wake(&mut self, slot: QueueWakeSlot) -> StorageResult<()>;
}

/// One customer queue operation owns one native attempt.
pub trait QueuePort: Clock + storage_types::AttemptDeadlineSource + Send + Sync {
    type Attempt<'native>: QueueAttempt
    where Self: 'native;

    /// Largest clock difference between the nodes that share this store
    /// (decision D14); a lease ends this much later for a new claim.
    fn max_clock_skew_millis(&self) -> i64 {
        crate::constants::DEFAULT_MAX_CLOCK_SKEW_MILLIS
    }

    /// The wake board of this store in this process. `None`: waiting
    /// receives probe every probe interval.
    fn queue_wake_board(&self) -> Option<Arc<QueueWakeBoard>> {
        None
    }

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
