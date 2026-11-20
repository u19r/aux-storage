//! One KV queue attempt shared by catalog and message operations.

use std::{future::Future, num::NonZeroU32, sync::Arc};

use futures::future::BoxFuture;
use queue_provider::{
    MessageId, QueueAttempt, QueueCatalogPage, QueueCatalogRecord, QueueCatalogRequest,
    QueueCleanupPage, QueueCleanupRequest, QueueDueBatch, QueueDueMark, QueueDueRequest,
    QueueIncarnation, QueueMessageCounts, QueueMessageRecord, QueueMessageState, QueuePort,
    QueueWakeBoard, QueueWakeSlot, ReceiptHandle,
};
use storage_types::{AttemptRequest, ReadProtection, StorageError, StorageResult, TimestampMillis};

use crate::{
    KvAttempt, KvBackend, KvStore,
    catalog::KvCatalogAttempt,
    queue_catalog,
    queue_message::{self, Checked},
};

pub(crate) fn store_wake_slots(
    cell: &std::sync::Mutex<Vec<QueueWakeSlot>>,
    slots: Vec<QueueWakeSlot>,
) {
    *cell
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = slots;
}

/// Run the native attempt, then publish its wake slots. On a board that
/// records commits the attempt and the publication run in one spawned task,
/// so a dropped caller or a commit-unknown outcome still reaches the board;
/// otherwise (no board, a native feed, no runtime) the caller's future runs
/// them. An extra advance after commit-unknown is only a probe.
pub(crate) async fn run_and_publish<B, R, Fut>(
    backend: Arc<B>,
    board: Option<Arc<QueueWakeBoard>>,
    start: impl FnOnce(Arc<B>, Arc<std::sync::Mutex<Vec<QueueWakeSlot>>>) -> Fut + Send + 'static,
) -> StorageResult<R>
where
    B: Send + Sync + 'static,
    R: Send + 'static,
    Fut: Future<Output = StorageResult<R>> + Send + 'static,
{
    let slots = Arc::new(std::sync::Mutex::new(Vec::new()));
    let shared = Arc::clone(&slots);
    let publish = board.clone();
    let run = async move {
        let outcome = start(backend, shared).await;
        if let Some(board) = publish
            && match &outcome {
                Ok(_) => true,
                Err(error) => error.is_commit_unknown(),
            }
        {
            let slots = slots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            board.committed(&slots);
        }
        outcome
    };
    let spawn = board.as_ref().is_some_and(|board| board.records_commits())
        && tokio::runtime::Handle::try_current().is_ok();
    if !spawn {
        return run.await;
    }
    match tokio::spawn(run).await {
        Ok(outcome) => outcome,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(StorageError::internal(&error)),
    }
}

pub(crate) fn stale_queue_catalog() -> StorageError {
    StorageError::logical_conflict("queue catalog hint is stale")
}

/// See `KvCatalogAttempt::queue_absence`.
pub(crate) struct CheckedAbsence {
    pub(crate) incarnation: QueueIncarnation,
    pub(crate) present: Vec<(MessageId, bool)>,
}

/// The hinted row, read with no lock in the same wave as a message read;
/// nothing is read without a pending row.
async fn read_checked_row<A: KvAttempt>(
    native: &A,
    check: Option<&QueueCatalogRecord>,
) -> StorageResult<Option<QueueCatalogRecord>> {
    match check {
        Some(expected) => {
            queue_catalog::read_queue_by_name(
                native,
                ReadProtection::Snapshot,
                &expected.queue.queue_name,
            )
            .await
        }
        None => Ok(None),
    }
}

impl<A: KvAttempt> KvCatalogAttempt<'_, A> {
    fn forget_queue_catalog(&self, name: &str) {
        if let Some((hints, _)) = &self.queue_catalog_hints {
            hints.forget(name);
        }
    }

    /// Replaces the hint by the row this attempt read (absent: removes it).
    fn refresh_queue_catalog(&self, name: &str, current: Option<&QueueCatalogRecord>) {
        if let Some((hints, now)) = &self.queue_catalog_hints {
            hints.observe(name, current, *now);
        }
    }

    /// The result of a first message read that carried the pending row's
    /// check (W7-H). A moved row refreshes the hint and fails the attempt
    /// with a transaction conflict; `QueuePort::run` retries it, and the
    /// refreshed hint serves the retry.
    fn finish_merged_read<T>(
        &self,
        check: Option<&QueueCatalogRecord>,
        read: StorageResult<Checked<T>>,
    ) -> StorageResult<T> {
        match read? {
            Checked::Read(value) => Ok(value),
            Checked::Moved(current) => {
                let expected = check
                    .ok_or_else(|| StorageError::internal("KV queue read moved without a check"))?;
                self.refresh_queue_catalog(&expected.queue.queue_name, current.as_ref());
                Err(stale_queue_catalog())
            }
        }
    }

    /// The result of a read that ran with the pending row's read.
    fn finish_checked_read<T>(
        &self,
        check: Option<QueueCatalogRecord>,
        current: StorageResult<Option<QueueCatalogRecord>>,
        read: StorageResult<T>,
    ) -> StorageResult<T> {
        let Some(expected) = check else {
            return read;
        };
        let current = current?;
        if current.as_ref() != Some(&expected) {
            self.refresh_queue_catalog(&expected.queue.queue_name, current.as_ref());
            return Err(stale_queue_catalog());
        }
        read
    }

    /// Reads a pending row that no message read verified. `Ok(false)`: the
    /// row differs and the hint is dropped.
    pub(crate) async fn verify_pending_queue_catalog(&mut self) -> StorageResult<bool> {
        let Some(expected) = self.queue_catalog_check.take() else {
            return Ok(true);
        };
        let current = read_checked_row(&*self.attempt, Some(&expected)).await?;
        if current.as_ref() == Some(&expected) {
            return Ok(true);
        }
        self.refresh_queue_catalog(&expected.queue.queue_name, current.as_ref());
        Ok(false)
    }
}

impl<A: KvAttempt> QueueAttempt for KvCatalogAttempt<'_, A> {
    fn read_queue_by_name(
        &mut self,
        name: &str,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Option<QueueCatalogRecord>>> + Send {
        queue_catalog::read_queue_by_name(self.attempt, protection, name)
    }

    async fn read_queue_for_messages(
        &mut self,
        name: &str,
    ) -> StorageResult<Option<QueueCatalogRecord>> {
        let Some((hints, now)) = self.queue_catalog_hints.clone() else {
            return queue_catalog::read_queue_by_name(
                &*self.attempt,
                ReadProtection::Snapshot,
                name,
            )
            .await;
        };
        if self.queue_catalog_check.is_none()
            && let Some(record) = hints.get(name, now)
        {
            self.queue_catalog_check = Some(record.clone());
            return Ok(Some(record));
        }
        let record =
            queue_catalog::read_queue_by_name(&*self.attempt, ReadProtection::Snapshot, name)
                .await?;
        hints.observe(name, record.as_ref(), now);
        Ok(record)
    }

    fn read_queues_by_name(
        &mut self,
        names: Vec<String>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<QueueCatalogRecord>>>> + Send
    {
        queue_catalog::read_queues_by_name(self.attempt, protection, names)
    }

    fn read_queue_page(
        &mut self,
        request: &QueueCatalogRequest,
    ) -> impl Future<Output = StorageResult<QueueCatalogPage>> + Send {
        queue_catalog::read_queue_page(self.attempt, request)
    }

    fn write_queue_record(
        &mut self,
        record: QueueCatalogRecord,
        expected: Option<QueueIncarnation>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        self.forget_queue_catalog(&record.queue.queue_name);
        queue_catalog::write_queue_record(self.attempt, record, expected)
    }

    fn delete_queue_record(
        &mut self,
        record: QueueCatalogRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        self.forget_queue_catalog(&record.queue.queue_name);
        queue_catalog::delete_queue_record(self.attempt, record)
    }

    async fn read_due_messages(
        &mut self,
        request: &QueueDueRequest,
    ) -> StorageResult<QueueDueBatch> {
        let check = self.queue_catalog_check.take();
        let read = queue_message::read_due_messages(&*self.attempt, request, check.as_ref()).await;
        self.finish_merged_read(check.as_ref(), read)
    }

    async fn read_due_head(
        &mut self,
        incarnation: QueueIncarnation,
        limit: NonZeroU32,
    ) -> StorageResult<Vec<QueueDueMark>> {
        let check = self.queue_catalog_check.take();
        let read =
            queue_message::read_due_head(&*self.attempt, incarnation, limit, check.as_ref()).await;
        self.finish_merged_read(check.as_ref(), read)
    }

    async fn read_messages_by_receipt(
        &mut self,
        incarnation: QueueIncarnation,
        receipts: Vec<ReceiptHandle>,
    ) -> StorageResult<storage_types::Exact<Option<QueueMessageState>>> {
        let check = self.queue_catalog_check.take();
        let read = queue_message::read_messages_by_receipt(
            &*self.attempt,
            incarnation,
            receipts,
            check.as_ref(),
        )
        .await;
        self.finish_merged_read(check.as_ref(), read)
    }

    async fn read_messages_by_id(
        &mut self,
        incarnation: QueueIncarnation,
        ids: Vec<MessageId>,
    ) -> StorageResult<storage_types::Exact<Option<QueueMessageRecord>>> {
        let check = self.queue_catalog_check.take();
        let read =
            queue_message::read_messages_by_id(&*self.attempt, incarnation, ids, check.as_ref())
                .await;
        self.finish_merged_read(check.as_ref(), read)
    }

    async fn read_message_states_by_id(
        &mut self,
        incarnation: QueueIncarnation,
        ids: Vec<MessageId>,
    ) -> StorageResult<storage_types::Exact<Option<QueueMessageState>>> {
        let check = self.queue_catalog_check.take();
        let native = &*self.attempt;
        let (current, read) = futures::join!(
            read_checked_row(native, check.as_ref()),
            queue_message::read_message_states_by_id(native, incarnation, ids),
        );
        self.finish_checked_read(check, current, read)
    }

    async fn insert_messages(&mut self, records: Vec<QueueMessageRecord>) -> StorageResult<()> {
        let absence = self.queue_absence.take();
        if records.is_empty() {
            // A pending check stays for the next read or the end of the
            // attempt.
            return Ok(());
        }
        let check = self.queue_catalog_check.take();
        if check.is_none()
            && let Some(absence) = absence
            && let Some(answer) =
                queue_message::checked_absence(&records, absence.incarnation, &absence.present)
        {
            answer?;
            return queue_message::stage_inserts(self.attempt, records);
        }
        let absent = queue_message::require_absent(&*self.attempt, &records, check.as_ref()).await;
        self.finish_merged_read(check.as_ref(), absent)?;
        queue_message::stage_inserts(self.attempt, records)
    }

    fn write_message_states(
        &mut self,
        changes: Vec<(QueueMessageState, QueueMessageState)>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        std::future::ready(queue_message::write_message_states(self.attempt, changes))
    }

    fn delete_messages(
        &mut self,
        states: Vec<QueueMessageState>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        std::future::ready(queue_message::delete_messages(self.attempt, states))
    }

    async fn count_queue_messages(
        &mut self,
        incarnation: QueueIncarnation,
        captured_at: TimestampMillis,
        scan_limit: NonZeroU32,
    ) -> StorageResult<QueueMessageCounts> {
        let check = self.queue_catalog_check.take();
        let native = &*self.attempt;
        let (current, read) = futures::join!(
            read_checked_row(native, check.as_ref()),
            queue_message::count_queue_messages(native, incarnation, captured_at, scan_limit),
        );
        self.finish_checked_read(check, current, read)
    }

    fn read_message_cleanup_page(
        &mut self,
        request: &QueueCleanupRequest,
    ) -> impl Future<Output = StorageResult<QueueCleanupPage>> + Send {
        queue_message::read_message_cleanup_page(self.attempt, request)
    }

    fn stage_queue_wake(&mut self, slot: QueueWakeSlot) -> StorageResult<()> {
        if !self.queue_wakes.insert(slot) {
            return Ok(());
        }
        if !matches!(
            self.attempt.backend_engine(),
            storage_common::diagnostics::NativeEngine::FoundationDb
        ) {
            return Ok(());
        }
        // Blind atomic write: no read, no read conflict; the stamp makes every
        // commit a new value, so a watch can never miss an A-B-A change.
        self.attempt.native_stage_versionstamped_value(
            crate::queue_wake_layout::queue_wake_key(slot),
            vec![0; 10],
            0,
        )
    }
}

impl<B: KvBackend + 'static> QueuePort for KvStore<B> {
    type Attempt<'native>
        = KvCatalogAttempt<'native, B::Attempt>
    where Self: 'native;

    fn queue_wake_board(&self) -> Option<Arc<QueueWakeBoard>> {
        self.queue_wake.clone()
    }

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
        let attempt = run_and_publish(
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
                            // A row that no read verified is read before the
                            // attempt returns; a stale row overrides the
                            // operation's outcome.
                            match (result, attempt.verify_pending_queue_catalog().await) {
                                (_, Ok(false)) => Err(stale_queue_catalog()),
                                (Ok(result), Ok(true)) => {
                                    store_wake_slots(&slots, attempt.queue_wakes.take());
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
