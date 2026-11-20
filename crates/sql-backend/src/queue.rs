//! One SQL queue attempt shared by catalog and message operations.

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
    SqlAttempt, SqlBackend, SqlLayout, SqlStore,
    catalog::{SqlCatalogAttempt, SqlDynamoCount},
    queue_catalog,
    queue_catalog_check::{CatalogCheck, CheckedSqlAttempt},
    queue_message,
    queue_wake_publish::{StagedWakes, publish_after, store_staged},
};

fn stale_queue_catalog() -> StorageError {
    StorageError::logical_conflict("queue catalog hint is stale")
}

impl<A: SqlAttempt, L: SqlLayout> SqlCatalogAttempt<'_, A, L> {
    fn forget_queue_catalog(&self, name: &str) {
        if let Some((hints, _)) = &self.queue_catalog_hints {
            hints.forget(name);
        }
    }

    fn finish_checked_read<T>(
        &mut self,
        check: CatalogCheck,
        read: StorageResult<T>,
    ) -> StorageResult<T> {
        match check {
            CatalogCheck::None => read,
            CatalogCheck::Pending(expected) => {
                self.queue_catalog_check = Some(expected);
                read
            }
            CatalogCheck::Read { expected, current } => {
                if current.as_ref() == Some(&expected) {
                    read
                } else {
                    self.forget_queue_catalog(&expected.queue.queue_name);
                    Err(stale_queue_catalog())
                }
            }
        }
    }

    /// Reads a pending row that no message read verified. `Ok(false)`: the
    /// row differs and the hint is dropped.
    async fn verify_pending_queue_catalog(&mut self) -> StorageResult<bool> {
        let Some(expected) = self.queue_catalog_check.take() else {
            return Ok(true);
        };
        let current = queue_catalog::read_queue_by_name(
            self.attempt,
            self.engine,
            ReadProtection::Snapshot,
            &expected.queue.queue_name,
        )
        .await?;
        if current.as_ref() == Some(&expected) {
            return Ok(true);
        }
        self.forget_queue_catalog(&expected.queue.queue_name);
        Ok(false)
    }
}

impl<A: SqlAttempt, L: SqlLayout> QueueAttempt for SqlCatalogAttempt<'_, A, L> {
    fn read_queue_by_name(
        &mut self,
        name: &str,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Option<QueueCatalogRecord>>> + Send {
        queue_catalog::read_queue_by_name(self.attempt, self.engine, protection, name)
    }

    async fn read_queue_for_messages(
        &mut self,
        name: &str,
    ) -> StorageResult<Option<QueueCatalogRecord>> {
        let Some((hints, now)) = self.queue_catalog_hints.clone() else {
            return queue_catalog::read_queue_by_name(
                self.attempt,
                self.engine,
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
        let record = queue_catalog::read_queue_by_name(
            self.attempt,
            self.engine,
            ReadProtection::Snapshot,
            name,
        )
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
        queue_catalog::read_queues_by_name(self.attempt, self.engine, protection, names)
    }

    fn read_queue_page(
        &mut self,
        request: &QueueCatalogRequest,
    ) -> impl Future<Output = StorageResult<QueueCatalogPage>> + Send {
        queue_catalog::read_queue_page(self.attempt, self.engine, request)
    }

    fn write_queue_record(
        &mut self,
        record: QueueCatalogRecord,
        expected: Option<QueueIncarnation>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        self.forget_queue_catalog(&record.queue.queue_name);
        queue_catalog::write_queue_record(self.attempt, self.engine, record, expected)
    }

    fn delete_queue_record(
        &mut self,
        record: QueueCatalogRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        self.forget_queue_catalog(&record.queue.queue_name);
        queue_catalog::delete_queue_record(self.attempt, self.engine, record)
    }

    async fn read_due_messages(
        &mut self,
        request: &QueueDueRequest,
    ) -> StorageResult<QueueDueBatch> {
        let mut native = CheckedSqlAttempt::new(
            &mut *self.attempt,
            self.engine,
            self.queue_catalog_check.take(),
        );
        let read = queue_message::read_due_messages(&mut native, self.engine, request).await;
        let check = native.finish();
        self.finish_checked_read(check, read)
    }

    async fn read_due_head(
        &mut self,
        incarnation: QueueIncarnation,
        limit: NonZeroU32,
    ) -> StorageResult<Vec<QueueDueMark>> {
        let mut native = CheckedSqlAttempt::new(
            &mut *self.attempt,
            self.engine,
            self.queue_catalog_check.take(),
        );
        let read = queue_message::read_due_head(&mut native, self.engine, incarnation, limit).await;
        let check = native.finish();
        self.finish_checked_read(check, read)
    }

    async fn read_messages_by_receipt(
        &mut self,
        incarnation: QueueIncarnation,
        receipts: Vec<ReceiptHandle>,
    ) -> StorageResult<storage_types::Exact<Option<QueueMessageState>>> {
        let mut native = CheckedSqlAttempt::new(
            &mut *self.attempt,
            self.engine,
            self.queue_catalog_check.take(),
        );
        let read = queue_message::read_messages_by_receipt(
            &mut native,
            self.engine,
            incarnation,
            receipts,
        )
        .await;
        let check = native.finish();
        self.finish_checked_read(check, read)
    }

    async fn read_messages_by_id(
        &mut self,
        incarnation: QueueIncarnation,
        ids: Vec<MessageId>,
    ) -> StorageResult<storage_types::Exact<Option<QueueMessageRecord>>> {
        let mut native = CheckedSqlAttempt::new(
            &mut *self.attempt,
            self.engine,
            self.queue_catalog_check.take(),
        );
        let read =
            queue_message::read_messages_by_id(&mut native, self.engine, incarnation, ids).await;
        let check = native.finish();
        self.finish_checked_read(check, read)
    }

    async fn read_message_states_by_id(
        &mut self,
        incarnation: QueueIncarnation,
        ids: Vec<MessageId>,
    ) -> StorageResult<storage_types::Exact<Option<QueueMessageState>>> {
        let mut native = CheckedSqlAttempt::new(
            &mut *self.attempt,
            self.engine,
            self.queue_catalog_check.take(),
        );
        let read =
            queue_message::read_message_states_by_id(&mut native, self.engine, incarnation, ids)
                .await;
        let check = native.finish();
        self.finish_checked_read(check, read)
    }

    async fn insert_messages(&mut self, records: Vec<QueueMessageRecord>) -> StorageResult<()> {
        // A pubsub attempt on Postgres carries the message writes with
        // its COMMIT (W3 C8); every other attempt writes them now.
        if self.carries_writes_to_commit() {
            for statement in queue_message::insert_statements(self.engine, records)? {
                self.stage_pubsub_write(statement, SqlDynamoCount::ExactlyOne)?;
            }
            return Ok(());
        }
        queue_message::insert_messages(self.attempt, self.engine, records).await
    }

    async fn write_message_states(
        &mut self,
        changes: Vec<(QueueMessageState, QueueMessageState)>,
    ) -> StorageResult<()> {
        queue_message::write_message_states(self.attempt, self.engine, changes).await
    }

    async fn delete_messages(&mut self, states: Vec<QueueMessageState>) -> StorageResult<()> {
        queue_message::delete_messages(self.attempt, self.engine, states).await
    }

    async fn count_queue_messages(
        &mut self,
        incarnation: QueueIncarnation,
        captured_at: TimestampMillis,
        scan_limit: NonZeroU32,
    ) -> StorageResult<QueueMessageCounts> {
        let mut native = CheckedSqlAttempt::new(
            &mut *self.attempt,
            self.engine,
            self.queue_catalog_check.take(),
        );
        let read = queue_message::count_queue_messages(
            &mut native,
            self.engine,
            incarnation,
            captured_at,
            scan_limit,
        )
        .await;
        let check = native.finish();
        self.finish_checked_read(check, read)
    }

    fn read_message_cleanup_page(
        &mut self,
        request: &QueueCleanupRequest,
    ) -> impl Future<Output = StorageResult<QueueCleanupPage>> + Send {
        queue_message::read_message_cleanup_page(self.attempt, self.engine, request)
    }

    fn stage_queue_wake(&mut self, slot: QueueWakeSlot) -> StorageResult<()> {
        self.queue_wakes.insert(slot);
        Ok(())
    }
}

impl<B: SqlBackend + 'static> QueuePort for SqlStore<B> {
    type Attempt<'native>
        = SqlCatalogAttempt<'native, B::Attempt>
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
        let engine = self.backend.engine();
        let hints = Arc::clone(&self.queue_catalog_hints);
        let clock = Arc::clone(&self.clock);
        let board = self.queue_wake.clone();
        let backend = Arc::clone(&self.backend);
        let staged = StagedWakes::default();
        let body_staged = Arc::clone(&staged);
        let attempt = async move {
            backend
                .with_attempt(request, move |native| {
                    Box::pin(async move {
                        let mut attempt = SqlCatalogAttempt::new(native, engine, request);
                        attempt.queue_catalog_hints = Some((hints, clock.now()));
                        let result = step(&mut attempt).await;
                        match (result, attempt.verify_pending_queue_catalog().await) {
                            (_, Ok(false)) => Err(stale_queue_catalog()),
                            (Ok(result), Ok(true)) => {
                                store_staged(&body_staged, attempt.queue_wakes.take());
                                Ok(result)
                            }
                            (Ok(_), Err(error)) | (Err(error), _) => Err(error),
                        }
                    })
                })
                .await
        };
        self.admission
            .settle(request, publish_after(board, staged, attempt))
    }
}
