//! One KV queue attempt shared by catalog and message operations.

use std::future::Future;

use futures::future::BoxFuture;
use queue_provider::{
    MessageId, QueueAttempt, QueueCatalogPage, QueueCatalogRecord, QueueCatalogRequest,
    QueueCleanupPage, QueueCleanupRequest, QueueDueRequest, QueueIncarnation, QueueMessageCounts,
    QueueMessageRecord, QueuePort, ReceiptHandle,
};
use storage_types::{AttemptMode, StorageResult, TimestampMillis};

use crate::{
    KvAttempt, KvBackend, KvStore, catalog::KvCatalogAttempt, queue_catalog, queue_message,
};

impl<A: KvAttempt> QueueAttempt for KvCatalogAttempt<'_, A> {
    fn read_queue_by_name(
        &mut self,
        name: &str,
    ) -> impl Future<Output = StorageResult<Option<QueueCatalogRecord>>> + Send {
        queue_catalog::read_queue_by_name(self.attempt, self.protection, name)
    }

    fn read_queues_by_url(
        &mut self,
        urls: Vec<String>,
    ) -> impl Future<Output = StorageResult<Vec<Option<QueueCatalogRecord>>>> + Send {
        queue_catalog::read_queues_by_url(self.attempt, self.protection, urls)
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
        queue_catalog::write_queue_record(self.attempt, record, expected)
    }

    fn delete_queue_record(
        &mut self,
        record: QueueCatalogRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        queue_catalog::delete_queue_record(self.attempt, record)
    }

    fn read_due_messages(
        &mut self,
        request: &QueueDueRequest,
    ) -> impl Future<Output = StorageResult<Vec<QueueMessageRecord>>> + Send {
        queue_message::read_due_messages(self.attempt, request)
    }

    fn read_messages_by_receipt(
        &mut self,
        incarnation: QueueIncarnation,
        receipts: Vec<ReceiptHandle>,
    ) -> impl Future<Output = StorageResult<Vec<Option<QueueMessageRecord>>>> + Send {
        queue_message::read_messages_by_receipt(self.attempt, incarnation, receipts)
    }

    fn read_messages_by_id(
        &mut self,
        incarnation: QueueIncarnation,
        ids: Vec<MessageId>,
    ) -> impl Future<Output = StorageResult<Vec<Option<QueueMessageRecord>>>> + Send {
        queue_message::read_messages_by_id(self.attempt, incarnation, ids)
    }

    fn write_message_records(
        &mut self,
        records: Vec<(QueueMessageRecord, Option<QueueMessageRecord>)>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        queue_message::write_message_records(self.attempt, records)
    }

    fn delete_message_records(
        &mut self,
        records: Vec<QueueMessageRecord>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        queue_message::delete_message_records(self.attempt, records)
    }

    fn count_queue_messages(
        &mut self,
        incarnation: QueueIncarnation,
        captured_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<QueueMessageCounts>> + Send {
        queue_message::count_queue_messages(self.attempt, incarnation, captured_at)
    }

    fn read_message_cleanup_page(
        &mut self,
        request: &QueueCleanupRequest,
    ) -> impl Future<Output = StorageResult<QueueCleanupPage>> + Send {
        queue_message::read_message_cleanup_page(self.attempt, request)
    }
}

impl<B: KvBackend + 'static> QueuePort for KvStore<B> {
    type Attempt<'native>
        = KvCatalogAttempt<'native, B::Attempt>
    where Self: 'native;

    fn with_queue_attempt<R, F>(
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
        self.backend.with_attempt(mode, move |native| {
            Box::pin(async move {
                let mut attempt = KvCatalogAttempt::new(native, mode.read_protection());
                operation(&mut attempt).await
            })
        })
    }
}
