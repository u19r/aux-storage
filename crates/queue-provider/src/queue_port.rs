//! Native-attempt capabilities lent to shared queue decisions.

use std::future::Future;

use futures::future::BoxFuture;
use storage_types::{AttemptMode, Clock, StorageResult, TimestampMillis};

use crate::{
    MessageId, QueueCatalogPage, QueueCatalogRecord, QueueCatalogRequest, QueueCleanupPage,
    QueueCleanupRequest, QueueDueRequest, QueueIncarnation, QueueMessageCounts, QueueMessageRecord,
    ReceiptHandle,
};

pub trait QueueAttempt: Send {
    fn read_queue_by_name(
        &mut self,
        name: &str,
    ) -> impl Future<Output = StorageResult<Option<QueueCatalogRecord>>> + Send;

    fn read_queues_by_url(
        &mut self,
        urls: Vec<String>,
    ) -> impl Future<Output = StorageResult<Vec<Option<QueueCatalogRecord>>>> + Send;

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

    fn read_due_messages(
        &mut self,
        request: &QueueDueRequest,
    ) -> impl Future<Output = StorageResult<Vec<QueueMessageRecord>>> + Send;

    fn read_messages_by_receipt(
        &mut self,
        incarnation: QueueIncarnation,
        receipts: Vec<ReceiptHandle>,
    ) -> impl Future<Output = StorageResult<Vec<Option<QueueMessageRecord>>>> + Send;

    /// Protected point reads preserve input order and explicit missing rows.
    fn read_messages_by_id(
        &mut self,
        incarnation: QueueIncarnation,
        ids: Vec<MessageId>,
    ) -> impl Future<Output = StorageResult<Vec<Option<QueueMessageRecord>>>> + Send;

    fn write_message_records(
        &mut self,
        records: Vec<(QueueMessageRecord, Option<QueueMessageRecord>)>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn delete_message_records(
        &mut self,
        records: Vec<QueueMessageRecord>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn count_queue_messages(
        &mut self,
        incarnation: QueueIncarnation,
        captured_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<QueueMessageCounts>> + Send;

    fn read_message_cleanup_page(
        &mut self,
        request: &QueueCleanupRequest,
    ) -> impl Future<Output = StorageResult<QueueCleanupPage>> + Send;
}

/// One customer queue operation owns one native attempt.
pub trait QueuePort: Clock + Send + Sync {
    type Attempt<'native>: QueueAttempt
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
            + 'static;
}
