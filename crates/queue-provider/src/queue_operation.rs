//! Queue actions consume the original catalog observation and native-attempt
//! borrow.

use std::collections::HashMap;

use storage_types::StorageResult;

use crate::{QueueAttempt, QueueCatalogRecord, QueueIncarnation};

/// The protected catalog observation retains its original native attempt.
/// External source authority must be established by the caller above this
/// owner.
pub struct QueueOperation<'native, A> {
    pub(super) attempt: &'native mut A,
    pub(super) record: QueueCatalogRecord,
}

impl<'native, A: QueueAttempt> QueueOperation<'native, A> {
    pub async fn load(attempt: &'native mut A, queue_url: &str) -> StorageResult<Option<Self>> {
        let Some(record) = crate::read_queue_by_url(attempt, queue_url).await? else {
            return Ok(None);
        };
        Ok(Some(Self { attempt, record }))
    }

    /// The original native identity for higher-level source binding.
    pub const fn incarnation(&self) -> QueueIncarnation {
        self.record.incarnation
    }

    pub async fn delete(self) -> StorageResult<()> {
        self.attempt.delete_queue_record(self.record).await
    }

    /// The caller captures a fresh next incarnation before execution/retry.
    pub async fn purge(mut self, next: QueueIncarnation) -> StorageResult<()> {
        let previous = self.record.incarnation;
        self.record.incarnation = next;
        self.attempt
            .write_queue_record(self.record, Some(previous))
            .await
    }

    pub async fn set_attributes(
        mut self,
        attributes: HashMap<String, String>,
    ) -> StorageResult<()> {
        let incarnation = self.record.incarnation;
        // SQS updates only the supplied attributes on the protected
        // observation.
        self.record.queue.attributes.extend(attributes);
        self.attempt
            .write_queue_record(self.record, Some(incarnation))
            .await
    }
}
