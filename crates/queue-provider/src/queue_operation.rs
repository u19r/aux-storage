//! Queue actions consume the original catalog observation and native-attempt
//! borrow.

use std::collections::HashMap;

use storage_types::{ReadProtection, StorageResult, TimestampMillis};

use crate::{QueueAttempt, QueueCatalogRecord, QueueIncarnation};

/// The protected catalog observation retains its original native attempt.
/// External source authority must be established by the caller above this
/// owner.
pub struct QueueOperation<'native, A> {
    pub(super) attempt: &'native mut A,
    pub(super) record: QueueCatalogRecord,
}

impl<'native, A: QueueAttempt> QueueOperation<'native, A> {
    /// A catalog change reads the record with conflict protection.
    pub async fn load(attempt: &'native mut A, queue_url: &str) -> StorageResult<Option<Self>> {
        Self::load_with(attempt, queue_url, ReadProtection::Conflict).await
    }

    /// A message operation reads the record without a lock (decision D1). The
    /// incarnation is the fence: purge and delete install a new one that never
    /// becomes current again, and every message row key contains it.
    /// The backend may answer from a catalog hint (W7 P7).
    pub async fn load_for_messages(
        attempt: &'native mut A,
        queue_url: &str,
    ) -> StorageResult<Option<Self>> {
        let Some(record) = attempt
            .read_queue_for_messages(crate::queue_catalog::queue_name_of(queue_url))
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(Self { attempt, record }))
    }

    /// A message operation of another domain on the same store (an SNS
    /// delivery, W3 design 5.2) knows the queue by name from its ARN. The
    /// catalog read has no lock, as in `load_for_messages`.
    pub async fn load_by_name_for_messages(
        attempt: &'native mut A,
        queue_name: &str,
    ) -> StorageResult<Option<Self>> {
        let Some(record) = attempt
            .read_queue_by_name(queue_name, ReadProtection::Snapshot)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(Self { attempt, record }))
    }

    /// A message operation of another domain that read the catalog row at
    /// `Snapshot` in its own read wave (an SNS local delivery, W3 C8). The
    /// record must come from this attempt; the incarnation is the fence, as
    /// in `load_for_messages`.
    #[must_use]
    pub fn for_messages(attempt: &'native mut A, record: QueueCatalogRecord) -> Self {
        Self { attempt, record }
    }

    /// The URL of the loaded queue, for messages that address it.
    #[must_use]
    pub fn queue_url(&self) -> &str {
        &self.record.queue.queue_url
    }

    async fn load_with(
        attempt: &'native mut A,
        queue_url: &str,
        protection: ReadProtection,
    ) -> StorageResult<Option<Self>> {
        let Some(record) = crate::read_queue_by_url(attempt, queue_url, protection).await? else {
            return Ok(None);
        };
        Ok(Some(Self { attempt, record }))
    }

    /// The original native identity for higher-level source binding.
    pub const fn incarnation(&self) -> QueueIncarnation {
        self.record.incarnation
    }

    pub async fn delete(self) -> StorageResult<()> {
        let slot = crate::QueueWakeSlot::of_queue(&self.record.queue.queue_name);
        self.attempt.stage_queue_wake(slot)?;
        self.attempt.delete_queue_record(self.record).await
    }

    /// The caller captures a fresh next incarnation before execution/retry.
    pub async fn purge(mut self, next: QueueIncarnation) -> StorageResult<()> {
        let previous = self.record.incarnation;
        self.record.incarnation = next;
        let slot = crate::QueueWakeSlot::of_queue(&self.record.queue.queue_name);
        self.attempt.stage_queue_wake(slot)?;
        self.attempt
            .write_queue_record(self.record, Some(previous))
            .await
    }

    pub async fn set_attributes(
        mut self,
        attributes: HashMap<String, String>,
        updated_at: TimestampMillis,
    ) -> StorageResult<()> {
        self.record.queue.updated_at = updated_at;
        let incarnation = self.record.incarnation;
        // SQS updates only the supplied attributes on the protected
        // observation. An empty value removes the attribute, so the queue
        // uses its default again. Request validation accepts an empty value
        // only for `RedrivePolicy` and `Policy`; a direct provider caller that
        // passes another empty value also removes that attribute.
        for (name, value) in attributes {
            if value.is_empty() {
                self.record.queue.attributes.remove(&name);
            } else {
                self.record.queue.attributes.insert(name, value);
            }
        }
        crate::queue_catalog::queue_settings_size(&self.record.queue)?;
        let slot = crate::QueueWakeSlot::of_queue(&self.record.queue.queue_name);
        self.attempt.stage_queue_wake(slot)?;
        self.attempt
            .write_queue_record(self.record, Some(incarnation))
            .await
    }
}
