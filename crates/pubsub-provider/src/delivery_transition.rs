//! Shared delivery transitions and selected effects in the caller's native
//! attempt.

use storage_types::{StorageError, StorageResult};

use crate::{
    ClaimDeliveryRecordsRequest, DeliveryRecord, DeliveryRecordUpdate, PubsubAttempt,
    delivery_operations::one_delivery,
};

impl DeliveryRecord {
    /// A claim changes ownership, never the accepted delivery payload.
    pub fn claim(&self, request: &ClaimDeliveryRecordsRequest) -> StorageResult<Option<Self>> {
        if request.owner.is_empty() || request.lease_expires_at <= request.now {
            return Err(StorageError::internal("delivery claim has invalid lease"));
        }
        if !self.is_claimable(request.now) {
            return Ok(None);
        }
        let mut current = self.clone();
        current.claim_generation = self
            .claim_generation
            .checked_add(1)
            .filter(|generation| i64::try_from(*generation).is_ok())
            .ok_or_else(|| StorageError::internal("delivery claim generation overflowed"))?;
        current.lease_owner = Some(request.owner.clone());
        current.lease_expires_at = Some(request.lease_expires_at);
        current.updated_at = request.now;
        Ok(Some(current))
    }

    /// A delayed outcome cannot rewind a newer claim or rewrite its payload.
    pub fn settle(&self, update: DeliveryRecordUpdate) -> Option<Self> {
        if self.id != update.record_id
            || update.expected_claim_generation == 0
            || self.claim_generation != update.expected_claim_generation
            || self.lease_owner.as_deref() != Some(&update.expected_lease_owner)
        {
            return None;
        }
        let mut current = self.clone();
        current.status = update.status;
        current.attempts = update.attempts;
        current.next_attempt_at = update.next_attempt_at;
        current.lease_owner = None;
        current.lease_expires_at = None;
        current.last_error = update.last_error;
        current.updated_at = update.updated_at;
        Some(current)
    }
}

impl DeliveryRecordUpdate {
    /// A retry with no explicit delay is due immediately. Terminal settlement
    /// is distinct from an immediately eligible retry during receipt recovery.
    pub fn retry_at(&self) -> Option<storage_types::TimestampMillis> {
        (self.status == crate::DeliveryStatus::RetryScheduled)
            .then(|| self.next_attempt_at.unwrap_or(self.updated_at))
    }
}

pub async fn settle_delivery_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    update: DeliveryRecordUpdate,
) -> StorageResult<bool> {
    let Some(previous) = one_delivery(
        attempt
            .read_deliveries(vec![update.record_id.clone()])
            .await?,
    )?
    else {
        return Ok(false);
    };
    if previous.id != update.record_id {
        return Err(StorageError::internal("delivery lookup identity differs"));
    }
    let Some(current) = previous.settle(update) else {
        return Ok(false);
    };
    attempt
        .replace_deliveries(vec![(previous, current)])
        .await?;
    Ok(true)
}
