//! Checks of stored publication observations and the chunk continuation.

use storage_types::{StorageError, StorageResult};

use crate::{PublishCandidate, PublishIntent};

impl PublishCandidate {
    /// Stored snapshots are checked at the shared observation boundary before
    /// either ordered materialization or ordinary leased work can write
    /// effects.
    pub fn validate(&self) -> StorageResult<()> {
        self.intent.validate_position()?;
        if self.intent.acceptance.request.topic_arn != self.intent.snapshot.topic_arn
            || self.intent.snapshot.chunk_count == 0 && !self.subscriptions.is_empty()
        {
            return Err(StorageError::internal("publish snapshot and intent differ"));
        }
        if self.intent.snapshot.chunk_count != 0 {
            validate_snapshot_chunk(&self.intent.snapshot.topic_arn, &self.subscriptions)?;
        }
        Ok(())
    }
}

impl PublishIntent {
    fn validate_position(&self) -> StorageResult<()> {
        if self.next_chunk > self.snapshot.chunk_count
            || self.snapshot.chunk_count != 0 && self.next_chunk == self.snapshot.chunk_count
        {
            return Err(StorageError::internal(
                "publish intent has no remaining chunk",
            ));
        }
        Ok(())
    }

    pub fn next_chunk(&self) -> StorageResult<Option<Self>> {
        self.validate_position()?;
        let next_chunk = self
            .next_chunk
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("publish chunk index overflowed"))?;
        if next_chunk >= self.snapshot.chunk_count {
            return Ok(None);
        }
        let mut next = self.clone();
        next.next_chunk = next_chunk;
        next.lease_expires_at = None;
        Ok(Some(next))
    }
}

/// Acceptance and stored observations share the same immutable chunk contract.
pub(crate) fn validate_snapshot_chunk(
    topic_arn: &crate::TopicArn,
    subscriptions: &[crate::Subscription],
) -> StorageResult<()> {
    if subscriptions.is_empty()
        || subscriptions.len() > crate::constants::PUBSUB_SNAPSHOT_CHUNK_SIZE
    {
        return Err(StorageError::internal(
            "publication snapshot chunk has invalid size",
        ));
    }
    for (index, subscription) in subscriptions.iter().enumerate() {
        if subscription.topic_arn != *topic_arn
            || subscriptions[..index]
                .iter()
                .any(|previous| previous.subscription_arn == subscription.subscription_arn)
        {
            return Err(StorageError::internal(
                "publication snapshot chunk has invalid subscriptions",
            ));
        }
    }
    Ok(())
}
