//! Selected publication materialization borrows its original native attempt.

use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult};

use crate::{
    DeliveryRecord, PublishCandidate, PublishIntent, PubsubAttempt, PubsubMessageId,
    SubscriptionSnapshotGeneration,
};

/// Identifies immutable accepted work without copying its message or snapshot.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishChunkPosition {
    generation: SubscriptionSnapshotGeneration,
    chunk: u32,
}

pub struct PreparedPublishChunk<'native, A> {
    attempt: &'native mut A,
    original: PublishIntent,
    records: Vec<DeliveryRecord>,
    next: Option<PublishIntent>,
}

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

    pub fn position(&self) -> PublishChunkPosition {
        PublishChunkPosition {
            generation: self.intent.snapshot.generation,
            chunk: self.intent.next_chunk,
        }
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

impl<'native, A: PubsubAttempt> PreparedPublishChunk<'native, A> {
    /// Missing or advanced work is a stale selection, not permission to scan
    /// for another publication. The caller chooses its semantic response.
    pub async fn prepare(
        attempt: &'native mut A,
        message_id: &PubsubMessageId,
        position: &PublishChunkPosition,
        topic_arn: &crate::TopicArn,
    ) -> StorageResult<Option<Self>> {
        let Some(candidate) = attempt.read_publish_candidate(message_id).await? else {
            return Ok(None);
        };
        candidate.validate()?;
        if candidate.intent.snapshot.topic_arn != *topic_arn {
            return Err(StorageError::internal(
                "selected publication belongs to another topic",
            ));
        }
        if candidate.position() != *position || candidate.intent.lease_expires_at.is_some() {
            return Ok(None);
        }
        let next = candidate.intent.next_chunk()?;
        let records = candidate.intent.delivery_records(candidate.subscriptions);
        for record in &records {
            record
                .validate_storage_range()
                .map_err(|_| StorageError::internal("publish delivery is outside storage range"))?;
        }
        Ok(Some(Self {
            attempt,
            original: candidate.intent,
            records,
            next,
        }))
    }

    /// Delivery records and continuation settle atomically in the borrowed
    /// attempt. This capability cannot be applied to a different provider.
    pub async fn apply(self) -> StorageResult<bool> {
        let has_more = self.next.is_some();
        self.attempt.write_delivery_records(self.records).await?;
        self.attempt
            .replace_publish_intent(self.original, self.next)
            .await?;
        Ok(has_more)
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
