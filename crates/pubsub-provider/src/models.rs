use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult, TimestampMillis};
use uuid::Uuid;

use crate::{PublishAcceptanceInput, Subscription, TopicArn};

const MAX_PUBLISH_CHUNK_CLAIM_LIMIT: u32 = 4_096;
const MAX_DELIVERY_CLAIM_LIMIT: u32 = 4_096;

#[derive(Clone, Serialize, Deserialize)]
pub struct AcceptedPublication {
    pub snapshot: SubscriptionSnapshot,
    pub intent: PublishIntent,
}

impl AcceptedPublication {
    #[must_use]
    pub fn new(snapshot: SubscriptionSnapshot, acceptance: PublishAcceptanceInput) -> Self {
        let intent = PublishIntent::new(acceptance, snapshot.root.clone());
        Self { snapshot, intent }
    }

    pub fn validate(&self) -> StorageResult<()> {
        if self.snapshot.root != self.intent.snapshot
            || self.snapshot.root.topic_arn != self.intent.acceptance.request.topic_arn
            || self.snapshot.chunks.len() != self.snapshot.root.chunk_count as usize
            || self.intent.next_chunk != 0
            || self.intent.lease_expires_at.is_some()
        {
            return Err(StorageError::internal(
                "publication snapshot and intent differ",
            ));
        }
        for chunk in &self.snapshot.chunks {
            crate::publication_transition::validate_snapshot_chunk(
                &self.snapshot.root.topic_arn,
                chunk,
            )?;
        }
        Ok(())
    }

    /// Persistence key for the publish intent: the accepted message id.
    #[must_use]
    pub fn intent_key(&self) -> String {
        self.intent.acceptance.message_id.to_string()
    }

    /// Canonical JSON encoding of the publish intent for intent columns.
    pub fn intent_json(&self) -> crate::PubsubResult<String> {
        Ok(serde_json::to_string(&self.intent)?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct DeliveryClaimLimit(NonZeroU32);

impl TryFrom<usize> for DeliveryClaimLimit {
    type Error = crate::PubsubError;

    fn try_from(limit: usize) -> crate::PubsubResult<Self> {
        let limit = u32::try_from(limit)
            .map_err(|_| crate::PubsubError::storage("delivery claim limit exceeds its maximum"))?;
        Self::try_from(limit)
    }
}

impl TryFrom<u32> for DeliveryClaimLimit {
    type Error = crate::PubsubError;

    fn try_from(limit: u32) -> crate::PubsubResult<Self> {
        let limit = NonZeroU32::new(limit)
            .ok_or_else(|| crate::PubsubError::storage("delivery claim limit must be positive"))?;
        if limit.get() > MAX_DELIVERY_CLAIM_LIMIT {
            return Err(crate::PubsubError::storage(
                "delivery claim limit exceeds its maximum",
            ));
        }
        Ok(Self(limit))
    }
}

impl From<DeliveryClaimLimit> for u32 {
    fn from(limit: DeliveryClaimLimit) -> Self {
        limit.0.get()
    }
}

impl From<DeliveryClaimLimit> for usize {
    fn from(limit: DeliveryClaimLimit) -> Self {
        limit.0.get() as Self
    }
}

impl From<DeliveryClaimLimit> for i64 {
    fn from(limit: DeliveryClaimLimit) -> Self {
        Self::from(limit.0.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionSnapshotGeneration(Uuid);

impl SubscriptionSnapshotGeneration {
    /// Creates an identity for one immutable publish snapshot.
    ///
    /// Generations are compared only for equality. Publication ordering comes
    /// from the durable intent queue, so generations do not need a global
    /// ordering across nodes.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for SubscriptionSnapshotGeneration {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SubscriptionSnapshotGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionSnapshotRoot {
    pub topic_arn: TopicArn,
    pub generation: SubscriptionSnapshotGeneration,
    pub chunk_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionSnapshot {
    pub root: SubscriptionSnapshotRoot,
    pub chunks: Vec<Vec<Subscription>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishIntent {
    pub acceptance: PublishAcceptanceInput,
    pub snapshot: SubscriptionSnapshotRoot,
    pub next_chunk: u32,
    pub lease_expires_at: Option<TimestampMillis>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimedPublishChunk {
    pub intent: PublishIntent,
    pub subscriptions: Vec<Subscription>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishChunkCommit {
    pub claimed_intent: PublishIntent,
    pub records: Vec<crate::DeliveryRecord>,
    pub next_intent: Option<PublishIntent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishAcceptancePlan {
    pub snapshot: SubscriptionSnapshot,
    pub acceptance: PublishAcceptanceInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishChunkClaimRequest {
    pub limit: PublishChunkClaimLimit,
    pub now: TimestampMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct PublishChunkClaimLimit(NonZeroU32);

impl TryFrom<usize> for PublishChunkClaimLimit {
    type Error = crate::PubsubError;

    fn try_from(limit: usize) -> crate::PubsubResult<Self> {
        let limit = u32::try_from(limit).map_err(|_| {
            crate::PubsubError::storage("publish chunk claim limit exceeds its maximum")
        })?;
        Self::try_from(limit)
    }
}

impl TryFrom<u32> for PublishChunkClaimLimit {
    type Error = crate::PubsubError;

    fn try_from(limit: u32) -> crate::PubsubResult<Self> {
        let limit = NonZeroU32::new(limit).ok_or_else(|| {
            crate::PubsubError::storage("publish chunk claim limit must be positive")
        })?;
        if limit.get() > MAX_PUBLISH_CHUNK_CLAIM_LIMIT {
            return Err(crate::PubsubError::storage(
                "publish chunk claim limit exceeds its maximum",
            ));
        }
        Ok(Self(limit))
    }
}

impl From<PublishChunkClaimLimit> for u32 {
    fn from(limit: PublishChunkClaimLimit) -> Self {
        limit.0.get()
    }
}

impl From<PublishChunkClaimLimit> for usize {
    fn from(limit: PublishChunkClaimLimit) -> Self {
        limit.0.get() as Self
    }
}

impl From<PublishChunkClaimLimit> for i64 {
    fn from(limit: PublishChunkClaimLimit) -> Self {
        Self::from(limit.0.get())
    }
}
