//! The stored shape of a delivery (W3S plan 5.2): an immutable payload,
//! written once by the chunk commit, and a small state that holds the
//! schedule. The due time exists only in the state's stored position (the SQL
//! `due_at` column or the KV key), never in a stored value (R2).

use std::{collections::HashMap, num::NonZeroU32};

use serde::{Deserialize, Serialize};
use storage_types::{ClaimSlot, StorageError, StorageResult, TimestampMillis, storage_serde};
use uuid::Uuid;

use crate::{
    DeliveryFailureCode, DeliveryRecordId, DeliveryRecordKind, DeliveryTarget, DestinationId,
    DestinationKey, MessageAttribute, PubsubError, PubsubMessageId, PubsubResult, Subscription,
    SubscriptionArn, models::CLAIM_BATCH_LIMIT,
};

/// The head entries of one destination that a `DeadlineOnly` ask reads
/// before it filters by deadline.
pub const DEADLINE_HEAD_WINDOW: NonZeroU32 = NonZeroU32::MIN.saturating_add(256 - 1);

/// What a delivery sends. Written once and deleted with its state (R1).
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct DeliveryPayload {
    pub id: DeliveryRecordId,
    pub kind: DeliveryRecordKind,
    pub message_id: PubsubMessageId,
    /// The partition of its state; an audit read finds the state from it.
    pub destination: DestinationKey,
    pub subscription: Option<Subscription>,
    pub message_body: Option<String>,
    pub subject: Option<String>,
    #[serde(serialize_with = "crate::types::serialize_sorted_attributes")]
    pub message_attributes: HashMap<String, MessageAttribute>,
    pub target: DeliveryTarget,
    pub created_at: TimestampMillis,
}

/// A new delivery: the payload and its first state, due at `accepted_at`.
pub type NewDelivery = (DeliveryPayload, DeliveryState);

impl DeliveryPayload {
    /// The first state of this payload: due at creation, never attempted.
    #[must_use]
    pub fn first_state(
        &self,
        subscription_arn: SubscriptionArn,
        retry_deadline: TimestampMillis,
    ) -> DeliveryState {
        DeliveryState {
            id: self.id.clone(),
            destination: self.destination.clone(),
            due_at: self.created_at,
            subscription_arn,
            retry_deadline,
            attempts: 0,
            generation: 0,
            lease: None,
            last_failure: None,
        }
    }

    pub fn encode(&self) -> StorageResult<Vec<u8>> {
        storage_serde::to_bytes(self)
    }

    pub fn decode(bytes: &[u8], id: &DeliveryRecordId) -> StorageResult<Self> {
        let payload: Self = storage_serde::from_bytes(bytes)?;
        if &payload.id != id {
            return Err(StorageError::internal("delivery payload has another id"));
        }
        Ok(payload)
    }
}

/// The claim of one node: a takeover writes another nonce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryLease {
    pub owner: String,
    pub nonce: Uuid,
    pub until: TimestampMillis,
}

/// The schedule of one delivery, about 100 to 200 bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryState {
    pub id: DeliveryRecordId,
    pub destination: DestinationKey,
    /// The stored position: creation, the next retry, or the lease end.
    pub due_at: TimestampMillis,
    pub subscription_arn: SubscriptionArn,
    /// Computed once when the delivery is written, so every node agrees.
    pub retry_deadline: TimestampMillis,
    pub attempts: u32,
    pub generation: u64,
    pub lease: Option<DeliveryLease>,
    pub last_failure: Option<DeliveryFailureCode>,
}

/// The KV state value: every field except the id and the due time, which
/// are in the key (R2).
#[derive(Serialize, Deserialize)]
struct StoredStateValue {
    destination: String,
    subscription_arn: SubscriptionArn,
    retry_deadline: TimestampMillis,
    attempts: u32,
    generation: u64,
    lease: Option<DeliveryLease>,
    last_failure: Option<String>,
}

impl DeliveryState {
    #[must_use]
    pub fn locator(&self) -> DeliveryLocator {
        DeliveryLocator {
            destination: self.destination.id(),
            due_at: self.due_at,
            id: self.id.clone(),
        }
    }

    /// Claim at `terms.now`: a new generation and lease; the record is due
    /// again when the lease ends, so a crashed holder needs no sweeper.
    pub fn claim(&self, terms: &ClaimTerms, nonce: Uuid) -> StorageResult<Self> {
        if terms.owner.is_empty() || terms.lease_until <= terms.now {
            return Err(StorageError::internal("delivery claim has invalid lease"));
        }
        let generation = self
            .generation
            .checked_add(1)
            .filter(|generation| i64::try_from(*generation).is_ok())
            .ok_or_else(|| StorageError::internal("delivery claim generation overflowed"))?;
        Ok(Self {
            generation,
            due_at: terms.lease_until,
            lease: Some(DeliveryLease {
                owner: terms.owner.clone(),
                nonce,
                until: terms.lease_until,
            }),
            ..self.clone()
        })
    }

    /// The state after a retry decision: no lease, due at the retry time.
    #[must_use]
    pub fn rescheduled(
        &self,
        next_attempt_at: TimestampMillis,
        attempts: u32,
        failure: DeliveryFailureCode,
    ) -> Self {
        Self {
            due_at: next_attempt_at,
            attempts,
            lease: None,
            last_failure: Some(failure),
            ..self.clone()
        }
    }

    pub fn encode_value(&self) -> StorageResult<Vec<u8>> {
        storage_serde::to_bytes(&StoredStateValue {
            destination: self.destination.as_str().to_owned(),
            subscription_arn: self.subscription_arn.clone(),
            retry_deadline: self.retry_deadline,
            attempts: self.attempts,
            generation: self.generation,
            lease: self.lease.clone(),
            last_failure: self.last_failure.map(DeliveryFailureCode::stored),
        })
    }

    /// Decode a KV value; the id and the due time come from its key.
    pub fn decode_value(locator: &DeliveryLocator, bytes: &[u8]) -> StorageResult<Self> {
        let value: StoredStateValue = storage_serde::from_bytes(bytes)?;
        let destination: DestinationKey = value.destination.parse()?;
        if destination.id() != locator.destination {
            return Err(StorageError::internal(
                "delivery state has another partition",
            ));
        }
        Ok(Self {
            id: locator.id.clone(),
            destination,
            due_at: locator.due_at,
            subscription_arn: value.subscription_arn,
            retry_deadline: value.retry_deadline,
            attempts: value.attempts,
            generation: value.generation,
            lease: value.lease,
            last_failure: value
                .last_failure
                .map(|failure| failure.parse())
                .transpose()?,
        })
    }
}

/// Where a state is stored: its partition, its due time and its id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeliveryLocator {
    pub destination: DestinationId,
    pub due_at: TimestampMillis,
    pub id: DeliveryRecordId,
}

/// The claim that a settle must still hold (R4). The owner is not part of
/// it: a nonce is unique for each claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryFence {
    pub locator: DeliveryLocator,
    pub generation: u64,
    pub nonce: Uuid,
}

impl DeliveryFence {
    pub fn of(state: &DeliveryState) -> PubsubResult<Self> {
        match &state.lease {
            Some(lease) if state.generation > 0 => Ok(Self {
                locator: state.locator(),
                generation: state.generation,
                nonce: lease.nonce,
            }),
            _ => Err(PubsubError::delivery_claim_lost(state.id.0.clone())),
        }
    }

    #[must_use]
    pub fn holds(&self, state: &DeliveryState) -> bool {
        state.id == self.locator.id
            && state.generation == self.generation
            && state.lease.as_ref().map(|lease| lease.nonce) == Some(self.nonce)
    }
}

/// How one destination is asked (plan 5.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskMode {
    /// The oldest due records, up to `take`.
    Due,
    /// One record, to test a half-open breaker.
    Probe,
    /// Due records at or past their retry deadline in the head window: their
    /// final attempt bypasses an open breaker (F6).
    DeadlineOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationAsk {
    pub destination: DestinationKey,
    pub take: NonZeroU32,
    pub mode: AskMode,
}

/// The owner, time and lease end of one claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimTerms {
    pub owner: String,
    pub now: TimestampMillis,
    pub lease_until: TimestampMillis,
}

/// One claim over several destinations, in the caller's (rotated) order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryClaimRequest {
    pub terms: ClaimTerms,
    pub asks: Vec<DestinationAsk>,
}

impl DeliveryClaimRequest {
    /// At least one ask, distinct destinations, and at most
    /// `CLAIM_BATCH_LIMIT` records in all.
    pub fn validate(&self) -> PubsubResult<()> {
        let total: u32 = self.asks.iter().map(|ask| ask.take.get()).sum();
        let is_distinct = self.asks.iter().enumerate().all(|(index, ask)| {
            self.asks[..index]
                .iter()
                .all(|other| other.destination != ask.destination)
        });
        if self.asks.is_empty() || total > CLAIM_BATCH_LIMIT || !is_distinct {
            return Err(PubsubError::storage("delivery claim asks are invalid"));
        }
        Ok(())
    }
}

/// The physical read of one claim attempt. A KV store reads a snapshot
/// window per ask and takes the slot of `rotation`; SQL takes the head with
/// claim protection.
#[derive(Debug, Clone)]
pub struct ClaimScan {
    pub now: TimestampMillis,
    pub asks: Vec<DestinationAsk>,
    pub first_attempt: bool,
    pub rotation: u32,
}

impl ClaimScan {
    #[must_use]
    pub fn slot(&self, ask: &DestinationAsk) -> ClaimSlot {
        ClaimSlot {
            limit: ask.take,
            first_attempt: self.first_attempt,
            rotation: self.rotation,
        }
    }
}

/// A stored state, or the locator of one that does not decode.
#[derive(Debug, Clone)]
pub enum StateRow {
    Decoded(DeliveryState),
    Undecodable(DeliveryLocator),
}

/// A stored payload, or a payload that does not decode or is missing.
#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub enum PayloadRow {
    /// Boxed: a payload holds up to 256 KiB of owned data.
    Decoded(Box<DeliveryPayload>),
    Undecodable,
    /// No payload is stored: the record can never be delivered (R1 writes
    /// a payload once).
    Missing,
}

/// The rows that one ask read, and the due time of the first record after
/// them (a timer hint).
#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct ClaimRows {
    pub destination: DestinationId,
    pub rows: Vec<(StateRow, PayloadRow)>,
    pub next_due: Option<TimestampMillis>,
}

/// A conditional state write: it applies only while `expected` holds.
#[derive(Debug, Clone)]
pub struct StateChange {
    pub expected: DeliveryFence,
    pub next: DeliveryState,
}

#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct ClaimedDelivery {
    pub fence: DeliveryFence,
    pub state: DeliveryState,
    pub payload: DeliveryPayload,
}

/// What one ask claimed.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct DestinationClaim {
    pub destination: DestinationKey,
    pub claimed: Vec<ClaimedDelivery>,
    /// The due time of the destination's first record that this claim did
    /// not take; at or before `now` means more work is due.
    pub next_due: Option<TimestampMillis>,
}

/// The oldest record of one destination, from the sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationHead {
    pub destination: DestinationKey,
    pub head_due: TimestampMillis,
    pub head_retry_deadline: TimestampMillis,
}

/// One lane of a sweep: the last destination it visited, or done.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepLane {
    After(Option<DestinationId>),
    Done,
}

/// Where the next sweep page starts. SQL uses one lane; a KV store splits
/// the destination space into lanes by the high bits of the id and reads
/// them in parallel waves (plan 5.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepCursor(pub Vec<SweepLane>);

impl SweepCursor {
    /// The start of a pass at `start` (a random start per node spreads the
    /// nodes over the space).
    #[must_use]
    pub fn from_start(start: Option<DestinationId>) -> Self {
        Self(vec![SweepLane::After(start)])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepPage {
    pub heads: Vec<DestinationHead>,
    /// `None`: the pass is complete.
    pub next: Option<SweepCursor>,
}
