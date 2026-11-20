//! Delivery results, the retry policy, and the next state of a delivery
//! (W3 design, sections 1.1 to 1.4). Pure: no I/O.
//!
//! No attempt counter ends a delivery. A transient failure is retried until
//! the record's retry deadline; only a permanent failure, the deadline, or a
//! deleted subscription makes a record terminal, and then the record moves to
//! the subscription's dead-letter queue or is dropped with a counted reason.

use std::time::Duration;

use queue_provider::QueueArn;
use storage_types::{StorageError, TimestampMillis};

use crate::{DeliveryState, DestinationKind, Subscription, SubscriptionProtocol};

/// What one delivery attempt gave. The transport produces it; `decide`
/// selects the next state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryAttemptResult {
    Delivered,
    Transient(DeliveryFailureCode),
    Permanent(DeliveryFailureCode),
}

/// Why an attempt failed. It carries no payload, so it can be stored, logged
/// and used as a metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryFailureCode {
    DestinationMissing,
    PayloadRejected,
    RawAttributeLimit,
    HttpStatus(u16),
    HttpTimeout,
    HttpConnect,
    HttpTransport,
    QueueStoreUnavailable,
    NotConfigured,
    CustomSender,
    Internal,
}

impl DeliveryFailureCode {
    /// A fixed-cardinality label (the HTTP status is not part of it).
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::DestinationMissing => "destination_missing",
            Self::PayloadRejected => "payload_rejected",
            Self::RawAttributeLimit => "raw_attribute_limit",
            Self::HttpStatus(_) => "http_status",
            Self::HttpTimeout => "http_timeout",
            Self::HttpConnect => "http_connect",
            Self::HttpTransport => "http_transport",
            Self::QueueStoreUnavailable => "queue_store_unavailable",
            Self::NotConfigured => "not_configured",
            Self::CustomSender => "custom_sender",
            Self::Internal => "internal",
        }
    }

    /// The value stored in `DeliveryRecord::last_error`.
    #[must_use]
    pub fn stored(self) -> String {
        match self {
            Self::HttpStatus(status) => format!("http_status_{status}"),
            other => other.label().to_owned(),
        }
    }
}

impl std::str::FromStr for DeliveryFailureCode {
    type Err = StorageError;

    /// The inverse of `stored`.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(status) = value.strip_prefix("http_status_") {
            return status
                .parse()
                .map(Self::HttpStatus)
                .map_err(|_| StorageError::internal("stored HTTP status is invalid"));
        }
        [
            Self::DestinationMissing,
            Self::PayloadRejected,
            Self::RawAttributeLimit,
            Self::HttpTimeout,
            Self::HttpConnect,
            Self::HttpTransport,
            Self::QueueStoreUnavailable,
            Self::NotConfigured,
            Self::CustomSender,
            Self::Internal,
        ]
        .into_iter()
        .find(|code| code.label() == value)
        .ok_or_else(|| StorageError::internal("stored delivery failure is invalid"))
    }
}

impl DeliveryAttemptResult {
    /// AWS documents 429 and 5xx as retryable and other codes as client
    /// errors. Redirects are not followed, so 3xx is permanent (not probed).
    #[must_use]
    pub const fn from_http_status(status: u16) -> Self {
        match status {
            200..=299 => Self::Delivered,
            429 | 500..=599 => Self::Transient(DeliveryFailureCode::HttpStatus(status)),
            _ => Self::Permanent(DeliveryFailureCode::HttpStatus(status)),
        }
    }

    /// Any failure of the attempt that writes to a queue on the same store is
    /// settled as transient (review C5): after a known abort nothing was
    /// written; after an unknown outcome the settle attempt reads the fenced
    /// record with conflict protection, so a first commit that applied makes
    /// the settle `ClaimLost`. Without this, `attempts` never grows and the
    /// record is claimed again each lease forever. The exception is a store
    /// whose durability failed: it refuses attempts until restart, so the
    /// error is returned.
    #[must_use]
    pub fn from_queue_store_error(error: &StorageError) -> Option<Self> {
        let durability_failed = matches!(
            error.as_ref(),
            storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::Unavailable(
                storage_types::NativeUnavailable::DurabilityFailed
            ))
        );
        (!durability_failed).then_some(Self::Transient(DeliveryFailureCode::QueueStoreUnavailable))
    }
}

/// Retry timing for one delivery. Defaults are constants, not node
/// configuration, so every node agrees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryRetryPolicy {
    /// From record creation to the retry deadline.
    pub lifetime: Duration,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl DeliveryRetryPolicy {
    /// AWS-managed endpoints: retries over 23 days, 20 s apart after the
    /// back-off (owner decision O2).
    pub const SQS: Self = Self {
        lifetime: Duration::from_secs(23 * 24 * 60 * 60),
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(20),
    };
    /// One hour, the AWS maximum for HTTP (owner decision O1). Also for
    /// subscription confirmations.
    pub const HTTP: Self = Self {
        lifetime: Duration::from_secs(60 * 60),
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(60),
    };

    #[must_use]
    pub const fn for_protocol(protocol: SubscriptionProtocol) -> Self {
        match protocol {
            SubscriptionProtocol::Queue => Self::SQS,
            SubscriptionProtocol::Http | SubscriptionProtocol::Https => Self::HTTP,
        }
    }

    /// Equal jitter in `[d/2, d]`, `d = min(max_delay, base_delay *
    /// 2^(attempts-1))`. The caller supplies `jitter`, so tests are
    /// deterministic.
    #[must_use]
    pub fn next_delay(&self, attempts: u32, jitter: u32) -> Duration {
        let exponent = attempts.saturating_sub(1).min(20);
        let full = self
            .base_delay
            .saturating_mul(1_u32 << exponent)
            .min(self.max_delay);
        let half = full / 2;
        let spread = u64::try_from((full - half).as_millis()).unwrap_or(u64::MAX);
        half + Duration::from_millis(u64::from(jitter) % spread.saturating_add(1))
    }

    /// The retry deadline of a record created at `created_at`.
    #[must_use]
    pub fn deadline(&self, created_at: TimestampMillis) -> TimestampMillis {
        after(created_at, self.lifetime)
    }
}

/// Why a terminal decision loses a notification. Each drop is counted and
/// logged with this label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    SubscriptionDeleted,
    PermanentNoDeadLetter,
    DeadlineNoDeadLetter,
    DeadLetterMissing,
    DeadLetterRejected,
}

impl DropReason {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::SubscriptionDeleted => "subscription_deleted",
            Self::PermanentNoDeadLetter => "permanent_no_dlq",
            Self::DeadlineNoDeadLetter => "deadline_no_dlq",
            Self::DeadLetterMissing => "dead_letter_missing",
            Self::DeadLetterRejected => "dead_letter_rejected",
        }
    }
}

/// The next state of a settled delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryDecision {
    /// Delete the record.
    Complete,
    Retry {
        next_attempt_at: TimestampMillis,
        attempts: u32,
        failure: DeliveryFailureCode,
    },
    /// Move to this queue and delete the record, in one attempt.
    DeadLetter {
        target: QueueArn,
        failure: DeliveryFailureCode,
    },
    /// Delete the record, count and log the reason.
    Drop {
        reason: DropReason,
        failure: Option<DeliveryFailureCode>,
    },
}

impl DeliveryState {
    /// The retry policy of this delivery: a queue uses the SQS policy; HTTP
    /// and confirmations use the HTTP policy.
    #[must_use]
    pub fn retry_policy(&self) -> DeliveryRetryPolicy {
        match self.destination.kind() {
            DestinationKind::LocalQueue => DeliveryRetryPolicy::SQS,
            DestinationKind::Http | DestinationKind::Confirmation => DeliveryRetryPolicy::HTTP,
        }
    }

    /// The next state after `result` (design 1.4). `current` is the active
    /// subscription that the settle attempt read, or `None` when it was
    /// deleted or its topic was recreated. The deadline is checked only after
    /// a failed attempt, so a record that waited past it gets one attempt.
    /// A confirmation never moves to a dead-letter queue.
    #[must_use]
    pub fn decide(
        &self,
        result: DeliveryAttemptResult,
        current: Option<&Subscription>,
        now: TimestampMillis,
        jitter: u32,
    ) -> DeliveryDecision {
        let (failure, permanent) = match result {
            DeliveryAttemptResult::Delivered => return DeliveryDecision::Complete,
            DeliveryAttemptResult::Transient(failure) => (failure, false),
            DeliveryAttemptResult::Permanent(failure) => (failure, true),
        };
        let Some(current) = current else {
            return drop(DropReason::SubscriptionDeleted, failure);
        };
        if !permanent {
            let attempts = self.attempts.saturating_add(1);
            let next_attempt_at = after(now, self.retry_policy().next_delay(attempts, jitter));
            if next_attempt_at < self.retry_deadline {
                return DeliveryDecision::Retry {
                    next_attempt_at,
                    attempts,
                    failure,
                };
            }
        }
        let is_notification = self.destination.kind() != DestinationKind::Confirmation;
        match (
            current.redrive_policy.as_ref().filter(|_| is_notification),
            permanent,
        ) {
            (Some(policy), _) => DeliveryDecision::DeadLetter {
                target: policy.target.clone(),
                failure,
            },
            (None, true) => drop(DropReason::PermanentNoDeadLetter, failure),
            (None, false) => drop(DropReason::DeadlineNoDeadLetter, failure),
        }
    }
}

fn drop(reason: DropReason, failure: DeliveryFailureCode) -> DeliveryDecision {
    DeliveryDecision::Drop {
        reason,
        failure: Some(failure),
    }
}

/// `time + duration`, saturating.
#[must_use]
pub fn after(time: TimestampMillis, duration: Duration) -> TimestampMillis {
    let millis = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
    TimestampMillis::from_timestamp(time.timestamp_millis().saturating_add(millis))
}
