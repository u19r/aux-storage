//! The one owner of SNS delivery settings and their rules (W3 design 4.1).
//! A lease must cover one delivery and its settle: a record is attempted only
//! when its lease has `request_timeout + SETTLE_MARGIN` left, and a valid
//! configuration has `lease >= 2 * request_timeout + SETTLE_MARGIN`.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use storage_types::TimestampMillis;

/// Time for the settle attempt after the request ends.
pub const SETTLE_MARGIN: Duration = Duration::from_secs(5);

/// The lease of a claim and the timeout of one delivery request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryTiming {
    lease: Duration,
    request_timeout: Duration,
}

/// Built from settings that `config::PubsubConfig::validate` accepted at
/// load (finding 9): that check is the one owner of the lease rule.
impl From<&config::PubsubRunnerConfig> for DeliveryTiming {
    fn from(settings: &config::PubsubRunnerConfig) -> Self {
        Self {
            lease: Duration::from_secs(settings.lease_seconds),
            request_timeout: Duration::from_secs(settings.request_timeout_seconds),
        }
    }
}

impl DeliveryTiming {
    #[must_use]
    pub const fn lease(&self) -> Duration {
        self.lease
    }

    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// The lease end of a claim made at `now`.
    #[must_use]
    pub fn lease_deadline(&self, now: TimestampMillis) -> TimestampMillis {
        pubsub_provider::after(now, self.lease)
    }

    /// True when a lease that ends at `lease_expires_at` still covers one
    /// request and its settle. A record without a lease is never attempted.
    #[must_use]
    pub fn covers_attempt(
        &self,
        lease_expires_at: Option<TimestampMillis>,
        now: TimestampMillis,
    ) -> bool {
        let needed = self.request_timeout.saturating_add(SETTLE_MARGIN);
        lease_expires_at.is_some_and(|end| end >= pubsub_provider::after(now, needed))
    }
}

impl Default for DeliveryTiming {
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(30),
            request_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PubsubDeliveryConfig {
    /// Unique for each process (design 7.4): logs and audits tell nodes
    /// apart. Safety rests on the claim generation and the nonce.
    pub worker_id: String,
    pub timing: DeliveryTiming,
    pub subscribe_url_base: Option<String>,
    pub unsubscribe_url_base: Option<String>,
    pub signing_cert_url: Option<String>,
}

impl Default for PubsubDeliveryConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("pubsub/{}", storage_types::ambient::uuid_now_v7()),
            timing: DeliveryTiming::default(),
            subscribe_url_base: None,
            unsubscribe_url_base: None,
            signing_cert_url: None,
        }
    }
}

/// The circuit breaker of one destination on one node (design 4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakerConfig {
    /// Consecutive `Transient` results that open the breaker.
    pub failures_to_open: NonZeroU32,
    /// The first open interval; each failed probe doubles it.
    pub open_initial: Duration,
    pub open_max: Duration,
    /// Destinations in the candidate set of this node; the least recently
    /// used one with nothing in flight goes.
    pub max_destinations: NonZeroUsize,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failures_to_open: NonZeroU32::new(5).unwrap_or(NonZeroU32::MIN),
            open_initial: Duration::from_secs(5),
            open_max: Duration::from_secs(300),
            max_destinations: NonZeroUsize::new(10_000).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

/// Limits of the delivery runner on one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryRunnerConfig {
    pub max_in_flight: NonZeroUsize,
    pub claim_batch: NonZeroU32,
    pub per_destination: NonZeroUsize,
    /// Publish intents that one materializer pass claims.
    pub materialize_batch: NonZeroUsize,
    /// The longest wait between two probes when no wake arrives.
    pub idle_probe: Duration,
    /// The wait after a failed probe, claim or materialization.
    pub error_backoff: Duration,
    pub breaker: BreakerConfig,
    /// Destination heads that one sweep tick reads (W3S plan 5.7, Q3).
    pub sweep_page: NonZeroU32,
    /// The time between two sweep ticks.
    pub sweep_interval: Duration,
}

/// Built from validated settings (finding 9); the other limits are constants.
impl From<&config::PubsubRunnerConfig> for DeliveryRunnerConfig {
    fn from(settings: &config::PubsubRunnerConfig) -> Self {
        let defaults = Self::default();
        Self {
            max_in_flight: NonZeroUsize::new(settings.max_in_flight)
                .unwrap_or(defaults.max_in_flight),
            per_destination: NonZeroUsize::new(settings.per_destination)
                .unwrap_or(defaults.per_destination),
            idle_probe: Duration::from_millis(settings.idle_probe_millis),
            ..defaults
        }
    }
}

impl Default for DeliveryRunnerConfig {
    fn default() -> Self {
        Self {
            max_in_flight: NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN),
            claim_batch: NonZeroU32::new(pubsub_provider::CLAIM_BATCH_LIMIT)
                .unwrap_or(NonZeroU32::MIN),
            per_destination: NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
            materialize_batch: NonZeroUsize::new(pubsub_provider::CLAIM_BATCH_LIMIT as usize)
                .unwrap_or(NonZeroUsize::MIN),
            idle_probe: Duration::from_secs(1),
            error_backoff: Duration::from_secs(5),
            breaker: BreakerConfig::default(),
            sweep_page: NonZeroU32::new(256).unwrap_or(NonZeroU32::MIN),
            sweep_interval: Duration::from_secs(1),
        }
    }
}
