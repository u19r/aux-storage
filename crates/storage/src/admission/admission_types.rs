use std::{fmt, time::Duration};

/// The bounded workload classes used by the controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdmissionClass {
    PointRead,
    RangeRead,
    Write,
}

impl AdmissionClass {
    pub(super) const ALL: [Self; 3] = [Self::PointRead, Self::RangeRead, Self::Write];

    pub(super) const fn index(self) -> usize {
        match self {
            Self::PointRead => 0,
            Self::RangeRead => 1,
            Self::Write => 2,
        }
    }
}

/// Adaptive controller states exposed by diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionState {
    Warmup,
    Stable,
    Probe,
    Backoff,
    Recovering,
    Emergency,
}

impl AdmissionState {
    pub(super) const ALL: [Self; 6] = [
        Self::Warmup,
        Self::Stable,
        Self::Probe,
        Self::Backoff,
        Self::Recovering,
        Self::Emergency,
    ];
}

/// Public tuning values. Queueing remains bounded when adaptation is disabled,
/// so disabling is a safe fixed-limit rollout escape hatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionConfig {
    pub enabled: bool,
    pub initial_sustainable_throughput_rps: u64,
    pub initial_latency_estimate_ms: u64,
    pub minimum_concurrency: usize,
    pub maximum_concurrency: usize,
    pub control_reserve_concurrency: usize,
    pub queue_capacity: usize,
    pub max_queue_wait_ms: u64,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_sustainable_throughput_rps: 20_000,
            initial_latency_estimate_ms: 5,
            minimum_concurrency: 4,
            maximum_concurrency: 1_024,
            control_reserve_concurrency: 4,
            queue_capacity: 256,
            max_queue_wait_ms: 25,
        }
    }
}

impl AdmissionConfig {
    /// Validate and copy the JSON-facing configuration into the storage crate.
    pub fn from_config(
        config: &config::StorageAdmissionConfig,
    ) -> Result<Self, AdmissionConfigError> {
        Self::try_new(Self {
            enabled: config.enabled,
            initial_sustainable_throughput_rps: config.initial_sustainable_throughput_rps,
            initial_latency_estimate_ms: config.initial_latency_estimate_ms,
            minimum_concurrency: config.minimum_concurrency,
            maximum_concurrency: config.maximum_concurrency,
            control_reserve_concurrency: config.control_reserve_concurrency,
            queue_capacity: config.queue_capacity,
            max_queue_wait_ms: config.max_queue_wait_ms,
        })
    }

    pub fn try_new(config: Self) -> Result<Self, AdmissionConfigError> {
        if config.initial_sustainable_throughput_rps == 0 {
            return Err(AdmissionConfigError(
                "initial_sustainable_throughput_rps must be greater than zero",
            ));
        }
        if config.initial_latency_estimate_ms == 0 {
            return Err(AdmissionConfigError(
                "initial_latency_estimate_ms must be greater than zero",
            ));
        }
        if config.minimum_concurrency == 0 {
            return Err(AdmissionConfigError(
                "minimum_concurrency must be greater than zero",
            ));
        }
        if config.maximum_concurrency < config.minimum_concurrency {
            return Err(AdmissionConfigError(
                "maximum_concurrency must be at least minimum_concurrency",
            ));
        }
        if config.control_reserve_concurrency >= config.maximum_concurrency {
            return Err(AdmissionConfigError(
                "control_reserve_concurrency must be less than maximum_concurrency",
            ));
        }
        if config.effective_maximum() < config.minimum_concurrency {
            return Err(AdmissionConfigError(
                "effective foreground maximum must be at least minimum_concurrency",
            ));
        }
        if config.max_queue_wait_ms == 0 {
            return Err(AdmissionConfigError(
                "max_queue_wait_ms must be greater than zero",
            ));
        }
        config.bootstrap_limit()?;
        Ok(config)
    }

    #[must_use]
    pub const fn effective_maximum(self) -> usize {
        self.maximum_concurrency
            .saturating_sub(self.control_reserve_concurrency)
    }

    pub(super) fn bootstrap_limit(self) -> Result<usize, AdmissionConfigError> {
        let product = u128::from(self.initial_sustainable_throughput_rps)
            .checked_mul(u128::from(self.initial_latency_estimate_ms))
            .ok_or(AdmissionConfigError(
                "bootstrap concurrency arithmetic overflow",
            ))?;
        let rounded = product.checked_add(999).ok_or(AdmissionConfigError(
            "bootstrap concurrency arithmetic overflow",
        ))? / 1_000;
        let estimate = usize::try_from(rounded.min(self.effective_maximum() as u128))
            .map_err(|_| AdmissionConfigError("bootstrap concurrency does not fit usize"))?;
        Ok(estimate.clamp(self.minimum_concurrency, self.effective_maximum()))
    }
}

/// Configuration validation failure at process startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionConfigError(pub &'static str);

impl fmt::Display for AdmissionConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for AdmissionConfigError {}

/// Provider outcome used by the controller feedback loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionOutcome {
    Success(Duration),
    SuccessAfterPressure(Duration),
    Failure(Duration),
    RetryablePressure(Duration),
    Timeout,
    ExplicitThrottle,
    Cancelled,
}

impl AdmissionOutcome {
    pub(super) const fn latency(self) -> Option<Duration> {
        match self {
            Self::Success(latency)
            | Self::SuccessAfterPressure(latency)
            | Self::Failure(latency)
            | Self::RetryablePressure(latency) => Some(latency),
            Self::Timeout | Self::ExplicitThrottle | Self::Cancelled => None,
        }
    }

    pub(super) const fn is_pressure(self) -> bool {
        matches!(
            self,
            Self::SuccessAfterPressure(_)
                | Self::RetryablePressure(_)
                | Self::Timeout
                | Self::ExplicitThrottle
        )
    }

    pub(super) const fn is_success(self) -> bool {
        matches!(self, Self::Success(_) | Self::SuccessAfterPressure(_))
    }
}

/// Why an operation could not enter the foreground lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejectionReason {
    QueueFull,
    QueueTimedOut,
    ControlReserveExhausted,
}

/// Retryable foreground admission rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionRejection {
    pub reason: AdmissionRejectionReason,
    pub retry_after_seconds: u64,
}

impl AdmissionRejection {
    pub(super) fn new(reason: AdmissionRejectionReason, wait_ms: u64) -> Self {
        let retry_after_seconds = wait_ms.saturating_add(999) / 1_000;
        Self {
            reason,
            retry_after_seconds: retry_after_seconds.max(1),
        }
    }
}

/// A bounded diagnostic snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct AdmissionSnapshot {
    pub connection_id: String,
    pub enabled: bool,
    pub state: AdmissionState,
    pub desired_limit: usize,
    pub minimum_concurrency: usize,
    pub effective_maximum: usize,
    pub in_flight: usize,
    pub control_in_flight: usize,
    pub queue_depth: usize,
    pub rejection_count: u64,
    pub baselines_ms: [f64; 3],
}
