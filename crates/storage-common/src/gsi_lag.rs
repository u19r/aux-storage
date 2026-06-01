use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use storage_types::{StorageEnum, StorageError, StorageResult, TimestampMillis};

pub const GSI_LAG_TARGET_MS: u64 = 200;
pub const GSI_LAG_SOFT_LIMIT_MS: u64 = 400;
pub const GSI_LAG_HARD_LIMIT_MS: u64 = 1_000;
pub const GSI_LAG_CRITICAL_LIMIT_MS: u64 = 3_000;

const MAX_SOFT_DELAY_MS: u64 = 25;
const HARD_DELAY_MS: u64 = 50;
const PRESSURE_WINDOW_MS: u64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GsiLagPolicy {
    pub target_ms: u64,
    pub soft_limit_ms: u64,
    pub hard_limit_ms: u64,
    pub critical_limit_ms: u64,
}

impl Default for GsiLagPolicy {
    fn default() -> Self {
        Self {
            target_ms: GSI_LAG_TARGET_MS,
            soft_limit_ms: GSI_LAG_SOFT_LIMIT_MS,
            hard_limit_ms: GSI_LAG_HARD_LIMIT_MS,
            critical_limit_ms: GSI_LAG_CRITICAL_LIMIT_MS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GsiWritePressure {
    Allow,
    Delay(Duration),
    Throttle,
}

#[derive(Debug)]
pub struct GsiPropagationGovernor {
    policy: GsiLagPolicy,
    lag_ms: AtomicU64,
    pressure_until_ms: AtomicU64,
    decision_counter: AtomicU64,
}

impl Default for GsiPropagationGovernor {
    fn default() -> Self {
        Self::new(GsiLagPolicy::default())
    }
}

impl GsiPropagationGovernor {
    #[must_use]
    pub const fn new(policy: GsiLagPolicy) -> Self {
        Self {
            policy,
            lag_ms: AtomicU64::new(0),
            pressure_until_ms: AtomicU64::new(0),
            decision_counter: AtomicU64::new(0),
        }
    }

    pub fn observe_lag(&self, lag_ms: u64, now_ms: u64) {
        self.lag_ms.store(lag_ms, Ordering::Relaxed);
        if lag_ms >= self.policy.hard_limit_ms {
            self.pressure_until_ms
                .store(now_ms.saturating_add(PRESSURE_WINDOW_MS), Ordering::Relaxed);
        }
    }

    pub fn observe_caught_up(&self) {
        self.lag_ms.store(0, Ordering::Relaxed);
        self.pressure_until_ms.store(0, Ordering::Relaxed);
    }

    #[must_use]
    pub fn lag_ms(&self) -> u64 {
        self.lag_ms.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn lag_above_target(&self) -> bool {
        self.lag_ms() > self.policy.target_ms
    }

    #[must_use]
    pub fn write_pressure(&self, now_ms: u64) -> GsiWritePressure {
        let lag_ms = self.lag_ms();
        if lag_ms < self.policy.soft_limit_ms {
            return GsiWritePressure::Allow;
        }

        if lag_ms < self.policy.hard_limit_ms {
            let over_soft = lag_ms.saturating_sub(self.policy.soft_limit_ms);
            let delay_ms = 1 + over_soft / 40;
            return GsiWritePressure::Delay(Duration::from_millis(delay_ms.min(MAX_SOFT_DELAY_MS)));
        }

        let pressure_until = self.pressure_until_ms.load(Ordering::Relaxed);
        let throttle_percent = if lag_ms >= self.policy.critical_limit_ms {
            90
        } else {
            10 + lag_ms.saturating_sub(self.policy.hard_limit_ms).min(
                self.policy
                    .critical_limit_ms
                    .saturating_sub(self.policy.hard_limit_ms),
            ) * 65
                / self
                    .policy
                    .critical_limit_ms
                    .saturating_sub(self.policy.hard_limit_ms)
                    .max(1)
        };

        if now_ms <= pressure_until || lag_ms >= self.policy.hard_limit_ms {
            let decision = self.decision_counter.fetch_add(1, Ordering::Relaxed) % 100;
            if decision < throttle_percent {
                return GsiWritePressure::Throttle;
            }
            return GsiWritePressure::Delay(Duration::from_millis(HARD_DELAY_MS));
        }

        GsiWritePressure::Allow
    }
}

#[must_use]
pub fn lag_ms_from_created_at(now_ms: u64, created_at: TimestampMillis) -> u64 {
    let created_at_ms = u64::try_from(*created_at).unwrap_or(0);
    now_ms.saturating_sub(created_at_ms)
}

pub fn emit_gsi_lag_metrics(lag_ms: u64, pending: bool) {
    #[expect(clippy::cast_precision_loss)]
    let lag_ms_f64 = lag_ms as f64;
    metrics::gauge!("gsi.update.oldest.pending.age.ms").set(lag_ms_f64);
    metrics::gauge!("gsi.update.pending").set(if pending { 1.0 } else { 0.0 });
}

pub fn observe_gsi_lag(
    governor: &GsiPropagationGovernor,
    oldest_pending_created_at: Option<TimestampMillis>,
    now_ms: u64,
) {
    let Some(created_at) = oldest_pending_created_at else {
        governor.observe_caught_up();
        emit_gsi_lag_metrics(0, false);
        return;
    };

    let lag_ms = lag_ms_from_created_at(now_ms, created_at);
    governor.observe_lag(lag_ms, now_ms);
    emit_gsi_lag_metrics(lag_ms, true);
}

pub async fn apply_gsi_write_pressure(
    immediate_gsi_consistency: bool,
    governor: &GsiPropagationGovernor,
    now_ms: u64,
) -> StorageResult<()> {
    if immediate_gsi_consistency {
        return Ok(());
    }

    match governor.write_pressure(now_ms) {
        GsiWritePressure::Allow => Ok(()),
        GsiWritePressure::Delay(delay) => {
            #[expect(clippy::cast_precision_loss)]
            let delay_ms = delay.as_millis() as f64;
            metrics::histogram!("gsi.write.delay.ms").record(delay_ms);
            tokio::time::sleep(delay).await;
            Ok(())
        }
        GsiWritePressure::Throttle => {
            metrics::counter!("gsi.write.throttled.total").increment(1);
            Err(gsi_write_throttled_error())
        }
    }
}

#[must_use]
pub fn gsi_write_throttled_error() -> StorageError {
    StorageEnum::ProvisionedThroughputExceeded {
        message: "GSI propagation is behind and write ingest is being throttled".to_string(),
    }
    .into()
}
