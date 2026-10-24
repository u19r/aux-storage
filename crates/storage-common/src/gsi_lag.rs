use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use storage_types::{StorageEnum, StorageError, StorageResult};

pub use crate::constants::{
    GSI_LAG_CRITICAL_LIMIT_MS, GSI_LAG_HARD_LIMIT_MS, GSI_LAG_SOFT_LIMIT_MS, GSI_LAG_TARGET_MS,
};
use crate::constants::{HARD_DELAY_MS, MAX_SOFT_DELAY_MS};

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
            decision_counter: AtomicU64::new(0),
        }
    }

    pub fn observe_lag(&self, lag_ms: u64, _now_ms: u64) {
        self.lag_ms.store(lag_ms, Ordering::Relaxed);
    }

    pub fn observe_caught_up(&self) {
        self.lag_ms.store(0, Ordering::Relaxed);
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
    pub fn write_pressure(&self, _now_ms: u64) -> GsiWritePressure {
        let lag_ms = self.lag_ms();
        if lag_ms < self.policy.soft_limit_ms {
            return GsiWritePressure::Allow;
        }

        if lag_ms < self.policy.hard_limit_ms {
            let over_soft = lag_ms.saturating_sub(self.policy.soft_limit_ms);
            let delay_ms = 1 + over_soft / 40;
            return GsiWritePressure::Delay(Duration::from_millis(delay_ms.min(MAX_SOFT_DELAY_MS)));
        }

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

        let decision = self.decision_counter.fetch_add(1, Ordering::Relaxed) % 100;
        if decision < throttle_percent {
            return GsiWritePressure::Throttle;
        }
        GsiWritePressure::Delay(Duration::from_millis(HARD_DELAY_MS))
    }
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
