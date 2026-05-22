use std::{
    collections::VecDeque,
    sync::Mutex,
    time::{Duration, Instant},
};

use serde::Serialize;
use tracing::debug;

#[derive(Default)]
pub struct HealthTracker {
    inner: Mutex<HealthState>,
}

#[derive(Default)]
struct HealthState {
    recent_failures: VecDeque<(Instant, String)>,
}

#[derive(Debug, Serialize)]
pub struct ProbeSummary {
    pub healthy: bool,
    pub reason: Option<String>,
}

impl HealthTracker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HealthState::default()),
        }
    }

    pub fn record_success(&self) {
        if let Ok(mut state) = self.inner.lock() {
            state.recent_failures.clear();
        }
    }

    pub fn record_failure(&self, reason: impl Into<String>) {
        if let Ok(mut state) = self.inner.lock() {
            let reason_string = reason.into();
            if state.recent_failures.len() == 5 {
                state.recent_failures.pop_front();
            }
            state
                .recent_failures
                .push_back((Instant::now(), reason_string));
        }
    }

    pub fn status(&self) -> ProbeSummary {
        let now = Instant::now();
        if let Ok(state) = self.inner.lock() {
            if state.recent_failures.len() < 5 {
                return ProbeSummary {
                    healthy: true,
                    reason: None,
                };
            }

            if let Some((first_failure, _)) = state.recent_failures.front()
                && now.duration_since(*first_failure) >= Duration::from_secs(5)
            {
                let reason = state
                    .recent_failures
                    .back()
                    .map(|(_, reason)| reason.clone());
                return ProbeSummary {
                    healthy: false,
                    reason,
                };
            }
            ProbeSummary {
                healthy: true,
                reason: None,
            }
        } else {
            debug!(
                target = "health",
                "failed to acquire lock; assuming healthy"
            );
            ProbeSummary {
                healthy: true,
                reason: None,
            }
        }
    }
}
