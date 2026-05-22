//! Retry helpers (currently minimal) to unify backoff semantics.
use std::time::Duration;

use rand::{RngExt as _, rngs::ThreadRng};
use tracing::warn;

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay_ms: 5,
            max_delay_ms: 200,
            jitter: true,
        }
    }
}

/// Execute an async operation with simple exponential backoff + optional
/// jitter.
pub async fn execute_with_retry<F, Fut, T, E>(policy: RetryPolicy, mut op: F) -> Result<T, E>
where
    F: FnMut(u32) -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut attempt: u32 = 1;
    loop {
        match op(attempt).await {
            Ok(v) => return Ok(v),
            Err(e) if attempt >= policy.max_attempts => return Err(e),
            Err(_e) => {
                let mut delay = policy.base_delay_ms * 2u64.saturating_pow(attempt - 1);
                if delay > policy.max_delay_ms {
                    delay = policy.max_delay_ms;
                }
                if policy.jitter {
                    let mut rng: ThreadRng = rand::rng();
                    let half = (delay / 2).max(1);
                    let j: u64 = rng.random_range(0..half);
                    delay = delay.saturating_sub(delay / 4) + j / 2;
                }
                warn!(attempt, delay_ms = delay, "retrying_operation");
                tokio::time::sleep(Duration::from_millis(delay)).await;
                attempt += 1;
            }
        }
    }
}
