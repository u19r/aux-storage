//! Retry helpers (currently minimal) to unify backoff semantics.
use std::time::Duration;

use rand::{RngExt as _, rngs::ThreadRng};
use storage_types::{Clock, NativeAttemptRejection, StorageResult, WriteRetryPolicy};
use tracing::warn;

/// Retry only attempts whose native owner proved that nothing committed.
pub async fn retry_native_conflicts<R, F, Fut>(
    clock: &(impl Clock + ?Sized),
    policy: WriteRetryPolicy,
    mut operation: F,
) -> StorageResult<R>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = StorageResult<R>>,
{
    for attempt in 1..=policy.max_attempts() {
        match operation().await {
            Err(error)
                if attempt < policy.max_attempts()
                    && error.native_attempt_rejection()
                        == Some(NativeAttemptRejection::Conflict) =>
            {
                crate::telemetry::record_provider_count("native", "conflict_retries", 1);
                let maximum_delay = policy.delay_after_conflict(attempt);
                // Competing API nodes must not retry a contended row in
                // synchronized waves. Equal jitter retains half the backoff and
                // never exceeds the policy budget.
                let half = maximum_delay / 2;
                let spread = (maximum_delay - half).as_nanos() as u64;
                let delay = half + Duration::from_nanos(rand::random::<u64>() % (spread + 1));
                if !delay.is_zero() {
                    let waiting = std::time::Instant::now();
                    clock.sleep(delay).await?;
                    crate::telemetry::record_provider_stage(
                        "native",
                        "conflict_backoff",
                        waiting.elapsed(),
                    );
                }
            }
            result => return result,
        }
    }
    unreachable!("WriteRetryPolicy always has at least one attempt")
}

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

/// Exponential backoff used for storage-conflict retries (`exp + jitter/2`).
///
/// `base_ms` stays per-engine (Turso 2ms, Postgres 5ms); only the formula is
/// shared so tuning cannot drift apart.
#[must_use]
pub fn conflict_backoff_duration(base_ms: u64, attempt: u32) -> Duration {
    Duration::from_millis(conflict_backoff_ms(base_ms, attempt, rand::random::<u64>()))
}

/// Pure form of [`conflict_backoff_duration`] for tests.
#[must_use]
pub fn conflict_backoff_ms(base_ms: u64, attempt: u32, jitter: u64) -> u64 {
    let exp = base_ms.saturating_mul(1_u64 << attempt.min(8));
    let jitter = jitter % (exp.saturating_add(1));
    exp.saturating_add(jitter / 2)
}

/// Whether a storage error is a retryable conflict (shared predicate so
/// backends do not drift on which errors retry).
#[must_use]
pub fn is_conflict_storage_error(error: &storage_types::StorageError) -> bool {
    matches!(
        error.as_ref(),
        storage_types::StorageEnum::TransactionConflict { .. }
            | storage_types::StorageEnum::TransactionInProgress { .. }
    )
}

/// Retry a fallible async operation on storage conflicts with exponential
/// backoff ([`conflict_backoff_duration`]).
///
/// `max_attempts` and `base_delay_ms` stay per-engine (Postgres uses 8
/// attempts from 5ms, Turso 13 from 2ms); `is_conflict` classifies retryable
/// errors so each error type keeps its own predicate; `on_exhausted` builds
/// the terminal error so budget-exhaustion messages stay per call site.
/// A non-conflict error returns immediately; a conflict on the final attempt
/// yields `on_exhausted()` rather than the last operation error.
pub async fn retry_on_conflict<T, E, F, Fut>(
    max_attempts: u32,
    base_delay_ms: u64,
    operation_name: &str,
    mut operation: F,
    is_conflict: impl Fn(&E) -> bool,
    on_exhausted: impl FnOnce() -> E,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    for attempt in 0..max_attempts {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if is_conflict(&error) && attempt + 1 < max_attempts => {
                let sleep = conflict_backoff_duration(base_delay_ms, attempt);
                tracing::debug!(
                    attempt,
                    sleep_ms = sleep.as_millis(),
                    error = %error,
                    operation = operation_name,
                    "retrying storage conflict",
                );
                tokio::time::sleep(sleep).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(on_exhausted())
}

/// [`retry_on_conflict`] with [`RetryPolicy::default`] tuning.
///
/// Only `max_attempts` and `base_delay_ms` apply; the conflict formula has
/// no delay cap (the attempt shift saturates at 8). Backends with measured
/// per-engine tuning keep calling [`retry_on_conflict`] explicitly.
pub async fn retry_on_conflict_with_defaults<T, E, F, Fut>(
    operation_name: &str,
    operation: F,
    is_conflict: impl Fn(&E) -> bool,
    on_exhausted: impl FnOnce() -> E,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let policy = RetryPolicy::default();
    retry_on_conflict(
        policy.max_attempts,
        policy.base_delay_ms,
        operation_name,
        operation,
        is_conflict,
        on_exhausted,
    )
    .await
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
