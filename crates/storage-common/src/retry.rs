//! The retry owners: `retry_tries` for every operation,
//! `retry_native_conflicts` for schema initialization.
use std::time::Duration;

use storage_types::{
    AttemptKind, AttemptPlan, AttemptRequest, AttemptTry, Clock, StorageResult, WriteRetryPolicy,
};

/// Retry known native conflicts and permit one complete fresh validation.
///
/// This owner starts the operation deadline once, from the class of `plan`
/// and the budgets of the backend. Every attempt receives the request
/// carrying that started deadline; backoff ends at the same deadline.
pub async fn retry_native_conflicts<R, F, Fut>(
    clock: &(impl Clock + ?Sized),
    policy: WriteRetryPolicy,
    budgets: storage_types::AttemptDeadlines,
    plan: AttemptPlan,
    operation: F,
) -> StorageResult<R>
where
    F: FnMut(AttemptRequest) -> Fut,
    Fut: std::future::Future<Output = StorageResult<R>>,
{
    retry_started(clock, policy, plan.start(budgets), operation).await
}

/// The retry owner of every domain `run`: one operation deadline started
/// here, a fresh `AttemptTry` for each try, retries of known aborts only
/// (`plan.retry`).
pub async fn retry_tries<R, F, Fut>(
    clock: &(impl Clock + ?Sized),
    budgets: storage_types::AttemptDeadlines,
    plan: AttemptPlan,
    attempt: F,
) -> StorageResult<R>
where
    F: FnMut(AttemptRequest, AttemptTry) -> Fut,
    Fut: std::future::Future<Output = StorageResult<R>>,
{
    retry_started_tries(clock, plan.start(budgets), attempt).await
}

/// `retry_tries` under an operation deadline that the caller started: every
/// group of one BatchWriteItem runs under the operation's one deadline.
pub async fn retry_started_tries<R, F, Fut>(
    clock: &(impl Clock + ?Sized),
    request: AttemptRequest,
    mut attempt: F,
) -> StorageResult<R>
where
    F: FnMut(AttemptRequest, AttemptTry) -> Fut,
    Fut: std::future::Future<Output = StorageResult<R>>,
{
    let mut number = 0_u32;
    retry_started(clock, request.retry, request, |attempt_request| {
        number += 1;
        attempt(
            attempt_request,
            AttemptTry {
                number,
                started_at: clock.now(),
            },
        )
    })
    .await
}

/// Every try receives `request` unchanged: its deadline started once.
async fn retry_started<R, F, Fut>(
    clock: &(impl Clock + ?Sized),
    policy: WriteRetryPolicy,
    request: AttemptRequest,
    mut operation: F,
) -> StorageResult<R>
where
    F: FnMut(AttemptRequest) -> Fut,
    Fut: std::future::Future<Output = StorageResult<R>>,
{
    retry_conflicts(clock, policy, Some(request.deadline), || operation(request)).await
}

async fn retry_conflicts<R, F, Fut>(
    clock: &(impl Clock + ?Sized),
    policy: WriteRetryPolicy,
    deadline: Option<storage_types::AttemptDeadline>,
    mut operation: F,
) -> StorageResult<R>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = StorageResult<R>>,
{
    let mut attempt = 1;
    let mut used_fresh_validation = false;
    loop {
        if let Some(deadline) = deadline {
            deadline.require_remaining(storage_types::AttemptPhase::Acquire)?;
        }
        match operation().await {
            Err(error) if error.attempt_kind() == Some(AttemptKind::FreshValidation) => {
                if used_fresh_validation {
                    return Err(storage_types::StorageError::internal(
                        "fresh attempt requested another fresh validation",
                    ));
                }
                used_fresh_validation = true;
                continue;
            }
            Err(error)
                if attempt < policy.max_attempts()
                    && error.attempt_kind() == Some(AttemptKind::Conflict) =>
            {
                crate::telemetry::record_provider_count("native", "conflict_retries", 1);
                let maximum_delay = policy.delay_after_conflict(attempt);
                // Competing API nodes must not retry a contended row in
                // synchronized waves. Equal jitter retains half the backoff and
                // never exceeds the policy budget.
                let half = maximum_delay / 2;
                let spread = (maximum_delay - half).as_nanos() as u64;
                let delay = half
                    + Duration::from_nanos(storage_types::ambient::random_u64() % (spread + 1));
                if !delay.is_zero() {
                    let waiting = std::time::Instant::now();
                    if let Some(deadline) = deadline
                        && delay >= deadline.remaining()
                    {
                        return Err(storage_types::expired_in(
                            storage_types::AttemptPhase::Acquire,
                        ));
                    }
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
        attempt += 1;
    }
}
