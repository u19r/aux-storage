use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::retry::{
    RetryPolicy, conflict_backoff_duration, conflict_backoff_ms, retry_on_conflict,
    retry_on_conflict_with_defaults,
};

#[test]
fn given_base_and_attempt_when_backoff_is_computed_then_exponential_with_capped_shift() {
    assert_eq!(conflict_backoff_ms(5, 0, 0), 5);
    assert_eq!(conflict_backoff_ms(5, 1, 0), 10);
    assert_eq!(conflict_backoff_ms(2, 2, 0), 8);
    assert_eq!(conflict_backoff_ms(5, 20, 0), conflict_backoff_ms(5, 8, 0));
}

#[test]
fn given_jitter_when_backoff_is_computed_then_half_jitter_is_added() {
    assert_eq!(conflict_backoff_ms(5, 0, 4), 5 + 2);
}

#[test]
fn given_base_when_duration_is_computed_then_result_is_within_budget() {
    let duration = conflict_backoff_duration(5, 1);
    let millis = duration.as_millis() as u64;
    assert!((10..=15).contains(&millis));
}

fn is_test_conflict(error: &String) -> bool {
    error == "conflict"
}

#[tokio::test]
async fn given_success_when_retrying_then_value_returns_without_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let probe = Arc::clone(&calls);

    let result = retry_on_conflict(
        3,
        0,
        "test-op",
        || {
            probe.fetch_add(1, Ordering::Relaxed);
            async { Ok::<_, String>(7) }
        },
        is_test_conflict,
        || "exhausted".to_string(),
    )
    .await;

    assert_eq!(result, Ok(7));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn given_terminal_error_when_retrying_then_it_returns_immediately() {
    let calls = Arc::new(AtomicUsize::new(0));
    let probe = Arc::clone(&calls);

    let result = retry_on_conflict(
        3,
        0,
        "test-op",
        || {
            probe.fetch_add(1, Ordering::Relaxed);
            async { Err::<u32, _>("fatal".to_string()) }
        },
        is_test_conflict,
        || "exhausted".to_string(),
    )
    .await;

    assert_eq!(result, Err("fatal".to_string()));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn given_conflict_then_success_when_retrying_then_value_returns() {
    let calls = Arc::new(AtomicUsize::new(0));
    let probe = Arc::clone(&calls);

    let result = retry_on_conflict(
        3,
        0,
        "test-op",
        || {
            let attempt = probe.fetch_add(1, Ordering::Relaxed);
            async move {
                if attempt == 0 {
                    Err::<u32, _>("conflict".to_string())
                } else {
                    Ok(9)
                }
            }
        },
        is_test_conflict,
        || "exhausted".to_string(),
    )
    .await;

    assert_eq!(result, Ok(9));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn given_persistent_conflict_when_retrying_then_last_error_returns() {
    let calls = Arc::new(AtomicUsize::new(0));
    let probe = Arc::clone(&calls);

    let result = retry_on_conflict(
        2,
        0,
        "test-op",
        || {
            probe.fetch_add(1, Ordering::Relaxed);
            async { Err::<u32, _>("conflict".to_string()) }
        },
        is_test_conflict,
        || "exhausted".to_string(),
    )
    .await;

    // A conflict on the final attempt returns that error directly; the
    // exhaustion constructor only fires when the budget admits no attempts.
    assert_eq!(result, Err("conflict".to_string()));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn given_defaults_when_retrying_then_default_policy_tuning_applies() {
    let calls = Arc::new(AtomicUsize::new(0));
    let probe = Arc::clone(&calls);

    let result = retry_on_conflict_with_defaults(
        "test-op",
        || {
            let attempt = probe.fetch_add(1, Ordering::Relaxed);
            async move {
                if attempt < RetryPolicy::default().max_attempts as usize - 1 {
                    Err::<u32, _>("conflict".to_string())
                } else {
                    Ok(11)
                }
            }
        },
        is_test_conflict,
        || "exhausted".to_string(),
    )
    .await;

    assert_eq!(result, Ok(11));
    assert_eq!(
        calls.load(Ordering::Relaxed),
        RetryPolicy::default().max_attempts as usize
    );
}

#[tokio::test]
async fn given_zero_attempts_when_retrying_then_operation_never_runs() {
    let calls = Arc::new(AtomicUsize::new(0));
    let probe = Arc::clone(&calls);

    let result = retry_on_conflict(
        0,
        0,
        "test-op",
        || {
            probe.fetch_add(1, Ordering::Relaxed);
            async { Ok::<_, String>(1) }
        },
        is_test_conflict,
        || "exhausted".to_string(),
    )
    .await;

    assert_eq!(result, Err("exhausted".to_string()));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}
