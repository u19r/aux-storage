use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn given_unavailable_attempt_when_retrying_native_conflicts_then_it_runs_once() {
    use storage_types::{
        AttemptPhase, NativeUnavailable, StorageEnum, SystemClock, WriteRetryPolicy,
    };

    let calls = AtomicUsize::new(0);
    let result: storage_types::StorageResult<()> = super::retry::retry_native_conflicts(
        &SystemClock,
        WriteRetryPolicy::new(5, std::time::Duration::from_millis(1)),
        storage_types::AttemptDeadlines::default(),
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadWrite,
            storage_types::DeadlineClass::Write,
        ),
        |_request| {
            calls.fetch_add(1, Ordering::Relaxed);
            async {
                Err(
                    storage_types::AttemptFailure::Unavailable(NativeUnavailable::Deadline(
                        AttemptPhase::Statement,
                    ))
                    .into(),
                )
            }
        },
    )
    .await;

    assert!(matches!(
        result.expect_err("unavailable is returned").as_ref(),
        StorageEnum::Attempt(storage_types::AttemptFailure::Unavailable(_))
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn given_one_attempt_budget_when_cached_validation_is_required_then_fresh_runs_without_backoff()
 {
    let calls = AtomicUsize::new(0);
    let result = super::retry::retry_native_conflicts(
        &storage_types::SystemClock,
        storage_types::WriteRetryPolicy::new(1, std::time::Duration::ZERO),
        storage_types::AttemptDeadlines::default(),
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadWrite,
            storage_types::DeadlineClass::Write,
        ),
        |_request| {
            let call = calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if call == 0 {
                    Err(storage_types::AttemptFailure::FRESH_VALIDATION.into())
                } else {
                    Ok(7)
                }
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(result, 7);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn given_one_attempt_budget_when_fresh_conflicts_after_validation_control_then_it_exhausts() {
    let calls = AtomicUsize::new(0);
    let result: storage_types::StorageResult<()> = super::retry::retry_native_conflicts(
        &storage_types::SystemClock,
        storage_types::WriteRetryPolicy::new(1, std::time::Duration::ZERO),
        storage_types::AttemptDeadlines::default(),
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadWrite,
            storage_types::DeadlineClass::Write,
        ),
        |_request| {
            let call = calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if call == 0 {
                    Err(storage_types::AttemptFailure::FRESH_VALIDATION.into())
                } else {
                    Err(storage_types::StorageEnum::Attempt(
                        storage_types::AttemptFailure::Conflict(
                            storage_types::ConflictCause::Logical("fresh conflict"),
                        ),
                    )
                    .into())
                }
            }
        },
    )
    .await;
    assert_eq!(
        result.unwrap_err().attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn given_fresh_validation_control_when_it_repeats_then_it_is_an_invariant_failure() {
    let calls = AtomicUsize::new(0);
    let result: storage_types::StorageResult<()> = super::retry::retry_native_conflicts(
        &storage_types::SystemClock,
        storage_types::WriteRetryPolicy::new(5, std::time::Duration::ZERO),
        storage_types::AttemptDeadlines::default(),
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadWrite,
            storage_types::DeadlineClass::Write,
        ),
        |_request| {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err(storage_types::AttemptFailure::FRESH_VALIDATION.into()) }
        },
    )
    .await;
    assert!(
        !result
            .unwrap_err()
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn given_unknown_commit_when_retrying_then_it_runs_once() {
    let calls = AtomicUsize::new(0);
    let result: storage_types::StorageResult<()> = super::retry::retry_native_conflicts(
        &storage_types::SystemClock,
        storage_types::WriteRetryPolicy::new(5, std::time::Duration::ZERO),
        storage_types::AttemptDeadlines::default(),
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadWrite,
            storage_types::DeadlineClass::Write,
        ),
        |_request| {
            calls.fetch_add(1, Ordering::Relaxed);
            async {
                Err(storage_types::StorageError::commit_unknown(
                    "synthetic unknown",
                ))
            }
        },
    )
    .await;
    assert!(result.unwrap_err().is_commit_unknown());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn given_permanent_failure_when_retrying_then_it_runs_once() {
    let calls = AtomicUsize::new(0);
    let result: storage_types::StorageResult<()> = super::retry::retry_native_conflicts(
        &storage_types::SystemClock,
        storage_types::WriteRetryPolicy::new(5, std::time::Duration::ZERO),
        storage_types::AttemptDeadlines::default(),
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadWrite,
            storage_types::DeadlineClass::Write,
        ),
        |_request| {
            calls.fetch_add(1, Ordering::Relaxed);
            async {
                Err(storage_types::StorageError::internal(
                    "synthetic permanent failure",
                ))
            }
        },
    )
    .await;
    assert!(
        !result
            .unwrap_err()
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn given_expired_operation_when_retry_starts_then_no_attempt_runs() {
    let deadline =
        storage_types::AttemptDeadlines::default().start(storage_types::DeadlineClass::Write);
    tokio::time::advance(deadline.remaining()).await;
    let result = super::retry::retry_started_tries(
        &storage_types::SystemClock,
        storage_types::AttemptPlan::new(storage_types::AttemptMode::ReadWrite, deadline.class)
            .retrying(storage_types::WriteRetryPolicy::new(
                5,
                std::time::Duration::ZERO,
            ))
            .started(deadline),
        |_request, _attempt_try| async { panic!("expired operation must not run") },
    )
    .await;
    let result: storage_types::StorageResult<()> = result;
    assert!(
        result
            .unwrap_err()
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
}

#[tokio::test(start_paused = true)]
async fn given_conflict_backoff_when_operation_expires_then_no_replay_runs() {
    let calls = AtomicUsize::new(0);
    let deadline = storage_types::AttemptDeadlines::try_new(
        [std::time::Duration::from_millis(100); storage_types::DeadlineClass::COUNT],
        std::time::Duration::from_secs(1),
    )
    .unwrap()
    .start(storage_types::DeadlineClass::Write);
    let result: storage_types::StorageResult<()> = super::retry::retry_started_tries(
        &storage_types::SystemClock,
        storage_types::AttemptPlan::new(storage_types::AttemptMode::ReadWrite, deadline.class)
            .retrying(storage_types::WriteRetryPolicy::new(
                5,
                std::time::Duration::from_secs(30),
            ))
            .started(deadline),
        |_request, _attempt_try| {
            calls.fetch_add(1, Ordering::Relaxed);
            async {
                Err(
                    storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                        storage_types::ConflictCause::Logical("known abort"),
                    ))
                    .into(),
                )
            }
        },
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(tokio::time::Instant::now() < deadline.expires_at);
}

#[tokio::test(start_paused = true)]
async fn given_fresh_replay_when_original_deadline_expires_then_no_fresh_attempt_runs() {
    let calls = AtomicUsize::new(0);
    let deadline =
        storage_types::AttemptDeadlines::default().start(storage_types::DeadlineClass::Transaction);
    let result: storage_types::StorageResult<()> = super::retry::retry_started_tries(
        &storage_types::SystemClock,
        storage_types::AttemptPlan::new(storage_types::AttemptMode::ReadWrite, deadline.class)
            .retrying(storage_types::WriteRetryPolicy::new(
                5,
                std::time::Duration::ZERO,
            ))
            .started(deadline),
        |_request, _attempt_try| {
            calls.fetch_add(1, Ordering::Relaxed);
            async {
                tokio::time::advance(deadline.remaining()).await;
                Err(storage_types::AttemptFailure::FRESH_VALIDATION.into())
            }
        },
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn given_expiry_during_attempt_when_unknown_or_success_returns_then_preserve_settlement() {
    let policy = storage_types::WriteRetryPolicy::new(5, std::time::Duration::ZERO);
    for unknown in [false, true] {
        let deadline =
            storage_types::AttemptDeadlines::default().start(storage_types::DeadlineClass::Write);
        let result = super::retry::retry_started_tries(
            &storage_types::SystemClock,
            storage_types::AttemptPlan::new(storage_types::AttemptMode::ReadWrite, deadline.class)
                .retrying(policy)
                .started(deadline),
            |_request, _attempt_try| async move {
                tokio::time::advance(deadline.remaining()).await;
                if unknown {
                    Err(storage_types::StorageError::commit_unknown("lost reply"))
                } else {
                    Ok(7)
                }
            },
        )
        .await;
        if unknown {
            assert!(result.unwrap_err().is_commit_unknown());
        } else {
            assert_eq!(result.unwrap(), 7);
        }
    }
}

fn tries_request() -> storage_types::AttemptPlan {
    storage_types::AttemptPlan::new(
        storage_types::AttemptMode::ReadWrite,
        storage_types::DeadlineClass::Write,
    )
}

#[tokio::test]
async fn given_two_conflicts_when_retry_tries_runs_then_tries_are_numbered_one_to_three() {
    let seen = std::sync::Mutex::new(Vec::new());
    let request = tries_request().retrying(storage_types::WriteRetryPolicy::new(
        5,
        std::time::Duration::from_millis(1),
    ));
    let result = super::retry::retry_tries(
        &storage_types::SystemClock,
        storage_types::AttemptDeadlines::default(),
        request,
        |_request, attempt_try| {
            seen.lock()
                .unwrap()
                .push((attempt_try.number, attempt_try.started_at));
            async move {
                if attempt_try.number < 3 {
                    Err(storage_types::StorageEnum::Attempt(
                        storage_types::AttemptFailure::Conflict(
                            storage_types::ConflictCause::Logical("test conflict"),
                        ),
                    )
                    .into())
                } else {
                    Ok(7)
                }
            }
        },
    )
    .await;
    assert_eq!(result.expect("third try succeeds"), 7);
    let seen = seen.into_inner().unwrap();
    assert_eq!(
        seen.iter().map(|(number, _)| *number).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(seen.windows(2).all(|pair| pair[0].1 <= pair[1].1));
}

#[tokio::test]
async fn given_an_unknown_commit_outcome_when_retry_tries_runs_then_it_returns_after_one_try() {
    let calls = AtomicUsize::new(0);
    let request = tries_request().retrying(storage_types::WriteRetryPolicy::new(
        5,
        std::time::Duration::from_millis(1),
    ));
    let result: storage_types::StorageResult<()> = super::retry::retry_tries(
        &storage_types::SystemClock,
        storage_types::AttemptDeadlines::default(),
        request,
        |_request, _try| {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err(storage_types::StorageError::commit_unknown("unknown")) }
        },
    )
    .await;
    assert!(result.unwrap_err().is_commit_unknown());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn given_default_retry_when_a_conflict_happens_then_retry_tries_does_not_retry() {
    let calls = AtomicUsize::new(0);
    let result: storage_types::StorageResult<()> = super::retry::retry_tries(
        &storage_types::SystemClock,
        storage_types::AttemptDeadlines::default(),
        tries_request(),
        |_request, _try| {
            calls.fetch_add(1, Ordering::Relaxed);
            async {
                Err(
                    storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                        storage_types::ConflictCause::Logical("test conflict"),
                    ))
                    .into(),
                )
            }
        },
    )
    .await;
    assert!(matches!(
        result.unwrap_err().as_ref(),
        storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(_))
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

async fn retry_counting(
    max_attempts: u32,
    error: impl Fn(usize) -> Option<storage_types::StorageError>,
) -> (storage_types::StorageResult<()>, usize) {
    let calls = AtomicUsize::new(0);
    let result = super::retry::retry_native_conflicts(
        &storage_types::SystemClock,
        storage_types::WriteRetryPolicy::new(max_attempts, std::time::Duration::ZERO),
        storage_types::AttemptDeadlines::default(),
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadWrite,
            storage_types::DeadlineClass::Write,
        ),
        |_request| {
            let call = calls.fetch_add(1, Ordering::Relaxed);
            let failure = error(call);
            async move { failure.map_or(Ok(()), Err) }
        },
    )
    .await;
    (result, calls.load(Ordering::Relaxed))
}

#[tokio::test]
async fn given_write_miss_each_try_when_retried_then_max_attempts_and_miss_returned() {
    let policy = storage_types::WriteRetryPolicy::new(4, std::time::Duration::ZERO);
    let (result, calls) = retry_counting(4, |_| {
        Some(storage_types::StorageError::write_miss(
            "x",
            storage_types::MissExpectation::Absent,
        ))
    })
    .await;
    assert_eq!(calls, policy.max_attempts() as usize);
    let error = result.unwrap_err();
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    assert!(error.to_string().contains('x'), "{error}");
}

#[tokio::test]
async fn given_logical_conflict_once_when_retried_then_second_try_succeeds() {
    let (result, calls) = retry_counting(5, |call| {
        (call == 0).then(|| storage_types::StorageError::logical_conflict("policy changed"))
    })
    .await;
    result.expect("second try succeeds");
    assert_eq!(calls, 2);
}

#[tokio::test]
async fn given_commit_unknown_when_retried_then_one_call() {
    let (result, calls) = retry_counting(5, |_| {
        Some(storage_types::StorageError::commit_unknown("u"))
    })
    .await;
    assert!(result.unwrap_err().is_commit_unknown());
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn given_abandoned_attempt_when_retried_then_one_call() {
    let (result, calls) =
        retry_counting(
            5,
            |_| Some(storage_types::StorageError::attempt_abandoned()),
        )
        .await;
    assert!(result.unwrap_err().is_attempt_abandoned());
    assert_eq!(calls, 1);
}

#[tokio::test(start_paused = true)]
async fn given_backoff_past_deadline_when_retried_then_acquire_deadline() {
    let deadline = storage_types::AttemptDeadlines::try_new(
        [std::time::Duration::from_millis(100); storage_types::DeadlineClass::COUNT],
        std::time::Duration::from_secs(1),
    )
    .unwrap()
    .start(storage_types::DeadlineClass::Write);
    let result: storage_types::StorageResult<()> = super::retry::retry_started_tries(
        &storage_types::SystemClock,
        storage_types::AttemptPlan::new(storage_types::AttemptMode::ReadWrite, deadline.class)
            .retrying(storage_types::WriteRetryPolicy::new(
                5,
                std::time::Duration::from_secs(30),
            ))
            .started(deadline),
        |_request, _attempt_try| async {
            Err(storage_types::StorageError::logical_conflict("again"))
        },
    )
    .await;
    assert_eq!(
        result.unwrap_err().deadline_phase(),
        Some(storage_types::AttemptPhase::Acquire)
    );
}

#[tokio::test(start_paused = true)]
async fn given_started_request_when_conflicts_retry_then_every_try_keeps_the_callers_deadline() {
    let deadline =
        storage_types::AttemptDeadlines::default().start(storage_types::DeadlineClass::Write);
    tokio::time::advance(std::time::Duration::from_millis(100)).await;
    let seen = std::sync::Mutex::new(Vec::new());
    let result = super::retry::retry_started_tries(
        &storage_types::SystemClock,
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadWrite,
            storage_types::DeadlineClass::Write,
        )
        .retrying(storage_types::WriteRetryPolicy::new(
            3,
            std::time::Duration::ZERO,
        ))
        .started(deadline),
        |request, attempt_try| {
            seen.lock()
                .unwrap()
                .push((request.deadline.expires_at, attempt_try.number));
            async move {
                if attempt_try.number < 3 {
                    Err(storage_types::StorageError::logical_conflict("again"))
                } else {
                    Ok(attempt_try.number)
                }
            }
        },
    )
    .await;
    assert_eq!(result.expect("third try succeeds"), 3);
    let seen = seen.into_inner().unwrap();
    assert_eq!(
        seen.iter().map(|(_, number)| *number).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(
        seen.iter()
            .all(|(expires_at, _)| *expires_at == deadline.expires_at),
        "a restart would give now + budget"
    );
}

#[tokio::test(start_paused = true)]
async fn given_plan_when_retry_tries_is_polled_later_then_the_deadline_starts_at_first_poll() {
    let budgets = storage_types::AttemptDeadlines::default();
    let recorded = std::sync::Mutex::new(None);
    let future = super::retry::retry_tries(
        &storage_types::SystemClock,
        budgets,
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadOnly,
            storage_types::DeadlineClass::PointRead,
        ),
        |request, _| {
            *recorded.lock().unwrap() = Some(request.deadline.expires_at);
            async { Ok(()) }
        },
    );
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    let polled = tokio::time::Instant::now();
    future.await.unwrap();
    assert_eq!(
        recorded.into_inner().unwrap(),
        Some(polled + budgets.budget(storage_types::DeadlineClass::PointRead))
    );
}

#[tokio::test(start_paused = true)]
async fn given_unknown_outcome_when_retry_started_tries_runs_then_one_try() {
    let calls = AtomicUsize::new(0);
    let result: storage_types::StorageResult<()> = super::retry::retry_started_tries(
        &storage_types::SystemClock,
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadWrite,
            storage_types::DeadlineClass::Write,
        )
        .retrying(storage_types::WriteRetryPolicy::new(
            5,
            std::time::Duration::ZERO,
        ))
        .start(storage_types::AttemptDeadlines::default()),
        |_, _| {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err(storage_types::StorageError::commit_unknown("lost reply")) }
        },
    )
    .await;
    assert!(result.unwrap_err().is_commit_unknown());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
