use std::{
    sync::{Arc, Barrier},
    time::Duration,
};

use storage_types::{
    AttemptDeadline, AttemptDeadlines, AttemptKind, AttemptPhase, Clock, DeadlineClass,
    NativeCallPhase, NativeCause, NativeCode, NativeEngine, StorageEnum, StorageError,
    StorageResult, SystemClock, TimestampMillis,
};
use tokio::time::Instant;

use super::native_attempt_lifecycle::*;
use crate::{
    native_observer::NativeObserver, native_observer_tests::RecordingObserver,
    native_outcome::NativeCommitError, native_supervision_tests::drive_to,
};

/// Bound of every wait in this file.
const WAIT_BOUND: Duration = Duration::from_secs(10);

fn deadline() -> AttemptDeadline {
    AttemptDeadlines::default().start(DeadlineClass::Write)
}

/// Call inside a Tokio test.
fn expired() -> AttemptDeadline {
    AttemptDeadline {
        expires_at: Instant::now() - Duration::from_millis(1),
        ..deadline()
    }
}

fn is_deadline(error: &StorageError, phase: AttemptPhase) -> bool {
    error.deadline_phase() == Some(phase)
}

/// A clock whose sleep always fails.
struct FailingClock;

impl Clock for FailingClock {
    fn now(&self) -> TimestampMillis {
        SystemClock.now()
    }

    fn sleep(
        &self,
        _: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async { Err(StorageError::internal("sleep failed")) })
    }
}

#[test]
fn given_open_attempt_when_commit_answers_success_then_committed() {
    let lifecycle = AttemptLifecycle::new();
    assert_eq!(lifecycle.state(), LifecycleState::Queued);
    assert!(lifecycle.open());
    assert_eq!(lifecycle.state(), LifecycleState::Open);
    let ticket = lifecycle.submit_commit(&deadline()).expect("submitted");
    assert_eq!(lifecycle.state(), LifecycleState::CommitSubmitted);
    assert!(lifecycle.awaits_answer());
    ticket.settle(Ok(())).expect("success is returned");
    assert_eq!(lifecycle.state(), LifecycleState::Committed);
    assert!(!lifecycle.awaits_answer());
}

#[test]
fn given_submitted_commit_when_answer_is_known_failure_or_unknown_then_aborted_or_still_submitted()
{
    let submitted = || {
        let lifecycle = AttemptLifecycle::new();
        assert!(lifecycle.open());
        lifecycle
    };

    let lifecycle = submitted();
    let ticket = lifecycle.submit_commit(&deadline()).expect("submitted");
    let error = ticket
        .settle::<()>(Err(StorageError::internal("x").into()))
        .expect_err("failure is returned");
    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { message } if message == "x"
    ));
    assert_eq!(lifecycle.state(), LifecycleState::Aborted);

    let lifecycle = submitted();
    let ticket = lifecycle.submit_commit(&deadline()).expect("submitted");
    let error = ticket
        .settle::<()>(Err(StorageError::commit_unknown("x").into()))
        .expect_err("unknown is returned");
    assert!(error.is_commit_unknown());
    assert_eq!(lifecycle.state(), LifecycleState::CommitSubmitted);

    let lifecycle = submitted();
    drop(lifecycle.submit_commit(&deadline()).expect("submitted"));
    assert_eq!(lifecycle.state(), LifecycleState::CommitSubmitted);
}

#[test]
fn given_abandoned_attempt_when_opened_or_submitted_then_refused() {
    let lifecycle = AttemptLifecycle::new();
    assert_eq!(lifecycle.abandon(), LifecycleState::Queued);
    assert_eq!(lifecycle.state(), LifecycleState::Abandoned);
    assert!(!lifecycle.open());

    let lifecycle = AttemptLifecycle::new();
    assert!(lifecycle.open());
    assert_eq!(lifecycle.abandon(), LifecycleState::Open);
    let error = lifecycle
        .submit_commit(&deadline())
        .err()
        .expect("refused after abandon");
    assert!(is_deadline(&error, AttemptPhase::Settle));
    assert_eq!(lifecycle.state(), LifecycleState::Abandoned);
    assert_eq!(lifecycle.abandon(), LifecycleState::Abandoned);
}

#[test]
fn given_submitted_commit_when_abandoned_then_unchanged() {
    let lifecycle = AttemptLifecycle::new();
    assert!(lifecycle.open());
    let ticket = lifecycle.submit_commit(&deadline()).expect("submitted");
    assert_eq!(lifecycle.abandon(), LifecycleState::CommitSubmitted);
    assert_eq!(lifecycle.state(), LifecycleState::CommitSubmitted);
    ticket.settle(Ok(())).expect("success");
    assert_eq!(lifecycle.state(), LifecycleState::Committed);
    assert_eq!(lifecycle.abandon(), LifecycleState::Committed);
    assert_eq!(lifecycle.state(), LifecycleState::Committed);
}

#[tokio::test(start_paused = true)]
async fn given_expired_deadline_when_submitting_then_known_settle_expiry_and_still_open() {
    let lifecycle = AttemptLifecycle::new();
    assert!(lifecycle.open());
    let error = lifecycle
        .submit_commit(&expired())
        .err()
        .expect("expired deadline refuses");
    assert!(is_deadline(&error, AttemptPhase::Settle));
    assert_eq!(lifecycle.state(), LifecycleState::Open);
}

#[test]
fn given_unopened_attempt_when_submitting_then_internal_error() {
    let lifecycle = AttemptLifecycle::new();
    let error = lifecycle
        .submit_commit(&deadline())
        .err()
        .expect("queued attempt cannot submit");
    assert!(!error.is_commit_unknown());
    assert!(!is_deadline(&error, AttemptPhase::Settle));
    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { .. }
    ));
    assert_eq!(lifecycle.state(), LifecycleState::Queued);
}

#[test]
fn given_each_state_when_task_stopped_then_known_only_before_commit_submission() {
    // (state at the stop, unknown outcome, state after the stop)
    for (state, unknown, after) in [
        (LifecycleState::Queued, false, LifecycleState::Abandoned),
        (LifecycleState::Open, false, LifecycleState::Abandoned),
        (
            LifecycleState::CommitSubmitted,
            true,
            LifecycleState::CommitSubmitted,
        ),
        (LifecycleState::Committed, true, LifecycleState::Committed),
        (LifecycleState::Aborted, false, LifecycleState::Aborted),
        (LifecycleState::Abandoned, false, LifecycleState::Abandoned),
    ] {
        let lifecycle = AttemptLifecycle::new();
        if state == LifecycleState::Abandoned {
            drive_to(&lifecycle, LifecycleState::Open);
            assert_eq!(lifecycle.abandon(), LifecycleState::Open);
        } else {
            drive_to(&lifecycle, state);
        }
        let error = lifecycle.stopped_error("stopped");
        assert_eq!(error.is_commit_unknown(), unknown, "{state:?}");
        assert_eq!(lifecycle.state(), after, "{state:?}");
    }
}

#[test]
fn given_concurrent_abandon_and_submit_when_racing_then_exactly_one_wins() {
    let deadline = deadline();
    for round in 0..1_000 {
        let lifecycle = Arc::new(AttemptLifecycle::new());
        assert!(lifecycle.open());
        let barrier = Arc::new(Barrier::new(2));
        let submitter = {
            let (lifecycle, barrier) = (Arc::clone(&lifecycle), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                lifecycle.submit_commit(&deadline).is_ok()
            })
        };
        let abandoner = {
            let (lifecycle, barrier) = (Arc::clone(&lifecycle), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                lifecycle.abandon()
            })
        };
        let submitted = submitter.join().unwrap();
        let abandoned_from = abandoner.join().unwrap();
        if submitted {
            assert_eq!(
                abandoned_from,
                LifecycleState::CommitSubmitted,
                "round {round}"
            );
            assert_eq!(
                lifecycle.state(),
                LifecycleState::CommitSubmitted,
                "round {round}"
            );
        } else {
            assert_eq!(abandoned_from, LifecycleState::Open, "round {round}");
            assert_eq!(
                lifecycle.state(),
                LifecycleState::Abandoned,
                "round {round}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn given_work_done_before_bound_when_awaiting_settle_then_its_result() {
    let value = await_settle(
        &SystemClock,
        deadline(),
        async { Ok(7) },
        LateSettle::Unknown("late"),
    )
    .await
    .expect("work result");
    assert_eq!(value, 7);

    let error = await_settle(
        &SystemClock,
        deadline(),
        async { Err::<u8, _>(StorageError::internal("x")) },
        LateSettle::Unknown("late"),
    )
    .await
    .expect_err("work error");
    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { message } if message == "x"
    ));
}

#[tokio::test(start_paused = true)]
async fn given_work_past_bound_when_awaiting_settle_then_late_error_and_work_finishes_detached() {
    for late in [LateSettle::Unknown("late"), LateSettle::Expired] {
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let (done, finished) = tokio::sync::oneshot::channel::<()>();
        let work = async move {
            released.await.ok();
            done.send(()).ok();
            Ok(())
        };
        let error = await_settle(&SystemClock, deadline(), work, late)
            .await
            .expect_err("the bound passed");
        match late {
            LateSettle::Unknown(_) => assert!(error.is_commit_unknown()),
            LateSettle::Expired => assert!(is_deadline(&error, AttemptPhase::Settle)),
        }
        release.send(()).unwrap();
        tokio::time::timeout(WAIT_BOUND, finished)
            .await
            .expect("detached work finished within the bound")
            .expect("detached work sent its completion");
    }
    assert!(matches!(
        LateSettle::of_write(true, "m"),
        LateSettle::Unknown("m")
    ));
    assert!(matches!(
        LateSettle::of_write(false, "m"),
        LateSettle::Expired
    ));
}

#[tokio::test]
async fn given_failing_clock_when_awaiting_settle_then_bound_counts_as_reached() {
    let result = tokio::time::timeout(
        WAIT_BOUND,
        await_settle(
            &FailingClock,
            deadline(),
            std::future::pending::<StorageResult<()>>(),
            LateSettle::Expired,
        ),
    )
    .await
    .expect("a failed sleep must end the wait, not hang it");
    let error = result.expect_err("bound reached");
    assert!(is_deadline(&error, AttemptPhase::Settle));
}

#[tokio::test(start_paused = true)]
async fn given_completed_work_when_bound_already_passed_then_settle_or_drop_returns_it() {
    assert_eq!(
        settle_or_drop(&SystemClock, expired(), std::future::ready(5)).await,
        Some(5)
    );
    let passed = AttemptDeadline {
        expires_at: Instant::now() - Duration::from_secs(3),
        commit_grace: Duration::from_secs(1),
        ..deadline()
    };
    assert_eq!(
        settle_or_drop(&SystemClock, passed, std::future::pending::<u8>()).await,
        None
    );
}

fn submitted_lifecycle() -> AttemptLifecycle {
    let lifecycle = AttemptLifecycle::new();
    assert!(lifecycle.open());
    lifecycle
}

#[test]
fn given_ticket_when_settled_with_unproven_then_unknown_and_still_submitted() {
    let lifecycle = submitted_lifecycle();
    let ticket = lifecycle.submit_commit(&deadline()).expect("submitted");
    let error = ticket
        .settle::<()>(Err(NativeCommitError::Unproven {
            reason: "reply lost",
            cause: None,
        }))
        .expect_err("unknown is returned");
    assert!(error.is_commit_unknown());
    assert_eq!(lifecycle.state(), LifecycleState::CommitSubmitted);
}

#[test]
fn given_ticket_when_settled_with_conflict_then_aborted_and_conflict() {
    let lifecycle = submitted_lifecycle();
    let ticket = lifecycle.submit_commit(&deadline()).expect("submitted");
    let cause = NativeCause {
        engine: NativeEngine::Sqlite,
        code: NativeCode::Sqlite(5),
        phase: NativeCallPhase::Commit,
    };
    let error = ticket
        .settle::<()>(Err(NativeCommitError::Conflict(cause)))
        .expect_err("conflict is returned");
    assert_eq!(error.attempt_kind(), Some(AttemptKind::Conflict));
    assert_eq!(lifecycle.state(), LifecycleState::Aborted);
}

#[test]
fn given_submitted_attempt_when_task_stopped_then_display_names_unknown() {
    let lifecycle = submitted_lifecycle();
    drop(lifecycle.submit_commit(&deadline()).expect("submitted"));
    assert_eq!(
        lifecycle
            .stopped_error("SQLite attempt task stopped")
            .to_string(),
        "commit outcome unknown: SQLite attempt task stopped"
    );
}

#[test]
fn new_lifecycle_has_no_observer() {
    let lifecycle = AttemptLifecycle::new();
    assert!(lifecycle.observer().is_none());
    assert!(lifecycle.observer_handle().is_none());
}

#[test]
fn observed_lifecycle_lends_its_observer() {
    let installed: Arc<dyn NativeObserver> = Arc::new(RecordingObserver::new(false));
    let lifecycle = AttemptLifecycle::observed(Some(Arc::clone(&installed)));
    assert!(lifecycle.observer().is_some());
    let handle = lifecycle.observer_handle().expect("handle");
    assert_eq!(
        Arc::as_ptr(&handle) as *const (),
        Arc::as_ptr(&installed) as *const ()
    );
}

#[test]
fn observer_does_not_change_state_moves() {
    let installed: Arc<dyn NativeObserver> = Arc::new(RecordingObserver::new(false));
    let lifecycle = AttemptLifecycle::observed(Some(installed));
    assert!(lifecycle.open());
    let ticket = lifecycle.submit_commit(&deadline()).expect("submitted");
    ticket.settle(Ok(())).expect("success is returned");
    assert_eq!(lifecycle.state(), LifecycleState::Committed);
}
