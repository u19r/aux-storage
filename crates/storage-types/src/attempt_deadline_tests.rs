use std::time::Duration;

use tokio::time::Instant;

use crate::{
    AttemptDeadlines, AttemptDurability, AttemptMode, AttemptPhase, AttemptPlan, AttemptScope,
    AttemptTry, Clock, DeadlineClass, NativeWork, ReadVersionPolicy, StorageError, StorageResult,
    SystemClock, TimestampMillis, WriteRetryPolicy,
};

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

fn deadlines() -> AttemptDeadlines {
    AttemptDeadlines::try_new(
        [
            Duration::from_secs(2),
            Duration::from_secs(5),
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_secs(30),
            Duration::from_secs(10),
            Duration::from_secs(1),
        ],
        Duration::from_secs(2),
    )
    .expect("positive budgets")
}

#[tokio::test(start_paused = true)]
async fn deadline_counts_down_with_the_clock() {
    let deadlines = deadlines();
    let started = Instant::now();
    let deadline = deadlines.start(DeadlineClass::Write);
    assert_eq!(deadline.remaining(), Duration::from_secs(3));
    assert_eq!(deadline.settle_by(), started + Duration::from_secs(5));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(deadline.remaining(), Duration::from_secs(2));
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(deadline.remaining(), Duration::ZERO);
}

#[test]
fn zero_budget_is_rejected() {
    let mut budgets = [Duration::from_secs(1); DeadlineClass::COUNT];
    budgets[DeadlineClass::RangeRead.index()] = Duration::ZERO;
    assert!(AttemptDeadlines::try_new(budgets, Duration::from_secs(1)).is_err());
    assert!(
        AttemptDeadlines::try_new(
            [Duration::from_secs(1); DeadlineClass::COUNT],
            Duration::ZERO
        )
        .is_err()
    );
}

#[test]
fn request_with_class_is_durable() {
    let read = AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead);
    assert_eq!(read.class, DeadlineClass::PointRead);
    assert_eq!(read.durability, AttemptDurability::Durable);
    for mode in [AttemptMode::ReadWrite, AttemptMode::ReadWriteItems] {
        let request = AttemptPlan::new(mode, DeadlineClass::Write);
        assert_eq!(request.class, DeadlineClass::Write);
        assert_eq!(request.durability, AttemptDurability::Durable);
    }
}

#[tokio::test(start_paused = true)]
async fn given_started_parent_when_nested_read_is_requested_then_class_and_instant_are_retained() {
    let deadline = deadlines().start(DeadlineClass::Transaction);
    tokio::time::advance(Duration::from_secs(1)).await;
    let request =
        AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).started(deadline);
    assert_eq!(request.class(), DeadlineClass::Transaction);
    assert_eq!(request.deadline.expires_at, deadline.expires_at);
    assert_eq!(request.read_version, crate::ReadVersionPolicy::Fresh);
}

#[test]
fn given_new_request_when_built_then_it_makes_one_try() {
    let request = AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write);
    assert_eq!(request.retry.max_attempts(), 1);
    let started = AttemptDeadlines::default().start(DeadlineClass::Write);
    let request = AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).started(started);
    assert_eq!(request.retry.max_attempts(), 1);
}

#[test]
fn given_retrying_and_with_durability_when_built_then_only_those_fields_change() {
    let request = AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Transaction)
        .retrying(WriteRetryPolicy::new(5, Duration::from_millis(2)))
        .with_durability(AttemptDurability::Relaxed);
    assert_eq!(request.retry.max_attempts(), 5);
    assert_eq!(request.durability, AttemptDurability::Relaxed);
    assert_eq!(request.mode, AttemptMode::ReadWriteItems);
    assert_eq!(request.class, DeadlineClass::Transaction);
    assert_eq!(request.read_version, ReadVersionPolicy::Fresh);
}

#[test]
fn given_try_number_one_when_asked_then_it_is_first() {
    let started_at = TimestampMillis::from_timestamp(1);
    assert!(
        AttemptTry {
            number: 1,
            started_at
        }
        .is_first()
    );
    assert!(
        !AttemptTry {
            number: 2,
            started_at
        }
        .is_first()
    );
}

#[test]
fn given_mode_when_request_is_built_then_work_follows_the_mode() {
    assert_eq!(
        AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).work,
        NativeWork::PointRead
    );
    assert_eq!(
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).work,
        NativeWork::Write
    );
    assert_eq!(
        AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write).work,
        NativeWork::Write
    );
    assert_eq!(
        AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
            .start(AttemptDeadlines::default())
            .work,
        NativeWork::PointRead
    );
}

#[test]
fn given_work_builder_when_applied_then_only_work_changes() {
    let request = AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::RangeRead)
        .retrying(WriteRetryPolicy::new(5, Duration::from_millis(2)))
        .work(NativeWork::RangeRead);
    assert_eq!(request.work, NativeWork::RangeRead);
    assert_eq!(request.mode, AttemptMode::ReadOnly);
    assert_eq!(request.class, DeadlineClass::RangeRead);
    assert_eq!(request.durability, AttemptDurability::Durable);
    assert_eq!(request.retry.max_attempts(), 5);
}

#[tokio::test(start_paused = true)]
async fn given_deadline_when_settled_then_it_expires_at_settle_by() {
    let d = deadlines().start(DeadlineClass::Write);
    assert_eq!(d.settle().expires_at, d.settle_by());
    assert_eq!(d.settle().class, d.class);
}

#[tokio::test(start_paused = true)]
async fn given_failing_clock_when_waiting_for_expiry_then_it_completes() {
    let d = deadlines().start(DeadlineClass::Write);
    assert!(d.remaining() > Duration::ZERO);
    tokio::time::timeout(Duration::from_secs(10), d.expired(&FailingClock))
        .await
        .expect("a failed sleep counts as expiry");
}

#[tokio::test(start_paused = true)]
async fn given_paused_clock_when_deadline_passes_then_expired_completes_and_require_remaining_fails()
 {
    let d = deadlines().start(DeadlineClass::Write);
    d.require_remaining(AttemptPhase::Statement)
        .expect("time remains");
    d.expired(&SystemClock).await;
    let error = d
        .require_remaining(AttemptPhase::Statement)
        .expect_err("deadline passed");
    assert_eq!(error.deadline_phase(), Some(AttemptPhase::Statement));
}

fn plan_budgets() -> AttemptDeadlines {
    let mut budgets = [Duration::from_secs(1); DeadlineClass::COUNT];
    budgets[DeadlineClass::Write.index()] = Duration::from_secs(2);
    AttemptDeadlines::try_new(budgets, Duration::from_secs(1)).expect("positive budgets")
}

#[tokio::test(start_paused = true)]
async fn given_plan_when_started_then_deadline_is_the_class_budget_from_now() {
    let budgets = plan_budgets();
    let plan = AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
        .scoped(AttemptScope::ItemWrite)
        .retrying(WriteRetryPolicy::new(5, Duration::from_millis(2)))
        .with_durability(AttemptDurability::Relaxed)
        .work(NativeWork::Recovery);
    let now = Instant::now();
    let request = plan.start(budgets);
    assert_eq!(request.deadline.expires_at, now + Duration::from_secs(2));
    assert_eq!(request.deadline.class, DeadlineClass::Write);
    assert_eq!(request.class(), DeadlineClass::Write);
    assert_eq!(request.mode, plan.mode);
    assert_eq!(request.scope, plan.scope);
    assert_eq!(request.durability, plan.durability);
    assert_eq!(request.retry.max_attempts(), 5);
    assert_eq!(request.work, plan.work);
    assert_eq!(request.read_version, plan.read_version);
}

#[tokio::test(start_paused = true)]
async fn given_started_deadline_when_plan_is_started_with_it_then_its_instant_and_class_win() {
    let deadline = plan_budgets().start(DeadlineClass::Transaction);
    tokio::time::advance(Duration::from_secs(1)).await;
    let request = AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).started(deadline);
    assert_eq!(request.deadline.expires_at, deadline.expires_at);
    assert_eq!(request.class(), DeadlineClass::Transaction);
}
