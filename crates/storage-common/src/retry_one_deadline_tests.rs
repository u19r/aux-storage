//! One process owns this simulated clock; no Tokio reactor is installed.
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use storage_types::{
    AttemptMode, AttemptPhase, AttemptPlan, Clock, DeadlineClass, StorageEnum, StorageResult,
    WriteRetryPolicy,
    ambient::{AmbientSource, install_ambient_source},
};

struct SimulatedSource(Arc<AtomicI64>);
impl AmbientSource for SimulatedSource {
    fn now_millis(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
    fn next_u64(&self) -> u64 {
        0
    }
}

struct SimulatedClock(Arc<AtomicI64>);
impl Clock for SimulatedClock {
    fn now(&self) -> storage_types::TimestampMillis {
        storage_types::TimestampMillis::now()
    }
    fn sleep(
        &self,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async move {
            self.0
                .fetch_add(duration.as_millis() as i64, Ordering::Relaxed);
            Ok(())
        })
    }
}

#[test]
fn given_known_conflicts_when_retrying_then_one_deadline_bounds_all_attempts() {
    let millis = Arc::new(AtomicI64::new(0));
    assert!(install_ambient_source(Box::new(SimulatedSource(
        Arc::clone(&millis)
    ))));
    let clock = SimulatedClock(Arc::clone(&millis));
    let mut budgets = [Duration::from_secs(1); DeadlineClass::COUNT];
    budgets[DeadlineClass::Write.index()] = Duration::from_millis(1_000);
    let budgets = storage_types::AttemptDeadlines::try_new(budgets, Duration::from_millis(100))
        .expect("positive budgets");

    let mut expiries = Vec::new();
    let result: StorageResult<()> = {
        let future = crate::retry::retry_native_conflicts(
            &clock,
            // Backoff after attempt n is half of 100 ms * 2^(n-1): 50, 100,
            // 200, 400, 500.
            WriteRetryPolicy::new(100, Duration::from_millis(100)),
            budgets,
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write),
            |request| {
                let deadline = request.deadline;
                expiries.push(deadline.expires_at);
                // Each attempt consumes 20 ms of the same budget.
                millis.fetch_add(20, Ordering::Relaxed);
                async {
                    Err(
                        StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                            storage_types::ConflictCause::Logical("known abort"),
                        ))
                        .into(),
                    )
                }
            },
        );
        let mut future = std::pin::pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("deterministic source never waits on wall time"),
        }
    };

    // t=0: attempt 1 ends 20, sleep 50 -> 70; attempt 2 -> 90, sleep 100 ->
    // 190; attempt 3 -> 210, sleep 200 -> 410; attempt 4 -> 430, sleep 400 ->
    // 830; attempt 5 -> 850: the next 500 ms backoff exceeds the 150 ms left.
    assert_eq!(
        expiries.len(),
        5,
        "a fresh budget per attempt would allow 100 attempts"
    );
    assert!(
        expiries.windows(2).all(|pair| pair[0] == pair[1]),
        "one deadline for every attempt"
    );
    assert!(matches!(
        result
            .expect_err("the deadline ends the operation")
            .deadline_phase(),
        Some(AttemptPhase::Acquire)
    ));
}
