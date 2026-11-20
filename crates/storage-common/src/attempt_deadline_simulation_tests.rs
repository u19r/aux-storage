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
    AttemptDeadlines, Clock, DeadlineClass, StorageResult, WriteRetryPolicy,
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
fn finish<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("deterministic source never waits on wall time"),
    }
}

#[test]
fn given_simulated_time_without_reactor_when_replaying_then_backoff_and_deadline_use_source() {
    let millis = Arc::new(AtomicI64::new(0));
    assert!(install_ambient_source(Box::new(SimulatedSource(
        Arc::clone(&millis)
    ))));
    let clock = SimulatedClock(Arc::clone(&millis));
    let deadline = AttemptDeadlines::default().start(DeadlineClass::Write);
    let mut calls = 0;
    let replay = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        finish(crate::retry::retry_started_tries(
            &clock,
            storage_types::AttemptPlan::new(storage_types::AttemptMode::ReadWrite, deadline.class)
                .retrying(WriteRetryPolicy::new(2, Duration::from_millis(100)))
                .started(deadline),
            |_request, _attempt_try| {
                calls += 1;
                let call = calls;
                async move {
                    if call == 1 {
                        Err(storage_types::StorageEnum::Attempt(
                            storage_types::AttemptFailure::Conflict(
                                storage_types::ConflictCause::Logical("known abort"),
                            ),
                        )
                        .into())
                    } else {
                        Ok(7)
                    }
                }
            },
        ))
    }));
    millis.fetch_add(10_000, Ordering::Relaxed);
    let virtual_expiry = deadline
        .require_remaining(storage_types::AttemptPhase::Acquire)
        .is_err();
    eprintln!(
        "reactor_free_backoff={} virtual_expiry={virtual_expiry}",
        replay.is_ok()
    );
    assert!(replay.is_ok(), "backoff must not require a Tokio reactor");
    assert_eq!(replay.unwrap().unwrap(), 7);
    assert_eq!(calls, 2);
    assert!(
        virtual_expiry,
        "virtual elapsed time must expire the same operation"
    );
}
