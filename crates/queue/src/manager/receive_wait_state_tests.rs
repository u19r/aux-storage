//! Ordering of recorded observations (W2 D17, hole H7 of
//! `quint/queue_wake.qnt`) and the claim's next due time (D16).

use std::time::Duration;

use queue_provider::{QueueWakeBoard, QueueWakeSlot};
use tokio::time::Instant;

use super::receive_wait::{Observed, QueueWaitState, ReceiveWaiters};

const INTERVAL: Duration = Duration::from_secs(60);

fn probe(next_due: Option<Instant>) -> Observed {
    Observed::Probe {
        next_due,
        wait_time_seconds: 0,
    }
}

#[tokio::test(start_paused = true)]
async fn given_two_observations_in_one_instant_when_the_older_finishes_last_then_the_newer_result_stays()
 {
    let state = QueueWaitState::new_for_test();
    let older = state.start();
    let newer = state.start();
    let due = Instant::now();
    state.record(newer, probe(Some(due)));
    state.record(older, probe(None));
    assert!(state.current().is_due(Instant::now(), INTERVAL, None));

    // An older observation can still lower the next due time.
    let state = QueueWaitState::new_for_test();
    let older = state.start();
    let newer = state.start();
    state.record(newer, probe(Some(due + Duration::from_secs(5))));
    state.record(older, probe(Some(due)));
    assert!(state.current().is_due(Instant::now(), INTERVAL, None));
}

/// D16: a drained claim keeps the next due time that it reports (the lease
/// end of a message that another consumer holds); an empty claim that left
/// due messages is due again after its backoff, not at once.
#[tokio::test(start_paused = true)]
async fn given_claims_when_recorded_then_reported_due_times_and_backoff_are_kept() {
    let state = QueueWaitState::new_for_test();
    let start = state.start();
    let lease_end = Instant::now() + Duration::from_secs(2);
    state.record(
        start,
        Observed::Claim {
            took: false,
            drained: true,
            next_due: Some(lease_end),
            backoff: Duration::from_millis(10),
            wait_time_seconds: 0,
        },
    );
    let observation = state.current();
    assert!(!observation.is_due(Instant::now(), INTERVAL, None));
    assert_eq!(
        observation.wake_at(Instant::now() + INTERVAL, INTERVAL, None),
        lease_end
    );

    let start = state.start();
    state.record(
        start,
        Observed::Claim {
            took: false,
            drained: false,
            next_due: None,
            backoff: Duration::from_millis(10),
            wait_time_seconds: 0,
        },
    );
    assert!(!state.current().is_due(Instant::now(), INTERVAL, None));
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(state.current().is_due(Instant::now(), INTERVAL, None));
    assert!(!state.current().is_deep(Instant::now(), INTERVAL, None));
}

#[tokio::test(start_paused = true)]
async fn given_covered_observation_when_an_hour_passes_then_it_stays_fresh_until_the_slot_advances()
{
    let board = QueueWakeBoard::local();
    let slot = QueueWakeSlot::of_queue("q");
    let waiters = ReceiveWaiters::new(10, INTERVAL, Some(board.clone()));
    let state = waiters.queue("q");
    let start = state.start();
    state.record(start, probe(None));
    tokio::time::advance(Duration::from_secs(3_600)).await;
    assert!(
        !state
            .current()
            .needs_probe(Instant::now(), INTERVAL, state.wake_mark())
    );

    // Observe again, then let the slot advance: re-probe after the spacing.
    let start = state.start();
    state.record(start, probe(None));
    let completed = Instant::now();
    board.advance(slot);
    tokio::time::advance(Duration::from_millis(199)).await;
    assert!(
        !state
            .current()
            .needs_probe(Instant::now(), INTERVAL, state.wake_mark())
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(Instant::now() - completed, Duration::from_millis(200));
    assert!(
        state
            .current()
            .needs_probe(Instant::now(), INTERVAL, state.wake_mark())
    );
}

#[tokio::test(start_paused = true)]
async fn given_uncovered_slot_when_observed_then_interval_rule_applies() {
    let board = QueueWakeBoard::feed(false);
    let waiters = ReceiveWaiters::new(10, INTERVAL, Some(board));
    let state = waiters.queue("q");
    let start = state.start();
    state.record(start, probe(None));
    tokio::time::advance(INTERVAL - Duration::from_millis(1)).await;
    assert!(
        !state
            .current()
            .needs_probe(Instant::now(), INTERVAL, state.wake_mark())
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(
        state
            .current()
            .needs_probe(Instant::now(), INTERVAL, state.wake_mark())
    );
}

#[tokio::test(start_paused = true)]
async fn given_covered_observation_when_slot_loses_coverage_then_it_is_stale_after_spacing() {
    let board = QueueWakeBoard::feed(false);
    let slot = QueueWakeSlot::of_queue("q");
    board.cover(slot);
    let waiters = ReceiveWaiters::new(10, INTERVAL, Some(board.clone()));
    let state = waiters.queue("q");
    let start = state.start();
    state.record(start, probe(None));
    board.uncover(slot);
    tokio::time::advance(Duration::from_millis(199)).await;
    assert!(
        !state
            .current()
            .needs_probe(Instant::now(), INTERVAL, state.wake_mark())
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(
        state
            .current()
            .needs_probe(Instant::now(), INTERVAL, state.wake_mark())
    );
}

#[tokio::test(start_paused = true)]
async fn given_observation_before_coverage_when_slot_is_covered_then_it_is_stale_after_spacing() {
    let board = QueueWakeBoard::feed(false);
    let slot = QueueWakeSlot::of_queue("q");
    let waiters = ReceiveWaiters::new(10, INTERVAL, Some(board.clone()));
    let state = waiters.queue("q");
    let start = state.start();
    state.record(start, probe(None));
    board.cover(slot);
    tokio::time::advance(Duration::from_millis(199)).await;
    assert!(
        !state
            .current()
            .needs_probe(Instant::now(), INTERVAL, state.wake_mark())
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(
        state
            .current()
            .needs_probe(Instant::now(), INTERVAL, state.wake_mark())
    );
}
