use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use super::{
    AdmissionClass, AdmissionConfig, AdmissionController, AdmissionOutcome, AdmissionRegistry,
    AdmissionRejectionReason, Waiter,
};

fn config(
    maximum_concurrency: usize,
    control_reserve_concurrency: usize,
    queue_capacity: usize,
    max_queue_wait_ms: u64,
) -> AdmissionConfig {
    AdmissionConfig {
        initial_sustainable_throughput_rps: 1,
        initial_latency_estimate_ms: 1,
        minimum_concurrency: 1,
        maximum_concurrency,
        control_reserve_concurrency,
        queue_capacity,
        max_queue_wait_ms,
        ..AdmissionConfig::default()
    }
}

fn class_counts(controller: &AdmissionController) -> [usize; 3] {
    let state = super::lock_state(&controller.inner);
    state.in_flight_by_class
}

fn assert_poststate(
    controller: &AdmissionController,
    in_flight: usize,
    classes: [usize; 3],
    queue_depth: usize,
    rejection_count: u64,
) {
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.in_flight, in_flight);
    assert_eq!(class_counts(controller), classes);
    assert_eq!(snapshot.queue_depth, queue_depth);
    assert_eq!(snapshot.rejection_count, rejection_count);
}

async fn wait_until_queue_depth(controller: &AdmissionController, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if controller.snapshot().queue_depth == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queue reaches its expected depth");
}

#[tokio::test]
async fn given_full_foreground_queue_when_new_work_arrives_then_poststate_reports_fifo_and_rejection_count()
 {
    let controller = AdmissionController::new("conformance", config(3, 1, 2, 1_000))
        .expect("valid admission config");
    let held = controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("initial foreground permit");
    let (granted_tx, mut granted_rx) = mpsc::unbounded_channel();
    let mut releases = Vec::new();
    let mut workers = Vec::new();

    for (index, class) in [(0, AdmissionClass::RangeRead), (1, AdmissionClass::Write)] {
        let (release_tx, release_rx) = oneshot::channel();
        releases.push(release_tx);
        let worker_controller = controller.clone();
        let worker_granted_tx = granted_tx.clone();
        workers.push(tokio::spawn(async move {
            let permit = worker_controller
                .acquire(class)
                .await
                .expect("queued work is eventually admitted");
            worker_granted_tx
                .send((index, class))
                .expect("conformance receiver remains open");
            release_rx.await.expect("test releases queued work");
            permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
        }));
        wait_until_queue_depth(&controller, index + 1).await;
    }
    drop(granted_tx);

    let rejection = controller
        .acquire(AdmissionClass::PointRead)
        .await
        .expect_err("a request beyond the bounded queue is rejected");
    assert_eq!(rejection.reason, AdmissionRejectionReason::QueueFull);
    assert_eq!(rejection.retry_after_seconds, 1);
    assert_poststate(&controller, 1, [1, 0, 0], 2, 1);

    held.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    for expected in [(0, AdmissionClass::RangeRead), (1, AdmissionClass::Write)] {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), granted_rx.recv())
                .await
                .expect("queued work receives a grant")
                .expect("grant sender remains open"),
            expected
        );
        let (classes, queue_depth) = if expected.1 == AdmissionClass::RangeRead {
            ([0, 1, 0], 1)
        } else {
            ([0, 0, 1], 0)
        };
        assert_poststate(&controller, 1, classes, queue_depth, 1);
        releases
            .remove(0)
            .send(())
            .expect("queued worker remains alive");
    }
    for worker in workers {
        worker.await.expect("queued worker task");
    }
    assert_poststate(&controller, 0, [0, 0, 0], 0, 1);
}

#[tokio::test]
async fn given_granted_waiter_when_grant_is_dropped_then_next_fifo_waiter_is_woken() {
    let controller = AdmissionController::new("grant-conformance", config(2, 1, 2, 1_000))
        .expect("valid admission config");
    let held = controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("initial foreground permit");
    let (first_sender, first_receiver) = oneshot::channel();
    let (second_sender, second_receiver) = oneshot::channel();
    {
        let mut state = super::lock_state(&controller.inner);
        state.queue.push_back(Waiter {
            id: 1,
            class: AdmissionClass::RangeRead,
            enqueued_at: Instant::now(),
            sender: first_sender,
        });
        state.queue.push_back(Waiter {
            id: 2,
            class: AdmissionClass::Write,
            enqueued_at: Instant::now(),
            sender: second_sender,
        });
    }

    held.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    let first_grant = first_receiver.await.expect("first waiter receives a grant");
    assert_poststate(&controller, 1, [0, 1, 0], 1, 0);
    drop(first_grant);

    let second_grant = tokio::time::timeout(Duration::from_secs(1), second_receiver)
        .await
        .expect("dropping a grant wakes the next waiter")
        .expect("second waiter receives a grant");
    assert_poststate(&controller, 1, [0, 0, 1], 0, 0);
    drop(second_grant);
    assert_poststate(&controller, 0, [0, 0, 0], 0, 0);
}

#[tokio::test]
async fn given_held_permit_when_waiter_cancels_or_times_out_then_capacity_and_classes_are_reclaimed()
 {
    let controller = AdmissionController::new("reclaim-conformance", config(2, 1, 1, 10))
        .expect("valid admission config");
    let held = controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("initial foreground permit");

    let cancelled_controller = controller.clone();
    let cancelled = tokio::spawn(async move {
        cancelled_controller
            .acquire(AdmissionClass::RangeRead)
            .await
    });
    wait_until_queue_depth(&controller, 1).await;
    cancelled.abort();
    assert!(
        cancelled
            .await
            .expect_err("waiter task is cancelled")
            .is_cancelled()
    );
    assert_poststate(&controller, 1, [1, 0, 0], 0, 0);

    let timed_out_controller = controller.clone();
    let timed_out = tokio::spawn(async move {
        timed_out_controller
            .acquire(AdmissionClass::Write)
            .await
            .expect_err("queued work reaches its deadline")
    });
    let rejection = timed_out.await.expect("timeout task");
    assert_eq!(rejection.reason, AdmissionRejectionReason::QueueTimedOut);
    assert_poststate(&controller, 1, [1, 0, 0], 0, 1);

    drop(held);
    assert_poststate(&controller, 0, [0, 0, 0], 0, 1);
    let reusable = controller
        .try_acquire(AdmissionClass::Write)
        .expect("cancelled permit returns capacity");
    reusable.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    assert_poststate(&controller, 0, [0, 0, 0], 0, 1);
}

#[tokio::test]
async fn given_in_flight_work_when_limit_is_lowered_then_existing_work_drains_before_queue_wakeup()
{
    let controller = AdmissionController::new(
        "lower-limit-conformance",
        AdmissionConfig {
            initial_sustainable_throughput_rps: 800,
            initial_latency_estimate_ms: 5,
            minimum_concurrency: 1,
            maximum_concurrency: 5,
            control_reserve_concurrency: 1,
            queue_capacity: 1,
            max_queue_wait_ms: 1_000,
            ..AdmissionConfig::default()
        },
    )
    .expect("valid admission config");
    let point_one = controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("point permit 1");
    let range = controller
        .try_acquire(AdmissionClass::RangeRead)
        .expect("range permit");
    let write = controller
        .try_acquire(AdmissionClass::Write)
        .expect("write permit");
    let point_two = controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("point permit 2");
    let (granted_tx, granted_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let waiter_controller = controller.clone();
    let waiter = tokio::spawn(async move {
        let permit = waiter_controller
            .acquire(AdmissionClass::Write)
            .await
            .expect("queued work is eventually admitted");
        granted_tx.send(()).expect("grant receiver remains open");
        release_rx.await.expect("test releases queued work");
        permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    });
    wait_until_queue_depth(&controller, 1).await;

    point_one.complete(AdmissionOutcome::ExplicitThrottle);
    assert_eq!(controller.snapshot().desired_limit, 2);
    assert_poststate(&controller, 3, [1, 1, 1], 1, 0);
    range.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    assert_poststate(&controller, 2, [1, 0, 1], 1, 0);
    write.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    tokio::time::timeout(Duration::from_secs(1), granted_rx)
        .await
        .expect("queue wakes after in-flight drops below the lowered limit")
        .expect("queued grant sender remains open");
    assert_poststate(&controller, 2, [1, 0, 1], 0, 0);
    release_tx.send(()).expect("queued worker remains alive");
    waiter.await.expect("queued worker task");
    point_two.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    assert_poststate(&controller, 0, [0, 0, 0], 0, 0);
}

#[test]
fn given_exhausted_control_reserve_when_control_arrives_then_foreground_state_stays_independent() {
    let controller = AdmissionController::new("control-conformance", config(4, 2, 0, 1_000))
        .expect("valid admission config");
    let first = controller
        .try_acquire_control()
        .expect("first control slot");
    let second = controller
        .try_acquire_control()
        .expect("second control slot");
    let rejection = match controller.try_acquire_control() {
        Ok(permit) => {
            drop(permit);
            panic!("control reserve is bounded");
        }
        Err(rejection) => rejection,
    };
    assert_eq!(
        rejection.reason,
        AdmissionRejectionReason::ControlReserveExhausted
    );
    assert_eq!(rejection.retry_after_seconds, 1);
    assert_poststate(&controller, 0, [0, 0, 0], 0, 1);
    assert_eq!(controller.snapshot().control_in_flight, 2);

    let foreground = controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("foreground capacity is independent of control slots");
    assert_poststate(&controller, 1, [1, 0, 0], 0, 1);
    assert_eq!(controller.snapshot().control_in_flight, 2);
    foreground.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    drop(first);
    drop(second);
    assert_poststate(&controller, 0, [0, 0, 0], 0, 1);
    assert_eq!(controller.snapshot().control_in_flight, 0);
}

#[tokio::test]
async fn given_one_connection_is_overloaded_when_another_admits_then_state_remains_isolated() {
    let registry = AdmissionRegistry::new(
        "default",
        ["default".to_string(), "secondary".to_string()],
        config(2, 1, 1, 1_000),
    )
    .expect("valid registry");
    let default_controller = registry.default_controller().clone();
    let secondary_controller = registry
        .for_connection("secondary")
        .expect("secondary controller")
        .clone();
    let held = default_controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("default foreground permit");
    let waiter_controller = default_controller.clone();
    let waiter = tokio::spawn(async move {
        waiter_controller
            .acquire(AdmissionClass::RangeRead)
            .await
            .expect("default queue waiter");
    });
    wait_until_queue_depth(&default_controller, 1).await;
    let rejection = default_controller
        .acquire(AdmissionClass::Write)
        .await
        .expect_err("default queue is full");
    assert_eq!(rejection.reason, AdmissionRejectionReason::QueueFull);
    let secondary = secondary_controller
        .try_acquire(AdmissionClass::Write)
        .expect("secondary connection remains available");
    assert_poststate(&default_controller, 1, [1, 0, 0], 1, 1);
    assert_poststate(&secondary_controller, 1, [0, 0, 1], 0, 0);
    secondary.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    drop(held);
    waiter.await.expect("queued test waiter is admitted");
    assert_poststate(&default_controller, 0, [0, 0, 0], 0, 1);
    assert_poststate(&secondary_controller, 0, [0, 0, 0], 0, 0);
}
