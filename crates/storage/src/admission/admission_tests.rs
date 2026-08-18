use std::{
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use tokio::sync::{Barrier, mpsc, oneshot};

use super::{
    AdmissionClass, AdmissionConfig, AdmissionController, AdmissionOutcome, AdmissionRegistry,
    AdmissionRejection, AdmissionRejectionReason, Waiter,
};

#[test]
fn defaults_bootstrap_from_littles_law() {
    let config = AdmissionConfig::default();
    assert_eq!(config.initial_sustainable_throughput_rps, 20_000);
    assert_eq!(config.initial_latency_estimate_ms, 5);
    let controller = AdmissionController::new("default", config).expect("valid admission config");
    assert_eq!(controller.snapshot().desired_limit, 100);
    assert_eq!(controller.snapshot().state, super::AdmissionState::Warmup);
}

#[test]
fn bootstrap_estimate_clamps_to_effective_maximum_without_wrapping() {
    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: u64::MAX,
        initial_latency_estimate_ms: 1,
        minimum_concurrency: 1,
        maximum_concurrency: 16,
        control_reserve_concurrency: 4,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("default", config).expect("valid admission config");
    assert_eq!(controller.snapshot().desired_limit, 12);
}

#[test]
fn validation_rejects_invalid_boundaries_but_allows_an_empty_queue() {
    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: 0,
        ..AdmissionConfig::default()
    };
    assert!(AdmissionConfig::try_new(config).is_err());

    let config = AdmissionConfig {
        initial_latency_estimate_ms: 0,
        ..AdmissionConfig::default()
    };
    assert!(AdmissionConfig::try_new(config).is_err());

    let config = AdmissionConfig {
        minimum_concurrency: 0,
        ..AdmissionConfig::default()
    };
    assert!(AdmissionConfig::try_new(config).is_err());

    let config = AdmissionConfig {
        minimum_concurrency: 8,
        maximum_concurrency: 7,
        ..AdmissionConfig::default()
    };
    assert!(AdmissionConfig::try_new(config).is_err());

    let config = AdmissionConfig {
        queue_capacity: 0,
        ..AdmissionConfig::default()
    };
    assert!(AdmissionConfig::try_new(config).is_ok());

    let config = AdmissionConfig {
        max_queue_wait_ms: 0,
        ..AdmissionConfig::default()
    };
    assert!(AdmissionConfig::try_new(config).is_err());

    let mut config = AdmissionConfig::default();
    config.control_reserve_concurrency = config.maximum_concurrency;
    assert!(AdmissionConfig::try_new(config).is_err());
}

#[test]
fn retry_hint_saturates_at_the_u64_boundary() {
    let rejection = AdmissionRejection::new(AdmissionRejectionReason::QueueTimedOut, u64::MAX);

    assert_eq!(rejection.reason, AdmissionRejectionReason::QueueTimedOut);
    assert_eq!(rejection.retry_after_seconds, u64::MAX / 1_000);
}

#[test]
fn disabled_controller_keeps_a_fixed_bounded_limit() {
    let config = AdmissionConfig {
        enabled: false,
        minimum_concurrency: 1,
        maximum_concurrency: 4,
        control_reserve_concurrency: 1,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("default", config).expect("valid admission config");
    let permits = (0..3)
        .map(|_| {
            controller
                .try_acquire(AdmissionClass::PointRead)
                .expect("fixed foreground limit")
        })
        .collect::<Vec<_>>();

    assert_eq!(controller.snapshot().state, super::AdmissionState::Stable);
    assert_eq!(controller.snapshot().desired_limit, 3);
    assert!(controller.try_acquire(AdmissionClass::Write).is_err());
    drop(permits);
}

#[test]
fn provider_pressure_enters_emergency_without_waiting_for_a_full_window() {
    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: 1_000,
        initial_latency_estimate_ms: 10,
        minimum_concurrency: 1,
        maximum_concurrency: 100,
        control_reserve_concurrency: 1,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("immediate-pressure", config).expect("valid config");
    let permit = controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("initial permit");

    permit.complete(AdmissionOutcome::ExplicitThrottle);

    let snapshot = controller.snapshot();
    assert_eq!(snapshot.state, super::AdmissionState::Emergency);
    assert_eq!(snapshot.desired_limit, 5);
}

#[test]
fn immediate_admission_path_has_no_per_request_metric_allocations() {
    const ISOLATED_ENV: &str = "AUX_STORAGE_ADMISSION_ALLOCATION_ISOLATED";
    if std::env::var_os(ISOLATED_ENV).is_none() {
        let status = Command::new(
            std::env::current_exe()
                .expect("admission allocation test executable should be available"),
        )
        .arg("--exact")
        .arg("admission::admission_tests::immediate_admission_path_has_no_per_request_metric_allocations")
        .arg("--nocapture")
        .env(ISOLATED_ENV, "1")
        .status()
        .expect("isolated admission allocation test child should start");
        assert!(
            status.success(),
            "isolated admission allocation test failed"
        );
        return;
    }

    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: 1,
        initial_latency_estimate_ms: 1,
        minimum_concurrency: 1,
        maximum_concurrency: 4,
        control_reserve_concurrency: 1,
        queue_capacity: 0,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("allocation-free", config).expect("valid config");

    // Warm the metric handles and fixed-size controller state before measuring.
    for _ in 0..8 {
        let permit = controller
            .try_acquire(AdmissionClass::PointRead)
            .expect("foreground capacity");
        permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    }

    let guard = AllocationGuard::start(
        module_path!(),
        "immediate_admission_path_has_no_per_request_metric_allocations",
        file!(),
        line!(),
        Some("try_acquire_and_complete"),
    );
    for _ in 0..256 {
        let permit = controller
            .try_acquire(AdmissionClass::PointRead)
            .expect("foreground capacity");
        permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    }
    let report = guard.finish();
    assert_eq!(report.allocation_count, 0, "{report:?}");
    assert_eq!(report.allocated_bytes, 0, "{report:?}");
}

#[tokio::test]
async fn a_full_queue_rejects_the_next_request() {
    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: 1,
        initial_latency_estimate_ms: 1,
        minimum_concurrency: 1,
        maximum_concurrency: 3,
        control_reserve_concurrency: 1,
        queue_capacity: 1,
        max_queue_wait_ms: 20,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("default", config).expect("valid admission config");
    let permit = controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("first permit");
    let waiter = {
        let controller = controller.clone();
        tokio::spawn(async move { controller.acquire(AdmissionClass::RangeRead).await })
    };
    tokio::task::yield_now().await;
    let rejection = controller
        .acquire(AdmissionClass::Write)
        .await
        .expect_err("queue full");
    assert_eq!(rejection.reason, AdmissionRejectionReason::QueueFull);
    permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    let _permit = waiter.await.expect("waiter task").expect("waiter permit");
}

#[tokio::test]
async fn queued_waiters_are_granted_in_fifo_order() {
    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: 1,
        initial_latency_estimate_ms: 1,
        minimum_concurrency: 1,
        maximum_concurrency: 2,
        control_reserve_concurrency: 1,
        queue_capacity: 3,
        max_queue_wait_ms: 1_000,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("fifo", config).expect("valid admission config");
    let held = controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("initial foreground permit");
    let (granted_tx, mut granted_rx) = mpsc::unbounded_channel();
    let mut release = Vec::new();
    let mut tasks = Vec::new();

    for index in 0..3 {
        let controller = controller.clone();
        let granted_tx = granted_tx.clone();
        let (release_tx, release_rx) = oneshot::channel();
        release.push(release_tx);
        tasks.push(tokio::spawn(async move {
            let permit = controller
                .acquire(AdmissionClass::RangeRead)
                .await
                .expect("queued waiter should be admitted");
            granted_tx.send(index).expect("grant receiver remains open");
            release_rx.await.expect("test releases the permit");
            permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
        }));
    }
    drop(granted_tx);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if controller.snapshot().queue_depth == 3 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all waiters reach the queue");

    held.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    for expected in 0..3 {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), granted_rx.recv())
                .await
                .expect("waiter is granted")
                .expect("grant channel remains open"),
            expected
        );
        release
            .remove(0)
            .send(())
            .expect("waiter remains alive until release");
    }
    for task in tasks {
        task.await.expect("FIFO waiter task");
    }
    assert_eq!(controller.snapshot().queue_depth, 0);
    assert_eq!(controller.snapshot().in_flight, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_queue_stress_reclaims_every_permit_after_cancellation() {
    const WAITERS: usize = 32;

    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: 1,
        initial_latency_estimate_ms: 1,
        minimum_concurrency: 2,
        maximum_concurrency: 4,
        control_reserve_concurrency: 1,
        queue_capacity: WAITERS,
        max_queue_wait_ms: 1_000,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("stress", config).expect("valid admission config");
    let held = (0..2)
        .map(|_| {
            controller
                .try_acquire(AdmissionClass::PointRead)
                .expect("foreground capacity")
        })
        .collect::<Vec<_>>();
    let barrier = Arc::new(Barrier::new(WAITERS + 1));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let mut handles = (0..WAITERS)
        .map(|index| {
            let barrier = Arc::clone(&barrier);
            let controller = controller.clone();
            let max_in_flight = Arc::clone(&max_in_flight);
            Some(tokio::spawn(async move {
                barrier.wait().await;
                let permit = controller
                    .acquire(if index % 2 == 0 {
                        AdmissionClass::RangeRead
                    } else {
                        AdmissionClass::Write
                    })
                    .await
                    .expect("a non-cancelled waiter is eventually admitted");
                let snapshot = controller.snapshot();
                max_in_flight.fetch_max(snapshot.in_flight, Ordering::Relaxed);
                assert!(snapshot.in_flight <= snapshot.desired_limit);
                if index % 3 == 0 {
                    drop(permit);
                } else {
                    permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
                }
            }))
        })
        .collect::<Vec<_>>();

    barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if controller.snapshot().queue_depth == WAITERS {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all stress waiters reached the bounded queue");

    for index in (0..WAITERS).step_by(2) {
        handles[index].as_ref().expect("stress handle").abort();
    }
    for index in (0..WAITERS).step_by(2) {
        let result = handles[index]
            .take()
            .expect("stress handle")
            .await
            .expect_err("cancelled waiter must not finish normally");
        assert!(result.is_cancelled());
    }
    assert_eq!(controller.snapshot().queue_depth, WAITERS / 2);

    for permit in held {
        permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    }
    for handle in handles.into_iter().flatten() {
        handle.await.expect("non-cancelled stress waiter");
    }

    let snapshot = controller.snapshot();
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.in_flight, 0);
    assert_eq!(
        max_in_flight.load(Ordering::Relaxed),
        snapshot.desired_limit
    );
}

#[tokio::test]
async fn timed_out_waiter_is_removed_without_leaking_capacity() {
    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: 1,
        initial_latency_estimate_ms: 1,
        minimum_concurrency: 1,
        maximum_concurrency: 3,
        control_reserve_concurrency: 1,
        queue_capacity: 1,
        max_queue_wait_ms: 5,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("default", config).expect("valid admission config");
    let permit = controller
        .try_acquire(AdmissionClass::PointRead)
        .expect("first permit");

    let waiter_controller = controller.clone();
    let waiter = tokio::spawn(async move {
        waiter_controller
            .acquire(AdmissionClass::RangeRead)
            .await
            .expect_err("waiter should time out")
    });
    let rejection = waiter.await.expect("waiter task");
    assert_eq!(rejection.reason, AdmissionRejectionReason::QueueTimedOut);
    assert_eq!(controller.snapshot().queue_depth, 0);
    assert_eq!(
        controller.snapshot().state,
        super::AdmissionState::Emergency
    );

    permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
    let _next = controller
        .try_acquire(AdmissionClass::Write)
        .expect("capacity is reusable after timeout");
}

#[test]
fn a_cancelled_grant_returns_its_slot_without_leaking_in_flight() {
    let config = AdmissionConfig {
        minimum_concurrency: 1,
        maximum_concurrency: 2,
        control_reserve_concurrency: 1,
        queue_capacity: 1,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("default", config).expect("valid admission config");
    let (sender, receiver) = oneshot::channel();
    drop(receiver);
    {
        let mut state = super::lock_state(&controller.inner);
        state.queue.push_back(Waiter {
            id: 1,
            class: AdmissionClass::RangeRead,
            enqueued_at: Instant::now(),
            sender,
        });
        let mut queue_waits = Vec::new();
        super::grant_waiters_from_locked(&controller.inner, &mut state, &mut queue_waits);
        assert_eq!(state.in_flight, 0);
        assert_eq!(state.in_flight_by_class, [0, 0, 0]);
    }
}

#[test]
fn a_dropped_grant_returns_a_slot_after_send_race() {
    let config = AdmissionConfig {
        minimum_concurrency: 1,
        maximum_concurrency: 2,
        control_reserve_concurrency: 1,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("default", config).expect("valid admission config");
    {
        let mut state = super::lock_state(&controller.inner);
        state.in_flight = 1;
        state.in_flight_by_class[AdmissionClass::PointRead.index()] = 1;
    }
    let grant = super::PermitGrant {
        controller: controller.inner.clone(),
        class: AdmissionClass::PointRead,
        reclaimed: false,
    };
    drop(grant);
    assert_eq!(controller.snapshot().in_flight, 0);
}

#[test]
fn named_connections_have_independent_admission_state() {
    let config = AdmissionConfig {
        initial_sustainable_throughput_rps: 1,
        initial_latency_estimate_ms: 1,
        minimum_concurrency: 1,
        maximum_concurrency: 2,
        control_reserve_concurrency: 1,
        queue_capacity: 0,
        ..AdmissionConfig::default()
    };
    let registry = AdmissionRegistry::new(
        "default",
        ["default".to_string(), "secondary".to_string()],
        config,
    )
    .expect("registry");
    let default_permit = registry
        .default_controller()
        .try_acquire(AdmissionClass::PointRead)
        .expect("default permit");
    let secondary_permit = registry
        .for_connection("secondary")
        .expect("secondary controller")
        .try_acquire(AdmissionClass::PointRead)
        .expect("secondary permit");
    assert_eq!(registry.default_controller().snapshot().in_flight, 1);
    assert_eq!(
        registry
            .for_connection("secondary")
            .expect("secondary controller")
            .snapshot()
            .in_flight,
        1
    );
    drop(default_permit);
    drop(secondary_permit);
}

#[test]
fn fixed_ingress_limit_is_bounded_from_connection_hard_limits() {
    let config = AdmissionConfig {
        maximum_concurrency: 10_000,
        queue_capacity: 10_000,
        ..AdmissionConfig::default()
    };
    let registry = AdmissionRegistry::new("default", Vec::new(), config).expect("registry");
    assert_eq!(registry.fixed_ingress_limit(), 2_048);

    let config = AdmissionConfig {
        minimum_concurrency: 1,
        maximum_concurrency: 5,
        queue_capacity: 0,
        ..AdmissionConfig::default()
    };
    let registry = AdmissionRegistry::new("default", Vec::new(), config).expect("registry");
    assert_eq!(registry.fixed_ingress_limit(), 64);
}

#[test]
fn control_reserve_is_separate_from_foreground_limit() {
    let config = AdmissionConfig {
        minimum_concurrency: 1,
        maximum_concurrency: 3,
        control_reserve_concurrency: 2,
        ..AdmissionConfig::default()
    };
    let controller = AdmissionController::new("default", config).expect("valid admission config");
    let first = controller
        .try_acquire_control()
        .expect("first control permit");
    let second = controller
        .try_acquire_control()
        .expect("second control permit");
    assert!(controller.try_acquire_control().is_err());
    assert!(controller.try_acquire(AdmissionClass::PointRead).is_ok());
    drop(first);
    drop(second);
}
