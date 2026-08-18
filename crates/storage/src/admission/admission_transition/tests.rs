use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use tokio::sync::oneshot;

use super::{
    AdmissionClass, AdmissionConfig, AdmissionState, ControllerState, WINDOW, Window,
    WindowSignals, evaluate_window, transition_window,
};
use crate::admission::Waiter;

fn state_at(now: Instant, state: AdmissionState, desired_limit: usize) -> ControllerState {
    ControllerState {
        desired_limit,
        in_flight: 0,
        in_flight_by_class: [0; 3],
        control_in_flight: 0,
        rejection_count: 0,
        next_waiter_id: 1,
        queue: VecDeque::new(),
        state,
        window: Window::new(now),
        baselines_ms: [5.0; 3],
        low_load_windows: 0,
        last_baseline_raise_at: None,
        healthy_windows: 0,
        congested_windows: 0,
        non_congested_windows: 0,
        emergency_clear_windows: 0,
        saturated_healthy_windows: 0,
        previous_saturated_goodput: None,
        probe_previous_limit: None,
        probe_due: now,
        rng: 1,
    }
}

fn signals(
    now: Instant,
    saturated: bool,
    congested: bool,
    pressure: bool,
    enough_samples: bool,
    gradient: f64,
    goodput: f64,
) -> WindowSignals {
    WindowSignals {
        saturated,
        congested,
        pressure,
        enough_samples,
        gradient,
        goodput,
        now,
    }
}

#[test]
fn explicit_pressure_halves_the_current_limit() {
    let config = AdmissionConfig::default();
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Stable, 100);
    transition_window(
        &config,
        &mut state,
        signals(now, true, true, true, false, 0.1, 0.0),
    );
    assert_eq!(state.desired_limit, 50);
    assert_eq!(state.state, AdmissionState::Emergency);
}

#[test]
fn explicit_pressure_reduces_at_most_once_per_window() {
    let config = AdmissionConfig::default();
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Stable, 100);
    let pressure = signals(now, true, true, true, false, 0.1, 0.0);

    transition_window(&config, &mut state, pressure);
    transition_window(&config, &mut state, pressure);

    assert_eq!(state.state, AdmissionState::Emergency);
    assert_eq!(state.desired_limit, 50);
}

#[test]
fn explicit_pressure_discards_a_stale_probe_restore_limit() {
    let config = AdmissionConfig::default();
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Probe, 105);
    state.probe_previous_limit = Some(100);
    transition_window(
        &config,
        &mut state,
        signals(now, true, true, true, true, 1.0, 100.0),
    );
    assert_eq!(state.state, AdmissionState::Emergency);
    assert_eq!(state.probe_previous_limit, None);
}

#[test]
fn warmup_needs_two_sampled_windows_before_stabilizing() {
    let config = AdmissionConfig::default();
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Warmup, 100);

    for _ in 0..2 {
        transition_window(
            &config,
            &mut state,
            signals(now, false, false, false, false, 1.0, 0.0),
        );
    }
    assert_eq!(state.state, AdmissionState::Warmup);

    for _ in 0..2 {
        transition_window(
            &config,
            &mut state,
            signals(now, false, false, false, true, 1.0, 0.0),
        );
    }
    assert_eq!(state.state, AdmissionState::Stable);
}

#[test]
fn ordinary_congestion_requires_two_windows_and_backoff_is_bounded() {
    let config = AdmissionConfig::default();
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Stable, 100);
    let congested = signals(now, true, true, false, true, 0.1, 100.0);

    transition_window(&config, &mut state, congested);
    assert_eq!(state.state, AdmissionState::Stable);
    assert_eq!(state.desired_limit, 100);

    transition_window(&config, &mut state, congested);
    assert_eq!(state.state, AdmissionState::Backoff);
    assert_eq!(state.desired_limit, 80);
}

#[test]
fn an_under_sampled_class_cannot_trigger_latency_backoff() {
    let config = AdmissionConfig::default();
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Stable, 100);
    state.window.peak_in_flight = 100;
    state
        .window
        .add_sample(AdmissionClass::PointRead, Duration::from_millis(100));

    let (saturated, pressure, gradient, _) = evaluate_window(&config, &mut state, now + WINDOW);
    assert!(saturated);
    assert!(!pressure);
    assert!(gradient.is_infinite());
    assert_eq!(state.state, AdmissionState::Stable);
    assert_eq!(state.desired_limit, 100);
}

#[test]
fn given_near_max_limit_when_window_load_is_small_then_saturation_does_not_overflow() {
    let config = AdmissionConfig {
        minimum_concurrency: 1,
        maximum_concurrency: usize::MAX,
        control_reserve_concurrency: 0,
        ..AdmissionConfig::default()
    };
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Stable, usize::MAX);
    state.window.peak_in_flight = 1;

    let (saturated, pressure, _, _) = evaluate_window(&config, &mut state, now + WINDOW);
    assert!(!saturated);
    assert!(!pressure);
}

#[test]
fn given_zero_probe_goodput_when_probe_completes_then_controller_backs_off() {
    let config = AdmissionConfig::default();
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Probe, 105);
    state.probe_previous_limit = Some(100);
    state.previous_saturated_goodput = Some(0.0);

    transition_window(
        &config,
        &mut state,
        signals(now, true, false, false, true, 1.0, 0.0),
    );

    assert_eq!(state.state, AdmissionState::Backoff);
    assert_eq!(state.desired_limit, 100);
    assert_eq!(state.probe_previous_limit, None);
}

#[test]
fn given_near_max_limit_when_pressure_is_explicit_then_emergency_reduction_is_exact() {
    let config = AdmissionConfig {
        minimum_concurrency: 1,
        maximum_concurrency: usize::MAX,
        control_reserve_concurrency: 0,
        ..AdmissionConfig::default()
    };
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Stable, usize::MAX);

    transition_window(
        &config,
        &mut state,
        signals(now, true, true, true, false, 0.1, 0.0),
    );

    assert_eq!(state.state, AdmissionState::Emergency);
    assert_eq!(state.desired_limit, usize::MAX / 2);
}

#[test]
fn given_near_max_limit_when_normal_congestion_occurs_then_backoff_uses_integer_floor() {
    let config = AdmissionConfig {
        minimum_concurrency: 1,
        maximum_concurrency: usize::MAX,
        control_reserve_concurrency: 0,
        ..AdmissionConfig::default()
    };
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Stable, usize::MAX);
    let congested = signals(now, true, true, false, true, 0.8, 100.0);

    transition_window(&config, &mut state, congested);
    transition_window(&config, &mut state, congested);

    let expected = (usize::MAX as u128 * 8 / 10) as usize;
    assert_eq!(state.state, AdmissionState::Backoff);
    assert_eq!(state.desired_limit, expected);
}

#[test]
fn queue_nonempty_duration_stops_at_the_observed_dequeue_boundary() {
    let start = Instant::now();
    let mut window = Window::new(start);
    let enqueued = start + Duration::from_millis(100);
    let dequeued = start + Duration::from_millis(350);

    window.observe_queue(enqueued, 1);
    window.observe_queue(dequeued, 0);
    window.observe_queue(start + Duration::from_secs(2), 0);

    assert_eq!(window.queue_nonempty_for, Duration::from_millis(250));
}

#[test]
fn queue_nonempty_duration_carries_across_window_boundaries() {
    let start = Instant::now();
    let mut state = state_at(start, AdmissionState::Stable, 100);
    let (sender, _receiver) = oneshot::channel();
    state.queue.push_back(Waiter {
        id: 1,
        class: AdmissionClass::PointRead,
        enqueued_at: start + Duration::from_millis(100),
        sender,
    });
    state
        .window
        .observe_queue(start + Duration::from_millis(100), 1);

    evaluate_window(&AdmissionConfig::default(), &mut state, start + WINDOW);
    state
        .window
        .observe_queue(start + WINDOW + Duration::from_millis(250), 1);

    assert_eq!(state.window.queue_nonempty_for, Duration::from_millis(250));
}

#[test]
fn emergency_requires_three_clean_windows_before_recovery() {
    let config = AdmissionConfig::default();
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Emergency, 50);
    let clean = signals(now, false, false, false, true, 1.0, 100.0);

    transition_window(&config, &mut state, clean);
    transition_window(&config, &mut state, clean);
    assert_eq!(state.state, AdmissionState::Emergency);
    transition_window(&config, &mut state, clean);
    assert_eq!(state.state, AdmissionState::Recovering);
}

#[test]
fn flat_goodput_probe_restores_previous_limit_and_backs_off() {
    let config = AdmissionConfig::default();
    let now = Instant::now();
    let mut state = state_at(now, AdmissionState::Stable, 100);
    state.saturated_healthy_windows = 2;
    let healthy = signals(now, true, false, false, true, 1.0, 100.0);

    transition_window(&config, &mut state, healthy);
    assert_eq!(state.state, AdmissionState::Probe);
    assert_eq!(state.desired_limit, 105);

    transition_window(&config, &mut state, healthy);
    assert_eq!(state.state, AdmissionState::Backoff);
    assert_eq!(state.desired_limit, 100);
}

#[test]
fn independent_controller_seeds_produce_distinct_probe_delays() {
    let config = AdmissionConfig::default();
    let now = Instant::now();
    let healthy = signals(now, true, false, false, true, 1.0, 102.0);
    let mut delays = std::collections::HashSet::new();

    for seed in [3_u64, 4, 7] {
        let mut state = state_at(now, AdmissionState::Stable, 100);
        state.rng = seed;
        state.saturated_healthy_windows = 2;
        transition_window(&config, &mut state, healthy);
        transition_window(&config, &mut state, healthy);
        delays.insert(state.probe_due.duration_since(now));
        assert!(state.desired_limit <= config.effective_maximum());
    }

    assert!(delays.len() > 1, "probe seeds should not synchronize");
}

#[test]
fn given_near_max_limit_when_probe_starts_then_limit_saturates_without_overflow() {
    let config = AdmissionConfig {
        minimum_concurrency: 1,
        maximum_concurrency: usize::MAX,
        control_reserve_concurrency: 0,
        ..AdmissionConfig::default()
    };
    let now = Instant::now();
    for admission_state in [AdmissionState::Stable, AdmissionState::Recovering] {
        let mut state = state_at(now, admission_state, usize::MAX - 1);
        state.saturated_healthy_windows = 2;

        transition_window(
            &config,
            &mut state,
            signals(now, true, false, false, true, 1.0, 100.0),
        );

        assert_eq!(state.state, AdmissionState::Probe);
        assert_eq!(state.desired_limit, usize::MAX);
        assert_eq!(state.probe_previous_limit, Some(usize::MAX - 1));
    }
}

#[test]
fn three_controllers_track_capacity_drop_and_recovery_without_locking_low() {
    let config = AdmissionConfig::default();
    let start = Instant::now();
    let mut controllers = [
        state_at(start, AdmissionState::Stable, 100),
        state_at(start, AdmissionState::Stable, 100),
        state_at(start, AdmissionState::Stable, 100),
    ];
    for (state, seed) in controllers.iter_mut().zip([3_u64, 4, 7]) {
        state.rng = seed;
        state.probe_due = start;
    }

    for state in &mut controllers {
        let congested = signals(start, true, true, false, true, 0.5, 60.0);
        transition_window(&config, state, congested);
        transition_window(&config, state, congested);
        assert_eq!(state.desired_limit, 80);
        transition_window(&config, state, congested);
        transition_window(&config, state, congested);
        assert_eq!(state.desired_limit, 64);
    }
    assert!(
        controllers
            .iter()
            .map(|state| state.desired_limit)
            .sum::<usize>()
            <= 3 * config.effective_maximum()
    );

    let mut probe_delays = std::collections::HashSet::new();
    for state in &mut controllers {
        let mut timestamp = start + Duration::from_secs(10);
        transition_window(
            &config,
            state,
            signals(timestamp, true, false, false, true, 1.0, 100.0),
        );
        timestamp += Duration::from_secs(10);
        transition_window(
            &config,
            state,
            signals(timestamp, true, false, false, true, 1.0, 100.0),
        );
        for _ in 0..3 {
            timestamp += Duration::from_secs(10);
            transition_window(
                &config,
                state,
                signals(timestamp, true, false, false, true, 1.0, 100.0),
            );
        }
        assert_eq!(state.state, AdmissionState::Probe);
        transition_window(
            &config,
            state,
            signals(
                timestamp + Duration::from_secs(1),
                true,
                false,
                false,
                true,
                1.0,
                103.0,
            ),
        );
        probe_delays.insert(
            state
                .probe_due
                .duration_since(timestamp + Duration::from_secs(1)),
        );
        assert_eq!(state.state, AdmissionState::Stable);
        assert!(state.desired_limit > 64);
    }
    assert!(
        probe_delays.len() > 1,
        "controllers should not synchronize probes"
    );
    assert!(
        controllers
            .iter()
            .all(|state| state.desired_limit <= config.effective_maximum())
    );
}
