use std::time::{Duration, Instant};

use crate::admission::{
    admission_transition::{
        constants::{
            BASELINE_RAISE_FACTOR, BASELINE_RAISE_INTERVAL, BASELINE_SAMPLE_MINIMUM,
            LATENCY_BUFFER, MAX_NORMAL_FACTOR, MIN_NORMAL_FACTOR, NORMAL_FACTOR_SCALE,
            PROBE_PERCENT, QUEUE_SATURATION_PERCENT,
        },
        window::{Window, WindowSignals},
    },
    admission_types::{AdmissionClass, AdmissionConfig, AdmissionState},
    state::ControllerState,
};

/// Compute a rounded-up percentage without multiplying the full limit. The
/// configured limit is user input and can be close to `usize::MAX`, where a
/// cross-multiplication comparison would otherwise saturate and misclassify
/// load.
fn percent_threshold(limit: usize, percent: usize) -> usize {
    let quotient = limit / 100;
    let remainder = limit % 100;
    quotient
        .saturating_mul(percent)
        .saturating_add((remainder * percent).saturating_add(99) / 100)
}

fn saturated_in_flight_threshold(limit: usize) -> usize {
    // ceil(limit * 90 / 100) == limit - floor(limit * 10 / 100).
    limit.saturating_sub(limit / 10)
}

fn low_load_in_flight_threshold(limit: usize) -> usize {
    // The low-load predicate is strict, so an odd limit rounds up here.
    limit / 2 + limit % 2
}

fn duration_percent_threshold(duration: Duration, percent: u32) -> u128 {
    (duration.as_nanos() * u128::from(percent)).saturating_add(99) / 100
}

pub(crate) fn evaluate_window(
    config: &AdmissionConfig,
    state: &mut ControllerState,
    now: Instant,
) -> (bool, bool, f64, f64) {
    evaluate_window_inner(config, state, now, true)
}

/// Evaluate an explicit pressure signal without closing the current sampling
/// window. This makes pressure immediate while preserving the invariant that
/// one window can apply at most one emergency reduction.
pub(crate) fn evaluate_pressure(
    config: &AdmissionConfig,
    state: &mut ControllerState,
    now: Instant,
) -> (bool, bool, f64, f64) {
    evaluate_window_inner(config, state, now, false)
}

fn evaluate_window_inner(
    config: &AdmissionConfig,
    state: &mut ControllerState,
    now: Instant,
    close_window: bool,
) -> (bool, bool, f64, f64) {
    let elapsed = now.saturating_duration_since(state.window.started_at);
    state.window.observe_queue(now, state.queue.len());
    let saturated = state.window.peak_in_flight
        >= saturated_in_flight_threshold(state.desired_limit)
        || state.window.queue_waits > 0
        || state.window.queue_nonempty_for.as_nanos()
            >= duration_percent_threshold(elapsed, QUEUE_SATURATION_PERCENT);
    let pressure = state.window.explicit_pressure || state.window.queue_timeout;
    let enough_samples =
        state.window.completed.iter().copied().sum::<usize>() >= BASELINE_SAMPLE_MINIMUM;
    let percentiles = std::array::from_fn(|index| Window::p90(&state.window.samples[index]));
    let gradient = AdmissionClass::ALL
        .iter()
        .filter_map(|class| {
            let index = class.index();
            if state.window.completed[index] < BASELINE_SAMPLE_MINIMUM {
                return None;
            }
            percentiles[index].map(|p90| (state.baselines_ms[index] * LATENCY_BUFFER) / p90)
        })
        .reduce(f64::min)
        .unwrap_or(f64::INFINITY);
    let congested = pressure || (saturated && gradient < 1.0);
    let goodput = if elapsed.is_zero() {
        0.0
    } else {
        state.window.successes as f64 / elapsed.as_secs_f64()
    };
    if !saturated
        && !pressure
        && matches!(state.state, AdmissionState::Warmup | AdmissionState::Stable)
    {
        update_low_load_baselines(state, now, percentiles);
    } else {
        state.low_load_windows = 0;
    }
    transition_window(
        config,
        state,
        WindowSignals {
            saturated,
            congested,
            pressure,
            enough_samples,
            gradient,
            goodput,
            now,
        },
    );
    if close_window {
        // A queued waiter can span the sampling boundary. Start the next
        // window in the current occupancy state so the interval after this
        // boundary is charged to queue pressure until the next dequeue
        // observation.
        let queue_nonempty = !state.queue.is_empty();
        state.window = Window::new(now);
        state.window.queue_was_nonempty = queue_nonempty;
    }
    (saturated, pressure, gradient, goodput)
}

fn update_low_load_baselines(
    state: &mut ControllerState,
    now: Instant,
    percentiles: [Option<f64>; 3],
) {
    let low_load = state.window.peak_in_flight < low_load_in_flight_threshold(state.desired_limit)
        && state.window.queue_waits == 0;
    if !low_load {
        state.low_load_windows = 0;
        return;
    }
    state.low_load_windows = state.low_load_windows.saturating_add(1);
    let can_raise = state.low_load_windows >= 5
        && state
            .last_baseline_raise_at
            .is_none_or(|last| now.saturating_duration_since(last) >= BASELINE_RAISE_INTERVAL);
    let mut raised = false;
    for class in AdmissionClass::ALL {
        let index = class.index();
        if state.window.completed[index] < BASELINE_SAMPLE_MINIMUM {
            continue;
        }
        if let Some(p90) = percentiles[index] {
            if p90 < state.baselines_ms[index] {
                state.baselines_ms[index] = p90;
            } else if can_raise {
                let raised_baseline = state.baselines_ms[index] * BASELINE_RAISE_FACTOR;
                if raised_baseline < p90 {
                    state.baselines_ms[index] = raised_baseline;
                    raised = true;
                }
            }
        }
    }
    if raised {
        state.last_baseline_raise_at = Some(now);
    }
}

pub(crate) fn transition_window(
    config: &AdmissionConfig,
    state: &mut ControllerState,
    signals: WindowSignals,
) {
    if !config.enabled {
        state.desired_limit = config.effective_maximum();
        state.state = AdmissionState::Stable;
        state.probe_previous_limit = None;
        return;
    }
    if signals.pressure {
        if !state.window.emergency_reduction_applied {
            state.desired_limit = emergency_limit(config, state.desired_limit);
            state.window.emergency_reduction_applied = true;
        }
        state.state = AdmissionState::Emergency;
        state.emergency_clear_windows = 0;
        state.congested_windows = 0;
        state.probe_previous_limit = None;
        return;
    }
    if signals.congested {
        state.congested_windows = state.congested_windows.saturating_add(1);
    } else {
        state.congested_windows = 0;
    }
    let sustained_congestion = state.congested_windows >= 2;
    match state.state {
        AdmissionState::Warmup => {
            if signals.congested {
                state.healthy_windows = 0;
                if sustained_congestion {
                    normal_backoff(config, state, signals.gradient);
                }
            } else if !signals.enough_samples {
                state.healthy_windows = 0;
            } else {
                state.healthy_windows = state.healthy_windows.saturating_add(1);
                if state.healthy_windows >= 2 {
                    state.state = AdmissionState::Stable;
                }
            }
        }
        AdmissionState::Stable => {
            if sustained_congestion {
                normal_backoff(config, state, signals.gradient);
                state.saturated_healthy_windows = 0;
            } else if signals.congested {
                state.saturated_healthy_windows = 0;
            } else if signals.saturated && signals.enough_samples && signals.gradient >= 1.0 {
                state.saturated_healthy_windows = state.saturated_healthy_windows.saturating_add(1);
                if state.saturated_healthy_windows >= 3 && signals.now >= state.probe_due {
                    state.probe_previous_limit = Some(state.desired_limit);
                    state.previous_saturated_goodput = Some(signals.goodput);
                    let delta = percent_threshold(state.desired_limit, PROBE_PERCENT).max(1);
                    state.desired_limit = state
                        .desired_limit
                        .saturating_add(delta)
                        .min(config.effective_maximum());
                    state.state = AdmissionState::Probe;
                }
            } else if !signals.saturated {
                state.saturated_healthy_windows = 0;
            }
        }
        AdmissionState::Probe => {
            let previous = state.probe_previous_limit.unwrap_or(state.desired_limit);
            let improved = signals.goodput > 0.0
                && state
                    .previous_saturated_goodput
                    .is_none_or(|prior| prior > 0.0 && signals.goodput >= prior * 1.02)
                && signals.enough_samples
                && signals.gradient >= 1.0;
            if !signals.saturated {
                state.state = AdmissionState::Stable;
                state.probe_previous_limit = None;
            } else if improved {
                state.previous_saturated_goodput = Some(signals.goodput);
                state.state = AdmissionState::Stable;
                state.probe_previous_limit = None;
            } else {
                state.desired_limit = previous.max(config.minimum_concurrency);
                state.state = AdmissionState::Backoff;
                state.probe_previous_limit = None;
            }
            state.probe_due = signals.now + probe_delay(state);
        }
        AdmissionState::Backoff => {
            if sustained_congestion {
                normal_backoff(config, state, signals.gradient);
            } else if signals.congested {
                state.non_congested_windows = 0;
            } else {
                state.non_congested_windows = state.non_congested_windows.saturating_add(1);
                if state.non_congested_windows >= 2 {
                    state.state = AdmissionState::Recovering;
                    state.non_congested_windows = 0;
                }
            }
        }
        AdmissionState::Emergency => {
            state.emergency_clear_windows = state.emergency_clear_windows.saturating_add(1);
            if state.emergency_clear_windows >= 3 {
                state.state = AdmissionState::Recovering;
            }
        }
        AdmissionState::Recovering => {
            if sustained_congestion {
                normal_backoff(config, state, signals.gradient);
                state.saturated_healthy_windows = 0;
            } else if signals.congested {
                state.saturated_healthy_windows = 0;
            } else if signals.saturated && signals.enough_samples && signals.gradient >= 1.0 {
                state.saturated_healthy_windows = state.saturated_healthy_windows.saturating_add(1);
                if state.saturated_healthy_windows >= 3 && signals.now >= state.probe_due {
                    state.probe_previous_limit = Some(state.desired_limit);
                    state.previous_saturated_goodput = Some(signals.goodput);
                    let delta = percent_threshold(state.desired_limit, PROBE_PERCENT).max(1);
                    state.desired_limit = state
                        .desired_limit
                        .saturating_add(delta)
                        .min(config.effective_maximum());
                    state.state = AdmissionState::Probe;
                }
            } else if !signals.saturated {
                state.saturated_healthy_windows = 0;
            }
        }
    }
}

fn normal_backoff(config: &AdmissionConfig, state: &mut ControllerState, gradient: f64) {
    let factor = gradient.clamp(MIN_NORMAL_FACTOR, MAX_NORMAL_FACTOR);
    // Do not multiply a near-`usize::MAX` limit in `f64`: conversion rounds
    // the limit before multiplication and can move the result above the
    // mathematical floor used by the model. Quantize only the bounded factor
    // and perform the product in `u128`, which is lossless for all supported
    // usize widths.
    let scaled_factor = (factor * NORMAL_FACTOR_SCALE as f64).floor() as u128;
    let reduced = (state.desired_limit as u128 * scaled_factor) / NORMAL_FACTOR_SCALE;
    let reduced = usize::try_from(reduced).unwrap_or(usize::MAX);
    state.desired_limit = reduced.max(config.minimum_concurrency);
    state.state = AdmissionState::Backoff;
    state.non_congested_windows = 0;
    state.congested_windows = 0;
    state.probe_previous_limit = None;
}

fn emergency_limit(config: &AdmissionConfig, current: usize) -> usize {
    // The emergency factor is exactly one half. Keep this integer so a
    // `usize::MAX` limit cannot round through `f64` and produce a value one
    // above the mathematical floor on wide platforms.
    let reduced = current / 2;
    reduced.max(config.minimum_concurrency)
}

fn probe_delay(state: &mut ControllerState) -> Duration {
    state.rng ^= state.rng << 7;
    state.rng ^= state.rng >> 9;
    state.rng ^= state.rng << 8;
    Duration::from_secs(4 + state.rng % 3)
}
