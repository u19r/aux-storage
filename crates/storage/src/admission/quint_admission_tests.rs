#![allow(non_snake_case)]

use std::{collections::VecDeque, time::Instant};

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use super::{AdmissionConfig, AdmissionState, ControllerState, Window, admission_transition};

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
struct MbtConfig {
    enabled: bool,
    minimum: usize,
    maximum: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct MbtControllerState {
    #[serde(rename = "desiredLimit")]
    desired_limit: usize,
    state: String,
    #[serde(rename = "healthyWindows")]
    healthy_windows: usize,
    #[serde(rename = "congestedWindows")]
    congested_windows: usize,
    #[serde(rename = "nonCongestedWindows")]
    non_congested_windows: usize,
    #[serde(rename = "emergencyClearWindows")]
    emergency_clear_windows: usize,
    #[serde(rename = "saturatedHealthyWindows")]
    saturated_healthy_windows: usize,
    #[serde(rename = "hasPreviousGoodput")]
    has_previous_goodput: bool,
    #[serde(rename = "probePreviousLimit")]
    probe_previous_limit: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
struct MbtSignals {
    saturated: bool,
    pressure: bool,
    #[serde(rename = "enoughSamples")]
    enough_samples: bool,
    #[serde(rename = "gradientHealthy")]
    gradient_healthy: bool,
    #[serde(rename = "goodputImproved")]
    goodput_improved: bool,
    #[serde(rename = "goodputPositive")]
    goodput_positive: bool,
    #[serde(rename = "probeDue")]
    probe_due: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct AdmissionControllerMbtState {
    #[serde(rename = "mbtLastConfig")]
    mbt_last_config: MbtConfig,
    #[serde(rename = "mbtLastState")]
    mbt_last_state: MbtControllerState,
    #[serde(rename = "mbtLastSignals")]
    mbt_last_signals: MbtSignals,
}

impl State<AdmissionControllerMbtDriver> for AdmissionControllerMbtState {
    fn from_driver(driver: &AdmissionControllerMbtDriver) -> Result<Self> {
        Ok(Self {
            mbt_last_config: driver.mbt_last_config,
            mbt_last_state: driver.mbt_last_state.clone(),
            mbt_last_signals: driver.mbt_last_signals,
        })
    }
}

#[derive(Debug)]
struct AdmissionControllerMbtDriver {
    mbt_last_config: MbtConfig,
    mbt_last_state: MbtControllerState,
    mbt_last_signals: MbtSignals,
}

impl Default for AdmissionControllerMbtDriver {
    fn default() -> Self {
        Self {
            mbt_last_config: MbtConfig {
                enabled: true,
                minimum: 2,
                maximum: 8,
            },
            mbt_last_state: MbtControllerState {
                desired_limit: 8,
                state: "warmup".to_string(),
                healthy_windows: 0,
                congested_windows: 0,
                non_congested_windows: 0,
                emergency_clear_windows: 0,
                saturated_healthy_windows: 0,
                has_previous_goodput: false,
                probe_previous_limit: 0,
            },
            mbt_last_signals: MbtSignals {
                saturated: false,
                pressure: false,
                enough_samples: true,
                gradient_healthy: true,
                goodput_improved: true,
                goodput_positive: true,
                probe_due: true,
            },
        }
    }
}

impl Driver for AdmissionControllerMbtDriver {
    type State = AdmissionControllerMbtState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            mbtInit => {
                *self = Self::default();
            },
            MbtCheck(
                mbtEnabled: bool,
                desiredLimit: usize,
                state: String,
                healthyWindows: usize,
                congestedWindows: usize,
                nonCongestedWindows: usize,
                emergencyClearWindows: usize,
                saturatedHealthyWindows: usize,
                hasPreviousGoodput: bool,
                probePreviousLimit: usize,
                saturated: bool,
                pressure: bool,
                enoughSamples: bool,
                gradientHealthy: bool,
                goodputImproved: bool,
                goodputPositive: bool,
                probeDue: bool,
            ) => {
                self.check(
                    MbtConfig {
                        enabled: mbtEnabled,
                        minimum: 2,
                        maximum: 8,
                    },
                    MbtControllerState {
                        desired_limit: desiredLimit,
                        state,
                        healthy_windows: healthyWindows,
                        congested_windows: congestedWindows,
                        non_congested_windows: nonCongestedWindows,
                        emergency_clear_windows: emergencyClearWindows,
                        saturated_healthy_windows: saturatedHealthyWindows,
                        has_previous_goodput: hasPreviousGoodput,
                        probe_previous_limit: probePreviousLimit,
                    },
                    MbtSignals {
                        saturated,
                        pressure,
                        enough_samples: enoughSamples,
                        gradient_healthy: gradientHealthy,
                        goodput_improved: goodputImproved,
                        goodput_positive: goodputPositive,
                        probe_due: probeDue,
                    },
                );
            },
            mbtStep(
                mbtEnabled: bool?,
                desiredLimit: usize?,
                state: String?,
                healthyWindows: usize?,
                congestedWindows: usize?,
                nonCongestedWindows: usize?,
                emergencyClearWindows: usize?,
                saturatedHealthyWindows: usize?,
                hasPreviousGoodput: bool?,
                probePreviousLimit: usize?,
                saturated: bool?,
                pressure: bool?,
                enoughSamples: bool?,
                gradientHealthy: bool?,
                goodputImproved: bool?,
                goodputPositive: bool?,
                probeDue: bool?,
            ) => {
                if let (
                    Some(mbt_enabled),
                    Some(desired_limit),
                    Some(state),
                    Some(healthy_windows),
                    Some(congested_windows),
                    Some(non_congested_windows),
                    Some(emergency_clear_windows),
                    Some(saturated_healthy_windows),
                    Some(has_previous_goodput),
                    Some(probe_previous_limit),
                    Some(saturated),
                    Some(pressure),
                    Some(enough_samples),
                    Some(gradient_healthy),
                    Some(goodput_improved),
                    Some(goodput_positive),
                    Some(probe_due),
                ) = (
                    mbtEnabled,
                    desiredLimit,
                    state,
                    healthyWindows,
                    congestedWindows,
                    nonCongestedWindows,
                    emergencyClearWindows,
                    saturatedHealthyWindows,
                    hasPreviousGoodput,
                    probePreviousLimit,
                    saturated,
                    pressure,
                    enoughSamples,
                    gradientHealthy,
                    goodputImproved,
                    goodputPositive,
                    probeDue,
                ) {
                    self.check(
                        MbtConfig {
                            enabled: mbt_enabled,
                            minimum: 2,
                            maximum: 8,
                        },
                        MbtControllerState {
                            desired_limit,
                            state,
                            healthy_windows,
                            congested_windows,
                            non_congested_windows,
                            emergency_clear_windows,
                        saturated_healthy_windows,
                        has_previous_goodput,
                        probe_previous_limit,
                        },
                        MbtSignals {
                            saturated,
                            pressure,
                            enough_samples,
                            gradient_healthy,
                        goodput_improved,
                        goodput_positive,
                        probe_due,
                        },
                    );
                }
            },
        })
    }
}

impl AdmissionControllerMbtDriver {
    fn check(&mut self, config: MbtConfig, current: MbtControllerState, signals: MbtSignals) {
        let now = Instant::now();
        let mut state = ControllerState {
            desired_limit: current.desired_limit,
            in_flight: 0,
            in_flight_by_class: [0; 3],
            control_in_flight: 0,
            rejection_count: 0,
            next_waiter_id: 1,
            queue: VecDeque::new(),
            state: parse_state(&current.state),
            window: Window::new(now),
            baselines_ms: [5.0; 3],
            low_load_windows: 0,
            last_baseline_raise_at: None,
            healthy_windows: current.healthy_windows,
            congested_windows: current.congested_windows,
            non_congested_windows: current.non_congested_windows,
            emergency_clear_windows: current.emergency_clear_windows,
            saturated_healthy_windows: current.saturated_healthy_windows,
            previous_saturated_goodput: current.has_previous_goodput.then_some(100.0),
            probe_previous_limit: (current.probe_previous_limit > 0)
                .then_some(current.probe_previous_limit),
            probe_due: now,
            rng: 0x9e37_79b9_7f4a_7c15,
        };
        let runtime_config = AdmissionConfig {
            enabled: config.enabled,
            initial_sustainable_throughput_rps: 800,
            initial_latency_estimate_ms: 10,
            minimum_concurrency: config.minimum,
            // Quint models the foreground/effective limit. The runtime
            // maximum also includes the reserved control slot.
            maximum_concurrency: config.maximum + 1,
            control_reserve_concurrency: 1,
            queue_capacity: 2,
            max_queue_wait_ms: 10,
        };
        admission_transition::transition_for_model(
            &runtime_config,
            &mut state,
            admission_transition::ModelSignals {
                saturated: signals.saturated,
                pressure: signals.pressure,
                enough_samples: signals.enough_samples,
                gradient_healthy: signals.gradient_healthy,
                goodput_improved: signals.goodput_improved,
                goodput_positive: signals.goodput_positive,
                probe_due: signals.probe_due,
            },
        );
        self.mbt_last_config = config;
        self.mbt_last_state = MbtControllerState {
            desired_limit: state.desired_limit,
            state: state_name(state.state).to_string(),
            healthy_windows: state.healthy_windows,
            congested_windows: state.congested_windows,
            non_congested_windows: state.non_congested_windows,
            emergency_clear_windows: state.emergency_clear_windows,
            saturated_healthy_windows: state.saturated_healthy_windows,
            has_previous_goodput: state.previous_saturated_goodput.is_some(),
            probe_previous_limit: state.probe_previous_limit.unwrap_or(0),
        };
        self.mbt_last_signals = signals;
    }
}

fn parse_state(state: &str) -> AdmissionState {
    match state {
        "stable" => AdmissionState::Stable,
        "probe" => AdmissionState::Probe,
        "backoff" => AdmissionState::Backoff,
        "recovering" => AdmissionState::Recovering,
        "emergency" => AdmissionState::Emergency,
        _ => AdmissionState::Warmup,
    }
}

fn state_name(state: AdmissionState) -> &'static str {
    match state {
        AdmissionState::Warmup => "warmup",
        AdmissionState::Stable => "stable",
        AdmissionState::Probe => "probe",
        AdmissionState::Backoff => "backoff",
        AdmissionState::Recovering => "recovering",
        AdmissionState::Emergency => "emergency",
    }
}

#[quint_run(
    spec = "../../quint/admission_controller_mbt.qnt",
    main = "admission_controller_mbt",
    init = "mbtInit",
    step = "mbtStep",
    max_samples = 256,
    max_steps = 8,
    seed = "0xa11d5eed"
)]
fn admission_controller_mbt_matches_rust_transition() -> impl Driver {
    AdmissionControllerMbtDriver::default()
}
