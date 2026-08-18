#![allow(non_snake_case)]

use std::time::Duration;

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use super::{
    AdmissionClass, AdmissionConfig, AdmissionController, AdmissionOutcome, AdmissionPermit,
    ControlPermit,
};

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
struct GateConfig {
    #[serde(rename = "foregroundLimit")]
    foreground_limit: usize,
    #[serde(rename = "controlLimit")]
    control_limit: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct GateState {
    #[serde(rename = "inFlight")]
    in_flight: usize,
    #[serde(rename = "controlInFlight")]
    control_in_flight: usize,
    #[serde(rename = "activePermits")]
    active_permits: usize,
    #[serde(rename = "activeControls")]
    active_controls: usize,
    #[serde(rename = "lastOperation")]
    last_operation: String,
    #[serde(rename = "lastClass")]
    last_class: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct AdmissionGateMbtState {
    config: GateConfig,
    gate: GateState,
}

impl State<AdmissionGateMbtDriver> for AdmissionGateMbtState {
    fn from_driver(driver: &AdmissionGateMbtDriver) -> Result<Self> {
        let snapshot = driver.controller.snapshot();
        Ok(Self {
            config: GateConfig {
                foreground_limit: driver.foreground_limit,
                control_limit: driver.control_limit,
            },
            gate: GateState {
                in_flight: snapshot.in_flight,
                control_in_flight: snapshot.control_in_flight,
                active_permits: driver.permits.len(),
                active_controls: driver.control_permits.len(),
                last_operation: driver.last_operation.clone(),
                last_class: driver.last_class.clone(),
            },
        })
    }
}

struct AdmissionGateMbtDriver {
    controller: AdmissionController,
    foreground_limit: usize,
    control_limit: usize,
    permits: Vec<AdmissionPermit>,
    control_permits: Vec<ControlPermit>,
    last_operation: String,
    last_class: String,
}

impl Default for AdmissionGateMbtDriver {
    fn default() -> Self {
        let config = AdmissionConfig {
            initial_sustainable_throughput_rps: 400,
            initial_latency_estimate_ms: 5,
            minimum_concurrency: 2,
            maximum_concurrency: 3,
            control_reserve_concurrency: 1,
            queue_capacity: 0,
            max_queue_wait_ms: 10,
            ..AdmissionConfig::default()
        };
        Self {
            controller: AdmissionController::new("quint-gate", config)
                .expect("model config is valid"),
            foreground_limit: 2,
            control_limit: 1,
            permits: Vec::new(),
            control_permits: Vec::new(),
            last_operation: "init".to_string(),
            last_class: "none".to_string(),
        }
    }
}

impl Driver for AdmissionGateMbtDriver {
    type State = AdmissionGateMbtState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            step(operation: String, class: String) => {
                match operation.as_str() {
                    "acquire" => self.acquire(&class),
                    "complete" => self.complete(),
                    "drop" => self.drop_permit(),
                    "control_acquire" => self.control_acquire(),
                    "control_drop" => self.control_drop(),
                    _ => unreachable!("unexpected model operation {operation}"),
                }
            },
        })
    }
}

impl AdmissionGateMbtDriver {
    fn acquire(&mut self, class: &str) {
        let class = match class {
            "range" => AdmissionClass::RangeRead,
            "write" => AdmissionClass::Write,
            _ => AdmissionClass::PointRead,
        };
        self.last_class = match class {
            AdmissionClass::PointRead => "point",
            AdmissionClass::RangeRead => "range",
            AdmissionClass::Write => "write",
        }
        .to_string();
        match self.controller.try_acquire(class) {
            Ok(permit) => {
                self.permits.push(permit);
                self.last_operation = "admit".to_string();
            }
            Err(_) => {
                self.last_operation = "reject".to_string();
            }
        }
    }

    fn complete(&mut self) {
        if let Some(permit) = self.permits.pop() {
            self.last_class = "none".to_string();
            permit.complete(AdmissionOutcome::Success(Duration::from_millis(1)));
            self.last_operation = "complete".to_string();
        }
    }

    fn drop_permit(&mut self) {
        if self.permits.pop().is_some() {
            self.last_class = "none".to_string();
            self.last_operation = "drop".to_string();
        }
    }

    fn control_acquire(&mut self) {
        self.last_class = "none".to_string();
        match self.controller.try_acquire_control() {
            Ok(permit) => {
                self.control_permits.push(permit);
                self.last_operation = "control_admit".to_string();
            }
            Err(_) => {
                self.last_operation = "control_reject".to_string();
            }
        }
    }

    fn control_drop(&mut self) {
        if self.control_permits.pop().is_some() {
            self.last_class = "none".to_string();
            self.last_operation = "control_drop".to_string();
        }
    }
}

#[quint_run(
    spec = "../../quint/admission_controller_gate_mbt.qnt",
    init = "init",
    step = "step",
    max_samples = 512,
    max_steps = 16,
    seed = "0xa11d5eed"
)]
fn admission_controller_gate_mbt_matches_rust_accounting() -> impl Driver {
    AdmissionGateMbtDriver::default()
}
