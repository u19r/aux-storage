#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{GsiCatchupApplyCase, plan_gsi_catchup_apply};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct MbtApplyCase {
    #[serde(rename = "currentVersion")]
    current_version: i64,
    #[serde(rename = "observationVersion")]
    observation_version: i64,
    #[serde(rename = "currentProjects")]
    current_projects: bool,
    #[serde(rename = "observationProjects")]
    observation_projects: bool,
    #[serde(rename = "historyAvailable")]
    history_available: bool,
    #[serde(rename = "scanComplete")]
    scan_complete: bool,
    #[serde(rename = "drainComplete")]
    drain_complete: bool,
}

impl From<MbtApplyCase> for GsiCatchupApplyCase {
    fn from(value: MbtApplyCase) -> Self {
        Self {
            current_version: value.current_version,
            observation_version: value.observation_version,
            current_projects: value.current_projects,
            observation_projects: value.observation_projects,
            history_available: value.history_available,
            scan_complete: value.scan_complete,
            drain_complete: value.drain_complete,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct GsiBackfillCatchupState {
    #[serde(rename = "lastCase")]
    last_case: MbtApplyCase,
    #[serde(rename = "lastOutcome")]
    last_outcome: String,
}

impl State<GsiBackfillCatchupDriver> for GsiBackfillCatchupState {
    fn from_driver(driver: &GsiBackfillCatchupDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            last_outcome: driver.last_outcome.clone(),
        })
    }
}

#[derive(Debug)]
struct GsiBackfillCatchupDriver {
    last_case: MbtApplyCase,
    last_outcome: String,
}

impl Default for GsiBackfillCatchupDriver {
    fn default() -> Self {
        Self {
            last_case: MbtApplyCase {
                current_version: 0,
                observation_version: 0,
                current_projects: false,
                observation_projects: false,
                history_available: true,
                scan_complete: false,
                drain_complete: false,
            },
            last_outcome: "not_checked".to_string(),
        }
    }
}

impl Driver for GsiBackfillCatchupDriver {
    type State = GsiBackfillCatchupState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                currentVersion: i64,
                observationVersion: i64,
                currentProjects: bool,
                observationProjects: bool,
                historyAvailable: bool,
                scanComplete: bool,
                drainComplete: bool,
            ) => {
                self.check(MbtApplyCase {
                    current_version: currentVersion,
                    observation_version: observationVersion,
                    current_projects: currentProjects,
                    observation_projects: observationProjects,
                    history_available: historyAvailable,
                    scan_complete: scanComplete,
                    drain_complete: drainComplete,
                });
            },
            step(
                currentVersion: i64?,
                observationVersion: i64?,
                currentProjects: bool?,
                observationProjects: bool?,
                historyAvailable: bool?,
                scanComplete: bool?,
                drainComplete: bool?,
            ) => {
                if let (
                    Some(current_version),
                    Some(observation_version),
                    Some(current_projects),
                    Some(observation_projects),
                    Some(history_available),
                    Some(scan_complete),
                    Some(drain_complete),
                ) = (
                    currentVersion,
                    observationVersion,
                    currentProjects,
                    observationProjects,
                    historyAvailable,
                    scanComplete,
                    drainComplete,
                ) {
                    self.check(MbtApplyCase {
                        current_version,
                        observation_version,
                        current_projects,
                        observation_projects,
                        history_available,
                        scan_complete,
                        drain_complete,
                    });
                }
            },
        })
    }
}

impl GsiBackfillCatchupDriver {
    fn check(&mut self, apply_case: MbtApplyCase) {
        let catchup_case = GsiCatchupApplyCase {
            current_version: apply_case.current_version,
            observation_version: apply_case.observation_version,
            current_projects: apply_case.current_projects,
            observation_projects: apply_case.observation_projects,
            history_available: apply_case.history_available,
            scan_complete: apply_case.scan_complete,
            drain_complete: apply_case.drain_complete,
        };
        self.last_outcome = plan_gsi_catchup_apply(&catchup_case).as_str().to_string();
        self.last_case = apply_case;
    }
}

#[quint_run(
    spec = "../../quint/gsi_backfill_catchup_mbt.qnt",
    max_samples = 96,
    max_steps = 12,
    seed = "0x651bac"
)]
fn gsi_backfill_catchup_mbt_matches_rust_boundary() -> impl Driver {
    GsiBackfillCatchupDriver::default()
}
