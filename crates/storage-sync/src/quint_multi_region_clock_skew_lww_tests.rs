#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct ClockSkewCase {
    #[serde(rename = "currentPhysicalMs")]
    current_physical_ms: i64,
    #[serde(rename = "currentRegion")]
    current_region: String,
    #[serde(rename = "currentSequence")]
    current_sequence: i64,
    #[serde(rename = "incomingPhysicalMs")]
    incoming_physical_ms: i64,
    #[serde(rename = "incomingRegion")]
    incoming_region: String,
    #[serde(rename = "incomingSequence")]
    incoming_sequence: i64,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct ClockSkewLwwState {
    #[serde(rename = "lastCase")]
    last_case: ClockSkewCase,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<ClockSkewLwwDriver> for ClockSkewLwwState {
    fn from_driver(driver: &ClockSkewLwwDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct ClockSkewLwwDriver {
    last_case: ClockSkewCase,
    last_decision: String,
}

impl Default for ClockSkewLwwDriver {
    fn default() -> Self {
        Self {
            last_case: ClockSkewCase {
                current_physical_ms: 0,
                current_region: "region-a".to_string(),
                current_sequence: 0,
                incoming_physical_ms: 0,
                incoming_region: "region-a".to_string(),
                incoming_sequence: 0,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for ClockSkewLwwDriver {
    type State = ClockSkewLwwState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                currentPhysicalMs: i64,
                currentRegion: String,
                currentSequence: i64,
                incomingPhysicalMs: i64,
                incomingRegion: String,
                incomingSequence: i64,
            ) => {
                self.check(ClockSkewCase {
                    current_physical_ms: currentPhysicalMs,
                    current_region: currentRegion,
                    current_sequence: currentSequence,
                    incoming_physical_ms: incomingPhysicalMs,
                    incoming_region: incomingRegion,
                    incoming_sequence: incomingSequence,
                });
            },
            step(
                currentPhysicalMs: i64?,
                currentRegion: String?,
                currentSequence: i64?,
                incomingPhysicalMs: i64?,
                incomingRegion: String?,
                incomingSequence: i64?,
            ) => {
                if let (
                    Some(current_physical_ms),
                    Some(current_region),
                    Some(current_sequence),
                    Some(incoming_physical_ms),
                    Some(incoming_region),
                    Some(incoming_sequence),
                ) = (
                    currentPhysicalMs,
                    currentRegion,
                    currentSequence,
                    incomingPhysicalMs,
                    incomingRegion,
                    incomingSequence,
                ) {
                    self.check(ClockSkewCase {
                        current_physical_ms,
                        current_region,
                        current_sequence,
                        incoming_physical_ms,
                        incoming_region,
                        incoming_sequence,
                    });
                }
            },
        })
    }
}

impl ClockSkewLwwDriver {
    fn check(&mut self, case: ClockSkewCase) {
        self.last_decision = lww_decision(&case).to_string();
        self.last_case = case;
    }
}

fn lww_decision(case: &ClockSkewCase) -> &'static str {
    match (
        case.current_physical_ms,
        case.current_region.as_str(),
        case.current_sequence,
    )
        .cmp(&(
            case.incoming_physical_ms,
            case.incoming_region.as_str(),
            case.incoming_sequence,
        )) {
        std::cmp::Ordering::Greater => "skip_stale",
        std::cmp::Ordering::Equal => "skip_duplicate",
        std::cmp::Ordering::Less => "apply",
    }
}

#[quint_run(
    spec = "../../quint/multi_region_clock_skew_lww_mbt.qnt",
    max_samples = 64,
    max_steps = 8,
    seed = "0xc10c55e9"
)]
fn multi_region_clock_skew_lww_mbt_matches_rust_boundary() -> impl Driver {
    ClockSkewLwwDriver::default()
}
