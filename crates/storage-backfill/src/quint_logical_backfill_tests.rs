#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_types::ItemStreamVersion;

use crate::{
    LogicalBootstrapPreflightCase, LogicalBootstrapPreflightDecision, LogicalImportApplyCase,
    LogicalImportApplyDecision, LogicalImportRecordKind, plan_logical_bootstrap_preflight,
    plan_logical_import_apply,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct MbtApplyCase {
    #[serde(rename = "currentVersion")]
    current_version: i64,
    #[serde(rename = "incomingVersion")]
    incoming_version: i64,
    #[serde(rename = "incomingKind")]
    incoming_kind: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct LogicalBackfillState {
    #[serde(rename = "lastCase")]
    last_case: MbtApplyCase,
    #[serde(rename = "lastDecision")]
    last_decision: String,
    #[serde(rename = "lastPreflightCase")]
    last_preflight_case: MbtPreflightCase,
    #[serde(rename = "lastPreflightDecision")]
    last_preflight_decision: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct MbtPreflightCase {
    #[serde(rename = "destinationEmpty")]
    destination_empty: bool,
    #[serde(rename = "preflightMarkerPresent")]
    preflight_marker_present: bool,
}

impl State<LogicalBackfillDriver> for LogicalBackfillState {
    fn from_driver(driver: &LogicalBackfillDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            last_decision: driver.last_decision.clone(),
            last_preflight_case: driver.last_preflight_case.clone(),
            last_preflight_decision: driver.last_preflight_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct LogicalBackfillDriver {
    last_case: MbtApplyCase,
    last_decision: String,
    last_preflight_case: MbtPreflightCase,
    last_preflight_decision: String,
}

impl Default for LogicalBackfillDriver {
    fn default() -> Self {
        Self {
            last_case: MbtApplyCase {
                current_version: -1,
                incoming_version: 1,
                incoming_kind: "present_item".to_string(),
            },
            last_decision: "not_checked".to_string(),
            last_preflight_case: MbtPreflightCase {
                destination_empty: true,
                preflight_marker_present: false,
            },
            last_preflight_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for LogicalBackfillDriver {
    type State = LogicalBackfillState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(currentVersion: i64, incomingVersion: i64, incomingKind: String) => {
                self.check(MbtApplyCase {
                    current_version: currentVersion,
                    incoming_version: incomingVersion,
                    incoming_kind: incomingKind,
                })?;
            },
            CheckPreflight(destinationEmpty: bool, preflightMarkerPresent: bool) => {
                self.check_preflight(MbtPreflightCase {
                    destination_empty: destinationEmpty,
                    preflight_marker_present: preflightMarkerPresent,
                });
            },
            step(currentVersion: i64?, incomingVersion: i64?, incomingKind: String?) => {
                if let (Some(current_version), Some(incoming_version), Some(incoming_kind)) =
                    (currentVersion, incomingVersion, incomingKind)
                {
                    self.check(MbtApplyCase {
                        current_version,
                        incoming_version,
                        incoming_kind,
                    })?;
                }
            },
            step(destinationEmpty: bool?, preflightMarkerPresent: bool?) => {
                if let (Some(destination_empty), Some(preflight_marker_present)) =
                    (destinationEmpty, preflightMarkerPresent)
                {
                    self.check_preflight(MbtPreflightCase {
                        destination_empty,
                        preflight_marker_present,
                    });
                }
            },
        })
    }
}

impl LogicalBackfillDriver {
    fn check(&mut self, apply_case: MbtApplyCase) -> Result {
        let current_version = if apply_case.current_version < 0 {
            None
        } else {
            Some(ItemStreamVersion::try_from(apply_case.current_version)?)
        };
        let incoming_kind = match apply_case.incoming_kind.as_str() {
            "present_item" => LogicalImportRecordKind::PresentItem,
            "tombstone" => LogicalImportRecordKind::Tombstone,
            other => {
                return Err(std::io::Error::other(format!(
                    "unsupported logical import kind {other}"
                ))
                .into());
            }
        };
        let decision = plan_logical_import_apply(LogicalImportApplyCase::new(
            current_version,
            ItemStreamVersion::try_from(apply_case.incoming_version)?,
            incoming_kind,
        ));

        self.last_decision = decision_name(decision).to_string();
        self.last_case = apply_case;
        Ok(())
    }

    fn check_preflight(&mut self, preflight_case: MbtPreflightCase) {
        let decision = plan_logical_bootstrap_preflight(LogicalBootstrapPreflightCase {
            destination_empty: preflight_case.destination_empty,
            preflight_marker_present: preflight_case.preflight_marker_present,
        });

        self.last_preflight_decision = preflight_decision_name(decision).to_string();
        self.last_preflight_case = preflight_case;
    }
}

fn decision_name(decision: LogicalImportApplyDecision) -> &'static str {
    match decision {
        LogicalImportApplyDecision::ApplyPresentItem => "apply_present_item",
        LogicalImportApplyDecision::ApplyTombstone => "apply_tombstone",
        LogicalImportApplyDecision::IgnoreDuplicate => "ignore_duplicate",
        LogicalImportApplyDecision::IgnoreStale => "ignore_stale",
    }
}

fn preflight_decision_name(decision: LogicalBootstrapPreflightDecision) -> &'static str {
    match decision {
        LogicalBootstrapPreflightDecision::AllowEmptyDestination => "allow_empty_destination",
        LogicalBootstrapPreflightDecision::AllowRetryAfterPreflight => {
            "allow_retry_after_preflight"
        }
        LogicalBootstrapPreflightDecision::RejectNonEmptyDestination => {
            "reject_non_empty_destination"
        }
    }
}

#[quint_run(
    spec = "../../quint/logical_backfill_mbt.qnt",
    max_samples = 96,
    max_steps = 12,
    seed = "0x1091ca1"
)]
fn logical_backfill_mbt_matches_rust_boundary() -> impl Driver {
    LogicalBackfillDriver::default()
}
