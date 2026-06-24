use std::{fs, path::Path};

use fdb_chaos_model::{
    BackgroundLeaseEvent, OperationHistory, SharedKeyAudit, TrimProviderSnapshot, TrimScopeReport,
    check_aggregate_trim_scopes, check_background_lease_events, check_shared_key_audits,
};

use crate::{
    artifact_io::{client_artifact_dirs, write_json},
    cli::CliArgs,
};

const AGGREGATE_CHECKERS: &[AggregateChecker] = &[
    AggregateChecker {
        name: "background-lease",
        run: run_background_lease_checker,
    },
    AggregateChecker {
        name: "shared-key",
        run: run_shared_key_checker,
    },
    AggregateChecker {
        name: "trim-aggregate",
        run: run_trim_aggregate_checker,
    },
];

struct AggregateChecker {
    name: &'static str,
    run: fn(&Path) -> Result<CheckerRunStatus, String>,
}

enum CheckerRunStatus {
    Skipped,
    Checked,
}
pub(crate) fn post_process_artifacts(_args: &CliArgs, artifact_dir: &Path) -> Result<(), String> {
    for checker in AGGREGATE_CHECKERS {
        (checker.run)(artifact_dir)
            .map_err(|err| format!("aggregate checker '{}' failed: {err}", checker.name))?;
    }
    Ok(())
}

fn run_background_lease_checker(artifact_dir: &Path) -> Result<CheckerRunStatus, String> {
    let mut events = Vec::new();
    for client_dir in client_artifact_dirs(artifact_dir)? {
        let lease_path = client_dir.join("background-lease-events.jsonl");
        if !lease_path.exists() {
            continue;
        }
        let input = fs::read_to_string(&lease_path)
            .map_err(|err| format!("failed to read {}: {err}", lease_path.display()))?;
        for (line_index, line) in input.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            events.push(
                serde_json::from_str::<BackgroundLeaseEvent>(trimmed).map_err(|err| {
                    format!(
                        "failed to parse {} line {}: {err}",
                        lease_path.display(),
                        line_index + 1
                    )
                })?,
            );
        }
    }
    if events.is_empty() {
        return Ok(CheckerRunStatus::Skipped);
    }

    let report = check_background_lease_events(&events);
    write_json(&artifact_dir.join("background-lease-check.json"), &report)?;
    if report.anomaly_count > 0 {
        return Err(format!(
            "background lease checker found {} anomalies; see {}",
            report.anomaly_count,
            artifact_dir.join("background-lease-check.json").display()
        ));
    }
    Ok(CheckerRunStatus::Checked)
}

fn run_shared_key_checker(artifact_dir: &Path) -> Result<CheckerRunStatus, String> {
    let (histories, audits) = read_histories_and_shared_key_audits(artifact_dir)?;
    if histories.is_empty() && audits.is_empty() {
        return Ok(CheckerRunStatus::Skipped);
    }
    let report = check_shared_key_audits(&histories, &audits);
    write_json(&artifact_dir.join("shared-key-check.json"), &report)?;
    if report.anomaly_count > 0 {
        return Err(format!(
            "shared-key checker found {} anomalies; see {}",
            report.anomaly_count,
            artifact_dir.join("shared-key-check.json").display()
        ));
    }
    Ok(CheckerRunStatus::Checked)
}

fn run_trim_aggregate_checker(artifact_dir: &Path) -> Result<CheckerRunStatus, String> {
    let mut trim_reports = Vec::new();
    let mut trim_snapshot = None;
    for client_dir in client_artifact_dirs(artifact_dir)? {
        let trim_report_path = client_dir.join("trim-scopes.json");
        if trim_report_path.exists() {
            let report = fs::read_to_string(&trim_report_path)
                .map_err(|err| format!("failed to read {}: {err}", trim_report_path.display()))?;
            trim_reports.push(
                serde_json::from_str::<TrimScopeReport>(&report).map_err(|err| {
                    format!("failed to parse {}: {err}", trim_report_path.display())
                })?,
            );
        }

        let trim_snapshot_path = client_dir.join("trim-provider-snapshot.json");
        if trim_snapshot_path.exists() {
            let snapshot = fs::read_to_string(&trim_snapshot_path)
                .map_err(|err| format!("failed to read {}: {err}", trim_snapshot_path.display()))?;
            if trim_snapshot
                .replace(
                    serde_json::from_str::<TrimProviderSnapshot>(&snapshot).map_err(|err| {
                        format!("failed to parse {}: {err}", trim_snapshot_path.display())
                    })?,
                )
                .is_some()
            {
                return Err(format!(
                    "multiple trim provider snapshots found under {}",
                    artifact_dir.display()
                ));
            }
        }
    }
    if trim_reports.is_empty() && trim_snapshot.is_none() {
        return Ok(CheckerRunStatus::Skipped);
    }

    let trim_snapshot = trim_snapshot.ok_or_else(|| {
        format!(
            "run did not write a trim provider snapshot under {}",
            artifact_dir.display()
        )
    })?;
    let trim_report = check_aggregate_trim_scopes(&trim_reports, &trim_snapshot);
    write_json(
        &artifact_dir.join("trim-aggregate-check.json"),
        &trim_report,
    )?;
    if trim_report.anomaly_count > 0 {
        return Err(format!(
            "aggregate trim checker found {} anomalies; see {}",
            trim_report.anomaly_count,
            artifact_dir.join("trim-aggregate-check.json").display()
        ));
    }
    Ok(CheckerRunStatus::Checked)
}

pub(crate) fn read_histories_and_shared_key_audits(
    artifact_dir: &Path,
) -> Result<(Vec<OperationHistory>, Vec<SharedKeyAudit>), String> {
    let mut histories = Vec::new();
    let mut audits = Vec::new();
    for client_dir in client_artifact_dirs(artifact_dir)? {
        let history_path = client_dir.join("history.jsonl");
        if history_path.exists() {
            let history = fs::read_to_string(&history_path)
                .map_err(|err| format!("failed to read {}: {err}", history_path.display()))?;
            histories.push(
                OperationHistory::from_json_lines(&history)
                    .map_err(|err| format!("failed to parse {}: {err}", history_path.display()))?,
            );
        }

        let audit_path = client_dir.join("shared-key-audit.json");
        if audit_path.exists() {
            let audit = fs::read_to_string(&audit_path)
                .map_err(|err| format!("failed to read {}: {err}", audit_path.display()))?;
            audits.push(
                serde_json::from_str::<SharedKeyAudit>(&audit)
                    .map_err(|err| format!("failed to parse {}: {err}", audit_path.display()))?,
            );
        }
    }
    Ok((histories, audits))
}

pub(crate) fn read_background_lease_event_groups(
    artifact_dir: &Path,
) -> Result<Vec<Vec<BackgroundLeaseEvent>>, String> {
    let mut event_groups = Vec::new();
    for client_dir in client_artifact_dirs(artifact_dir)? {
        let lease_path = client_dir.join("background-lease-events.jsonl");
        if !lease_path.exists() {
            continue;
        }
        let input = fs::read_to_string(&lease_path)
            .map_err(|err| format!("failed to read {}: {err}", lease_path.display()))?;
        let mut events = Vec::new();
        for (line_index, line) in input.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            events.push(
                serde_json::from_str::<BackgroundLeaseEvent>(trimmed).map_err(|err| {
                    format!(
                        "failed to parse {} line {}: {err}",
                        lease_path.display(),
                        line_index + 1
                    )
                })?,
            );
        }
        event_groups.push(events);
    }
    Ok(event_groups)
}
