use std::{collections::BTreeSet, fs, path::Path};

use fdb_chaos_model::{
    BackgroundLeaseEvent, OperationHistory, SharedKeyAudit, check_background_lease_events,
    check_shared_key_audits,
};
use serde::{Deserialize, Serialize};

use crate::{
    aggregate::{read_background_lease_event_groups, read_histories_and_shared_key_audits},
    artifact_io::write_json,
    cli::CliArgs,
};

#[derive(Debug, Serialize, Deserialize)]
struct HistoryPrefixMinimizationReport {
    schema_version: u32,
    checker: String,
    original_event_count: usize,
    minimized_event_count: usize,
    original_anomaly_count: usize,
    minimized_anomaly_count: usize,
    client_prefix_lengths: Vec<usize>,
    output_dir: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EventPrefixMinimizationReport {
    schema_version: u32,
    checker: String,
    original_event_count: usize,
    minimized_event_count: usize,
    original_anomaly_count: usize,
    minimized_anomaly_count: usize,
    client_prefix_lengths: Vec<usize>,
    output_dir: String,
}

pub(crate) fn minimize_history_command(args: CliArgs) -> Result<(), String> {
    let artifact_dir = args
        .artifact
        .ok_or_else(|| "minimize-history requires --artifact <run-dir>".to_string())?;
    let shared_key_minimized = minimize_shared_key_history_artifact(&artifact_dir)?;
    let background_lease_minimized = minimize_background_lease_artifact(&artifact_dir)?;
    if !shared_key_minimized && !background_lease_minimized {
        return Err(format!(
            "no minimizable aggregate artifacts found under {}",
            artifact_dir.display()
        ));
    }
    Ok(())
}

fn minimize_shared_key_history_artifact(artifact_dir: &Path) -> Result<bool, String> {
    let (histories, audits) = read_histories_and_shared_key_audits(artifact_dir)?;
    if histories.is_empty() {
        return Ok(false);
    }
    let original_report = check_shared_key_audits(&histories, &audits);
    let original_event_count = history_event_count(&histories);
    let mut prefix_lengths = histories
        .iter()
        .map(|history| history.events().len())
        .collect::<Vec<_>>();
    if original_report.anomaly_count > 0 {
        let required_signatures = anomaly_signatures(&original_report.anomalies);
        minimize_shared_key_prefixes(
            &histories,
            &audits,
            &required_signatures,
            &mut prefix_lengths,
        );
    }
    let minimized_histories = prefix_histories(&histories, &prefix_lengths);
    let minimized_report = check_shared_key_audits(&minimized_histories, &audits);
    let output_dir = artifact_dir.join("minimized-history");
    write_minimized_history_artifact(
        &output_dir,
        &minimized_histories,
        &audits,
        &minimized_report,
    )?;
    let report = HistoryPrefixMinimizationReport {
        schema_version: fdb_chaos_model::ARTIFACT_SCHEMA_VERSION,
        checker: "shared-key".to_string(),
        original_event_count,
        minimized_event_count: history_event_count(&minimized_histories),
        original_anomaly_count: original_report.anomaly_count,
        minimized_anomaly_count: minimized_report.anomaly_count,
        client_prefix_lengths: prefix_lengths,
        output_dir: output_dir.display().to_string(),
    };
    write_json(
        &artifact_dir.join("history-prefix-minimization.json"),
        &report,
    )?;
    println!(
        "fdb chaos minimized history: {}",
        artifact_dir
            .join("history-prefix-minimization.json")
            .display()
    );
    Ok(true)
}

fn minimize_background_lease_artifact(artifact_dir: &Path) -> Result<bool, String> {
    let event_groups = read_background_lease_event_groups(artifact_dir)?;
    if event_groups.iter().all(Vec::is_empty) {
        return Ok(false);
    }

    let original_events = flatten_event_groups(&event_groups);
    let original_report = check_background_lease_events(&original_events);
    let original_event_count = original_events.len();
    let mut prefix_lengths = event_groups.iter().map(Vec::len).collect::<Vec<_>>();
    if original_report.anomaly_count > 0 {
        let required_signatures = anomaly_signatures(&original_report.anomalies);
        minimize_background_lease_prefixes(
            &event_groups,
            &required_signatures,
            &mut prefix_lengths,
        );
    }
    let minimized_groups = prefix_event_groups(&event_groups, &prefix_lengths);
    let minimized_events = flatten_event_groups(&minimized_groups);
    let minimized_report = check_background_lease_events(&minimized_events);
    let output_dir = artifact_dir.join("minimized-background-lease");
    write_minimized_background_lease_artifact(&output_dir, &minimized_groups, &minimized_report)?;
    let report = EventPrefixMinimizationReport {
        schema_version: fdb_chaos_model::ARTIFACT_SCHEMA_VERSION,
        checker: "background-lease".to_string(),
        original_event_count,
        minimized_event_count: minimized_events.len(),
        original_anomaly_count: original_report.anomaly_count,
        minimized_anomaly_count: minimized_report.anomaly_count,
        client_prefix_lengths: prefix_lengths,
        output_dir: output_dir.display().to_string(),
    };
    write_json(
        &artifact_dir.join("background-lease-prefix-minimization.json"),
        &report,
    )?;
    println!(
        "fdb chaos minimized background leases: {}",
        artifact_dir
            .join("background-lease-prefix-minimization.json")
            .display()
    );
    Ok(true)
}
fn minimize_shared_key_prefixes(
    histories: &[OperationHistory],
    audits: &[SharedKeyAudit],
    required_signatures: &BTreeSet<String>,
    prefix_lengths: &mut [usize],
) {
    let mut changed = true;
    while changed {
        changed = false;
        for history_index in 0..prefix_lengths.len() {
            let current_len = prefix_lengths[history_index];
            for candidate_len in 0..current_len {
                let mut candidate_lengths = prefix_lengths.to_vec();
                candidate_lengths[history_index] = candidate_len;
                let candidate_histories = prefix_histories(histories, &candidate_lengths);
                let candidate_report = check_shared_key_audits(&candidate_histories, audits);
                if anomaly_signatures(&candidate_report.anomalies).is_superset(required_signatures)
                {
                    prefix_lengths[history_index] = candidate_len;
                    changed = true;
                    break;
                }
            }
        }
    }
}

fn minimize_background_lease_prefixes(
    event_groups: &[Vec<BackgroundLeaseEvent>],
    required_signatures: &BTreeSet<String>,
    prefix_lengths: &mut [usize],
) {
    let mut changed = true;
    while changed {
        changed = false;
        for group_index in 0..prefix_lengths.len() {
            let current_len = prefix_lengths[group_index];
            for candidate_len in 0..current_len {
                let mut candidate_lengths = prefix_lengths.to_vec();
                candidate_lengths[group_index] = candidate_len;
                let candidate_groups = prefix_event_groups(event_groups, &candidate_lengths);
                let candidate_events = flatten_event_groups(&candidate_groups);
                let candidate_report = check_background_lease_events(&candidate_events);
                if anomaly_signatures(&candidate_report.anomalies).is_superset(required_signatures)
                {
                    prefix_lengths[group_index] = candidate_len;
                    changed = true;
                    break;
                }
            }
        }
    }
}

fn anomaly_signatures(anomalies: &[fdb_chaos_model::Anomaly]) -> BTreeSet<String> {
    anomalies
        .iter()
        .map(|anomaly| format!("{:?}:{}", anomaly.kind, anomaly.key))
        .collect()
}

fn prefix_histories(
    histories: &[OperationHistory],
    prefix_lengths: &[usize],
) -> Vec<OperationHistory> {
    histories
        .iter()
        .zip(prefix_lengths.iter().copied())
        .map(|(history, prefix_len)| {
            let mut prefixed = OperationHistory::default();
            for event in history.events().iter().take(prefix_len).cloned() {
                prefixed.push(event);
            }
            prefixed
        })
        .collect()
}

fn prefix_event_groups(
    event_groups: &[Vec<BackgroundLeaseEvent>],
    prefix_lengths: &[usize],
) -> Vec<Vec<BackgroundLeaseEvent>> {
    event_groups
        .iter()
        .zip(prefix_lengths.iter().copied())
        .map(|(events, prefix_len)| events.iter().take(prefix_len).cloned().collect())
        .collect()
}

fn flatten_event_groups(event_groups: &[Vec<BackgroundLeaseEvent>]) -> Vec<BackgroundLeaseEvent> {
    event_groups
        .iter()
        .flat_map(|events| events.iter().cloned())
        .collect()
}

fn history_event_count(histories: &[OperationHistory]) -> usize {
    histories.iter().map(|history| history.events().len()).sum()
}

fn write_minimized_history_artifact(
    output_dir: &Path,
    histories: &[OperationHistory],
    audits: &[SharedKeyAudit],
    report: &fdb_chaos_model::SharedKeyCheckReport,
) -> Result<(), String> {
    fs::create_dir_all(output_dir)
        .map_err(|err| format!("failed to create {}: {err}", output_dir.display()))?;
    for (client_index, history) in histories.iter().enumerate() {
        let client_dir = output_dir.join(format!("client-{client_index}"));
        fs::create_dir_all(&client_dir)
            .map_err(|err| format!("failed to create {}: {err}", client_dir.display()))?;
        let history_jsonl = history
            .to_json_lines()
            .map_err(|err| format!("failed to serialize minimized history: {err}"))?;
        fs::write(client_dir.join("history.jsonl"), history_jsonl)
            .map_err(|err| format!("failed to write minimized history: {err}"))?;
    }
    for audit in audits {
        let client_dir = output_dir.join(format!("client-{}", audit.client_id));
        fs::create_dir_all(&client_dir)
            .map_err(|err| format!("failed to create {}: {err}", client_dir.display()))?;
        let audit_json = serde_json::to_string_pretty(audit)
            .map_err(|err| format!("failed to serialize shared-key audit: {err}"))?;
        fs::write(
            client_dir.join("shared-key-audit.json"),
            format!("{audit_json}\n"),
        )
        .map_err(|err| format!("failed to write minimized shared-key audit: {err}"))?;
    }
    write_json(&output_dir.join("shared-key-check.json"), report)
}

fn write_minimized_background_lease_artifact(
    output_dir: &Path,
    event_groups: &[Vec<BackgroundLeaseEvent>],
    report: &fdb_chaos_model::BackgroundLeaseCheckReport,
) -> Result<(), String> {
    fs::create_dir_all(output_dir)
        .map_err(|err| format!("failed to create {}: {err}", output_dir.display()))?;
    for (client_index, events) in event_groups.iter().enumerate() {
        let client_dir = output_dir.join(format!("client-{client_index}"));
        fs::create_dir_all(&client_dir)
            .map_err(|err| format!("failed to create {}: {err}", client_dir.display()))?;
        let mut lines = String::new();
        for event in events {
            lines.push_str(&serde_json::to_string(event).map_err(|err| {
                format!("failed to serialize minimized background lease event: {err}")
            })?);
            lines.push('\n');
        }
        fs::write(client_dir.join("background-lease-events.jsonl"), lines)
            .map_err(|err| format!("failed to write minimized background lease events: {err}"))?;
    }
    write_json(&output_dir.join("background-lease-check.json"), report)
}
