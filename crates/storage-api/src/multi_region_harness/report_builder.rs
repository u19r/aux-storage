use storage_types::{StorageError, StorageResult};

use super::{
    report::{
        HarnessConsistencyCheck, HarnessLatencySummary, HarnessOperationSummary,
        MultiRegionHarnessReport,
    },
    runner::{HarnessRunConfig, HarnessScenario},
};

pub(super) struct HarnessReportInput {
    pub(super) regions: Vec<String>,
    pub(super) operations: HarnessOperationSummary,
    pub(super) local_write_latencies_ms: Vec<f64>,
    pub(super) read_latencies_ms: Vec<f64>,
    pub(super) sampled_convergence_latencies_ms: Vec<f64>,
    pub(super) sampled_convergence_failures: u64,
    pub(super) divergent_keys: Vec<String>,
}

pub(super) fn build_report(
    config: &HarnessRunConfig,
    input: HarnessReportInput,
) -> MultiRegionHarnessReport {
    MultiRegionHarnessReport {
        scenario: scenario_name(config.scenario).to_string(),
        seed: config.seed,
        regions: input.regions,
        duration_secs: config.duration.as_secs(),
        ops_per_sec: config.ops_per_sec,
        achieved_ops_per_sec: input.operations.succeeded as f64 / config.duration.as_secs_f64(),
        item_size_bytes: config.item_size_bytes,
        hot_key_percent: config.hot_key_percent,
        hot_key_count: config.hot_key_count,
        delete_percent: config.delete_percent,
        read_percent: config.read_percent,
        bootstrap_item_count: config.bootstrap_item_count,
        clock_skew_emulation_ms: config.emulated_clock_skew_ms,
        fault_profile: format_fault_profile(config),
        operations: input.operations,
        local_latency_ms: HarnessLatencySummary::from_millis(&input.local_write_latencies_ms),
        read_latency_ms: HarnessLatencySummary::from_millis(&input.read_latencies_ms),
        sampled_convergence_latency_ms: HarnessLatencySummary::from_millis(
            &input.sampled_convergence_latencies_ms,
        ),
        consistency: HarnessConsistencyCheck {
            final_converged: input.divergent_keys.is_empty(),
            divergent_keys: input.divergent_keys,
            sampled_convergence_failures: input.sampled_convergence_failures,
        },
        notes: scenario_notes(config),
    }
}

pub(super) fn write_report_if_requested(
    report: &MultiRegionHarnessReport,
    config: HarnessRunConfig,
) -> StorageResult<()> {
    let Some(path) = config.report_path else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            StorageError::internal(&format!(
                "create report directory '{}': {error}",
                parent.display()
            ))
        })?;
    }
    let bytes = serde_json::to_vec_pretty(report)
        .map_err(|error| StorageError::internal(&format!("serialize harness report: {error}")))?;
    std::fs::write(&path, bytes).map_err(|error| {
        StorageError::internal(&format!(
            "write harness report '{}': {error}",
            path.display()
        ))
    })
}

fn format_fault_profile(config: &HarnessRunConfig) -> String {
    format!(
        "apply={}±{} heartbeat={}±{} drop={} duplicate={} queue={}",
        format_latency(
            config.fault_profile.apply_latency_ms,
            config.fault_profile.apply_latency_us
        ),
        format_latency(
            config.fault_profile.apply_latency_jitter_ms,
            config.fault_profile.apply_latency_jitter_us
        ),
        format_latency(
            config.fault_profile.heartbeat_latency_ms,
            config.fault_profile.heartbeat_latency_us
        ),
        format_latency(
            config.fault_profile.heartbeat_latency_jitter_ms,
            config.fault_profile.heartbeat_latency_jitter_us
        ),
        config.fault_profile.drop_probability_per_10k,
        config.fault_profile.duplicate_probability_per_10k,
        config.fault_profile.queue_probability_per_10k
    )
}

fn format_latency(ms: u64, us: u64) -> String {
    match (ms, us) {
        (0, 0) => "0ms".to_string(),
        (0, us) => format!("{us}us"),
        (ms, 0) => format!("{ms}ms"),
        (ms, us) => format!("{ms}ms+{us}us"),
    }
}

fn scenario_name(scenario: HarnessScenario) -> &'static str {
    match scenario {
        HarnessScenario::Perf => "perf",
        HarnessScenario::Soak => "soak",
        HarnessScenario::Chaos => "chaos",
        HarnessScenario::Bootstrap => "bootstrap",
    }
}

fn scenario_notes(config: &HarnessRunConfig) -> Vec<String> {
    let mut notes = vec![
        "Runs fully on one machine using the shared simulation harness".to_string(),
        "Convergence failures are reported with the exact scenario seed for reproduction"
            .to_string(),
    ];
    if config.emulated_clock_skew_ms > 0 {
        notes.push(
            "Clock skew is emulated as per-region write bias; true storage-layer clock injection \
             remains a separate hardening task"
                .to_string(),
        );
    }
    if config.hot_key_percent > 0 {
        notes.push(
            "Hot-key traffic exercises last-write-wins behavior and catches deterministic \
             tie-break regressions"
                .to_string(),
        );
    }
    notes
}
