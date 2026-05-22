use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct HarnessLatencySummary {
    pub samples: usize,
    pub p50_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub mean_ms: Option<f64>,
    pub max_ms: Option<f64>,
}

impl HarnessLatencySummary {
    #[must_use]
    pub fn from_millis(samples: &[f64]) -> Self {
        if samples.is_empty() {
            return Self {
                samples: 0,
                p50_ms: None,
                p95_ms: None,
                p99_ms: None,
                mean_ms: None,
                max_ms: None,
            };
        }

        let mut sorted = samples.to_vec();
        sorted.sort_by(|left, right| left.total_cmp(right));
        let mean_ms = sorted.iter().sum::<f64>() / sorted.len() as f64;

        Self {
            samples: sorted.len(),
            p50_ms: percentile(&sorted, 0.50),
            p95_ms: percentile(&sorted, 0.95),
            p99_ms: percentile(&sorted, 0.99),
            mean_ms: Some(mean_ms),
            max_ms: sorted.last().copied(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct HarnessOperationSummary {
    pub attempted: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HarnessConsistencyCheck {
    pub final_converged: bool,
    pub divergent_keys: Vec<String>,
    pub sampled_convergence_failures: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MultiRegionHarnessReport {
    pub scenario: String,
    pub seed: u64,
    pub regions: Vec<String>,
    pub duration_secs: u64,
    pub ops_per_sec: u64,
    pub achieved_ops_per_sec: f64,
    pub item_size_bytes: usize,
    pub hot_key_percent: u8,
    pub hot_key_count: usize,
    pub delete_percent: u8,
    pub read_percent: u8,
    pub bootstrap_item_count: usize,
    pub clock_skew_emulation_ms: u64,
    pub fault_profile: String,
    pub operations: HarnessOperationSummary,
    pub local_latency_ms: HarnessLatencySummary,
    pub read_latency_ms: HarnessLatencySummary,
    pub sampled_convergence_latency_ms: HarnessLatencySummary,
    pub consistency: HarnessConsistencyCheck,
    pub notes: Vec<String>,
}

fn percentile(sorted: &[f64], quantile: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }

    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted.get(index).copied()
}
