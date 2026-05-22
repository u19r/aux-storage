use std::{path::PathBuf, time::Duration};

use clap::ValueEnum;
use storage_types::TableName;

use crate::multi_region_harness::{
    HarnessFaultProfile, HarnessRunConfig, HarnessScenario, SimulationStorageBackend,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum HarnessScenarioArg {
    Perf,
    Soak,
    Chaos,
    Bootstrap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum HarnessStorageBackendArg {
    Sqlite,
    Turso,
    Postgres,
    Rocksdb,
    Foundationdb,
}

#[derive(Debug)]
pub struct HarnessCliRunArgs {
    pub scenario: HarnessScenarioArg,
    pub regions: usize,
    pub storage_backend: HarnessStorageBackendArg,
    pub region_storage_backends: Vec<HarnessStorageBackendArg>,
    pub sqlite_database_dir: Option<PathBuf>,
    pub postgres_dsn_template: Option<String>,
    pub postgres_max_pool_size: usize,
    pub postgres_tls: bool,
    pub foundationdb_cluster_file: Option<String>,
    pub foundationdb_subspace_prefix: Option<String>,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub cooldown_secs: u64,
    pub ops_per_sec: u64,
    pub item_size_bytes: usize,
    pub hot_key_percent: u8,
    pub hot_key_count: usize,
    pub delete_percent: u8,
    pub read_percent: u8,
    pub sample_every: usize,
    pub load_workers: usize,
    pub max_in_flight_convergence_checks: usize,
    pub batch_mutation_limit: usize,
    pub batch_byte_limit: usize,
    pub bootstrap_item_count: usize,
    pub seed: u64,
    pub emulated_clock_skew_ms: u64,
    pub apply_latency_ms: u64,
    pub apply_latency_us: u64,
    pub apply_latency_jitter_ms: u64,
    pub apply_latency_jitter_us: u64,
    pub heartbeat_latency_ms: u64,
    pub heartbeat_latency_us: u64,
    pub heartbeat_latency_jitter_ms: u64,
    pub heartbeat_latency_jitter_us: u64,
    pub drop_probability_per_10k: u16,
    pub duplicate_probability_per_10k: u16,
    pub queue_probability_per_10k: u16,
    pub table_name: String,
    pub report_path: Option<PathBuf>,
}

pub fn harness_scenario(arg: HarnessScenarioArg) -> HarnessScenario {
    match arg {
        HarnessScenarioArg::Perf => HarnessScenario::Perf,
        HarnessScenarioArg::Soak => HarnessScenario::Soak,
        HarnessScenarioArg::Chaos => HarnessScenario::Chaos,
        HarnessScenarioArg::Bootstrap => HarnessScenario::Bootstrap,
    }
}

pub fn scenario_name(scenario: HarnessScenario) -> &'static str {
    match scenario {
        HarnessScenario::Perf => "perf",
        HarnessScenario::Soak => "soak",
        HarnessScenario::Chaos => "chaos",
        HarnessScenario::Bootstrap => "bootstrap",
    }
}

pub fn storage_backend(arg: HarnessStorageBackendArg) -> SimulationStorageBackend {
    match arg {
        HarnessStorageBackendArg::Sqlite => SimulationStorageBackend::Sqlite,
        HarnessStorageBackendArg::Turso => SimulationStorageBackend::Turso,
        HarnessStorageBackendArg::Postgres => SimulationStorageBackend::Postgres,
        HarnessStorageBackendArg::Rocksdb => SimulationStorageBackend::Rocksdb,
        HarnessStorageBackendArg::Foundationdb => SimulationStorageBackend::Foundationdb,
    }
}

pub fn default_report_path(scenario: HarnessScenario) -> PathBuf {
    let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S");
    workspace_root()
        .join("run-artifacts")
        .join("multi-region")
        .join(format!("{}-{ts}.json", scenario_name(scenario)))
}

pub fn harness_run_config_from_args(args: HarnessCliRunArgs) -> HarnessRunConfig {
    let scenario = harness_scenario(args.scenario);
    let report_path = args
        .report_path
        .unwrap_or_else(|| default_report_path(scenario));
    HarnessRunConfig {
        scenario,
        table_name: TableName::new(&args.table_name),
        regions: args.regions,
        storage_backend: storage_backend(args.storage_backend),
        region_storage_backends: args
            .region_storage_backends
            .into_iter()
            .map(storage_backend)
            .collect(),
        sqlite_database_dir: args.sqlite_database_dir,
        postgres_dsn_template: args.postgres_dsn_template,
        postgres_max_pool_size: args.postgres_max_pool_size,
        postgres_tls: args.postgres_tls,
        foundationdb_cluster_file: args.foundationdb_cluster_file,
        foundationdb_subspace_prefix: args.foundationdb_subspace_prefix,
        duration: Duration::from_secs(args.duration_secs),
        warmup: Duration::from_secs(args.warmup_secs),
        cooldown: Duration::from_secs(args.cooldown_secs),
        ops_per_sec: args.ops_per_sec,
        item_size_bytes: args.item_size_bytes,
        hot_key_percent: args.hot_key_percent,
        hot_key_count: args.hot_key_count,
        delete_percent: args.delete_percent,
        read_percent: args.read_percent,
        sample_every: args.sample_every,
        load_workers: args.load_workers,
        max_in_flight_convergence_checks: args.max_in_flight_convergence_checks,
        batch_mutation_limit: args.batch_mutation_limit,
        batch_byte_limit: args.batch_byte_limit,
        bootstrap_item_count: args.bootstrap_item_count,
        seed: args.seed,
        emulated_clock_skew_ms: args.emulated_clock_skew_ms,
        fault_profile: HarnessFaultProfile {
            apply_latency_ms: args.apply_latency_ms,
            apply_latency_us: args.apply_latency_us,
            apply_latency_jitter_ms: args.apply_latency_jitter_ms,
            apply_latency_jitter_us: args.apply_latency_jitter_us,
            heartbeat_latency_ms: args.heartbeat_latency_ms,
            heartbeat_latency_us: args.heartbeat_latency_us,
            heartbeat_latency_jitter_ms: args.heartbeat_latency_jitter_ms,
            heartbeat_latency_jitter_us: args.heartbeat_latency_jitter_us,
            drop_probability_per_10k: args.drop_probability_per_10k,
            duplicate_probability_per_10k: args.duplicate_probability_per_10k,
            queue_probability_per_10k: args.queue_probability_per_10k,
        },
        report_path: Some(report_path),
    }
}

fn workspace_root() -> PathBuf {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    if let Some(crates_dir) = manifest_dir.parent()
        && let Some(root) = crates_dir.parent()
    {
        return root.to_path_buf();
    }
    manifest_dir.to_path_buf()
}
