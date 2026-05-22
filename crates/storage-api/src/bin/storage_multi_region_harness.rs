use std::path::PathBuf;

use clap::Parser;
use storage_api::{
    multi_region_harness::MultiRegionHarnessRunner,
    multi_region_harness_cli::{
        HarnessCliRunArgs, HarnessScenarioArg, HarnessStorageBackendArg,
        harness_run_config_from_args,
    },
};
use storage_types::{StorageEnum, StorageResult, context::WrappedError as _};

#[derive(Parser)]
#[command(name = "storage_multi_region_harness")]
#[command(about = "Local single-machine multi-region chaos, soak, and perf harness")]
struct Args {
    #[arg(value_enum)]
    scenario: HarnessScenarioArg,

    #[arg(long, default_value_t = 2)]
    regions: usize,

    #[arg(long, value_enum, default_value = "sqlite")]
    storage_backend: HarnessStorageBackendArg,

    #[arg(long, value_enum, value_delimiter = ',')]
    region_storage_backends: Vec<HarnessStorageBackendArg>,

    #[arg(long)]
    sqlite_database_dir: Option<PathBuf>,

    #[arg(long)]
    postgres_dsn_template: Option<String>,

    #[arg(long, default_value_t = 16)]
    postgres_max_pool_size: usize,

    #[arg(long, default_value_t = false)]
    postgres_tls: bool,

    #[arg(long)]
    foundationdb_cluster_file: Option<String>,

    #[arg(long)]
    foundationdb_subspace_prefix: Option<String>,

    #[arg(long, default_value_t = 10)]
    duration_secs: u64,

    #[arg(long, default_value_t = 1)]
    warmup_secs: u64,

    #[arg(long, default_value_t = 2)]
    cooldown_secs: u64,

    #[arg(long, default_value_t = 200)]
    ops_per_sec: u64,

    #[arg(long, default_value_t = 1024)]
    item_size_bytes: usize,

    #[arg(long, default_value_t = 0)]
    hot_key_percent: u8,

    #[arg(long, default_value_t = 8)]
    hot_key_count: usize,

    #[arg(long, default_value_t = 0)]
    delete_percent: u8,

    #[arg(long, default_value_t = 0)]
    read_percent: u8,

    #[arg(long, default_value_t = 25)]
    sample_every: usize,

    #[arg(long, default_value_t = 8)]
    load_workers: usize,

    #[arg(long, default_value_t = 32)]
    max_in_flight_convergence_checks: usize,

    #[arg(long, default_value_t = 1_000)]
    batch_mutation_limit: usize,

    #[arg(long, default_value_t = 512 * 1024)]
    batch_byte_limit: usize,

    #[arg(long, default_value_t = 1)]
    bootstrap_item_count: usize,

    #[arg(long, default_value_t = 1)]
    seed: u64,

    #[arg(long, default_value_t = 0)]
    emulated_clock_skew_ms: u64,

    #[arg(long, default_value_t = 0)]
    apply_latency_ms: u64,

    #[arg(long, default_value_t = 0)]
    apply_latency_us: u64,

    #[arg(long, default_value_t = 0)]
    apply_latency_jitter_ms: u64,

    #[arg(long, default_value_t = 0)]
    apply_latency_jitter_us: u64,

    #[arg(long, default_value_t = 0)]
    heartbeat_latency_ms: u64,

    #[arg(long, default_value_t = 0)]
    heartbeat_latency_us: u64,

    #[arg(long, default_value_t = 0)]
    heartbeat_latency_jitter_ms: u64,

    #[arg(long, default_value_t = 0)]
    heartbeat_latency_jitter_us: u64,

    #[arg(long, default_value_t = 0)]
    drop_probability_per_10k: u16,

    #[arg(long, default_value_t = 0)]
    duplicate_probability_per_10k: u16,

    #[arg(long, default_value_t = 0)]
    queue_probability_per_10k: u16,

    #[arg(long, default_value = "multi-region-local-harness")]
    table_name: String,

    #[arg(long)]
    report_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> StorageResult<()> {
    let args = Args::parse();
    let report =
        match MultiRegionHarnessRunner::run(harness_run_config_from_args(args.into())).await {
            Ok(report) => report,
            Err(error) => {
                let (root, contexts) = error.recursive_context(Vec::new());
                if let StorageEnum::InternalServerError { message } = root {
                    println!("Internal error detail: {message}");
                }
                if !contexts.is_empty() {
                    println!("Context:");
                    for (idx, context) in contexts.iter().enumerate() {
                        println!("  {}. {context}", idx + 1);
                    }
                }
                return Err(error);
            }
        };

    let body = serde_json::to_string_pretty(&report).map_err(|error| {
        storage_types::StorageError::internal(&format!("serialize report: {error}"))
    })?;
    println!("{body}");
    Ok(())
}

impl From<Args> for HarnessCliRunArgs {
    fn from(args: Args) -> Self {
        Self {
            scenario: args.scenario,
            regions: args.regions,
            storage_backend: args.storage_backend,
            region_storage_backends: args.region_storage_backends,
            sqlite_database_dir: args.sqlite_database_dir,
            postgres_dsn_template: args.postgres_dsn_template,
            postgres_max_pool_size: args.postgres_max_pool_size,
            postgres_tls: args.postgres_tls,
            foundationdb_cluster_file: args.foundationdb_cluster_file,
            foundationdb_subspace_prefix: args.foundationdb_subspace_prefix,
            duration_secs: args.duration_secs,
            warmup_secs: args.warmup_secs,
            cooldown_secs: args.cooldown_secs,
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
            table_name: args.table_name,
            report_path: args.report_path,
        }
    }
}
