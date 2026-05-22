use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use storage_types::{StorageError, StorageResult, TableName};
use tokio::{task::JoinSet, time::Instant};

use crate::multi_region_harness::{
    convergence::{ConvergenceSampler, collect_divergent_keys},
    load::{execute_put, merge_operation_summary, region_for_operation, run_load_worker},
    report::{HarnessOperationSummary, MultiRegionHarnessReport},
    report_builder::{HarnessReportInput, build_report, write_report_if_requested},
    simulation::{SimulationHarness, SimulationHarnessConfig, SimulationStorageBackend},
    validation::{latency_duration, region_names, validate_run_config},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessScenario {
    Perf,
    Soak,
    Chaos,
    Bootstrap,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HarnessFaultProfile {
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
}

#[derive(Debug, Clone)]
pub struct HarnessRunConfig {
    pub scenario: HarnessScenario,
    pub table_name: TableName,
    pub regions: usize,
    pub storage_backend: SimulationStorageBackend,
    pub region_storage_backends: Vec<SimulationStorageBackend>,
    pub sqlite_database_dir: Option<PathBuf>,
    pub postgres_dsn_template: Option<String>,
    pub postgres_max_pool_size: usize,
    pub postgres_tls: bool,
    pub foundationdb_cluster_file: Option<String>,
    pub foundationdb_subspace_prefix: Option<String>,
    pub duration: Duration,
    pub warmup: Duration,
    pub cooldown: Duration,
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
    pub fault_profile: HarnessFaultProfile,
    pub report_path: Option<PathBuf>,
}

impl Default for HarnessRunConfig {
    fn default() -> Self {
        Self {
            scenario: HarnessScenario::Perf,
            table_name: TableName::new("multi-region-local-harness"),
            regions: 2,
            storage_backend: SimulationStorageBackend::Sqlite,
            region_storage_backends: Vec::new(),
            sqlite_database_dir: None,
            postgres_dsn_template: None,
            postgres_max_pool_size: 16,
            postgres_tls: false,
            foundationdb_cluster_file: None,
            foundationdb_subspace_prefix: None,
            duration: Duration::from_secs(10),
            warmup: Duration::from_secs(1),
            cooldown: Duration::from_secs(2),
            ops_per_sec: 200,
            item_size_bytes: 1024,
            hot_key_percent: 0,
            hot_key_count: 8,
            delete_percent: 0,
            read_percent: 0,
            sample_every: 25,
            load_workers: 8,
            max_in_flight_convergence_checks: 32,
            batch_mutation_limit: 1_000,
            batch_byte_limit: 512 * 1024,
            bootstrap_item_count: 1,
            seed: 1,
            emulated_clock_skew_ms: 0,
            fault_profile: HarnessFaultProfile::default(),
            report_path: None,
        }
    }
}

pub struct MultiRegionHarnessRunner;

impl MultiRegionHarnessRunner {
    pub async fn run(config: HarnessRunConfig) -> StorageResult<MultiRegionHarnessReport> {
        validate_run_config(&config)?;
        let harness = Arc::new(start_harness(&config).await?);
        if config.scenario == HarnessScenario::Bootstrap {
            return run_bootstrap(harness, config).await;
        }
        harness.create_global_table(&config.table_name).await?;

        warm_up(&harness, &config).await?;
        let (operations, local_write_latencies_ms, read_latencies_ms, touched_keys, mut rx) =
            run_load(&harness, &config).await?;

        tokio::time::sleep(config.cooldown).await;
        harness.run_until_idle(200).await?;

        let (sampled_convergence_latencies_ms, sampled_convergence_failures) =
            collect_convergence_samples(&mut rx).await?;
        let divergent_keys =
            collect_divergent_keys(&harness, &config.table_name, &touched_keys).await?;
        let report = build_report(
            &config,
            HarnessReportInput {
                regions: harness.region_names().to_vec(),
                operations,
                local_write_latencies_ms,
                read_latencies_ms,
                sampled_convergence_latencies_ms,
                sampled_convergence_failures,
                divergent_keys,
            },
        );
        write_report_if_requested(&report, config)?;
        if !report.consistency.final_converged
            || report.consistency.sampled_convergence_failures > 0
        {
            return Err(StorageError::internal(&format!(
                "multi-region {} run detected convergence issues; rerun with seed {} and scenario \
                 parameters from the report",
                report.scenario, report.seed
            )));
        }
        Ok(report)
    }
}

async fn run_bootstrap(
    harness: Arc<SimulationHarness>,
    config: HarnessRunConfig,
) -> StorageResult<MultiRegionHarnessReport> {
    let source_region = harness
        .region_names()
        .first()
        .ok_or_else(|| StorageError::validation("bootstrap harness requires a source region"))?
        .clone();
    let peer_region = harness
        .region_names()
        .get(1)
        .ok_or_else(|| StorageError::validation("bootstrap harness requires a peer region"))?
        .clone();
    harness
        .create_stream_table_in_all_regions(&config.table_name)
        .await?;

    let mut write_latencies_ms = Vec::with_capacity(config.bootstrap_item_count);
    for item_index in 0..config.bootstrap_item_count {
        let write_started = Instant::now();
        let item_id = bootstrap_item_id(item_index);
        harness
            .put_item_value(
                &source_region,
                &config.table_name,
                &item_id,
                &item_id,
                &bootstrap_item_value(item_index),
                config.item_size_bytes,
            )
            .await?;
        write_latencies_ms.push(write_started.elapsed().as_secs_f64() * 1_000.0);
    }

    let bootstrap_started = Instant::now();
    harness
        .create_bootstrap_replica(&source_region, &peer_region, &config.table_name)
        .await?;
    harness.run_until_idle(200).await?;
    let bootstrap_latency_ms = bootstrap_started.elapsed().as_secs_f64() * 1_000.0;
    let divergent_keys = collect_bootstrap_divergent_keys(
        harness.as_ref(),
        &peer_region,
        &config.table_name,
        config.bootstrap_item_count,
    )
    .await?;
    let succeeded = config
        .bootstrap_item_count
        .saturating_sub(divergent_keys.len());
    let report = build_report(
        &config,
        HarnessReportInput {
            regions: harness.region_names().to_vec(),
            operations: HarnessOperationSummary {
                attempted: config.bootstrap_item_count as u64,
                succeeded: succeeded as u64,
                failed: divergent_keys.len() as u64,
                reads: 0,
                writes: config.bootstrap_item_count as u64,
                deletes: 0,
            },
            local_write_latencies_ms: write_latencies_ms,
            read_latencies_ms: Vec::new(),
            sampled_convergence_latencies_ms: vec![bootstrap_latency_ms],
            sampled_convergence_failures: 0,
            divergent_keys,
        },
    );
    write_report_if_requested(&report, config)?;
    if !report.consistency.final_converged {
        return Err(StorageError::internal(&format!(
            "multi-region bootstrap run did not converge source '{}' to peer '{}'",
            source_region, peer_region
        )));
    }
    Ok(report)
}

async fn collect_bootstrap_divergent_keys(
    harness: &SimulationHarness,
    peer_region: &str,
    table_name: &TableName,
    bootstrap_item_count: usize,
) -> StorageResult<Vec<String>> {
    let mut divergent_keys = Vec::new();
    for item_index in 0..bootstrap_item_count {
        let item_id = bootstrap_item_id(item_index);
        let expected = bootstrap_item_value(item_index);
        let peer_value = harness
            .get_item_value(peer_region, table_name, &item_id, &item_id)
            .await?;
        if peer_value.as_deref() != Some(expected.as_str()) {
            divergent_keys.push(format!("{item_id}/{item_id}"));
        }
    }
    Ok(divergent_keys)
}

fn bootstrap_item_id(item_index: usize) -> String {
    format!("bootstrap-{item_index:06}")
}

fn bootstrap_item_value(item_index: usize) -> String {
    format!("bootstrap-value-{item_index:06}")
}

async fn start_harness(config: &HarnessRunConfig) -> StorageResult<SimulationHarness> {
    SimulationHarness::new(SimulationHarnessConfig {
        region_names: region_names(config.regions),
        single_node_sync_regions: Vec::new(),
        storage_backend: config.storage_backend,
        region_storage_backends: config.region_storage_backends.clone(),
        sqlite_database_dir: config.sqlite_database_dir.clone(),
        postgres_dsn_template: config.postgres_dsn_template.clone(),
        postgres_max_pool_size: config.postgres_max_pool_size,
        postgres_tls: config.postgres_tls,
        foundationdb_cluster_file: config.foundationdb_cluster_file.clone(),
        foundationdb_subspace_prefix: config.foundationdb_subspace_prefix.clone(),
        poll_interval: Duration::from_millis(5),
        heartbeat_interval: Duration::from_secs(10),
        heartbeat_jitter: Duration::ZERO,
        batch_mutation_limit: config.batch_mutation_limit,
        batch_byte_limit: config.batch_byte_limit,
        link_latency: latency_duration(
            config.fault_profile.apply_latency_ms,
            config.fault_profile.apply_latency_us,
        ),
        link_latency_jitter: latency_duration(
            config.fault_profile.apply_latency_jitter_ms,
            config.fault_profile.apply_latency_jitter_us,
        ),
        heartbeat_latency: latency_duration(
            config.fault_profile.heartbeat_latency_ms,
            config.fault_profile.heartbeat_latency_us,
        ),
        heartbeat_latency_jitter: latency_duration(
            config.fault_profile.heartbeat_latency_jitter_ms,
            config.fault_profile.heartbeat_latency_jitter_us,
        ),
        drop_probability_per_10k: config.fault_profile.drop_probability_per_10k,
        duplicate_probability_per_10k: config.fault_profile.duplicate_probability_per_10k,
        queue_probability_per_10k: config.fault_profile.queue_probability_per_10k,
        emulated_clock_skew_ms: config.emulated_clock_skew_ms,
        seed: config.seed,
    })
    .await
}

async fn warm_up(harness: &SimulationHarness, config: &HarnessRunConfig) -> StorageResult<()> {
    let warmup_deadline = Instant::now() + config.warmup;
    let mut warmup_counter = 0_u64;
    while Instant::now() < warmup_deadline {
        execute_put(
            harness,
            &config.table_name,
            region_for_operation(harness.region_names(), warmup_counter),
            "warmup",
            warmup_counter,
            config.item_size_bytes,
        )
        .await?;
        warmup_counter = warmup_counter.saturating_add(1);
    }
    Ok(())
}

async fn run_load(
    harness: &Arc<SimulationHarness>,
    config: &HarnessRunConfig,
) -> StorageResult<(
    HarnessOperationSummary,
    Vec<f64>,
    Vec<f64>,
    std::collections::HashSet<String>,
    tokio::sync::mpsc::UnboundedReceiver<StorageResult<Option<f64>>>,
)> {
    let stop = Arc::new(AtomicBool::new(false));
    let replication_loop =
        super::load::spawn_replication_loop(Arc::clone(harness), Arc::clone(&stop));
    let start = Instant::now();
    let end = start + config.duration;
    let next_operation = Arc::new(AtomicU64::new(0));
    let (convergence_sampler, convergence_rx) =
        ConvergenceSampler::new(config.max_in_flight_convergence_checks);
    let mut worker_set = JoinSet::new();
    for _ in 0..config.load_workers {
        worker_set.spawn(run_load_worker(
            Arc::clone(harness),
            config.clone(),
            start,
            end,
            Arc::clone(&next_operation),
            convergence_sampler.clone(),
        ));
    }

    let mut operations = HarnessOperationSummary {
        attempted: 0,
        succeeded: 0,
        failed: 0,
        reads: 0,
        writes: 0,
        deletes: 0,
    };
    let mut local_write_latencies_ms = Vec::new();
    let mut read_latencies_ms = Vec::new();
    let mut touched_keys = std::collections::HashSet::new();
    while let Some(worker_result) = worker_set.join_next().await {
        let outcome = worker_result
            .map_err(|error| StorageError::internal(&format!("join load worker: {error}")))??;
        merge_operation_summary(&mut operations, &outcome.operations);
        local_write_latencies_ms.extend(outcome.local_write_latencies_ms);
        read_latencies_ms.extend(outcome.read_latencies_ms);
        touched_keys.extend(outcome.touched_keys);
    }
    drop(convergence_sampler);
    stop.store(true, Ordering::Relaxed);
    let _ = replication_loop.await;
    Ok((
        operations,
        local_write_latencies_ms,
        read_latencies_ms,
        touched_keys,
        convergence_rx,
    ))
}

async fn collect_convergence_samples(
    convergence_rx: &mut tokio::sync::mpsc::UnboundedReceiver<StorageResult<Option<f64>>>,
) -> StorageResult<(Vec<f64>, u64)> {
    let mut sampled_convergence_latencies_ms = Vec::new();
    let mut sampled_convergence_failures = 0_u64;
    while let Some(convergence_result) = convergence_rx.recv().await {
        match convergence_result? {
            Some(latency_ms) => sampled_convergence_latencies_ms.push(latency_ms),
            None => {
                sampled_convergence_failures = sampled_convergence_failures.saturating_add(1);
            }
        }
    }
    Ok((
        sampled_convergence_latencies_ms,
        sampled_convergence_failures,
    ))
}
