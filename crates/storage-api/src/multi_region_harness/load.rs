use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use storage_types::{StorageError, StorageResult, TableName};
use tokio::{task::JoinHandle, time::Instant};

use super::{
    convergence::{
        ConvergenceSampler, wait_for_commit_watermark_convergence, wait_for_value_convergence,
    },
    report::HarnessOperationSummary,
    runner::{HarnessRunConfig, HarnessScenario},
    simulation::SimulationHarness,
};

#[derive(Debug, Default)]
pub(super) struct WorkerOutcome {
    pub(super) operations: HarnessOperationSummary,
    pub(super) local_write_latencies_ms: Vec<f64>,
    pub(super) read_latencies_ms: Vec<f64>,
    pub(super) touched_keys: HashSet<String>,
}

#[derive(Debug, Clone, Copy)]
enum HarnessOperationKind {
    Put,
    Read,
    Delete,
}

#[derive(Debug, Clone)]
struct SyntheticKey {
    pk: String,
    sk: String,
}

pub(super) async fn run_load_worker(
    harness: Arc<SimulationHarness>,
    config: HarnessRunConfig,
    start: Instant,
    end: Instant,
    next_operation: Arc<AtomicU64>,
    convergence_sampler: ConvergenceSampler,
) -> StorageResult<WorkerOutcome> {
    let mut outcome = WorkerOutcome::default();
    loop {
        let operation_counter = next_operation.fetch_add(1, Ordering::Relaxed);
        let target_at = scheduled_at(start, operation_counter, config.ops_per_sec);
        if target_at >= end {
            break;
        }
        tokio::time::sleep_until(target_at).await;

        if config.scenario == HarnessScenario::Chaos {
            maybe_toggle_chaos_faults(&harness, config.seed, operation_counter);
        }

        let region = region_for_operation(harness.region_names(), operation_counter);
        let key = key_for_operation(
            config.seed,
            config.hot_key_percent,
            config.hot_key_count.max(1),
            operation_counter,
        );
        outcome
            .touched_keys
            .insert(format!("{}/{}", key.pk, key.sk));
        outcome.operations.attempted = outcome.operations.attempted.saturating_add(1);

        match operation_kind_for(
            config.seed,
            config.read_percent,
            config.delete_percent,
            operation_counter,
        ) {
            HarnessOperationKind::Read => {
                outcome.operations.reads = outcome.operations.reads.saturating_add(1);
                let read_started = Instant::now();
                let _ = harness
                    .get_item_value(region, &config.table_name, &key.pk, &key.sk)
                    .await?;
                outcome.operations.succeeded = outcome.operations.succeeded.saturating_add(1);
                outcome
                    .read_latencies_ms
                    .push(read_started.elapsed().as_secs_f64() * 1_000.0);
            }
            HarnessOperationKind::Delete => {
                outcome.operations.deletes = outcome.operations.deletes.saturating_add(1);
                let write_started = Instant::now();
                harness
                    .delete_item(region, &config.table_name, &key.pk, &key.sk)
                    .await?;
                outcome.operations.succeeded = outcome.operations.succeeded.saturating_add(1);
                outcome
                    .local_write_latencies_ms
                    .push(write_started.elapsed().as_secs_f64() * 1_000.0);
            }
            HarnessOperationKind::Put => {
                outcome.operations.writes = outcome.operations.writes.saturating_add(1);
                let value = synthetic_value(region, operation_counter, &key.pk);
                let write_started = Instant::now();
                harness
                    .put_item_value(
                        region,
                        &config.table_name,
                        &key.pk,
                        &key.sk,
                        &value,
                        config.item_size_bytes,
                    )
                    .await?;
                outcome.operations.succeeded = outcome.operations.succeeded.saturating_add(1);
                outcome
                    .local_write_latencies_ms
                    .push(write_started.elapsed().as_secs_f64() * 1_000.0);

                if should_track_convergence(
                    config.scenario,
                    operation_counter,
                    config.sample_every,
                    config.hot_key_percent,
                ) {
                    match config.scenario {
                        HarnessScenario::Perf => {
                            let source_commit_ts = harness
                                .get_item_origin_commit_ts(
                                    region,
                                    &config.table_name,
                                    &key.pk,
                                    &key.sk,
                                )
                                .await?
                                .ok_or_else(|| {
                                    StorageError::internal(
                                        "sampled perf write missing replication metadata",
                                    )
                                })?;
                            convergence_sampler.spawn(wait_for_commit_watermark_convergence(
                                Arc::clone(&harness),
                                region.to_string(),
                                source_commit_ts,
                            ));
                        }
                        HarnessScenario::Soak
                        | HarnessScenario::Chaos
                        | HarnessScenario::Bootstrap => {
                            convergence_sampler.spawn(wait_for_value_convergence(
                                Arc::clone(&harness),
                                config.table_name.clone(),
                                key.pk.clone(),
                                key.sk.clone(),
                                value,
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(outcome)
}

pub(super) fn spawn_replication_loop(
    harness: Arc<SimulationHarness>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while !stop.load(Ordering::Relaxed) {
            let _ = harness.step_all_regions(true).await;
            let _ = harness.flush_all_queued_applies(false).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
}

pub(super) async fn execute_put(
    harness: &SimulationHarness,
    table_name: &TableName,
    region: &str,
    value_prefix: &str,
    index: u64,
    item_size_bytes: usize,
) -> StorageResult<()> {
    let key = format!("warmup-{index}");
    harness
        .put_item_value(
            region,
            table_name,
            &key,
            &key,
            &format!("{value_prefix}-{index}"),
            item_size_bytes,
        )
        .await
}

pub(super) fn merge_operation_summary(
    target: &mut HarnessOperationSummary,
    source: &HarnessOperationSummary,
) {
    target.attempted = target.attempted.saturating_add(source.attempted);
    target.succeeded = target.succeeded.saturating_add(source.succeeded);
    target.failed = target.failed.saturating_add(source.failed);
    target.reads = target.reads.saturating_add(source.reads);
    target.writes = target.writes.saturating_add(source.writes);
    target.deletes = target.deletes.saturating_add(source.deletes);
}

pub(super) fn region_for_operation(regions: &[String], operation_counter: u64) -> &str {
    let index = operation_counter as usize % regions.len().max(1);
    regions[index].as_str()
}

fn scheduled_at(start: Instant, operation_counter: u64, ops_per_sec: u64) -> Instant {
    start
        + Duration::from_nanos(operation_counter.saturating_mul(1_000_000_000) / ops_per_sec.max(1))
}

fn key_for_operation(
    seed: u64,
    hot_key_percent: u8,
    hot_key_count: usize,
    operation_counter: u64,
) -> SyntheticKey {
    let hot_roll = deterministic_roll(seed, operation_counter, 1) % 100;
    if hot_roll < u64::from(hot_key_percent) {
        let hot_index = (deterministic_roll(seed, operation_counter, 2) as usize) % hot_key_count;
        return SyntheticKey {
            pk: format!("hot-pk-{hot_index}"),
            sk: format!("hot-sk-{hot_index}"),
        };
    }
    SyntheticKey {
        pk: format!("pk-{operation_counter}"),
        sk: format!("sk-{operation_counter}"),
    }
}

fn operation_kind_for(
    seed: u64,
    read_percent: u8,
    delete_percent: u8,
    operation_counter: u64,
) -> HarnessOperationKind {
    let roll = deterministic_roll(seed, operation_counter, 3) % 100;
    if roll < u64::from(read_percent) {
        return HarnessOperationKind::Read;
    }
    if roll < u64::from(read_percent) + u64::from(delete_percent) {
        return HarnessOperationKind::Delete;
    }
    HarnessOperationKind::Put
}

fn synthetic_value(region: &str, operation_counter: u64, pk: &str) -> String {
    format!("{region}:{operation_counter}:{pk}:{operation_counter}")
}

fn maybe_toggle_chaos_faults(harness: &SimulationHarness, seed: u64, operation_counter: u64) {
    if harness.region_names().len() < 2 {
        return;
    }
    if operation_counter.is_multiple_of(50) {
        let regions = harness.region_names();
        let source = regions
            [(deterministic_roll(seed, operation_counter, 10) as usize) % regions.len()]
        .as_str();
        let destination = regions
            [(deterministic_roll(seed, operation_counter, 11) as usize) % regions.len()]
        .as_str();
        if source != destination {
            harness.block_link(source, destination, true);
        }
    }
    if operation_counter.is_multiple_of(75) {
        for source in harness.region_names() {
            for destination in harness.region_names() {
                if source != destination {
                    harness.block_link(source, destination, false);
                }
            }
        }
    }
    if operation_counter.is_multiple_of(20) {
        let regions = harness.region_names();
        let source = regions
            [(deterministic_roll(seed, operation_counter, 12) as usize) % regions.len()]
        .as_str();
        let destination = regions
            [(deterministic_roll(seed, operation_counter, 13) as usize) % regions.len()]
        .as_str();
        if source != destination {
            harness.duplicate_next_apply(source, destination);
        }
    }
    if operation_counter.is_multiple_of(30) {
        let regions = harness.region_names();
        let source = regions
            [(deterministic_roll(seed, operation_counter, 14) as usize) % regions.len()]
        .as_str();
        let destination = regions
            [(deterministic_roll(seed, operation_counter, 15) as usize) % regions.len()]
        .as_str();
        if source != destination {
            harness.drop_next_apply(source, destination);
        }
    }
}

fn should_track_convergence(
    scenario: HarnessScenario,
    operation_counter: u64,
    sample_every: usize,
    hot_key_percent: u8,
) -> bool {
    scenario == HarnessScenario::Perf
        && hot_key_percent == 0
        && sample_every > 0
        && operation_counter.is_multiple_of(sample_every as u64)
}

fn mix64(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn deterministic_roll(seed: u64, operation_counter: u64, salt: u64) -> u64 {
    mix64(
        seed ^ operation_counter.wrapping_mul(0x9e3779b97f4a7c15)
            ^ salt.wrapping_mul(0xbf58476d1ce4e5b9),
    )
}
