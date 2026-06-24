//! FoundationDB partition-family reconcile loop.
//!
//! Production blocker:
//! multi-node load and soak validation on a real FoundationDB cluster is still
//! required before enabling this path in production.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bg_jobs::{BackgroundJob, JobConfig, errors::JobError};
use storage_common::PARTITION_RECONCILE_JOB;
use storage_types::{StorageError, StorageResult, TimestampMillis};
use tracing::{debug, info, instrument};

use crate::{
    SortedKvDbStorageProvider,
    constants::{
        PARTITION_CONTROLLER_EWMA_ALPHA, PARTITION_CONTROLLER_HIGH_STREAK_TARGET,
        PARTITION_CONTROLLER_INTEGRAL_MAX, PARTITION_CONTROLLER_INTEGRAL_MIN,
        PARTITION_CONTROLLER_LOW_STREAK_TARGET, PARTITION_CONTROLLER_LOW_THRESHOLD,
        PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD, PARTITION_CONTROLLER_SPLIT_THRESHOLD,
        PARTITION_FAMILY_HOT_FAMILIES_METRIC, PARTITION_FAMILY_MANAGED_FAMILIES_METRIC,
        PARTITION_FAMILY_OPEN_PARTITIONS_METRIC, PARTITION_FAMILY_PRESSURE_METRIC,
        PARTITION_FAMILY_TRANSITION_PARTITIONS_METRIC, PARTITION_LOAD_SAMPLE_RETENTION_WINDOWS,
        PARTITION_LOAD_SAMPLE_WINDOW_SECONDS, PARTITION_LOAD_SAMPLES_FLUSHED_TOTAL_METRIC,
        PARTITION_RECONCILE_ACTIONS_TOTAL_METRIC, PARTITION_RECONCILE_RUNS_TOTAL_METRIC,
        PARTITION_RECONCILE_RUNTIME_MS_METRIC,
    },
    keyspace::compact::{self, QueueStorageId, U48},
    newtypes::MessageVisibilityKey,
    partition_family::{
        PartitionFamilyConfig, PartitionFamilyKind, PartitionFamilyKvStore, PartitionInfo,
        PartitionLoadSample, PartitionLoadSampleRecord, PartitionState, PiControllerState,
        ResolvedPartitionFamily, decode_hex_component, merge_partition_load, next_partition_id,
        next_placement_slot, open_partition_count,
        parse_partition_family_component_from_config_value, parse_partition_load_sample,
        partition_family_kind_prefix, partition_load_sample_bytes, partition_load_sample_key,
        partition_load_sample_prefix, partition_sample_retention_cutoff_ms,
        partition_sample_window_start_ms, queue_ready_prefix_with_slot, queue_state_key_with_slot,
        routing_key_bucket_count,
    },
    queue::constants::QUEUE_PREWARM_MESSAGE_ID,
};

pub struct PartitionReconcileJob<S: PartitionFamilyKvStore + 'static> {
    provider: Arc<SortedKvDbStorageProvider<S>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueueReconcileAction {
    AddPartition,
    BeginDrain { partition_id: u16 },
    Retire { partition_id: u16 },
}

struct PartitionSampleLoad {
    samples: HashMap<u16, PartitionLoadSample>,
    deleted_stale: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct FamilyStateCounts {
    open: u16,
    write_closed: u16,
    draining: u16,
    retired: u16,
}

#[derive(Clone, Copy, Debug, Default)]
struct FamilyReconcileOutcome {
    changed: bool,
    pressure: f64,
    hot: bool,
    states: FamilyStateCounts,
}

impl<S: PartitionFamilyKvStore + 'static> PartitionReconcileJob<S> {
    pub fn new(provider: Arc<SortedKvDbStorageProvider<S>>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<S: PartitionFamilyKvStore + 'static> BackgroundJob for PartitionReconcileJob<S> {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let work_done = self.provider.run_partition_reconcile().await?;
        Ok(work_done)
    }
}

impl<S: PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    async fn queue_storage_id_for_url(
        &self,
        queue_url: &str,
    ) -> StorageResult<Option<QueueStorageId>> {
        let Some(bytes) = self
            .kv_store
            .get(&compact::queue_url_lookup_key(queue_url), true)
            .await?
        else {
            return Ok(None);
        };
        if bytes.len() != 6 {
            return Err(StorageError::internal(&format!(
                "invalid queue storage id width: expected 6 bytes, got {}",
                bytes.len()
            )));
        }
        let mut padded = [0u8; 8];
        padded[2..].copy_from_slice(&bytes);
        U48::new(u64::from_be_bytes(padded))
            .map(QueueStorageId::from)
            .map(Some)
            .map_err(|error| StorageError::internal(&format!("invalid queue storage id: {error}")))
    }

    pub(crate) async fn start_partition_reconcile_task(&self) -> StorageResult<()> {
        if !self.database_jobs_enabled || !self.kv_store.supports_partition_families() {
            return Ok(());
        }

        if self
            .job_manager
            .is_job_running(PARTITION_RECONCILE_JOB)
            .await
        {
            return Ok(());
        }

        let job = PartitionReconcileJob::new(Arc::new(self.clone()));
        let config = JobConfig {
            start_immediately: true,
            sleep_duration: Duration::from_millis(
                self.database_job_intervals
                    .partition_family_reconcile_interval_ms
                    .0,
            ),
            jitter_percent: 10,
        };

        match self
            .job_manager
            .register_job(PARTITION_RECONCILE_JOB, job, config)
            .await
        {
            Ok(_) | Err(JobError::JobAlreadyRunning) => Ok(()),
            Err(error) => Err(StorageError::internal(&format!(
                "partition reconcile job registration failed: {error}"
            ))),
        }
    }

    async fn partition_family_components(
        &self,
        family_kind: PartitionFamilyKind,
    ) -> StorageResult<Vec<String>> {
        let prefix = partition_family_kind_prefix(family_kind);
        let entries = self.kv_store.get_prefix(&prefix, true, None, true).await?;
        let mut components = BTreeSet::new();
        for (_key, value) in entries.items {
            if let Some(component) = parse_partition_family_component_from_config_value(&value)? {
                components.insert(component);
            }
        }
        Ok(components.into_iter().collect())
    }

    async fn flush_runtime_partition_load_samples(
        &self,
        window_start_ms: i64,
    ) -> StorageResult<bool> {
        let mut aggregated: HashMap<(PartitionFamilyKind, String, u16), PartitionLoadSample> =
            HashMap::new();
        let mut samples = self.runtime_partition_load_tracker.drain();
        samples.extend(self.kv_store.drain_runtime_partition_load_samples().await?);
        if samples.is_empty() {
            return Ok(false);
        }

        for sample in samples {
            let entry = aggregated
                .entry((
                    sample.family_kind,
                    sample.family_component.clone(),
                    sample.partition_id,
                ))
                .or_default();
            merge_partition_load(entry, &sample.sample);
        }

        let flushed = u64::try_from(aggregated.len()).unwrap_or(u64::MAX);
        for ((family_kind, family_component, partition_id), sample) in aggregated {
            let publisher_id = self.partition_sample_publisher_id.as_ref().clone();
            let sample_key = partition_load_sample_key(
                family_kind,
                &family_component,
                partition_id,
                window_start_ms,
                &publisher_id,
            );
            let mut record = match self.kv_store.get(&sample_key, true).await? {
                Some(existing) => parse_partition_load_sample(&existing)?,
                None => PartitionLoadSampleRecord {
                    partition_id,
                    window_start_ms,
                    publisher_id: publisher_id.clone(),
                    sample: PartitionLoadSample::default(),
                },
            };
            record.partition_id = partition_id;
            record.window_start_ms = window_start_ms;
            record.publisher_id = publisher_id;
            merge_partition_load(&mut record.sample, &sample);
            self.kv_store
                .put(&sample_key, &partition_load_sample_bytes(&record)?, None)
                .await?;
        }

        metrics_facade::counter!(PARTITION_LOAD_SAMPLES_FLUSHED_TOTAL_METRIC).increment(flushed);
        debug!(
            sample_window_start_ms = window_start_ms,
            samples_flushed = flushed,
            "flushed partition-family runtime load samples"
        );

        Ok(true)
    }

    async fn load_recent_partition_samples(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
        cutoff_ms: i64,
    ) -> StorageResult<PartitionSampleLoad> {
        let prefix = partition_load_sample_prefix(family_kind, family_component);
        let entries = self.kv_store.get_prefix(&prefix, true, None, true).await?;
        let mut samples = HashMap::new();
        let mut stale_keys = Vec::new();

        for (key, value) in entries.items {
            let record = parse_partition_load_sample(&value)?;
            if record.window_start_ms < cutoff_ms {
                stale_keys.push(key);
                continue;
            }

            let entry = samples.entry(record.partition_id).or_default();
            merge_partition_load(entry, &record.sample);
        }

        let deleted_stale = !stale_keys.is_empty();
        for key in stale_keys {
            self.kv_store.delete(&key).await?;
        }

        Ok(PartitionSampleLoad {
            samples,
            deleted_stale,
        })
    }

    async fn queue_partition_snapshot(
        &self,
        queue_id: QueueStorageId,
        partition: &PartitionInfo,
        now_ms: i64,
    ) -> StorageResult<(PartitionLoadSample, bool)> {
        let ready_prefix = queue_ready_prefix_with_slot(
            queue_id,
            partition.placement_slot,
            partition.partition_id,
        );
        let ready_entries = self
            .kv_store
            .get_prefix(&ready_prefix, true, Some(1), true)
            .await?;
        let state_entries = self
            .kv_store
            .get_prefix(
                &queue_state_key_with_slot(
                    queue_id,
                    partition.placement_slot,
                    partition.partition_id,
                    "",
                ),
                true,
                Some(2),
                true,
            )
            .await?;
        let prewarm_state_key = queue_state_key_with_slot(
            queue_id,
            partition.placement_slot,
            partition.partition_id,
            QUEUE_PREWARM_MESSAGE_ID,
        );
        let has_queue_state = state_entries
            .items
            .iter()
            .any(|(key, _)| key.as_ref() != prewarm_state_key.as_slice());
        let mut sample = PartitionLoadSample::default();
        if let Some((key, _)) = ready_entries.items.first() {
            let visibility_key = ready_visibility_key(&ready_prefix, key)?;
            let timestamp_ms = visibility_key
                .get_timestamp()
                .map_err(|error| {
                    StorageError::internal(&format!("decode queue visibility timestamp: {error}"))
                })?
                .timestamp_millis();
            if timestamp_ms <= now_ms {
                sample.oldest_visible_age_ms =
                    u64::try_from(now_ms.saturating_sub(timestamp_ms)).unwrap_or(u64::MAX);
                sample.visible_count = 1;
            }
        }
        if sample.visible_count == 0 && has_queue_state {
            sample.invisible_count = 1;
        }

        let is_empty = ready_entries.items.is_empty() && !has_queue_state;

        Ok((sample, is_empty))
    }

    async fn reconcile_ordered_log_family(
        &self,
        family_component: &str,
        now_ms: i64,
        cutoff_ms: i64,
    ) -> StorageResult<FamilyReconcileOutcome> {
        let Some(mut family) = self
            .load_partition_family_state(PartitionFamilyKind::OrderedLog, family_component)
            .await?
        else {
            return Ok(FamilyReconcileOutcome::default());
        };

        let load = self
            .load_recent_partition_samples(
                PartitionFamilyKind::OrderedLog,
                family_component,
                cutoff_ms,
            )
            .await?;
        let mut changed = load.deleted_stale;
        let pressure =
            hottest_ordered_log_pressure(&family.partitions, &load.samples, &family.config);
        let open_partitions = open_partition_count(&family.partitions);
        changed |= step_pi_controller(&mut family.config, pressure);
        let mut split_applied = false;

        if let Some(partition_id) =
            ordered_log_autosplit_candidate(&family, &load.samples, pressure, now_ms)
        {
            let cache_key = crate::sorted_kv::PartitionFamilyCacheKey::new(
                PartitionFamilyKind::OrderedLog,
                family_component,
            );
            let split_changed = self
                .kv_store
                .split_partitioned_ordered_log_family(family_component, partition_id, now_ms)
                .await?;
            self.invalidate_partition_family_cache(&cache_key);
            changed |= split_changed;
            split_applied = split_changed;
            if split_changed {
                metrics_facade::counter!(PARTITION_RECONCILE_ACTIONS_TOTAL_METRIC,
                    "family_kind" => "ordered_log",
                    "action" => "split"
                )
                .increment(1);
                info!(
                    family_kind = "ordered_log",
                    family_component,
                    partition_id,
                    pressure,
                    open_partitions,
                    high_streak = family.config.controller.high_streak,
                    ewma_pressure = family.config.controller.ewma_pressure,
                    "split ordered-log partition family"
                );
            }
        }

        let family = if split_applied {
            self.load_partition_family_state(PartitionFamilyKind::OrderedLog, family_component)
                .await?
                .unwrap_or(family)
        } else {
            if changed {
                self.save_partition_family_state(
                    PartitionFamilyKind::OrderedLog,
                    family_component,
                    &family,
                )
                .await?;
            }
            family
        };

        Ok(FamilyReconcileOutcome {
            changed,
            pressure,
            hot: pressure >= PARTITION_CONTROLLER_SPLIT_THRESHOLD,
            states: family_state_counts(&family.partitions),
        })
    }

    /// Runs one bounded ordered-log reconcile pass for a single partition
    /// family.
    ///
    /// This is an internal probe/simulation helper. Operator job paths should
    /// use `PARTITION_RECONCILE_JOB`, which preserves catch-up semantics
    /// across all families.
    pub async fn run_ordered_log_partition_reconcile_once(
        &self,
        family_component: &str,
    ) -> StorageResult<bool> {
        if !self.kv_store.supports_partition_families() {
            return Ok(false);
        }

        let now_ms = TimestampMillis::now().timestamp_millis();
        let window_start_ms =
            partition_sample_window_start_ms(now_ms, PARTITION_LOAD_SAMPLE_WINDOW_SECONDS);
        let cutoff_ms = partition_sample_retention_cutoff_ms(
            now_ms,
            PARTITION_LOAD_SAMPLE_WINDOW_SECONDS,
            PARTITION_LOAD_SAMPLE_RETENTION_WINDOWS,
        );

        let changed = self
            .flush_runtime_partition_load_samples(window_start_ms)
            .await?;
        let outcome = self
            .reconcile_ordered_log_family(family_component, now_ms, cutoff_ms)
            .await?;
        Ok(changed || outcome.changed)
    }

    async fn reconcile_queue_family(
        &self,
        family_component: &str,
        now_ms: i64,
        cutoff_ms: i64,
    ) -> StorageResult<FamilyReconcileOutcome> {
        let Some(mut family) = self
            .load_partition_family_state(PartitionFamilyKind::StandardQueue, family_component)
            .await?
        else {
            return Ok(FamilyReconcileOutcome::default());
        };
        let queue_url =
            String::from_utf8(decode_hex_component(family_component)?).map_err(|error| {
                StorageError::internal(&format!("decode queue family component: {error}"))
            })?;
        let Some(queue_id) = self.queue_storage_id_for_url(&queue_url).await? else {
            return Ok(FamilyReconcileOutcome::default());
        };

        let load = self
            .load_recent_partition_samples(
                PartitionFamilyKind::StandardQueue,
                family_component,
                cutoff_ms,
            )
            .await?;
        let mut samples = load.samples;
        let mut empty_partitions = HashMap::new();
        for partition in &family.partitions {
            if !partition.is_readable() {
                continue;
            }
            let (snapshot, is_empty) = self
                .queue_partition_snapshot(queue_id, partition, now_ms)
                .await?;
            let entry = samples.entry(partition.partition_id).or_default();
            entry.oldest_visible_age_ms = entry
                .oldest_visible_age_ms
                .max(snapshot.oldest_visible_age_ms);
            entry.visible_count = snapshot.visible_count;
            entry.invisible_count = snapshot.invisible_count;
            empty_partitions.insert(partition.partition_id, is_empty);
        }

        let mut changed = load.deleted_stale;
        let pressure = hottest_queue_pressure(&family.partitions, &samples, &family.config);
        let open_partitions = open_partition_count(&family.partitions);
        changed |= step_pi_controller(&mut family.config, pressure);

        let action = plan_queue_action(&family, &samples, &empty_partitions, now_ms);
        if let Some(action) = action {
            let action_name = queue_action_name(action);
            let action_partition_id = match action {
                QueueReconcileAction::AddPartition => None,
                QueueReconcileAction::BeginDrain { partition_id }
                | QueueReconcileAction::Retire { partition_id } => Some(partition_id),
            };
            let prior_high_streak = family.config.controller.high_streak;
            let prior_low_streak = family.config.controller.low_streak;
            let prior_ewma_pressure = family.config.controller.ewma_pressure;
            let action_changed = apply_queue_action(&mut family, action, now_ms);
            changed |= action_changed;
            if action_changed {
                metrics_facade::counter!(PARTITION_RECONCILE_ACTIONS_TOTAL_METRIC,
                    "family_kind" => "standard_queue",
                    "action" => action_name
                )
                .increment(1);
                info!(
                    family_kind = "standard_queue",
                    family_component,
                    action = action_name,
                    partition_id = action_partition_id,
                    pressure,
                    open_partitions,
                    high_streak = prior_high_streak,
                    low_streak = prior_low_streak,
                    ewma_pressure = prior_ewma_pressure,
                    "applied queue partition-family reconcile action"
                );
            }
        }

        if changed {
            self.save_partition_family_state(
                PartitionFamilyKind::StandardQueue,
                family_component,
                &family,
            )
            .await?;
        }

        Ok(FamilyReconcileOutcome {
            changed,
            pressure,
            hot: pressure >= PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD,
            states: family_state_counts(&family.partitions),
        })
    }

    #[instrument(skip_all, fields(feature = "storage"))]
    pub(crate) async fn run_partition_reconcile(&self) -> StorageResult<bool> {
        if !self.kv_store.supports_partition_families() {
            return Ok(false);
        }

        let run_started = Instant::now();
        let now_ms = TimestampMillis::now().timestamp_millis();
        let window_start_ms =
            partition_sample_window_start_ms(now_ms, PARTITION_LOAD_SAMPLE_WINDOW_SECONDS);
        let cutoff_ms = partition_sample_retention_cutoff_ms(
            now_ms,
            PARTITION_LOAD_SAMPLE_WINDOW_SECONDS,
            PARTITION_LOAD_SAMPLE_RETENTION_WINDOWS,
        );

        let mut ordered_log_families = 0u64;
        let mut queue_families = 0u64;
        let mut hot_ordered_log_families = 0u64;
        let mut hot_queue_families = 0u64;
        let mut max_ordered_log_open = 0u16;
        let mut max_queue_open = 0u16;
        let mut max_ordered_log_pressure = 0.0f64;
        let mut max_queue_pressure = 0.0f64;
        let mut ordered_log_write_closed = 0u64;
        let mut queue_write_closed = 0u64;
        let mut queue_draining = 0u64;
        let mut queue_retired = 0u64;
        let mut changed = self
            .flush_runtime_partition_load_samples(window_start_ms)
            .await?;
        for family_component in self
            .partition_family_components(PartitionFamilyKind::OrderedLog)
            .await?
        {
            ordered_log_families = ordered_log_families.saturating_add(1);
            let outcome = self
                .reconcile_ordered_log_family(&family_component, now_ms, cutoff_ms)
                .await?;
            changed |= outcome.changed;
            if outcome.hot {
                hot_ordered_log_families = hot_ordered_log_families.saturating_add(1);
            }
            max_ordered_log_open = max_ordered_log_open.max(outcome.states.open);
            max_ordered_log_pressure = max_ordered_log_pressure.max(outcome.pressure);
            ordered_log_write_closed =
                ordered_log_write_closed.saturating_add(u64::from(outcome.states.write_closed));
        }
        for family_component in self
            .partition_family_components(PartitionFamilyKind::StandardQueue)
            .await?
        {
            queue_families = queue_families.saturating_add(1);
            let outcome = self
                .reconcile_queue_family(&family_component, now_ms, cutoff_ms)
                .await?;
            changed |= outcome.changed;
            if outcome.hot {
                hot_queue_families = hot_queue_families.saturating_add(1);
            }
            max_queue_open = max_queue_open.max(outcome.states.open);
            max_queue_pressure = max_queue_pressure.max(outcome.pressure);
            queue_write_closed =
                queue_write_closed.saturating_add(u64::from(outcome.states.write_closed));
            queue_draining = queue_draining.saturating_add(u64::from(outcome.states.draining));
            queue_retired = queue_retired.saturating_add(u64::from(outcome.states.retired));
        }

        let runtime_ms = run_started.elapsed().as_secs_f64() * 1000.0;
        metrics_facade::counter!(PARTITION_RECONCILE_RUNS_TOTAL_METRIC,
            "result" => if changed { "changed" } else { "noop" }
        )
        .increment(1);
        metrics_facade::histogram!(PARTITION_RECONCILE_RUNTIME_MS_METRIC).record(runtime_ms);
        metrics_facade::gauge!(PARTITION_FAMILY_OPEN_PARTITIONS_METRIC, "family_kind" => "ordered_log")
            .set(f64::from(max_ordered_log_open));
        metrics_facade::gauge!(PARTITION_FAMILY_OPEN_PARTITIONS_METRIC, "family_kind" => "standard_queue")
            .set(f64::from(max_queue_open));
        metrics_facade::gauge!(PARTITION_FAMILY_MANAGED_FAMILIES_METRIC, "family_kind" => "ordered_log")
            .set(ordered_log_families as f64);
        metrics_facade::gauge!(PARTITION_FAMILY_MANAGED_FAMILIES_METRIC, "family_kind" => "standard_queue")
            .set(queue_families as f64);
        metrics_facade::gauge!(PARTITION_FAMILY_HOT_FAMILIES_METRIC, "family_kind" => "ordered_log")
            .set(hot_ordered_log_families as f64);
        metrics_facade::gauge!(PARTITION_FAMILY_HOT_FAMILIES_METRIC, "family_kind" => "standard_queue")
            .set(hot_queue_families as f64);
        metrics_facade::gauge!(PARTITION_FAMILY_PRESSURE_METRIC, "family_kind" => "ordered_log")
            .set(max_ordered_log_pressure);
        metrics_facade::gauge!(PARTITION_FAMILY_PRESSURE_METRIC, "family_kind" => "standard_queue")
            .set(max_queue_pressure);
        metrics_facade::gauge!(
            PARTITION_FAMILY_TRANSITION_PARTITIONS_METRIC,
            "family_kind" => "ordered_log",
            "state" => "write_closed"
        )
        .set(ordered_log_write_closed as f64);
        metrics_facade::gauge!(
            PARTITION_FAMILY_TRANSITION_PARTITIONS_METRIC,
            "family_kind" => "ordered_log",
            "state" => "draining"
        )
        .set(0.0);
        metrics_facade::gauge!(
            PARTITION_FAMILY_TRANSITION_PARTITIONS_METRIC,
            "family_kind" => "ordered_log",
            "state" => "retired"
        )
        .set(0.0);
        metrics_facade::gauge!(
            PARTITION_FAMILY_TRANSITION_PARTITIONS_METRIC,
            "family_kind" => "standard_queue",
            "state" => "write_closed"
        )
        .set(queue_write_closed as f64);
        metrics_facade::gauge!(
            PARTITION_FAMILY_TRANSITION_PARTITIONS_METRIC,
            "family_kind" => "standard_queue",
            "state" => "draining"
        )
        .set(queue_draining as f64);
        metrics_facade::gauge!(
            PARTITION_FAMILY_TRANSITION_PARTITIONS_METRIC,
            "family_kind" => "standard_queue",
            "state" => "retired"
        )
        .set(queue_retired as f64);
        debug!(
            changed,
            ordered_log_families,
            queue_families,
            hot_ordered_log_families,
            hot_queue_families,
            max_ordered_log_open,
            max_queue_open,
            max_ordered_log_pressure,
            max_queue_pressure,
            ordered_log_write_closed,
            queue_write_closed,
            queue_draining,
            queue_retired,
            runtime_ms,
            "completed partition-family reconcile run"
        );

        Ok(changed)
    }
}

pub(crate) fn controller_pressure(state: &PiControllerState) -> f64 {
    state.ewma_pressure + (0.2 * state.integral)
}

pub(crate) fn step_pi_controller(config: &mut PartitionFamilyConfig, pressure: f64) -> bool {
    let previous = config.controller.clone();
    let ewma_pressure = if previous.ewma_pressure <= 0.0 {
        pressure
    } else {
        (previous.ewma_pressure * (1.0 - PARTITION_CONTROLLER_EWMA_ALPHA))
            + (pressure * PARTITION_CONTROLLER_EWMA_ALPHA)
    };
    let error = ewma_pressure - 1.0;
    let integral = (previous.integral + error).clamp(
        PARTITION_CONTROLLER_INTEGRAL_MIN,
        PARTITION_CONTROLLER_INTEGRAL_MAX,
    );
    let (high_streak, low_streak) = if pressure >= 1.0 {
        (previous.high_streak.saturating_add(1), 0)
    } else if pressure <= PARTITION_CONTROLLER_LOW_THRESHOLD {
        (0, previous.low_streak.saturating_add(1))
    } else {
        (0, 0)
    };

    config.controller = PiControllerState {
        ewma_pressure,
        integral,
        high_streak,
        low_streak,
    };

    config.controller.ewma_pressure != previous.ewma_pressure
        || config.controller.integral != previous.integral
        || config.controller.high_streak != previous.high_streak
        || config.controller.low_streak != previous.low_streak
}

fn cooldown_active(config: &PartitionFamilyConfig, now_ms: i64) -> bool {
    config
        .cooldown_until_ms
        .is_some_and(|cooldown_until_ms| cooldown_until_ms > now_ms)
}

fn family_state_counts(partitions: &[PartitionInfo]) -> FamilyStateCounts {
    let mut counts = FamilyStateCounts::default();
    for partition in partitions {
        match partition.state {
            PartitionState::Open => {
                counts.open = counts.open.saturating_add(1);
            }
            PartitionState::WriteClosed => {
                counts.write_closed = counts.write_closed.saturating_add(1);
            }
            PartitionState::Draining => {
                counts.write_closed = counts.write_closed.saturating_add(1);
                counts.draining = counts.draining.saturating_add(1);
            }
            PartitionState::Retired => {
                counts.retired = counts.retired.saturating_add(1);
            }
        }
    }
    counts
}

fn ordered_log_partition_pressure(
    sample: Option<&PartitionLoadSample>,
    config: &PartitionFamilyConfig,
) -> f64 {
    let Some(sample) = sample else {
        return 0.0;
    };
    let writes = ratio(sample.writes, config.target_writes_per_second);
    let bytes = ratio(sample.bytes, config.target_bytes_per_second);
    let conflicts = ratio(sample.conflicts, config.target_conflicts_per_window);
    writes.max(bytes).max(conflicts)
}

fn queue_partition_pressure(
    sample: Option<&PartitionLoadSample>,
    config: &PartitionFamilyConfig,
) -> f64 {
    let Some(sample) = sample else {
        return 0.0;
    };
    let writes = ratio(sample.writes, config.target_writes_per_second);
    let bytes = ratio(sample.bytes, config.target_bytes_per_second);
    let conflicts = ratio(
        sample.queue_claim_conflicts,
        config.target_conflicts_per_window,
    );
    let oldest_visible = ratio(
        sample.oldest_visible_age_ms,
        config.target_oldest_visible_age_ms.max(1),
    );
    writes.max(bytes).max(conflicts).max(oldest_visible)
}

fn hottest_ordered_log_pressure(
    partitions: &[PartitionInfo],
    samples: &HashMap<u16, PartitionLoadSample>,
    config: &PartitionFamilyConfig,
) -> f64 {
    partitions
        .iter()
        .filter(|partition| partition.is_writable())
        .map(|partition| {
            ordered_log_partition_pressure(samples.get(&partition.partition_id), config)
        })
        .fold(0.0, f64::max)
}

fn hottest_queue_pressure(
    partitions: &[PartitionInfo],
    samples: &HashMap<u16, PartitionLoadSample>,
    config: &PartitionFamilyConfig,
) -> f64 {
    partitions
        .iter()
        .filter(|partition| partition.is_writable())
        .map(|partition| queue_partition_pressure(samples.get(&partition.partition_id), config))
        .fold(0.0, f64::max)
}

pub(crate) fn ordered_log_autosplit_candidate(
    family: &ResolvedPartitionFamily,
    samples: &HashMap<u16, PartitionLoadSample>,
    pressure: f64,
    now_ms: i64,
) -> Option<u16> {
    if family.config.freeze
        || !family.config.autoscale_enabled
        || cooldown_active(&family.config, now_ms)
        || open_partition_count(&family.partitions) >= family.config.max_open_partitions
        || family.config.controller.high_streak < PARTITION_CONTROLLER_HIGH_STREAK_TARGET
        || controller_pressure(&family.config.controller) < PARTITION_CONTROLLER_SPLIT_THRESHOLD
        || pressure < PARTITION_CONTROLLER_SPLIT_THRESHOLD
    {
        return None;
    }

    hottest_splittable_open_partition_id(&family.partitions, samples, &family.config)
}

pub(crate) fn hottest_splittable_open_partition_id(
    partitions: &[PartitionInfo],
    samples: &HashMap<u16, PartitionLoadSample>,
    config: &PartitionFamilyConfig,
) -> Option<u16> {
    partitions
        .iter()
        .filter(|partition| partition.is_writable())
        .filter(|partition| {
            samples
                .get(&partition.partition_id)
                .is_some_and(|sample| routing_key_bucket_count(sample) >= 2)
        })
        .map(|partition| {
            (
                partition.partition_id,
                ordered_log_partition_pressure(samples.get(&partition.partition_id), config),
            )
        })
        .max_by(|left, right| left.1.total_cmp(&right.1).then(left.0.cmp(&right.0)))
        .map(|(partition_id, _)| partition_id)
}

pub(crate) fn plan_queue_action(
    family: &crate::partition_family::ResolvedPartitionFamily,
    samples: &HashMap<u16, PartitionLoadSample>,
    empty_partitions: &HashMap<u16, bool>,
    now_ms: i64,
) -> Option<QueueReconcileAction> {
    if family.config.freeze
        || !family.config.autoscale_enabled
        || cooldown_active(&family.config, now_ms)
    {
        return None;
    }

    if let Some(partition) = family.partitions.iter().find(|partition| {
        partition.is_draining()
            && empty_partitions
                .get(&partition.partition_id)
                .copied()
                .unwrap_or(false)
    }) {
        return Some(QueueReconcileAction::Retire {
            partition_id: partition.partition_id,
        });
    }

    let open_count = open_partition_count(&family.partitions);
    if family.config.controller.high_streak >= PARTITION_CONTROLLER_HIGH_STREAK_TARGET
        && controller_pressure(&family.config.controller)
            >= PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD
        && hottest_queue_pressure(&family.partitions, samples, &family.config)
            >= PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD
        && open_count < family.config.max_open_partitions
    {
        return Some(QueueReconcileAction::AddPartition);
    }

    if family.partitions.iter().any(PartitionInfo::is_draining) {
        return None;
    }

    if family.config.controller.low_streak < PARTITION_CONTROLLER_LOW_STREAK_TARGET
        || controller_pressure(&family.config.controller) > PARTITION_CONTROLLER_LOW_THRESHOLD
        || open_count <= family.config.min_open_partitions
    {
        return None;
    }

    family
        .partitions
        .iter()
        .filter(|partition| partition.is_writable())
        .min_by(|left, right| {
            let left_pressure =
                queue_partition_pressure(samples.get(&left.partition_id), &family.config);
            let right_pressure =
                queue_partition_pressure(samples.get(&right.partition_id), &family.config);
            left_pressure
                .total_cmp(&right_pressure)
                .then(left.partition_id.cmp(&right.partition_id))
        })
        .map(|partition| QueueReconcileAction::BeginDrain {
            partition_id: partition.partition_id,
        })
}

fn queue_action_name(action: QueueReconcileAction) -> &'static str {
    match action {
        QueueReconcileAction::AddPartition => "add_partition",
        QueueReconcileAction::BeginDrain { .. } => "begin_drain",
        QueueReconcileAction::Retire { .. } => "retire",
    }
}

pub(crate) fn apply_queue_action(
    family: &mut crate::partition_family::ResolvedPartitionFamily,
    action: QueueReconcileAction,
    now_ms: i64,
) -> bool {
    match action {
        QueueReconcileAction::AddPartition => {
            family.partitions.push(PartitionInfo::new_open(
                next_partition_id(&family.partitions),
                next_placement_slot(&family.partitions),
                0,
                None,
            ));
            family.sort_by_partition_id();
        }
        QueueReconcileAction::BeginDrain { partition_id } => {
            let Some(partition) = family.partition_mut(partition_id) else {
                return false;
            };
            if partition.begin_draining().is_err() {
                return false;
            }
        }
        QueueReconcileAction::Retire { partition_id } => {
            let Some(partition) = family.partition_mut(partition_id) else {
                return false;
            };
            if partition.retire().is_err() {
                return false;
            }
        }
    }

    family.config.note_topology_change(now_ms);
    family.refresh_partition_count();
    true
}

fn ratio(value: u64, target: u64) -> f64 {
    if target == 0 {
        return 0.0;
    }
    value as f64 / target as f64
}

fn ready_visibility_key(prefix: &[u8], key: &[u8]) -> StorageResult<MessageVisibilityKey> {
    let suffix = key
        .strip_prefix(prefix)
        .ok_or_else(|| StorageError::internal("queue ready key does not match prefix"))?;
    let value = String::from_utf8(suffix.to_vec())
        .map_err(|error| StorageError::internal(&format!("decode queue ready key: {error}")))?;
    Ok(MessageVisibilityKey(value))
}
