use storage_types::{StorageError, StorageResult};

use super::{
    PartitionFamilyKind, PartitionFamilyReconciliation, PartitionInfo, PartitionLoadObservation,
    PartitionLoadSample, PartitionParentChildren, PartitionTopologyChange, ResolvedPartitionFamily,
};
use crate::constants::{
    PARTITION_CONTROLLER_EWMA_ALPHA, PARTITION_CONTROLLER_HIGH_STREAK_TARGET,
    PARTITION_CONTROLLER_INTEGRAL_MAX, PARTITION_CONTROLLER_INTEGRAL_MIN,
    PARTITION_CONTROLLER_LOW_STREAK_TARGET, PARTITION_CONTROLLER_LOW_THRESHOLD,
    PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD, PARTITION_CONTROLLER_SPLIT_THRESHOLD,
};

impl PartitionLoadSample {
    pub fn merge(&mut self, delta: &Self) {
        self.writes = self.writes.saturating_add(delta.writes);
        self.bytes = self.bytes.saturating_add(delta.bytes);
        self.conflicts = self.conflicts.saturating_add(delta.conflicts);
        self.routing_key_bucket_bitmap |= delta.routing_key_bucket_bitmap;
        self.queue_scan_work = self.queue_scan_work.saturating_add(delta.queue_scan_work);
        self.queue_claim_conflicts = self
            .queue_claim_conflicts
            .saturating_add(delta.queue_claim_conflicts);
        self.oldest_visible_age_ms = self.oldest_visible_age_ms.max(delta.oldest_visible_age_ms);
        self.visible_count = self.visible_count.saturating_add(delta.visible_count);
        self.invisible_count = self.invisible_count.saturating_add(delta.invisible_count);
    }

    #[must_use]
    pub const fn routing_key_bucket_count(&self) -> u32 {
        self.routing_key_bucket_bitmap.count_ones()
    }
}

impl ResolvedPartitionFamily {
    pub fn reconcile(
        mut self,
        observations: &mut [PartitionLoadObservation],
        now_ms: i64,
    ) -> StorageResult<PartitionFamilyReconciliation> {
        self.validate()?;
        observations.sort_unstable_by_key(|observation| observation.partition_id);
        let pressure = hottest_pressure(&self, observations);
        let expected_epoch = self.config.family_epoch;
        let controller_changed = update_controller(&mut self, pressure);
        let topology_change = match self.config.family_kind {
            PartitionFamilyKind::OrderedLog => {
                self.reconcile_ordered_log(observations, pressure, now_ms)?
            }
            PartitionFamilyKind::StandardQueue => {
                self.reconcile_queue(observations, pressure, now_ms)?
            }
        };
        let reconciliation = PartitionFamilyReconciliation {
            expected_epoch,
            write_required: controller_changed || topology_change.is_some(),
            family: self,
            topology_change,
        };
        reconciliation.family.validate()?;
        Ok(reconciliation)
    }

    fn reconcile_ordered_log(
        &mut self,
        observations: &[PartitionLoadObservation],
        pressure: f64,
        now_ms: i64,
    ) -> StorageResult<Option<PartitionTopologyChange>> {
        if !ordered_log_split_allowed(self, pressure, now_ms) {
            return Ok(None);
        }
        let Some(parent_index) = select_ordered_log_parent(self, observations) else {
            return Ok(None);
        };
        let parent = self.partitions[parent_index].clone();
        let Some(children) = create_ordered_log_children(self, &parent)? else {
            return Ok(None);
        };
        apply_ordered_log_split(self, parent_index, children, now_ms).map(Some)
    }

    fn reconcile_queue(
        &mut self,
        observations: &[PartitionLoadObservation],
        pressure: f64,
        now_ms: i64,
    ) -> StorageResult<Option<PartitionTopologyChange>> {
        if self.config.freeze || !self.config.autoscale_enabled || cooldown_active(self, now_ms) {
            return Ok(None);
        }
        if let Some(change) = retire_empty_queue_partition(self, observations, now_ms)? {
            return Ok(Some(change));
        }
        let open_count = self.open_partition_count();
        if queue_scale_out_required(self, pressure, open_count) {
            return add_queue_partition(self, now_ms).map(Some);
        }
        if !queue_scale_in_required(self, open_count) {
            return Ok(None);
        }
        begin_coldest_queue_drain(self, observations, now_ms)
    }

    fn note_topology_change(&mut self, now_ms: i64) {
        self.config.note_topology_change(now_ms);
        self.refresh_partition_count();
    }

    fn open_partition_count(&self) -> u16 {
        u16::try_from(
            self.partitions
                .iter()
                .filter(|partition| partition.is_writable())
                .count(),
        )
        .unwrap_or(u16::MAX)
    }

    fn next_partition_id(&self) -> StorageResult<u16> {
        let current = self
            .partitions
            .iter()
            .map(|partition| partition.partition_id)
            .max()
            .ok_or_else(|| StorageError::internal("partition family has no partitions"))?;
        current
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("partition id space is exhausted"))
    }

    fn next_placement_slot(&self) -> StorageResult<u16> {
        let current = self
            .partitions
            .iter()
            .map(|partition| partition.placement_slot)
            .max()
            .ok_or_else(|| StorageError::internal("partition family has no placement slots"))?;
        current
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("partition placement slot space is exhausted"))
    }
}

fn ordered_log_split_allowed(family: &ResolvedPartitionFamily, pressure: f64, now_ms: i64) -> bool {
    !family.config.freeze
        && family.config.autoscale_enabled
        && !cooldown_active(family, now_ms)
        && family.open_partition_count() < family.config.max_open_partitions
        && family.config.controller.high_streak >= PARTITION_CONTROLLER_HIGH_STREAK_TARGET
        && controller_pressure(family) >= PARTITION_CONTROLLER_SPLIT_THRESHOLD
        && pressure >= PARTITION_CONTROLLER_SPLIT_THRESHOLD
}

fn select_ordered_log_parent(
    family: &ResolvedPartitionFamily,
    observations: &[PartitionLoadObservation],
) -> Option<usize> {
    family
        .partitions
        .iter()
        .enumerate()
        .filter(|(_, partition)| partition.is_writable())
        .filter_map(|(index, partition)| {
            let sample = sample_for(observations, partition.partition_id)?;
            (sample.routing_key_bucket_count() >= 2).then_some((
                index,
                partition.partition_id,
                ordered_log_partition_pressure(sample, &family.config),
            ))
        })
        .max_by(|left, right| left.2.total_cmp(&right.2).then(left.1.cmp(&right.1)))
        .map(|candidate| candidate.0)
}

fn create_ordered_log_children(
    family: &ResolvedPartitionFamily,
    parent: &PartitionInfo,
) -> StorageResult<Option<(PartitionInfo, PartitionInfo)>> {
    let left_id = family.next_partition_id()?;
    let right_id = left_id
        .checked_add(1)
        .ok_or_else(|| StorageError::internal("ordered-log partition id space is exhausted"))?;
    let left_slot = family.next_placement_slot()?;
    let right_slot = left_slot
        .checked_add(1)
        .ok_or_else(|| StorageError::internal("ordered-log placement slot space is exhausted"))?;
    Ok(partition_children(
        parent, left_id, right_id, left_slot, right_slot,
    ))
}

fn apply_ordered_log_split(
    family: &mut ResolvedPartitionFamily,
    parent_index: usize,
    mut children: (PartitionInfo, PartitionInfo),
    now_ms: i64,
) -> StorageResult<PartitionTopologyChange> {
    let parent_partition_id = family.partitions[parent_index].partition_id;
    family.partitions[parent_index]
        .mark_write_closed()
        .map_err(|error| StorageError::internal(&error.to_string()))?;
    family.partitions[parent_index].sealed_after_id = None;
    children.0.opened_after_id = None;
    children.1.opened_after_id = None;
    let left_child_partition_id = children.0.partition_id;
    let right_child_partition_id = children.1.partition_id;
    family.memberships.push(PartitionParentChildren {
        parent_partition_id,
        left_child_partition_id,
        right_child_partition_id,
        boundary: None,
    });
    family.partitions.push(children.0);
    family.partitions.push(children.1);
    family.sort_by_hash_range();
    family.config.note_topology_change(now_ms);
    family.refresh_partition_count();
    family.config.min_open_partitions = family
        .config
        .min_open_partitions
        .max(family.open_partition_count());
    Ok(PartitionTopologyChange::SplitOrderedLog {
        parent_partition_id,
        left_child_partition_id,
        right_child_partition_id,
    })
}

fn retire_empty_queue_partition(
    family: &mut ResolvedPartitionFamily,
    observations: &[PartitionLoadObservation],
    now_ms: i64,
) -> StorageResult<Option<PartitionTopologyChange>> {
    let candidate = family
        .partitions
        .iter()
        .find(|partition| {
            partition.is_draining()
                && observation_for(observations, partition.partition_id)
                    .is_some_and(|observation| observation.queue_empty)
        })
        .map(|partition| partition.partition_id);
    let Some(partition_id) = candidate else {
        return Ok(None);
    };
    family
        .partition_mut(partition_id)
        .ok_or_else(|| StorageError::internal("selected draining partition disappeared"))?
        .retire()
        .map_err(|error| StorageError::internal(&error.to_string()))?;
    family.note_topology_change(now_ms);
    Ok(Some(PartitionTopologyChange::RetireQueuePartition {
        partition_id,
    }))
}

fn queue_scale_out_required(
    family: &ResolvedPartitionFamily,
    pressure: f64,
    open_count: u16,
) -> bool {
    family.config.controller.high_streak >= PARTITION_CONTROLLER_HIGH_STREAK_TARGET
        && controller_pressure(family) >= PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD
        && pressure >= PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD
        && open_count < family.config.max_open_partitions
}

fn add_queue_partition(
    family: &mut ResolvedPartitionFamily,
    now_ms: i64,
) -> StorageResult<PartitionTopologyChange> {
    let partition_id = family.next_partition_id()?;
    let placement_slot = family.next_placement_slot()?;
    family.partitions.push(PartitionInfo::new_open(
        partition_id,
        placement_slot,
        0,
        None,
    ));
    family.sort_by_partition_id();
    family.note_topology_change(now_ms);
    Ok(PartitionTopologyChange::AddQueuePartition { partition_id })
}

fn queue_scale_in_required(family: &ResolvedPartitionFamily, open_count: u16) -> bool {
    !family.partitions.iter().any(PartitionInfo::is_draining)
        && family.config.controller.low_streak >= PARTITION_CONTROLLER_LOW_STREAK_TARGET
        && controller_pressure(family) <= PARTITION_CONTROLLER_LOW_THRESHOLD
        && open_count > family.config.min_open_partitions
}

fn begin_coldest_queue_drain(
    family: &mut ResolvedPartitionFamily,
    observations: &[PartitionLoadObservation],
    now_ms: i64,
) -> StorageResult<Option<PartitionTopologyChange>> {
    let candidate = family
        .partitions
        .iter()
        .filter(|partition| partition.is_writable())
        .min_by(|left, right| {
            queue_partition_pressure(sample_for(observations, left.partition_id), &family.config)
                .total_cmp(&queue_partition_pressure(
                    sample_for(observations, right.partition_id),
                    &family.config,
                ))
                .then(left.partition_id.cmp(&right.partition_id))
        })
        .map(|partition| partition.partition_id);
    let Some(partition_id) = candidate else {
        return Ok(None);
    };
    family
        .partition_mut(partition_id)
        .ok_or_else(|| StorageError::internal("selected queue partition disappeared"))?
        .begin_draining()
        .map_err(|error| StorageError::internal(&error.to_string()))?;
    family.note_topology_change(now_ms);
    Ok(Some(PartitionTopologyChange::BeginQueueDrain {
        partition_id,
    }))
}

fn update_controller(family: &mut ResolvedPartitionFamily, pressure: f64) -> bool {
    let previous = &family.config.controller;
    let previous_ewma_pressure = previous.ewma_pressure;
    let previous_integral = previous.integral;
    let previous_high_streak = previous.high_streak;
    let previous_low_streak = previous.low_streak;
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
    family.config.controller.ewma_pressure = ewma_pressure;
    family.config.controller.integral = integral;
    family.config.controller.high_streak = high_streak;
    family.config.controller.low_streak = low_streak;
    previous_ewma_pressure != ewma_pressure
        || previous_integral != integral
        || previous_high_streak != high_streak
        || previous_low_streak != low_streak
}

fn hottest_pressure(
    family: &ResolvedPartitionFamily,
    observations: &[PartitionLoadObservation],
) -> f64 {
    family
        .partitions
        .iter()
        .filter(|partition| partition.is_writable())
        .map(|partition| {
            let sample = sample_for(observations, partition.partition_id);
            match family.config.family_kind {
                PartitionFamilyKind::OrderedLog => sample.map_or(0.0, |sample| {
                    ordered_log_partition_pressure(sample, &family.config)
                }),
                PartitionFamilyKind::StandardQueue => {
                    queue_partition_pressure(sample, &family.config)
                }
            }
        })
        .fold(0.0, f64::max)
}

fn observation_for(
    observations: &[PartitionLoadObservation],
    partition_id: u16,
) -> Option<&PartitionLoadObservation> {
    observations
        .binary_search_by_key(&partition_id, |observation| observation.partition_id)
        .ok()
        .map(|index| &observations[index])
}

fn sample_for(
    observations: &[PartitionLoadObservation],
    partition_id: u16,
) -> Option<&PartitionLoadSample> {
    observation_for(observations, partition_id).map(|observation| &observation.sample)
}

fn controller_pressure(family: &ResolvedPartitionFamily) -> f64 {
    family.config.controller.ewma_pressure + (0.2 * family.config.controller.integral)
}

fn cooldown_active(family: &ResolvedPartitionFamily, now_ms: i64) -> bool {
    family
        .config
        .cooldown_until_ms
        .is_some_and(|cooldown_until_ms| cooldown_until_ms > now_ms)
}

fn ordered_log_partition_pressure(
    sample: &PartitionLoadSample,
    config: &super::PartitionFamilyConfig,
) -> f64 {
    ratio(sample.writes, config.target_writes_per_second)
        .max(ratio(sample.bytes, config.target_bytes_per_second))
        .max(ratio(sample.conflicts, config.target_conflicts_per_window))
}

fn queue_partition_pressure(
    sample: Option<&PartitionLoadSample>,
    config: &super::PartitionFamilyConfig,
) -> f64 {
    let Some(sample) = sample else {
        return 0.0;
    };
    ratio(sample.writes, config.target_writes_per_second)
        .max(ratio(sample.bytes, config.target_bytes_per_second))
        .max(ratio(
            sample.queue_claim_conflicts,
            config.target_conflicts_per_window,
        ))
        .max(ratio(
            sample.oldest_visible_age_ms,
            config.target_oldest_visible_age_ms.max(1),
        ))
}

fn ratio(value: u64, target: u64) -> f64 {
    if target == 0 {
        return 0.0;
    }
    value as f64 / target as f64
}

fn partition_children(
    parent: &PartitionInfo,
    left_partition_id: u16,
    right_partition_id: u16,
    left_slot: u16,
    right_slot: u16,
) -> Option<(PartitionInfo, PartitionInfo)> {
    const HASH_SPACE_SIZE: u128 = (u64::MAX as u128) + 1;
    let start = u128::from(parent.hash_start_inclusive);
    let end = parent
        .hash_end_exclusive
        .map_or(HASH_SPACE_SIZE, u128::from);
    if end <= start.saturating_add(1) {
        return None;
    }
    let midpoint = start + ((end - start) / 2);
    if midpoint <= start || midpoint >= end {
        return None;
    }
    let midpoint = u64::try_from(midpoint).ok()?;
    let mut left = PartitionInfo::new_open(
        left_partition_id,
        left_slot,
        parent.hash_start_inclusive,
        Some(midpoint),
    );
    left.opened_after_id = parent.sealed_after_id;
    let mut right = PartitionInfo::new_open(
        right_partition_id,
        right_slot,
        midpoint,
        parent.hash_end_exclusive,
    );
    right.opened_after_id = parent.sealed_after_id;
    Some((left, right))
}
