use std::collections::HashMap;

use crate::{
    constants::{
        PARTITION_AUTOSCALE_COOLDOWN_MS, PARTITION_CONTROLLER_HIGH_STREAK_TARGET,
        PARTITION_CONTROLLER_LOW_STREAK_TARGET, PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD,
        PARTITION_CONTROLLER_SPLIT_THRESHOLD,
    },
    partition_family::{
        PartitionFamilyKind, PartitionInfo, PartitionLoadSample, PartitionState,
        ResolvedPartitionFamily, default_partition_family_config, initial_partition_infos,
        next_partition_id, next_placement_slot, open_partition_count, routing_key_bucket_bit,
        split_partition_children,
    },
    partition_reconcile::{
        QueueReconcileAction, apply_queue_action, controller_pressure,
        hottest_splittable_open_partition_id, ordered_log_autosplit_candidate, plan_queue_action,
        step_pi_controller,
    },
};

fn writable_owner_count(partitions: &[PartitionInfo], hash: u64) -> usize {
    partitions
        .iter()
        .filter(|partition| partition.is_writable())
        .filter(|partition| {
            partition.hash_start_inclusive <= hash
                && partition
                    .hash_end_exclusive
                    .is_none_or(|exclusive_end| hash < exclusive_end)
        })
        .count()
}

#[test]
fn step_pi_controller_accumulates_high_pressure_streaks_tests() {
    let mut config = default_partition_family_config(PartitionFamilyKind::OrderedLog, 1);

    for _ in 0..PARTITION_CONTROLLER_HIGH_STREAK_TARGET {
        assert!(step_pi_controller(
            &mut config,
            PARTITION_CONTROLLER_SPLIT_THRESHOLD + 0.2,
        ));
    }

    assert_eq!(
        config.controller.high_streak,
        PARTITION_CONTROLLER_HIGH_STREAK_TARGET
    );
    assert_eq!(config.controller.low_streak, 0);
    assert!(controller_pressure(&config.controller) >= PARTITION_CONTROLLER_SPLIT_THRESHOLD);
}

#[test]
fn queue_plans_scale_out_after_sustained_pressure_tests() {
    let mut family = ResolvedPartitionFamily {
        config: default_partition_family_config(PartitionFamilyKind::StandardQueue, 2),
        partitions: initial_partition_infos(2),
    };
    family.config.controller.high_streak = PARTITION_CONTROLLER_HIGH_STREAK_TARGET;
    family.config.controller.ewma_pressure = PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD;
    family.config.controller.integral = 1.0;

    let mut samples = HashMap::new();
    samples.insert(
        0,
        PartitionLoadSample {
            writes: family.config.target_writes_per_second.saturating_mul(2),
            ..Default::default()
        },
    );

    let action = plan_queue_action(&family, &samples, &HashMap::new(), 0);
    assert_eq!(action, Some(QueueReconcileAction::AddPartition));
}

#[test]
fn ordered_log_split_candidate_requires_routing_key_diversity_tests() {
    let mut family = ResolvedPartitionFamily {
        config: default_partition_family_config(PartitionFamilyKind::OrderedLog, 2),
        partitions: initial_partition_infos(2),
    };
    family.config.controller.high_streak = PARTITION_CONTROLLER_HIGH_STREAK_TARGET;
    family.config.controller.ewma_pressure = PARTITION_CONTROLLER_SPLIT_THRESHOLD;
    family.config.controller.integral = 1.0;
    let mut single_key_samples = HashMap::new();
    single_key_samples.insert(
        0,
        PartitionLoadSample {
            writes: family.config.target_writes_per_second.saturating_mul(2),
            routing_key_bucket_bitmap: routing_key_bucket_bit(7),
            ..Default::default()
        },
    );
    assert_eq!(
        hottest_splittable_open_partition_id(
            &family.partitions,
            &single_key_samples,
            &family.config,
        ),
        None
    );

    let mut diverse_samples = HashMap::new();
    diverse_samples.insert(
        0,
        PartitionLoadSample {
            writes: family.config.target_writes_per_second.saturating_mul(2),
            routing_key_bucket_bitmap: routing_key_bucket_bit(7) | routing_key_bucket_bit(11),
            ..Default::default()
        },
    );
    assert_eq!(
        hottest_splittable_open_partition_id(&family.partitions, &diverse_samples, &family.config),
        Some(0)
    );
    assert_eq!(
        ordered_log_autosplit_candidate(
            &family,
            &diverse_samples,
            PARTITION_CONTROLLER_SPLIT_THRESHOLD,
            0,
        ),
        Some(0)
    );
}

#[test]
fn ordered_log_sustained_hot_sample_splits_with_total_writable_coverage_tests() {
    let mut family = ResolvedPartitionFamily {
        config: default_partition_family_config(PartitionFamilyKind::OrderedLog, 2),
        partitions: initial_partition_infos(2),
    };
    let mut samples = HashMap::new();
    samples.insert(
        0,
        PartitionLoadSample {
            writes: family.config.target_writes_per_second.saturating_mul(2),
            routing_key_bucket_bitmap: routing_key_bucket_bit(7) | routing_key_bucket_bit(11),
            ..Default::default()
        },
    );

    for _ in 0..PARTITION_CONTROLLER_HIGH_STREAK_TARGET {
        assert!(step_pi_controller(
            &mut family.config,
            PARTITION_CONTROLLER_SPLIT_THRESHOLD + 0.2,
        ));
    }

    let partition_id = ordered_log_autosplit_candidate(
        &family,
        &samples,
        PARTITION_CONTROLLER_SPLIT_THRESHOLD + 0.2,
        0,
    )
    .expect("sustained diverse hot sample should select a partition");
    let parent_index = family
        .partitions
        .iter()
        .position(|partition| partition.partition_id == partition_id)
        .expect("candidate partition exists");
    let parent = family.partitions[parent_index].clone();
    let (left_child, right_child) = split_partition_children(
        &parent,
        next_partition_id(&family.partitions),
        next_partition_id(&family.partitions).saturating_add(1),
        next_placement_slot(&family.partitions),
        next_placement_slot(&family.partitions).saturating_add(1),
    )
    .expect("selected partition can split");

    family.partitions[parent_index]
        .mark_write_closed()
        .expect("open parent can close writes");
    family.partitions.push(left_child);
    family.partitions.push(right_child);

    assert_eq!(
        family.partitions[parent_index].state,
        PartitionState::WriteClosed
    );
    assert_eq!(open_partition_count(&family.partitions), 3);
    for hash in [0, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
        assert_eq!(writable_owner_count(&family.partitions, hash), 1);
    }
}

#[test]
fn queue_plans_drain_for_cold_partition_after_low_pressure_tests() {
    let mut family = ResolvedPartitionFamily {
        config: default_partition_family_config(PartitionFamilyKind::StandardQueue, 3),
        partitions: initial_partition_infos(3),
    };
    family.config.min_open_partitions = 2;
    family.config.controller.low_streak = PARTITION_CONTROLLER_LOW_STREAK_TARGET;
    family.config.controller.ewma_pressure = 0.2;
    family.config.controller.integral = -1.0;

    let mut samples = HashMap::new();
    samples.insert(
        0,
        PartitionLoadSample {
            writes: family.config.target_writes_per_second,
            ..Default::default()
        },
    );

    let action = plan_queue_action(&family, &samples, &HashMap::new(), 0);
    assert_eq!(
        action,
        Some(QueueReconcileAction::BeginDrain { partition_id: 1 })
    );
}

#[test]
fn queue_retires_empty_draining_partition_before_other_changes_tests() {
    let mut family = ResolvedPartitionFamily {
        config: default_partition_family_config(PartitionFamilyKind::StandardQueue, 2),
        partitions: initial_partition_infos(2),
    };
    family
        .partitions
        .get_mut(1)
        .expect("partition present")
        .begin_draining()
        .expect("open partition can start draining");
    family.config.controller.low_streak = PARTITION_CONTROLLER_LOW_STREAK_TARGET;
    family.config.controller.ewma_pressure = 0.1;
    family.config.controller.integral = -1.0;

    let mut empty = HashMap::new();
    empty.insert(1, true);

    let action = plan_queue_action(&family, &HashMap::new(), &empty, 0);
    assert_eq!(
        action,
        Some(QueueReconcileAction::Retire { partition_id: 1 })
    );
}

#[test]
fn queue_apply_action_add_partition_updates_epoch_and_cooldown_tests() {
    let mut family = ResolvedPartitionFamily {
        config: default_partition_family_config(PartitionFamilyKind::StandardQueue, 2),
        partitions: initial_partition_infos(2),
    };
    family.config.controller.high_streak = 9;
    family.config.controller.low_streak = 4;

    let changed = apply_queue_action(&mut family, QueueReconcileAction::AddPartition, 1234);

    assert!(changed);
    assert_eq!(family.partitions.len(), 3);
    assert_eq!(
        family.config.cooldown_until_ms,
        Some(1234 + PARTITION_AUTOSCALE_COOLDOWN_MS)
    );
    assert_eq!(family.config.controller.high_streak, 0);
    assert_eq!(family.config.controller.low_streak, 0);
}
