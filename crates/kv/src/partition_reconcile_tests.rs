use std::collections::HashMap;

use crate::{
    constants::{
        PARTITION_AUTOSCALE_COOLDOWN_MS, PARTITION_CONTROLLER_HIGH_STREAK_TARGET,
        PARTITION_CONTROLLER_LOW_STREAK_TARGET, PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD,
        PARTITION_CONTROLLER_SPLIT_THRESHOLD,
    },
    partition_family::{
        PartitionFamilyKind, PartitionLoadSample, ResolvedPartitionFamily,
        default_partition_family_config, initial_partition_infos, routing_key_bucket_bit,
    },
    partition_reconcile::{
        QueueReconcileAction, apply_queue_action, controller_pressure,
        hottest_splittable_open_partition_id, ordered_log_autosplit_candidate, plan_queue_action,
        step_pi_controller,
    },
};

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
