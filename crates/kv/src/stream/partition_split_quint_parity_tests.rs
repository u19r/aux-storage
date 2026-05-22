use std::collections::BTreeSet;

use proptest::prelude::*;
use storage_types::StreamItemId;

use crate::{
    constants::{
        PARTITION_AUTOSCALE_COOLDOWN_MS, PARTITION_CONTROLLER_HIGH_STREAK_TARGET,
        PARTITION_CONTROLLER_SPLIT_THRESHOLD,
    },
    partition_family::{
        PartitionFamilyKind, PartitionInfo, PartitionLoadSample, PartitionState,
        ResolvedPartitionFamily, default_partition_family_config, initial_partition_infos,
        next_partition_id, next_placement_slot, open_partition_count, partition_contains_hash,
        routing_key_bucket_bit, split_partition_children,
    },
    partition_reconcile::ordered_log_autosplit_candidate,
    stream::provider::partition_is_candidate_for_read,
};

const INITIAL_OPEN_COUNT: u16 = 2;

const MIRRORED_QUINT_SCENARIOS: &[&str] = &[
    "initial_family_covers_every_hash_once",
    "split_preserves_hash_coverage",
    "initial_family_preserves_item_level_total_ordering",
    "split_preserves_item_level_total_ordering",
    "split_routes_parent_range_across_children",
    "split_leaves_one_closed_parent_and_two_open_children",
    "forward_fan_in_keeps_parent_before_boundary",
    "forward_fan_in_drops_parent_after_boundary",
    "reverse_fan_in_keeps_children_after_boundary",
    "reverse_fan_in_drops_children_at_boundary",
    "hot_single_routing_bucket_does_not_autosplit",
    "hot_diverse_routing_buckets_can_autosplit",
    "freeze_blocks_autosplit",
    "cooldown_blocks_autosplit",
];

fn quint_run_scenarios(source: &str) -> BTreeSet<String> {
    source
        .lines()
        .filter_map(|line| line.trim().strip_prefix("run "))
        .filter_map(|rest| {
            rest.split_once(" =")
                .map(|(name, _)| camel_to_snake(name.trim()))
        })
        .collect()
}

fn camel_to_snake(name: &str) -> String {
    let mut snake = String::with_capacity(name.len() + 8);
    for (index, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if index != 0 {
                snake.push('_');
            }
            for lower in ch.to_lowercase() {
                snake.push(lower);
            }
        } else {
            snake.push(ch);
        }
    }
    snake
}

#[test]
fn mirrored_rust_scenarios_cover_every_quint_run_scenario() {
    let expected = quint_run_scenarios(include_str!(
        "../../../../quint/stream_partition_split_tests.qnt"
    ));
    let actual = MIRRORED_QUINT_SCENARIOS
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();

    assert_eq!(actual, expected);
}

fn item_id(value: u8) -> StreamItemId {
    let mut bytes = [0_u8; 12];
    bytes[11] = value;
    StreamItemId::from(bytes)
}

fn split_family(partition_count: u16, parent_partition_id: u16) -> Vec<PartitionInfo> {
    split_existing_family(
        initial_partition_infos(partition_count),
        parent_partition_id,
        item_id(10),
    )
    .expect("generated parent partition should be splittable")
}

fn split_existing_family(
    mut partitions: Vec<PartitionInfo>,
    parent_partition_id: u16,
    boundary: StreamItemId,
) -> Option<Vec<PartitionInfo>> {
    let index = partitions
        .iter()
        .position(|partition| partition.partition_id == parent_partition_id)
        .expect("generated parent partition should exist");
    let parent = partitions[index].clone();
    let (mut left_child, mut right_child) = split_partition_children(
        &parent,
        next_partition_id(&partitions),
        next_partition_id(&partitions).saturating_add(1),
        next_placement_slot(&partitions),
        next_placement_slot(&partitions).saturating_add(1),
    )
    .expect("generated hash ranges should be splittable");

    let mut closed_parent = parent;
    closed_parent
        .mark_write_closed()
        .expect("open parent can be write closed");
    closed_parent.sealed_after_id = Some(boundary);
    left_child.opened_after_id = Some(boundary);
    right_child.opened_after_id = Some(boundary);

    partitions[index] = closed_parent;
    partitions.push(left_child);
    partitions.push(right_child);
    Some(partitions)
}

fn writable_owner_count(partitions: &[PartitionInfo], hash: u64) -> usize {
    partitions
        .iter()
        .filter(|partition| partition.is_writable())
        .filter(|partition| partition_contains_hash(partition, hash))
        .count()
}

fn readable_owners(partitions: &[PartitionInfo], hash: u64) -> Vec<&PartitionInfo> {
    partitions
        .iter()
        .filter(|partition| partition.is_readable())
        .filter(|partition| partition_contains_hash(partition, hash))
        .collect()
}

fn item_id_ranges_do_not_overlap(left: &PartitionInfo, right: &PartitionInfo) -> bool {
    left.partition_id == right.partition_id
        || left
            .sealed_after_id
            .zip(right.opened_after_id)
            .is_some_and(|(sealed_after_id, opened_after_id)| sealed_after_id <= opened_after_id)
        || right
            .sealed_after_id
            .zip(left.opened_after_id)
            .is_some_and(|(sealed_after_id, opened_after_id)| sealed_after_id <= opened_after_id)
}

fn total_ordering_preserved_at_item_level(partitions: &[PartitionInfo], hash: u64) -> bool {
    if writable_owner_count(partitions, hash) != 1 {
        return false;
    }

    let owners = readable_owners(partitions, hash);
    owners.iter().all(|left| {
        owners
            .iter()
            .all(|right| item_id_ranges_do_not_overlap(left, right))
    })
}

fn partition_range_valid(partition: &PartitionInfo) -> bool {
    if partition.is_readable() {
        partition.hash_start_inclusive < partition.hash_end_exclusive.unwrap_or(u64::MAX)
    } else {
        true
    }
}

fn first_splittable_open_partition(partitions: &[PartitionInfo]) -> Option<u16> {
    partitions
        .iter()
        .find(|partition| {
            partition.is_writable()
                && partition
                    .hash_end_exclusive
                    .is_some_and(|end| end > partition.hash_start_inclusive.saturating_add(1))
        })
        .map(|partition| partition.partition_id)
}

fn family_with_hot_controller(partitions: Vec<PartitionInfo>) -> ResolvedPartitionFamily {
    let mut config = default_partition_family_config(
        PartitionFamilyKind::OrderedLog,
        open_partition_count(&partitions),
    );
    config.controller.high_streak = PARTITION_CONTROLLER_HIGH_STREAK_TARGET;
    config.controller.ewma_pressure = PARTITION_CONTROLLER_SPLIT_THRESHOLD;
    config.controller.integral = 1.0;
    ResolvedPartitionFamily { config, partitions }
}

#[test]
fn initial_family_covers_every_hash_once() {
    let partitions = initial_partition_infos(2);

    for hash in [0, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
        assert_eq!(writable_owner_count(&partitions, hash), 1);
    }
}

proptest! {
    #[test]
    fn split_preserves_hash_coverage(partition_count in 1_u16..32, parent_index in 0_u16..32, hash in any::<u64>()) {
        let parent_partition_id = parent_index % partition_count;
        let partitions = split_family(partition_count, parent_partition_id);

        prop_assert_eq!(writable_owner_count(&partitions, hash), 1);
    }
}

proptest! {
    #[test]
    fn repeated_splits_keep_count_monotonic_bounded_and_ordered(split_rounds in 0_u8..6, hash in any::<u64>()) {
        let mut partitions = initial_partition_infos(INITIAL_OPEN_COUNT);
        let mut split_count = 0_u16;
        let mut previous_open_count = open_partition_count(&partitions);

        for round in 0..split_rounds {
            if open_partition_count(&partitions) >= 8 {
                break;
            }
            let Some(parent_id) = first_splittable_open_partition(&partitions) else {
                break;
            };
            let Some(next_partitions) = split_existing_family(
                partitions,
                parent_id,
                item_id(10_u8.saturating_add(round)),
            ) else {
                break;
            };
            partitions = next_partitions;
            split_count = split_count.saturating_add(1);
            let open_count = open_partition_count(&partitions);

            prop_assert!(open_count >= previous_open_count);
            prop_assert!(open_count <= 8);
            prop_assert_eq!(open_count, INITIAL_OPEN_COUNT + split_count);
            prop_assert_eq!(writable_owner_count(&partitions, hash), 1);
            prop_assert!(total_ordering_preserved_at_item_level(&partitions, hash));
            prop_assert!(partitions.iter().all(partition_range_valid));

            previous_open_count = open_count;
        }
    }
}

#[test]
fn initial_family_preserves_item_level_total_ordering() {
    let partitions = initial_partition_infos(2);

    for hash in [0, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
        assert!(total_ordering_preserved_at_item_level(&partitions, hash));
    }
}

proptest! {
    #[test]
    fn split_preserves_item_level_total_ordering(partition_count in 1_u16..32, parent_index in 0_u16..32, hash in any::<u64>()) {
        let parent_partition_id = parent_index % partition_count;
        let partitions = split_family(partition_count, parent_partition_id);

        prop_assert!(total_ordering_preserved_at_item_level(&partitions, hash));
    }
}

#[test]
fn split_routes_parent_range_across_children() {
    let partitions = split_family(2, 0);
    let left_child = partitions
        .iter()
        .find(|partition| partition.partition_id == 2)
        .expect("left child exists");
    let right_child = partitions
        .iter()
        .find(|partition| partition.partition_id == 3)
        .expect("right child exists");

    assert!(partition_contains_hash(left_child, 1));
    assert!(partition_contains_hash(right_child, u64::MAX / 2));
    assert_eq!(writable_owner_count(&partitions, u64::MAX), 1);
}

#[test]
fn split_leaves_one_closed_parent_and_two_open_children() {
    let partitions = split_family(2, 0);

    assert_eq!(partitions[0].state, PartitionState::WriteClosed);
    assert!(partitions[2].is_writable());
    assert!(partitions[3].is_writable());
    assert_eq!(open_partition_count(&partitions), 3);
}

#[test]
fn forward_fan_in_keeps_parent_before_boundary() {
    let partitions = split_family(2, 0);

    assert!(partition_is_candidate_for_read(
        &partitions[0],
        Some(item_id(9)),
        false,
    ));
}

#[test]
fn forward_fan_in_drops_parent_after_boundary() {
    let partitions = split_family(2, 0);

    assert!(!partition_is_candidate_for_read(
        &partitions[0],
        Some(item_id(10)),
        false,
    ));
}

#[test]
fn reverse_fan_in_keeps_children_after_boundary() {
    let partitions = split_family(2, 0);

    assert!(partition_is_candidate_for_read(
        &partitions[2],
        Some(item_id(11)),
        true,
    ));
    assert!(partition_is_candidate_for_read(
        &partitions[3],
        Some(item_id(11)),
        true,
    ));
}

#[test]
fn reverse_fan_in_drops_children_at_boundary() {
    let partitions = split_family(2, 0);

    assert!(!partition_is_candidate_for_read(
        &partitions[2],
        Some(item_id(10)),
        true,
    ));
    assert!(!partition_is_candidate_for_read(
        &partitions[3],
        Some(item_id(10)),
        true,
    ));
}

#[test]
fn hot_single_routing_bucket_does_not_autosplit() {
    let family = family_with_hot_controller(initial_partition_infos(2));
    let samples = [(
        0,
        PartitionLoadSample {
            writes: family.config.target_writes_per_second.saturating_mul(2),
            routing_key_bucket_bitmap: routing_key_bucket_bit(7),
            ..Default::default()
        },
    )]
    .into_iter()
    .collect();

    assert_eq!(
        ordered_log_autosplit_candidate(&family, &samples, PARTITION_CONTROLLER_SPLIT_THRESHOLD, 0),
        None
    );
}

#[test]
fn hot_diverse_routing_buckets_can_autosplit() {
    let family = family_with_hot_controller(initial_partition_infos(2));
    let samples = [(
        0,
        PartitionLoadSample {
            writes: family.config.target_writes_per_second.saturating_mul(2),
            routing_key_bucket_bitmap: routing_key_bucket_bit(7) | routing_key_bucket_bit(11),
            ..Default::default()
        },
    )]
    .into_iter()
    .collect();

    assert_eq!(
        ordered_log_autosplit_candidate(&family, &samples, PARTITION_CONTROLLER_SPLIT_THRESHOLD, 0),
        Some(0)
    );
}

#[test]
fn freeze_blocks_autosplit() {
    let mut family = family_with_hot_controller(initial_partition_infos(2));
    family.config.freeze = true;
    let samples = [(
        0,
        PartitionLoadSample {
            writes: family.config.target_writes_per_second.saturating_mul(2),
            routing_key_bucket_bitmap: routing_key_bucket_bit(7) | routing_key_bucket_bit(11),
            ..Default::default()
        },
    )]
    .into_iter()
    .collect();

    assert_eq!(
        ordered_log_autosplit_candidate(&family, &samples, PARTITION_CONTROLLER_SPLIT_THRESHOLD, 0),
        None
    );
}

#[test]
fn cooldown_blocks_autosplit() {
    let mut family = family_with_hot_controller(initial_partition_infos(2));
    family.config.cooldown_until_ms = Some(1);
    let samples = [(
        0,
        PartitionLoadSample {
            writes: family.config.target_writes_per_second.saturating_mul(2),
            routing_key_bucket_bitmap: routing_key_bucket_bit(7) | routing_key_bucket_bit(11),
            ..Default::default()
        },
    )]
    .into_iter()
    .collect();

    assert_eq!(
        ordered_log_autosplit_candidate(&family, &samples, PARTITION_CONTROLLER_SPLIT_THRESHOLD, 0),
        None
    );
}

#[test]
fn topology_change_sets_cooldown_and_resets_controller_streaks() {
    let mut family = family_with_hot_controller(initial_partition_infos(2));
    family.config.controller.low_streak = 5;

    family.config.note_topology_change(1234);

    assert_eq!(
        family.config.cooldown_until_ms,
        Some(1234 + PARTITION_AUTOSCALE_COOLDOWN_MS)
    );
    assert_eq!(family.config.controller.high_streak, 0);
    assert_eq!(family.config.controller.low_streak, 0);
}
