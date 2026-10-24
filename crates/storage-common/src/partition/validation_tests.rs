use storage_types::StreamItemId;

use crate::partition::{
    PartitionFamilyKind, PartitionParentChildren, PartitionRoutingStrategy, ResolvedPartitionFamily,
};

#[test]
fn given_partition_enums_when_serialized_then_wire_names_match_domain_text() {
    assert_eq!(
        serde_json::to_string(&PartitionFamilyKind::OrderedLog).expect("serialize family kind"),
        r#""ordered-log""#
    );
    assert_eq!(
        serde_json::to_string(&PartitionFamilyKind::StandardQueue).expect("serialize family kind"),
        r#""standard-queue""#
    );
    assert_eq!(
        serde_json::to_string(&PartitionRoutingStrategy::HashKeyOrdered)
            .expect("serialize routing strategy"),
        r#""hash-key-ordered""#
    );
    assert_eq!(
        serde_json::to_string(&PartitionRoutingStrategy::StandardQueue)
            .expect("serialize routing strategy"),
        r#""standard-queue""#
    );
}

fn split_family() -> ResolvedPartitionFamily {
    let mut family =
        ResolvedPartitionFamily::new(PartitionFamilyKind::OrderedLog, std::num::NonZeroU16::MIN);
    family.partitions[0]
        .mark_write_closed()
        .expect("close parent");
    family
        .partitions
        .push(crate::partition::PartitionInfo::new_open(
            1,
            1,
            0,
            Some(1_u64 << 63),
        ));
    family
        .partitions
        .push(crate::partition::PartitionInfo::new_open(
            2,
            2,
            1_u64 << 63,
            None,
        ));
    family.memberships.push(PartitionParentChildren {
        parent_partition_id: 0,
        left_child_partition_id: 1,
        right_child_partition_id: 2,
        boundary: None,
    });
    family.refresh_partition_count();
    family.validate().expect("complete atomic split family");
    family
}

#[test]
fn given_missing_split_member_when_family_is_validated_then_no_change_is_produced() {
    for missing_id in 0..3 {
        let mut candidate = split_family();
        candidate
            .partitions
            .retain(|member| member.partition_id != missing_id);
        assert!(candidate.validate().is_err(), "missing member {missing_id}");
    }
}

#[test]
fn given_duplicate_membership_or_identity_when_family_is_validated_then_corruption_is_rejected() {
    let mut candidate = split_family();
    candidate.memberships.push(PartitionParentChildren {
        parent_partition_id: 0,
        left_child_partition_id: 1,
        right_child_partition_id: 2,
        boundary: None,
    });
    assert!(candidate.validate().is_err());
    let mut candidate = split_family();
    candidate.memberships[0].right_child_partition_id = 1;
    assert!(candidate.validate().is_err());
    let mut candidate = split_family();
    candidate.partitions[2].partition_id = 1;
    assert!(candidate.validate().is_err());
    let mut candidate = split_family();
    candidate.partitions[2].placement_slot = 1;
    assert!(candidate.validate().is_err());
}

#[test]
fn given_gap_overlap_or_incomplete_coverage_when_family_is_validated_then_corruption_is_rejected() {
    for start in [0, (1_u64 << 63) - 1, (1_u64 << 63) + 1] {
        let mut candidate = split_family();
        candidate.partitions[2].hash_start_inclusive = start;
        assert!(candidate.validate().is_err(), "invalid child start {start}");
    }
    let mut candidate = split_family();
    candidate.partitions[0].hash_start_inclusive = 1;
    candidate.partitions[1].hash_start_inclusive = 1;
    assert!(candidate.validate().is_err());
    let mut candidate = split_family();
    candidate.partitions[0].hash_end_exclusive = Some(u64::MAX);
    candidate.partitions[2].hash_end_exclusive = Some(u64::MAX);
    assert!(candidate.validate().is_err());
}

#[test]
fn given_wrong_member_state_or_split_boundary_when_family_is_validated_then_corruption_is_rejected()
{
    for member in 0..3 {
        let mut candidate = split_family();
        candidate.partitions[member].state = crate::partition::PartitionState::Draining;
        assert!(candidate.validate().is_err());
        let mut candidate = split_family();
        if member == 0 {
            candidate.partitions[member].sealed_after_id = Some(StreamItemId::from([9; 12]));
        } else {
            candidate.partitions[member].opened_after_id = Some(StreamItemId::from([9; 12]));
        }
        assert!(candidate.validate().is_err());
    }
}

#[test]
fn given_wrong_family_routing_or_open_count_when_family_is_validated_then_corruption_is_rejected() {
    let mut candidate = split_family();
    candidate.config.family_kind = PartitionFamilyKind::StandardQueue;
    assert!(candidate.validate().is_err());
    let mut candidate = split_family();
    candidate.config.routing_strategy = PartitionRoutingStrategy::StandardQueue;
    assert!(candidate.validate().is_err());
    let mut candidate = split_family();
    candidate.config.max_open_partitions = 1;
    assert!(candidate.validate().is_err());
    let mut candidate = split_family();
    candidate.config.min_open_partitions = 3;
    assert!(candidate.validate().is_err());
}

#[test]
fn given_child_shared_by_two_parents_when_family_is_validated_then_corruption_is_rejected() {
    let mut candidate = split_family();
    candidate.partitions[1].partition_id = 2;
    candidate.partitions[1].placement_slot = 2;
    candidate.partitions[2].partition_id = 3;
    candidate.partitions[2].placement_slot = 3;
    let mut second_parent = crate::partition::PartitionInfo::new_open(1, 1, 0, None);
    second_parent
        .mark_write_closed()
        .expect("close second parent");
    candidate.partitions.push(second_parent);
    candidate.refresh_partition_count();
    candidate.memberships = (0..2)
        .map(|parent_partition_id| PartitionParentChildren {
            parent_partition_id,
            left_child_partition_id: 2,
            right_child_partition_id: 3,
            boundary: None,
        })
        .collect();
    assert!(candidate.validate().is_err());
}
