use crate::partition::{
    PartitionFamilyKind, PartitionLoadObservation, PartitionLoadSample, PartitionTopologyChange,
    ResolvedPartitionFamily,
};

#[test]
fn given_sustained_diverse_ordered_log_pressure_when_reconciled_then_one_split_is_selected() {
    let mut family =
        ResolvedPartitionFamily::new(PartitionFamilyKind::OrderedLog, std::num::NonZeroU16::MIN);

    for pass in 0..3 {
        let mut observations = vec![PartitionLoadObservation {
            partition_id: 0,
            sample: PartitionLoadSample {
                writes: 1_000,
                routing_key_bucket_bitmap: 0b11,
                ..PartitionLoadSample::default()
            },
            queue_empty: false,
        }];
        let reconciliation = family
            .reconcile(&mut observations, i64::from(pass))
            .expect("ordered-log decision");
        family = reconciliation.family;

        if pass < 2 {
            assert!(reconciliation.topology_change.is_none());
            continue;
        }
        assert!(matches!(
            reconciliation.topology_change,
            Some(PartitionTopologyChange::SplitOrderedLog {
                parent_partition_id: 0,
                left_child_partition_id: 1,
                right_child_partition_id: 2,
            })
        ));
    }

    let encoded = storage_types::storage_serde::to_bytes(&family).expect("persist atomic family");
    let restored: ResolvedPartitionFamily =
        storage_types::storage_serde::from_bytes(&encoded).expect("restore atomic family");
    restored
        .validate()
        .expect("validate persisted split lineage");
    assert_eq!(restored.memberships.len(), 1);
    assert_eq!(restored.memberships[0].parent_partition_id, 0);
    assert_eq!(restored.memberships[0].left_child_partition_id, 1);
    assert_eq!(restored.memberships[0].right_child_partition_id, 2);
    let mut missing_lineage = restored.clone();
    missing_lineage.memberships.clear();
    assert!(missing_lineage.validate().is_err());
    let mut wrong_child = restored.clone();
    wrong_child.memberships[0].right_child_partition_id = 1;
    assert!(wrong_child.validate().is_err());
    let mut wrong_boundary = restored.clone();
    wrong_boundary.memberships[0].boundary = Some(storage_types::StreamItemId::from([9; 12]));
    assert!(wrong_boundary.validate().is_err());
    let mut wrong_count = restored;
    wrong_count.config.partition_count = 1;
    assert!(wrong_count.validate().is_err());
    assert_eq!(family.config.partition_count, 3);
    assert_eq!(family.partitions.len(), 3);
    assert!(family.partitions[0].is_write_closed());
    assert!(family.partitions[1].is_writable());
    assert!(family.partitions[2].is_writable());
}

#[test]
fn given_active_cooldown_when_pressure_is_sustained_then_split_waits_for_expiry() {
    let mut family =
        ResolvedPartitionFamily::new(PartitionFamilyKind::OrderedLog, std::num::NonZeroU16::MIN);
    family.config.cooldown_until_ms = Some(10);
    for now in [1, 2, 3, 9, 10] {
        let mut observations = vec![PartitionLoadObservation {
            partition_id: 0,
            sample: PartitionLoadSample {
                writes: 1_000,
                routing_key_bucket_bitmap: 0b11,
                ..Default::default()
            },
            queue_empty: false,
        }];
        let result = family.reconcile(&mut observations, now).expect("decision");
        assert_eq!(result.topology_change.is_some(), now == 10);
        family = result.family;
    }
    assert_eq!(family.config.family_epoch, 1);
    assert_eq!(family.config.controller.high_streak, 0);
    assert!(family.config.cooldown_until_ms.expect("renewed cooldown") > 10);
}
