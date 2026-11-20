use std::num::NonZeroU16;

use storage_types::{
    AttributeValue, ItemProvenance, KeyAttributes, PairId, PairState, RegionId, RegionalBinding,
    RegionalMarker, RegionalOrder, StorageEnum, StoredWireItem, TableName, TimestampMillis,
    WireItem,
};

use crate::{
    DeleteCause, ItemWriteObservation,
    dynamo_put_item::ObservedWrite,
    regional_write::{WriteAdmission, admit, local_delete, local_upsert},
};

fn region(id: u16) -> RegionId {
    RegionId(NonZeroU16::new(id).unwrap())
}

fn now() -> TimestampMillis {
    TimestampMillis::from_timestamp(1_000)
}

const CLOCK_STAMP: u64 = 1_024_000;

fn order(stamp: u64, id: u16) -> RegionalOrder {
    RegionalOrder::from_parts(stamp, region(id), false)
}

fn binding(state: PairState) -> RegionalBinding {
    RegionalBinding {
        pair: PairId::from_bytes([7; 16]),
        local: region(1),
        peer: region(2),
        peer_name: "peer".to_owned(),
        state,
    }
}

fn table() -> TableName {
    TableName::new("orders")
}

fn provenance(stamp: u64, id: u16) -> ItemProvenance {
    ItemProvenance::regional(order(stamp, id))
}

fn marker(stamp: u64) -> RegionalMarker {
    RegionalMarker {
        order: order(stamp, 2),
        received: false,
    }
}

#[test]
fn given_unpaired_when_admit_then_unpaired() {
    for cause in [DeleteCause::Customer, DeleteCause::Expiration] {
        assert_eq!(
            admit(None, &table(), cause).unwrap(),
            WriteAdmission::Unpaired
        );
    }
}

#[test]
fn given_joining_when_customer_admit_then_resource_not_found() {
    let error = admit(
        Some(&binding(PairState::Joining)),
        &table(),
        DeleteCause::Customer,
    )
    .unwrap_err();
    assert!(
        matches!(error.base_error(), Some(StorageEnum::TableNotFound { .. })),
        "{error:?}"
    );
}

#[test]
fn given_leaving_when_customer_admit_then_validation() {
    let error = admit(
        Some(&binding(PairState::Leaving)),
        &table(),
        DeleteCause::Customer,
    )
    .unwrap_err();
    assert!(
        matches!(error.base_error(), Some(StorageEnum::RawValidation { .. })),
        "{error:?}"
    );
}

#[test]
fn given_joining_or_leaving_when_expiration_admit_then_internal_error() {
    for state in [PairState::Joining, PairState::Leaving] {
        let error = admit(Some(&binding(state)), &table(), DeleteCause::Expiration).unwrap_err();
        assert!(
            matches!(
                error.base_error(),
                Some(StorageEnum::InternalServerError { .. })
            ),
            "{error:?}"
        );
    }
}

#[test]
fn given_active_or_seeding_when_admit_then_paired_local() {
    for state in [PairState::Active, PairState::Seeding] {
        for cause in [DeleteCause::Customer, DeleteCause::Expiration] {
            assert_eq!(
                admit(Some(&binding(state)), &table(), cause).unwrap(),
                WriteAdmission::Paired(region(1))
            );
        }
    }
}

#[test]
fn given_absent_key_when_local_upsert_then_clock_order_and_no_clear() {
    let effect = local_upsert(region(1), None, None, now()).unwrap();
    assert_eq!(effect.order.stamp(), CLOCK_STAMP);
    assert_eq!(effect.order.region(), region(1));
    assert!(!effect.order.is_expiry());
    assert!(!effect.clear_marker);
}

#[test]
fn given_marker_ahead_when_local_upsert_then_marker_plus_one_and_clear() {
    let effect = local_upsert(region(1), None, Some(marker(5_000_000)), now()).unwrap();
    assert_eq!(effect.order.stamp(), 5_000_001);
    assert_eq!(effect.order.region(), region(1));
    assert!(effect.clear_marker);
}

#[test]
fn given_local_provenance_when_local_upsert_then_above_baseline() {
    let default = ItemProvenance::default();
    let effect = local_upsert(region(1), Some(&default), None, now()).unwrap();
    assert_eq!(effect.order.stamp(), CLOCK_STAMP);
    assert!(effect.order > RegionalOrder::baseline(region(1)));
    assert!(!effect.clear_marker);
}

#[test]
fn given_live_item_when_customer_delete_then_marker_above_item() {
    let item = provenance(2_000_000, 2);
    let delete = local_delete(region(1), DeleteCause::Customer, Some(&item), None, now())
        .unwrap()
        .unwrap();
    assert_eq!(delete.marker.order, order(2_000_001, 1));
    assert!(!delete.marker.received);
}

#[test]
fn given_absent_key_when_customer_delete_then_marker_at_clock() {
    let delete = local_delete(region(1), DeleteCause::Customer, None, None, now())
        .unwrap()
        .unwrap();
    assert_eq!(delete.marker.order, order(CLOCK_STAMP, 1));
    assert!(!delete.marker.received);
    let above_marker = local_delete(
        region(1),
        DeleteCause::Customer,
        None,
        Some(marker(9_000_000)),
        now(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(above_marker.marker.order, order(9_000_001, 1));
}

#[test]
fn given_live_item_when_expiration_delete_then_expiry_child() {
    let item = provenance(7, 2);
    let expected = order(7, 2).expiry_child().unwrap();
    for local in [region(1), region(2)] {
        let delete = local_delete(local, DeleteCause::Expiration, Some(&item), None, now())
            .unwrap()
            .unwrap();
        assert_eq!(delete.marker.order, expected);
        assert!(!delete.marker.received);
    }
}

#[test]
fn given_absent_item_when_expiration_delete_then_none() {
    assert!(
        local_delete(region(1), DeleteCause::Expiration, None, None, now())
            .unwrap()
            .is_none()
    );
    assert!(
        local_delete(
            region(1),
            DeleteCause::Expiration,
            None,
            Some(marker(5)),
            now()
        )
        .unwrap()
        .is_none()
    );
}

fn key() -> KeyAttributes {
    KeyAttributes::from_iter([("pk".into(), AttributeValue::S("a".into()))])
}

fn live() -> ItemWriteObservation {
    ItemWriteObservation::Live {
        stored: Box::new(StoredWireItem {
            item: WireItem::dynamo_json(br#"{"pk":{"S":"a"}}"#.to_vec()),
            indexers: Vec::new(),
        }),
        provenance: provenance(5, 1),
        ttl_epoch: None,
    }
}

#[test]
fn given_live_observation_with_marker_when_with_marker_then_internal_error() {
    assert!(live().with_marker(Some(marker(9))).is_err());
    assert!(matches!(
        live().with_marker(None).unwrap(),
        ItemWriteObservation::Live { .. }
    ));
}

#[test]
fn given_absent_with_marker_then_deleted() {
    let observed = ItemWriteObservation::Absent
        .with_marker(Some(marker(9)))
        .unwrap();
    assert!(
        matches!(observed, ItemWriteObservation::Deleted { marker: found } if found == marker(9))
    );
    let again = observed.with_marker(Some(marker(10))).unwrap();
    assert!(
        matches!(again, ItemWriteObservation::Deleted { marker: found } if found == marker(10))
    );
    assert!(matches!(
        ItemWriteObservation::Absent.with_marker(None).unwrap(),
        ItemWriteObservation::Absent
    ));
}

#[test]
fn given_deleted_when_observed_write_then_no_item_and_marker() {
    let observed = ItemWriteObservation::Deleted { marker: marker(9) };
    let write = ObservedWrite::try_new(observed, &key()).unwrap();
    assert!(write.item.is_none());
    assert!(write.indexers.is_empty());
    assert!(write.provenance.is_none());
    assert_eq!(write.marker, Some(marker(9)));
    let absent = ObservedWrite::try_new(ItemWriteObservation::Absent, &key()).unwrap();
    assert!(absent.marker.is_none());
    let live = ObservedWrite::try_new(live(), &key()).unwrap();
    assert!(live.item.is_some() && live.marker.is_none());
    assert_eq!(live.provenance, Some(provenance(5, 1)));
}
