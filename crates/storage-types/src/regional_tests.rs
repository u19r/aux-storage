use std::num::NonZeroU16;

use crate::{RegionId, RegionalMarker, RegionalOrder, TimestampMillis};

fn region(id: u16) -> RegionId {
    RegionId(NonZeroU16::new(id).unwrap())
}

fn order(stamp: u64, id: u16) -> RegionalOrder {
    RegionalOrder::from_parts(stamp, region(id), false)
}

#[test]
fn given_orders_when_compared_then_stamp_region_expiry_order() {
    assert!(order(5, 1) < order(5, 2));
    assert!(order(5, 2) < order(6, 1));
    let child = order(5, 1).expiry_child().unwrap();
    assert!(order(5, 1) < child);
    assert!(child < order(6, 1));
    assert!(child.is_expiry());
    assert_eq!((child.stamp(), child.region()), (5, region(1)));
}

#[test]
fn given_order_when_encoded_then_bytes_sort_as_ord_and_decode_round_trips() {
    let orders = [
        order(5, 2),
        order(5, 1).expiry_child().unwrap(),
        order(300, 1),
        order(5, 1),
        order(u64::MAX, 65_535),
    ];
    let mut by_ord = orders.to_vec();
    by_ord.sort();
    let mut by_bytes = orders.to_vec();
    by_bytes.sort_by_key(|order| order.encode());
    assert_eq!(by_ord, by_bytes);
    for order in orders {
        let bytes = order.encode();
        assert_eq!(bytes.len(), RegionalOrder::ENCODED_BYTES);
        assert_eq!(RegionalOrder::decode(&bytes).unwrap(), order);
    }
    for marker in [false, true].map(|received| RegionalMarker {
        order: order(9, 2),
        received,
    }) {
        let bytes = marker.encode();
        assert_eq!(bytes.len(), RegionalMarker::ENCODED_BYTES);
        assert_eq!(RegionalMarker::decode(&bytes).unwrap(), marker);
    }
}

#[test]
fn given_invalid_bytes_when_decoded_then_error() {
    let valid = order(5, 1).encode();
    assert!(RegionalOrder::decode(&valid[..10]).is_err());
    let mut zero_region = valid;
    zero_region[8..10].copy_from_slice(&0_u16.to_be_bytes());
    assert!(RegionalOrder::decode(&zero_region).is_err());
    let mut bad_expiry = valid;
    bad_expiry[10] = 2;
    assert!(RegionalOrder::decode(&bad_expiry).is_err());
    let mut bad_flag = RegionalMarker {
        order: order(5, 1),
        received: false,
    }
    .encode();
    bad_flag[11] = 2;
    assert!(RegionalMarker::decode(&bad_flag).is_err());
    assert!(RegionalMarker::decode(&valid).is_err());
}

#[test]
fn given_winner_ahead_of_clock_when_next_local_then_winner_plus_one() {
    let now = TimestampMillis::from_timestamp(1);
    let next =
        |winner: Option<RegionalOrder>| RegionalOrder::next_local(now, winner, region(1)).unwrap();
    assert_eq!(next(Some(order(5000, 2))).stamp(), 5001);
    assert_eq!(next(None).stamp(), 1024);
    let above_higher_region = next(Some(order(1024, 9)));
    assert_eq!(above_higher_region.stamp(), 1025);
    assert_eq!(above_higher_region.region(), region(1));
    assert!(!above_higher_region.is_expiry());
}

#[test]
fn given_expiry_order_when_expiry_child_then_error() {
    let child = order(5, 1).expiry_child().unwrap();
    assert!(child.expiry_child().is_err());
}

#[test]
fn given_region_when_baseline_then_stamp_zero_and_not_expiry() {
    let baseline = RegionalOrder::baseline(region(3));
    assert_eq!(
        (baseline.stamp(), baseline.region(), baseline.is_expiry()),
        (0, region(3), false)
    );
    assert!(baseline < order(1, 1));
}
