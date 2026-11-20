use std::num::NonZeroU16;

use crate::{ItemProvenance, RegionId, RegionalOrder};

fn order() -> RegionalOrder {
    RegionalOrder::from_parts(1_024_000, RegionId(NonZeroU16::new(2).unwrap()), false)
}

#[test]
fn given_default_provenance_when_stored_then_empty_bytes_and_local() {
    let value = ItemProvenance::default();
    assert!(value.as_bytes().is_empty());
    assert!(value.is_local());
    assert_eq!(value.order(), None);
    assert_eq!(ItemProvenance::decode(&[]).unwrap(), value);
}

#[test]
fn given_regional_provenance_when_stored_then_eleven_bytes_and_round_trip() {
    let value = ItemProvenance::regional(order());
    assert_eq!(value.as_bytes().len(), RegionalOrder::ENCODED_BYTES);
    assert_eq!(value.as_bytes(), order().encode());
    assert!(!value.is_local());
    let decoded = ItemProvenance::decode(value.as_bytes()).unwrap();
    assert_eq!(decoded, value);
    assert_eq!(decoded.order(), Some(order()));
}

#[test]
fn given_other_lengths_when_decoded_then_error() {
    for length in [1, 10, 12, 23] {
        assert!(ItemProvenance::decode(&vec![0; length]).is_err());
    }
}

#[test]
fn given_provenance_when_serialized_as_json_then_null_or_order_object_and_round_trip() {
    let local = ItemProvenance::default();
    assert_eq!(serde_json::to_vec(&local).unwrap(), b"null");
    assert_eq!(
        serde_json::from_slice::<ItemProvenance>(b"null").unwrap(),
        local
    );

    let regional = ItemProvenance::regional(order());
    let json = serde_json::to_value(regional).unwrap();
    assert!(json.is_object());
    assert_eq!(json, serde_json::to_value(order()).unwrap());
    assert_eq!(
        serde_json::from_value::<ItemProvenance>(json).unwrap(),
        regional
    );
}
