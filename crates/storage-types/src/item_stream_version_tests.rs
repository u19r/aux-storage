use crate::{ItemStreamVersion, StreamItemId};

#[test]
fn item_stream_version_round_trips_key_bytes() {
    let version = ItemStreamVersion::new(42);
    let bytes = version.to_be_bytes();

    assert_eq!(ItemStreamVersion::from(bytes), version);
    assert_eq!(ItemStreamVersion::try_from(bytes.as_slice()), Ok(version));
    assert_eq!(Vec::<u8>::from(version), bytes.to_vec());
    assert_eq!(version.to_string(), "42");
}

#[test]
fn item_stream_version_big_endian_bytes_preserve_numeric_order() {
    let versions = [
        ItemStreamVersion::new(1),
        ItemStreamVersion::new(2),
        ItemStreamVersion::new(255),
        ItemStreamVersion::new(256),
        ItemStreamVersion::new(u32::MAX as u64),
        ItemStreamVersion::new(u64::from(u32::MAX) + 1),
    ];

    for pair in versions.windows(2) {
        assert!(pair[0] < pair[1]);
        assert!(pair[0].to_be_bytes() < pair[1].to_be_bytes());
    }
}

#[test]
fn item_stream_version_increment_rejects_overflow() {
    assert_eq!(
        ItemStreamVersion::new(41).checked_increment(),
        Some(ItemStreamVersion::new(42))
    );
    assert_eq!(ItemStreamVersion::new(u64::MAX).checked_increment(), None);
}

#[test]
fn item_stream_version_rejects_invalid_key_lengths() {
    assert!(ItemStreamVersion::try_from([1, 2, 3].as_slice()).is_err());
}

#[test]
fn item_stream_version_can_be_seeded_from_legacy_stream_item_id_during_transition() {
    let mut bytes = [0u8; 12];
    bytes[4..].copy_from_slice(&42u64.to_be_bytes());

    assert_eq!(
        ItemStreamVersion::from(StreamItemId::from(bytes)),
        ItemStreamVersion::new(42)
    );
}
