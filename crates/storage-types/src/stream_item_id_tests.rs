use uuid::Uuid;

use crate::{ItemStreamVersion, StreamItemId};

#[test]
fn increment_item_id() {
    let mut bytes = [0u8; 12];
    bytes[11] = 0x0c;
    let id1 = StreamItemId::from(bytes);
    let id2 = id1.increment();
    assert_eq!(id2.as_bytes()[11], 0x0d);

    let mut wrap_bytes = [0u8; 12];
    wrap_bytes[10] = 0xFF;
    wrap_bytes[11] = 0xFF;
    let wrap_id = StreamItemId::from(wrap_bytes);
    let wrapped = wrap_id.increment();
    assert_eq!(wrapped.as_bytes()[10], 0x00);
    assert_eq!(wrapped.as_bytes()[11], 0x00);

    let mut carry_bytes = [0u8; 12];
    carry_bytes[9] = 0x10;
    carry_bytes[10] = 0xFF;
    carry_bytes[11] = 0xFF;
    let carry_id = StreamItemId::from(carry_bytes);
    let incremented = carry_id.increment();
    assert_eq!(incremented.as_bytes()[9], 0x11);
    assert_eq!(incremented.as_bytes()[10], 0x00);
    assert_eq!(incremented.as_bytes()[11], 0x00);
}

#[test]
fn stream_item_id_round_trips_between_hex_json_and_key_bytes() {
    let bytes = [0xAB; 12];
    let id = StreamItemId::from(bytes);

    let json = serde_json::to_string(&id).expect("stream item id should serialize");
    let decoded: StreamItemId =
        serde_json::from_str(&json).expect("stream item id should deserialize");
    let from_key = StreamItemId::try_from(bytes.as_slice()).expect("key bytes should parse");

    assert_eq!(json, format!("\"{}\"", id.to_hex()));
    assert_eq!(decoded, id);
    assert_eq!(from_key, id);
    assert_eq!(Vec::<u8>::from(id), bytes.to_vec());
    assert_eq!(id.to_string(), "abababababababababababab");
}

#[test]
fn stream_item_id_rejects_invalid_hex_and_key_lengths() {
    assert!("not-hex".parse::<StreamItemId>().is_err());
    assert!("abcd".parse::<StreamItemId>().is_err());
    assert!(StreamItemId::try_from([1, 2, 3].as_slice()).is_err());
}

#[test]
fn stream_item_id_from_uuid_uses_ordered_uuid_prefix() {
    let uuid = Uuid::parse_str("018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd").expect("uuid should parse");
    let id = StreamItemId::from_uuid(uuid);
    let mut expected = [0; 12];
    expected.copy_from_slice(&uuid.as_bytes()[..12]);

    assert_eq!(id.as_bytes(), &expected);
}

#[test]
fn stream_item_id_encodes_item_stream_version_for_transition_boundary() {
    let version = ItemStreamVersion::new(42);
    let id = StreamItemId::from(version);

    assert_eq!(&id.as_bytes()[..4], &[0; 4]);
    assert_eq!(ItemStreamVersion::from(id), version);
}
