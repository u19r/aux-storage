use std::str::FromStr;

use uuid::Uuid;

use crate::{MessageId, ReceiptHandle, constants::MESSAGE_ID_VERSIONSTAMP_LEN};

#[test]
fn message_id_round_trips_between_bytes_hex_and_display() {
    let bytes = [0xAB; MESSAGE_ID_VERSIONSTAMP_LEN];
    let message_id = MessageId::from_bytes(bytes);

    let hex = message_id.to_hex();
    let parsed = MessageId::from_str(&hex).expect("message id hex should parse");

    assert_eq!(message_id.as_bytes(), &bytes);
    assert_eq!(message_id.to_string(), hex);
    assert_eq!(parsed, message_id);
    assert_eq!(String::from(&message_id), hex);
}

#[test]
fn message_id_rejects_invalid_hex_and_length() {
    assert!("not-hex".parse::<MessageId>().is_err());
    assert!("abcd".parse::<MessageId>().is_err());
}

#[test]
fn message_id_lossy_from_string_defaults_invalid_input_to_zero_id() {
    assert_eq!(MessageId::from("not-a-message-id"), MessageId::default());
    assert_eq!(
        MessageId::from("not-a-message-id".to_string()),
        MessageId::default()
    );
}

#[test]
fn message_id_from_uuid_uses_versionstamp_prefix_bytes() {
    let uuid = Uuid::parse_str("018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd").expect("uuid should parse");
    let message_id = MessageId::from_uuid(uuid);
    let mut expected = [0; MESSAGE_ID_VERSIONSTAMP_LEN];
    expected.copy_from_slice(&uuid.as_bytes()[..MESSAGE_ID_VERSIONSTAMP_LEN]);

    assert_eq!(message_id.as_bytes(), &expected);
}

#[test]
fn receipt_handle_encodes_timestamp_and_uuid_payload() {
    let uuid = Uuid::parse_str("018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd").expect("uuid should parse");
    let handle = ReceiptHandle::new(42, uuid);

    assert_eq!(handle.to_string(), *handle);
    assert_eq!(
        ReceiptHandle::from(handle.to_string().as_str()).to_string(),
        handle.to_string()
    );
}
