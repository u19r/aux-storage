use std::str::FromStr;

use uuid::Uuid;

use crate::{InvalidMessageId, MessageId, ReceiptHandle, constants::MESSAGE_ID_LEN};

#[test]
fn message_id_round_trips_between_bytes_text_and_display() {
    let bytes = [0xAB; MESSAGE_ID_LEN];
    let message_id = MessageId::from_bytes(bytes);
    let text = "abababab-abab-abab-abab-abababababab";

    let parsed = MessageId::from_str(text).expect("message id text should parse");
    let json = serde_json::to_string(&message_id).expect("message id should serialize");
    let from_json: MessageId = serde_json::from_str(&json).expect("message id should deserialize");

    assert_eq!(message_id.as_bytes(), &bytes);
    assert_eq!(message_id.to_string(), text);
    assert_eq!(parsed, message_id);
    assert_eq!(String::from(&message_id), text);
    assert_eq!(json, format!("\"{text}\""));
    assert_eq!(from_json, message_id);
}

#[test]
fn message_id_rejects_other_forms() {
    for text in [
        "not-hex",
        "abcd",
        "018f1f612a6f7ac3b9b67f65",
        "018f1f612a6f7ac3b9b67f65bb2d91fd",
        "{018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd}",
        "urn:uuid:018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd",
        "018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fz",
    ] {
        assert_eq!(text.parse::<MessageId>(), Err(InvalidMessageId), "{text}");
    }

    let upper = "018F1F61-2A6F-7AC3-B9B6-7F65BB2D91FD"
        .parse::<MessageId>()
        .expect("uppercase hyphenated form should parse");
    assert_eq!(upper.to_string(), "018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd");
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
fn message_id_from_uuid_keeps_all_sixteen_bytes() {
    let uuid = Uuid::parse_str("018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd").expect("uuid should parse");
    let message_id = MessageId::from_uuid(uuid);

    assert_eq!(message_id.as_bytes(), uuid.as_bytes());
    assert_eq!(message_id.to_string(), uuid.hyphenated().to_string());
}

#[test]
fn message_id_random_is_uuid_v7() {
    let uuid = Uuid::from_bytes(*MessageId::random().as_bytes());

    assert_eq!(uuid.get_version_num(), 7);
}

#[test]
fn receipt_handle_encodes_timestamp_and_uuid_payload() {
    let uuid = Uuid::parse_str("018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd").expect("uuid should parse");
    let handle = ReceiptHandle::new(42, uuid, "0123456789abcdef0123456789abcdef:orders");

    assert_eq!(handle.to_string(), *handle);
    assert_eq!(
        ReceiptHandle::from(handle.to_string().as_str()).to_string(),
        handle.to_string()
    );
}
