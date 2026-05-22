use std::str::FromStr;

use queue_provider::{MessageId, QueueError, QueueInternalKind};
use storage_types::{SerializesToKey, TableName, TimestampMillis};

use crate::newtypes::{MessageVisibilityKey, TablePageKey};

#[test]
fn message_visibility_key_extracts_sort_timestamp_and_message_id() {
    let message_id =
        MessageId::from_str("0102030405060708090a0b0c").expect("message id should parse");
    let key = MessageVisibilityKey(format!("1700000000123:{message_id}"));

    assert_eq!(
        key.get_timestamp().expect("timestamp should parse"),
        TimestampMillis::from_timestamp(1_700_000_000_123)
    );
    assert_eq!(
        key.get_message_id().expect("message id should parse"),
        message_id
    );
    assert_eq!(key.to_string(), format!("1700000000123:{message_id}"));
}

#[test]
fn message_visibility_key_rejects_values_without_timestamp_or_message_id() {
    for invalid in ["", "1700000000123", "not-a-number:0102030405060708090a0b0c"] {
        let key = MessageVisibilityKey(invalid.to_string());
        let error = key
            .get_timestamp()
            .and_then(|_| key.get_message_id())
            .expect_err("malformed visibility keys must fail");

        assert_invalid_visibility_key(error);
    }
}

#[test]
fn min_message_visibility_key_sorts_from_epoch_for_default_message_id() {
    let min = MessageVisibilityKey::min();

    assert_eq!(
        min.get_timestamp().expect("timestamp should parse"),
        TimestampMillis::from(0)
    );
    assert_eq!(
        min.get_message_id().expect("message id should parse"),
        MessageId::default()
    );
}

#[test]
fn table_page_key_serializes_to_table_metadata_keyspace() {
    let table_name = TableName::new("Orders");
    let from_table_name = TablePageKey::from(table_name)
        .serialize_to_bytes()
        .expect("page key should serialize");
    let from_string = TablePageKey::from("Invoices")
        .serialize_to_bytes()
        .expect("page key should serialize");

    assert_eq!(from_table_name, b"tables/Orders");
    assert_eq!(from_string, b"tables/Invoices");
}

fn assert_invalid_visibility_key(error: QueueError) {
    assert!(
        matches!(
            error,
            QueueError::Internal {
                kind: QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
}
