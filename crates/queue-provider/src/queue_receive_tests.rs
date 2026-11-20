//! Stored size of a message record (W7-A).

use std::collections::HashMap;

use storage_types::TimestampMillis;

use crate::{
    MessageAttributeValue, QueueIncarnation, QueueMessage, QueueMessageRecord,
    queue_receive::stored_record_bytes,
};

fn record(index: usize, body: String) -> QueueMessageRecord {
    QueueMessageRecord {
        incarnation: QueueIncarnation::new(),
        message: QueueMessage {
            queue_url: index.to_string(),
            body,
            ..QueueMessage::default()
        },
        expires_at: TimestampMillis::from_timestamp(1_000),
        checkpoint_data: None,
        dead_letter_source_arn: None,
    }
}

#[test]
fn given_binary_attribute_when_sized_then_stored_base64_length_counts() {
    let mut sized = record(0, "x".to_owned());
    sized.message.message_attributes = Some(HashMap::from([(
        "b".to_owned(),
        MessageAttributeValue {
            string_value: None,
            binary_value: Some("AAAA".to_owned()),
            data_type: "Binary".to_owned(),
        },
    )]));
    assert_eq!(stored_record_bytes(&sized), 1 + 1 + 6 + 4);
}
