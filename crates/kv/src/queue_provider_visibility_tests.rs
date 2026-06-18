use std::str::FromStr;

use queue_provider::MessageId;
use storage_types::TimestampMillis;

use crate::{newtypes::MessageVisibilityKey, queue_provider::visibility_key};

#[test]
fn queue_visibility_keys_are_zero_padded_for_lexicographic_time_ordering() {
    let timestamp = TimestampMillis::from_timestamp(42);
    let message_id =
        MessageId::from_str("0102030405060708090a0b0c").expect("message id should parse");
    let visibility = MessageVisibilityKey(visibility_key(timestamp, &message_id));

    assert_eq!(
        visibility.to_string(),
        "0000000000042:0102030405060708090a0b0c"
    );
}
