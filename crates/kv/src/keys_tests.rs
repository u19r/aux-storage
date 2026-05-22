use std::str::FromStr;

use queue_provider::{MessageId, ReceiptHandle};
use storage_types::{IndexName, StreamName, TableName, TimestampMillis, UserStreamName};

use crate::{
    key_template::{PlaceholderBinding, PlaceholderId},
    keys::{
        gsi_backfill_key, gsi_tombstone_key_from_index_key, gsi_tombstone_prefix_from_name,
        key_message, key_queue_root, key_receipt_checkpoint_uuid, key_receipt_handle,
        key_visibility_pointer, queue_message_prefix, queue_message_storage_key,
        queue_message_template, queue_message_template_with_binding, queue_visibility_storage_key,
        queue_visibility_template, stream_cursor_key, stream_cursors_prefix, stream_metadata_key,
        table_metadata_key, visibility_index_key_path, visibility_key,
    },
    newtypes::MessageVisibilityKey,
};

#[test]
fn table_and_stream_metadata_keys_use_stable_prefixes() {
    let table_name = TableName::new("Orders");
    let index_name = IndexName::new("ByCustomer");
    let user_stream_name = UserStreamName::new("order-events");

    assert_eq!(table_metadata_key(&table_name), b"tables/Orders");
    assert_eq!(
        gsi_backfill_key(&table_name, &index_name),
        b"tables/Orders/gsi-backfill/ByCustomer"
    );
    assert_eq!(
        gsi_tombstone_prefix_from_name(&table_name, &index_name),
        b"Orders/index-tombstone/ByCustomer/data/"
    );
    assert_eq!(
        stream_metadata_key(&user_stream_name),
        b"streams/order-events"
    );
}

#[test]
fn gsi_tombstone_key_reuses_index_key_suffix_under_isolated_prefix() {
    let table_name = TableName::new("Orders");
    let index_name = IndexName::new("ByCustomer");
    let index_key = b"Orders/index/ByCustomer/data/\x00\x02s1\x00\x02s2";

    assert_eq!(
        gsi_tombstone_key_from_index_key(&table_name, &index_name, index_key),
        Some(b"Orders/index-tombstone/ByCustomer/data/\x00\x02s1\x00\x02s2".to_vec())
    );
}

#[test]
fn stream_cursor_keys_are_namespaced_under_stream_with_separator() {
    let stream_name = StreamName::from("order-events");

    assert_eq!(
        stream_cursors_prefix(&stream_name),
        b"stream-cursors/order-events/"
    );
    assert_eq!(
        stream_cursor_key(&stream_name, "worker-a"),
        b"stream-cursors/order-events/worker-a"
    );
}

#[test]
fn queue_keys_preserve_sqs_resource_layout() {
    let queue_url = "https://queue.local/123/orders";
    let message_id =
        MessageId::from_str("0102030405060708090a0b0c").expect("message id should parse");
    let receipt_handle = ReceiptHandle::from("receipt-1");

    assert_eq!(key_queue_root(queue_url), format!("sys/queues/{queue_url}"));
    assert_eq!(
        key_message(queue_url, &message_id),
        format!("sys/queues/{queue_url}/messages/{message_id}")
    );
    assert_eq!(
        key_receipt_handle(queue_url, &receipt_handle),
        format!("sys/queues/{queue_url}/receipt_handles/{receipt_handle}")
    );
    assert_eq!(
        key_receipt_checkpoint_uuid(queue_url, &receipt_handle),
        format!("sys/queues/{queue_url}/receipt_handles/{receipt_handle}/checkpoint")
    );
    assert_eq!(
        key_visibility_pointer(queue_url),
        format!("sys/queues/{queue_url}/visibility-pointer")
    );
}

#[test]
fn queue_message_templates_use_message_prefix_and_versionstamp_binding() {
    let queue_url = "queue-url";
    let message_id =
        MessageId::from_str("0102030405060708090a0b0c").expect("message id should parse");
    let binding = PlaceholderBinding::new(
        PlaceholderId::Shared(7),
        message_id.as_bytes().to_vec(),
        [0x01, 0x02],
    );

    assert_eq!(
        queue_message_prefix(queue_url),
        b"sys/queues/queue-url/messages/"
    );
    assert_eq!(
        queue_message_storage_key(queue_url, &message_id),
        [
            b"sys/queues/queue-url/messages/".as_slice(),
            message_id.as_bytes().as_slice()
        ]
        .concat()
    );
    assert_eq!(
        queue_message_template(queue_url, &message_id).rocks_key(),
        [
            b"sys/queues/queue-url/messages/".as_slice(),
            message_id.as_bytes().as_slice()
        ]
        .concat()
    );
    assert_eq!(
        queue_message_template_with_binding(queue_url, binding)
            .prefix()
            .expect("message template should expose prefix"),
        b"sys/queues/queue-url/messages/"
    );
}

#[test]
fn queue_visibility_keys_are_zero_padded_for_lexicographic_time_ordering() {
    let queue_url = "queue-url";
    let timestamp = TimestampMillis::from_timestamp(42);
    let message_id =
        MessageId::from_str("0102030405060708090a0b0c").expect("message id should parse");
    let binding = PlaceholderBinding::new(
        PlaceholderId::Shared(8),
        message_id.as_bytes().to_vec(),
        [0x03, 0x04],
    );
    let visibility = MessageVisibilityKey(visibility_key(timestamp, &message_id));

    assert_eq!(
        visibility.to_string(),
        "0000000000042:0102030405060708090a0b0c"
    );
    assert_eq!(
        queue_visibility_storage_key(queue_url, timestamp, &message_id),
        [
            b"sys/queues/queue-url/visibility/0000000000042:".as_slice(),
            message_id.as_bytes().as_slice()
        ]
        .concat()
    );
    assert_eq!(
        queue_visibility_template(queue_url, timestamp, binding)
            .prefix()
            .expect("visibility template should expose prefix"),
        b"sys/queues/queue-url/visibility/0000000000042:"
    );
    assert_eq!(
        visibility_index_key_path(queue_url, &visibility),
        "sys/queues/queue-url/visibility/0000000000042:0102030405060708090a0b0c"
    );
}
