use storage_types::{StreamName, UserStreamName};

use crate::stream::metadata_keys::{stream_cursor_key, stream_cursors_prefix, stream_metadata_key};

#[test]
fn stream_metadata_keys_use_stable_prefixes() {
    let user_stream_name = UserStreamName::new("order-events");

    assert_eq!(
        stream_metadata_key(&user_stream_name),
        b"streams/order-events"
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
