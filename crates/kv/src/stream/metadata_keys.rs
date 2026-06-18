use storage_types::{StreamName, UserStreamName};

pub(crate) const STREAMS_PREFIX: &str = "streams/";
pub(crate) const STREAM_CURSORS_PREFIX: &str = "stream-cursors/";

#[must_use]
pub(crate) fn stream_metadata_key(user_stream_name: &UserStreamName) -> Vec<u8> {
    let mut key_parts = STREAMS_PREFIX.as_bytes().to_vec();
    key_parts.extend(user_stream_name.as_str().as_bytes().to_vec());
    key_parts
}

#[must_use]
pub(crate) fn stream_cursor_key(stream_name: &StreamName, cursor_id: &str) -> Vec<u8> {
    let mut key_parts = stream_cursors_prefix(stream_name);
    key_parts.extend(cursor_id.as_bytes());
    key_parts
}

#[must_use]
pub(crate) fn stream_cursors_prefix(stream_name: &StreamName) -> Vec<u8> {
    let mut key_parts = STREAM_CURSORS_PREFIX.as_bytes().to_vec();
    key_parts.extend(stream_name.to_vec());
    key_parts.push(b'/');
    key_parts
}
