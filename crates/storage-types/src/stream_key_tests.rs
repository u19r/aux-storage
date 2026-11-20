use crate::StreamKey;

#[test]
fn stream_key_debug_renders_lossy_bytes_for_operator_diagnostics() {
    let key = StreamKey::new(b"stream/key");

    assert_eq!(format!("{key:?}"), "StreamKey(stream/key)");
}
