use crate::key_template::{KeyTemplate, PlaceholderBinding, PlaceholderId};

#[test]
fn stream_plain_key_matches_previous_layout() {
    let binding = PlaceholderBinding::new(
        PlaceholderId::Shared(1),
        b"00000000-0000-0000-0000-000000000000".to_vec(),
        [0x12, 0x34],
    );
    let template = KeyTemplate::placeholder(b"streams/example/".to_vec(), Vec::new(), binding);
    let plain = template.rocks_key();
    assert_eq!(
        plain,
        b"streams/example/00000000-0000-0000-0000-000000000000".to_vec()
    );
}

#[test]
fn stream_foundationdb_key_encodes_placeholder_and_offset() {
    let binding = PlaceholderBinding::new(
        PlaceholderId::Shared(1),
        b"00000000-0000-0000-0000-000000000000".to_vec(),
        [0x12, 0x34],
    );
    let template = KeyTemplate::placeholder(b"streams/example/".to_vec(), Vec::new(), binding);
    let encoded = template.foundationdb_key().expect("should encode");

    // final 4 bytes store offset to placeholder, which should equal prefix len
    let offset_bytes = &encoded[encoded.len() - 4..];
    let offset = u32::from_le_bytes(offset_bytes.try_into().unwrap());
    assert_eq!(offset as usize, b"streams/example/".len());

    // bytes at offset..offset+10 are placeholder markers
    for byte in &encoded[offset as usize..offset as usize + 10] {
        assert_eq!(*byte, 0xFF);
    }

    // Following bytes should include user bytes 0x12, 0x34
    let user_start = offset as usize + 10;
    assert_eq!(&encoded[user_start..user_start + 2], &[0x12, 0x34]);
}

#[test]
fn queue_plain_key_still_uses_message_id() {
    let binding = PlaceholderBinding::new(
        PlaceholderId::Shared(42),
        b"00000000-0000-0000-0000-000000000000".to_vec(),
        [0xAB, 0xCD],
    );
    let template =
        KeyTemplate::placeholder(b"sys/queues/url/messages/".to_vec(), Vec::new(), binding);
    let key = template.rocks_key();
    assert_eq!(
        key,
        b"sys/queues/url/messages/00000000-0000-0000-0000-000000000000".to_vec()
    );
}
