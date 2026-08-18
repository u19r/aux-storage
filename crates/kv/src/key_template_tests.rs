use crate::key_template::{
    KeyTemplate, PlaceholderBinding, PlaceholderId, UniquePlaceholderBinding,
    VersionstampedWriteConflictPolicy,
};

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

#[test]
fn given_unique_binding_when_template_is_built_then_write_conflict_can_be_omitted() {
    let template = KeyTemplate::unique_placeholder(
        b"streams/example/".to_vec(),
        Vec::new(),
        UniquePlaceholderBinding::new(b"fallback".to_vec()),
    );

    assert_eq!(
        template.versionstamped_write_conflict_policy(),
        VersionstampedWriteConflictPolicy::OmitWriteConflictForUniqueKey
    );
}

#[test]
fn given_shared_binding_when_template_is_built_then_write_conflict_is_preserved() {
    let template = KeyTemplate::placeholder(
        b"streams/example/".to_vec(),
        Vec::new(),
        PlaceholderBinding::shared(1, b"fallback".to_vec()),
    );

    assert_eq!(
        template.versionstamped_write_conflict_policy(),
        VersionstampedWriteConflictPolicy::PreserveWriteConflict
    );
}

#[test]
fn given_manual_unique_id_when_regular_template_is_built_then_write_conflict_is_preserved() {
    let template = KeyTemplate::placeholder(
        b"streams/example/".to_vec(),
        Vec::new(),
        PlaceholderBinding::new(PlaceholderId::Unique(7), b"fallback".to_vec(), [0, 1]),
    );

    assert_eq!(
        template.versionstamped_write_conflict_policy(),
        VersionstampedWriteConflictPolicy::PreserveWriteConflict
    );
}

#[test]
fn given_literal_template_when_policy_is_requested_then_it_is_not_versionstamped() {
    let template = KeyTemplate::literal(b"streams/example/literal".to_vec());

    assert_eq!(
        template.versionstamped_write_conflict_policy(),
        VersionstampedWriteConflictPolicy::NotVersionstamped
    );
}

#[test]
fn given_unique_template_when_prefix_is_replaced_then_write_conflict_policy_is_preserved() {
    let template = KeyTemplate::unique_placeholder(
        b"streams/example/".to_vec(),
        Vec::new(),
        UniquePlaceholderBinding::new(b"fallback".to_vec()),
    );

    let rewritten = template.with_replaced_prefix(b"streams/rewritten/".to_vec());

    assert_eq!(
        rewritten.versionstamped_write_conflict_policy(),
        VersionstampedWriteConflictPolicy::OmitWriteConflictForUniqueKey
    );
}
