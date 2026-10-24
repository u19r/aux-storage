use super::key_template::{
    KeyTemplate, PlaceholderBinding, PlaceholderId, UniquePlaceholderBinding,
};

#[test]
fn stream_plain_key_matches_previous_layout() {
    let binding = PlaceholderBinding::new(
        PlaceholderId::Shared(1),
        b"00000000-0000-0000-0000-000000000000".to_vec(),
        [0x12, 0x34],
    );
    let template = KeyTemplate::placeholder(b"streams/example/".to_vec(), Vec::new(), binding);
    let plain = template.materialized_key();
    assert_eq!(
        plain,
        b"streams/example/00000000-0000-0000-0000-000000000000".to_vec()
    );
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
    let key = template.materialized_key();
    assert_eq!(
        key,
        b"sys/queues/url/messages/00000000-0000-0000-0000-000000000000".to_vec()
    );
}

#[test]
fn given_unique_binding_when_template_is_built_then_template_is_unique() {
    let template = KeyTemplate::unique_placeholder(
        b"streams/example/".to_vec(),
        Vec::new(),
        UniquePlaceholderBinding::new(b"fallback".to_vec()),
    );

    assert!(matches!(template, KeyTemplate::UniquePlaceholder(_)));
}

#[test]
fn given_shared_binding_when_template_is_built_then_template_is_shared() {
    let template = KeyTemplate::placeholder(
        b"streams/example/".to_vec(),
        Vec::new(),
        PlaceholderBinding::shared(1, b"fallback".to_vec()),
    );

    assert!(matches!(template, KeyTemplate::Placeholder(_)));
}

#[test]
fn given_manual_unique_id_when_regular_template_is_built_then_template_is_not_unique() {
    let template = KeyTemplate::placeholder(
        b"streams/example/".to_vec(),
        Vec::new(),
        PlaceholderBinding::new(PlaceholderId::Unique(7), b"fallback".to_vec(), [0, 1]),
    );

    assert!(matches!(template, KeyTemplate::Placeholder(_)));
}

#[test]
fn given_literal_key_when_template_is_built_then_template_is_literal() {
    let template = KeyTemplate::literal(b"streams/example/literal".to_vec());

    assert!(matches!(template, KeyTemplate::Literal(_)));
}

#[test]
fn given_unique_template_when_prefix_is_replaced_then_unique_kind_is_preserved() {
    let template = KeyTemplate::unique_placeholder(
        b"streams/example/".to_vec(),
        Vec::new(),
        UniquePlaceholderBinding::new(b"fallback".to_vec()),
    );

    let rewritten = template.with_replaced_prefix(b"streams/rewritten/".to_vec());

    assert!(matches!(rewritten, KeyTemplate::UniquePlaceholder(_)));
}
