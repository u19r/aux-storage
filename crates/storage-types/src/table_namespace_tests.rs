use std::str::FromStr;

use crate::{
    StorageEnum, StorageError, TableNamespace, TableNamespaceParseError, WireAttributeDecode,
};

#[test]
fn table_namespace_seed_generates_stable_canonical_namespace() {
    let first = TableNamespace::from_seed("orders");
    let second = TableNamespace::from_seed("orders");

    assert_eq!(first, second);
    assert!(first.as_str().starts_with(TableNamespace::PREFIX));
    assert_eq!(first.as_str().len(), TableNamespace::canonical_length());
    assert_eq!(
        first.storage_key(),
        first.as_str().strip_prefix(TableNamespace::PREFIX).unwrap()
    );
}

#[test]
fn table_namespace_parse_accepts_system_and_normalizes_crockford_suffix() {
    let system = TableNamespace::parse("SYSTEM").expect("system namespace should parse");
    let namespace = TableNamespace::from_seed("orders");
    let lowercase = namespace.as_str().to_ascii_lowercase();
    let without_prefix = namespace.storage_key().to_ascii_lowercase();

    assert_eq!(system, TableNamespace::system());
    assert_eq!(system.storage_key(), "system");
    assert_eq!(
        TableNamespace::parse(&lowercase).expect("namespace should parse"),
        namespace
    );
    assert_eq!(
        TableNamespace::parse(&without_prefix).expect("namespace suffix should parse"),
        namespace
    );
    assert_eq!(
        TableNamespace::from_str(namespace.as_str()).expect("namespace should parse"),
        namespace
    );
    assert_eq!(namespace.clone().into_string(), namespace.to_string());
}

#[test]
fn table_namespace_rejects_invalid_length_and_characters() {
    let length_error = TableNamespace::parse("ns_123").expect_err("short namespace must fail");
    let character_error =
        TableNamespace::parse("ns_ZZZZZZZZZZZZZZZZZZZZZZZ").expect_err("bad range must fail");

    assert!(
        matches!(
            length_error,
            TableNamespaceParseError::InvalidLength {
                expected: TableNamespace::KEY_LENGTH,
                ..
            }
        ),
        "unexpected error: {length_error:?}"
    );
    assert!(
        matches!(
            character_error,
            TableNamespaceParseError::InvalidCharacters { .. }
        ),
        "unexpected error: {character_error:?}"
    );
}

#[test]
fn table_namespace_wire_decode_returns_typed_storage_errors() {
    let namespace = TableNamespace::from_seed("orders");
    let decoded = TableNamespace::decode(Some(namespace.as_str()), "namespace")
        .expect("namespace should decode");
    let missing = TableNamespace::decode(None, "namespace").expect_err("missing field must fail");
    let invalid =
        TableNamespace::decode(Some("bad"), "namespace").expect_err("invalid field must fail");

    assert_eq!(decoded, namespace);
    assert_internal_message(missing, "missing namespace field");
    assert_internal_message(invalid, "invalid namespace field:");
}

#[test]
fn table_namespace_schema_bounds_include_system_and_canonical_namespaces() {
    assert_eq!(TableNamespace::schema_min_length(), "system".len());
    assert_eq!(
        TableNamespace::schema_max_length(),
        TableNamespace::canonical_length()
    );
    assert!(TableNamespace::schema_pattern().contains("system"));
    assert!(TableNamespace::schema_example().starts_with(TableNamespace::PREFIX));
}

fn assert_internal_message(error: StorageError, expected: &str) {
    assert!(
        matches!(
            error,
            StorageError::Base(StorageEnum::InternalServerError { ref message })
                if message.contains(expected)
        ),
        "unexpected error: {error:?}"
    );
}
