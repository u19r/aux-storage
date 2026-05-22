use storage_types::{IndexName, TableNamespace};

use crate::migration_index_keys::{
    MigrationIndexKeyCodec, migration_index_pk, migration_index_sk, parse_migration_index_sort_key,
};

#[test]
fn migration_index_pk_uses_namespace_storage_key_and_entity_type() {
    let namespace =
        TableNamespace::parse_str("ns_1BCDEFGHJKMNPQRSTVWXYZ0").expect("valid namespace");
    assert_eq!(
        migration_index_pk(&namespace, "USER"),
        "1BCDEFGHJKMNPQRSTVWXYZ0#USER"
    );
}

#[test]
fn migration_index_sk_round_trips_pk_and_sk() {
    let encoded = migration_index_sk("U#abc123", "PROFILE#v1");
    let (pk, sk) =
        parse_migration_index_sort_key(&encoded, "custom_index_sk").expect("parse succeeds");
    assert_eq!(pk, "U#abc123");
    assert_eq!(sk, "PROFILE#v1");
}

#[test]
fn migration_index_sk_parse_rejects_invalid_payloads() {
    assert!(parse_migration_index_sort_key("no-separator", "custom_index_sk").is_err());
    assert!(parse_migration_index_sort_key("abc|payload", "custom_index_sk").is_err());
    assert!(parse_migration_index_sort_key("9|short", "custom_index_sk").is_err());
}

#[test]
fn migration_index_codec_derives_attributes_from_supplied_index_name() {
    let codec = MigrationIndexKeyCodec::new(IndexName::new("custom_index"));

    assert_eq!(codec.index_name().as_ref(), "custom_index");
    assert_eq!(codec.partition_key_attribute(), "custom_indexpk");
    assert_eq!(codec.sort_key_attribute(), "custom_indexsk");
    assert_eq!(codec.key_condition_expression(), "custom_indexpk = :pk");
}
