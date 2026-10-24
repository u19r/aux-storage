use std::collections::HashMap;

use crate::{AttributeValue, IndexName, ItemKey, SerializesToKey, TableKey, TableName};

#[test]
fn sorted_storage_suffix_matches_legacy_primary_storage_suffix() {
    let table_name = TableName::new("orders");
    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("account#1".to_string()),
        Some(AttributeValue::N("42".to_string())),
    );
    let full_key = item_key
        .serialize_to_bytes()
        .expect("legacy item key should serialize");
    let legacy_prefix = ItemKey::table_prefix_from_name(&table_name);

    assert_eq!(
        item_key.sorted_storage_suffix().expect("sorted suffix"),
        full_key
            .strip_prefix(legacy_prefix.as_slice())
            .expect("legacy key should include table prefix")
    );
}

#[test]
fn sorted_storage_suffix_matches_legacy_gsi_storage_suffix() {
    let table_name = TableName::new("orders");
    let index_name = IndexName::new("by_status");
    let table_key = TableKey::new(
        table_name.clone(),
        AttributeValue::S("account#1".to_string()),
        Some(AttributeValue::N("42".to_string())),
    );
    let item_key = ItemKey::index_key(
        table_name.clone(),
        index_name.clone(),
        AttributeValue::S("open".to_string()),
        Some(AttributeValue::S("2026-06-16".to_string())),
        table_key,
    );
    let full_key = item_key
        .serialize_to_bytes()
        .expect("legacy gsi key should serialize");
    let legacy_prefix = ItemKey::index_prefix_from_name(&table_name, &index_name);

    assert_eq!(
        item_key.sorted_storage_suffix().expect("sorted suffix"),
        full_key
            .strip_prefix(legacy_prefix.as_slice())
            .expect("legacy key should include index prefix")
    );
}

fn expected_key_part(hash_key: &AttributeValue, range_key: Option<&AttributeValue>) -> Vec<u8> {
    let hash_bytes = ItemKey::serialize_attribute_value_to_bytes(hash_key).expect("hash bytes");
    let mut parts = Vec::with_capacity(2 + hash_bytes.len());
    let length = u16::try_from(hash_bytes.len()).expect("Dynamo key length");
    parts.extend_from_slice(&length.to_be_bytes());
    parts.extend_from_slice(&hash_bytes);
    if let Some(range_key) = range_key {
        let range_bytes =
            ItemKey::serialize_attribute_value_to_bytes(range_key).expect("range bytes");
        parts.extend_from_slice(&range_bytes);
    }
    parts
}

#[test]
fn given_mixed_key_values_when_serialized_then_direct_bytes_match_ordered_shape() {
    let cases = [
        (
            AttributeValue::S("tenant/42".to_string()),
            Some(AttributeValue::N("-1.250E+3".to_string())),
        ),
        (
            AttributeValue::B("AAECAwQF".to_string()),
            Some(AttributeValue::S("partition\0suffix".to_string())),
        ),
        (AttributeValue::N("0".to_string()), None),
    ];

    for (hash_key, range_key) in cases {
        let candidate = TableKey::new(
            TableName::new("orders"),
            hash_key.clone(),
            range_key.clone(),
        )
        .sorted_storage_suffix()
        .expect("candidate key bytes");
        assert_eq!(candidate, expected_key_part(&hash_key, range_key.as_ref()));
    }
}

#[test]
fn given_invalid_rocksdb_key_value_when_serialized_then_validation_error_is_preserved() {
    let invalid = AttributeValue::M(HashMap::new());
    let error = TableKey::new(
        TableName::new("orders"),
        AttributeValue::S("tenant/42".to_string()),
        Some(invalid),
    )
    .sorted_storage_suffix()
    .expect_err("maps are not key values");

    assert_eq!(
        error.to_string(),
        "Item key validation error: Only S, N, and B types are supported for keys"
    );
}

#[test]
fn binary_key_bytes_follow_decoded_order_and_preserve_prefixes() {
    let values = ["AA==", "AAA=", "AQAC", "fw==", "gA==", "/w=="];
    let encoded = values
        .iter()
        .map(|value| {
            ItemKey::serialize_attribute_value_to_bytes(&AttributeValue::B((*value).into()))
                .expect("binary key")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        encoded,
        vec![
            vec![0],
            vec![0, 0],
            vec![1, 0, 2],
            vec![127],
            vec![128],
            vec![255]
        ]
    );
    assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        ItemKey::serialize_attribute_value_to_bytes(&AttributeValue::B("not-base64".into()))
            .is_err()
    );
}

#[test]
fn gsi_range_key_terminator_keeps_binary_prefix_before_its_extension() {
    let table = TableName::new("binary-gsi-order");
    let index = IndexName::new("by_binary");
    let table_key = TableKey::new(
        table.clone(),
        AttributeValue::B("AQ==".into()),
        Some(AttributeValue::B("Ag==".into())),
    );
    let suffix = |binary: &str| {
        ItemKey::index_key(
            table.clone(),
            index.clone(),
            AttributeValue::B("Aw==".into()),
            Some(AttributeValue::B(binary.into())),
            table_key.clone(),
        )
        .sorted_storage_suffix()
        .expect("GSI key")
    };
    assert!(suffix("AA==") < suffix("AAA="));
    assert!(suffix("fw==") < suffix("gA=="));
    assert!(suffix("gA==") < suffix("/w=="));
}

fn ordered_table(
    hash: crate::KeyAttributeType,
    range: crate::KeyAttributeType,
) -> crate::StoredTableInfo {
    use crate::{
        AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, KeyType, MaxIndexers,
        Projection, ProjectionType, TableStatus,
    };
    let schema = |hash: &str, range: &str| {
        vec![
            KeySchemaElement {
                attribute_name: hash.into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: range.into(),
                key_type: KeyType::Range,
            },
        ]
    };
    crate::StoredTableInfo {
        table_name: TableName::new("ordered-source"),
        table_status: TableStatus::Active,
        created_at: 0.into(),
        max_indexers: MaxIndexers::ZERO,
        key_schema: schema("pk", "sk"),
        attribute_definitions: [("pk", hash), ("sk", range), ("gpk", range), ("gsk", hash)]
            .into_iter()
            .map(|(name, kind)| AttributeDefinition {
                attribute_name: name.into(),
                attribute_type: kind,
            })
            .collect(),
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("by-source"),
            key_schema: schema("gpk", "gsk"),
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: crate::StreamRetentionDuration::default(),
        default_item_stream_duration: crate::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

#[test]
fn given_native_ordered_keys_when_decoded_without_an_image_then_table_and_index_addresses_are_preserved()
 {
    use crate::KeyAttributeType;
    let values = [
        (KeyAttributeType::S, AttributeValue::S("tenant\0/é".into())),
        (KeyAttributeType::N, AttributeValue::N("-1.250E+3".into())),
        (KeyAttributeType::B, AttributeValue::B("AAEA/w==".into())),
    ];
    for (hash_kind, hash) in &values {
        for (range_kind, range) in &values {
            let table = ordered_table(*hash_kind, *range_kind);
            let primary =
                TableKey::new(table.table_name.clone(), hash.clone(), Some(range.clone()));
            let keys = [
                ItemKey::Table(primary.clone()),
                ItemKey::index_key(
                    table.table_name.clone(),
                    IndexName::new("by-source"),
                    range.clone(),
                    Some(hash.clone()),
                    primary,
                ),
            ];
            for key in keys {
                let encoded = key.sorted_storage_suffix().unwrap();
                let decoded =
                    ItemKey::from_sorted_storage_suffix(&table, key.index_id(), &encoded).unwrap();
                assert_eq!(decoded.sorted_storage_suffix().unwrap(), encoded);
            }
        }
    }
}

#[test]
fn given_native_hash_only_keys_when_decoded_then_the_table_tie_and_absent_range_are_preserved() {
    let mut table = ordered_table(crate::KeyAttributeType::S, crate::KeyAttributeType::S);
    table.key_schema.truncate(1);
    table.global_secondary_indexes.as_mut().unwrap()[0]
        .key_schema
        .truncate(1);
    let key = ItemKey::index_key(
        table.table_name.clone(),
        IndexName::new("by-source"),
        AttributeValue::S("index".into()),
        None,
        TableKey::new(
            table.table_name.clone(),
            AttributeValue::S("source".into()),
            None,
        ),
    );
    let encoded = key.sorted_storage_suffix().unwrap();
    let decoded = ItemKey::from_sorted_storage_suffix(&table, key.index_id(), &encoded).unwrap();
    assert_eq!(decoded.sorted_storage_suffix().unwrap(), encoded);
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(ItemKey::from_sorted_storage_suffix(&table, key.index_id(), &trailing).is_err());
    for length in 0..encoded.len() {
        assert!(
            ItemKey::from_sorted_storage_suffix(&table, key.index_id(), &encoded[..length])
                .is_err()
        );
    }
}

#[test]
fn given_malformed_native_index_escape_when_decoded_then_the_address_is_rejected() {
    let table = ordered_table(crate::KeyAttributeType::S, crate::KeyAttributeType::S);
    let key = ItemKey::index_key(
        table.table_name.clone(),
        IndexName::new("by-source"),
        AttributeValue::S("index".into()),
        Some(AttributeValue::S("sort".into())),
        TableKey::new(
            table.table_name.clone(),
            AttributeValue::S("source".into()),
            Some(AttributeValue::S("row".into())),
        ),
    );
    let mut encoded = key.sorted_storage_suffix().unwrap();
    encoded[7] = 2;
    assert!(ItemKey::from_sorted_storage_suffix(&table, key.index_id(), &encoded).is_err());
}
