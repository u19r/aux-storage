use std::collections::HashMap;

use base64::{Engine as _, engine::general_purpose::URL_SAFE};

use crate::{
    AttributeDefinition, AttributeValue, IndexName, ItemKey, KeyAttributeType, KeySchemaElement,
    KeyType, Projection, ProjectionType, StoredTableInfo, TableName, TableStatus,
    item_key::split_next_token_to_keys,
};

#[test]
fn hash_range_key_part_length_prefix() {
    let hash_key = AttributeValue::S("test_hash".to_string());
    let item_key = ItemKey::table_key(TableName::new("test"), hash_key, None);

    let parts = item_key.hash_range_key_part().expect("hash range key part");

    // Should have: 2 bytes prefix + 9 bytes data = 11 bytes
    assert_eq!(parts.len(), 11);
    let prefix = u16::from_be_bytes([parts[0], parts[1]]);
    let length = (prefix >> 6) as usize;
    assert_eq!(length, 9); // "test_hash" is 9 bytes
    assert_eq!(&parts[2..], b"test_hash");
}

#[test]
fn hash_range_key_part_with_range_key() {
    let hash_key = AttributeValue::S("hash_key".to_string());
    let range_key = AttributeValue::S("range_key".to_string());
    let item_key = ItemKey::table_key(TableName::new("test"), hash_key, Some(range_key));

    let parts = item_key.hash_range_key_part().expect("hash range key part");

    // Should have: hash(2+8) + range(2+9) = 21 bytes
    assert_eq!(parts.len(), 21);

    // Parse first part (hash key)
    let hash_prefix = u16::from_be_bytes([parts[0], parts[1]]);
    let hash_length = (hash_prefix >> 6) as usize;
    assert_eq!(hash_length, 8); // "hash_key" is 8 bytes
    assert_eq!(&parts[2..10], b"hash_key");

    // Parse second part (range key)
    let range_prefix = u16::from_be_bytes([parts[10], parts[11]]);
    let range_length = (range_prefix >> 6) as usize;
    assert_eq!(range_length, 9); // "range_key" is 9 bytes
    assert_eq!(&parts[12..], b"range_key");
}

#[test]
fn pagination_token_splits_into_key_parts() {
    let hash_key = AttributeValue::S("test_hash".to_string());
    let range_key = AttributeValue::S("test_range".to_string());
    let item_key = ItemKey::table_key(TableName::new("test"), hash_key, Some(range_key));

    let encoded = item_key.hash_range_key_part().expect("hash range key part");
    let decoded_parts = split_next_token_to_keys(&encoded);

    assert_eq!(decoded_parts.len(), 2);
    assert_eq!(decoded_parts[0], b"test_hash");
    assert_eq!(decoded_parts[1], b"test_range");
}

#[test]
fn round_trip_pagination_token() {
    let hash_key = AttributeValue::S("user123".to_string());
    let range_key = AttributeValue::N("12345".to_string());
    let _item_key = ItemKey::table_key(
        TableName::new("test"),
        hash_key.clone(),
        Some(range_key.clone()),
    );
    let table_info = StoredTableInfo {
        table_name: TableName::new("test_table"),
        table_status: TableStatus::Active,
        created_at: 0.into(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
        ],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: crate::StreamRetentionDuration::default(),
        default_item_stream_duration: crate::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    };

    // Generate last evaluated key
    let last_item = HashMap::from([("pk".to_string(), hash_key), ("sk".to_string(), range_key)]);

    let token = ItemKey::last_evaluated_key_from_last_item(&last_item, &table_info, &None)
        .expect("last evaluated key");
    assert!(token.is_some());

    let token_str = token.unwrap();

    // Decode the token back
    let decoded_key = ItemKey::item_key_from_next_page_token(&token_str, &table_info, &None);
    assert!(decoded_key.is_ok());

    let decoded_key = decoded_key.unwrap();
    assert!(decoded_key.is_some());

    let decoded_key = decoded_key.unwrap();
    assert_eq!(decoded_key.table_name().as_ref(), "test_table");
    assert_eq!(
        decoded_key.hash_key().clone(),
        AttributeValue::S("user123".to_string())
    );
    assert_eq!(
        decoded_key.range_key().cloned(),
        Some(AttributeValue::N("12345".to_string()))
    );
}

#[test]
fn key_consistency_across_methods() {
    // This test verifies that different methods that should produce the same key
    // actually do produce identical keys
    let hash_key = AttributeValue::S("consistent_key".to_string());
    let range_key = AttributeValue::S("range_value".to_string());
    let item_key = ItemKey::table_key(
        TableName::new("test"),
        hash_key.clone(),
        Some(range_key.clone()),
    );
    let pagination_key = item_key.hash_range_key_part().expect("hash range key part");
    let table_info = StoredTableInfo {
        table_name: TableName::new("consistency_test"),
        table_status: TableStatus::Active,
        created_at: 0.into(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "hash_attr".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "range_attr".to_string(),
                key_type: KeyType::Range,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "hash_attr".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "range_attr".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: crate::StreamRetentionDuration::default(),
        default_item_stream_duration: crate::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    };

    let item = HashMap::from([
        ("hash_attr".to_string(), hash_key),
        ("range_attr".to_string(), range_key),
    ]);

    let schema_key =
        ItemKey::from_key_schema(TableName::new("test"), &table_info.key_schema, &item).unwrap();
    let schema_pagination_key = schema_key
        .hash_range_key_part()
        .expect("hash range key part");

    // Both should produce identical pagination keys
    assert_eq!(pagination_key, schema_pagination_key);
}

#[test]
fn version_and_flags() {
    let hash_key = AttributeValue::S("test".to_string());
    let item_key = ItemKey::table_key(TableName::new("test"), hash_key, None);

    let parts = item_key.hash_range_key_part().expect("hash range key part");

    let prefix = u16::from_be_bytes([parts[0], parts[1]]);
    let version_and_flags = prefix & 0x3F; // Lower 6 bits
    assert_eq!(version_and_flags, 0); // Version 00, flags 0000
}

#[cfg(feature = "rocksdb")]
#[test]
fn rocksdb_number_key_encoding_accepts_dynamodb_number_boundaries() {
    for number in [
        "99999999999999999999999999999999999999",
        "1E-130",
        "-1E-130",
    ] {
        let encoded =
            ItemKey::serialize_attribute_value_to_bytes(&AttributeValue::N(number.to_string()))
                .expect("valid DynamoDB number key should encode");

        assert_eq!(encoded.len(), 41);
    }
}

#[cfg(feature = "rocksdb")]
#[test]
fn rocksdb_number_key_encoding_orders_dynamodb_numbers_numerically() {
    let numbers = [
        "-99999999999999999999999999999999999999",
        "-100",
        "-2",
        "-1E-130",
        "0",
        "1E-130",
        "2",
        "10",
        "99999999999999999999999999999999999999",
    ];

    let mut encoded = numbers
        .iter()
        .map(|number| {
            (
                ItemKey::serialize_attribute_value_to_bytes(&AttributeValue::N(
                    (*number).to_string(),
                ))
                .expect("valid DynamoDB number key should encode"),
                *number,
            )
        })
        .collect::<Vec<_>>();
    encoded.sort_by(|left, right| left.0.cmp(&right.0));

    let sorted_numbers = encoded
        .into_iter()
        .map(|(_, number)| number)
        .collect::<Vec<_>>();
    assert_eq!(sorted_numbers, numbers);
}

#[cfg(feature = "rocksdb")]
#[test]
fn rocksdb_number_key_encoding_normalizes_equivalent_numbers() {
    let canonical =
        ItemKey::serialize_attribute_value_to_bytes(&AttributeValue::N("1".to_string()))
            .expect("number key should encode");

    for equivalent in ["1.0", "01E0", "+1.0000E+0"] {
        let encoded =
            ItemKey::serialize_attribute_value_to_bytes(&AttributeValue::N(equivalent.to_string()))
                .expect("equivalent number key should encode");
        assert_eq!(encoded, canonical);
    }
}

#[test]
fn gsi_round_trip_hash_only() {
    let table_info = StoredTableInfo {
        table_name: TableName::new("test_table"),
        table_status: TableStatus::Active,
        created_at: 0.into(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        global_secondary_indexes: Some(vec![crate::GlobalSecondaryIndex {
            index_name: IndexName::new("test_gsi"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            }],
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
    };

    let last_item = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("table_hash".to_string()),
        ),
        (
            "sk".to_string(),
            AttributeValue::S("table_range".to_string()),
        ),
        (
            "gsi_pk".to_string(),
            AttributeValue::S("gsi_hash".to_string()),
        ),
    ]);

    let token = ItemKey::last_evaluated_key_from_last_item(
        &last_item,
        &table_info,
        &Some(IndexName::new("test_gsi")),
    )
    .expect("last evaluated key");
    assert!(token.is_some());

    let token_str = token.unwrap();

    // Decode the token back
    let decoded_key = ItemKey::item_key_from_next_page_token(
        &token_str,
        &table_info,
        &Some(IndexName::new("test_gsi")),
    );
    assert!(decoded_key.is_ok());

    let decoded_key = decoded_key.unwrap();
    assert!(decoded_key.is_some());

    let decoded_key = decoded_key.unwrap();
    let ItemKey::Index(index_key) = decoded_key else {
        panic!("expected index key");
    };
    assert_eq!(index_key.table_name.as_ref(), "test_table");
    assert_eq!(index_key.index_id, IndexName::new("test_gsi"));
    assert_eq!(
        index_key.hash_key,
        AttributeValue::S("gsi_hash".to_string())
    );
    assert!(index_key.range_key.is_none());

    let table_key = index_key.table_key;
    assert_eq!(
        table_key.hash_key,
        AttributeValue::S("table_hash".to_string())
    );
    assert_eq!(
        table_key.range_key,
        Some(AttributeValue::S("table_range".to_string()))
    );
}

#[test]
fn gsi_round_trip_with_gsi_range() {
    let table_info = StoredTableInfo {
        table_name: TableName::new("test_table"),
        table_status: TableStatus::Active,
        created_at: 0.into(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
        ],
        global_secondary_indexes: Some(vec![crate::GlobalSecondaryIndex {
            index_name: IndexName::new("test_gsi"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi_pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi_sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
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
    };

    let last_item = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("table_hash".to_string()),
        ),
        (
            "gsi_pk".to_string(),
            AttributeValue::S("gsi_hash".to_string()),
        ),
        ("gsi_sk".to_string(), AttributeValue::N("123".to_string())),
    ]);

    let token = ItemKey::last_evaluated_key_from_last_item(
        &last_item,
        &table_info,
        &Some(IndexName::new("test_gsi")),
    )
    .expect("last evaluated key");
    assert!(token.is_some());

    let token_str = token.unwrap();

    // Decode the token back
    let decoded_key = ItemKey::item_key_from_next_page_token(
        &token_str,
        &table_info,
        &Some(IndexName::new("test_gsi")),
    );
    assert!(decoded_key.is_ok());

    let decoded_key = decoded_key.unwrap();
    assert!(decoded_key.is_some());
    let decoded_key = decoded_key.unwrap();
    let ItemKey::Index(index_key) = decoded_key else {
        panic!("expected index key");
    };

    assert_eq!(index_key.table_name.as_ref(), "test_table");
    assert_eq!(index_key.index_id, IndexName::new("test_gsi"));
    assert_eq!(
        index_key.hash_key,
        AttributeValue::S("gsi_hash".to_string())
    );
    assert_eq!(
        index_key.range_key,
        Some(AttributeValue::N("123".to_string()))
    );

    let table_key = index_key.table_key;
    assert_eq!(
        table_key.hash_key,
        AttributeValue::S("table_hash".to_string())
    );
    assert!(table_key.range_key.is_none());
}

#[test]
fn gsi_round_trip_all_keys() {
    let table_info = StoredTableInfo {
        table_name: TableName::new("test_table"),
        table_status: TableStatus::Active,
        created_at: 0.into(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
        ],
        global_secondary_indexes: Some(vec![crate::GlobalSecondaryIndex {
            index_name: IndexName::new("test_gsi"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi_pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi_sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
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
    };

    let last_item = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("table_hash".to_string()),
        ),
        (
            "sk".to_string(),
            AttributeValue::S("table_range".to_string()),
        ),
        (
            "gsi_pk".to_string(),
            AttributeValue::S("gsi_hash".to_string()),
        ),
        ("gsi_sk".to_string(), AttributeValue::N("456".to_string())),
    ]);

    let token = ItemKey::last_evaluated_key_from_last_item(
        &last_item,
        &table_info,
        &Some(IndexName::new("test_gsi")),
    )
    .expect("last evaluated key");
    assert!(token.is_some());

    let token_str = token.unwrap();

    // Decode the token back
    let decoded_key = ItemKey::item_key_from_next_page_token(
        &token_str,
        &table_info,
        &Some(IndexName::new("test_gsi")),
    );
    assert!(decoded_key.is_ok());

    let decoded_key = decoded_key.unwrap();
    assert!(decoded_key.is_some());

    let decoded_key = decoded_key.unwrap();
    let ItemKey::Index(index_key) = decoded_key else {
        panic!("expected index key");
    };
    assert_eq!(index_key.table_name.as_ref(), "test_table");
    assert_eq!(index_key.index_id, IndexName::new("test_gsi"));
    assert_eq!(
        index_key.hash_key,
        AttributeValue::S("gsi_hash".to_string())
    );
    assert_eq!(
        index_key.range_key,
        Some(AttributeValue::N("456".to_string()))
    );

    let table_key = index_key.table_key;
    assert_eq!(
        table_key.hash_key,
        AttributeValue::S("table_hash".to_string())
    );
    assert_eq!(
        table_key.range_key,
        Some(AttributeValue::S("table_range".to_string()))
    );
}

#[test]
fn gsi_invalid_token_parts() {
    let table_info = StoredTableInfo {
        table_name: TableName::new("test_table"),
        table_status: TableStatus::Active,
        created_at: 0.into(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        global_secondary_indexes: Some(vec![crate::GlobalSecondaryIndex {
            index_name: IndexName::new("test_gsi"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            }],
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
    };
    let empty_token = URL_SAFE.encode([]);
    let result = ItemKey::item_key_from_next_page_token(
        &empty_token,
        &table_info,
        &Some(IndexName::new("test_gsi")),
    );
    assert!(result.is_err());
    let mut parts = Vec::new();
    for i in 0..5 {
        let data_str = format!("part{i}");
        let data = data_str.as_bytes();
        ItemKey::add_length_prefixed_part(&mut parts, data);
    }
    let invalid_token = URL_SAFE.encode(&parts);
    let result = ItemKey::item_key_from_next_page_token(
        &invalid_token,
        &table_info,
        &Some(IndexName::new("test_gsi")),
    );
    assert!(result.is_err());
}
