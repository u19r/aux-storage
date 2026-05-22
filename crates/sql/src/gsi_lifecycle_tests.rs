use std::collections::HashMap;

use storage_types::{
    AttributeValue, GlobalSecondaryIndex, IndexName, KeySchemaElement, KeyType, Projection,
    ProjectionType, TTL_PARTITION_ATTRIBUTE,
};

use crate::gsi_lifecycle::{
    apply_gsi_projection, encode_gsi_attributes_blob, encode_gsi_json,
    non_key_attributes_for_gsi_row, ttl_attribute_for_gsi,
};

#[test]
fn apply_gsi_projection_include_keeps_keys_and_selected_attributes() {
    let full_item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        ("sk".to_string(), AttributeValue::S("user#1".to_string())),
        (
            "gsi1pk".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        ("name".to_string(), AttributeValue::S("Alice".to_string())),
        ("role".to_string(), AttributeValue::S("admin".to_string())),
    ]);
    let gsi_key = HashMap::from([(
        "gsi1pk".to_string(),
        AttributeValue::S("active".to_string()),
    )]);
    let main_key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        ("sk".to_string(), AttributeValue::S("user#1".to_string())),
    ]);
    let projection = Projection {
        projection_type: Some(ProjectionType::Include),
        non_key_attributes: Some(vec!["name".to_string()]),
    };

    let projected = apply_gsi_projection(&full_item, &gsi_key, &main_key, &projection);

    assert_eq!(projected.len(), 4);
    assert_eq!(
        projected.get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );
    assert!(!projected.contains_key("role"));
}

#[test]
fn apply_gsi_projection_returns_full_item_for_all_projection() {
    let full_item = HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]);
    let projection = Projection {
        projection_type: Some(ProjectionType::All),
        non_key_attributes: None,
    };

    let projected = apply_gsi_projection(&full_item, &HashMap::new(), &HashMap::new(), &projection);

    assert_eq!(projected, full_item);
}

#[test]
fn apply_gsi_projection_keys_only_keeps_gsi_and_table_keys() {
    let full_item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        ("sk".to_string(), AttributeValue::S("user#1".to_string())),
        (
            "gsi1pk".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        ("name".to_string(), AttributeValue::S("Alice".to_string())),
    ]);
    let gsi_key = HashMap::from([(
        "gsi1pk".to_string(),
        AttributeValue::S("active".to_string()),
    )]);
    let main_key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        ("sk".to_string(), AttributeValue::S("user#1".to_string())),
    ]);
    let projection = Projection {
        projection_type: Some(ProjectionType::KeysOnly),
        non_key_attributes: None,
    };

    let projected = apply_gsi_projection(&full_item, &gsi_key, &main_key, &projection);

    assert_eq!(projected.len(), 3);
    assert!(projected.contains_key("pk"));
    assert!(projected.contains_key("sk"));
    assert!(projected.contains_key("gsi1pk"));
    assert!(!projected.contains_key("name"));
}

#[test]
fn non_key_attributes_for_gsi_row_removes_gsi_and_main_table_keys_before_storing_blob() {
    let filtered_item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        (
            "gsi1pk".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        ("name".to_string(), AttributeValue::S("Alice".to_string())),
    ]);
    let gsi_key = HashMap::from([(
        "gsi1pk".to_string(),
        AttributeValue::S("active".to_string()),
    )]);
    let main_key = HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]);

    let non_keys = non_key_attributes_for_gsi_row(filtered_item, &gsi_key, &main_key);

    assert_eq!(non_keys.len(), 1);
    assert_eq!(
        non_keys.get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );
}

#[test]
fn encode_gsi_attributes_blob_uses_empty_json_for_key_only_rows() {
    let empty_blob = encode_gsi_attributes_blob(&HashMap::new()).expect("empty blob");
    let non_empty_blob = encode_gsi_attributes_blob(&HashMap::from([(
        "name".to_string(),
        AttributeValue::S("Alice".to_string()),
    )]))
    .expect("non-empty blob");

    assert_eq!(empty_blob, "{}");
    assert!(non_empty_blob.contains("Alice"));
}

#[test]
fn ttl_attribute_for_gsi_returns_non_partition_ttl_key_only_for_ttl_indexes() {
    let key_schema = vec![
        KeySchemaElement {
            attribute_name: TTL_PARTITION_ATTRIBUTE.to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "expires_at".to_string(),
            key_type: KeyType::Range,
        },
    ];

    assert_eq!(
        ttl_attribute_for_gsi(&IndexName::new("__ttl#accounts"), &key_schema).as_deref(),
        Some("expires_at")
    );
    assert_eq!(
        ttl_attribute_for_gsi(&IndexName::new("by_status"), &key_schema),
        None
    );
}

#[test]
fn encode_gsi_json_serializes_index_names_when_present() {
    let gsis = vec![GlobalSecondaryIndex {
        index_name: IndexName::new("gsi_by_status"),
        key_schema: Vec::new(),
        projection: Projection {
            projection_type: Some(ProjectionType::KeysOnly),
            non_key_attributes: None,
        },
    }];

    let encoded = encode_gsi_json(Some(&gsis)).expect("encoded gsis");

    assert!(
        encoded
            .as_deref()
            .is_some_and(|json| json.contains("gsi_by_status"))
    );
    assert_eq!(encode_gsi_json(None).expect("none"), None);
}
