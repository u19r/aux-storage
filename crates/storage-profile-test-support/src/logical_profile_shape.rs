//! Parameterized GSI-backfill fixtures for storage profile tests.

use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, IndexName, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType,
    Projection, ProjectionType, StreamSpecification, StreamViewType, TableName,
};

use crate::constants::VISIBLE_ATTRIBUTES;

/// Build a logical profile table with either two or three user GSIs.
pub fn request(
    table_name: &TableName,
    stream_enabled: bool,
    gsi_count: usize,
) -> CreateTableRequest {
    validate_gsi_count(gsi_count);
    let request = CreateTableRequest::builder()
        .table_name(table_name.clone())
        .attribute_definitions(table_definitions(gsi_count))
        .key_schema(primary_key_schema())
        .billing_mode(Some(BillingMode::PayPerRequest))
        .global_secondary_indexes(Some(global_secondary_indexes(gsi_count)))
        .build();
    add_stream_specification(request, stream_enabled)
}

fn validate_gsi_count(gsi_count: usize) {
    assert!(
        matches!(gsi_count, 2 | 3),
        "logical storage profile requires 2 or 3 GSIs"
    );
}

fn table_definitions(gsi_count: usize) -> Vec<AttributeDefinition> {
    let mut definitions = primary_definitions();
    for index in 0..gsi_count {
        definitions.extend(index_definitions(index));
    }
    definitions
}

fn primary_definitions() -> Vec<AttributeDefinition> {
    vec![
        AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
    ]
}

fn index_definitions(index: usize) -> [AttributeDefinition; 2] {
    [
        AttributeDefinition {
            attribute_name: format!("gsi{index}_pk"),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: format!("gsi{index}_sk"),
            attribute_type: KeyAttributeType::S,
        },
    ]
}

fn global_secondary_indexes(gsi_count: usize) -> Vec<CreateGlobalSecondaryIndex> {
    (0..gsi_count).map(global_secondary_index).collect()
}

fn global_secondary_index(index: usize) -> CreateGlobalSecondaryIndex {
    CreateGlobalSecondaryIndex {
        index_name: IndexName::new(&format!("gsi{index}")),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: format!("gsi{index}_pk"),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: format!("gsi{index}_sk"),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }
}

fn primary_key_schema() -> Vec<KeySchemaElement> {
    vec![
        KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: KeyType::Range,
        },
    ]
}

fn add_stream_specification(
    mut request: CreateTableRequest,
    stream_enabled: bool,
) -> CreateTableRequest {
    if stream_enabled {
        request.stream_specification = Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        });
    }
    request
}

/// Generate a full item independently of its key projection.
pub fn item(
    index: usize,
    gsi_count: usize,
    payload_bytes: usize,
) -> HashMap<String, AttributeValue> {
    validate_gsi_count(gsi_count);
    let mut item = HashMap::with_capacity(VISIBLE_ATTRIBUTES);
    item.extend(key(index));
    item.insert(
        "ttl".to_string(),
        AttributeValue::N("2200000000".to_string()),
    );
    item.insert(
        "payload".to_string(),
        AttributeValue::S(payload(index, payload_bytes)),
    );
    insert_gsi_attributes(&mut item, index, gsi_count);
    insert_two_gsi_attributes(&mut item, index, gsi_count);
    assert_eq!(
        item.len(),
        VISIBLE_ATTRIBUTES,
        "logical profile attribute count"
    );
    item
}

fn insert_gsi_attributes(
    item: &mut HashMap<String, AttributeValue>,
    index: usize,
    gsi_count: usize,
) {
    for gsi in 0..gsi_count {
        item.insert(
            format!("gsi{gsi}_pk"),
            AttributeValue::S(format!("group#{gsi}#{:091}", index % 128)),
        );
        item.insert(
            format!("gsi{gsi}_sk"),
            AttributeValue::S(format!("rank#{index:096}")),
        );
    }
}

fn insert_two_gsi_attributes(
    item: &mut HashMap<String, AttributeValue>,
    index: usize,
    gsi_count: usize,
) {
    if gsi_count == 2 {
        item.insert(
            "category".to_string(),
            AttributeValue::S(format!("category-{:03}", index % 1000)),
        );
        item.insert(
            "counter".to_string(),
            AttributeValue::N((index as u64 + 1).to_string()),
        );
    }
}

/// Generate a primary key without observing an exported record.
pub fn key(index: usize) -> HashMap<String, AttributeValue> {
    HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("tenant#{:093}", index % 64)),
        ),
        (
            "sk".to_string(),
            AttributeValue::S(format!(
                "item#{index:08}#sort-key-component-realistic-length"
            )),
        ),
    ])
}

fn payload(index: usize, bytes: usize) -> String {
    let prefix = format!("payload-{index}-");
    assert!(bytes >= prefix.len(), "logical payload must fit its prefix");
    let value = format!("{prefix}{}", "x".repeat(bytes - prefix.len()));
    assert_eq!(value.len(), bytes, "logical profile payload length");
    value
}

pub fn canonical_key(index: usize) -> String {
    KeyAttributes::from(key(index))
        .canonical_dynamo_json()
        .expect("logical profile canonical key")
}
