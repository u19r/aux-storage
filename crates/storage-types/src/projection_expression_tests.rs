use std::collections::HashMap;

use crate::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, QueryTableRequest, StoredTableInfo,
    StreamRetentionDuration, TableName, TableStatus, TimestampMillis, WireItem, project_wire_items,
    validate_gsi_projection,
};

fn include_projection_table() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("tenant_data"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        max_indexers: crate::MaxIndexers::ZERO,
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi1pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("gsi1"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi1pk".to_string(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: Some(ProjectionType::Include),
                non_key_attributes: Some(vec!["profile".to_string()]),
            },
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: StreamRetentionDuration::default(),
        default_item_stream_duration: StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

#[test]
fn projection_expression_preserves_alias_and_document_path_semantics() {
    let item = WireItem::from_attribute_map(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        (
            "profile".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "display_name".to_string(),
                    AttributeValue::S("Ada".to_string()),
                ),
                (
                    "private_note".to_string(),
                    AttributeValue::S("hidden".to_string()),
                ),
            ])),
        ),
        (
            "tags".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("first".to_string()),
                AttributeValue::S("second".to_string()),
            ]),
        ),
    ]))
    .expect("encode item");

    let projected = project_wire_items(
        vec![item],
        Some("#pk, profile.#name, tags[1]"),
        Some(&HashMap::from([
            ("#pk".to_string(), "pk".to_string()),
            ("#name".to_string(), "display_name".to_string()),
        ])),
    )
    .expect("project item")
    .pop()
    .expect("projected item")
    .into_attribute_map()
    .expect("decode projected item");

    assert_eq!(projected.len(), 3);
    assert_eq!(
        projected.get("pk"),
        Some(&AttributeValue::S("tenant#1".to_string()))
    );
    assert_eq!(
        projected.get("profile"),
        Some(&AttributeValue::M(HashMap::from([(
            "display_name".to_string(),
            AttributeValue::S("Ada".to_string()),
        )])))
    );
    assert_eq!(
        projected.get("tags"),
        Some(&AttributeValue::L(vec![AttributeValue::S(
            "second".to_string()
        )]))
    );
}

#[test]
fn query_table_request_rejects_reserved_projection_attribute_name() {
    let request = QueryTableRequest {
        table_name: TableName::new("tenant_data"),
        index_name: Some(IndexName::new("gsi1")),
        key_condition_expression: "#pk = :pk".to_string(),
        expression_attribute_names: Some(HashMap::from([(
            "#pk".to_string(),
            "gsi1pk".to_string(),
        )])),
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        projection_expression: Some("COMMENT".to_string()),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };

    let error = request
        .validate_for_dynamodb()
        .expect_err("reserved projection name should fail");
    assert_eq!(
        error.to_string(),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
}

#[test]
fn gsi_projection_accepts_keys_included_attributes_and_nested_paths() {
    let table = include_projection_table();
    validate_gsi_projection(
        &table,
        Some(&IndexName::new("gsi1")),
        Some("#pk, #gsi, #profile.display_name"),
        None,
        Some(&HashMap::from([
            ("#pk".to_string(), "pk".to_string()),
            ("#gsi".to_string(), "gsi1pk".to_string()),
            ("#profile".to_string(), "profile".to_string()),
        ])),
    )
    .expect("all requested roots are projected");
}

#[test]
fn gsi_projection_rejects_aliased_unprojected_attributes_in_expression_order() {
    let table = include_projection_table();
    let error = validate_gsi_projection(
        &table,
        Some(&IndexName::new("gsi1")),
        Some("#secret.value, missing, #secret.other"),
        None,
        Some(&HashMap::from([(
            "#secret".to_string(),
            "private_note".to_string(),
        )])),
    )
    .expect_err("unprojected GSI attributes must be rejected");

    assert_eq!(
        error.to_string(),
        "One or more parameter values were invalid: Global secondary index gsi1 does not project \
         [private_note, missing]"
    );
}

#[test]
fn all_projection_accepts_any_requested_attribute() {
    let mut table = include_projection_table();
    table.global_secondary_indexes.as_mut().unwrap()[0]
        .projection
        .projection_type = Some(ProjectionType::All);

    validate_gsi_projection(
        &table,
        Some(&IndexName::new("gsi1")),
        Some("private_note"),
        None,
        None,
    )
    .expect("ALL projects every attribute");
}

#[test]
fn gsi_projection_validates_attributes_to_get_without_building_an_expression() {
    let table = include_projection_table();
    validate_gsi_projection(
        &table,
        Some(&IndexName::new("gsi1")),
        None,
        Some(&["pk".to_string(), "profile".to_string()]),
        None,
    )
    .expect("requested attributes are projected");

    let error = validate_gsi_projection(
        &table,
        Some(&IndexName::new("gsi1")),
        None,
        Some(&["private_note".to_string()]),
        None,
    )
    .expect_err("unprojected attribute must fail");
    assert!(error.to_string().contains("private_note"));
}
