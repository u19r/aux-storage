use std::collections::HashMap;

use crate::{
    AttributeValue, IndexName, QueryTableRequest, TableName, WireItem, project_wire_items,
};

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
