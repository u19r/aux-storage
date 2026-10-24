use crate::{
    AttributeDefinition, KeyAttributeType, KeySchemaElement, KeyType, PlannedKeyColumn,
    planned_key_columns,
};

fn definition(name: &str, attribute_type: KeyAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type,
    }
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type,
    }
}

#[test]
fn given_table_and_gsi_schemas_when_planned_then_order_is_table_first_deduped() {
    let definitions = vec![
        definition("pk", KeyAttributeType::S),
        definition("sk", KeyAttributeType::S),
        definition("gsi_pk", KeyAttributeType::S),
    ];

    let planned = planned_key_columns(
        &[key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        [&[key("gsi_pk", KeyType::Hash), key("pk", KeyType::Range)][..]],
        &definitions,
    );

    assert_eq!(
        planned,
        vec![
            PlannedKeyColumn {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            PlannedKeyColumn {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            PlannedKeyColumn {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ]
    );
}

#[test]
fn given_missing_definition_when_planned_then_key_is_skipped() {
    let definitions = vec![definition("pk", KeyAttributeType::S)];

    let planned = planned_key_columns(
        &[key("pk", KeyType::Hash), key("ghost", KeyType::Range)],
        Vec::<&[KeySchemaElement]>::new(),
        &definitions,
    );

    assert_eq!(planned.len(), 1);
    assert_eq!(planned[0].attribute_name, "pk");
}

#[test]
fn given_reversed_schema_when_ordered_then_hash_precedes_range_without_mutation() {
    let schema = [key("sk", KeyType::Range), key("pk", KeyType::Hash)];
    let names = KeySchemaElement::ordered(&schema)
        .map(|key| key.attribute_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["pk", "sk"]);
    assert_eq!(schema[0].attribute_name, "sk");
    assert_eq!(KeySchemaElement::ordered(&[]).count(), 0);
}
