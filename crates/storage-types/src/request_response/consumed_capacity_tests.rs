use crate::ConsumedCapacity;

#[test]
fn given_capacity_wire_objects_when_decoded_then_preserve_names_values_and_duplicates() {
    for indexes in [
        None,
        Some(serde_json::json!({})),
        Some(
            serde_json::json!({"index_1": {"CapacityUnits": 1.5}, "quoted\"index": {"CapacityUnits": 2.5}}),
        ),
    ] {
        let mut expected = serde_json::json!({"TableName": "items", "CapacityUnits": 3.5});
        if let Some(indexes) = indexes {
            expected["GlobalSecondaryIndexes"] = indexes;
        }
        let decoded: ConsumedCapacity = serde_json::from_value(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(&decoded).unwrap(), expected);
    }
    let decoded: ConsumedCapacity = serde_json::from_str(r#"{"TableName":"items","CapacityUnits":0.5,"GlobalSecondaryIndexes":{"same":{"CapacityUnits":1.0},"same":{"CapacityUnits":2.0}}}"#).unwrap();
    let indexes = decoded.global_secondary_indexes.unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes.get("same").unwrap().capacity_units, 2.0);
}

#[test]
fn given_consumed_capacity_schema_when_generated_then_preserve_index_map_shape() {
    let schema =
        serde_json::to_value(<ConsumedCapacity as utoipa::PartialSchema>::schema()).unwrap();
    let field = &schema["properties"]["GlobalSecondaryIndexes"];
    assert!(field.is_object());
    assert_eq!(
        field,
        &serde_json::json!({"additionalProperties":{"$ref":"#/components/schemas/ConsumedCapacityMetrics"},"propertyNames":{"type":"string"},"type":["object","null"]})
    );
}

#[test]
fn given_no_capacity_request_when_rendering_tables_then_table_names_are_not_read() {
    use crate::{CapacityUnitField, ReturnConsumedCapacity, TableName};

    for mode in [None, Some(ReturnConsumedCapacity::None)] {
        let tables = std::iter::from_fn(|| -> Option<&TableName> {
            panic!("capacity names must not be read when no answer is requested")
        });
        assert!(
            ConsumedCapacity::zero_for_tables(mode, CapacityUnitField::Plain, tables).is_none()
        );
    }
}

#[test]
fn given_repeated_table_spellings_when_capacity_is_requested_then_zero_answers_keep_first_spelling()
{
    use crate::{CapacityUnitField, ReturnConsumedCapacity, TableName};

    let names = [
        TableName::new("a"),
        TableName::new("arn:aws:dynamodb:us-east-1:123456789012:table/b"),
        TableName::new("b"),
    ];
    let response = ConsumedCapacity::zero_for_tables(
        Some(ReturnConsumedCapacity::Indexes),
        CapacityUnitField::Read,
        &names,
    );
    assert_eq!(
        serde_json::to_value(response).unwrap(),
        serde_json::json!([
            {"TableName": "arn:aws:dynamodb:us-east-1:123456789012:table/b",
             "CapacityUnits": 0.0, "ReadCapacityUnits": 0.0,
             "Table": {"CapacityUnits": 0.0, "ReadCapacityUnits": 0.0}},
            {"TableName": "a", "CapacityUnits": 0.0, "ReadCapacityUnits": 0.0,
             "Table": {"CapacityUnits": 0.0, "ReadCapacityUnits": 0.0}}
        ])
    );
}
