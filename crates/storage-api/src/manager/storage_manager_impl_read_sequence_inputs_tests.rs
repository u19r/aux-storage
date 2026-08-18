use std::collections::BTreeMap;

use storage_types::{
    AttributeValue, GetItemRequest, KeyAttributes, QueryRequest, ReadSequenceInputReference,
    ReadSequenceNodeOperation, ReadSequenceValidationError, TableName,
    read_sequence_string_template,
};

use super::storage_manager_impl_read_sequence_inputs::{ResolvedInput, bind_operation};

#[test]
fn string_template_composes_a_key_from_multiple_inputs() {
    let operation = ReadSequenceNodeOperation::Get(GetItemRequest::new(
        TableName::new("application_data"),
        KeyAttributes::from([
            (
                "pk".to_string(),
                read_sequence_string_template("entity#{id}#sub_model#{sub_id}#v1"),
            ),
            ("sk".to_string(), AttributeValue::S("metadata".to_string())),
        ]),
    ));
    let inputs = BTreeMap::from([
        ("id".to_string(), resolved_string("42")),
        ("sub_id".to_string(), resolved_string("7")),
    ]);

    let ReadSequenceNodeOperation::Get(bound) =
        bind_operation(&operation, &inputs).expect("bind template")
    else {
        panic!("expected Get");
    };

    assert_eq!(
        bound.key.get("pk"),
        Some(&AttributeValue::S("entity#42#sub_model#7#v1".to_string()))
    );
}

#[test]
fn string_template_rejects_non_string_inputs_without_coercion() {
    let operation = ReadSequenceNodeOperation::Get(GetItemRequest::new(
        TableName::new("application_data"),
        KeyAttributes::from([(
            "pk".to_string(),
            read_sequence_string_template("entity#{id}"),
        )]),
    ));
    let inputs = BTreeMap::from([(
        "id".to_string(),
        ResolvedInput {
            value: AttributeValue::N("42".to_string()),
            reference: reference(),
        },
    )]);

    assert!(matches!(
        bind_operation(&operation, &inputs),
        Err(ReadSequenceValidationError::InputType {
            input,
            expected,
            actual,
            ..
        }) if input == "id" && expected == "S" && actual == "N"
    ));
}

#[test]
fn string_template_binds_query_expression_values() {
    let mut request = QueryRequest::new(
        TableName::new("application_data"),
        "gsi1pk = :entity".to_string(),
    );
    request.expression_attribute_values = Some(std::collections::HashMap::from([(
        ":entity".to_string(),
        read_sequence_string_template("entity#{id}"),
    )]));
    let operation = ReadSequenceNodeOperation::Query(request);
    let inputs = BTreeMap::from([("id".to_string(), resolved_string("42"))]);

    let ReadSequenceNodeOperation::Query(bound) =
        bind_operation(&operation, &inputs).expect("bind query template")
    else {
        panic!("expected Query");
    };
    assert_eq!(
        bound
            .expression_attribute_values
            .as_ref()
            .and_then(|values| values.get(":entity")),
        Some(&AttributeValue::S("entity#42".to_string()))
    );
}

fn resolved_string(value: &str) -> ResolvedInput {
    ResolvedInput {
        value: AttributeValue::S(value.to_string()),
        reference: reference(),
    }
}

fn reference() -> ReadSequenceInputReference {
    ReadSequenceInputReference {
        node: "source".to_string(),
        invocation_ordinal: 0,
        item_ordinal: None,
    }
}
