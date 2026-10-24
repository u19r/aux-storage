use serde_json::json;

use crate::serde_types::reject_unknown_fields;

#[test]
fn reject_unknown_fields_reports_first_unknown_key() {
    let err = reject_unknown_fields(
        &json!({ "TableName": "users", "Unexpected": true }),
        &["TableName"],
    )
    .expect_err("unknown field should fail");

    assert_eq!(err, "Invalid request format: unknown field 'Unexpected'");
}

#[test]
fn given_invalid_put_options_when_normalized_then_messages_match_the_wire_contract() {
    use crate::{
        DynamoRequestValidate as _, PutItemRequest, StorageEnum, context::WrappedError as _,
    };

    for payload in [
        json!({"TableName": "", "Item": {"pk": {"S": "p"}}}),
        json!({"TableName": "items", "Item": {"pk": {"S": "p"}}, "ReturnItemCollectionMetrics": "invalid"}),
        json!({"TableName": "items", "Item": {"pk": {"S": "p"}}, "ConditionExpression": "#missing = :missing"}),
    ] {
        let request: PutItemRequest = serde_json::from_value(payload).unwrap();
        let expected = request.validate_for_dynamodb().unwrap_err();
        let error = request.normalized_for_dynamodb().unwrap_err();
        assert!(
            matches!(error.to_enum(), StorageEnum::RawValidation { message } if message == &expected),
            "{error:?} != {expected}"
        );
    }
}

#[test]
fn given_valid_put_when_normalized_then_numbers_are_canonical_and_write_options_survive() {
    use crate::{AttributeValue, PutItemRequest};
    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": "items", "Item": {"pk": {"S": "p"}, "number": {"N": "1e2"}},
        "ConditionExpression": "attribute_not_exists(pk)",
        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
    }))
    .unwrap();
    let (request, size) = request.normalized_for_dynamodb().unwrap();
    assert_eq!(
        request.item.attribute_value("number").unwrap(),
        Some(AttributeValue::N("100".into()))
    );
    assert_eq!(size, request.item.logical_size().unwrap());
    assert_eq!(
        request.condition_expression.as_deref(),
        Some("attribute_not_exists(pk)")
    );
    assert_eq!(
        request.return_values_on_condition_check_failure.as_deref(),
        Some("ALL_OLD")
    );
}

#[test]
fn given_bound_query_values_when_operand_rules_change_then_value_validation_rejects_them() {
    use crate::{DynamoRequestValidate as _, QueryRequest};
    for payload in [
        json!({"TableName": "items", "KeyConditionExpression": "pk = :pk", "FilterExpression": "begins_with(note, :prefix)", "ExpressionAttributeValues": {":pk": {"S": "p"}, ":prefix": {"N": "2"}}}),
        json!({"TableName": "items", "KeyConditionExpression": "pk = :pk", "FilterExpression": "attribute_type(note, :kind)", "ExpressionAttributeValues": {":pk": {"S": "p"}, ":kind": {"S": "invalid"}}}),
    ] {
        let request: QueryRequest = serde_json::from_value(payload).unwrap();
        assert_eq!(
            request
                .validate_bound_values()
                .expect_err(&format!("invalid bound request: {request:?}")),
            request.validate_for_dynamodb().unwrap_err()
        );
    }
}
