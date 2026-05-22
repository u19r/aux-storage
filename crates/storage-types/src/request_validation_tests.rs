use std::convert::TryInto;

use serde_json::json;

use crate::{
    BatchGetItemRequest, BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest,
    DeleteTableRequest, DescribeTableRequest, GetItemRequest, GetStreamRecordsRequest,
    ListTablesRequest, PutItemRequest, QueryRequest, ScanRequest, TransactGetItemsRequest,
    TransactWriteItemsRequest, UpdateItemRequest, UpdateTableRequest,
};

fn valid_create_table_payload() -> serde_json::Value {
    json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            { "AttributeName": "pk", "AttributeType": "S" },
            { "AttributeName": "sk", "AttributeType": "S" }
        ],
        "KeySchema": [
            { "AttributeName": "pk", "KeyType": "HASH" },
            { "AttributeName": "sk", "KeyType": "RANGE" }
        ],
        "BillingMode": "PAY_PER_REQUEST"
    })
}

#[test]
fn get_item_rejects_unknown_fields() {
    let payload = json!({
        "TableName": "TestTable",
        "Key": {"id": {"S": "test123"}},
        "InvalidField": "invalid",
    });

    let result: Result<GetItemRequest, String> = payload.try_into();
    let err = result.expect_err("GetItem should reject unknown fields");
    assert!(err.contains("unknown field"), "unexpected error: {err}");
}

#[test]
fn put_item_rejects_unknown_fields() {
    let payload = json!({
        "TableName": "TestTable",
        "Item": {"id": {"S": "test123"}},
        "InvalidField": "invalid",
    });

    let result: Result<PutItemRequest, String> = payload.try_into();
    let err = result.expect_err("PutItem should reject unknown fields");
    assert!(err.contains("unknown field"), "unexpected error: {err}");
}

#[test]
fn delete_item_rejects_unknown_fields() {
    let payload = json!({
        "TableName": "TestTable",
        "Key": {"id": {"S": "test123"}},
        "InvalidField": "invalid",
    });

    let result: Result<DeleteItemRequest, String> = payload.try_into();
    let err = result.expect_err("DeleteItem should reject unknown fields");
    assert!(err.contains("unknown field"), "unexpected error: {err}");
}

#[test]
fn scan_rejects_unknown_fields() {
    let payload = json!({
        "TableName": "TestTable",
        "InvalidField": "invalid",
    });

    let result: Result<ScanRequest, String> = payload.try_into();
    let err = result.expect_err("Scan should reject unknown fields");
    assert!(err.contains("unknown field"), "unexpected error: {err}");
}

#[test]
fn query_rejects_unknown_fields() {
    let payload = json!({
        "TableName": "TestTable",
        "KeyConditionExpression": "pk = :pk_val",
        "ExpressionAttributeValues": {":pk_val": {"S": "test"}},
        "InvalidField": "invalid",
    });

    let result: Result<QueryRequest, String> = payload.try_into();
    let err = result.expect_err("Query should reject unknown fields");
    assert!(err.contains("unknown field"), "unexpected error: {err}");
}

#[test]
fn create_table_rejects_invalid_table_and_key_shapes() {
    let cases = vec![
        (
            "whitespace table name",
            {
                let mut payload = valid_create_table_payload();
                payload["TableName"] = json!("   ");
                payload
            },
            "TableName cannot be empty",
        ),
        (
            "table name with invalid characters",
            {
                let mut payload = valid_create_table_payload();
                payload["TableName"] = json!("bad/table");
                payload
            },
            "TableName contains invalid characters",
        ),
        (
            "empty attribute definitions",
            {
                let mut payload = valid_create_table_payload();
                payload["AttributeDefinitions"] = json!([]);
                payload
            },
            "AttributeDefinitions cannot be empty",
        ),
        (
            "empty attribute name",
            {
                let mut payload = valid_create_table_payload();
                payload["AttributeDefinitions"][0]["AttributeName"] = json!("");
                payload
            },
            "AttributeName in AttributeDefinitions cannot be empty",
        ),
        (
            "invalid attribute name characters",
            {
                let mut payload = valid_create_table_payload();
                payload["AttributeDefinitions"][0]["AttributeName"] = json!("bad/name");
                payload
            },
            "AttributeName in AttributeDefinitions contains invalid characters",
        ),
        (
            "empty key schema",
            {
                let mut payload = valid_create_table_payload();
                payload["KeySchema"] = json!([]);
                payload
            },
            "KeySchema cannot be empty",
        ),
        (
            "too many key schema elements",
            {
                let mut payload = valid_create_table_payload();
                payload["KeySchema"] = json!([
                    { "AttributeName": "pk", "KeyType": "HASH" },
                    { "AttributeName": "sk", "KeyType": "RANGE" },
                    { "AttributeName": "gsi1pk", "KeyType": "RANGE" }
                ]);
                payload
            },
            "KeySchema cannot have more than 2 elements",
        ),
        (
            "duplicate hash key",
            {
                let mut payload = valid_create_table_payload();
                payload["KeySchema"] = json!([
                    { "AttributeName": "pk", "KeyType": "HASH" },
                    { "AttributeName": "sk", "KeyType": "HASH" }
                ]);
                payload
            },
            "KeySchema can only have one HASH key",
        ),
        (
            "missing hash key",
            {
                let mut payload = valid_create_table_payload();
                payload["KeySchema"] = json!([
                    { "AttributeName": "sk", "KeyType": "RANGE" }
                ]);
                payload
            },
            "KeySchema must have a HASH key",
        ),
        (
            "missing attribute definition for key",
            {
                let mut payload = valid_create_table_payload();
                payload["KeySchema"] = json!([
                    { "AttributeName": "missing_pk", "KeyType": "HASH" }
                ]);
                payload
            },
            "Key attribute 'missing_pk' not found in AttributeDefinitions",
        ),
        (
            "too many gsis",
            {
                let mut payload = valid_create_table_payload();
                payload["GlobalSecondaryIndexes"] = serde_json::Value::Array(
                    (0..21)
                        .map(|idx| {
                            json!({
                                "IndexName": format!("gsi_{idx}"),
                                "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                                "Projection": { "ProjectionType": "ALL" }
                            })
                        })
                        .collect(),
                );
                payload
            },
            "Cannot have more than 20 Global Secondary Indexes",
        ),
    ];

    for (case_name, payload, expected_error) in cases {
        let result: Result<CreateTableRequest, String> = payload.try_into();
        let err = result.expect_err(case_name);
        assert!(
            err.contains(expected_error),
            "{case_name}: expected error containing '{expected_error}', got '{err}'"
        );
    }
}

#[test]
fn delete_and_describe_table_reject_empty_table_names() {
    let payload = json!({ "TableName": "" });

    let delete_result: Result<DeleteTableRequest, String> = payload.clone().try_into();
    let describe_result: Result<DescribeTableRequest, String> = payload.try_into();

    assert_eq!(
        delete_result.expect_err("delete table should fail"),
        "TableName cannot be empty"
    );
    assert_eq!(
        describe_result.expect_err("describe table should fail"),
        "TableName cannot be empty"
    );
}

#[test]
fn get_stream_records_rejects_limits_outside_supported_range() {
    let too_small = json!({
        "TableName": "TestTable",
        "Limit": 0
    });
    let too_large = json!({
        "TableName": "TestTable",
        "Limit": 1001
    });

    let too_small_err =
        GetStreamRecordsRequest::try_from(too_small).expect_err("limit below minimum should fail");
    let too_large_err =
        GetStreamRecordsRequest::try_from(too_large).expect_err("limit above maximum should fail");

    assert_eq!(too_small_err, crate::DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE);
    assert_eq!(too_large_err, crate::DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE);
}

#[test]
fn batch_and_transaction_requests_reject_structurally_invalid_operations() {
    let batch_write_both_payload = json!({
        "RequestItems": {
            "TestTable": [{
                "PutRequest": { "Item": { "pk": { "S": "1" } } },
                "DeleteRequest": { "Key": { "pk": { "S": "1" } } }
            }]
        }
    });
    let batch_get_empty_key_payload = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{}]
            }
        }
    });
    let transact_write_multiple_ops_payload = json!({
        "TransactItems": [{
            "Put": {
                "TableName": "TestTable",
                "Item": { "pk": { "S": "1" } }
            },
            "Delete": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } }
            }
        }]
    });

    let batch_write_err = BatchWriteItemRequest::try_from(batch_write_both_payload)
        .expect_err("batch write with two operations should fail");
    let batch_get_err = BatchGetItemRequest::try_from(batch_get_empty_key_payload)
        .expect_err("batch get with empty key should fail");
    let transact_err = TransactWriteItemsRequest::try_from(transact_write_multiple_ops_payload)
        .expect_err("transact write item with multiple operations should fail");

    assert!(batch_write_err.contains("exactly one of PutRequest or DeleteRequest"));
    assert_eq!(batch_get_err, "Key cannot be empty");
    assert_eq!(
        transact_err,
        "TransactItems can only contain one of Check, Put, Update or Delete"
    );
}

#[test]
fn update_table_rejects_invalid_gsi_update_shapes() {
    let empty_index_name_payload = json!({
        "TableName": "TestTable",
        "GlobalSecondaryIndexUpdates": [{
            "Update": { "IndexName": "" }
        }]
    });
    let missing_create_key_schema_payload = json!({
        "TableName": "TestTable",
        "GlobalSecondaryIndexUpdates": [{
            "Create": {
                "IndexName": "gsi_1",
                "KeySchema": [],
                "Projection": { "ProjectionType": "ALL" }
            }
        }]
    });

    let update_err = UpdateTableRequest::try_from(empty_index_name_payload)
        .expect_err("blank update index name should fail");
    let create_err = UpdateTableRequest::try_from(missing_create_key_schema_payload)
        .expect_err("missing create key schema should fail");

    assert!(update_err.contains("Update.IndexName cannot be empty"));
    assert!(create_err.contains("Create.KeySchema cannot be empty"));
}

#[test]
fn update_item_rejects_unknown_fields_and_missing_identity() {
    let unknown_field_payload = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #name = :value",
        "UnknownField": true
    });
    let empty_table_payload = json!({
        "TableName": "",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #name = :value"
    });
    let empty_key_payload = json!({
        "TableName": "TestTable",
        "Key": {},
        "UpdateExpression": "SET #name = :value"
    });

    let unknown_field_err =
        UpdateItemRequest::try_from(unknown_field_payload).expect_err("unknown field should fail");
    let empty_table_err =
        UpdateItemRequest::try_from(empty_table_payload).expect_err("empty table should fail");
    let empty_key_err =
        UpdateItemRequest::try_from(empty_key_payload).expect_err("empty key should fail");

    assert!(unknown_field_err.contains("unknown field"));
    assert_eq!(empty_table_err, "TableName cannot be empty");
    assert_eq!(empty_key_err, "Key cannot be empty");
}

#[test]
fn update_item_rejects_missing_and_invalid_expression_attribute_values_with_dynamodb_messages() {
    let missing_value_payload = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #name = :missing",
        "ExpressionAttributeNames": { "#name": "name" }
    });
    let invalid_value_key_payload = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #name = :value",
        "ExpressionAttributeNames": { "#name": "name" },
        "ExpressionAttributeValues": { "value": { "S": "name" } }
    });

    let missing_value_err =
        UpdateItemRequest::try_from(missing_value_payload).expect_err("missing value should fail");
    let invalid_value_key_err = UpdateItemRequest::try_from(invalid_value_key_payload)
        .expect_err("invalid expression value key should fail");

    assert_eq!(
        missing_value_err,
        "1 validation error detected: Invalid UpdateExpression: An expression attribute value \
         used in expression is not defined; attribute value: :missing"
    );
    assert_eq!(
        invalid_value_key_err,
        "1 validation error detected: ExpressionAttributeValues contains invalid key: Syntax \
         error; key: \"value\""
    );
}

#[test]
fn update_item_rejects_missing_and_invalid_expression_attribute_names_with_dynamodb_messages() {
    let missing_name_payload = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #name = :value",
        "ExpressionAttributeValues": { ":value": { "S": "name" } }
    });
    let invalid_name_key_payload = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #name = :value",
        "ExpressionAttributeNames": { "name": "name" },
        "ExpressionAttributeValues": { ":value": { "S": "name" } }
    });

    let missing_name_err =
        UpdateItemRequest::try_from(missing_name_payload).expect_err("missing name should fail");
    let invalid_name_key_err = UpdateItemRequest::try_from(invalid_name_key_payload)
        .expect_err("invalid expression name key should fail");

    assert_eq!(
        missing_name_err,
        "1 validation error detected: Invalid UpdateExpression: An expression attribute name used \
         in the document path is not defined; attribute name: #name"
    );
    assert_eq!(
        invalid_name_key_err,
        "1 validation error detected: ExpressionAttributeNames contains invalid key: Syntax \
         error; key: \"name\""
    );
}

#[test]
fn scan_rejects_missing_expression_values_and_invalid_filter_syntax() {
    let missing_values_payload = json!({
        "TableName": "TestTable",
        "FilterExpression": "pk = :pk"
    });
    let invalid_filter_payload = json!({
        "TableName": "TestTable",
        "FilterExpression": "invalid filter"
    });

    let missing_values_err = ScanRequest::try_from(missing_values_payload)
        .expect_err("missing expression values should fail");
    let invalid_filter_err = ScanRequest::try_from(invalid_filter_payload)
        .expect_err("invalid filter syntax should fail");

    assert_eq!(
        missing_values_err,
        "Invalid FilterExpression: An expression attribute value used in expression is not \
         defined; attribute value: :pk"
    );
    assert_eq!(invalid_filter_err, "Invalid FilterExpression");
}

#[test]
fn query_and_scan_reject_expression_attribute_errors_with_dynamodb_messages() {
    let query_missing_value = json!({
        "TableName": "TestTable",
        "KeyConditionExpression": "pk = :pk"
    });
    let query_invalid_value_key = json!({
        "TableName": "TestTable",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": { "pk": { "S": "1" } }
    });
    let query_missing_name = json!({
        "TableName": "TestTable",
        "KeyConditionExpression": "pk = :pk",
        "ProjectionExpression": "#name",
        "ExpressionAttributeValues": { ":pk": { "S": "1" } }
    });
    let scan_invalid_name_key = json!({
        "TableName": "TestTable",
        "ProjectionExpression": "#name",
        "ExpressionAttributeNames": { "name": "name" }
    });
    let scan_unused_name = json!({
        "TableName": "TestTable",
        "ProjectionExpression": "#name",
        "ExpressionAttributeNames": { "#name": "name", "#unused": "unused" }
    });
    let get_reserved_comment = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ProjectionExpression": "COMMENT"
    });
    let get_reserved_name_lowercase = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ProjectionExpression": "name"
    });
    let get_reserved_update_keyword = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ProjectionExpression": "ADD"
    });
    let get_reserved_alias = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ProjectionExpression": "#name",
        "ExpressionAttributeNames": { "#name": "name" }
    });

    assert_eq!(
        QueryRequest::try_from(query_missing_value).expect_err("missing value should fail"),
        "Invalid KeyConditionExpression: An expression attribute value used in expression is not \
         defined; attribute value: :pk"
    );
    assert_eq!(
        QueryRequest::try_from(query_invalid_value_key).expect_err("invalid value key should fail"),
        "ExpressionAttributeValues contains invalid key: Syntax error; key: \"pk\""
    );
    assert_eq!(
        QueryRequest::try_from(query_missing_name).expect_err("missing name should fail"),
        "Invalid ProjectionExpression: An expression attribute name used in the document path is \
         not defined; attribute name: #name"
    );
    assert_eq!(
        ScanRequest::try_from(scan_invalid_name_key).expect_err("invalid name key should fail"),
        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"name\""
    );
    assert_eq!(
        ScanRequest::try_from(scan_unused_name).expect_err("unused name should fail"),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}"
    );
    assert_eq!(
        GetItemRequest::try_from(get_reserved_comment).expect_err("reserved word should fail"),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert_eq!(
        GetItemRequest::try_from(get_reserved_name_lowercase)
            .expect_err("lowercase reserved word should fail"),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         name"
    );
    assert_eq!(
        GetItemRequest::try_from(get_reserved_update_keyword)
            .expect_err("reserved update keyword in projection should fail"),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: ADD"
    );
    assert!(GetItemRequest::try_from(get_reserved_alias).is_ok());
}

#[test]
fn write_condition_expressions_reject_attribute_errors_with_dynamodb_prefix() {
    let put_missing_value = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "attribute_exists(:value)"
    });
    let delete_invalid_name_key = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ConditionExpression": "#name = :value",
        "ExpressionAttributeNames": { "name": "name" },
        "ExpressionAttributeValues": { ":value": { "S": "1" } }
    });
    let delete_reserved_word = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ConditionExpression": "COUNT = :value",
        "ExpressionAttributeValues": { ":value": { "S": "1" } }
    });
    let put_single_parenthesized_condition = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "(#name = :value)",
        "ExpressionAttributeNames": { "#name": "name" },
        "ExpressionAttributeValues": { ":value": { "S": "1" } }
    });
    let put_double_parenthesized_condition = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "((#name = :value))",
        "ExpressionAttributeNames": { "#name": "name" },
        "ExpressionAttributeValues": { ":value": { "S": "1" } }
    });
    let put_double_parenthesized_operand = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "((#name)) = :value",
        "ExpressionAttributeNames": { "#name": "name" },
        "ExpressionAttributeValues": { ":value": { "S": "1" } }
    });
    let put_nested_reserved_word = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "m.COMMENT = :value",
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let put_function_nested_reserved_word = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "begins_with(m.COMMENT, :value)",
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let put_function_alias_nested_reserved_word = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "begins_with(#m.#comment, :value)",
        "ExpressionAttributeNames": {
            "#m": "m",
            "#comment": "COMMENT"
        },
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let put_contains_same_operand = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "contains(name, name)"
    });
    let put_attribute_type_literal = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "attribute_type(#n, S)",
        "ExpressionAttributeNames": { "#n": "name" }
    });
    let put_attribute_type_invalid_type_name = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "attribute_type(#n, :type)",
        "ExpressionAttributeNames": { "#n": "name" },
        "ExpressionAttributeValues": { ":type": { "S": "STRING" } }
    });
    let put_begins_with_number_operand = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "begins_with(#n, :prefix)",
        "ExpressionAttributeNames": { "#n": "name" },
        "ExpressionAttributeValues": { ":prefix": { "N": "1" } }
    });
    let put_contains_one_arg = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "contains(#n)",
        "ExpressionAttributeNames": { "#n": "name" }
    });
    let put_begins_with_three_args = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "begins_with(#n, :prefix, :extra)",
        "ExpressionAttributeNames": { "#n": "name" },
        "ExpressionAttributeValues": {
            ":prefix": { "S": "a" },
            ":extra": { "S": "x" }
        }
    });
    let put_attribute_type_no_args = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "attribute_type()"
    });
    let put_size_two_args = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "size(#n, #m) = :size",
        "ExpressionAttributeNames": {
            "#n": "name",
            "#m": "more"
        },
        "ExpressionAttributeValues": { ":size": { "N": "4" } }
    });
    let put_size_no_comparison = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "size(#n)",
        "ExpressionAttributeNames": { "#n": "name" }
    });
    let put_size_nested_function_arg = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "size(contains(#n, :value)) = :size",
        "ExpressionAttributeNames": { "#n": "name" },
        "ExpressionAttributeValues": {
            ":value": { "S": "a" },
            ":size": { "N": "4" }
        }
    });
    let put_size_greater_than = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "size(#n) > :size",
        "ExpressionAttributeNames": { "#n": "name" },
        "ExpressionAttributeValues": { ":size": { "N": "3" } }
    });
    let put_between_number_bounds_reversed = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "pk BETWEEN :lower AND :upper",
        "ExpressionAttributeValues": {
            ":lower": { "N": "5" },
            ":upper": { "N": "4" }
        }
    });
    let put_between_string_bounds_reversed = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "pk BETWEEN :lower AND :upper",
        "ExpressionAttributeValues": {
            ":lower": { "S": "z" },
            ":upper": { "S": "a" }
        }
    });
    let put_between_binary_bounds_reversed = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "pk BETWEEN :lower AND :upper",
        "ExpressionAttributeValues": {
            ":lower": { "B": "eg==" },
            ":upper": { "B": "YQ==" }
        }
    });
    let put_between_equal_bounds = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "pk BETWEEN :lower AND :upper",
        "ExpressionAttributeValues": {
            ":lower": { "N": "5" },
            ":upper": { "N": "5" }
        }
    });

    assert_eq!(
        PutItemRequest::try_from(put_missing_value).expect_err("missing value should fail"),
        "1 validation error detected: Invalid ConditionExpression: An expression attribute value \
         used in expression is not defined; attribute value: :value"
    );
    assert_eq!(
        DeleteItemRequest::try_from(delete_invalid_name_key)
            .expect_err("invalid name key should fail"),
        "1 validation error detected: ExpressionAttributeNames contains invalid key: Syntax \
         error; key: \"name\""
    );
    assert_eq!(
        DeleteItemRequest::try_from(delete_reserved_word).expect_err("reserved word should fail"),
        "1 validation error detected: Invalid ConditionExpression: Attribute name is a reserved \
         keyword; reserved keyword: COUNT"
    );
    assert!(PutItemRequest::try_from(put_single_parenthesized_condition).is_ok());
    assert_eq!(
        PutItemRequest::try_from(put_double_parenthesized_condition)
            .expect_err("redundant whole-expression parentheses should fail"),
        "1 validation error detected: Invalid ConditionExpression: The expression has redundant \
         parentheses;"
    );
    assert_eq!(
        PutItemRequest::try_from(put_double_parenthesized_operand)
            .expect_err("redundant operand parentheses should fail"),
        "1 validation error detected: Invalid ConditionExpression: The expression has redundant \
         parentheses;"
    );
    assert_eq!(
        PutItemRequest::try_from(put_nested_reserved_word)
            .expect_err("nested reserved word should fail"),
        "1 validation error detected: Invalid ConditionExpression: Attribute name is a reserved \
         keyword; reserved keyword: COMMENT"
    );
    assert_eq!(
        PutItemRequest::try_from(put_function_nested_reserved_word)
            .expect_err("function argument reserved word should fail"),
        "1 validation error detected: Invalid ConditionExpression: Attribute name is a reserved \
         keyword; reserved keyword: COMMENT"
    );
    assert!(PutItemRequest::try_from(put_function_alias_nested_reserved_word).is_ok());
    assert_eq!(
        PutItemRequest::try_from(put_contains_same_operand)
            .expect_err("contains same path and operand should fail"),
        "1 validation error detected: Invalid ConditionExpression: The first operand must be \
         distinct from the remaining operands for this operator or function; operator: contains, \
         first operand: [name]"
    );
    assert_eq!(
        PutItemRequest::try_from(put_attribute_type_literal)
            .expect_err("attribute_type literal type should fail"),
        "1 validation error detected: Invalid ConditionExpression: Incorrect operand type for \
         operator or function; operator or function: attribute_type, operand type: \
         {S,SS,N,NS,B,BS,BOOL,NULL,L,M,HD,DOUBLE,FLOAT,HDS,FS,DOUBLESET,DICT,DECIMAL,INT,\
         DECIMALSET,INTSET}"
    );
    assert_eq!(
        PutItemRequest::try_from(put_attribute_type_invalid_type_name)
            .expect_err("invalid attribute_type code should fail"),
        "1 validation error detected: Invalid ConditionExpression: Invalid attribute type name \
         found; type: STRING, valid types: {S,SS,N,NS,B,BS,BOOL,NULL,L,M}"
    );
    assert_eq!(
        PutItemRequest::try_from(put_begins_with_number_operand)
            .expect_err("begins_with non-string/binary operand should fail"),
        "1 validation error detected: Invalid ConditionExpression: Incorrect operand type for \
         operator or function; operator or function: begins_with, operand type: N"
    );
    assert_eq!(
        PutItemRequest::try_from(put_contains_one_arg).expect_err("contains one arg should fail"),
        "1 validation error detected: Invalid ConditionExpression: Incorrect number of operands \
         for operator or function; operator or function: contains, number of operands: 1"
    );
    assert_eq!(
        PutItemRequest::try_from(put_begins_with_three_args)
            .expect_err("begins_with three args should fail"),
        "1 validation error detected: Invalid ConditionExpression: Incorrect number of operands \
         for operator or function; operator or function: begins_with, number of operands: 3"
    );
    assert_eq!(
        PutItemRequest::try_from(put_attribute_type_no_args)
            .expect_err("attribute_type no args should fail"),
        "1 validation error detected: Invalid ConditionExpression: Syntax error; token: \")\", \
         near: \"()\""
    );
    assert_eq!(
        PutItemRequest::try_from(put_size_two_args).expect_err("size two args should fail"),
        "1 validation error detected: Invalid ConditionExpression: Incorrect number of operands \
         for operator or function; operator or function: size, number of operands: 2"
    );
    assert_eq!(
        PutItemRequest::try_from(put_size_no_comparison)
            .expect_err("size without comparison should fail"),
        "1 validation error detected: Invalid ConditionExpression: The function is not allowed to \
         be used this way in an expression; function: size"
    );
    assert_eq!(
        PutItemRequest::try_from(put_size_nested_function_arg)
            .expect_err("size with function argument should fail"),
        "1 validation error detected: Invalid ConditionExpression: The function is not allowed to \
         be used this way in an expression; function: contains"
    );
    assert!(PutItemRequest::try_from(put_size_greater_than).is_ok());
    assert_eq!(
        PutItemRequest::try_from(put_between_number_bounds_reversed)
            .expect_err("BETWEEN number lower bound greater than upper should fail"),
        "1 validation error detected: Invalid ConditionExpression: The BETWEEN operator requires \
         upper bound to be greater than or equal to lower bound; lower bound operand: \
         AttributeValue: {N:5}, upper bound operand: AttributeValue: {N:4}"
    );
    assert_eq!(
        PutItemRequest::try_from(put_between_string_bounds_reversed)
            .expect_err("BETWEEN string lower bound greater than upper should fail"),
        "1 validation error detected: Invalid ConditionExpression: The BETWEEN operator requires \
         upper bound to be greater than or equal to lower bound; lower bound operand: \
         AttributeValue: {S:z}, upper bound operand: AttributeValue: {S:a}"
    );
    assert_eq!(
        PutItemRequest::try_from(put_between_binary_bounds_reversed)
            .expect_err("BETWEEN binary lower bound greater than upper should fail"),
        "1 validation error detected: Invalid ConditionExpression: The BETWEEN operator requires \
         upper bound to be greater than or equal to lower bound; lower bound operand: \
         AttributeValue: {B:eg==}, upper bound operand: AttributeValue: {B:YQ==}"
    );
    assert!(PutItemRequest::try_from(put_between_equal_bounds).is_ok());
}

#[test]
fn write_condition_expressions_reject_reversed_between_bounds_for_all_write_shapes() {
    let expected = "1 validation error detected: Invalid ConditionExpression: The BETWEEN \
                    operator requires upper bound to be greater than or equal to lower bound; \
                    lower bound operand: AttributeValue: {N:5}, upper bound operand: \
                    AttributeValue: {N:4}";
    let expression_attribute_values = json!({
        ":lower": { "N": "5" },
        ":upper": { "N": "4" }
    });

    let put = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "pk BETWEEN :lower AND :upper",
        "ExpressionAttributeValues": expression_attribute_values.clone()
    });
    let delete = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ConditionExpression": "pk BETWEEN :lower AND :upper",
        "ExpressionAttributeValues": expression_attribute_values.clone()
    });
    let update = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET touched = :touched",
        "ConditionExpression": "pk BETWEEN :lower AND :upper",
        "ExpressionAttributeValues": {
            ":touched": { "S": "yes" },
            ":lower": { "N": "5" },
            ":upper": { "N": "4" }
        }
    });
    let transact_write = json!({
        "TransactItems": [
            {
                "ConditionCheck": {
                    "TableName": "TestTable",
                    "Key": { "pk": { "S": "1" } },
                    "ConditionExpression": "pk BETWEEN :lower AND :upper",
                    "ExpressionAttributeValues": expression_attribute_values
                }
            }
        ]
    });

    assert_eq!(
        PutItemRequest::try_from(put).expect_err("PutItem should reject reversed BETWEEN bounds"),
        expected
    );
    assert_eq!(
        DeleteItemRequest::try_from(delete)
            .expect_err("DeleteItem should reject reversed BETWEEN bounds"),
        expected
    );
    assert_eq!(
        UpdateItemRequest::try_from(update)
            .expect_err("UpdateItem should reject reversed BETWEEN bounds"),
        expected
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_write)
            .expect_err("TransactWriteItems should reject reversed BETWEEN bounds"),
        "Invalid ConditionExpression: The BETWEEN operator requires upper bound to be greater \
         than or equal to lower bound; lower bound operand: AttributeValue: {N:5}, upper bound \
         operand: AttributeValue: {N:4}"
    );
}

#[test]
fn update_and_transaction_expressions_reject_functions_in_wrong_contexts() {
    let update_contains_value = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #r = contains(s, :needle)",
        "ExpressionAttributeNames": { "#r": "result" },
        "ExpressionAttributeValues": { ":needle": { "S": "e" } }
    });
    let update_size_value = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #r = size(s)",
        "ExpressionAttributeNames": { "#r": "result" }
    });
    let update_nested_list_append = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #r = list_append(list_append(l, :tail), :tail2)",
        "ExpressionAttributeNames": { "#r": "result" },
        "ExpressionAttributeValues": {
            ":tail": { "L": [{ "S": "b" }] },
            ":tail2": { "L": [{ "S": "c" }] }
        }
    });
    let update_nested_if_not_exists_first_arg = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #r = if_not_exists(if_not_exists(n, :zero), :one)",
        "ExpressionAttributeNames": { "#r": "result" },
        "ExpressionAttributeValues": {
            ":zero": { "N": "0" },
            ":one": { "N": "1" }
        }
    });
    let update_list_append_if_not_exists = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #r = list_append(if_not_exists(l, :empty), :tail)",
        "ExpressionAttributeNames": { "#r": "result" },
        "ExpressionAttributeValues": {
            ":empty": { "L": [] },
            ":tail": { "L": [{ "S": "b" }] }
        }
    });
    let condition_if_not_exists = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #r = :touch",
        "ConditionExpression": "if_not_exists(s, :fallback) = :fallback",
        "ExpressionAttributeNames": { "#r": "result" },
        "ExpressionAttributeValues": {
            ":touch": { "N": "1" },
            ":fallback": { "S": "fallback" }
        }
    });
    let mixed_update_and_condition_invalid = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #r = contains(s, :needle)",
        "ConditionExpression": "if_not_exists(s, :fallback) = :fallback",
        "ExpressionAttributeNames": { "#r": "result" },
        "ExpressionAttributeValues": {
            ":needle": { "S": "e" },
            ":fallback": { "S": "fallback" }
        }
    });
    let transact_update_contains_value = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "UpdateExpression": "SET #r = contains(s, :needle)",
                "ExpressionAttributeNames": { "#r": "result" },
                "ExpressionAttributeValues": { ":needle": { "S": "e" } }
            }
        }]
    });
    let transact_condition_if_not_exists = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "UpdateExpression": "SET #r = :touch",
                "ConditionExpression": "if_not_exists(s, :fallback) = :fallback",
                "ExpressionAttributeNames": { "#r": "result" },
                "ExpressionAttributeValues": {
                    ":touch": { "N": "1" },
                    ":fallback": { "S": "fallback" }
                }
            }
        }]
    });
    let transact_mixed_update_and_condition_invalid = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "UpdateExpression": "SET #r = contains(s, :needle)",
                "ConditionExpression": "if_not_exists(s, :fallback) = :fallback",
                "ExpressionAttributeNames": { "#r": "result" },
                "ExpressionAttributeValues": {
                    ":needle": { "S": "e" },
                    ":fallback": { "S": "fallback" }
                }
            }
        }]
    });

    assert_eq!(
        UpdateItemRequest::try_from(update_contains_value)
            .expect_err("contains cannot produce an update value"),
        "1 validation error detected: Invalid UpdateExpression: The function is not allowed in an \
         update expression; function: contains"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_size_value)
            .expect_err("size cannot produce an update value"),
        "1 validation error detected: Invalid UpdateExpression: The function is not allowed in an \
         update expression; function: size"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_nested_list_append)
            .expect_err("nested list_append should fail"),
        "1 validation error detected: Invalid UpdateExpression: The function is not allowed to be \
         used this way in an expression; function: list_append"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_nested_if_not_exists_first_arg)
            .expect_err("if_not_exists first argument must be a path"),
        "1 validation error detected: Invalid UpdateExpression: Operator or function requires a \
         document path; operator or function: if_not_exists"
    );
    assert!(
        UpdateItemRequest::try_from(update_list_append_if_not_exists).is_ok(),
        "DynamoDB accepts if_not_exists as a list_append operand"
    );
    assert_eq!(
        UpdateItemRequest::try_from(condition_if_not_exists)
            .expect_err("if_not_exists cannot be used in a condition expression"),
        "1 validation error detected: Invalid ConditionExpression: The function is not allowed in \
         a condition expression; function: if_not_exists"
    );
    assert_eq!(
        UpdateItemRequest::try_from(mixed_update_and_condition_invalid)
            .expect_err("UpdateItem reports condition-context errors first"),
        "1 validation error detected: Invalid ConditionExpression: The function is not allowed in \
         a condition expression; function: if_not_exists"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_update_contains_value)
            .expect_err("transaction update should reject condition function in update"),
        "Invalid UpdateExpression: The function is not allowed in an update expression; function: \
         contains"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_condition_if_not_exists)
            .expect_err("transaction condition should reject update function in condition"),
        "Invalid ConditionExpression: The function is not allowed in a condition expression; \
         function: if_not_exists"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_mixed_update_and_condition_invalid)
            .expect_err("TransactWriteItems reports update-context errors first"),
        "Invalid UpdateExpression: The function is not allowed in an update expression; function: \
         contains"
    );
}

#[test]
fn update_and_transaction_expressions_reject_update_action_grammar_errors() {
    let update_duplicate_set = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET fieldx = :s SET otherx = :o",
        "ExpressionAttributeValues": {
            ":s": { "S": "new" },
            ":o": { "S": "other" }
        }
    });
    let update_repeated_path = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET fieldx = :s, fieldx = :o",
        "ExpressionAttributeValues": {
            ":s": { "S": "new" },
            ":o": { "S": "other" }
        }
    });
    let update_overlapping_path = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET mapx = :m, mapx.num = :n",
        "ExpressionAttributeValues": {
            ":m": { "M": {} },
            ":n": { "N": "2" }
        }
    });
    let update_overlapping_list_index_path = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET lst[0] = :value, lst[0].ok = :value",
        "ExpressionAttributeValues": { ":value": { "S": "new" } }
    });
    let update_overlapping_alias_path = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #attribute = :value, fieldx = :value",
        "ExpressionAttributeNames": { "#attribute": "fieldx" },
        "ExpressionAttributeValues": { ":value": { "S": "new" } }
    });
    let update_add_no_value = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "ADD numx"
    });
    let update_set_missing_arithmetic_operand = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET note = :v +",
        "ExpressionAttributeValues": { ":v": { "N": "1" } }
    });
    let update_set_unsupported_arithmetic_operator = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET numx = numx * :v",
        "ExpressionAttributeValues": { ":v": { "N": "2" } }
    });
    let update_delete_no_value = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "DELETE setx"
    });
    let update_add_literal_value = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "ADD numx 1"
    });
    let update_delete_literal_value = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "DELETE setx a"
    });
    let update_add_list_value = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "ADD numx :list",
        "ExpressionAttributeValues": { ":list": { "L": [{ "N": "1" }] } }
    });
    let update_delete_string_value = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "DELETE setx :s",
        "ExpressionAttributeValues": { ":s": { "S": "a" } }
    });
    let update_order_add_then_set = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "ADD numx :inc SET fieldx = :s",
        "ExpressionAttributeValues": {
            ":inc": { "N": "1" },
            ":s": { "S": "new" }
        }
    });
    let update_add_nested_path = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "ADD mapx.num :inc",
        "ExpressionAttributeValues": { ":inc": { "N": "1" } }
    });
    let update_delete_nested_path = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "DELETE mapx.colors :rm",
        "ExpressionAttributeValues": { ":rm": { "SS": ["a"] } }
    });
    let update_alias_reserved_nested_path = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET mapx.#comment = :value",
        "ExpressionAttributeNames": { "#comment": "COMMENT" },
        "ExpressionAttributeValues": { ":value": { "S": "new" } }
    });
    let update_list_alias_reserved_child = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET lst[0].#comment = :value",
        "ExpressionAttributeNames": { "#comment": "COMMENT" },
        "ExpressionAttributeValues": { ":value": { "S": "new" } }
    });
    let transact_duplicate_set = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "UpdateExpression": "SET fieldx = :s SET otherx = :o",
                "ExpressionAttributeValues": {
                    ":s": { "S": "new" },
                    ":o": { "S": "other" }
                }
            }
        }]
    });
    let transact_repeated_path = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "UpdateExpression": "SET fieldx = :s, fieldx = :o",
                "ExpressionAttributeValues": {
                    ":s": { "S": "new" },
                    ":o": { "S": "other" }
                }
            }
        }]
    });
    let transact_overlapping_list_index_path = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "UpdateExpression": "SET lst[0] = :value, lst[0].ok = :value",
                "ExpressionAttributeValues": { ":value": { "S": "new" } }
            }
        }]
    });
    let transact_set_missing_arithmetic_operand = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "UpdateExpression": "SET note = :v +",
                "ExpressionAttributeValues": { ":v": { "N": "1" } }
            }
        }]
    });
    let transact_set_unsupported_arithmetic_operator = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "UpdateExpression": "SET numx = numx / :v",
                "ExpressionAttributeValues": { ":v": { "N": "2" } }
            }
        }]
    });

    assert_eq!(
        UpdateItemRequest::try_from(update_duplicate_set)
            .expect_err("duplicate SET section should fail"),
        "1 validation error detected: Invalid UpdateExpression: The \"SET\" section can only be \
         used once in an update expression;"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_repeated_path)
            .expect_err("repeated update path should fail"),
        "1 validation error detected: Invalid UpdateExpression: Two document paths overlap with \
         each other; must remove or rewrite one of these paths; path one: [fieldx], path two: \
         [fieldx]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_overlapping_path)
            .expect_err("overlapping update paths should fail"),
        "1 validation error detected: Invalid UpdateExpression: Two document paths overlap with \
         each other; must remove or rewrite one of these paths; path one: [mapx], path two: \
         [mapx, num]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_overlapping_list_index_path)
            .expect_err("overlapping list-index update paths should fail"),
        "1 validation error detected: Invalid UpdateExpression: Two document paths overlap with \
         each other; must remove or rewrite one of these paths; path one: [lst, [0]], path two: \
         [lst, [0], ok]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_overlapping_alias_path)
            .expect_err("alias-resolved overlapping update paths should fail"),
        "1 validation error detected: Invalid UpdateExpression: Two document paths overlap with \
         each other; must remove or rewrite one of these paths; path one: [fieldx], path two: \
         [fieldx]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_add_no_value)
            .expect_err("ADD without value should fail"),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"<EOF>\", \
         near: \"numx\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_set_missing_arithmetic_operand)
            .expect_err("SET arithmetic without right operand should fail"),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"<EOF>\", \
         near: \"+\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_set_unsupported_arithmetic_operator)
            .expect_err("SET multiplication should fail"),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"*\", near: \
         \"numx * :v\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_delete_no_value)
            .expect_err("DELETE without value should fail"),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"<EOF>\", \
         near: \"setx\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_add_literal_value)
            .expect_err("ADD literal value should fail"),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"1\", near: \
         \"numx 1\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_delete_literal_value)
            .expect_err("DELETE literal value should fail"),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"a\", near: \
         \"setx a\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_add_list_value).expect_err("ADD list value should fail"),
        "1 validation error detected: Invalid UpdateExpression: Incorrect operand type for \
         operator or function; operator: ADD, operand type: LIST, typeSet: ALLOWED_FOR_ADD_OPERAND"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_delete_string_value)
            .expect_err("DELETE string value should fail"),
        "1 validation error detected: Invalid UpdateExpression: Incorrect operand type for \
         operator or function; operator: DELETE, operand type: STRING, typeSet: \
         ALLOWED_FOR_ADD_OPERAND"
    );
    assert!(UpdateItemRequest::try_from(update_order_add_then_set).is_ok());
    assert!(UpdateItemRequest::try_from(update_add_nested_path).is_ok());
    assert!(UpdateItemRequest::try_from(update_delete_nested_path).is_ok());
    assert!(UpdateItemRequest::try_from(update_alias_reserved_nested_path).is_ok());
    assert!(UpdateItemRequest::try_from(update_list_alias_reserved_child).is_ok());
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_duplicate_set)
            .expect_err("transaction duplicate SET section should fail"),
        "Invalid UpdateExpression: The \"SET\" section can only be used once in an update \
         expression;"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_repeated_path)
            .expect_err("transaction repeated update path should fail"),
        "Invalid UpdateExpression: Two document paths overlap with each other; must remove or \
         rewrite one of these paths; path one: [fieldx], path two: [fieldx]"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_overlapping_list_index_path)
            .expect_err("transaction overlapping list-index update paths should fail"),
        "Invalid UpdateExpression: Two document paths overlap with each other; must remove or \
         rewrite one of these paths; path one: [lst, [0]], path two: [lst, [0], ok]"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_set_missing_arithmetic_operand)
            .expect_err("transaction SET arithmetic without right operand should fail"),
        "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"+\""
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_set_unsupported_arithmetic_operator)
            .expect_err("transaction SET division should fail"),
        "Invalid UpdateExpression: Syntax error; token: \"/\", near: \"numx / :v\""
    );
}

#[test]
fn list_index_document_paths_match_dynamodb_expression_validation() {
    let get_raw_reserved_list = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ProjectionExpression": "list[0].COMMENT"
    });
    let get_raw_reserved_child = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ProjectionExpression": "#list[0].COMMENT",
        "ExpressionAttributeNames": { "#list": "list" }
    });
    let get_alias_list_path = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ProjectionExpression": "#list[0].#comment",
        "ExpressionAttributeNames": {
            "#list": "list",
            "#comment": "COMMENT"
        }
    });
    let get_empty_index = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ProjectionExpression": "#list[].#comment",
        "ExpressionAttributeNames": {
            "#list": "list",
            "#comment": "COMMENT"
        }
    });
    let delete_raw_reserved_child = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ConditionExpression": "#list[0].COMMENT = :value",
        "ExpressionAttributeNames": { "#list": "list" },
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let delete_alias_list_path = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ConditionExpression": "#list[0].#comment = :value",
        "ExpressionAttributeNames": {
            "#list": "list",
            "#comment": "COMMENT"
        },
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let delete_alpha_index = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ConditionExpression": "#list[abc].#comment = :value",
        "ExpressionAttributeNames": {
            "#list": "list",
            "#comment": "COMMENT"
        },
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let scan_raw_reserved_child = json!({
        "TableName": "TestTable",
        "FilterExpression": "#list[0].COMMENT = :value",
        "ExpressionAttributeNames": { "#list": "list" },
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let scan_alias_list_path = json!({
        "TableName": "TestTable",
        "FilterExpression": "#list[0].#comment = :value",
        "ExpressionAttributeNames": {
            "#list": "list",
            "#comment": "COMMENT"
        },
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let scan_negative_index = json!({
        "TableName": "TestTable",
        "FilterExpression": "#list[-1].#comment = :value",
        "ExpressionAttributeNames": {
            "#list": "list",
            "#comment": "COMMENT"
        },
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let update_raw_reserved_child = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #list[0].COMMENT = :value",
        "ExpressionAttributeNames": { "#list": "list" },
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let update_alias_list_path = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #list[0].#comment = :value",
        "ExpressionAttributeNames": {
            "#list": "list",
            "#comment": "COMMENT"
        },
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });
    let update_unclosed_index = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #list[0.#comment = :value",
        "ExpressionAttributeNames": {
            "#list": "list",
            "#comment": "COMMENT"
        },
        "ExpressionAttributeValues": { ":value": { "S": "nested" } }
    });

    assert_eq!(
        GetItemRequest::try_from(get_raw_reserved_list)
            .expect_err("raw reserved list path should fail"),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         list"
    );
    assert_eq!(
        GetItemRequest::try_from(get_raw_reserved_child)
            .expect_err("raw reserved child path should fail"),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert!(GetItemRequest::try_from(get_alias_list_path).is_ok());
    assert_eq!(
        GetItemRequest::try_from(get_empty_index).expect_err("empty index should fail"),
        "Invalid ProjectionExpression: Syntax error; token: \"]\", near: \"[].\""
    );
    assert_eq!(
        DeleteItemRequest::try_from(delete_raw_reserved_child)
            .expect_err("condition raw reserved child should fail"),
        "1 validation error detected: Invalid ConditionExpression: Attribute name is a reserved \
         keyword; reserved keyword: COMMENT"
    );
    assert!(DeleteItemRequest::try_from(delete_alias_list_path).is_ok());
    assert_eq!(
        DeleteItemRequest::try_from(delete_alpha_index).expect_err("alpha index should fail"),
        "1 validation error detected: Invalid ConditionExpression: Syntax error; token: \"abc\", \
         near: \"[abc]\""
    );
    assert_eq!(
        ScanRequest::try_from(scan_raw_reserved_child)
            .expect_err("filter raw reserved child should fail"),
        "Invalid FilterExpression: Attribute name is a reserved keyword; reserved keyword: COMMENT"
    );
    assert!(ScanRequest::try_from(scan_alias_list_path).is_ok());
    assert_eq!(
        ScanRequest::try_from(scan_negative_index).expect_err("negative index should fail"),
        "Invalid FilterExpression: Syntax error; token: \"-\", near: \"[-1\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_raw_reserved_child)
            .expect_err("update raw reserved child should fail"),
        "1 validation error detected: Invalid UpdateExpression: Attribute name is a reserved \
         keyword; reserved keyword: COMMENT"
    );
    assert!(UpdateItemRequest::try_from(update_alias_list_path).is_ok());
    assert_eq!(
        UpdateItemRequest::try_from(update_unclosed_index).expect_err("unclosed index should fail"),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \".\", near: \
         \"0.#comment\""
    );
}

#[test]
fn transaction_expressions_reject_attribute_errors_with_dynamodb_messages() {
    let transact_get_reserved_projection = json!({
        "TransactItems": [{
            "Get": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ProjectionExpression": "COMMENT"
            }
        }]
    });
    let transact_get_missing_name = json!({
        "TransactItems": [{
            "Get": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ProjectionExpression": "#comment"
            }
        }]
    });
    let transact_get_invalid_name_key = json!({
        "TransactItems": [{
            "Get": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ProjectionExpression": "#comment",
                "ExpressionAttributeNames": { "comment": "COMMENT" }
            }
        }]
    });
    let transact_get_alias_reserved_projection = json!({
        "TransactItems": [{
            "Get": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ProjectionExpression": "#comment",
                "ExpressionAttributeNames": { "#comment": "COMMENT" }
            }
        }]
    });
    let condition_check_missing_value = json!({
        "TransactItems": [{
            "ConditionCheck": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ConditionExpression": "#comment = :value",
                "ExpressionAttributeNames": { "#comment": "COMMENT" }
            }
        }]
    });
    let condition_check_invalid_value_key = json!({
        "TransactItems": [{
            "ConditionCheck": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ConditionExpression": "#comment = :value",
                "ExpressionAttributeNames": { "#comment": "COMMENT" },
                "ExpressionAttributeValues": { "value": { "S": "note" } }
            }
        }]
    });
    let update_reserved_word = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "UpdateExpression": "SET COMMENT = :value",
                "ExpressionAttributeValues": { ":value": { "S": "note" } }
            }
        }]
    });
    let update_condition_syntax = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "UpdateExpression": "SET #attribute = :value",
                "ConditionExpression": "#comment =",
                "ExpressionAttributeNames": {
                    "#attribute": "a",
                    "#comment": "COMMENT"
                },
                "ExpressionAttributeValues": { ":value": { "S": "note" } }
            }
        }]
    });
    let condition_check_single_parenthesized_condition = json!({
        "TransactItems": [{
            "ConditionCheck": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ConditionExpression": "(#comment = :value)",
                "ExpressionAttributeNames": { "#comment": "COMMENT" },
                "ExpressionAttributeValues": { ":value": { "S": "note" } }
            }
        }]
    });
    let condition_check_double_parenthesized_condition = json!({
        "TransactItems": [{
            "ConditionCheck": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ConditionExpression": "((#comment = :value))",
                "ExpressionAttributeNames": { "#comment": "COMMENT" },
                "ExpressionAttributeValues": { ":value": { "S": "note" } }
            }
        }]
    });

    assert_eq!(
        TransactGetItemsRequest::try_from(transact_get_reserved_projection)
            .expect_err("reserved projection should fail"),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert_eq!(
        TransactGetItemsRequest::try_from(transact_get_missing_name)
            .expect_err("missing name should fail"),
        "Invalid ProjectionExpression: An expression attribute name used in the document path is \
         not defined; attribute name: #comment"
    );
    assert_eq!(
        TransactGetItemsRequest::try_from(transact_get_invalid_name_key)
            .expect_err("invalid name key should fail"),
        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"comment\""
    );
    assert!(TransactGetItemsRequest::try_from(transact_get_alias_reserved_projection).is_ok());
    assert_eq!(
        TransactWriteItemsRequest::try_from(condition_check_missing_value)
            .expect_err("missing value should fail"),
        "Invalid ConditionExpression: An expression attribute value used in expression is not \
         defined; attribute value: :value"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(condition_check_invalid_value_key)
            .expect_err("invalid value key should fail"),
        "ExpressionAttributeValues contains invalid key: Syntax error; key: \"value\""
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(update_reserved_word)
            .expect_err("reserved update word should fail"),
        "Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: COMMENT"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(update_condition_syntax)
            .expect_err("invalid condition syntax should fail"),
        "Invalid ConditionExpression: Syntax error; token: \"<EOF>\", near: \"=\""
    );
    assert!(
        TransactWriteItemsRequest::try_from(condition_check_single_parenthesized_condition).is_ok()
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(condition_check_double_parenthesized_condition)
            .expect_err("transaction redundant parentheses should fail"),
        "Invalid ConditionExpression: The expression has redundant parentheses;"
    );
}

#[test]
fn batch_write_and_batch_get_reject_limits_and_empty_tables() {
    let batch_write_empty_request_items = json!({
        "RequestItems": {}
    });
    let batch_write_too_many_payload = json!({
        "RequestItems": {
            "TestTable": (0..26)
                .map(|idx| json!({
                    "PutRequest": { "Item": { "pk": { "S": format!("item#{idx}") } } }
                }))
                .collect::<Vec<_>>()
        }
    });
    let batch_get_empty_table_name_payload = json!({
        "RequestItems": {
            "": {
                "Keys": [{ "pk": { "S": "1" } }]
            }
        }
    });
    let batch_get_too_many_payload = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": (0..101)
                    .map(|idx| json!({ "pk": { "S": format!("item#{idx}") } }))
                    .collect::<Vec<_>>()
            }
        }
    });

    let batch_write_empty_err = BatchWriteItemRequest::try_from(batch_write_empty_request_items)
        .expect_err("empty request items should fail");
    let batch_write_too_many_err = BatchWriteItemRequest::try_from(batch_write_too_many_payload)
        .expect_err("too many write items should fail");
    let batch_get_empty_table_err =
        BatchGetItemRequest::try_from(batch_get_empty_table_name_payload)
            .expect_err("empty table name should fail");
    let batch_get_too_many_err = BatchGetItemRequest::try_from(batch_get_too_many_payload)
        .expect_err("too many get items should fail");

    assert_eq!(batch_write_empty_err, "RequestItems cannot be empty");
    assert_eq!(
        batch_write_too_many_err,
        "Too many items requested. Maximum allowed: 25"
    );
    assert_eq!(batch_get_empty_table_err, "TableName cannot be empty");
    assert_eq!(
        batch_get_too_many_err,
        "1 validation error detected: Value at 'RequestItems.TestTable.member.Keys' failed to \
         satisfy constraint: Member must have length less than or equal to 100"
    );
}

#[test]
fn query_scan_and_batch_requests_reject_invalid_return_parameters() {
    let query_invalid_return_consumed_capacity = json!({
        "TableName": "TestTable",
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": "p"}},
        "ReturnConsumedCapacity": "BOGUS"
    });
    let scan_invalid_return_consumed_capacity = json!({
        "TableName": "TestTable",
        "ReturnConsumedCapacity": "BOGUS"
    });
    let batch_get_invalid_return_consumed_capacity = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{"pk": {"S": "p"}}]
            }
        },
        "ReturnConsumedCapacity": "BOGUS"
    });
    let batch_write_invalid_return_consumed_capacity = json!({
        "RequestItems": {
            "TestTable": [{
                "PutRequest": {
                    "Item": {"pk": {"S": "p"}}
                }
            }]
        },
        "ReturnConsumedCapacity": "BOGUS"
    });
    let batch_write_invalid_return_item_collection_metrics = json!({
        "RequestItems": {
            "TestTable": [{
                "PutRequest": {
                    "Item": {"pk": {"S": "p"}}
                }
            }]
        },
        "ReturnItemCollectionMetrics": "BOGUS"
    });
    let invalid_return_consumed_capacity_message =
        "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy \
         constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]";

    assert_eq!(
        QueryRequest::try_from(query_invalid_return_consumed_capacity)
            .expect_err("invalid Query ReturnConsumedCapacity should fail"),
        invalid_return_consumed_capacity_message
    );
    assert_eq!(
        ScanRequest::try_from(scan_invalid_return_consumed_capacity)
            .expect_err("invalid Scan ReturnConsumedCapacity should fail"),
        invalid_return_consumed_capacity_message
    );
    assert_eq!(
        BatchGetItemRequest::try_from(batch_get_invalid_return_consumed_capacity)
            .expect_err("invalid BatchGetItem ReturnConsumedCapacity should fail"),
        invalid_return_consumed_capacity_message
    );
    assert_eq!(
        BatchWriteItemRequest::try_from(batch_write_invalid_return_consumed_capacity)
            .expect_err("invalid BatchWriteItem ReturnConsumedCapacity should fail"),
        invalid_return_consumed_capacity_message
    );
    assert_eq!(
        BatchWriteItemRequest::try_from(batch_write_invalid_return_item_collection_metrics)
            .expect_err("invalid BatchWriteItem ReturnItemCollectionMetrics should fail"),
        "1 validation error detected: Value 'BOGUS' at 'returnItemCollectionMetrics' failed to \
         satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"
    );
}

#[test]
fn batch_get_projection_expressions_reject_attribute_errors_with_dynamodb_messages() {
    let raw_reserved_projection = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{ "pk": { "S": "1" } }],
                "ProjectionExpression": "COMMENT"
            }
        }
    });
    let nested_reserved_projection = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{ "pk": { "S": "1" } }],
                "ProjectionExpression": "m.COMMENT"
            }
        }
    });
    let alias_parent_raw_reserved_child = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{ "pk": { "S": "1" } }],
                "ProjectionExpression": "#m.COMMENT",
                "ExpressionAttributeNames": { "#m": "m" }
            }
        }
    });
    let alias_nested_reserved_projection = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{ "pk": { "S": "1" } }],
                "ProjectionExpression": "#m.#comment",
                "ExpressionAttributeNames": {
                    "#m": "m",
                    "#comment": "COMMENT"
                }
            }
        }
    });
    let invalid_name_key = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{ "pk": { "S": "1" } }],
                "ProjectionExpression": "#m",
                "ExpressionAttributeNames": { "m": "m" }
            }
        }
    });
    let missing_name = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{ "pk": { "S": "1" } }],
                "ProjectionExpression": "#m"
            }
        }
    });
    let unused_name = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{ "pk": { "S": "1" } }],
                "ProjectionExpression": "#m",
                "ExpressionAttributeNames": {
                    "#m": "m",
                    "#unused": "unused"
                }
            }
        }
    });

    assert_eq!(
        BatchGetItemRequest::try_from(raw_reserved_projection)
            .expect_err("raw reserved projection should fail"),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert_eq!(
        BatchGetItemRequest::try_from(nested_reserved_projection)
            .expect_err("nested reserved projection should fail"),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert_eq!(
        BatchGetItemRequest::try_from(alias_parent_raw_reserved_child)
            .expect_err("raw reserved child projection should fail"),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert!(BatchGetItemRequest::try_from(alias_nested_reserved_projection).is_ok());
    assert_eq!(
        BatchGetItemRequest::try_from(invalid_name_key).expect_err("invalid name key should fail"),
        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"m\""
    );
    assert_eq!(
        BatchGetItemRequest::try_from(missing_name).expect_err("missing name should fail"),
        "Invalid ProjectionExpression: An expression attribute name used in the document path is \
         not defined; attribute name: #m"
    );
    assert_eq!(
        BatchGetItemRequest::try_from(unused_name).expect_err("unused name should fail"),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}"
    );
}

#[test]
fn transact_write_and_update_table_reject_empty_operation_sets() {
    let empty_transact_items_payload = json!({
        "TransactItems": []
    });
    let too_many_transact_items_payload = json!({
        "TransactItems": (0..101)
            .map(|idx| json!({
                "Put": {
                    "TableName": "TestTable",
                    "Item": { "pk": { "S": format!("item#{idx}") } }
                }
            }))
            .collect::<Vec<_>>()
    });
    let empty_gsi_updates_payload = json!({
        "TableName": "TestTable",
        "GlobalSecondaryIndexUpdates": []
    });
    let delete_blank_index_payload = json!({
        "TableName": "TestTable",
        "GlobalSecondaryIndexUpdates": [{
            "Delete": { "IndexName": "   " }
        }]
    });

    let empty_transact_err = TransactWriteItemsRequest::try_from(empty_transact_items_payload)
        .expect_err("empty transact items should fail");
    let too_many_transact_err =
        TransactWriteItemsRequest::try_from(too_many_transact_items_payload)
            .expect_err("too many transact items should fail");
    let empty_gsi_updates_err = UpdateTableRequest::try_from(empty_gsi_updates_payload)
        .expect_err("empty gsi updates should fail");
    let delete_blank_index_err = UpdateTableRequest::try_from(delete_blank_index_payload)
        .expect_err("blank delete index name should fail");

    assert_eq!(empty_transact_err, "TransactItems cannot be empty");
    assert_eq!(
        too_many_transact_err,
        "TransactItems cannot contain more than 100 operations"
    );
    assert_eq!(
        empty_gsi_updates_err,
        "GlobalSecondaryIndexUpdates cannot be empty"
    );
    assert!(delete_blank_index_err.contains("Delete.IndexName cannot be empty"));
}

#[test]
fn transact_write_request_parameter_validation_matches_dynamodb() {
    let invalid_return_consumed_capacity = json!({
        "ReturnConsumedCapacity": "BOGUS",
        "TransactItems": [{
            "Put": {
                "TableName": "TestTable",
                "Item": { "pk": { "S": "1" } }
            }
        }]
    });
    let invalid_return_item_collection_metrics = json!({
        "ReturnItemCollectionMetrics": "BOGUS",
        "TransactItems": [{
            "Put": {
                "TableName": "TestTable",
                "Item": { "pk": { "S": "1" } }
            }
        }]
    });
    let invalid_return_values_on_condition_failure = json!({
        "TransactItems": [{
            "Put": {
                "TableName": "TestTable",
                "Item": { "pk": { "S": "1" } },
                "ConditionExpression": "attribute_not_exists(pk)",
                "ReturnValuesOnConditionCheckFailure": "BOGUS"
            }
        }]
    });
    let empty_expression_names = json!({
        "TransactItems": [{
            "ConditionCheck": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ConditionExpression": "attribute_exists(pk)",
                "ExpressionAttributeNames": {}
            }
        }]
    });
    let empty_expression_values = json!({
        "TransactItems": [{
            "ConditionCheck": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ConditionExpression": "attribute_exists(pk)",
                "ExpressionAttributeValues": {}
            }
        }]
    });
    let empty_transact_item = json!({
        "TransactItems": [{}]
    });

    assert_eq!(
        TransactWriteItemsRequest::try_from(invalid_return_consumed_capacity)
            .expect_err("invalid ReturnConsumedCapacity should fail"),
        "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy \
         constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(invalid_return_item_collection_metrics)
            .expect_err("invalid ReturnItemCollectionMetrics should fail"),
        "1 validation error detected: Value 'BOGUS' at 'returnItemCollectionMetrics' failed to \
         satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(invalid_return_values_on_condition_failure)
            .expect_err("invalid ReturnValuesOnConditionCheckFailure should fail"),
        "1 validation error detected: Value 'BOGUS' at \
         'transactItems.1.member.put.returnValuesOnConditionCheckFailure' failed to satisfy \
         constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(empty_expression_names)
            .expect_err("empty ExpressionAttributeNames should fail"),
        "ExpressionAttributeNames must not be empty"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(empty_expression_values)
            .expect_err("empty ExpressionAttributeValues should fail"),
        "ExpressionAttributeValues must not be empty"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(empty_transact_item)
            .expect_err("empty transact write item should fail"),
        "Invalid Request: TransactWriteRequest should contain Delete or Put or Update request"
    );
}

#[test]
fn single_item_and_transact_get_request_parameter_validation_matches_dynamodb() {
    let put_empty_values = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "attribute_not_exists(pk)",
        "ExpressionAttributeValues": {}
    });
    let put_invalid_return_values_on_condition_failure = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ReturnValuesOnConditionCheckFailure": "BOGUS"
    });
    let put_invalid_return_consumed_capacity = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ReturnConsumedCapacity": "BOGUS"
    });
    let put_invalid_return_item_collection_metrics = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ReturnItemCollectionMetrics": "BOGUS"
    });
    let delete_invalid_return_values_on_condition_failure = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ReturnValuesOnConditionCheckFailure": "BOGUS"
    });
    let update_invalid_return_values_on_condition_failure = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET note = :v",
        "ExpressionAttributeValues": { ":v": { "S": "x" } },
        "ReturnValuesOnConditionCheckFailure": "BOGUS"
    });
    let update_invalid_return_consumed_capacity = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET note = :v",
        "ExpressionAttributeValues": { ":v": { "S": "x" } },
        "ReturnConsumedCapacity": "BOGUS"
    });
    let update_invalid_return_item_collection_metrics = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET note = :v",
        "ExpressionAttributeValues": { ":v": { "S": "x" } },
        "ReturnItemCollectionMetrics": "BOGUS"
    });
    let get_invalid_return_consumed_capacity = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ReturnConsumedCapacity": "BOGUS"
    });
    let get_empty_expression_names = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ProjectionExpression": "note",
        "ExpressionAttributeNames": {}
    });
    let transact_get_invalid_return_consumed_capacity = json!({
        "ReturnConsumedCapacity": "BOGUS",
        "TransactItems": [{
            "Get": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } }
            }
        }]
    });
    let transact_get_empty_expression_names = json!({
        "TransactItems": [{
            "Get": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ProjectionExpression": "note",
                "ExpressionAttributeNames": {}
            }
        }]
    });

    assert_eq!(
        PutItemRequest::try_from(put_empty_values).expect_err("empty values should fail"),
        "1 validation error detected: ExpressionAttributeValues must not be empty"
    );
    assert_eq!(
        PutItemRequest::try_from(put_invalid_return_values_on_condition_failure)
            .expect_err("invalid ReturnValuesOnConditionCheckFailure should fail"),
        "1 validation error detected: Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' \
         failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"
    );
    assert_eq!(
        PutItemRequest::try_from(put_invalid_return_consumed_capacity)
            .expect_err("invalid ReturnConsumedCapacity should fail"),
        "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy \
         constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );
    assert_eq!(
        PutItemRequest::try_from(put_invalid_return_item_collection_metrics)
            .expect_err("invalid ReturnItemCollectionMetrics should fail"),
        "1 validation error detected: ReturnItemCollectionMetrics can only be SIZE or NONE"
    );
    assert_eq!(
        DeleteItemRequest::try_from(delete_invalid_return_values_on_condition_failure)
            .expect_err("invalid ReturnValuesOnConditionCheckFailure should fail"),
        "1 validation error detected: Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' \
         failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_invalid_return_values_on_condition_failure)
            .expect_err("invalid ReturnValuesOnConditionCheckFailure should fail"),
        "Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' failed to satisfy constraint: \
         Member must satisfy enum value set: [ALL_OLD, NONE]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_invalid_return_consumed_capacity)
            .expect_err("invalid ReturnConsumedCapacity should fail"),
        "Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must \
         satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_invalid_return_item_collection_metrics)
            .expect_err("invalid ReturnItemCollectionMetrics should fail"),
        "1 validation error detected: ReturnItemCollectionMetrics can only be SIZE or NONE"
    );
    assert_eq!(
        GetItemRequest::try_from(get_invalid_return_consumed_capacity)
            .expect_err("invalid ReturnConsumedCapacity should fail"),
        "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy \
         constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );
    assert_eq!(
        GetItemRequest::try_from(get_empty_expression_names)
            .expect_err("empty ExpressionAttributeNames should fail"),
        "ExpressionAttributeNames must not be empty"
    );
    assert_eq!(
        TransactGetItemsRequest::try_from(transact_get_invalid_return_consumed_capacity)
            .expect_err("invalid ReturnConsumedCapacity should fail"),
        "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy \
         constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );
    assert_eq!(
        TransactGetItemsRequest::try_from(transact_get_empty_expression_names)
            .expect_err("empty ExpressionAttributeNames should fail"),
        "ExpressionAttributeNames must not be empty"
    );
}

#[test]
fn dynamodb_limit_boundaries_are_accepted() {
    let max_item = "x".repeat(crate::MAX_ITEM_SIZE_BYTES - "pk".len());
    let max_binary_item_bytes = crate::MAX_ITEM_SIZE_BYTES - "pk".len() - "p".len() - "data".len();
    let max_nested_list_binary_item_bytes = max_binary_item_bytes - 3 - 1;
    let max_nested_map_binary_item_bytes = max_binary_item_bytes - 3 - 1 - "child".len();
    let nested_map_binary_set_first_member_bytes = max_nested_map_binary_item_bytes / 2 - 1;
    let put_item = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": max_item } },
        "ConditionExpression": "a".repeat(crate::MAX_EXPRESSION_BYTES)
    });
    let binary_put_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": { "B": zero_binary_base64(max_binary_item_bytes) }
        }
    });
    let binary_set_put_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": {
                "BS": [
                    zero_binary_base64(max_binary_item_bytes / 2),
                    zero_binary_base64(max_binary_item_bytes - (max_binary_item_bytes / 2))
                ]
            }
        }
    });
    let numeric_set_put_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": { "NS": number_string_set(20_479) }
        }
    });
    let nested_list_number_put_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": { "L": number_list(19_504) }
        }
    });
    let nested_map_numeric_set_put_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": {
                "M": {
                    "c": { "NS": number_string_set(20_479) }
                }
            }
        }
    });
    let nested_list_binary_put_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": {
                "L": [
                    { "B": zero_binary_base64(max_nested_list_binary_item_bytes) }
                ]
            }
        }
    });
    let nested_map_binary_set_put_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": {
                "M": {
                    "child": {
                        "BS": [
                            zero_binary_base64(nested_map_binary_set_first_member_bytes),
                            zero_binary_base64(
                                max_nested_map_binary_item_bytes
                                    - nested_map_binary_set_first_member_bytes
                            )
                        ]
                    }
                }
            }
        }
    });
    let list_tables = json!({ "Limit": crate::MAX_LIST_TABLES_LIMIT });
    let query = json!({
        "TableName": "TestTable",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": { ":pk": { "S": "1" } },
        "FilterExpression": "a".repeat(crate::MAX_EXPRESSION_BYTES),
        "ProjectionExpression": "b".repeat(crate::MAX_EXPRESSION_BYTES)
    });
    let create_table = create_table_with_projected_attribute_count(20, 5);
    let transact_get = json!({
        "TransactItems": (0..100)
            .map(|idx| json!({
                "Get": {
                    "TableName": "TestTable",
                    "Key": { "pk": { "S": format!("item#{idx}") } }
                }
            }))
            .collect::<Vec<_>>()
    });

    assert!(PutItemRequest::try_from(put_item).is_ok());
    assert!(PutItemRequest::try_from(binary_put_item).is_ok());
    assert!(PutItemRequest::try_from(binary_set_put_item).is_ok());
    assert!(PutItemRequest::try_from(numeric_set_put_item).is_ok());
    assert!(PutItemRequest::try_from(nested_list_number_put_item).is_ok());
    assert!(PutItemRequest::try_from(nested_map_numeric_set_put_item).is_ok());
    assert!(PutItemRequest::try_from(nested_list_binary_put_item).is_ok());
    assert!(PutItemRequest::try_from(nested_map_binary_set_put_item).is_ok());
    assert!(ListTablesRequest::try_from(list_tables).is_ok());
    assert!(QueryRequest::try_from(query).is_ok());
    assert!(CreateTableRequest::try_from(create_table).is_ok());
    assert!(TransactGetItemsRequest::try_from(transact_get).is_ok());
}

#[test]
fn dynamodb_item_limits_reject_invalid_requests() {
    let max_binary_item_bytes = crate::MAX_ITEM_SIZE_BYTES - "pk".len() - "p".len() - "data".len();
    let max_nested_list_binary_item_bytes = max_binary_item_bytes - 3 - 1;
    let max_nested_map_binary_item_bytes = max_binary_item_bytes - 3 - 1 - "child".len();
    let oversized_item = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "x".repeat(crate::MAX_ITEM_SIZE_BYTES) } }
    });
    let oversized_binary_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": { "B": zero_binary_base64(max_binary_item_bytes + 1) }
        }
    });
    let oversized_binary_set_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": {
                "BS": [
                    zero_binary_base64(max_binary_item_bytes / 2),
                    zero_binary_base64((max_binary_item_bytes - (max_binary_item_bytes / 2)) + 1)
                ]
            }
        }
    });
    let oversized_numeric_set_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": { "NS": number_string_set(20_480) }
        }
    });
    let oversized_nested_list_number_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": { "L": number_list(19_505) }
        }
    });
    let oversized_nested_map_numeric_set_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": {
                "M": {
                    "c": { "NS": number_string_set(20_480) }
                }
            }
        }
    });
    let oversized_nested_list_binary_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": {
                "L": [
                    { "B": zero_binary_base64(max_nested_list_binary_item_bytes + 1) }
                ]
            }
        }
    });
    let oversized_nested_map_binary_set_item = json!({
        "TableName": "TestTable",
        "Item": {
            "pk": { "S": "p" },
            "data": {
                "M": {
                    "child": {
                        "BS": [
                            zero_binary_base64(max_nested_map_binary_item_bytes / 2),
                            zero_binary_base64(
                                (max_nested_map_binary_item_bytes
                                    - (max_nested_map_binary_item_bytes / 2))
                                    + 1
                            )
                        ]
                    }
                }
            }
        }
    });
    let oversized_attribute_name = json!({
        "TableName": "TestTable",
        "Item": { "x".repeat(crate::MAX_ATTRIBUTE_NAME_BYTES + 1): { "S": "1" } }
    });
    let too_deep_item = json!({
        "TableName": "TestTable",
        "Item": { "pk": nested_attribute_value(crate::MAX_ATTRIBUTE_NESTING_DEPTH + 1) }
    });

    assert_eq!(
        PutItemRequest::try_from(oversized_item).expect_err("oversized item should fail"),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_binary_item)
            .expect_err("oversized binary item should fail by decoded byte length"),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_binary_set_item)
            .expect_err("oversized binary set item should fail by decoded byte length"),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_numeric_set_item)
            .expect_err("oversized numeric set item should use numeric byte accounting"),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_nested_list_number_item)
            .expect_err("oversized nested number list item should include list overhead"),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_nested_map_numeric_set_item)
            .expect_err("oversized nested numeric set map item should include map overhead"),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_nested_list_binary_item)
            .expect_err("oversized nested binary list item should include list overhead"),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_nested_map_binary_set_item)
            .expect_err("oversized nested binary set map item should include map overhead"),
        "Item size has exceeded the maximum allowed size"
    );
    assert!(
        PutItemRequest::try_from(oversized_attribute_name)
            .expect_err("oversized attribute name should fail")
            .contains("attribute name cannot exceed")
    );
    assert!(
        PutItemRequest::try_from(too_deep_item)
            .expect_err("deep item should fail")
            .contains("nesting depth")
    );
}

fn zero_binary_base64(byte_len: usize) -> String {
    let full_groups = byte_len / 3;
    let remainder = byte_len % 3;
    let mut encoded = "AAAA".repeat(full_groups);
    match remainder {
        0 => {}
        1 => encoded.push_str("AA=="),
        2 => encoded.push_str("AAA="),
        _ => unreachable!("remainder is modulo 3"),
    }
    encoded
}

fn number_string_set(count: usize) -> Vec<String> {
    (0..count).map(number_value).collect()
}

fn number_list(count: usize) -> Vec<serde_json::Value> {
    number_string_set(count)
        .into_iter()
        .map(|value| json!({ "N": value }))
        .collect()
}

fn number_value(index: usize) -> String {
    format!("1{:037}", (index * 2) + 1)
}

#[test]
fn dynamodb_index_limits_reject_invalid_requests() {
    let too_short_index_name = {
        let mut payload = valid_create_table_payload();
        payload["GlobalSecondaryIndexes"] = json!([{
            "IndexName": "ab",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" }
        }]);
        payload
    };
    let invalid_index_name = {
        let mut payload = valid_create_table_payload();
        payload["GlobalSecondaryIndexes"] = json!([{
            "IndexName": "bad/name",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" }
        }]);
        payload
    };
    let too_many_projected = create_table_with_projected_attribute_count(20, 6);

    assert!(
        CreateTableRequest::try_from(too_short_index_name)
            .expect_err("short index name should fail")
            .contains("between 3 and 255")
    );
    assert!(
        CreateTableRequest::try_from(invalid_index_name)
            .expect_err("invalid index name should fail")
            .contains("invalid characters")
    );
    assert!(
        CreateTableRequest::try_from(too_many_projected)
            .expect_err("too many projected attributes should fail")
            .contains("Projected attributes")
    );
}

#[test]
fn dynamodb_expression_and_api_limits_reject_invalid_requests() {
    let long_expression = "a".repeat(crate::MAX_EXPRESSION_BYTES + 1);
    let scan = json!({
        "TableName": "TestTable",
        "FilterExpression": long_expression
    });
    let query = json!({
        "TableName": "TestTable",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": { ":pk": { "S": "1" } },
        "ProjectionExpression": "b".repeat(crate::MAX_EXPRESSION_BYTES + 1)
    });
    let put = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "c".repeat(crate::MAX_EXPRESSION_BYTES + 1)
    });
    let list_tables = json!({ "Limit": crate::MAX_LIST_TABLES_LIMIT + 1 });

    assert!(
        ScanRequest::try_from(scan)
            .expect_err("long filter should fail")
            .contains("FilterExpression")
    );
    assert!(
        QueryRequest::try_from(query)
            .expect_err("long projection should fail")
            .contains("ProjectionExpression")
    );
    assert!(
        PutItemRequest::try_from(put)
            .expect_err("long condition should fail")
            .contains("ConditionExpression")
    );
    assert_eq!(
        ListTablesRequest::try_from(list_tables).expect_err("list limit should fail"),
        "Limit must be between 1 and 100"
    );
}

#[test]
fn dynamodb_transaction_limits_reject_invalid_requests() {
    let too_many_gets = json!({
        "TransactItems": (0..101)
            .map(|idx| json!({
                "Get": {
                    "TableName": "TestTable",
                    "Key": { "pk": { "S": format!("item#{idx}") } }
                }
            }))
            .collect::<Vec<_>>()
    });
    let oversized_request = json!({
        "TransactItems": [{
            "Put": {
                "TableName": "TestTable",
                "Item": { "pk": { "S": "x".repeat(crate::MAX_TRANSACTION_REQUEST_BYTES) } }
            }
        }]
    });

    assert_eq!(
        TransactGetItemsRequest::try_from(too_many_gets).expect_err("too many gets should fail"),
        "TransactItems cannot contain more than 100 operations"
    );
    assert!(
        TransactWriteItemsRequest::try_from(oversized_request)
            .expect_err("oversized transaction should fail")
            .contains("TransactWriteItems request cannot exceed")
    );
}

#[test]
fn put_item_rejects_invalid_sets_before_insert() {
    for (item, expected_message) in [
        (
            json!({
                "pk": { "S": "ss-duplicate" },
                "v": { "SS": ["a", "a"] }
            }),
            "One or more parameter values were invalid: Input collection [\"a\", \"a\"] contains \
             duplicates.",
        ),
        (
            json!({
                "pk": { "S": "ns-duplicate" },
                "v": { "NS": ["3", "1", "1", "1", "1", "3"] }
            }),
            "One or more parameter values were invalid: Input collection contains duplicates.",
        ),
        (
            json!({
                "pk": { "S": "ns-numeric-equivalent-duplicate" },
                "v": { "NS": ["1", "1.0"] }
            }),
            "One or more parameter values were invalid: Input collection contains duplicates.",
        ),
        (
            json!({
                "pk": { "S": "bs-duplicate" },
                "v": { "BS": ["QUE=", "QkI=", "QUE=", "QkI=", "QUE="] }
            }),
            "One or more parameter values were invalid: Input collection \
             [\"QUE=\",\"QkI=\",\"QUE=\",\"QkI=\",\"QUE=\"]of type BS contains duplicates.",
        ),
        (
            json!({
                "pk": { "S": "nested-map-ns-duplicate" },
                "m": { "M": { "v": { "NS": ["1", "1"] } } }
            }),
            "One or more parameter values were invalid: Input collection contains duplicates.",
        ),
        (
            json!({
                "pk": { "S": "nested-list-bs-duplicate" },
                "l": { "L": [{ "BS": ["QQ==", "QQ=="] }] }
            }),
            "One or more parameter values were invalid: Input collection [\"QQ==\",\"QQ==\"]of \
             type BS contains duplicates.",
        ),
        (
            json!({
                "pk": { "S": "empty-ss" },
                "v": { "SS": [] }
            }),
            "One or more parameter values were invalid: An string set  may not be empty",
        ),
        (
            json!({
                "pk": { "S": "empty-ns" },
                "v": { "NS": [] }
            }),
            "One or more parameter values were invalid: An number set  may not be empty",
        ),
        (
            json!({
                "pk": { "S": "empty-bs" },
                "v": { "BS": [] }
            }),
            "One or more parameter values were invalid: Binary sets should not be empty",
        ),
    ] {
        let put_item = json!({
            "TableName": "TestTable",
            "Item": item
        });
        assert_eq!(
            PutItemRequest::try_from(put_item).expect_err("invalid set should fail"),
            expected_message
        );
    }
}

#[test]
fn shared_item_validation_rejects_invalid_sets_in_batch_and_transaction_puts() {
    let batch_write = json!({
        "RequestItems": {
            "TestTable": [
                {
                    "PutRequest": {
                        "Item": {
                            "pk": { "S": "batch-ns-duplicate" },
                            "v": { "NS": ["1", "1.0"] }
                        }
                    }
                }
            ]
        }
    });
    let transact_write = json!({
        "TransactItems": [
            {
                "Put": {
                    "TableName": "TestTable",
                    "Item": {
                        "pk": { "S": "transact-bs-duplicate" },
                        "v": { "BS": ["QQ==", "QQ=="] }
                    }
                }
            }
        ]
    });

    assert_eq!(
        BatchWriteItemRequest::try_from(batch_write)
            .expect_err("BatchWriteItem PutRequest should reject invalid sets"),
        "One or more parameter values were invalid: Input collection contains duplicates."
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_write)
            .expect_err("TransactWriteItems Put should reject invalid sets"),
        "One or more parameter values were invalid: Input collection [\"QQ==\",\"QQ==\"]of type \
         BS contains duplicates."
    );
}

#[test]
fn update_expression_values_reject_invalid_sets_before_execution() {
    for (update_expression, expression_attribute_values, expected_message) in [
        (
            "SET v = :set",
            json!({ ":set": { "SS": ["a", "a"] } }),
            "1 validation error detected: One or more parameter values were invalid: Input \
             collection [\"a\", \"a\"] contains duplicates.",
        ),
        (
            "SET v = :set",
            json!({ ":set": { "NS": ["1", "1.0"] } }),
            "1 validation error detected: One or more parameter values were invalid: Input \
             collection contains duplicates.",
        ),
        (
            "SET v = :set",
            json!({ ":set": { "BS": ["QQ==", "QQ=="] } }),
            "1 validation error detected: One or more parameter values were invalid: Input \
             collection [\"QQ==\",\"QQ==\"]of type BS contains duplicates.",
        ),
        (
            "SET v = :set",
            json!({ ":set": { "SS": [] } }),
            "1 validation error detected: One or more parameter values were invalid: An string \
             set  may not be empty",
        ),
        (
            "ADD v :set",
            json!({ ":set": { "NS": ["1", "1.0"] } }),
            "1 validation error detected: One or more parameter values were invalid: Input \
             collection contains duplicates.",
        ),
        (
            "ADD v :set",
            json!({ ":set": { "BS": [] } }),
            "1 validation error detected: One or more parameter values were invalid: Binary sets \
             should not be empty",
        ),
    ] {
        let update_item = json!({
            "TableName": "TestTable",
            "Key": { "pk": { "S": "p" } },
            "UpdateExpression": update_expression,
            "ExpressionAttributeValues": expression_attribute_values
        });
        assert_eq!(
            UpdateItemRequest::try_from(update_item).expect_err("invalid update set should fail"),
            expected_message
        );
    }
}

#[test]
fn transaction_update_expression_values_reject_invalid_sets_before_execution() {
    let transact_set_duplicate = json!({
        "TransactItems": [
            {
                "Update": {
                    "TableName": "TestTable",
                    "Key": { "pk": { "S": "p" } },
                    "UpdateExpression": "SET v = :set",
                    "ExpressionAttributeValues": {
                        ":set": { "NS": ["1", "1.0"] }
                    }
                }
            }
        ]
    });
    let transact_add_empty = json!({
        "TransactItems": [
            {
                "Update": {
                    "TableName": "TestTable",
                    "Key": { "pk": { "S": "p" } },
                    "UpdateExpression": "ADD v :set",
                    "ExpressionAttributeValues": {
                        ":set": { "BS": [] }
                    }
                }
            }
        ]
    });

    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_set_duplicate)
            .expect_err("transaction SET should reject invalid sets"),
        "One or more parameter values were invalid: Input collection contains duplicates."
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_add_empty)
            .expect_err("transaction ADD should reject empty sets"),
        "One or more parameter values were invalid: Binary sets should not be empty"
    );
}

fn nested_attribute_value(depth: usize) -> serde_json::Value {
    let mut value = json!({ "S": "leaf" });
    for _ in 0..depth {
        value = json!({ "M": { "child": value } });
    }
    value
}

fn create_table_with_projected_attribute_count(
    index_count: usize,
    per_index_count: usize,
) -> serde_json::Value {
    let mut payload = valid_create_table_payload();
    payload["GlobalSecondaryIndexes"] = serde_json::Value::Array(
        (0..index_count)
            .map(|index| {
                json!({
                    "IndexName": format!("gsi_{index}"),
                    "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": (0..per_index_count)
                            .map(|attr| format!("attr_{index}_{attr}"))
                            .collect::<Vec<_>>()
                    }
                })
            })
            .collect(),
    );
    payload
}
