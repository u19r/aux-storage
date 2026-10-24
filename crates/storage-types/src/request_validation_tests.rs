use std::convert::TryInto;

use serde_json::json;

use crate::{
    BatchGetItemRequest, BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest,
    DeleteTableRequest, DescribeTableRequest, GetItemRequest, GetStreamRecordsRequest,
    ListTablesRequest, PutItemRequest, QueryRequest, ScanRequest, StreamRetentionDuration,
    SystemStreamCheckpointToken, TableName, TransactGetItemsRequest, TransactWriteItemsRequest,
    UpdateItemRequest, UpdateTableRequest,
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
fn create_table_accepts_aux_stream_duration_extensions() {
    let mut payload = valid_create_table_payload();
    payload["AuxStreamDurationHours"] = json!(168);
    payload["AuxDefaultItemStreamDurationHours"] = json!(-1);

    let request = CreateTableRequest::try_from(payload).unwrap();

    assert_eq!(
        request.aux_stream_duration_hours,
        Some(StreamRetentionDuration::FiniteHours(168))
    );
    assert_eq!(
        request.aux_default_item_stream_duration_hours,
        Some(StreamRetentionDuration::Forever)
    );
}

#[test]
fn create_table_omits_aux_stream_duration_extensions_by_default() {
    let request = CreateTableRequest::try_from(valid_create_table_payload()).unwrap();

    assert_eq!(request.aux_stream_duration_hours, None);
    assert_eq!(request.aux_default_item_stream_duration_hours, None);
}

#[test]
fn create_table_rejects_duplicate_gsi_names_before_backend_layout() {
    let mut payload = valid_create_table_payload();
    payload["GlobalSecondaryIndexes"] = json!([
        {
            "IndexName": "by_pk",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" }
        },
        {
            "IndexName": "by_pk",
            "KeySchema": [{ "AttributeName": "sk", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" }
        }
    ]);
    let request = CreateTableRequest::try_from(payload).unwrap();
    let error = request
        .stored_table_info_at(crate::TimestampMillis::from_timestamp(0))
        .unwrap_err();
    assert!(error.to_string().contains("duplicate GSI name 'by_pk'"));
}

#[test]
fn create_table_rejects_invalid_aux_stream_duration_extensions() {
    for (field, value) in [
        ("AuxStreamDurationHours", json!(0)),
        ("AuxStreamDurationHours", json!(-2)),
        ("AuxStreamDurationHours", json!(61321)),
        ("AuxStreamDurationHours", json!(1.5)),
        ("AuxStreamDurationHours", json!("72")),
        ("AuxDefaultItemStreamDurationHours", json!(0)),
    ] {
        let mut payload = valid_create_table_payload();
        payload[field] = value;

        let err = CreateTableRequest::try_from(payload).unwrap_err();

        assert!(
            err.contains("Invalid request format"),
            "{field}: unexpected error: {err}"
        );
    }
}

#[test]
fn update_table_accepts_aux_stream_duration_extensions() {
    let payload = json!({
        "TableName": "TestTable",
        "AuxStreamDurationHours": -1,
        "AuxDefaultItemStreamDurationHours": 24
    });

    let request = UpdateTableRequest::try_from(payload).unwrap();

    assert_eq!(
        request.aux_stream_duration_hours,
        Some(StreamRetentionDuration::Forever)
    );
    assert_eq!(
        request.aux_default_item_stream_duration_hours,
        Some(StreamRetentionDuration::FiniteHours(24))
    );
}

#[test]
fn update_table_rejects_invalid_aux_stream_duration_extensions() {
    for (field, value) in [
        ("AuxStreamDurationHours", json!(0)),
        ("AuxStreamDurationHours", json!(-2)),
        ("AuxStreamDurationHours", json!(61321)),
        ("AuxStreamDurationHours", json!(1.5)),
        ("AuxStreamDurationHours", json!("72")),
        ("AuxDefaultItemStreamDurationHours", json!(0)),
    ] {
        let payload = json!({
            "TableName": "TestTable",
            field: value
        });

        let err = UpdateTableRequest::try_from(payload).unwrap_err();

        assert!(
            err.contains("Invalid request format"),
            "{field}: unexpected error: {err}"
        );
    }
}

#[test]
fn item_write_requests_accept_aux_item_stream_ttl_extension() {
    let put = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "AuxItemStreamTtlHours": -1
    });
    let update = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #value = :value",
        "ExpressionAttributeNames": { "#value": "value" },
        "ExpressionAttributeValues": { ":value": { "S": "next" } },
        "AuxItemStreamTtlHours": 24
    });
    let delete = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "AuxItemStreamTtlHours": 168
    });
    let batch = json!({
        "RequestItems": {
            "TestTable": [
                {
                    "PutRequest": {
                        "Item": { "pk": { "S": "1" } },
                        "AuxItemStreamTtlHours": 12
                    }
                },
                {
                    "DeleteRequest": {
                        "Key": { "pk": { "S": "2" } },
                        "AuxItemStreamTtlHours": 6
                    }
                }
            ]
        }
    });
    let transact = json!({
        "TransactItems": [
            {
                "Put": {
                    "TableName": "TestTable",
                    "Item": { "pk": { "S": "1" } },
                    "AuxItemStreamTtlHours": 8
                }
            },
            {
                "Delete": {
                    "TableName": "TestTable",
                    "Key": { "pk": { "S": "3" } },
                    "AuxItemStreamTtlHours": 4
                }
            },
            {
                "Update": {
                    "TableName": "TestTable",
                    "Key": { "pk": { "S": "2" } },
                    "UpdateExpression": "SET #value = :value",
                    "ExpressionAttributeNames": { "#value": "value" },
                    "ExpressionAttributeValues": { ":value": { "S": "next" } },
                    "AuxItemStreamTtlHours": -1
                }
            }
        ]
    });

    let put = PutItemRequest::try_from(put).unwrap();
    let update = UpdateItemRequest::try_from(update).unwrap();
    let delete = DeleteItemRequest::try_from(delete).unwrap();
    let batch = BatchWriteItemRequest::try_from(batch).unwrap();
    let transact = TransactWriteItemsRequest::try_from(transact).unwrap();

    assert_eq!(
        put.aux_item_stream_ttl_hours,
        Some(StreamRetentionDuration::Forever)
    );
    assert_eq!(
        update.aux_item_stream_ttl_hours,
        Some(StreamRetentionDuration::FiniteHours(24))
    );
    assert_eq!(
        delete.aux_item_stream_ttl_hours,
        Some(StreamRetentionDuration::FiniteHours(168))
    );
    assert_eq!(
        batch
            .request_items
            .get(&TableName::new("TestTable"))
            .unwrap()[0]
            .put_request
            .as_ref()
            .and_then(|put| put.aux_item_stream_ttl_hours),
        Some(StreamRetentionDuration::FiniteHours(12))
    );
    assert_eq!(
        batch
            .request_items
            .get(&TableName::new("TestTable"))
            .unwrap()[1]
            .delete_request
            .as_ref()
            .and_then(|delete| delete.aux_item_stream_ttl_hours),
        Some(StreamRetentionDuration::FiniteHours(6))
    );
    assert_eq!(
        transact.transact_items[0]
            .put
            .as_ref()
            .and_then(|put| put.aux_item_stream_ttl_hours),
        Some(StreamRetentionDuration::FiniteHours(8))
    );
    assert_eq!(
        transact.transact_items[1]
            .delete
            .as_ref()
            .and_then(|delete| delete.aux_item_stream_ttl_hours),
        Some(StreamRetentionDuration::FiniteHours(4))
    );
    assert_eq!(
        transact.transact_items[2]
            .update
            .as_ref()
            .and_then(|update| update.aux_item_stream_ttl_hours),
        Some(StreamRetentionDuration::Forever)
    );
}

#[test]
fn item_write_requests_reject_invalid_aux_item_stream_ttl_extension() {
    let put_zero = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "AuxItemStreamTtlHours": 0
    });
    let update_string = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #value = :value",
        "ExpressionAttributeNames": { "#value": "value" },
        "ExpressionAttributeValues": { ":value": { "S": "next" } },
        "AuxItemStreamTtlHours": "24"
    });
    let delete_negative = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "AuxItemStreamTtlHours": -2
    });
    let batch_fraction = json!({
        "RequestItems": {
            "TestTable": [{
                "PutRequest": {
                    "Item": { "pk": { "S": "1" } },
                    "AuxItemStreamTtlHours": 1.5
                }
            }]
        }
    });
    let transact_too_large = json!({
        "TransactItems": [{
            "Delete": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "AuxItemStreamTtlHours": 61321
            }
        }]
    });

    let put_err = PutItemRequest::try_from(put_zero).unwrap_err();
    let update_err = UpdateItemRequest::try_from(update_string).unwrap_err();
    let delete_err = DeleteItemRequest::try_from(delete_negative).unwrap_err();
    assert!(put_err.contains("Invalid request format"));
    assert!(update_err.contains("Invalid request format"));
    assert!(delete_err.contains("Invalid request format"));

    let batch_err = BatchWriteItemRequest::try_from(batch_fraction).unwrap_err();
    let transact_err = TransactWriteItemsRequest::try_from(transact_too_large).unwrap_err();

    assert!(batch_err.contains("Invalid request format"));
    assert!(transact_err.contains("Invalid request format"));
}

#[test]
fn get_item_ignores_unknown_fields() {
    let payload = json!({
        "TableName": "TestTable",
        "Key": {"id": {"S": "test123"}},
        "InvalidField": "invalid",
    });

    let result: Result<GetItemRequest, String> = payload.try_into();
    let request = result.unwrap();
    assert_eq!(request.table_name.as_ref(), "TestTable");
}

#[test]
fn get_item_empty_key_uses_dynamodb_schema_message() {
    let payload = json!({
        "TableName": "TestTable",
        "Key": {},
    });

    let err = GetItemRequest::try_from(payload).unwrap_err();

    assert_eq!(err, "The provided key element does not match the schema");
}

#[test]
fn put_item_ignores_unknown_fields() {
    let payload = json!({
        "TableName": "TestTable",
        "Item": {"id": {"S": "test123"}},
        "InvalidField": "invalid",
    });

    let result: Result<PutItemRequest, String> = payload.try_into();
    let request = result.unwrap();
    assert_eq!(request.table_name.as_ref(), "TestTable");
}

#[test]
fn delete_item_ignores_unknown_fields() {
    let payload = json!({
        "TableName": "TestTable",
        "Key": {"id": {"S": "test123"}},
        "InvalidField": "invalid",
    });

    let result: Result<DeleteItemRequest, String> = payload.try_into();
    let request = result.unwrap();
    assert_eq!(request.table_name.as_ref(), "TestTable");
}

#[test]
fn scan_ignores_unknown_fields() {
    let payload = json!({
        "TableName": "TestTable",
        "InvalidField": "invalid",
    });

    let result: Result<ScanRequest, String> = payload.try_into();
    let request = result.unwrap();
    assert_eq!(request.table_name.as_ref(), "TestTable");
}

#[test]
fn query_ignores_unknown_fields() {
    let payload = json!({
        "TableName": "TestTable",
        "KeyConditionExpression": "pk = :pk_val",
        "ExpressionAttributeValues": {":pk_val": {"S": "test"}},
        "InvalidField": "invalid",
    });

    let result: Result<QueryRequest, String> = payload.try_into();
    let request = result.unwrap();
    assert_eq!(request.table_name.as_ref(), "TestTable");
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
            "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
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
        let err = result.unwrap_err();
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

    assert_eq!(delete_result.unwrap_err(), "TableName cannot be empty");
    assert_eq!(describe_result.unwrap_err(), "TableName cannot be empty");
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

    let too_small_err = GetStreamRecordsRequest::try_from(too_small).unwrap_err();
    let too_large_err = GetStreamRecordsRequest::try_from(too_large).unwrap_err();

    assert_eq!(too_small_err, crate::DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE);
    assert_eq!(too_large_err, crate::DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE);
}

#[test]
fn given_system_stream_when_large_page_is_requested_then_request_is_valid() {
    let request = json!({
        "SystemStream": true,
        "SourceScope": {
            "Version": 1,
            "ScopeId": "catalog",
            "Generation": 1,
        },
        "Limit": 8192
    });

    let parsed = GetStreamRecordsRequest::try_from(request).unwrap();

    assert!(parsed.is_system_stream());
    assert!(parsed.table_name().is_none());
}

#[test]
fn system_stream_without_source_scope_is_rejected_before_provider_reads() {
    let request = json!({
        "SystemStream": true,
    });

    let error = GetStreamRecordsRequest::try_from(request).unwrap_err();

    assert_eq!(error, "SystemStream SourceScope is required");
}

#[test]
fn system_stream_rejects_legacy_last_evaluated_key_resume_tokens() {
    let request = json!({
        "SystemStream": true,
        "SourceScope": {
            "Version": 1,
            "ScopeId": "catalog",
            "Generation": 1,
        },
        "LastEvaluatedKey": "legacy-raw-token",
    });

    let error = GetStreamRecordsRequest::try_from(request).unwrap_err();

    assert_eq!(error, "LastEvaluatedKey is not supported for SystemStream");
}

#[test]
fn system_stream_checkpoint_requires_source_scope() {
    let token = SystemStreamCheckpointToken::try_from(format!("v1.{}", "42".repeat(16))).unwrap();
    let request = json!({
        "SystemStream": true,
        "Checkpoint": token.as_str(),
    });

    let error = GetStreamRecordsRequest::try_from(request).unwrap_err();

    assert_eq!(error, "SystemStream Checkpoint requires SourceScope");
}

#[test]
fn ordinary_stream_rejects_system_scope_and_checkpoint_fields() {
    let token = SystemStreamCheckpointToken::try_from(format!("v1.{}", "42".repeat(16))).unwrap();
    let request = json!({
        "TableName": "TestTable",
        "SourceScope": {
            "Version": 1,
            "ScopeId": "catalog",
            "Generation": 1,
        },
        "Checkpoint": token.as_str(),
    });

    let error = GetStreamRecordsRequest::try_from(request).unwrap_err();

    assert_eq!(error, "SystemStream fields require SystemStream=true");
}

#[test]
fn ordinary_get_stream_records_request_preserves_legacy_table_cursor_wire_shape() {
    let payload = json!({
        "TableName": "LedgerEntries",
        "LastEvaluatedKey": "cursor-1",
        "Limit": 100,
    });

    let parsed = GetStreamRecordsRequest::try_from(payload).unwrap();

    assert!(!parsed.is_system_stream());
    assert_eq!(
        parsed.table_name().map(TableName::as_ref),
        Some("LedgerEntries")
    );
    assert_eq!(parsed.last_evaluated_key(), Some("cursor-1"));
    assert_eq!(parsed.limit(), Some(100));
    assert_eq!(
        serde_json::to_value(parsed).unwrap(),
        json!({
            "TableName": "LedgerEntries",
            "SystemStream": false,
            "LastEvaluatedKey": "cursor-1",
            "Limit": 100,
        })
    );
}

#[test]
fn ordinary_get_stream_records_request_rejects_unknown_wire_fields() {
    let error = GetStreamRecordsRequest::try_from(json!({
        "TableName": "LedgerEntries",
        "UnexpectedField": true,
    }))
    .unwrap_err();

    assert!(error.contains("unknown field"), "unexpected error: {error}");
}

#[test]
fn given_conflicting_stream_source_when_request_is_parsed_then_request_is_rejected() {
    let request = json!({
        "TableName": "TestTable",
        "SystemStream": true
    });

    let error = GetStreamRecordsRequest::try_from(request).unwrap_err();

    assert_eq!(error, "TableName and SystemStream cannot be used together");
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

    let batch_write_err = BatchWriteItemRequest::try_from(batch_write_both_payload).unwrap_err();
    let batch_get_err = BatchGetItemRequest::try_from(batch_get_empty_key_payload).unwrap_err();
    let transact_err =
        TransactWriteItemsRequest::try_from(transact_write_multiple_ops_payload).unwrap_err();

    assert!(batch_write_err.contains("exactly one of PutRequest or DeleteRequest"));
    assert_eq!(
        batch_get_err,
        "The provided key element does not match the schema"
    );
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

    let update_err = UpdateTableRequest::try_from(empty_index_name_payload).unwrap_err();
    let create_err = UpdateTableRequest::try_from(missing_create_key_schema_payload).unwrap_err();

    assert!(update_err.contains("Update.IndexName cannot be empty"));
    assert!(create_err.contains("Create.KeySchema cannot be empty"));
}

#[test]
fn update_item_ignores_unknown_fields_and_rejects_missing_identity() {
    let unknown_field_payload = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "UpdateExpression": "SET #name = :value",
        "ExpressionAttributeNames": { "#name": "name" },
        "ExpressionAttributeValues": { ":value": { "S": "value" } },
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

    let unknown_field_request = UpdateItemRequest::try_from(unknown_field_payload).unwrap();
    let empty_table_err = UpdateItemRequest::try_from(empty_table_payload).unwrap_err();
    let empty_key_err = UpdateItemRequest::try_from(empty_key_payload).unwrap_err();

    assert_eq!(unknown_field_request.table_name.as_ref(), "TestTable");
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

    let missing_value_err = UpdateItemRequest::try_from(missing_value_payload).unwrap_err();
    let invalid_value_key_err = UpdateItemRequest::try_from(invalid_value_key_payload).unwrap_err();

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

    let missing_name_err = UpdateItemRequest::try_from(missing_name_payload).unwrap_err();
    let invalid_name_key_err = UpdateItemRequest::try_from(invalid_name_key_payload).unwrap_err();

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

    let missing_values_err = ScanRequest::try_from(missing_values_payload).unwrap_err();
    let invalid_filter_err = ScanRequest::try_from(invalid_filter_payload).unwrap_err();

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
        QueryRequest::try_from(query_missing_value).unwrap_err(),
        "Invalid KeyConditionExpression: An expression attribute value used in expression is not \
         defined; attribute value: :pk"
    );
    assert_eq!(
        QueryRequest::try_from(query_invalid_value_key).unwrap_err(),
        "ExpressionAttributeValues contains invalid key: Syntax error; key: \"pk\""
    );
    assert_eq!(
        QueryRequest::try_from(query_missing_name).unwrap_err(),
        "Invalid ProjectionExpression: An expression attribute name used in the document path is \
         not defined; attribute name: #name"
    );
    assert_eq!(
        ScanRequest::try_from(scan_invalid_name_key).unwrap_err(),
        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"name\""
    );
    assert_eq!(
        ScanRequest::try_from(scan_unused_name).unwrap_err(),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}"
    );
    assert_eq!(
        GetItemRequest::try_from(get_reserved_comment).unwrap_err(),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert_eq!(
        GetItemRequest::try_from(get_reserved_name_lowercase).unwrap_err(),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         name"
    );
    assert_eq!(
        GetItemRequest::try_from(get_reserved_update_keyword).unwrap_err(),
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
    let put_trailing_open_parenthesis = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "attribute_exists("
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
    let put_size_between = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "1" } },
        "ConditionExpression": "size(#n) BETWEEN :lower AND :upper",
        "ExpressionAttributeNames": { "#n": "name" },
        "ExpressionAttributeValues": {
            ":lower": { "N": "1" },
            ":upper": { "N": "8" }
        }
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
        PutItemRequest::try_from(put_missing_value).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: An expression attribute value \
         used in expression is not defined; attribute value: :value"
    );
    assert_eq!(
        DeleteItemRequest::try_from(delete_invalid_name_key).unwrap_err(),
        "1 validation error detected: ExpressionAttributeNames contains invalid key: Syntax \
         error; key: \"name\""
    );
    assert_eq!(
        DeleteItemRequest::try_from(delete_reserved_word).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Attribute name is a reserved \
         keyword; reserved keyword: COUNT"
    );
    assert!(PutItemRequest::try_from(put_single_parenthesized_condition).is_ok());
    assert_eq!(
        PutItemRequest::try_from(put_double_parenthesized_condition).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: The expression has redundant \
         parentheses;"
    );
    assert_eq!(
        PutItemRequest::try_from(put_double_parenthesized_operand).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: The expression has redundant \
         parentheses;"
    );
    assert_eq!(
        PutItemRequest::try_from(put_nested_reserved_word).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Attribute name is a reserved \
         keyword; reserved keyword: COMMENT"
    );
    assert_eq!(
        PutItemRequest::try_from(put_function_nested_reserved_word).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Attribute name is a reserved \
         keyword; reserved keyword: COMMENT"
    );
    assert!(PutItemRequest::try_from(put_function_alias_nested_reserved_word).is_ok());
    assert_eq!(
        PutItemRequest::try_from(put_contains_same_operand).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: The first operand must be \
         distinct from the remaining operands for this operator or function; operator: contains, \
         first operand: [name]"
    );
    assert_eq!(
        PutItemRequest::try_from(put_trailing_open_parenthesis).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Syntax error; token: \
         \"<EOF>\", near: \"(\""
    );
    assert_eq!(
        PutItemRequest::try_from(put_attribute_type_literal).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Incorrect operand type for \
         operator or function; operator or function: attribute_type, operand type: \
         {FS,NULL,DICT,DOUBLESET,M,DOUBLE,SS,DECIMALSET,S,INT,HDS,FLOAT,B,INTSET,L,DECIMAL,N,NS,\
         BOOL,BS,HD}"
    );
    assert_eq!(
        PutItemRequest::try_from(put_attribute_type_invalid_type_name).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Invalid attribute type name \
         found; type: STRING, valid types: {S,SS,N,NS,B,BS,BOOL,NULL,L,M}"
    );
    assert_eq!(
        PutItemRequest::try_from(put_begins_with_number_operand).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Incorrect operand type for \
         operator or function; operator or function: begins_with, operand type: N"
    );
    assert_eq!(
        PutItemRequest::try_from(put_contains_one_arg).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Incorrect number of operands \
         for operator or function; operator or function: contains, number of operands: 1"
    );
    assert_eq!(
        PutItemRequest::try_from(put_begins_with_three_args).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Incorrect number of operands \
         for operator or function; operator or function: begins_with, number of operands: 3"
    );
    assert_eq!(
        PutItemRequest::try_from(put_attribute_type_no_args).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Syntax error; token: \")\", \
         near: \"()\""
    );
    assert_eq!(
        PutItemRequest::try_from(put_size_two_args).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Incorrect number of operands \
         for operator or function; operator or function: size, number of operands: 2"
    );
    assert_eq!(
        PutItemRequest::try_from(put_size_no_comparison).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: The function is not allowed to \
         be used this way in an expression; function: size"
    );
    assert_eq!(
        PutItemRequest::try_from(put_size_nested_function_arg).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: The function is not allowed to \
         be used this way in an expression; function: contains"
    );
    assert!(PutItemRequest::try_from(put_size_greater_than).is_ok());
    assert!(PutItemRequest::try_from(put_size_between).is_ok());
    assert_eq!(
        PutItemRequest::try_from(put_between_number_bounds_reversed).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: The BETWEEN operator requires \
         upper bound to be greater than or equal to lower bound; lower bound operand: \
         AttributeValue: {N:5}, upper bound operand: AttributeValue: {N:4}"
    );
    assert_eq!(
        PutItemRequest::try_from(put_between_string_bounds_reversed).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: The BETWEEN operator requires \
         upper bound to be greater than or equal to lower bound; lower bound operand: \
         AttributeValue: {S:z}, upper bound operand: AttributeValue: {S:a}"
    );
    assert_eq!(
        PutItemRequest::try_from(put_between_binary_bounds_reversed).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: The BETWEEN operator requires \
         upper bound to be greater than or equal to lower bound; lower bound operand: \
         AttributeValue: {B:eg==}, upper bound operand: AttributeValue: {B:YQ==}"
    );
    assert!(PutItemRequest::try_from(put_between_equal_bounds).is_ok());
}

#[test]
fn expression_attribute_names_reject_aggregate_size_above_dynamodb_limit() {
    let oversized_name = "a".repeat((2 * 1024 * 1024) + 1);
    let delete_item = json!({
        "TableName": "TestTable",
        "Key": { "pk": { "S": "1" } },
        "ConditionExpression": "attribute_not_exists(#n)",
        "ExpressionAttributeNames": { "#n": oversized_name }
    });
    let transact_write = json!({
        "TransactItems": [{
            "Delete": {
                "TableName": "TestTable",
                "Key": { "pk": { "S": "1" } },
                "ConditionExpression": "attribute_not_exists(#n)",
                "ExpressionAttributeNames": { "#n": "a".repeat((2 * 1024 * 1024) + 1) }
            }
        }]
    });

    assert_eq!(
        DeleteItemRequest::try_from(delete_item).unwrap_err(),
        "1 validation error detected: ExpressionAttributeNames exceeds max size"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_write).unwrap_err(),
        "ExpressionAttributeNames exceeds max size"
    );
}

#[test]
fn write_condition_expressions_reject_reversed_between_bounds_for_all_write_shapes() {
    for (lower, upper) in [
        ("5", "4"),
        ("9007199254740993", "9007199254740992"),
        ("-9007199254740992", "-9007199254740993"),
    ] {
        let detail = format!(
            "Invalid ConditionExpression: The BETWEEN operator requires upper bound to be greater \
             than or equal to lower bound; lower bound operand: AttributeValue: {{N:{lower}}}, \
             upper bound operand: AttributeValue: {{N:{upper}}}"
        );
        let expected = format!("1 validation error detected: {detail}");
        let expression_attribute_values = json!({
            ":lower": { "N": lower },
            ":upper": { "N": upper }
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
                ":lower": { "N": lower },
                ":upper": { "N": upper }
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

        assert_eq!(PutItemRequest::try_from(put).unwrap_err(), expected);
        assert_eq!(DeleteItemRequest::try_from(delete).unwrap_err(), expected);
        assert_eq!(UpdateItemRequest::try_from(update).unwrap_err(), expected);
        assert_eq!(
            TransactWriteItemsRequest::try_from(transact_write).unwrap_err(),
            detail
        );
    }
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
        UpdateItemRequest::try_from(update_contains_value).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: The function is not allowed in an \
         update expression; function: contains"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_size_value).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: The function is not allowed in an \
         update expression; function: size"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_nested_list_append).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: The function is not allowed to be \
         used this way in an expression; function: list_append"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_nested_if_not_exists_first_arg).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Operator or function requires a \
         document path; operator or function: if_not_exists"
    );
    assert!(
        UpdateItemRequest::try_from(update_list_append_if_not_exists).is_ok(),
        "DynamoDB accepts if_not_exists as a list_append operand"
    );
    assert_eq!(
        UpdateItemRequest::try_from(condition_if_not_exists).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: The function is not allowed in \
         a condition expression; function: if_not_exists"
    );
    assert_eq!(
        UpdateItemRequest::try_from(mixed_update_and_condition_invalid).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: The function is not allowed in \
         a condition expression; function: if_not_exists"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_update_contains_value).unwrap_err(),
        "Invalid UpdateExpression: The function is not allowed in an update expression; function: \
         contains"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_condition_if_not_exists).unwrap_err(),
        "Invalid ConditionExpression: The function is not allowed in a condition expression; \
         function: if_not_exists"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_mixed_update_and_condition_invalid)
            .unwrap_err(),
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
        UpdateItemRequest::try_from(update_duplicate_set).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: The \"SET\" section can only be \
         used once in an update expression;"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_repeated_path).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Two document paths overlap with \
         each other; must remove or rewrite one of these paths; path one: [fieldx], path two: \
         [fieldx]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_overlapping_path).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Two document paths overlap with \
         each other; must remove or rewrite one of these paths; path one: [mapx], path two: \
         [mapx, num]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_overlapping_list_index_path).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Two document paths overlap with \
         each other; must remove or rewrite one of these paths; path one: [lst, [0]], path two: \
         [lst, [0], ok]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_overlapping_alias_path).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Two document paths overlap with \
         each other; must remove or rewrite one of these paths; path one: [fieldx], path two: \
         [fieldx]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_add_no_value).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"<EOF>\", \
         near: \"numx\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_set_missing_arithmetic_operand).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"<EOF>\", \
         near: \"+\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_set_unsupported_arithmetic_operator).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"*\", near: \
         \"numx * :v\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_delete_no_value).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"<EOF>\", \
         near: \"setx\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_add_literal_value).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"1\", near: \
         \"numx 1\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_delete_literal_value).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"a\", near: \
         \"setx a\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_add_list_value).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Incorrect operand type for \
         operator or function; operator: ADD, operand type: LIST, typeSet: ALLOWED_FOR_ADD_OPERAND"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_delete_string_value).unwrap_err(),
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
        TransactWriteItemsRequest::try_from(transact_duplicate_set).unwrap_err(),
        "Invalid UpdateExpression: The \"SET\" section can only be used once in an update \
         expression;"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_repeated_path).unwrap_err(),
        "Invalid UpdateExpression: Two document paths overlap with each other; must remove or \
         rewrite one of these paths; path one: [fieldx], path two: [fieldx]"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_overlapping_list_index_path).unwrap_err(),
        "Invalid UpdateExpression: Two document paths overlap with each other; must remove or \
         rewrite one of these paths; path one: [lst, [0]], path two: [lst, [0], ok]"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_set_missing_arithmetic_operand).unwrap_err(),
        "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"+\""
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_set_unsupported_arithmetic_operator)
            .unwrap_err(),
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
        GetItemRequest::try_from(get_raw_reserved_list).unwrap_err(),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         list"
    );
    assert_eq!(
        GetItemRequest::try_from(get_raw_reserved_child).unwrap_err(),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert!(GetItemRequest::try_from(get_alias_list_path).is_ok());
    assert_eq!(
        GetItemRequest::try_from(get_empty_index).unwrap_err(),
        "Invalid ProjectionExpression: Syntax error; token: \"]\", near: \"[].\""
    );
    assert_eq!(
        DeleteItemRequest::try_from(delete_raw_reserved_child).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Attribute name is a reserved \
         keyword; reserved keyword: COMMENT"
    );
    assert!(DeleteItemRequest::try_from(delete_alias_list_path).is_ok());
    assert_eq!(
        DeleteItemRequest::try_from(delete_alpha_index).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Syntax error; token: \"abc\", \
         near: \"[abc]\""
    );
    assert_eq!(
        ScanRequest::try_from(scan_raw_reserved_child).unwrap_err(),
        "Invalid FilterExpression: Attribute name is a reserved keyword; reserved keyword: COMMENT"
    );
    assert!(ScanRequest::try_from(scan_alias_list_path).is_ok());
    assert_eq!(
        ScanRequest::try_from(scan_negative_index).unwrap_err(),
        "Invalid FilterExpression: Syntax error; token: \"-\", near: \"[-1\""
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_raw_reserved_child).unwrap_err(),
        "1 validation error detected: Invalid UpdateExpression: Attribute name is a reserved \
         keyword; reserved keyword: COMMENT"
    );
    assert!(UpdateItemRequest::try_from(update_alias_list_path).is_ok());
    assert_eq!(
        UpdateItemRequest::try_from(update_unclosed_index).unwrap_err(),
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
        TransactGetItemsRequest::try_from(transact_get_reserved_projection).unwrap_err(),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert_eq!(
        TransactGetItemsRequest::try_from(transact_get_missing_name).unwrap_err(),
        "Invalid ProjectionExpression: An expression attribute name used in the document path is \
         not defined; attribute name: #comment"
    );
    assert_eq!(
        TransactGetItemsRequest::try_from(transact_get_invalid_name_key).unwrap_err(),
        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"comment\""
    );
    assert!(TransactGetItemsRequest::try_from(transact_get_alias_reserved_projection).is_ok());
    assert_eq!(
        TransactWriteItemsRequest::try_from(condition_check_missing_value).unwrap_err(),
        "Invalid ConditionExpression: An expression attribute value used in expression is not \
         defined; attribute value: :value"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(condition_check_invalid_value_key).unwrap_err(),
        "ExpressionAttributeValues contains invalid key: Syntax error; key: \"value\""
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(update_reserved_word).unwrap_err(),
        "Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: COMMENT"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(update_condition_syntax).unwrap_err(),
        "Invalid ConditionExpression: Syntax error; token: \"<EOF>\", near: \"=\""
    );
    assert!(
        TransactWriteItemsRequest::try_from(condition_check_single_parenthesized_condition).is_ok()
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(condition_check_double_parenthesized_condition)
            .unwrap_err(),
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

    let batch_write_empty_err =
        BatchWriteItemRequest::try_from(batch_write_empty_request_items).unwrap_err();
    let batch_write_too_many_err =
        BatchWriteItemRequest::try_from(batch_write_too_many_payload).unwrap_err();
    let batch_get_empty_table_err =
        BatchGetItemRequest::try_from(batch_get_empty_table_name_payload).unwrap_err();
    let batch_get_too_many_err =
        BatchGetItemRequest::try_from(batch_get_too_many_payload).unwrap_err();

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
        QueryRequest::try_from(query_invalid_return_consumed_capacity).unwrap_err(),
        invalid_return_consumed_capacity_message
    );
    assert_eq!(
        ScanRequest::try_from(scan_invalid_return_consumed_capacity).unwrap_err(),
        invalid_return_consumed_capacity_message
    );
    assert_eq!(
        BatchGetItemRequest::try_from(batch_get_invalid_return_consumed_capacity).unwrap_err(),
        invalid_return_consumed_capacity_message
    );
    assert_eq!(
        BatchWriteItemRequest::try_from(batch_write_invalid_return_consumed_capacity).unwrap_err(),
        invalid_return_consumed_capacity_message
    );
    assert_eq!(
        BatchWriteItemRequest::try_from(batch_write_invalid_return_item_collection_metrics)
            .unwrap_err(),
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
    let too_long_name_token = format!("#{}", "a".repeat(255));
    let too_long_projection_name = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{ "pk": { "S": "1" } }],
                "ProjectionExpression": too_long_name_token,
                "ExpressionAttributeNames": {
                    format!("#{}", "a".repeat(255)): "m"
                }
            }
        }
    });
    let too_long_projection_expression = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{ "pk": { "S": "1" } }],
                "ProjectionExpression": "a".repeat(crate::MAX_EXPRESSION_BYTES + 1)
            }
        }
    });
    let overlapping_projection = json!({
        "RequestItems": {
            "TestTable": {
                "Keys": [{ "pk": { "S": "1" } }],
                "ProjectionExpression": "m, m.child"
            }
        }
    });

    assert_eq!(
        BatchGetItemRequest::try_from(raw_reserved_projection).unwrap_err(),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert_eq!(
        BatchGetItemRequest::try_from(nested_reserved_projection).unwrap_err(),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert_eq!(
        BatchGetItemRequest::try_from(alias_parent_raw_reserved_child).unwrap_err(),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
    assert!(BatchGetItemRequest::try_from(alias_nested_reserved_projection).is_ok());
    assert_eq!(
        BatchGetItemRequest::try_from(invalid_name_key).unwrap_err(),
        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"m\""
    );
    assert_eq!(
        BatchGetItemRequest::try_from(missing_name).unwrap_err(),
        "Invalid ProjectionExpression: An expression attribute name used in the document path is \
         not defined; attribute name: #m"
    );
    assert_eq!(
        BatchGetItemRequest::try_from(unused_name).unwrap_err(),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}"
    );
    assert_eq!(
        BatchGetItemRequest::try_from(too_long_projection_name).unwrap_err(),
        "ExpressionAttributeNames contains invalid key: The expression attribute map contains a \
         key that is too long; size of key: 256"
    );
    assert_eq!(
        BatchGetItemRequest::try_from(too_long_projection_expression).unwrap_err(),
        "Invalid ProjectionExpression: Expression size has exceeded the maximum allowed size;"
    );
    assert_eq!(
        BatchGetItemRequest::try_from(overlapping_projection).unwrap_err(),
        "Invalid ProjectionExpression: Two document paths overlap with each other; must remove or \
         rewrite one of these paths; path one: [m], path two: [m, child]"
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

    let empty_transact_err =
        TransactWriteItemsRequest::try_from(empty_transact_items_payload).unwrap_err();
    let too_many_transact_err =
        TransactWriteItemsRequest::try_from(too_many_transact_items_payload).unwrap_err();
    let empty_gsi_updates_err =
        UpdateTableRequest::try_from(empty_gsi_updates_payload).unwrap_err();
    let delete_blank_index_err =
        UpdateTableRequest::try_from(delete_blank_index_payload).unwrap_err();

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
        TransactWriteItemsRequest::try_from(invalid_return_consumed_capacity).unwrap_err(),
        "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy \
         constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(invalid_return_item_collection_metrics).unwrap_err(),
        "1 validation error detected: Value 'BOGUS' at 'returnItemCollectionMetrics' failed to \
         satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(invalid_return_values_on_condition_failure)
            .unwrap_err(),
        "1 validation error detected: Value 'BOGUS' at \
         'transactItems.1.member.put.returnValuesOnConditionCheckFailure' failed to satisfy \
         constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(empty_expression_names).unwrap_err(),
        "ExpressionAttributeNames must not be empty"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(empty_expression_values).unwrap_err(),
        "ExpressionAttributeValues must not be empty"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(empty_transact_item).unwrap_err(),
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
        PutItemRequest::try_from(put_empty_values).unwrap_err(),
        "1 validation error detected: ExpressionAttributeValues must not be empty"
    );
    assert_eq!(
        PutItemRequest::try_from(put_invalid_return_values_on_condition_failure).unwrap_err(),
        "1 validation error detected: Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' \
         failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"
    );
    assert_eq!(
        PutItemRequest::try_from(put_invalid_return_consumed_capacity).unwrap_err(),
        "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy \
         constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );
    assert_eq!(
        PutItemRequest::try_from(put_invalid_return_item_collection_metrics).unwrap_err(),
        "1 validation error detected: ReturnItemCollectionMetrics can only be SIZE or NONE"
    );
    assert_eq!(
        DeleteItemRequest::try_from(delete_invalid_return_values_on_condition_failure).unwrap_err(),
        "1 validation error detected: Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' \
         failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_invalid_return_values_on_condition_failure).unwrap_err(),
        "Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' failed to satisfy constraint: \
         Member must satisfy enum value set: [ALL_OLD, NONE]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_invalid_return_consumed_capacity).unwrap_err(),
        "Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must \
         satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );
    assert_eq!(
        UpdateItemRequest::try_from(update_invalid_return_item_collection_metrics).unwrap_err(),
        "1 validation error detected: ReturnItemCollectionMetrics can only be SIZE or NONE"
    );
    assert_eq!(
        GetItemRequest::try_from(get_invalid_return_consumed_capacity).unwrap_err(),
        "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy \
         constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );
    assert_eq!(
        GetItemRequest::try_from(get_empty_expression_names).unwrap_err(),
        "ExpressionAttributeNames must not be empty"
    );
    assert_eq!(
        TransactGetItemsRequest::try_from(transact_get_invalid_return_consumed_capacity)
            .unwrap_err(),
        "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy \
         constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );
    assert_eq!(
        TransactGetItemsRequest::try_from(transact_get_empty_expression_names).unwrap_err(),
        "ExpressionAttributeNames must not be empty"
    );
}

#[test]
fn dynamodb_limit_boundaries_are_accepted() {
    let max_item = "x".repeat(crate::constants::MAX_ITEM_SIZE_BYTES - "pk".len());
    let max_binary_item_bytes =
        crate::constants::MAX_ITEM_SIZE_BYTES - "pk".len() - "p".len() - "data".len();
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
    let max_binary_item_bytes =
        crate::constants::MAX_ITEM_SIZE_BYTES - "pk".len() - "p".len() - "data".len();
    let max_nested_list_binary_item_bytes = max_binary_item_bytes - 3 - 1;
    let max_nested_map_binary_item_bytes = max_binary_item_bytes - 3 - 1 - "child".len();
    let oversized_item = json!({
        "TableName": "TestTable",
        "Item": { "pk": { "S": "x".repeat(crate::constants::MAX_ITEM_SIZE_BYTES) } }
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
        "Item": { "x".repeat(crate::constants::MAX_ATTRIBUTE_NAME_BYTES + 1): { "S": "1" } }
    });
    let too_deep_item = json!({
        "TableName": "TestTable",
        "Item": { "pk": nested_attribute_value(crate::constants::MAX_ATTRIBUTE_NESTING_DEPTH + 1) }
    });

    assert_eq!(
        PutItemRequest::try_from(oversized_item).unwrap_err(),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_binary_item).unwrap_err(),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_binary_set_item).unwrap_err(),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_numeric_set_item).unwrap_err(),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_nested_list_number_item).unwrap_err(),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_nested_map_numeric_set_item).unwrap_err(),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_nested_list_binary_item).unwrap_err(),
        "Item size has exceeded the maximum allowed size"
    );
    assert_eq!(
        PutItemRequest::try_from(oversized_nested_map_binary_set_item).unwrap_err(),
        "Item size has exceeded the maximum allowed size"
    );
    assert!(
        PutItemRequest::try_from(oversized_attribute_name)
            .unwrap_err()
            .contains("attribute name cannot exceed")
    );
    assert!(
        PutItemRequest::try_from(too_deep_item)
            .unwrap_err()
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
            .unwrap_err()
            .contains("between 3 and 255")
    );
    assert!(
        CreateTableRequest::try_from(invalid_index_name)
            .unwrap_err()
            .contains("invalid characters")
    );
    assert!(
        CreateTableRequest::try_from(too_many_projected)
            .unwrap_err()
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
            .unwrap_err()
            .contains("FilterExpression")
    );
    assert!(
        QueryRequest::try_from(query)
            .unwrap_err()
            .contains("ProjectionExpression")
    );
    assert!(
        PutItemRequest::try_from(put)
            .unwrap_err()
            .contains("ConditionExpression")
    );
    assert_eq!(
        ListTablesRequest::try_from(list_tables).unwrap_err(),
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
                "Item": { "pk": { "S": "x".repeat(crate::constants::MAX_TRANSACTION_REQUEST_BYTES) } }
            }
        }]
    });

    assert_eq!(
        TransactGetItemsRequest::try_from(too_many_gets).unwrap_err(),
        "TransactItems cannot contain more than 100 operations"
    );
    assert!(
        TransactWriteItemsRequest::try_from(oversized_request)
            .unwrap_err()
            .contains("Item size has exceeded the maximum allowed size")
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
            PutItemRequest::try_from(put_item).unwrap_err(),
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
        BatchWriteItemRequest::try_from(batch_write).unwrap_err(),
        "One or more parameter values were invalid: Input collection contains duplicates."
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_write).unwrap_err(),
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
            UpdateItemRequest::try_from(update_item).unwrap_err(),
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
        TransactWriteItemsRequest::try_from(transact_set_duplicate).unwrap_err(),
        "ExpressionAttributeValues contains invalid value: Input collection contains duplicates \
         for key :set"
    );
    assert_eq!(
        TransactWriteItemsRequest::try_from(transact_add_empty).unwrap_err(),
        "ExpressionAttributeValues contains invalid value: One or more parameter values were \
         invalid: Binary sets should not be empty for key :set"
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
