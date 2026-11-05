use std::sync::Arc;

use storage_types::{BoundTableInfo, StorageEnum, UpdateItemRequest, context::WrappedError as _};

use crate::UpdateItemShape;

fn table() -> Arc<BoundTableInfo> {
    let info = serde_json::from_value(serde_json::json!({
        "table_name": "key-update",
        "table_status": "ACTIVE",
        "created_at": 0,
        "attribute_definitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "key_schema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "max_indexers": 0,
        "global_secondary_indexes": null,
        "table_size_bytes": 0,
        "item_count": 0,
        "stream_specification": null
    }))
    .expect("table info");
    Arc::new(BoundTableInfo {
        info: Arc::new(info),
        history_id: 1_u32.into(),
    })
}

#[test]
fn given_key_attribute_set_when_bound_then_validation_rejects_it() {
    let request: UpdateItemRequest = serde_json::from_value(serde_json::json!({
        "TableName": "key-update",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET pk = :new",
        "ExpressionAttributeValues": {":new": {"S": "b"}}
    }))
    .expect("request");

    let error = UpdateItemShape::try_from_request(request)
        .expect("shape")
        .bind_table(table())
        .err()
        .expect("key update must be rejected");

    assert!(
        matches!(error.to_enum(), StorageEnum::Validation { .. }),
        "expected validation, got {error:?}"
    );
}

#[test]
fn given_literal_legacy_updates_when_parsed_then_unsupported_operation_fails_explicitly() {
    let request: UpdateItemRequest = serde_json::from_value(serde_json::json!({
        "TableName": "key-update", "Key": {"pk": {"S": "a"}},
        "AttributeUpdates": {"a.b": {"Action": "PUT", "Value": {"S": "value"}}}
    }))
    .unwrap();
    assert!(UpdateItemShape::try_from_request(request).is_err());
}

#[test]
fn given_literal_path_alias_when_parsed_then_it_cannot_mutate_a_nested_attribute() {
    let request: UpdateItemRequest = serde_json::from_value(serde_json::json!({
        "TableName": "key-update", "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET #literal = :value",
        "ExpressionAttributeNames": {"#literal": "a.b"},
        "ExpressionAttributeValues": {":value": {"S": "value"}}
    }))
    .unwrap();
    assert!(UpdateItemShape::try_from_request(request).is_err());
}
