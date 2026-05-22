use std::collections::HashMap;

use serde_json::{Value, json};
use storage::DatabaseManager;
use storage_types::{AttributeValue, TableName, UpdateItemRequest, UpdateItemResponse};

use crate::{
    routes::routes_test_support::{
        default_conformance_backends, handle_create_table, handle_put_item, handle_update_item,
    },
    types::Response,
};

#[test]
fn update_item_request_serialization() {
    let mut key = HashMap::new();
    key.insert("id".to_string(), AttributeValue::S("test-id".to_string()));

    let mut expression_values = HashMap::new();
    expression_values.insert(
        ":val".to_string(),
        AttributeValue::S("new-value".to_string()),
    );

    let request = UpdateItemRequest::builder()
        .table_name(TableName::new("test-table"))
        .key(key)
        .update_expression("SET attr = :val")
        .expression_attribute_values(Some(expression_values))
        .return_values(Some(storage_types::ReturnValuesOldNewUpdated::AllNew))
        .build();

    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("test-table"));
    assert!(json.contains("SET attr = :val"));
}

#[test]
fn update_item_response_serialization() {
    let mut attributes = HashMap::new();
    attributes.insert("id".to_string(), AttributeValue::S("test-id".to_string()));

    let response = UpdateItemResponse {
        attributes: Some(attributes.into()),
    };

    let json = serde_json::to_string(&response).unwrap();
    assert!(json.contains("Attributes"));
}

#[tokio::test]
async fn update_item_key_schema_validation_matches_across_conformance_backends() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_composite_table(db.clone(), "UpdateItemKeySchema").await;

        for payload in [
            json!({
                "TableName": "UpdateItemKeySchema",
                "Key": {
                    "pk": {"S": "p"}
                },
                "UpdateExpression": "SET payload = :value",
                "ExpressionAttributeValues": {
                    ":value": {"S": "updated"}
                }
            }),
            json!({
                "TableName": "UpdateItemKeySchema",
                "Key": {
                    "pk": {"N": "1"},
                    "sk": {"S": "s"}
                },
                "UpdateExpression": "SET payload = :value",
                "ExpressionAttributeValues": {
                    ":value": {"S": "updated"}
                }
            }),
        ] {
            let err = handle_update_item(db.clone(), payload.try_into().unwrap())
                .await
                .expect_err("invalid UpdateItem key should fail");
            assert_eq!(err.status_code, 400, "{}", backend.name);
            assert_eq!(
                err.error_type, "com.amazon.coral.validate#ValidationException",
                "{}",
                backend.name
            );
            assert_eq!(
                err.message, "The provided key element does not match the schema",
                "{}",
                backend.name
            );
        }
    }
}

#[tokio::test]
async fn update_item_runtime_validation_matches_across_conformance_backends() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_hash_table(db.clone(), "UpdateRuntime").await;
        handle_put_item(
            db.clone(),
            json!({
                "TableName": "UpdateRuntime",
                "Item": {
                    "pk": {"S": "p"},
                    "n": {"N": "3"},
                    "s": {"S": "old"}
                }
            })
            .try_into()
            .expect("put item request"),
        )
        .await
        .unwrap_or_else(|err| panic!("{} seed item: {err:?}", backend.name));

        let err = handle_update_item(
            db,
            json!({
                "TableName": "UpdateRuntime",
                "Key": {"pk": {"S": "p"}},
                "UpdateExpression": "SET n = n + s"
            })
            .try_into()
            .expect("update item request"),
        )
        .await
        .expect_err("invalid update runtime should fail");

        assert_eq!(err.status_code, 400, "{}", backend.name);
        assert_eq!(
            err.error_type, "com.amazon.coral.validate#ValidationException",
            "{}",
            backend.name
        );
        assert_eq!(
            err.message, "An operand in the update expression has an incorrect data type",
            "{}",
            backend.name
        );
    }
}

#[tokio::test]
async fn update_item_condition_failure_matches_across_conformance_backends() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_hash_table(db.clone(), "UpdateConditional").await;
        handle_put_item(
            db.clone(),
            json!({
                "TableName": "UpdateConditional",
                "Item": {
                    "pk": {"S": "p"},
                    "status": {"S": "open"}
                }
            })
            .try_into()
            .expect("put item request"),
        )
        .await
        .unwrap_or_else(|err| panic!("{} seed item: {err:?}", backend.name));

        let err = handle_update_item(
            db,
            json!({
                "TableName": "UpdateConditional",
                "Key": {"pk": {"S": "p"}},
                "UpdateExpression": "SET #status = :next",
                "ConditionExpression": "#status = :expected",
                "ExpressionAttributeNames": {
                    "#status": "status"
                },
                "ExpressionAttributeValues": {
                    ":next": {"S": "closed"},
                    ":expected": {"S": "missing"}
                }
            })
            .try_into()
            .expect("update item request"),
        )
        .await
        .expect_err("conditional update should fail");

        assert_eq!(err.status_code, 400, "{}", backend.name);
        assert_eq!(
            err.error_type, "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
            "{}",
            backend.name
        );
        assert_eq!(
            err.message, "The conditional request failed",
            "{}",
            backend.name
        );
    }
}

#[tokio::test]
async fn update_item_return_values_for_deep_paths_match_dynamodb_across_conformance_backends() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_hash_table(db.clone(), "UpdateDeepReturnValues").await;

        for case in deep_return_values_cases() {
            put_deep_return_value_item(db.clone(), case.pk).await;

            let response = handle_update_item(
                db.clone(),
                case.request().try_into().expect("update item request"),
            )
            .await
            .unwrap_or_else(|err| {
                panic!("{} {} update item failed: {err:?}", backend.name, case.pk)
            });

            let Response::UpdateItem(response) = response else {
                panic!("{} {} expected UpdateItem response", backend.name, case.pk);
            };
            assert_eq!(
                serde_json::to_value(response).expect("response json"),
                case.expected_response,
                "{} {}: {}",
                backend.name,
                case.pk,
                case.dynamodb_behavior
            );
        }
    }
}

struct DeepReturnValuesCase {
    pk: &'static str,
    dynamodb_behavior: &'static str,
    update_expression: &'static str,
    expression_attribute_values: Option<Value>,
    return_values: &'static str,
    expected_response: Value,
}

impl DeepReturnValuesCase {
    fn request(&self) -> Value {
        let mut request = json!({
            "TableName": "UpdateDeepReturnValues",
            "Key": {"pk": {"S": self.pk}},
            "UpdateExpression": self.update_expression,
            "ReturnValues": self.return_values
        });
        if let Some(values) = self.expression_attribute_values.as_ref() {
            request["ExpressionAttributeValues"] = values.clone();
        }
        request
    }
}

fn deep_return_values_cases() -> Vec<DeepReturnValuesCase> {
    vec![
        case(
            "deep-map-all-new",
            "ALL_NEW returns the complete item after the update",
            "SET m.a.b = :v",
            Some(json!({":v": {"S": "new-b"}})),
            "ALL_NEW",
            full_item_response("deep-map-all-new", map_after_b_set(), original_list()),
        ),
        case(
            "deep-map-all-old",
            "ALL_OLD returns the complete item before the update",
            "SET m.a.b = :v",
            Some(json!({":v": {"S": "new-b"}})),
            "ALL_OLD",
            full_item_response("deep-map-all-old", original_map(), original_list()),
        ),
        case(
            "list-child-all-new",
            "ALL_NEW includes the complete updated list, not a fragment",
            "SET l[1].a = :v",
            Some(json!({":v": {"S": "new1"}})),
            "ALL_NEW",
            full_item_response(
                "list-child-all-new",
                original_map(),
                list_with_second_a("new1"),
            ),
        ),
        case(
            "list-child-all-old",
            "ALL_OLD includes the complete old list, not a fragment",
            "SET l[1].a = :v",
            Some(json!({":v": {"S": "new1"}})),
            "ALL_OLD",
            full_item_response("list-child-all-old", original_map(), original_list()),
        ),
        case(
            "deep-map-updated-new",
            "UPDATED_NEW returns only the changed nested map leaf",
            "SET m.a.b = :v",
            Some(json!({":v": {"S": "new-b"}})),
            "UPDATED_NEW",
            attributes(json!({"m": {"M": {"a": {"M": {"b": {"S": "new-b"}}}}}})),
        ),
        case(
            "deep-map-updated-old",
            "UPDATED_OLD returns only the old nested map leaf",
            "SET m.a.b = :v",
            Some(json!({":v": {"S": "new-b"}})),
            "UPDATED_OLD",
            attributes(json!({"m": {"M": {"a": {"M": {"b": {"S": "old-b"}}}}}})),
        ),
        case(
            "deep-map-multi-updated-new",
            "UPDATED_NEW merges sibling nested leaves changed by the same update",
            "SET m.a.b = :v, m.a.c = :n",
            Some(json!({":v": {"S": "new-b"}, ":n": {"N": "2"}})),
            "UPDATED_NEW",
            attributes(json!({"m": {"M": {"a": {"M": {"b": {"S": "new-b"}, "c": {"N": "2"}}}}}})),
        ),
        case(
            "list-child-updated-new",
            "UPDATED_NEW returns only the changed field inside the addressed list element",
            "SET l[1].a = :v",
            Some(json!({":v": {"S": "new1"}})),
            "UPDATED_NEW",
            attributes(json!({"l": {"L": [{"M": {"a": {"S": "new1"}}}]}})),
        ),
        case(
            "list-child-updated-old",
            "UPDATED_OLD returns only the old field inside the addressed list element",
            "SET l[1].a = :v",
            Some(json!({":v": {"S": "new1"}})),
            "UPDATED_OLD",
            attributes(json!({"l": {"L": [{"M": {"a": {"S": "old1"}}}]}})),
        ),
        case(
            "list-element-updated-new",
            "UPDATED_NEW returns the replacement value for the addressed list element",
            "SET l[0] = :v",
            Some(json!({":v": {"M": {"a": {"S": "replacement"}, "z": {"S": "z0"}}}})),
            "UPDATED_NEW",
            attributes(json!({"l": {"L": [{"M": {"a": {"S": "replacement"}, "z": {"S": "z0"}}}]}})),
        ),
        case(
            "out-of-range-list-set-updated-new",
            "SET on an out-of-range list index appends, but UPDATED_NEW has no exact-path value",
            "SET rootList[99] = :v",
            Some(json!({":v": {"S": "z"}})),
            "UPDATED_NEW",
            json!({}),
        ),
        case(
            "remove-deep-map-updated-new",
            "REMOVE deletes the exact path, so UPDATED_NEW has no remaining exact-path value",
            "REMOVE m.a.b",
            None,
            "UPDATED_NEW",
            json!({}),
        ),
        case(
            "remove-deep-map-updated-old",
            "UPDATED_OLD returns only the removed nested map leaf",
            "REMOVE m.a.b",
            None,
            "UPDATED_OLD",
            attributes(json!({"m": {"M": {"a": {"M": {"b": {"S": "old-b"}}}}}})),
        ),
        case(
            "remove-list-index-updated-new",
            "UPDATED_NEW returns the list element that shifted into the removed index",
            "REMOVE l[0]",
            None,
            "UPDATED_NEW",
            attributes(json!({"l": {"L": [{"M": {"a": {"S": "old1"}, "b": {"S": "b1"}}}]}})),
        ),
        case(
            "remove-list-index-updated-old",
            "UPDATED_OLD returns only the removed list element",
            "REMOVE l[0]",
            None,
            "UPDATED_OLD",
            attributes(json!({"l": {"L": [{"M": {"a": {"S": "old0"}, "b": {"S": "b0"}}}]}})),
        ),
        case(
            "nested-add-updated-new",
            "ADD on a nested number returns only the changed nested number",
            "ADD m.a.c :inc",
            Some(json!({":inc": {"N": "3"}})),
            "UPDATED_NEW",
            attributes(json!({"m": {"M": {"a": {"M": {"c": {"N": "4"}}}}}})),
        ),
    ]
}

fn case(
    pk: &'static str,
    dynamodb_behavior: &'static str,
    update_expression: &'static str,
    expression_attribute_values: Option<Value>,
    return_values: &'static str,
    expected_response: Value,
) -> DeepReturnValuesCase {
    DeepReturnValuesCase {
        pk,
        dynamodb_behavior,
        update_expression,
        expression_attribute_values,
        return_values,
        expected_response,
    }
}

async fn create_hash_table(db: std::sync::Arc<DatabaseManager>, table_name: &str) {
    handle_create_table(
        db,
        json!({
            "TableName": table_name,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"}
            ]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create hash table");
}

async fn put_deep_return_value_item(db: std::sync::Arc<DatabaseManager>, pk: &str) {
    handle_put_item(
        db,
        json!({
            "TableName": "UpdateDeepReturnValues",
            "Item": {
                "pk": {"S": pk},
                "top": {"S": "keep"},
                "m": {
                    "M": {
                        "a": {
                            "M": {
                                "b": {"S": "old-b"},
                                "c": {"N": "1"}
                            }
                        },
                        "keep": {"S": "mk"}
                    }
                },
                "l": {
                    "L": [
                        {"M": {"a": {"S": "old0"}, "b": {"S": "b0"}}},
                        {"M": {"a": {"S": "old1"}, "b": {"S": "b1"}}}
                    ]
                },
                "rootList": {
                    "L": [
                        {"S": "x"},
                        {"S": "y"}
                    ]
                }
            }
        })
        .try_into()
        .expect("put item request"),
    )
    .await
    .expect("seed deep return value item");
}

fn attributes(value: Value) -> Value {
    json!({ "Attributes": value })
}

fn full_item_response(pk: &str, map: Value, list: Value) -> Value {
    json!({
        "Attributes": {
            "pk": {"S": pk},
            "top": {"S": "keep"},
            "m": map,
            "l": {"L": list},
            "rootList": {"L": [{"S": "x"}, {"S": "y"}]}
        }
    })
}

fn original_map() -> Value {
    json!({
        "M": {
            "a": {
                "M": {
                    "b": {"S": "old-b"},
                    "c": {"N": "1"}
                }
            },
            "keep": {"S": "mk"}
        }
    })
}

fn map_after_b_set() -> Value {
    json!({
        "M": {
            "a": {
                "M": {
                    "b": {"S": "new-b"},
                    "c": {"N": "1"}
                }
            },
            "keep": {"S": "mk"}
        }
    })
}

fn original_list() -> Value {
    list_with_second_a("old1")
}

fn list_with_second_a(second_a: &str) -> Value {
    json!([
        {"M": {"a": {"S": "old0"}, "b": {"S": "b0"}}},
        {"M": {"a": {"S": second_a}, "b": {"S": "b1"}}}
    ])
}

async fn create_composite_table(db: std::sync::Arc<DatabaseManager>, table_name: &str) {
    handle_create_table(
        db,
        json!({
            "TableName": table_name,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create composite table");
}
