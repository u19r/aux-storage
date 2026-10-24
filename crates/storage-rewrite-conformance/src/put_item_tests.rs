//! PutItem business and failure contracts through the public replacement store.

use base64::Engine as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    AllOld, AttributeMap, AttributeValue, GetItemRequest, KeyAttributeType, PutItemRequest,
    StorageEnum, TableName, context::WrappedError as _,
};

use crate::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

fn put_request(table: TableName, value: &str) -> PutItemRequest {
    PutItemRequest::new(
        table,
        storage_types::WireItem::try_from(&AttributeMap::from_iter([
            ("pk".to_owned(), AttributeValue::S("one".to_owned())),
            ("value".to_owned(), AttributeValue::S(value.to_owned())),
        ]))
        .unwrap(),
    )
}

async fn read_item(store: &TestStore, table: TableName) -> serde_json::Value {
    let response = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [("pk".to_owned(), AttributeValue::S("one".to_owned()))],
        ))
        .await
        .expect("read the item through the shared Dynamo operation");
    serde_json::to_value(response).expect("serialize Dynamo response")
}

/// Format rule from dynamo-conformance PutItem/200: unknown fields are ignored
/// and success returns {}.
#[tokio::test]
async fn given_unknown_top_level_field_when_put_item_runs_then_response_is_empty_and_item_is_readable()
 {
    let store = TestStore::new().await;
    let table = store.create_standard_table().await;

    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {"pk": {"S": "one"}, "value": {"S": "written"}},
        "UnknownField": "ignored"
    }))
    .expect("parse PutItem request and ignore unknown field");
    let response = store.operations.put_item(request).await.expect("put item");

    assert_eq!(
        serde_json::to_value(response).expect("serialize put response"),
        json!({}),
    );
    assert_eq!(
        read_item(&store, table).await,
        json!({"Item": {"pk": {"S": "one"}, "value": {"S": "written"}}}),
    );
    store.finish().await;
}

/// Business and format rules from the API and Cucumber overwrite tests.
#[tokio::test]
async fn given_existing_item_when_put_item_requests_all_old_then_old_image_is_exact() {
    let store = TestStore::new().await;
    let table = store.create_standard_table().await;
    store
        .operations
        .put_item(put_request(table.clone(), "before"))
        .await
        .expect("put old item");

    let mut request = put_request(table.clone(), "after");
    request.return_values = Some(AllOld::AllOld);
    let response = store
        .operations
        .put_item(request)
        .await
        .expect("replace item");

    assert_eq!(
        serde_json::to_value(response)
            .expect("serialize put response")
            .to_string(),
        r#"{"Attributes":{"pk":{"S":"one"},"value":{"S":"before"}}}"#,
    );
    assert_eq!(
        read_item(&store, table).await,
        json!({"Item": {"pk": {"S": "one"}, "value": {"S": "after"}}}),
    );
    store.finish().await;
}

/// Negative rule from legacy KV and Dynamo conformance: failed condition
/// preserves old data.
#[tokio::test]
async fn given_existing_item_when_conditional_put_fails_then_error_is_exact_and_item_is_unchanged()
{
    let store = TestStore::new().await;
    let table = store.create_standard_table().await;
    store
        .operations
        .put_item(put_request(table.clone(), "existing"))
        .await
        .expect("put old item");
    let mut request = put_request(table.clone(), "replacement");
    request.condition_expression = Some("attribute_not_exists(pk)".to_owned());

    let observed = observe(store.operations.put_item(request)).await;
    let error = observed.result.as_ref().expect_err("condition must fail");

    assert_eq!(error.to_string(), "The conditional request failed");
    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
    observed.assert_no_warning_or_error();
    assert_eq!(
        read_item(&store, table).await,
        json!({"Item": {"pk": {"S": "one"}, "value": {"S": "existing"}}}),
    );
    store.finish().await;
}

/// Boundary and format rules from dynamo-conformance: 38 significant digits are
/// valid.
#[tokio::test]
async fn given_38_digit_number_key_when_put_item_runs_then_response_and_readback_are_exact() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::N)
        .await;
    let number = "99999999999999999999999999999999999999";
    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"N": number},
            "sk": {"N": "1"},
            "label": {"S": "number-pk-max-precision"}
        }
    }))
    .expect("parse Dynamo PutItem request");

    let response = store
        .operations
        .put_item(request)
        .await
        .expect("put valid key");
    let read = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [
                ("pk".to_owned(), AttributeValue::N(number.to_owned())),
                ("sk".to_owned(), AttributeValue::N("1".to_owned())),
            ],
        ))
        .await
        .expect("read valid number key");

    assert_eq!(
        serde_json::to_value(response).expect("serialize response"),
        json!({})
    );
    assert_eq!(
        serde_json::to_value(read).expect("serialize read response"),
        json!({"Item": {
            "pk": {"N": number},
            "sk": {"N": "1"},
            "label": {"S": "number-pk-max-precision"}
        }}),
    );
    store.finish().await;
}

/// Negative and boundary rules from dynamo-conformance: 39 significant digits
/// are rejected.
#[tokio::test]
async fn given_39_digit_number_key_when_put_item_runs_then_validation_message_is_exact() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::N)
        .await;
    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"N": "999999999999999999999999999999999999999"},
            "sk": {"N": "1"},
            "label": {"S": "number-pk-too-precise"}
        }
    }))
    .expect("parse Dynamo PutItem request");

    let observed = observe(store.operations.put_item(request)).await;
    let error = observed
        .result
        .as_ref()
        .expect_err("39 digit number key must fail");

    assert_eq!(
        error.to_string(),
        "Attempting to store more than 38 significant digits in a Number"
    );
    assert!(matches!(error.to_enum(), StorageEnum::Validation { .. }));
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed.result.expect_err("39 digit number key must fail"),
        axum::http::StatusCode::BAD_REQUEST,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Attempting to store more than 38 significant digits in a Number"}"#,
    );
    store.finish().await;
}

/// Negative and format rules from Dynamo conformance and the API ALL_OLD
/// failure case.
#[tokio::test]
async fn given_existing_item_when_conditional_put_requests_all_old_then_error_contains_exact_old_image()
 {
    let store = TestStore::new().await;
    let table = store.create_standard_table().await;
    store
        .operations
        .put_item(put_request(table.clone(), "existing"))
        .await
        .expect("seed existing item");
    let mut request = put_request(table.clone(), "replacement");
    request.condition_expression = Some("attribute_not_exists(pk)".to_owned());
    request.return_values_on_condition_check_failure = Some("ALL_OLD".to_owned());

    let observed = observe(store.operations.put_item(request)).await;
    let error = observed.result.as_ref().expect_err("condition must fail");

    assert_eq!(error.to_string(), "The conditional request failed");
    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailedWithItem { .. }
    ));
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed.result.expect_err("condition must fail"),
        axum::http::StatusCode::BAD_REQUEST,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed","Item":{"pk":{"S":"one"},"value":{"S":"existing"}}}"#,
    );
    assert_eq!(
        read_item(&store, table).await,
        json!({"Item": {"pk": {"S": "one"}, "value": {"S": "existing"}}}),
    );
    store.finish().await;
}

/// Business rule from Dynamo conformance: a condition reads the old image
/// before replacement.
#[tokio::test]
async fn given_existing_item_when_condition_matches_old_value_then_put_replaces_entire_item() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::S)
        .await;
    let seed: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "s": {"S": "abc"},
            "obsolete": {"S": "must disappear"}
        }
    }))
    .expect("parse seed PutItem request");
    store.operations.put_item(seed).await.expect("seed item");
    let conditional: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "replacement": {"S": "new"}
        },
        "ConditionExpression": "s = :old",
        "ExpressionAttributeValues": {":old": {"S": "abc"}}
    }))
    .expect("parse conditional PutItem request");

    let response = store
        .operations
        .put_item(conditional)
        .await
        .expect("condition reads the existing item");
    let read = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [
                ("pk".to_owned(), AttributeValue::S("one".to_owned())),
                ("sk".to_owned(), AttributeValue::S("two".to_owned())),
            ],
        ))
        .await
        .expect("read replacement");

    assert_eq!(
        serde_json::to_value(response).expect("serialize response"),
        json!({})
    );
    assert_eq!(
        serde_json::to_value(read).expect("serialize read response"),
        json!({"Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "replacement": {"S": "new"}
        }}),
    );
    store.finish().await;
}

/// Negative and format rules from Dynamo conformance: the numeric parse error
/// keeps its trailing space.
#[tokio::test]
async fn given_empty_number_partition_key_when_put_item_runs_then_full_validation_response_is_exact()
 {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::N)
        .await;
    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"N": ""},
            "sk": {"N": "1"},
            "label": {"S": "number-pk-empty"}
        }
    }))
    .expect("parse Dynamo PutItem request");

    let observed = observe(store.operations.put_item(request)).await;
    let error = observed
        .result
        .as_ref()
        .expect_err("empty numeric key must fail");

    assert_eq!(
        error.to_string(),
        "The parameter cannot be converted to a numeric value"
    );
    assert!(matches!(error.to_enum(), StorageEnum::Validation { .. }));
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed.result.expect_err("empty numeric key must fail"),
        axum::http::StatusCode::BAD_REQUEST,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: The parameter cannot be converted to a numeric value: "}"#,
    );
    store.finish().await;
}

/// Boundary rule from Dynamo conformance: a binary partition key may contain
/// 2048 decoded bytes.
#[tokio::test]
async fn given_2048_byte_binary_partition_key_when_put_item_runs_then_item_is_readable() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::B)
        .await;
    let key = base64::engine::general_purpose::STANDARD.encode(vec![7u8; 2048]);
    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"B": key},
            "sk": {"B": "Bw=="},
            "label": {"S": "binary-pk-max"}
        }
    }))
    .expect("parse binary-key PutItem request");

    let response = store
        .operations
        .put_item(request)
        .await
        .expect("put maximum binary key");
    let read = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [
                ("pk".to_owned(), AttributeValue::B(key.clone())),
                ("sk".to_owned(), AttributeValue::B("Bw==".to_owned())),
            ],
        ))
        .await
        .expect("read maximum binary key");

    assert_eq!(
        serde_json::to_value(response).expect("serialize response"),
        json!({})
    );
    assert_eq!(
        serde_json::to_value(read).expect("serialize read response"),
        json!({"Item": {
            "pk": {"B": key},
            "sk": {"B": "Bw=="},
            "label": {"S": "binary-pk-max"}
        }}),
    );
    store.finish().await;
}

/// Negative boundary rule from Dynamo conformance: one extra decoded byte is
/// rejected.
#[tokio::test]
async fn given_2049_byte_binary_partition_key_when_put_item_runs_then_full_error_is_exact() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::B)
        .await;
    let key = base64::engine::general_purpose::STANDARD.encode(vec![7u8; 2049]);
    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"B": key},
            "sk": {"B": "Bw=="},
            "label": {"S": "binary-pk-too-long"}
        }
    }))
    .expect("parse binary-key PutItem request");

    let observed = observe(store.operations.put_item(request)).await;
    let error = observed
        .result
        .as_ref()
        .expect_err("oversize binary key must fail");

    assert_eq!(
        error.to_string(),
        "One or more parameter values were invalid: Size of hashkey has exceeded the maximum size \
         limit of2048 bytes"
    );
    assert!(matches!(error.to_enum(), StorageEnum::Validation { .. }));
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed.result.expect_err("oversize binary key must fail"),
        axum::http::StatusCode::BAD_REQUEST,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Size of hashkey has exceeded the maximum size limit of2048 bytes"}"#,
    );
    store.finish().await;
}

/// Negative rule from the API and Cucumber cases: an unknown table wins over
/// missing key data.
#[tokio::test]
async fn given_missing_table_and_missing_key_when_put_item_runs_then_table_error_wins() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {"other": {"S": "x"}}
    }))
    .expect("parse PutItem request with missing key");

    let observed = observe(store.operations.put_item(request)).await;
    let error = observed
        .result
        .as_ref()
        .expect_err("missing table must fail");
    assert!(matches!(
        error.to_enum(),
        StorageEnum::TableNotFound { name, .. } if name == &table.to_string()
    ));
    observed.assert_no_warning_or_error();
    let response =
        http_error::HttpApiError::from(observed.result.expect_err("missing table must fail"));
    assert_eq!(response.status_code, 400);
    assert_eq!(
        response.error_type,
        "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException"
    );
    store.finish().await;
}

/// Negative rule from the API and Cucumber cases: the table key is required.
#[tokio::test]
async fn given_existing_table_and_missing_key_when_put_item_runs_then_validation_is_returned() {
    let store = TestStore::new().await;
    let table = store.create_standard_table().await;
    let request = PutItemRequest::new(
        table,
        storage_types::WireItem::try_from(&AttributeMap::from_iter([(
            "other".to_owned(),
            AttributeValue::S("x".to_owned()),
        )]))
        .unwrap(),
    );

    let observed = observe(store.operations.put_item(request)).await;
    let error = observed.result.as_ref().expect_err("missing key must fail");

    assert!(matches!(error.to_enum(), StorageEnum::Validation { .. }));
    observed.assert_no_warning_or_error();
    let response =
        http_error::HttpApiError::from(observed.result.expect_err("missing key must fail"));
    assert_eq!(response.status_code, 400);
    assert_eq!(
        response.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
    store.finish().await;
}

/// Negative rule from Cucumber: the wire request validator rejects an empty
/// item.
#[tokio::test]
async fn given_empty_item_when_put_request_is_parsed_then_validation_rejects_it() {
    let table = unique_table_name();
    let request = json!({"TableName": table.to_string(), "Item": {}});

    let observed = observe(async { PutItemRequest::try_from(request) }).await;

    assert!(observed.result.is_err(), "empty item must be rejected");
    observed.assert_no_warning_or_error();
}

/// Business and format rule from Dynamo conformance: aliases address reserved
/// nested names.
#[tokio::test]
async fn given_nested_reserved_word_when_conditional_put_uses_aliases_then_item_is_written() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::S)
        .await;
    let seed: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "m": {"M": {"COMMENT": {"S": "nested"}}}
        }
    }))
    .expect("parse seed PutItem request");
    store
        .operations
        .put_item(seed)
        .await
        .expect("seed nested item");
    let conditional: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "m": {"M": {"COMMENT": {"S": "nested"}}}
        },
        "ConditionExpression": "#m.#c = :v",
        "ExpressionAttributeNames": {"#m": "m", "#c": "COMMENT"},
        "ExpressionAttributeValues": {":v": {"S": "nested"}}
    }))
    .expect("parse aliased PutItem request");

    let response = store
        .operations
        .put_item(conditional)
        .await
        .expect("nested reserved word condition matches");
    let read = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [
                ("pk".to_owned(), AttributeValue::S("one".to_owned())),
                ("sk".to_owned(), AttributeValue::S("two".to_owned())),
            ],
        ))
        .await
        .expect("read conditional replacement");

    assert_eq!(
        serde_json::to_value(response).expect("serialize response"),
        json!({})
    );
    assert_eq!(
        serde_json::to_value(read).expect("serialize read response"),
        json!({"Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "m": {"M": {"COMMENT": {"S": "nested"}}}
        }}),
    );
    store.finish().await;
}

/// Negative and format rule from Dynamo conformance at the wire request
/// boundary.
#[tokio::test]
async fn given_reversed_between_bounds_when_put_request_is_parsed_then_conformance_message_is_exact()
 {
    let table = unique_table_name();
    let request = json!({
        "TableName": table.to_string(),
        "Item": {"pk": {"S": "one"}, "sk": {"S": "two"}, "replacement": {"S": "new"}},
        "ConditionExpression": "s BETWEEN :lo AND :hi",
        "ExpressionAttributeValues": {":lo": {"S": "abd"}, ":hi": {"S": "abc"}}
    });

    let observed = observe(async { PutItemRequest::try_from(request) }).await;

    assert_eq!(
        observed
            .result
            .as_ref()
            .expect_err("reversed BETWEEN must fail"),
        "1 validation error detected: Invalid ConditionExpression: The BETWEEN operator requires \
         upper bound to be greater than or equal to lower bound; lower bound operand: \
         AttributeValue: {S:abd}, upper bound operand: AttributeValue: {S:abc}"
    );
    observed.assert_no_warning_or_error();
}

/// Negative rule: direct provider validation must not log customer operand
/// values.
#[tokio::test]
async fn given_reversed_between_bounds_when_direct_put_runs_then_customer_values_are_not_logged() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    let lower = "z-private-lower-57d6a9";
    let upper = "a-private-upper-57d6a9";
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::S)
        .await;
    let seed: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {"pk": {"S": "one"}, "sk": {"S": "two"}, "s": {"S": "abc"}}
    }))
    .expect("parse seed PutItem request");
    store.operations.put_item(seed).await.expect("seed item");
    let conditional: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {"pk": {"S": "one"}, "sk": {"S": "two"}, "replacement": {"S": "new"}},
        "ConditionExpression": "s BETWEEN :lo AND :hi",
        "ExpressionAttributeValues": {":lo": {"S": lower}, ":hi": {"S": upper}}
    }))
    .expect("parse conditional PutItem request");

    let observed = observe(store.operations.put_item(conditional)).await;
    let error = observed.result.expect_err("reversed BETWEEN must fail");

    let response = http_error::HttpApiError::from(error);
    assert_eq!(response.status_code, 400);
    assert_eq!(
        response.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
    let read = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [
                ("pk".to_owned(), AttributeValue::S("one".to_owned())),
                ("sk".to_owned(), AttributeValue::S("two".to_owned())),
            ],
        ))
        .await
        .expect("read unchanged item");
    assert_eq!(
        serde_json::to_value(read).expect("serialize read response"),
        json!({"Item": {"pk": {"S": "one"}, "sk": {"S": "two"}, "s": {"S": "abc"}}}),
    );
    assert!(
        observed
            .events
            .iter()
            .all(|event| event.level != tracing::Level::ERROR),
        "customer validation emitted an ERROR event: {:?}",
        observed.events
    );
    assert!(
        observed.events.iter().all(|event| event
            .fields
            .values()
            .all(|value| !value.contains(lower) && !value.contains(upper))),
        "customer validation logged request values: {:?}",
        observed.events
    );
    store.finish().await;
}

/// Negative and format rule from Dynamo conformance: unused aliases fail before
/// a false condition.
#[tokio::test]
async fn given_unused_expression_name_and_false_condition_when_put_is_parsed_then_validation_wins()
{
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::S)
        .await;
    let seed: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "status": {"S": "open"},
            "s": {"S": "abc"}
        }
    }))
    .expect("parse seed PutItem request");
    store.operations.put_item(seed).await.expect("seed item");
    let request = json!({
        "TableName": table.to_string(),
        "Item": {"pk": {"S": "one"}, "sk": {"S": "two"}, "replacement": {"S": "new"}},
        "ConditionExpression": "#status = :closed",
        "ExpressionAttributeNames": {"#status": "status", "#unused": "s"},
        "ExpressionAttributeValues": {":closed": {"S": "closed"}}
    });

    let observed = observe(async { PutItemRequest::try_from(request) }).await;

    assert_eq!(
        observed
            .result
            .as_ref()
            .expect_err("unused alias must fail"),
        "1 validation error detected: Value provided in ExpressionAttributeNames unused in \
         expressions: keys: {#unused}"
    );
    observed.assert_no_warning_or_error();
    let read = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [
                ("pk".to_owned(), AttributeValue::S("one".to_owned())),
                ("sk".to_owned(), AttributeValue::S("two".to_owned())),
            ],
        ))
        .await
        .expect("read unchanged item");
    assert_eq!(
        serde_json::to_value(read).expect("serialize read response"),
        json!({"Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "status": {"S": "open"},
            "s": {"S": "abc"}
        }}),
    );
    store.finish().await;
}

/// Negative business rule from Dynamo conformance: number 7 differs from string
/// "7".
#[tokio::test]
async fn given_number_attribute_when_condition_compares_string_then_put_fails_without_replacement()
{
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::S)
        .await;
    let seed: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {"pk": {"S": "one"}, "sk": {"S": "two"}, "n": {"N": "7"}}
    }))
    .expect("parse seed PutItem request");
    store.operations.put_item(seed).await.expect("seed item");
    let conditional: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {"pk": {"S": "one"}, "sk": {"S": "two"}, "replacement": {"S": "new"}},
        "ConditionExpression": "n = :v",
        "ExpressionAttributeValues": {":v": {"S": "7"}}
    }))
    .expect("parse conditional PutItem request");

    let observed = observe(store.operations.put_item(conditional)).await;
    let error = observed
        .result
        .as_ref()
        .expect_err("type mismatch makes condition false");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed
            .result
            .expect_err("type mismatch makes condition false"),
        axum::http::StatusCode::BAD_REQUEST,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
    );
    let read = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [
                ("pk".to_owned(), AttributeValue::S("one".to_owned())),
                ("sk".to_owned(), AttributeValue::S("two".to_owned())),
            ],
        ))
        .await
        .expect("read unchanged item");
    assert_eq!(
        serde_json::to_value(read).expect("serialize read response"),
        json!({"Item": {"pk": {"S": "one"}, "sk": {"S": "two"}, "n": {"N": "7"}}}),
    );
    store.finish().await;
}

/// Business rule from Dynamo conformance: IN compares complete map values.
#[tokio::test]
async fn given_existing_map_when_in_checks_partial_then_whole_map_then_only_whole_map_replaces_item()
 {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::S)
        .await;
    let seed: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "m": {"M": {"child": {"S": "value"}, "num": {"N": "5"}}}
        }
    }))
    .expect("parse seed PutItem request");
    store
        .operations
        .put_item(seed)
        .await
        .expect("seed map item");
    let partial_match: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "replacement": {"S": "wrong"}
        },
        "ConditionExpression": "m IN (:a, :b)",
        "ExpressionAttributeValues": {
            ":a": {"M": {"other": {"S": "x"}}},
            ":b": {"M": {"child": {"S": "value"}}}
        }
    }))
    .expect("parse partial-map conditional PutItem request");
    let failed = observe(store.operations.put_item(partial_match)).await;
    assert!(matches!(
        failed
            .result
            .as_ref()
            .expect_err("partial map must not match")
            .to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
    failed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        failed.result.expect_err("partial map must not match"),
        axum::http::StatusCode::BAD_REQUEST,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
    );
    let before = store
        .operations
        .get_item(GetItemRequest::new(
            table.clone(),
            [
                ("pk".to_owned(), AttributeValue::S("one".to_owned())),
                ("sk".to_owned(), AttributeValue::S("two".to_owned())),
            ],
        ))
        .await
        .expect("read unchanged map item");
    assert_eq!(
        serde_json::to_value(before).expect("serialize read response"),
        json!({"Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "m": {"M": {"child": {"S": "value"}, "num": {"N": "5"}}}
        }}),
    );
    let conditional: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "replacement": {"S": "new"}
        },
        "ConditionExpression": "m IN (:a, :b)",
        "ExpressionAttributeValues": {
            ":a": {"M": {"other": {"S": "x"}}},
            ":b": {"M": {"child": {"S": "value"}, "num": {"N": "5"}}}
        }
    }))
    .expect("parse conditional PutItem request");

    let response = store
        .operations
        .put_item(conditional)
        .await
        .expect("whole map equals the second IN operand");
    let read = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [
                ("pk".to_owned(), AttributeValue::S("one".to_owned())),
                ("sk".to_owned(), AttributeValue::S("two".to_owned())),
            ],
        ))
        .await
        .expect("read replacement");

    assert_eq!(
        serde_json::to_value(response).expect("serialize response"),
        json!({})
    );
    assert_eq!(
        serde_json::to_value(read).expect("serialize read response"),
        json!({"Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "replacement": {"S": "new"}
        }}),
    );
    store.finish().await;
}

/// Negative business rule from Dynamo conformance: contains requires a whole
/// list member match.
#[tokio::test]
async fn given_list_of_maps_when_contains_checks_partial_map_then_put_fails_without_replacement() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::S)
        .await;
    let seed: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "l": {"L": [{"M": {"name": {"S": "one"}}}]}
        }
    }))
    .expect("parse seed PutItem request");
    store
        .operations
        .put_item(seed)
        .await
        .expect("seed list item");
    let conditional: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "replacement": {"S": "new"}
        },
        "ConditionExpression": "contains(l, :v)",
        "ExpressionAttributeValues": {":v": {"M": {"name": {"S": "on"}}}}
    }))
    .expect("parse conditional PutItem request");

    let observed = observe(store.operations.put_item(conditional)).await;
    let error = observed
        .result
        .as_ref()
        .expect_err("partial map must not match");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed.result.expect_err("partial map must not match"),
        axum::http::StatusCode::BAD_REQUEST,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
    );
    let read = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [
                ("pk".to_owned(), AttributeValue::S("one".to_owned())),
                ("sk".to_owned(), AttributeValue::S("two".to_owned())),
            ],
        ))
        .await
        .expect("read unchanged item");
    assert_eq!(
        serde_json::to_value(read).expect("serialize read response"),
        json!({"Item": {
            "pk": {"S": "one"},
            "sk": {"S": "two"},
            "l": {"L": [{"M": {"name": {"S": "one"}}}]}
        }}),
    );
    store.finish().await;
}

/// Boundary rule from Dynamo conformance: a binary sort key may contain 1024
/// decoded bytes.
#[tokio::test]
async fn given_1024_byte_binary_sort_key_when_put_item_runs_then_item_is_readable() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::B)
        .await;
    let sort_key = base64::engine::general_purpose::STANDARD.encode(vec![7u8; 1024]);
    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"B": "Bw=="},
            "sk": {"B": sort_key},
            "label": {"S": "binary-sk-max"}
        }
    }))
    .expect("parse binary-key PutItem request");

    let response = store
        .operations
        .put_item(request)
        .await
        .expect("put maximum sort key");
    let read = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            [
                ("pk".to_owned(), AttributeValue::B("Bw==".to_owned())),
                ("sk".to_owned(), AttributeValue::B(sort_key.clone())),
            ],
        ))
        .await
        .expect("read maximum sort key");

    assert_eq!(
        serde_json::to_value(response).expect("serialize response"),
        json!({})
    );
    assert_eq!(
        serde_json::to_value(read).expect("serialize read response"),
        json!({"Item": {
            "pk": {"B": "Bw=="},
            "sk": {"B": sort_key},
            "label": {"S": "binary-sk-max"}
        }}),
    );
    store.finish().await;
}

/// Negative boundary rule from Dynamo conformance: one extra decoded sort-key
/// byte is rejected.
#[tokio::test]
async fn given_1025_byte_binary_sort_key_when_put_item_runs_then_full_error_is_exact() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::B)
        .await;
    let sort_key = base64::engine::general_purpose::STANDARD.encode(vec![7u8; 1025]);
    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"B": "Bw=="},
            "sk": {"B": sort_key},
            "label": {"S": "binary-sk-too-long"}
        }
    }))
    .expect("parse binary-key PutItem request");

    let observed = observe(store.operations.put_item(request)).await;
    let error = observed
        .result
        .as_ref()
        .expect_err("oversize sort key must fail");

    assert!(matches!(error.to_enum(), StorageEnum::Validation { .. }));
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed.result.expect_err("oversize sort key must fail"),
        axum::http::StatusCode::BAD_REQUEST,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Aggregated size of all range keys has exceeded the size limit of 1024 bytes"}"#,
    );
    store.finish().await;
}

/// Negative boundary rule from Dynamo conformance: sort keys cannot contain
/// empty binary data.
#[tokio::test]
async fn given_empty_binary_sort_key_when_put_item_runs_then_full_error_is_exact() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .create_hash_range_table(table.clone(), KeyAttributeType::B)
        .await;
    let request: PutItemRequest = serde_json::from_value(json!({
        "TableName": table.to_string(),
        "Item": {
            "pk": {"B": "Bw=="},
            "sk": {"B": ""},
            "label": {"S": "binary-sk-empty"}
        }
    }))
    .expect("parse binary-key PutItem request");

    let observed = observe(store.operations.put_item(request)).await;
    let error = observed
        .result
        .as_ref()
        .expect_err("empty sort key must fail");

    assert!(matches!(error.to_enum(), StorageEnum::Validation { .. }));
    observed.assert_no_warning_or_error();
    assert_dynamo_error_response_exact(
        observed.result.expect_err("empty sort key must fail"),
        axum::http::StatusCode::BAD_REQUEST,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: sk"}"#,
    );
    store.finish().await;
}

#[tokio::test]
async fn given_concurrent_puts_when_keys_overlap_or_differ_then_every_change_is_retained() {
    for overlapping in [false, true] {
        let store = TestStore::new().await;
        let table = unique_table_name();
        let mut create = crate::test_support::hash_table_request(table.clone());
        create.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        store.operations.create_table(create).await.unwrap();
        let requests = (0..32).map(|index| {
            let key = if overlapping {
                "one".to_owned()
            } else {
                index.to_string()
            };
            let request = PutItemRequest::new(
                table.clone(),
                storage_types::WireItem::try_from(&AttributeMap::from_iter([
                    ("pk".to_owned(), AttributeValue::S(key)),
                    ("value".to_owned(), AttributeValue::S(index.to_string())),
                ]))
                .unwrap(),
            );
            store.operations.put_item(request)
        });
        for result in futures::future::join_all(requests).await {
            result.expect("ordinary concurrent put must commit");
        }
        let records = crate::image_retention_tests::table_records(&store, &table)
            .await
            .unwrap();
        assert_eq!(
            records.len(),
            32,
            "one retained stream event per committed put"
        );
        let values = records
            .iter()
            .map(|record| {
                record
                    .new_image
                    .as_ref()
                    .unwrap()
                    .get("value")
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>();
        for index in 0..32 {
            assert_eq!(
                values
                    .iter()
                    .filter(|value| **value == AttributeValue::S(index.to_string()))
                    .count(),
                1
            );
        }
        store.finish().await;
    }
}
