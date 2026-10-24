use std::collections::HashMap;

use storage_types::{
    AttributeValue, TableName, TimestampMillis, TransactPutRequest, TransactWriteItem,
    TransactWriteItemsRequest, TransactWriteItemsResponse,
};

use super::transact_idempotency::{
    TRANSACT_WRITE_IDEMPOTENCY_WINDOW_MILLIS, decode_token_response, transact_write_token_expired,
};

fn token_request() -> TransactWriteItemsRequest {
    TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            put: Some(TransactPutRequest {
                table_name: TableName::new("orders"),
                item: storage_types::WireItem::try_from(&HashMap::from([(
                    "pk".to_string(),
                    AttributeValue::S("a".to_string()),
                )]))
                .unwrap(),
                indexers: None,
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            update: None,
            delete: None,
            condition_check: None,
        }],
        client_request_token: Some("token".to_string()),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

#[test]
fn given_equivalent_or_changed_job_requests_when_replay_identity_is_encoded_then_object_order_is_ignored_and_changed_input_is_distinct()
 {
    use sha2::{Digest as _, Sha256};

    use super::transact_request_identity::TransactRequestIdentity;

    fn identity(execution: &[u8], request: &[u8]) -> Vec<u8> {
        let mut identity = TransactRequestIdentity::from_stored(request).unwrap();
        identity.bind_execution(Sha256::digest(execution).into());
        identity.stored_bytes().as_slice().to_vec()
    }

    let first = br#"{"put":{"item":{"b":2,"a":1}},"checks":[{"y":2,"x":1}]}"#;
    let reordered = br#"{"checks":[{"x":1,"y":2}],"put":{"item":{"a":1,"b":2}}}"#;
    let changed = br#"{"checks":[{"x":1,"y":2}],"put":{"item":{"a":1,"b":3}}}"#;
    let encoded = identity(&[7; 16], first);
    assert_eq!(encoded, identity(&[7; 16], reordered));
    assert_ne!(encoded, identity(&[7; 16], changed));
    assert_ne!(encoded, identity(&[8; 16], first));
    let execution_digest: [u8; 32] = Sha256::digest([7; 16]).into();
    let mut value: serde_json::Value = serde_json::from_slice(first).unwrap();
    value.sort_all_objects();
    let request_digest: [u8; 32] = Sha256::digest(serde_json::to_vec(&value).unwrap()).into();
    let prior = serde_json::to_vec(&serde_json::json!({
        "execution_digest": execution_digest,
        "request_digest": request_digest,
    }))
    .unwrap();
    assert_eq!(
        encoded,
        TransactRequestIdentity::from_stored(&prior)
            .unwrap()
            .stored_bytes()
            .as_slice()
    );
    assert!(TransactRequestIdentity::from_stored(br#"{"execution_digest":[]}"#).is_err());
}

#[test]
fn given_tokens_when_canonicalized_then_token_is_stripped_deterministically() {
    let first = token_request();
    let mut second = token_request();
    second.client_request_token = Some("other".to_string());

    let first_identity =
        super::transact_request_identity::TransactRequestIdentity::try_from_request(&first)
            .unwrap();
    let first_bytes = first_identity.stored_bytes();
    let second_bytes =
        super::transact_request_identity::TransactRequestIdentity::try_from_request(&second)
            .unwrap()
            .stored_bytes();

    assert_eq!(first_bytes.as_slice(), second_bytes.as_slice());
    let restored = super::transact_request_identity::TransactRequestIdentity::from_stored(
        first_bytes.as_slice(),
    )
    .unwrap();
    assert!(first_identity == restored);
}

#[test]
fn given_response_bytes_when_decoded_then_canonical_form_round_trips() {
    let response = TransactWriteItemsResponse {
        consumed_capacity: None,
        item_collection_metrics: None,
    };
    let bytes = serde_json::to_vec(&response).unwrap();

    decode_token_response(&bytes).unwrap();
    assert!(decode_token_response(b"not json").is_err());
}

#[test]
fn given_completion_time_when_expiry_is_checked_then_window_boundary_applies() {
    let completed_at = TimestampMillis::from_timestamp(1_000_000);
    let within =
        TimestampMillis::from_timestamp(1_000_000 + TRANSACT_WRITE_IDEMPOTENCY_WINDOW_MILLIS - 1);
    let boundary =
        TimestampMillis::from_timestamp(1_000_000 + TRANSACT_WRITE_IDEMPOTENCY_WINDOW_MILLIS);

    assert!(!transact_write_token_expired(completed_at, within));
    assert!(transact_write_token_expired(completed_at, boundary));
}
