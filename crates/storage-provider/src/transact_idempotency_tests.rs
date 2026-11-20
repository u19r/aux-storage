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
fn given_job_requests_when_replay_identity_is_encoded_then_changed_input_or_execution_is_distinct()
{
    use super::transact_request_identity::TransactRequestIdentity;

    fn identity(execution: [u8; 32], request: &TransactWriteItemsRequest) -> Vec<u8> {
        let mut identity = TransactRequestIdentity::try_from_request(request).unwrap();
        identity.bind_execution(execution);
        identity.stored_bytes().as_slice().to_vec()
    }

    let first = token_request();
    let mut changed = token_request();
    changed.client_request_token = None;
    changed.return_item_collection_metrics = Some("SIZE".to_string());
    let encoded = identity([7; 32], &first);
    assert_eq!(encoded, identity([7; 32], &token_request()));
    assert_ne!(encoded, identity([7; 32], &changed));
    assert_ne!(encoded, identity([8; 32], &first));
    assert!(
        TransactRequestIdentity::from_stored(&encoded).unwrap() == {
            let mut expected = TransactRequestIdentity::try_from_request(&first).unwrap();
            expected.bind_execution([7; 32]);
            expected
        }
    );
}

#[test]
fn given_json_identity_of_a_prior_format_when_read_then_it_is_rejected() {
    use super::transact_request_identity::TransactRequestIdentity;

    assert!(TransactRequestIdentity::from_stored(br#"{"put":{"item":{"a":1}}}"#).is_err());
    assert!(TransactRequestIdentity::from_stored(br#"[[7],{"put":{}}]"#).is_err());
    assert!(
        TransactRequestIdentity::from_stored(br#"{"execution_digest":[],"request_digest":[]}"#)
            .is_err()
    );
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

#[test]
fn given_request_window_receipt_when_trim_is_checked_then_it_survives_until_window_plus_trim_delay()
{
    use super::transact_idempotency::{
        TRANSACT_RECEIPT_TRIM_DELAY_MILLIS, TransactTokenRequest, encode_transact_receipt,
        transact_receipt_is_trimmable,
    };

    const MINUTE: i64 = 60_000;
    assert_eq!(TRANSACT_RECEIPT_TRIM_DELAY_MILLIS, 5 * MINUTE);
    let completed = 1_000_000;
    let request = TransactTokenRequest::try_from_request(&token_request())
        .unwrap()
        .unwrap();
    let receipt =
        encode_transact_receipt(&request, TimestampMillis::from_timestamp(completed)).unwrap();
    let at = |offset: i64| {
        TimestampMillis::from_timestamp(
            completed + TRANSACT_WRITE_IDEMPOTENCY_WINDOW_MILLIS + offset,
        )
    };

    for offset in [1_000, 4 * MINUTE + 59_000, 5 * MINUTE - 1] {
        assert!(
            !transact_receipt_is_trimmable(&receipt, at(offset)).unwrap(),
            "{offset}"
        );
    }
    assert!(transact_receipt_is_trimmable(&receipt, at(5 * MINUTE)).unwrap());

    // A retry captured inside the window still replays the surviving receipt.
    let replay = super::transact_idempotency::accept_transact_response(
        Some(&request),
        Some(receipt),
        at(-1_000),
    )
    .unwrap();
    assert!(matches!(replay, crate::SemanticDecision::Accepted(Some(_))));
}

#[test]
fn given_request_receipt_when_encoding_then_it_has_no_retention_field_and_fits_the_bound() {
    use super::transact_idempotency::{
        TRANSACT_RECEIPT_WRITE_BYTES, TransactTokenRequest, encode_transact_receipt,
    };

    let request = TransactTokenRequest::try_from_request(&token_request())
        .unwrap()
        .unwrap();
    assert!(matches!(
        request.slot(),
        crate::ReceiptSlot::Request("token")
    ));
    let bytes =
        encode_transact_receipt(&request, TimestampMillis::from_timestamp(1_000_000)).unwrap();
    assert!(
        !bytes
            .windows(b"retention".len())
            .any(|window| window == b"retention"),
        "receipt must not carry a retention field"
    );
    assert!(bytes.len() <= TRANSACT_RECEIPT_WRITE_BYTES);
}
