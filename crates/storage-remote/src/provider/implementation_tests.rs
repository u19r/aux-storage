use std::time::Duration;

use http::{HeaderMap, HeaderValue};
use storage_types::{StorageEnum, StorageError, context::WrappedError as _};

use crate::{
    constants::MAX_RETRY_AFTER_SECS,
    provider::implementation::{parse_retry_after, preserve_retry_after, remote_request_context},
};

#[test]
fn given_query_body_when_remote_request_context_read_then_table_and_index_are_returned() {
    let body = br#"{"TableName":"tenant_data","IndexName":"gsi1"}"#;

    let context = remote_request_context(body);

    assert_eq!(context.table_name.as_deref(), Some("tenant_data"));
    assert_eq!(context.index_name.as_deref(), Some("gsi1"));
}

#[test]
fn given_invalid_body_when_remote_request_context_read_then_context_is_empty() {
    let context = remote_request_context(b"not-json");

    assert_eq!(context, Default::default());
}

#[test]
fn given_put_item_body_when_remote_request_context_read_then_key_is_returned() {
    let body = br#"{
        "TableName": "tenant_data",
        "Item": {
            "pk": {"S": "ZC"},
            "sk": {"S": "CURV"}
        }
    }"#;

    let context = remote_request_context(body);

    assert_eq!(context.table_name.as_deref(), Some("tenant_data"));
    assert_eq!(context.item_pk.as_deref(), Some("ZC"));
    assert_eq!(context.item_sk.as_deref(), Some("CURV"));
}

#[test]
fn given_service_unavailable_when_retry_after_is_parsed_then_hint_reaches_returned_error() {
    let error = preserve_retry_after(
        StorageError::service_unavailable(1),
        Some(Duration::from_secs(7)),
    );

    assert!(matches!(
        error.to_enum(),
        StorageEnum::ServiceUnavailable {
            retry_after_seconds: 7,
            ..
        }
    ));
}

#[test]
fn given_non_service_error_when_retry_after_is_parsed_then_error_class_is_preserved() {
    let error = preserve_retry_after(
        StorageError::Base(StorageEnum::Throttled {
            message: "slow down".to_string(),
        }),
        Some(Duration::from_secs(7)),
    );

    assert!(matches!(error.to_enum(), StorageEnum::Throttled { .. }));
}

#[test]
fn given_delta_seconds_retry_after_when_parsed_then_duration_is_preserved() {
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", HeaderValue::from_static("7"));

    assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(7)));
}

#[test]
fn given_absurd_retry_after_when_parsed_then_duration_is_clamped() {
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", HeaderValue::from_static("999999999"));

    assert_eq!(
        parse_retry_after(&headers),
        Some(Duration::from_secs(MAX_RETRY_AFTER_SECS))
    );
}
