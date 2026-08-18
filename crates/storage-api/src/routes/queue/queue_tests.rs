use axum::{
    body::Bytes,
    http::{HeaderMap, HeaderValue, header},
};
use http_error::HttpApiError;
use queue_provider::QueueRequest;

use crate::routes::queue::{QueueWireRequest, decode_request, is_provider_pressure_error};

fn decode_query(body: &'static [u8]) -> QueueWireRequest {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );
    decode_request(&headers, Bytes::from_static(body)).expect("query request decodes")
}

#[test]
fn given_numbered_fields_when_decoding_query_then_maps_and_lists_are_ordered() {
    let request = decode_query(
        b"Action=ReceiveMessage&QueueUrl=https%3A%2F%2Fqueue.example%2Fjobs&AttributeName.2=SentTimestamp&AttributeName.1=All&MessageAttributeName.1=TraceId",
    );

    let QueueRequest::ReceiveMessage(request) = request.request else {
        panic!("expected receive request");
    };
    assert_eq!(
        request.attribute_names,
        Some(vec!["All".to_string(), "SentTimestamp".to_string()])
    );
    assert_eq!(
        request.message_attribute_names,
        Some(vec!["TraceId".to_string()])
    );
}

#[test]
fn given_message_attributes_when_decoding_query_then_values_are_preserved() {
    let request = decode_query(
        b"Action=SendMessage&QueueUrl=https%3A%2F%2Fqueue.example%2Fjobs&MessageBody=body&MessageAttribute.1.Name=Trace&MessageAttribute.1.Value.DataType=String&MessageAttribute.1.Value.StringValue=abc",
    );

    let QueueRequest::SendMessage(request) = request.request else {
        panic!("expected send request");
    };
    let attributes = request
        .message_attributes
        .as_ref()
        .expect("message attributes");
    let trace = attributes.get("Trace").expect("trace attribute");
    assert_eq!(trace.data_type, "String");
    assert_eq!(trace.string_value.as_deref(), Some("abc"));
}

#[test]
fn given_indexed_batch_entries_when_decoding_query_then_entries_are_ordered() {
    let request = decode_query(
        b"Action=SendMessageBatch&QueueUrl=https%3A%2F%2Fqueue.example%2Fjobs&SendMessageBatchRequestEntry.2.Id=second&SendMessageBatchRequestEntry.2.MessageBody=two&SendMessageBatchRequestEntry.1.Id=first&SendMessageBatchRequestEntry.1.MessageBody=one&SendMessageBatchRequestEntry.1.DelaySeconds=5",
    );

    let QueueRequest::SendMessageBatch(request) = request.request else {
        panic!("expected send batch request");
    };
    assert_eq!(request.entries[0].id, "first");
    assert_eq!(request.entries[0].delay_seconds, Some(5));
    assert_eq!(request.entries[1].id, "second");
}

#[test]
fn given_provider_pressure_errors_when_classifying_queue_responses_then_retries_are_signalled() {
    for (error_type, status_code) in [
        ("RequestTimeout", 504),
        ("RequestTimeoutException", 500),
        ("com.amazonaws.dynamodb.v20120810#RequestTimeout", 504),
        ("ThrottlingException", 429),
        ("com.amazonaws.dynamodb.v20120810#ThrottlingException", 429),
        ("ServiceUnavailableException", 503),
        (
            "com.amazonaws.dynamodb.v20120810#ServiceUnavailableException",
            503,
        ),
    ] {
        let error = HttpApiError::aws_query_error(error_type, "provider pressure", status_code);
        assert!(
            is_provider_pressure_error(&error),
            "{error_type} should be classified as provider pressure"
        );
    }
}

#[test]
fn given_client_errors_when_classifying_queue_responses_then_retries_are_not_signalled() {
    for (error_type, status_code) in [
        ("ValidationException", 400),
        ("ResourceNotFoundException", 404),
        ("ConditionalCheckFailedException", 400),
    ] {
        let error = HttpApiError::aws_query_error(error_type, "client error", status_code);
        assert!(
            !is_provider_pressure_error(&error),
            "{error_type} should not be classified as provider pressure"
        );
    }
}
