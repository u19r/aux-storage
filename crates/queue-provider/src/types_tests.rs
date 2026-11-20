use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;
use storage_types::TimestampMillis;

use crate::{
    MessageId, ReceiptHandle,
    request_validation::QueueRequestValidation,
    types::{
        ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityRequest, CreateQueueRequest,
        DeleteMessageBatchRequest, DeleteMessageRequest, DeleteQueueRequest,
        GetQueueAttributesRequest, GetQueueUrlRequest, ListQueuesRequest, MessageAttributeValue,
        MessageResponse, PurgeQueueRequest, QueueMessage, ReceiveMessageRequest,
        SendMessageBatchRequest, SendMessageRequest, SetQueueAttributesRequest,
        VisibleQueueMessage, md5_of_message_attributes,
    },
};

/// The typed decode and the one validation of the wire owner.
fn decode<T: serde::de::DeserializeOwned + QueueRequestValidation>(
    value: serde_json::Value,
) -> Result<T, http_error::HttpApiError> {
    let request: T = serde_json::from_value(value)
        .map_err(|error| http_error::HttpApiError::validation_error(error.to_string()))?;
    request
        .validate_request()
        .map_err(http_error::HttpApiError::from)?;
    Ok(request)
}

#[test]
fn create_queue_request_valid() {
    let json = json!({
        "QueueName": "valid-queue_name"
    });

    let result = decode::<CreateQueueRequest>(json);
    assert!(result.is_ok());
    let request = result.unwrap();
    assert_eq!(request.queue_name, "valid-queue_name");
}

#[test]
fn create_queue_request_empty_name() {
    let json = json!({
        "QueueName": ""
    });

    let result = decode::<CreateQueueRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "Queue name cannot be empty");
}

#[test]
fn create_queue_request_name_too_long() {
    let long_name = "a".repeat(81);
    let json = json!({
        "QueueName": long_name
    });

    let result = decode::<CreateQueueRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length"
    );
}

#[test]
fn create_queue_request_name_max_length() {
    let max_name = "a".repeat(80);
    let json = json!({
        "QueueName": max_name
    });

    let result = decode::<CreateQueueRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn create_queue_request_invalid_characters() {
    let json = json!({
        "QueueName": "invalid@queue"
    });

    let result = decode::<CreateQueueRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length"
    );
}

#[test]
fn create_queue_request_valid_characters() {
    let json = json!({
        "QueueName": "Valid-Queue_Name123"
    });

    let result = decode::<CreateQueueRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn create_queue_request_rejects_fifo_name() {
    let json = json!({
        "QueueName": "jobs.fifo"
    });

    let result = decode::<CreateQueueRequest>(json);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().message, "FIFO queues are not supported");
}

#[test]
fn delete_queue_request_valid() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue"
    });

    let result = decode::<DeleteQueueRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn list_queues_request_valid_prefix() {
    let json = json!({
        "QueueNamePrefix": "billing_"
    });

    let result = decode::<ListQueuesRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn list_queues_request_invalid_prefix() {
    let json = json!({
        "QueueNamePrefix": "bad prefix"
    });

    let result = decode::<ListQueuesRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Queue name prefix can only contain alphanumeric characters, hyphens, and underscores"
    );
}

#[test]
fn get_queue_url_request_valid() {
    let json = json!({
        "QueueName": "queue_name-01"
    });

    let result = decode::<GetQueueUrlRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn given_empty_attribute_name_when_decoded_then_it_is_an_unknown_attribute() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue",
        "AttributeNames": [""]
    });

    let error = decode::<GetQueueAttributesRequest>(json).unwrap_err();
    assert_eq!(error.error_type, "InvalidAttributeName");
    assert_eq!(error.message, "Unknown Attribute .");
}

#[test]
fn set_queue_attributes_request_valid() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue",
        "Attributes": {
            "VisibilityTimeout": "30"
        }
    });

    let result = decode::<SetQueueAttributesRequest>(json);
    assert!(result.is_ok());
}

/// W14 C8: `Policy` is stored; an unknown name is `InvalidAttributeName`.
#[test]
fn given_policy_or_unknown_attribute_when_set_then_policy_is_accepted_and_unknown_rejected() {
    let url = "https://queue.example.com/000000000000/test-queue";
    let policy = json!({"QueueUrl": url, "Attributes": {"Policy": "{}"}});
    assert!(decode::<SetQueueAttributesRequest>(policy).is_ok());

    let unknown = json!({"QueueUrl": url, "Attributes": {"Bogus": "1"}});
    let error = decode::<SetQueueAttributesRequest>(unknown).unwrap_err();
    assert_eq!(error.error_type, "InvalidAttributeName");
    assert_eq!(error.message, "Unknown Attribute Bogus.");
}

#[test]
fn set_queue_attributes_request_rejects_out_of_range_retention() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue",
        "Attributes": {
            "MessageRetentionPeriod": "59"
        }
    });

    let result = decode::<SetQueueAttributesRequest>(json);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().message,
        "Invalid value for the parameter MessageRetentionPeriod."
    );
}

#[test]
fn set_queue_attributes_request_rejects_fifo_attribute() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue",
        "Attributes": {
            "FifoQueue": "true"
        }
    });

    let result = decode::<SetQueueAttributesRequest>(json);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().message,
        "FIFO queue attributes are not supported"
    );
}

#[test]
fn purge_queue_request_valid() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue"
    });

    let result = decode::<PurgeQueueRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_request_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello World"
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_request_empty_body() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": ""
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "The request must contain the parameter MessageBody."
    );
}

#[test]
fn send_message_request_body_too_large() {
    let large_body = "a".repeat(1_048_577); // 1 byte over limit
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": large_body
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    // The AWS text, observed live on 2026-11-10.
    assert_eq!(
        error.message,
        "One or more parameters are invalid. Reason: Message must be shorter than 1048576 bytes."
    );
}

#[test]
fn send_message_request_body_max_size() {
    let max_body = "a".repeat(1_048_576); // Exactly at limit
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": max_body
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_request_valid_unicode() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello\t\n\rWorld 🌍"
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_request_delay_seconds_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "DelaySeconds": 300
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_request_delay_seconds_too_high() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "DelaySeconds": 901
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Value 901 for parameter DelaySeconds is invalid. Reason: DelaySeconds must be >= 0 and \
         <= 900."
    );
}

#[test]
fn send_message_batch_request_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "Entries": [
            {
                "Id": "first",
                "MessageBody": "Hello"
            },
            {
                "Id": "second",
                "MessageBody": "World",
                "DelaySeconds": 1
            }
        ]
    });

    let result = decode::<SendMessageBatchRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_batch_request_rejects_duplicate_ids() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "Entries": [
            {
                "Id": "same",
                "MessageBody": "Hello"
            },
            {
                "Id": "same",
                "MessageBody": "World"
            }
        ]
    });

    let result = decode::<SendMessageBatchRequest>(json);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().message, "Id same repeated.");
}

#[test]
fn delete_message_batch_request_rejects_too_many_entries() {
    let entries: Vec<_> = (0..11)
        .map(|index| {
            json!({
                "Id": format!("entry_{index}"),
                "ReceiptHandle": format!("handle-{index}")
            })
        })
        .collect();
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "Entries": entries
    });

    let result = decode::<DeleteMessageBatchRequest>(json);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().message,
        "Maximum number of entries per request are 10. You have sent 11."
    );
}

#[test]
fn change_message_visibility_batch_request_validates_visibility_timeout() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "Entries": [
            {
                "Id": "entry",
                "ReceiptHandle": "handle",
                "VisibilityTimeout": 43201
            }
        ]
    });

    let result = decode::<ChangeMessageVisibilityBatchRequest>(json);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().message,
        "Value 43201 for parameter VisibilityTimeout is invalid. Reason: VisibilityTimeout must \
         be an integer between 0 and 43200."
    );
}

#[test]
fn send_message_request_delay_seconds_negative() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "DelaySeconds": -1
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Value -1 for parameter DelaySeconds is invalid. Reason: DelaySeconds must be >= 0 and <= \
         900."
    );
}

#[test]
fn given_negative_or_non_integer_delay_when_decoded_then_range_or_type_error() {
    let base = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello"
    });

    for value in [json!(-1), json!(901), json!(u64::from(u32::MAX) + 1)] {
        let mut request = base.clone();
        request["DelaySeconds"] = value.clone();
        assert_eq!(
            decode::<SendMessageRequest>(request).unwrap_err().message,
            format!(
                "Value {value} for parameter DelaySeconds is invalid. Reason: DelaySeconds must \
                 be >= 0 and <= 900."
            )
        );
    }

    for value in [json!(1.5), json!("not-a-number")] {
        let mut request = base.clone();
        request["DelaySeconds"] = value;
        assert!(decode::<SendMessageRequest>(request).is_err());
    }

    for request in [
        base.clone(),
        json!({
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
            "MessageBody": "Hello",
            "DelaySeconds": null
        }),
    ] {
        assert!(decode::<SendMessageRequest>(request).is_ok());
    }
}

#[test]
fn send_message_batch_request_negative_delay_fails_its_entry_alone() {
    let request = decode::<SendMessageBatchRequest>(json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "Entries": [{
            "Id": "entry",
            "MessageBody": "Hello",
            "DelaySeconds": -1
        }]
    }))
    .expect("entry faults are per entry");
    let error = request.entries[0].validate(1).unwrap_err();
    assert!(error.to_string().contains("DelaySeconds"), "{error}");
}

#[test]
fn send_message_request_message_attributes_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "MessageAttributes": {
            "Author": {
                "StringValue": "John",
                "DataType": "String"
            }
        }
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn message_attribute_md5_matches_aws_canonical_encoding_for_strings() {
    let mut attributes = HashMap::new();
    attributes.insert(
        "Author".to_string(),
        MessageAttributeValue {
            string_value: Some("Alice".to_string()),
            binary_value: None,
            data_type: "String".to_string(),
        },
    );
    attributes.insert(
        "Priority".to_string(),
        MessageAttributeValue {
            string_value: Some("7".to_string()),
            binary_value: None,
            data_type: "Number.int".to_string(),
        },
    );

    assert_eq!(
        md5_of_message_attributes(&attributes).as_deref(),
        Some("59f4208f3c9087f6253ce0fce97917da")
    );
}

#[test]
fn message_attribute_md5_streams_binary_and_string_values() {
    let mut attributes = HashMap::new();
    attributes.insert(
        "binary".to_string(),
        MessageAttributeValue {
            string_value: None,
            binary_value: Some(STANDARD.encode([0, 1, 2, 255])),
            data_type: "Binary.custom".to_string(),
        },
    );
    attributes.insert(
        "string".to_string(),
        MessageAttributeValue {
            string_value: Some("value".to_string()),
            binary_value: None,
            data_type: "String".to_string(),
        },
    );

    assert_eq!(
        md5_of_message_attributes(&attributes),
        legacy_md5_of_message_attributes(&attributes)
    );
}

#[test]
fn message_attribute_md5_rejects_missing_value_without_hashing_partial_input() {
    let mut attributes = HashMap::new();
    attributes.insert(
        "missing".to_string(),
        MessageAttributeValue {
            string_value: None,
            binary_value: None,
            data_type: "String".to_string(),
        },
    );

    assert_eq!(md5_of_message_attributes(&attributes), None);
}

#[test]
fn message_response_moves_receipt_handle_without_changing_wire_fields() {
    let candidate = sample_invisible_message().into_message_response();
    let baseline = legacy_message_response(sample_queue_message());
    assert_eq!(
        serde_json::to_value(candidate.unwrap()).unwrap(),
        serde_json::to_value(baseline.unwrap()).unwrap()
    );
}

#[test]
fn message_response_receipt_ownership_reduces_allocations() {
    let baseline = measure_message_response_allocations("clone_receipt", || {
        legacy_message_response(sample_queue_message())
    });
    let candidate = measure_message_response_allocations("move_receipt", || {
        sample_invisible_message().into_message_response()
    });

    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(candidate.allocation_count < baseline.allocation_count);
    assert!(candidate.allocated_bytes < baseline.allocated_bytes);
}

fn sample_invisible_message() -> crate::types::InvisibleQueueMessage {
    VisibleQueueMessage::new(sample_queue_message()).into_invisible(
        ReceiptHandle("receipt-handle-with-enough-length-for-a-real-message".to_string()),
        TimestampMillis::now(),
    )
}

fn sample_queue_message() -> QueueMessage {
    QueueMessage {
        receive_count: 0,
        message_id: MessageId::default(),
        queue_url: "https://queue.example.test/queue".to_string(),
        body: "payload".to_string(),
        message_attributes: Some(HashMap::from([(
            "source".to_string(),
            MessageAttributeValue {
                string_value: Some("test".to_string()),
                binary_value: None,
                data_type: "String".to_string(),
            },
        )])),
        receipt_handle: Some(ReceiptHandle(
            "receipt-handle-with-enough-length-for-a-real-message".to_string(),
        )),
        created_at: TimestampMillis::from_timestamp(1_700_000_000_123),
        visibility_timestamp: None,
        ..Default::default()
    }
}

fn legacy_message_response(message: QueueMessage) -> Option<MessageResponse> {
    let receipt_handle = message.receipt_handle.clone()?;
    let mut response = MessageResponse::from_message(message, receipt_handle.clone());
    response.receipt_handle = receipt_handle.to_string();
    Some(response)
}

fn measure_message_response_allocations(
    label: &'static str,
    build: impl Fn() -> Option<MessageResponse>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "message_response_receipt_ownership_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    let mut checksum = 0usize;
    for _ in 0..64 {
        checksum =
            checksum.saturating_add(build().expect("invisible message").receipt_handle.len());
    }
    std::hint::black_box(checksum);
    guard.finish()
}

#[test]
fn message_attribute_md5_streaming_reduces_large_payload_allocations() {
    let attributes = large_message_attributes();
    let legacy = measure_message_attribute_md5_allocations("legacy", || {
        legacy_md5_of_message_attributes(&attributes)
    });
    let streaming = measure_message_attribute_md5_allocations("streaming", || {
        md5_of_message_attributes(&attributes)
    });

    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&streaming);
    assert!(streaming.allocation_count < legacy.allocation_count);
    assert!(streaming.allocated_bytes < legacy.allocated_bytes);
}

fn large_message_attributes() -> HashMap<String, MessageAttributeValue> {
    (0..10)
        .map(|index| {
            (
                format!("attribute-{index}"),
                MessageAttributeValue {
                    string_value: Some("value".repeat(2_048)),
                    binary_value: None,
                    data_type: "String".to_string(),
                },
            )
        })
        .collect()
}

fn measure_message_attribute_md5_allocations(
    label: &'static str,
    hash: impl Fn() -> Option<String>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "message_attribute_md5_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    let mut checksum = 0usize;
    for _ in 0..8 {
        checksum = checksum.saturating_add(hash().expect("valid message attributes").len());
    }
    std::hint::black_box(checksum);
    guard.finish()
}

fn legacy_md5_of_message_attributes(
    attributes: &HashMap<String, MessageAttributeValue>,
) -> Option<String> {
    let mut encoded = Vec::new();
    let mut names: Vec<_> = attributes.keys().collect();
    names.sort();

    for name in names {
        let value = attributes.get(name)?;
        legacy_encode_string(&mut encoded, name);
        legacy_encode_string(&mut encoded, &value.data_type);
        if value.data_type.starts_with("Binary") {
            encoded.push(2);
            let binary_value = value.binary_value.as_ref()?;
            let decoded = STANDARD.decode(binary_value).ok()?;
            legacy_encode_binary(&mut encoded, &decoded);
        } else {
            encoded.push(1);
            let string_value = value.string_value.as_ref()?;
            legacy_encode_string(&mut encoded, string_value);
        }
    }

    Some(format!("{:x}", md5::compute(encoded)))
}

fn legacy_encode_string(output: &mut Vec<u8>, value: &str) {
    legacy_encode_binary(output, value.as_bytes());
}

fn legacy_encode_binary(output: &mut Vec<u8>, value: &[u8]) {
    let length = value.len().min(u32::MAX as usize) as u32;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
}

#[test]
fn send_message_request_too_many_message_attributes() {
    let mut attributes = HashMap::new();
    for i in 0..11 {
        attributes.insert(
            format!("attr{i}"),
            json!({
                "StringValue": "value",
                "DataType": "String"
            }),
        );
    }

    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "MessageAttributes": attributes
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Number of message attributes [11] exceeds the allowed maximum [10]."
    );
}

#[test]
fn receive_message_request_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
    });

    let result = decode::<ReceiveMessageRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn receive_message_request_max_messages_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MaxNumberOfMessages": 5
    });

    let result = decode::<ReceiveMessageRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn receive_message_request_max_messages_too_high() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MaxNumberOfMessages": 11
    });

    let result = decode::<ReceiveMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Value 11 for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, \
         if provided."
    );
}

#[test]
fn receive_message_request_max_messages_zero() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MaxNumberOfMessages": 0
    });

    let result = decode::<ReceiveMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Value 0 for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, \
         if provided."
    );
}

#[test]
fn receive_message_request_visibility_timeout_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "VisibilityTimeout": 30
    });

    let result = decode::<ReceiveMessageRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn receive_message_request_visibility_timeout_too_high() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "VisibilityTimeout": 43201
    });

    let result = decode::<ReceiveMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Value 43201 for parameter VisibilityTimeout is invalid. Reason: VisibilityTimeout must \
         be an integer between 0 and 43200."
    );
}

#[test]
fn receive_message_request_wait_time_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "WaitTimeSeconds": 10
    });

    let result = decode::<ReceiveMessageRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn receive_message_request_wait_time_too_high() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "WaitTimeSeconds": 21
    });

    let result = decode::<ReceiveMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Value 21 for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if \
         provided."
    );
}

#[test]
fn delete_message_request_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "ReceiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a..."
    });

    let result = decode::<DeleteMessageRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn delete_message_request_empty_receipt_handle() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "ReceiptHandle": ""
    });

    let result = decode::<DeleteMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "Receipt handle cannot be empty");
}

#[test]
fn change_message_visibility_request_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "ReceiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
        "VisibilityTimeout": 60
    });

    let result = decode::<ChangeMessageVisibilityRequest>(json);
    assert!(result.is_ok());
}

#[test]
fn change_message_visibility_request_invalid_timeout() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "ReceiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
        "VisibilityTimeout": -1
    });

    let result = decode::<ChangeMessageVisibilityRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Value -1 for parameter VisibilityTimeout is invalid. Reason: VisibilityTimeout must be \
         an integer between 0 and 43200."
    );
}

#[test]
fn change_message_visibility_batch_negative_timeout_uses_range_error() {
    let result = decode::<ChangeMessageVisibilityBatchRequest>(json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "Entries": [{
            "Id": "entry",
            "ReceiptHandle": "handle",
            "VisibilityTimeout": "-1"
        }]
    }));
    assert_eq!(
        result.unwrap_err().message,
        "Value -1 for parameter VisibilityTimeout is invalid. Reason: VisibilityTimeout must be \
         an integer between 0 and 43200."
    );
}

#[test]
fn message_attribute_name_too_long() {
    let long_name = "a".repeat(257);
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "MessageAttributes": {
            long_name: {
                "StringValue": "value",
                "DataType": "String"
            }
        }
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message (user) attribute name must be shorter than 256 bytes."
    );
}

#[test]
fn message_attribute_name_invalid_characters() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "MessageAttributes": {
            "invalid@name": {
                "StringValue": "value",
                "DataType": "String"
            }
        }
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message (user) attribute name contains invalid non-alphanumeric character '#x40'. You \
         can use only alphanumeric characters, hyphens (-), underscores (_), or periods (.)."
    );
}

#[test]
fn message_attribute_empty_data_type() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "MessageAttributes": {
            "test": {
                "StringValue": "value",
                "DataType": ""
            }
        }
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "The type of message (user) attribute 'test' is invalid. You must use only the following \
         supported type prefixes: Binary, Number, String."
    );
}

#[test]
fn message_attribute_invalid_data_type() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "MessageAttributes": {
            "test": {
                "StringValue": "value",
                "DataType": "Invalid"
            }
        }
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "The type of message (user) attribute 'test' is invalid. You must use only the following \
         supported type prefixes: Binary, Number, String."
    );
}

#[test]
fn message_attribute_both_values() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "MessageAttributes": {
            "test": {
                "StringValue": "value",
                "BinaryValue": "dGVzdA==",
                "DataType": "String"
            }
        }
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message (user) attribute 'test' with type 'String' must use field 'String'."
    );
}

#[test]
fn message_attribute_no_value() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "MessageAttributes": {
            "test": {
                "DataType": "String"
            }
        }
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message (user) attribute 'test' must contain a non-empty value of type 'String'."
    );
}

#[test]
fn empty_queue_url() {
    let json = json!({
        "QueueUrl": "",
        "MessageBody": "Hello"
    });

    let result = decode::<SendMessageRequest>(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "Queue URL cannot be empty");
}

#[test]
fn given_only_unknown_fields_when_decoded_then_required_member_is_missing() {
    // The wire maps this to `MissingParameter` (route protocol tests).
    let error = decode::<CreateQueueRequest>(json!({"InvalidField": "value"})).unwrap_err();
    assert!(
        error.message.starts_with("missing field `QueueName`"),
        "{error:?}"
    );
}

/// D3 and probe 12: AWS ignores an unknown top-level member.
#[test]
fn given_unknown_field_when_decoded_then_it_is_ignored() {
    let request =
        decode::<CreateQueueRequest>(json!({"QueueName": "MyQueue", "Tags": {"a": "1"}})).unwrap();
    assert_eq!(request.queue_name, "MyQueue");
}

#[test]
fn given_wrong_member_type_when_decoded_then_request_is_rejected() {
    assert!(decode::<CreateQueueRequest>(json!({"QueueName": 10})).is_err());
}

#[test]
fn given_invalid_number_message_attribute_when_validated_then_it_is_rejected() {
    let attribute = MessageAttributeValue {
        string_value: Some("not-a-number".to_string()),
        binary_value: None,
        data_type: "Number".to_string(),
    };
    assert!(attribute.validate_named("attr").is_err());
}

#[test]
fn given_invalid_binary_message_attribute_when_validated_then_it_is_rejected() {
    let attribute = MessageAttributeValue {
        string_value: None,
        binary_value: Some("!!!!".to_string()),
        data_type: "Binary".to_string(),
    };
    assert!(attribute.validate_named("attr").is_err());
}

#[test]
fn given_valid_number_and_binary_attributes_when_validated_then_they_are_accepted() {
    for attribute in [
        MessageAttributeValue {
            string_value: Some("42.5".to_string()),
            binary_value: None,
            data_type: "Number".to_string(),
        },
        MessageAttributeValue {
            string_value: None,
            binary_value: Some("YQ==".to_string()),
            data_type: "Binary".to_string(),
        },
    ] {
        attribute
            .validate_named("attr")
            .expect("valid message attribute");
    }
}

#[test]
fn given_invalid_attribute_type_or_value_variant_when_validated_then_request_is_rejected() {
    for (data_type, string_value, binary_value) in [
        ("StringGarbage", Some("value"), None),
        ("NumberGarbage", Some("1"), None),
        ("String", None, Some("AQID")),
        ("Binary", Some("AQID"), None),
    ] {
        let attribute = MessageAttributeValue {
            data_type: data_type.to_owned(),
            string_value: string_value.map(str::to_owned),
            binary_value: binary_value.map(str::to_owned),
        };
        assert!(attribute.validate_named("attr").is_err(), "{data_type}");
    }
}

#[test]
fn given_sqs_number_attributes_when_validated_then_sqs_magnitude_and_precision_limits_apply() {
    for (value, valid) in [
        ("1e-128", true),
        ("-1e126", true),
        ("0", true),
        ("1e-129", false),
        ("1.1e126", false),
        ("1e127", false),
        ("123456789012345678901234567890123456789", false),
    ] {
        let attribute = MessageAttributeValue {
            data_type: "Number".into(),
            string_value: Some(value.into()),
            binary_value: None,
        };
        assert_eq!(attribute.validate_named("attr").is_ok(), valid, "{value}");
    }
}

/// W2 D8 (C3): the sum of body and attribute bytes of a batch is at most
/// 1,048,576; one byte more is `BatchRequestTooLong` with the AWS text.
#[test]
fn given_batch_payload_at_and_over_the_limit_when_validated_then_only_the_larger_is_rejected() {
    let batch = |second_body: usize| {
        json!({
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
            "Entries": [
                {
                    "Id": "first",
                    "MessageBody": "a".repeat(524_288 - 8),
                    "MessageAttributes": {
                        "k": {"DataType": "String", "StringValue": "v"}
                    }
                },
                {"Id": "second", "MessageBody": "b".repeat(second_body)}
            ]
        })
    };
    // Attribute bytes count: name (1), data type (6), and value (1).
    assert!(decode::<SendMessageBatchRequest>(batch(524_288)).is_ok());
    let error = decode::<SendMessageBatchRequest>(batch(524_289)).unwrap_err();
    assert_eq!(
        error.message,
        "Batch requests cannot be longer than 1048576 bytes. You have sent 1048577 bytes."
    );
    assert_eq!(
        crate::sqs_validation_error_type(&error.message),
        "AWS.SimpleQueueService.BatchRequestTooLong"
    );
    assert_eq!(
        crate::sqs_json_error_type(crate::sqs_validation_error_type(&error.message)),
        "com.amazonaws.sqs#BatchRequestTooLong"
    );
}
