use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;
use storage_types::TimestampMillis;

use crate::{
    MessageId, ReceiptHandle,
    types::{
        ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityRequest, CreateQueueRequest,
        DeleteMessageBatchRequest, DeleteMessageRequest, DeleteQueueRequest,
        GetQueueAttributesRequest, GetQueueUrlRequest, ListQueuesRequest, MessageAttributeValue,
        MessageResponse, PurgeQueueRequest, QueueMessage, ReceiveMessageRequest,
        SendMessageBatchRequest, SendMessageRequest, SetQueueAttributesRequest,
        VisibleQueueMessage, md5_of_message_attributes,
    },
};

#[test]
fn create_queue_request_valid() {
    let json = json!({
        "QueueName": "valid-queue_name"
    });

    let result = CreateQueueRequest::from_json(json);
    assert!(result.is_ok());
    let request = result.unwrap();
    assert_eq!(request.queue_name, "valid-queue_name");
}

#[test]
fn create_queue_request_empty_name() {
    let json = json!({
        "QueueName": ""
    });

    let result = CreateQueueRequest::from_json(json);
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

    let result = CreateQueueRequest::from_json(json);
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

    let result = CreateQueueRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn create_queue_request_invalid_characters() {
    let json = json!({
        "QueueName": "invalid@queue"
    });

    let result = CreateQueueRequest::from_json(json);
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

    let result = CreateQueueRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn create_queue_request_rejects_fifo_name() {
    let json = json!({
        "QueueName": "jobs.fifo"
    });

    let result = CreateQueueRequest::from_json(json);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().message, "FIFO queues are not supported");
}

#[test]
fn delete_queue_request_valid() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue"
    });

    let result = DeleteQueueRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn list_queues_request_valid_prefix() {
    let json = json!({
        "QueueNamePrefix": "billing_"
    });

    let result = ListQueuesRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn list_queues_request_invalid_prefix() {
    let json = json!({
        "QueueNamePrefix": "bad prefix"
    });

    let result = ListQueuesRequest::from_json(json);
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

    let result = GetQueueUrlRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn get_queue_attributes_request_empty_attribute_name() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue",
        "AttributeNames": [""]
    });

    let result = GetQueueAttributesRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "Attribute name cannot be empty");
}

#[test]
fn set_queue_attributes_request_valid() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue",
        "Attributes": {
            "VisibilityTimeout": "30"
        }
    });

    let result = SetQueueAttributesRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn set_queue_attributes_request_rejects_unsupported_attribute() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue",
        "Attributes": {
            "Policy": "{}"
        }
    });

    let result = SetQueueAttributesRequest::from_json(json);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().message,
        "Unsupported queue attribute: Policy"
    );
}

#[test]
fn set_queue_attributes_request_rejects_out_of_range_retention() {
    let json = json!({
        "QueueUrl": "https://queue.example.com/000000000000/test-queue",
        "Attributes": {
            "MessageRetentionPeriod": "59"
        }
    });

    let result = SetQueueAttributesRequest::from_json(json);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().message,
        "MessageRetentionPeriod must be between 60 and 1209600"
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

    let result = SetQueueAttributesRequest::from_json(json);
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

    let result = PurgeQueueRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_request_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello World"
    });

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_request_empty_body() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": ""
    });

    let result = SendMessageRequest::from_json(json);
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

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message body cannot exceed 1 MiB (1,048,576 bytes)"
    );
}

#[test]
fn send_message_request_body_max_size() {
    let max_body = "a".repeat(1_048_576); // Exactly at limit
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": max_body
    });

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_request_valid_unicode() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello\t\n\rWorld 🌍"
    });

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_request_delay_seconds_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "DelaySeconds": 300
    });

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn send_message_request_delay_seconds_too_high() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "DelaySeconds": 901
    });

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "DelaySeconds must be between 0 and 900");
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

    let result = SendMessageBatchRequest::from_json(json);
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

    let result = SendMessageBatchRequest::from_json(json);
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

    let result = DeleteMessageBatchRequest::from_json(json);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().message,
        "Batch request cannot include more than 10 entries"
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

    let result = ChangeMessageVisibilityBatchRequest::from_json(json);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().message,
        "VisibilityTimeout must be between 0 and 43200 seconds"
    );
}

#[test]
fn send_message_request_delay_seconds_negative() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello",
        "DelaySeconds": -1
    });

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "DelaySeconds must be between 0 and 900");
}

#[test]
fn send_message_request_delay_seconds_string_and_malformed_values_preserve_contracts() {
    let base = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MessageBody": "Hello"
    });

    for value in [json!("-1"), json!(901)] {
        let mut request = base.clone();
        request["DelaySeconds"] = value;
        assert_eq!(
            SendMessageRequest::from_json(request).unwrap_err().message,
            "DelaySeconds must be between 0 and 900"
        );
    }

    for value in [
        json!(u64::from(u32::MAX) + 1),
        json!(1.5),
        json!("not-a-number"),
    ] {
        let mut request = base.clone();
        request["DelaySeconds"] = value;
        assert_eq!(
            SendMessageRequest::from_json(request).unwrap_err().message,
            "DelaySeconds must be a unsigned 32-bit integer"
        );
    }

    for request in [
        base.clone(),
        json!({
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
            "MessageBody": "Hello",
            "DelaySeconds": null
        }),
    ] {
        assert!(SendMessageRequest::from_json(request).is_ok());
    }
}

#[test]
fn send_message_batch_request_negative_delay_uses_range_error() {
    let result = SendMessageBatchRequest::from_json(json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "Entries": [{
            "Id": "entry",
            "MessageBody": "Hello",
            "DelaySeconds": "-1"
        }]
    }));
    assert_eq!(
        result.unwrap_err().message,
        "DelaySeconds must be between 0 and 900"
    );
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

    let result = SendMessageRequest::from_json(json);
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

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message can have at most 10 message attributes"
    );
}

#[test]
fn receive_message_request_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
    });

    let result = ReceiveMessageRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn receive_message_request_max_messages_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MaxNumberOfMessages": 5
    });

    let result = ReceiveMessageRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn receive_message_request_max_messages_too_high() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "MaxNumberOfMessages": 11
    });

    let result = ReceiveMessageRequest::from_json(json);
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

    let result = ReceiveMessageRequest::from_json(json);
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

    let result = ReceiveMessageRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn receive_message_request_visibility_timeout_too_high() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "VisibilityTimeout": 43201
    });

    let result = ReceiveMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "VisibilityTimeout must be between 0 and 43200 seconds"
    );
}

#[test]
fn receive_message_request_wait_time_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "WaitTimeSeconds": 10
    });

    let result = ReceiveMessageRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn receive_message_request_wait_time_too_high() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "WaitTimeSeconds": 21
    });

    let result = ReceiveMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "WaitTimeSeconds must be between 0 and 20");
}

#[test]
fn delete_message_request_valid() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "ReceiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a..."
    });

    let result = DeleteMessageRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn delete_message_request_empty_receipt_handle() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "ReceiptHandle": ""
    });

    let result = DeleteMessageRequest::from_json(json);
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

    let result = ChangeMessageVisibilityRequest::from_json(json);
    assert!(result.is_ok());
}

#[test]
fn change_message_visibility_request_invalid_timeout() {
    let json = json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "ReceiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
        "VisibilityTimeout": -1
    });

    let result = ChangeMessageVisibilityRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "VisibilityTimeout must be between 0 and 43200 seconds"
    );
}

#[test]
fn change_message_visibility_batch_negative_timeout_uses_range_error() {
    let result = ChangeMessageVisibilityBatchRequest::from_json(json!({
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        "Entries": [{
            "Id": "entry",
            "ReceiptHandle": "handle",
            "VisibilityTimeout": "-1"
        }]
    }));
    assert_eq!(
        result.unwrap_err().message,
        "VisibilityTimeout must be between 0 and 43200 seconds"
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

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message attribute name cannot exceed 256 characters"
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

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message attribute name can only contain alphanumeric characters, underscores, hyphens, \
         and periods"
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

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "Message attribute data type cannot be empty");
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

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message attribute data type must be String, Number, or Binary (with optional .custom \
         suffix)"
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

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message attribute cannot have both StringValue and BinaryValue"
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

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message,
        "Message attribute must have either StringValue or BinaryValue"
    );
}

#[test]
fn empty_queue_url() {
    let json = json!({
        "QueueUrl": "",
        "MessageBody": "Hello"
    });

    let result = SendMessageRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "Queue URL cannot be empty");
}

#[test]
fn invalid_json_structure() {
    let json = json!({
        "InvalidField": "value"
    });

    let result = CreateQueueRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "Unknown field: InvalidField");
}

#[test]
fn unknown_field_is_preserved_as_unknown_field_error() {
    let json = json!({
        "QueueName": "MyQueue",
        "ExtraField": "value"
    });

    let result = CreateQueueRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "Unknown field: ExtraField");
}

#[test]
fn wrong_type_is_preserved_as_wrong_type_error() {
    let json = json!({
        "QueueName": 10
    });

    let result = CreateQueueRequest::from_json(json);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message, "QueueName must be a string");
}
