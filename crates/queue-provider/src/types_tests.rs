use std::collections::HashMap;

use serde_json::json;

use crate::types::{
    ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityRequest, CreateQueueRequest,
    DeleteMessageBatchRequest, DeleteMessageRequest, DeleteQueueRequest, GetQueueAttributesRequest,
    GetQueueUrlRequest, ListQueuesRequest, MessageAttributeValue, PurgeQueueRequest,
    ReceiveMessageRequest, SendMessageBatchRequest, SendMessageRequest, SetQueueAttributesRequest,
    md5_of_message_attributes,
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
#[ignore = "Implement validation errors when an API is available"]
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
#[ignore = "Implement validation errors when an API is available"]
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
