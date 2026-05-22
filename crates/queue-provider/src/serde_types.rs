use http_error::HttpApiError;

use crate::{
    ReceiptHandle,
    request_fields::{
        RequestDecoder, decode_change_message_visibility_batch_entry,
        decode_delete_message_batch_entry, decode_send_message_batch_entry,
    },
    request_validation::*,
    types::{
        ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityRequest, CreateQueueRequest,
        DeleteMessageBatchRequest, DeleteMessageRequest, DeleteQueueRequest,
        GetQueueAttributesRequest, GetQueueUrlRequest, ListQueuesRequest, PurgeQueueRequest,
        ReceiveMessageRequest, SendMessageBatchRequest, SendMessageRequest,
        SetQueueAttributesRequest,
    },
};

impl CreateQueueRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueName", "Attributes"])?;
        let request = Self {
            queue_name: decoder.required_string("QueueName")?,
            attributes: decoder.optional_string_map("Attributes")?,
        };
        decoder.finish()?;

        validate_queue_name(&request.queue_name)?;
        if let Some(ref attributes) = request.attributes {
            validate_queue_attributes(attributes)?;
        }
        Ok(request)
    }
}

impl DeleteQueueRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueUrl"])?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        Ok(request)
    }
}

impl ListQueuesRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueNamePrefix"])?;
        let request = Self {
            queue_name_prefix: decoder.optional_string("QueueNamePrefix")?,
        };
        decoder.finish()?;

        if let Some(prefix) = request.queue_name_prefix.as_deref() {
            validate_queue_name_prefix(prefix)?;
        }
        Ok(request)
    }
}

impl GetQueueUrlRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueName"])?;
        let request = Self {
            queue_name: decoder.required_string("QueueName")?,
        };
        decoder.finish()?;

        validate_queue_name(&request.queue_name)?;
        Ok(request)
    }
}

impl GetQueueAttributesRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueUrl", "AttributeNames"])?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
            attribute_names: decoder.optional_string_list("AttributeNames")?,
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        if let Some(attribute_names) = request.attribute_names.as_ref() {
            validate_attribute_names(attribute_names)?;
        }
        Ok(request)
    }
}

impl SetQueueAttributesRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueUrl", "Attributes"])?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
            attributes: decoder.required_string_map("Attributes")?,
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        validate_queue_attributes(&request.attributes)?;
        Ok(request)
    }
}

impl PurgeQueueRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueUrl"])?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        Ok(request)
    }
}

impl SendMessageRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(
            value,
            &[
                "QueueUrl",
                "MessageBody",
                "DelaySeconds",
                "MessageAttributes",
            ],
        )?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
            message_body: decoder.required_string("MessageBody")?,
            delay_seconds: decoder.optional_u32("DelaySeconds")?,
            message_attributes: decoder.optional_message_attributes("MessageAttributes")?,
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        validate_message_body(&request.message_body)?;
        if let Some(delay_seconds) = request.delay_seconds {
            validate_delay_seconds(delay_seconds)?;
        }
        if let Some(ref message_attributes) = request.message_attributes {
            validate_message_attributes(message_attributes)?;
        }
        Ok(request)
    }
}

impl SendMessageBatchRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueUrl", "Entries"])?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
            entries: decoder.required_array("Entries", decode_send_message_batch_entry)?,
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        validate_batch_entries(request.entries.iter().map(|entry| entry.id.as_str()))?;
        for entry in &request.entries {
            validate_message_body(&entry.message_body)?;
            if let Some(delay_seconds) = entry.delay_seconds {
                validate_delay_seconds(delay_seconds)?;
            }
            if let Some(message_attributes) = entry.message_attributes.as_ref() {
                validate_message_attributes(message_attributes)?;
            }
        }
        Ok(request)
    }
}

impl ReceiveMessageRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(
            value,
            &[
                "QueueUrl",
                "MaxNumberOfMessages",
                "VisibilityTimeout",
                "WaitTimeSeconds",
                "AttributeNames",
                "MessageAttributeNames",
            ],
        )?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
            max_number_of_messages: decoder.optional_u32("MaxNumberOfMessages")?,
            visibility_timeout: decoder.optional_u32("VisibilityTimeout")?,
            wait_time_seconds: decoder.optional_u32("WaitTimeSeconds")?,
            attribute_names: decoder.optional_string_list("AttributeNames")?,
            message_attribute_names: decoder.optional_string_list("MessageAttributeNames")?,
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        if let Some(max_messages) = request.max_number_of_messages {
            validate_max_number_of_messages(max_messages)?;
        }
        if let Some(visibility_timeout) = request.visibility_timeout {
            validate_visibility_timeout(visibility_timeout)?;
        }
        if let Some(wait_time) = request.wait_time_seconds {
            validate_wait_time_seconds(wait_time)?;
        }
        Ok(request)
    }
}

impl DeleteMessageRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueUrl", "ReceiptHandle"])?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
            receipt_handle: ReceiptHandle::from(decoder.required_string("ReceiptHandle")?.as_str()),
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        validate_receipt_handle(&request.receipt_handle)?;
        Ok(request)
    }
}

impl DeleteMessageBatchRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueUrl", "Entries"])?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
            entries: decoder.required_array("Entries", decode_delete_message_batch_entry)?,
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        validate_batch_entries(request.entries.iter().map(|entry| entry.id.as_str()))?;
        for entry in &request.entries {
            validate_receipt_handle(&entry.receipt_handle)?;
        }
        Ok(request)
    }
}

impl ChangeMessageVisibilityRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder =
            RequestDecoder::new(value, &["QueueUrl", "ReceiptHandle", "VisibilityTimeout"])?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
            receipt_handle: ReceiptHandle::from(decoder.required_string("ReceiptHandle")?.as_str()),
            visibility_timeout: decoder.required_u32("VisibilityTimeout")?,
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        validate_receipt_handle(&request.receipt_handle)?;
        validate_visibility_timeout(request.visibility_timeout)?;
        Ok(request)
    }
}

impl ChangeMessageVisibilityBatchRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let mut decoder = RequestDecoder::new(value, &["QueueUrl", "Entries"])?;
        let request = Self {
            queue_url: decoder.required_string("QueueUrl")?,
            entries: decoder
                .required_array("Entries", decode_change_message_visibility_batch_entry)?,
        };
        decoder.finish()?;

        validate_queue_url(&request.queue_url)?;
        validate_batch_entries(request.entries.iter().map(|entry| entry.id.as_str()))?;
        for entry in &request.entries {
            validate_receipt_handle(&entry.receipt_handle)?;
            validate_visibility_timeout(entry.visibility_timeout)?;
        }
        Ok(request)
    }
}
