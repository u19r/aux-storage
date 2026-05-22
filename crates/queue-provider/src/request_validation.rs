use std::collections::HashMap;

use http_error::HttpApiError;
use storage_types::PaginationLimit;

use crate::{
    QueueError, QueueResult,
    constants::{
        MAX_DELAY_SECONDS, MAX_MAXIMUM_MESSAGE_SIZE_BYTES, MAX_MESSAGE_ATTRIBUTES,
        MAX_MESSAGE_RETENTION_SECONDS, MAX_RECEIVE_MESSAGES, MAX_VISIBILITY_TIMEOUT_SECONDS,
        MAX_WAIT_TIME_SECONDS, MIN_MAXIMUM_MESSAGE_SIZE_BYTES, MIN_MESSAGE_RETENTION_SECONDS,
    },
    types::{
        ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchRequestEntry,
        ChangeMessageVisibilityRequest, CreateQueueRequest, DeleteMessageBatchRequest,
        DeleteMessageBatchRequestEntry, DeleteMessageRequest, DeleteQueueRequest,
        GetQueueAttributesRequest, GetQueueUrlRequest, ListQueuesRequest, MessageAttributeValue,
        PurgeQueueRequest, Queue, ReceiveMessageRequest, SendMessageBatchRequest,
        SendMessageBatchRequestEntry, SendMessageRequest, SetQueueAttributesRequest,
    },
};

impl CreateQueueRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_name(&self.queue_name))?;
        if let Some(attributes) = self.attributes.as_ref() {
            queue_validation(validate_queue_attributes(attributes))?;
        }
        Ok(())
    }
}

impl DeleteQueueRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))
    }
}

impl Queue {
    pub fn validate_url(queue_url: &str) -> QueueResult<()> {
        queue_validation(validate_queue_url(queue_url))
    }
}

impl ListQueuesRequest {
    pub fn validate(&self) -> QueueResult<()> {
        if let Some(prefix) = self.queue_name_prefix.as_deref() {
            queue_validation(validate_queue_name_prefix(prefix))?;
        }
        Ok(())
    }
}

impl GetQueueUrlRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_name(&self.queue_name))
    }
}

impl GetQueueAttributesRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        if let Some(attribute_names) = self.attribute_names.as_ref() {
            queue_validation(validate_attribute_names(attribute_names))?;
        }
        Ok(())
    }
}

impl SetQueueAttributesRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        queue_validation(validate_queue_attributes(&self.attributes))
    }
}

impl PurgeQueueRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))
    }
}

impl SendMessageRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        queue_validation(validate_message_body(&self.message_body))?;
        if let Some(delay_seconds) = self.delay_seconds {
            queue_validation(validate_delay_seconds(delay_seconds))?;
        }
        if let Some(message_attributes) = self.message_attributes.as_ref() {
            queue_validation(validate_message_attributes(message_attributes))?;
        }
        Ok(())
    }
}

impl SendMessageBatchRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        queue_validation(validate_batch_entries(
            self.entries.iter().map(|entry| entry.id.as_str()),
        ))?;
        for entry in &self.entries {
            entry.validate()?;
        }
        Ok(())
    }
}

impl SendMessageBatchRequestEntry {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_message_body(&self.message_body))?;
        if let Some(delay_seconds) = self.delay_seconds {
            queue_validation(validate_delay_seconds(delay_seconds))?;
        }
        if let Some(message_attributes) = self.message_attributes.as_ref() {
            queue_validation(validate_message_attributes(message_attributes))?;
        }
        Ok(())
    }
}

impl ReceiveMessageRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        if let Some(max_messages) = self.max_number_of_messages {
            queue_validation(validate_max_number_of_messages(max_messages))?;
        }
        if let Some(visibility_timeout) = self.visibility_timeout {
            queue_validation(validate_visibility_timeout(visibility_timeout))?;
        }
        if let Some(wait_time) = self.wait_time_seconds {
            queue_validation(validate_wait_time_seconds(wait_time))?;
        }
        if let Some(attribute_names) = self.attribute_names.as_ref() {
            queue_validation(validate_attribute_names(attribute_names))?;
        }
        if let Some(attribute_names) = self.message_attribute_names.as_ref() {
            queue_validation(validate_attribute_names(attribute_names))?;
        }
        Ok(())
    }
}

impl DeleteMessageRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        queue_validation(validate_receipt_handle(&self.receipt_handle))
    }
}

impl DeleteMessageBatchRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        queue_validation(validate_batch_entries(
            self.entries.iter().map(|entry| entry.id.as_str()),
        ))?;
        for entry in &self.entries {
            entry.validate()?;
        }
        Ok(())
    }
}

impl DeleteMessageBatchRequestEntry {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_receipt_handle(&self.receipt_handle))
    }
}

impl ChangeMessageVisibilityRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        queue_validation(validate_receipt_handle(&self.receipt_handle))?;
        queue_validation(validate_visibility_timeout(self.visibility_timeout))
    }
}

impl ChangeMessageVisibilityBatchRequest {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        queue_validation(validate_batch_entries(
            self.entries.iter().map(|entry| entry.id.as_str()),
        ))?;
        for entry in &self.entries {
            entry.validate()?;
        }
        Ok(())
    }
}

impl ChangeMessageVisibilityBatchRequestEntry {
    pub fn validate(&self) -> QueueResult<()> {
        queue_validation(validate_receipt_handle(&self.receipt_handle))?;
        queue_validation(validate_visibility_timeout(self.visibility_timeout))
    }
}

impl MessageAttributeValue {
    pub fn validate(&self) -> Result<(), HttpApiError> {
        if self.data_type.is_empty() {
            return Err(HttpApiError::validation_error(
                "Message attribute data type cannot be empty",
            ));
        }

        if !self.data_type.starts_with("String")
            && !self.data_type.starts_with("Number")
            && !self.data_type.starts_with("Binary")
        {
            return Err(HttpApiError::validation_error(
                "Message attribute data type must be String, Number, or Binary (with optional \
                 .custom suffix)",
            ));
        }

        if self.string_value.is_some() && self.binary_value.is_some() {
            return Err(HttpApiError::validation_error(
                "Message attribute cannot have both StringValue and BinaryValue",
            ));
        }

        if self.string_value.is_none() && self.binary_value.is_none() {
            return Err(HttpApiError::validation_error(
                "Message attribute must have either StringValue or BinaryValue",
            ));
        }

        Ok(())
    }
}

fn queue_validation(result: Result<(), HttpApiError>) -> QueueResult<()> {
    result.map_err(|error| {
        QueueError::validation_with_detail(
            crate::QueueValidationKind::InvalidParameterValue,
            error.message,
        )
    })
}

pub(crate) fn validate_queue_name(queue_name: &str) -> Result<(), HttpApiError> {
    if queue_name.is_empty() {
        return Err(HttpApiError::validation_error("Queue name cannot be empty"));
    }

    if queue_name.len() > 80 {
        return Err(HttpApiError::validation_error(
            "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length",
        ));
    }

    if queue_name.ends_with(".fifo") {
        return Err(HttpApiError::validation_error(
            "FIFO queues are not supported",
        ));
    }

    for ch in queue_name.chars() {
        if !ch.is_alphanumeric() && ch != '-' && ch != '_' {
            return Err(HttpApiError::validation_error(
                "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in \
                 length",
            ));
        }
    }

    Ok(())
}

pub(crate) fn validate_queue_name_prefix(queue_name_prefix: &str) -> Result<(), HttpApiError> {
    if queue_name_prefix.is_empty() {
        return Err(HttpApiError::validation_error(
            "Queue name prefix cannot be empty",
        ));
    }

    for ch in queue_name_prefix.chars() {
        if !ch.is_alphanumeric() && ch != '-' && ch != '_' {
            return Err(HttpApiError::validation_error(
                "Queue name prefix can only contain alphanumeric characters, hyphens, and \
                 underscores",
            ));
        }
    }

    Ok(())
}

pub(crate) fn validate_queue_url(queue_url: &str) -> Result<(), HttpApiError> {
    if queue_url.is_empty() {
        return Err(HttpApiError::validation_error("Queue URL cannot be empty"));
    }

    Ok(())
}

pub(crate) fn validate_message_body(message_body: &str) -> Result<(), HttpApiError> {
    if message_body.is_empty() {
        return Err(HttpApiError::validation_error(
            "The request must contain the parameter MessageBody.",
        ));
    }

    if message_body.len() > 1_048_576 {
        return Err(HttpApiError::validation_error(
            "Message body cannot exceed 1 MiB (1,048,576 bytes)",
        ));
    }

    for ch in message_body.chars() {
        let code_point = ch as u32;
        if !(code_point == 0x9
            || code_point == 0xA
            || code_point == 0xD
            || (0x20..=0xD7FF).contains(&code_point)
            || (0xE000..=0xFFFD).contains(&code_point)
            || (0x10000..=0x0010_FFFF).contains(&code_point))
        {
            return Err(HttpApiError::validation_error(
                "Message body contains invalid Unicode characters",
            ));
        }
    }

    Ok(())
}

pub(crate) fn validate_delay_seconds(delay_seconds: u32) -> Result<(), HttpApiError> {
    if !(0..=MAX_DELAY_SECONDS).contains(&delay_seconds) {
        return Err(HttpApiError::validation_error(format!(
            "DelaySeconds must be between 0 and {MAX_DELAY_SECONDS}"
        )));
    }

    Ok(())
}

pub(crate) fn validate_max_number_of_messages(max_messages: u32) -> Result<(), HttpApiError> {
    let limits = PaginationLimit::new(1, MAX_RECEIVE_MESSAGES);
    limits.validate(max_messages).map_err(|_| {
        HttpApiError::validation_error(format!(
            "Value {max_messages} for parameter MaxNumberOfMessages is invalid. Reason: Must be \
             between {} and {}, if provided.",
            limits.min_limit(),
            limits.max_limit(),
        ))
    })?;
    Ok(())
}

pub(crate) fn validate_visibility_timeout(visibility_timeout: u32) -> Result<(), HttpApiError> {
    if !(0..=MAX_VISIBILITY_TIMEOUT_SECONDS).contains(&visibility_timeout) {
        return Err(HttpApiError::validation_error(format!(
            "VisibilityTimeout must be between 0 and {MAX_VISIBILITY_TIMEOUT_SECONDS} seconds"
        )));
    }

    Ok(())
}

pub(crate) fn validate_wait_time_seconds(wait_time: u32) -> Result<(), HttpApiError> {
    if !(0..=MAX_WAIT_TIME_SECONDS).contains(&wait_time) {
        return Err(HttpApiError::validation_error(format!(
            "WaitTimeSeconds must be between 0 and {MAX_WAIT_TIME_SECONDS}"
        )));
    }

    Ok(())
}

pub(crate) fn validate_receipt_handle(receipt_handle: &str) -> Result<(), HttpApiError> {
    if receipt_handle.is_empty() {
        return Err(HttpApiError::validation_error(
            "Receipt handle cannot be empty",
        ));
    }

    Ok(())
}

pub(crate) fn validate_queue_attributes(
    attributes: &HashMap<String, String>,
) -> Result<(), HttpApiError> {
    for (key, value) in attributes {
        if key.is_empty() {
            return Err(HttpApiError::validation_error(
                "Attribute key cannot be empty",
            ));
        }

        if value.is_empty() {
            return Err(HttpApiError::validation_error(
                "Attribute value cannot be empty",
            ));
        }

        validate_queue_attribute(key, value)?;
    }

    Ok(())
}

fn validate_queue_attribute(key: &str, value: &str) -> Result<(), HttpApiError> {
    match key {
        "DelaySeconds" => validate_numeric_attribute(key, value, 0, MAX_DELAY_SECONDS),
        "VisibilityTimeout" => {
            validate_numeric_attribute(key, value, 0, MAX_VISIBILITY_TIMEOUT_SECONDS)
        }
        "MaximumMessageSize" => validate_numeric_attribute(
            key,
            value,
            MIN_MAXIMUM_MESSAGE_SIZE_BYTES,
            MAX_MAXIMUM_MESSAGE_SIZE_BYTES,
        ),
        "MessageRetentionPeriod" => validate_numeric_attribute(
            key,
            value,
            MIN_MESSAGE_RETENTION_SECONDS,
            MAX_MESSAGE_RETENTION_SECONDS,
        ),
        "ReceiveMessageWaitTimeSeconds" => {
            validate_numeric_attribute(key, value, 0, MAX_WAIT_TIME_SECONDS)
        }
        "FifoQueue" | "ContentBasedDeduplication" => Err(HttpApiError::validation_error(
            "FIFO queue attributes are not supported",
        )),
        _ => Err(HttpApiError::validation_error(format!(
            "Unsupported queue attribute: {key}"
        ))),
    }
}

fn validate_numeric_attribute(
    key: &str,
    value: &str,
    min: u32,
    max: u32,
) -> Result<(), HttpApiError> {
    let parsed = value.parse::<u32>().map_err(|_| {
        HttpApiError::validation_error(format!("{key} must be a number between {min} and {max}"))
    })?;
    if !(min..=max).contains(&parsed) {
        return Err(HttpApiError::validation_error(format!(
            "{key} must be between {min} and {max}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_attribute_names(attribute_names: &[String]) -> Result<(), HttpApiError> {
    for name in attribute_names {
        if name.trim().is_empty() {
            return Err(HttpApiError::validation_error(
                "Attribute name cannot be empty",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_message_attributes(
    message_attributes: &HashMap<String, MessageAttributeValue>,
) -> Result<(), HttpApiError> {
    if message_attributes.len() > MAX_MESSAGE_ATTRIBUTES {
        return Err(HttpApiError::validation_error(format!(
            "Message can have at most {MAX_MESSAGE_ATTRIBUTES} message attributes"
        )));
    }

    for (key, value) in message_attributes {
        if key.is_empty() {
            return Err(HttpApiError::validation_error(
                "Message attribute name cannot be empty",
            ));
        }

        if key.len() > 256 {
            return Err(HttpApiError::validation_error(
                "Message attribute name cannot exceed 256 characters",
            ));
        }

        for ch in key.chars() {
            if !ch.is_alphanumeric() && ch != '_' && ch != '-' && ch != '.' {
                return Err(HttpApiError::validation_error(
                    "Message attribute name can only contain alphanumeric characters, \
                     underscores, hyphens, and periods",
                ));
            }
        }

        value.validate()?;
    }

    Ok(())
}

pub(crate) fn validate_batch_entries<'a>(
    entry_ids: impl IntoIterator<Item = &'a str>,
) -> Result<(), HttpApiError> {
    let mut seen = std::collections::HashSet::new();
    let mut count = 0;

    for id in entry_ids {
        count += 1;
        if id.is_empty() {
            return Err(HttpApiError::validation_error(
                "Batch entry id cannot be empty",
            ));
        }
        if id.len() > 80 {
            return Err(HttpApiError::validation_error(
                "Batch entry id must not exceed 80 characters",
            ));
        }
        for ch in id.chars() {
            if !ch.is_alphanumeric() && ch != '-' && ch != '_' {
                return Err(HttpApiError::validation_error(
                    "Batch entry id can only contain alphanumeric characters, hyphens, and \
                     underscores",
                ));
            }
        }
        if !seen.insert(id) {
            return Err(HttpApiError::validation_error(format!("Id {id} repeated.")));
        }
    }

    if count == 0 {
        return Err(HttpApiError::validation_error(
            "Batch request must include at least one entry",
        ));
    }
    if count > 10 {
        return Err(HttpApiError::validation_error(
            "Batch request cannot include more than 10 entries",
        ));
    }

    Ok(())
}
