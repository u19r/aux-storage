use std::collections::HashMap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use http_error::HttpApiError;
use storage_types::PaginationLimit;

use crate::{
    QueueError, QueueResult,
    constants::{
        MAX_BATCH_PAYLOAD_BYTES, MAX_DEAD_LETTER_SOURCE_RESULTS, MAX_DELAY_SECONDS,
        MAX_KMS_DATA_KEY_REUSE_SECONDS, MAX_KMS_MASTER_KEY_ID_BYTES, MAX_LIST_QUEUES_RESULTS,
        MAX_MAXIMUM_MESSAGE_SIZE_BYTES, MAX_MESSAGE_ATTRIBUTES, MAX_MESSAGE_RETENTION_SECONDS,
        MAX_RECEIVE_MESSAGES, MAX_TRACE_HEADER_BYTES, MAX_VISIBILITY_TIMEOUT_SECONDS,
        MAX_WAIT_TIME_SECONDS, MIN_KMS_DATA_KEY_REUSE_SECONDS, MIN_MAXIMUM_MESSAGE_SIZE_BYTES,
        MIN_MESSAGE_RETENTION_SECONDS, QUEUE_ATTRIBUTE_NAMES, QUEUE_REDRIVE_POLICY_ATTRIBUTE,
        SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE, SQS_BATCH_REQUEST_TOO_LONG_ERROR_TYPE,
        SQS_EMPTY_VALUE_ERROR_TYPE, SQS_INVALID_ATTRIBUTE_VALUE_ERROR_TYPE,
        SQS_INVALID_BATCH_ENTRY_ID_ERROR_TYPE, SQS_INVALID_MESSAGE_CONTENTS_ERROR_TYPE,
        SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE, SQS_MISSING_PARAMETER_ERROR_TYPE,
    },
    types::{
        ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchRequestEntry,
        ChangeMessageVisibilityRequest, CreateQueueRequest, DeleteMessageBatchRequest,
        DeleteMessageBatchRequestEntry, DeleteMessageRequest, DeleteQueueRequest,
        GetQueueAttributesRequest, GetQueueUrlRequest, ListDeadLetterSourceQueuesRequest,
        ListQueueTagsRequest, ListQueuesRequest, MessageAttributeValue, PurgeQueueRequest, Queue,
        QueueMessage, ReceiveMessageRequest, SendMessageBatchRequest, SendMessageBatchRequestEntry,
        SendMessageRequest, SetQueueAttributesRequest, TagQueueRequest, UntagQueueRequest,
    },
};

pub trait QueueRequestValidation {
    fn validate_request(&self) -> QueueResult<()>;
}

impl QueueRequestValidation for CreateQueueRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_name(&self.queue_name))?;
        if let Some(attributes) = self.attributes.as_ref() {
            validate_queue_attributes(attributes)?;
        }
        if let Some(tags) = self.tags.as_ref() {
            queue_validation(validate_tags(tags))?;
        }
        if let Some(tags) = self.tags.clone() {
            crate::apply_tag_change(&mut std::collections::BTreeMap::new(), tags, &[]).map_err(
                |violation| crate::queue_catalog::tag_violation(violation, &self.queue_name),
            )?;
        }
        Ok(())
    }
}

impl QueueRequestValidation for TagQueueRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        if self.tags.is_empty() {
            return queue_validation(Err(missing_parameter("Tags")));
        }
        queue_validation(validate_tags(&self.tags))
    }
}

/// The AWS text of a collection member that the request leaves empty.
fn missing_parameter(member: &str) -> HttpApiError {
    HttpApiError::validation_error(format!("The request must contain the parameter {member}."))
}

/// Tag keys and values are returned as Query text (not probed).
fn validate_tags(tags: &std::collections::BTreeMap<String, String>) -> Result<(), HttpApiError> {
    for (key, value) in tags {
        validate_returned_text("Tag key", key, usize::MAX)?;
        validate_returned_text("Tag value", value, usize::MAX)?;
    }
    Ok(())
}

impl QueueRequestValidation for UntagQueueRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        if self.tag_keys.is_empty() {
            return queue_validation(Err(missing_parameter("TagKeys")));
        }
        if self
            .tag_keys
            .iter()
            .any(|key| key.is_empty() || key.chars().count() > 128)
        {
            return queue_validation(Err(HttpApiError::validation_error(
                crate::queue_catalog::TAG_KEY_LENGTH_MESSAGE,
            )));
        }
        Ok(())
    }
}

impl QueueRequestValidation for ListQueueTagsRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))
    }
}

impl QueueRequestValidation for DeleteQueueRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))
    }
}

impl Queue {
    pub fn validate_url(queue_url: &str) -> QueueResult<()> {
        queue_validation(validate_queue_url(queue_url))
    }
}

impl QueueRequestValidation for ListQueuesRequest {
    fn validate_request(&self) -> QueueResult<()> {
        if let Some(prefix) = self.queue_name_prefix.as_deref() {
            queue_validation(validate_queue_name_prefix(prefix))?;
        }
        // Probe 7: 0 is accepted and means no limit below 1,000.
        if self
            .max_results
            .is_some_and(|max| !(0..=i64::from(MAX_LIST_QUEUES_RESULTS.get())).contains(&max))
        {
            return queue_validation(Err(max_results_error(MAX_LIST_QUEUES_RESULTS.get())));
        }
        Ok(())
    }
}

/// Probe 7 text of a `MaxResults` outside its range.
fn max_results_error(maximum: u32) -> HttpApiError {
    HttpApiError::validation_error(format!(
        "Value for parameter MaxResults is invalid. Reason: MaxResults must be an integer between \
         1 and {maximum}."
    ))
}

impl QueueRequestValidation for ListDeadLetterSourceQueuesRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_dead_letter_source_request(self))
    }
}

pub(crate) fn validate_dead_letter_source_request(
    request: &ListDeadLetterSourceQueuesRequest,
) -> Result<(), HttpApiError> {
    validate_queue_url(&request.queue_url)?;
    if request
        .max_results
        .is_some_and(|max| !(1..=i64::from(MAX_DEAD_LETTER_SOURCE_RESULTS.get())).contains(&max))
    {
        return Err(max_results_error(MAX_DEAD_LETTER_SOURCE_RESULTS.get()));
    }
    Ok(())
}

impl QueueRequestValidation for GetQueueUrlRequest {
    /// The suite shows that a lookup does not check the name's format: a
    /// malformed name is a missing queue.
    fn validate_request(&self) -> QueueResult<()> {
        if self.queue_name.is_empty() {
            return queue_validation(Err(HttpApiError::validation_error(
                "Queue name cannot be empty",
            )));
        }
        Ok(())
    }
}

impl QueueRequestValidation for GetQueueAttributesRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        let names = self.attribute_names.as_deref().unwrap_or_default();
        match names
            .iter()
            .find(|name| !QUEUE_ATTRIBUTE_NAMES.contains(&name.as_str()))
        {
            Some(name) => Err(unknown_attribute(name)),
            None => Ok(()),
        }
    }
}

/// Probe 12: `AWSTraceHeader` is the only system attribute. A `String` value
/// must not be empty (suite script); a value of another `DataType`, or one
/// that is no trace header, is accepted and not kept
/// ([`keeps_trace_header`]).
fn validate_system_attributes(
    attributes: Option<&HashMap<String, MessageAttributeValue>>,
) -> Result<(), HttpApiError> {
    for (name, value) in attributes.into_iter().flatten() {
        if name != crate::constants::TRACE_HEADER_ATTRIBUTE {
            return Err(HttpApiError::validation_error(format!(
                "Message system attribute name '{name}' is invalid."
            )));
        }
        if value.data_type != "String" {
            continue;
        }
        match value.string_value.as_deref() {
            None | Some("") => {
                return Err(HttpApiError::validation_error(format!(
                    "Message system attribute '{name}' must contain a non-empty value of type \
                     'String'."
                )));
            }
            Some(trace_header) => {
                validate_returned_text(name, trace_header, MAX_TRACE_HEADER_BYTES)?;
            }
        }
    }
    Ok(())
}

/// Whether a send keeps this `AWSTraceHeader` value: a `String` that names
/// its `Root=` segment (suite script: any other value is dropped silently).
pub(crate) fn keeps_trace_header(value: &MessageAttributeValue) -> bool {
    value.data_type == "String"
        && value.string_value.as_deref().is_some_and(|header| {
            header
                .split(';')
                .any(|segment| segment.trim_start().starts_with("Root="))
        })
}

/// Probe 12: a standard queue rejects `MessageDeduplicationId`.
fn reject_deduplication_id(value: Option<&str>) -> Result<(), HttpApiError> {
    value.map_or(Ok(()), |value| {
        Err(HttpApiError::validation_error(format!(
            "Value {value} for parameter MessageDeduplicationId is invalid. Reason: The request \
             include parameter that is not valid for this queue type."
        )))
    })
}

/// Probe 7: 400 `InvalidAttributeName` "Unknown Attribute X.".
pub(crate) fn unknown_attribute(name: &str) -> QueueError {
    QueueError::validation_with_detail(
        crate::QueueValidationKind::InvalidAttributeName,
        format!("Unknown Attribute {name}."),
    )
}

impl QueueRequestValidation for SetQueueAttributesRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        if self.attributes.is_empty() {
            return queue_validation(Err(missing_parameter("Attribute.Name")));
        }
        validate_queue_attributes(&self.attributes)
    }
}

impl QueueRequestValidation for PurgeQueueRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))
    }
}

impl QueueRequestValidation for SendMessageRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        queue_validation(reject_deduplication_id(
            self.message_deduplication_id.as_deref(),
        ))?;
        queue_validation(validate_system_attributes(
            self.message_system_attributes.as_ref(),
        ))?;
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

impl QueueMessage {
    /// Reuse public send validation for already captured message content.
    pub fn validate_for_send(&self) -> QueueResult<()> {
        queue_validation(validate_message_body(&self.body))?;
        if let Some(attributes) = &self.message_attributes {
            queue_validation(validate_message_attributes(attributes))?;
        }
        Ok(())
    }
}

impl QueueRequestValidation for SendMessageBatchRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        validate_batch_count("SendMessageBatchRequestEntry", self.entries.len())?;
        queue_validation(validate_batch_entries(
            self.entries.iter().map(|entry| entry.id.as_str()),
        ))?;
        queue_validation(validate_batch_payload(&self.entries))
    }
}

impl SendMessageBatchRequestEntry {
    /// An entry fails alone: the caller reports it as `Failed` and sends the
    /// others. `position` is the 1-based place of the entry in the request.
    /// An empty body is `EmptyValue` with the AWS text.
    pub fn validate(&self, position: usize) -> QueueResult<()> {
        if self.message_body.is_empty() {
            return Err(QueueError::batch_entry(
                true,
                SQS_EMPTY_VALUE_ERROR_TYPE.to_owned(),
                format!("No Value Found for SendMessageBatchRequestEntry.{position}.MessageBody."),
            ));
        }
        self.validate_fields().map_err(|error| {
            QueueError::batch_entry(
                true,
                sqs_validation_error_type(&error.message).to_owned(),
                error.message,
            )
        })
    }

    fn validate_fields(&self) -> Result<(), HttpApiError> {
        reject_deduplication_id(self.message_deduplication_id.as_deref())?;
        validate_system_attributes(self.message_system_attributes.as_ref())?;
        validate_message_body(&self.message_body)?;
        if let Some(delay_seconds) = self.delay_seconds {
            validate_delay_seconds(delay_seconds)?;
        }
        if let Some(message_attributes) = self.message_attributes.as_ref() {
            validate_message_attributes(message_attributes)?;
        }
        Ok(())
    }
}

impl QueueRequestValidation for ReceiveMessageRequest {
    fn validate_request(&self) -> QueueResult<()> {
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

impl QueueRequestValidation for DeleteMessageRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        queue_validation(validate_receipt_handle(&self.receipt_handle))
    }
}

impl QueueRequestValidation for DeleteMessageBatchRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        validate_batch_count("DeleteMessageBatchRequestEntry", self.entries.len())?;
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

impl QueueRequestValidation for ChangeMessageVisibilityRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        queue_validation(validate_receipt_handle(&self.receipt_handle))?;
        queue_validation(validate_visibility_timeout(self.visibility_timeout))
    }
}

impl QueueRequestValidation for ChangeMessageVisibilityBatchRequest {
    fn validate_request(&self) -> QueueResult<()> {
        queue_validation(validate_queue_url(&self.queue_url))?;
        validate_batch_count(
            "ChangeMessageVisibilityBatchRequestEntry",
            self.entries.len(),
        )?;
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
    /// Suite scripts: every refusal names the attribute as `message (user)
    /// attribute '<name>'`.
    pub fn validate_named(&self, name: &str) -> Result<(), HttpApiError> {
        let bad_type = || {
            HttpApiError::validation_error(format!(
                "The type of message (user) attribute '{name}' is invalid. You must use only the \
                 following supported type prefixes: Binary, Number, String."
            ))
        };
        let data_type = self.data_type.as_str();
        let (prefix, label) = match data_type.split_once('.') {
            Some((prefix, label)) => (prefix, Some(label)),
            None => (data_type, None),
        };
        if !matches!(prefix, "String" | "Number" | "Binary") || label == Some("") {
            return Err(bad_type());
        }
        if data_type.len() > 256 {
            return Err(HttpApiError::validation_error(
                "Message (user) attribute type must be shorter than 256 bytes.",
            ));
        }

        let wrong_field = |field: &str| {
            HttpApiError::validation_error(format!(
                "Message (user) attribute '{name}' with type '{prefix}' must use field '{field}'."
            ))
        };
        let wants_binary = prefix == "Binary";
        let field = if wants_binary { "Binary" } else { "String" };
        fn non_empty(value: Option<&str>) -> Option<&str> {
            value.filter(|value| !value.is_empty())
        }
        match (
            non_empty(self.string_value.as_deref()),
            non_empty(self.binary_value.as_deref()),
        ) {
            (None, None) => {
                return Err(HttpApiError::validation_error(format!(
                    "Message (user) attribute '{name}' must contain a non-empty value of type \
                     '{prefix}'."
                )));
            }
            (Some(_), Some(_)) => return Err(wrong_field(field)),
            (Some(_), None) if wants_binary => return Err(wrong_field(field)),
            (None, Some(_)) if !wants_binary => return Err(wrong_field(field)),
            _ => {}
        }

        if let Some(value) = self.string_value.as_deref() {
            validate_returned_text("Message attribute value", value, usize::MAX)?;
            if prefix == "Number" {
                storage_types::canonical_sqs_number(value).map_err(|error| {
                    if matches!(error, storage_types::DynamodbNumberError::Precision) {
                        HttpApiError::validation_error(format!(
                            "Message (user) attribute '{name}' number value [{value}] must \
                             contain fewer than 38 precision digits."
                        ))
                    } else {
                        HttpApiError::validation_error(format!(
                            "Can't cast the value of message (user) attribute '{name}' to a \
                             number."
                        ))
                    }
                })?;
            }
        }
        if let Some(value) = self.binary_value.as_deref() {
            STANDARD.decode(value).map_err(|_| {
                HttpApiError::validation_error("Binary message attribute is not valid base64")
            })?;
        }
        Ok(())
    }
}

const BATCH_REQUEST_TOO_LONG_PREFIX: &str = "Batch requests cannot be longer than ";
/// The AWS text of a batch entry id that is empty, over 80 characters, or has
/// a character outside letters, digits, `-` and `_`.
const INVALID_BATCH_ENTRY_ID_MESSAGE: &str = "A batch entry id can only contain alphanumeric \
                                              characters, hyphens and underscores. It can be at \
                                              most 80 letters long.";

/// The batch total of body and attribute bytes (decision D8).
pub(crate) fn validate_batch_payload(
    entries: &[SendMessageBatchRequestEntry],
) -> Result<(), HttpApiError> {
    let total = entries
        .iter()
        .map(|entry| {
            crate::queue_send::message_payload_bytes(
                &entry.message_body,
                entry.message_attributes.as_ref(),
            )
        })
        .sum::<usize>();
    if total > MAX_BATCH_PAYLOAD_BYTES {
        return Err(HttpApiError::validation_error(format!(
            "{BATCH_REQUEST_TOO_LONG_PREFIX}{MAX_BATCH_PAYLOAD_BYTES} bytes. You have sent \
             {total} bytes."
        )));
    }
    Ok(())
}

/// The one SQS error type for a validation message. Validation reaches the
/// wire as text from several decoders, so the type follows the text.
#[must_use]
pub fn sqs_validation_error_type(message: &str) -> &'static str {
    if message.starts_with("Id ") && message.ends_with(" repeated.") {
        SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE
    } else if message.starts_with("The request must contain the parameter ") {
        SQS_MISSING_PARAMETER_ERROR_TYPE
    } else if message.starts_with(BATCH_REQUEST_TOO_LONG_PREFIX) {
        SQS_BATCH_REQUEST_TOO_LONG_ERROR_TYPE
    } else if message.starts_with("Invalid binary character '") {
        SQS_INVALID_MESSAGE_CONTENTS_ERROR_TYPE
    } else if message == INVALID_BATCH_ENTRY_ID_MESSAGE {
        SQS_INVALID_BATCH_ENTRY_ID_ERROR_TYPE
    } else {
        SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE
    }
}

fn queue_validation(result: Result<(), HttpApiError>) -> QueueResult<()> {
    result.map_err(queue_validation_error)
}

/// A validation text as a queue error; `InvalidAttributeValue` keeps its kind.
pub(crate) fn queue_validation_error(error: HttpApiError) -> QueueError {
    let kind = if error.error_type == SQS_INVALID_ATTRIBUTE_VALUE_ERROR_TYPE {
        crate::QueueValidationKind::InvalidAttributeValue
    } else {
        crate::QueueValidationKind::InvalidParameterValue
    };
    QueueError::validation_with_detail(kind, error.message)
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

    if message_body.len() > MAX_MAXIMUM_MESSAGE_SIZE_BYTES as usize {
        return Err(HttpApiError::validation_error(
            crate::queue_send::message_too_long(MAX_MAXIMUM_MESSAGE_SIZE_BYTES as usize),
        ));
    }

    if let Some(character) = message_body.chars().find(|ch| !is_xml_char(*ch)) {
        return Err(HttpApiError::validation_error(format!(
            "Invalid binary character '#x{:X}' was found in the message body, the set of allowed \
             characters is #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF].",
            u32::from(character)
        )));
    }

    Ok(())
}

fn is_xml_char(ch: char) -> bool {
    matches!(u32::from(ch),
        0x9 | 0xA | 0xD | 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x10000..=0x0010_FFFF)
}

/// The XML 1.0 character rule of the message body. Every value that a Query
/// response writes as text obeys it, else the response is not XML.
pub(crate) fn is_xml_text(value: &str) -> bool {
    value.chars().all(is_xml_char)
}

/// A value that a Query response returns: the body's character rule and a
/// size bound. Tags, `Policy`, and the bounds are not probed.
pub(crate) fn validate_returned_text(
    parameter: &str,
    value: &str,
    max_bytes: usize,
) -> Result<(), HttpApiError> {
    if !is_xml_text(value) {
        return Err(HttpApiError::validation_error(format!(
            "{parameter} contains characters that are not allowed."
        )));
    }
    if value.len() > max_bytes {
        return Err(HttpApiError::validation_error(format!(
            "{parameter} must be at most {max_bytes} bytes."
        )));
    }
    Ok(())
}

pub(crate) fn validate_delay_seconds(delay_seconds: i64) -> Result<(), HttpApiError> {
    if !(0..=i64::from(MAX_DELAY_SECONDS)).contains(&delay_seconds) {
        return Err(HttpApiError::validation_error(format!(
            "Value {delay_seconds} for parameter DelaySeconds is invalid. Reason: DelaySeconds \
             must be >= 0 and <= {MAX_DELAY_SECONDS}."
        )));
    }

    Ok(())
}

pub(crate) fn validate_max_number_of_messages(max_messages: i64) -> Result<(), HttpApiError> {
    let limits = PaginationLimit::new(1, MAX_RECEIVE_MESSAGES);
    u32::try_from(max_messages)
        .ok()
        .and_then(|max_messages| limits.validate(max_messages).ok())
        .ok_or_else(|| {
            HttpApiError::validation_error(format!(
                "Value {max_messages} for parameter MaxNumberOfMessages is invalid. Reason: Must \
                 be between {} and {}, if provided.",
                limits.min_limit(),
                limits.max_limit(),
            ))
        })?;
    Ok(())
}

pub(crate) fn validate_visibility_timeout(visibility_timeout: i64) -> Result<(), HttpApiError> {
    if !(0..=i64::from(MAX_VISIBILITY_TIMEOUT_SECONDS)).contains(&visibility_timeout) {
        return Err(HttpApiError::validation_error(format!(
            "Value {visibility_timeout} for parameter VisibilityTimeout is invalid. Reason: \
             VisibilityTimeout must be an integer between 0 and {MAX_VISIBILITY_TIMEOUT_SECONDS}."
        )));
    }

    Ok(())
}

pub(crate) fn validate_wait_time_seconds(wait_time: i64) -> Result<(), HttpApiError> {
    if !(0..=i64::from(MAX_WAIT_TIME_SECONDS)).contains(&wait_time) {
        return Err(HttpApiError::validation_error(format!(
            "Value {wait_time} for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and \
             <= {MAX_WAIT_TIME_SECONDS}, if provided."
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

/// The create and set rule for each queue attribute (W14 plan section 6.2):
/// stored values read back byte for byte; an ignored value that would change
/// delivery semantics stays a rejection.
pub(crate) fn validate_queue_attributes(attributes: &HashMap<String, String>) -> QueueResult<()> {
    for (key, value) in attributes {
        // Suite script: a computed attribute is as unknown as any other name.
        if READ_ONLY_QUEUE_ATTRIBUTES.contains(&key.as_str())
            || !QUEUE_ATTRIBUTE_NAMES.contains(&key.as_str())
            || key == "All"
        {
            return Err(unknown_attribute(key));
        }
        // An empty `RedrivePolicy` or `Policy` removes it (probe 9).
        if value.is_empty() && REMOVABLE_QUEUE_ATTRIBUTES.contains(&key.as_str()) {
            continue;
        }
        if value.is_empty() {
            return queue_validation(Err(HttpApiError::validation_error(
                "Attribute value cannot be empty",
            )));
        }
        queue_validation(validate_queue_attribute(key, value))?;
    }
    Ok(())
}

/// Names that `GetQueueAttributes` computes; create and set reject them.
const READ_ONLY_QUEUE_ATTRIBUTES: &[&str] = &[
    "QueueArn",
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible",
    "ApproximateNumberOfMessagesDelayed",
    "CreatedTimestamp",
    "LastModifiedTimestamp",
];

const REMOVABLE_QUEUE_ATTRIBUTES: &[&str] = &[QUEUE_REDRIVE_POLICY_ATTRIBUTE, "Policy"];

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
        "KmsDataKeyReusePeriodSeconds" => validate_numeric_attribute(
            key,
            value,
            MIN_KMS_DATA_KEY_REUSE_SECONDS,
            MAX_KMS_DATA_KEY_REUSE_SECONDS,
        ),
        QUEUE_REDRIVE_POLICY_ATTRIBUTE => value.parse::<crate::RedrivePolicy>().map(|_| ()),
        // Stored verbatim and not enforced; it must be a JSON object.
        "Policy" if !is_xml_text(value) => validate_returned_text(key, value, usize::MAX),
        "Policy" => match serde_json::from_str::<serde_json::Value>(value) {
            Ok(serde_json::Value::Object(_)) => Ok(()),
            _ => Err(invalid_attribute_value(key)),
        },
        // Stored and returned; Aux does not encrypt.
        "KmsMasterKeyId" => validate_returned_text(key, value, MAX_KMS_MASTER_KEY_ID_BYTES),
        "SqsManagedSseEnabled" if matches!(value, "true" | "false") => Ok(()),
        "SqsManagedSseEnabled" => Err(HttpApiError::validation_error(format!(
            "Invalid value for the parameter SqsManagedSseEnabled. Reason: {value} is not true or \
             false."
        ))),
        "FifoQueue"
        | "ContentBasedDeduplication"
        | "DeduplicationScope"
        | "FifoThroughputLimit" => Err(HttpApiError::validation_error(
            "FIFO queue attributes are not supported",
        )),
        // An ignored allow policy would let any queue redrive here.
        _ => Err(HttpApiError::validation_error(format!(
            "{key} is not supported"
        ))),
    }
}

fn validate_numeric_attribute(
    key: &str,
    value: &str,
    min: u32,
    max: u32,
) -> Result<(), HttpApiError> {
    match value.parse::<u32>() {
        Ok(parsed) if (min..=max).contains(&parsed) => Ok(()),
        _ => Err(invalid_attribute_value(key)),
    }
}

/// Suite scripts: a queue attribute outside its range, or not a number, is
/// `InvalidAttributeValue` "Invalid value for the parameter X.".
fn invalid_attribute_value(key: &str) -> HttpApiError {
    HttpApiError::aws_query_error(
        SQS_INVALID_ATTRIBUTE_VALUE_ERROR_TYPE,
        format!("Invalid value for the parameter {key}."),
        400,
    )
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
            "Number of message attributes [{}] exceeds the allowed maximum \
             [{MAX_MESSAGE_ATTRIBUTES}].",
            message_attributes.len()
        )));
    }

    for (key, value) in message_attributes {
        validate_message_attribute_name(key)?;
        value.validate_named(key)?;
    }

    Ok(())
}

/// Suite scripts: the exact text of each message attribute name rule.
fn validate_message_attribute_name(key: &str) -> Result<(), HttpApiError> {
    let lower = key.to_ascii_lowercase();
    if lower.starts_with("aws.") || lower.starts_with("amazon.") {
        return Err(HttpApiError::validation_error(
            "You can't use message attribute names beginning with 'AWS.' or 'Amazon'. These \
             strings are reserved for internal use.",
        ));
    }
    if key.starts_with('.') {
        return Err(HttpApiError::validation_error(
            "Message (user) attribute name must not begin with character '.'.",
        ));
    }
    if key.ends_with('.') {
        return Err(HttpApiError::validation_error(
            "Message (user) attribute name must not end with character '.'.",
        ));
    }
    if key.contains("..") {
        return Err(HttpApiError::validation_error(
            "Message (user) attribute name must not have successive '.' characters.",
        ));
    }
    if let Some(ch) = key
        .chars()
        .find(|ch| !ch.is_ascii_alphanumeric() && !matches!(ch, '_' | '-' | '.'))
    {
        return Err(HttpApiError::validation_error(format!(
            "Message (user) attribute name contains invalid non-alphanumeric character '#x{:X}'. \
             You can use only alphanumeric characters, hyphens (-), underscores (_), or periods \
             (.).",
            u32::from(ch)
        )));
    }
    if key.is_empty() || key.len() > 256 {
        return Err(HttpApiError::validation_error(
            "Message (user) attribute name must be shorter than 256 bytes.",
        ));
    }
    Ok(())
}

pub(crate) fn validate_batch_entries<'a>(
    entry_ids: impl IntoIterator<Item = &'a str>,
) -> Result<(), HttpApiError> {
    let mut seen = std::collections::HashSet::new();

    for id in entry_ids {
        if id.is_empty()
            || id.len() > 80
            || !id
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
        {
            return Err(HttpApiError::validation_error(
                INVALID_BATCH_ENTRY_ID_MESSAGE,
            ));
        }
        if !seen.insert(id) {
            return Err(HttpApiError::validation_error(format!("Id {id} repeated.")));
        }
    }

    Ok(())
}

/// Probe 5: an empty or over-long batch has its own SQS codes.
fn validate_batch_count(entry_name: &str, count: usize) -> QueueResult<()> {
    if count == 0 {
        return Err(QueueError::batch_entry(
            true,
            "AWS.SimpleQueueService.EmptyBatchRequest".to_owned(),
            format!("There should be at least one {entry_name} in the request."),
        ));
    }
    if count > 10 {
        return Err(QueueError::batch_entry(
            true,
            "AWS.SimpleQueueService.TooManyEntriesInBatchRequest".to_owned(),
            format!("Maximum number of entries per request are 10. You have sent {count}."),
        ));
    }
    Ok(())
}
