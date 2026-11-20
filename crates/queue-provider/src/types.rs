use std::{
    collections::{BTreeMap, HashMap},
    marker::PhantomData,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use storage_types::TimestampMillis;
use utoipa::ToSchema;

use crate::{ReceiptHandle, constants::TRACE_HEADER_ATTRIBUTE, newtypes::MessageId};

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct CreateQueueRequest {
    #[serde(rename = "QueueName")]
    #[schema(
        min_length = 1,
        max_length = 80,
        pattern = "^[a-zA-Z0-9_-]+$",
        example = "MyQueue"
    )]
    pub queue_name: String,

    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, String>>,

    /// SQS spells this member with a lower-case first letter.
    #[serde(rename = "tags", skip_serializing_if = "Option::is_none")]
    pub tags: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateQueueResponse {
    #[serde(rename = "QueueUrl")]
    #[schema(example = "http://127.0.0.1:3000/queue/000000000000/MyQueue")]
    pub queue_url: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct DeleteQueueRequest {
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct DeleteQueueResponse {}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ListQueuesRequest {
    #[serde(rename = "QueueNamePrefix", skip_serializing_if = "Option::is_none")]
    pub queue_name_prefix: Option<String>,

    /// 1 to 1,000; 0 or absent lists up to 1,000 queues with no `NextToken`.
    #[serde(rename = "MaxResults", skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 0, maximum = 1000)] // Keep in sync with MAX_LIST_QUEUES_RESULTS.
    #[serde(default, deserialize_with = "crate::integer_member::optional")]
    pub max_results: Option<i64>,

    #[serde(rename = "NextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListQueuesResponse {
    #[serde(rename = "QueueUrls")]
    pub queue_urls: Vec<String>,

    /// Present only when `MaxResults` was given and more queues remain.
    #[serde(rename = "NextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ListDeadLetterSourceQueuesRequest {
    /// The dead-letter queue.
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,

    #[serde(rename = "MaxResults", skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1, maximum = 1000)] // Keep in sync with MAX_DEAD_LETTER_SOURCE_RESULTS.
    #[serde(default, deserialize_with = "crate::integer_member::optional")]
    pub max_results: Option<i64>,

    /// The last URL of the previous page.
    #[serde(rename = "NextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListDeadLetterSourceQueuesResponse {
    /// SQS spells this field with a lower-case first letter.
    #[serde(rename = "queueUrls")]
    pub queue_urls: Vec<String>,

    #[serde(rename = "NextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TagQueueRequest {
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,

    #[serde(rename = "Tags")]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct UntagQueueRequest {
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,

    #[serde(rename = "TagKeys")]
    pub tag_keys: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ListQueueTagsRequest {
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ListQueueTagsResponse {
    #[serde(rename = "Tags", skip_serializing_if = "BTreeMap::is_empty", default)]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct GetQueueUrlRequest {
    #[serde(rename = "QueueName")]
    pub queue_name: String,

    /// Another account than the configured one names no queue.
    #[serde(
        rename = "QueueOwnerAWSAccountId",
        skip_serializing_if = "Option::is_none"
    )]
    pub queue_owner_aws_account_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetQueueUrlResponse {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct GetQueueAttributesRequest {
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,

    #[serde(rename = "AttributeNames", skip_serializing_if = "Option::is_none")]
    pub attribute_names: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetQueueAttributesResponse {
    /// Probe 7: no selected attribute gives `{}`, with no `Attributes` member.
    #[serde(
        rename = "Attributes",
        default,
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct SetQueueAttributesRequest {
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,

    #[serde(rename = "Attributes")]
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct SetQueueAttributesResponse {}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct PurgeQueueRequest {
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct PurgeQueueResponse {}

#[derive(Clone, Default, Serialize, Deserialize, ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct SendMessageRequest {
    #[serde(rename = "QueueUrl", default)]
    #[schema(
        min_length = 1,
        example = "http://127.0.0.1:3000/queue/000000000000/MyQueue"
    )]
    pub queue_url: String,

    #[serde(rename = "MessageBody")]
    #[schema(min_length = 1, max_length = 1_048_576, example = "Hello, World!")]
    pub message_body: String,

    #[serde(rename = "DelaySeconds", skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 0, maximum = 900, example = 0)] // Keep in sync with MAX_DELAY_SECONDS.
    #[serde(default, deserialize_with = "crate::integer_member::optional")]
    pub delay_seconds: Option<i64>,

    #[serde(rename = "MessageAttributes", skip_serializing_if = "Option::is_none")]
    pub message_attributes: Option<HashMap<String, MessageAttributeValue>>,

    /// FIFO only: a standard queue rejects it (probe 12).
    /// Only `AWSTraceHeader` (a `String`) exists.
    #[serde(
        rename = "MessageSystemAttributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_system_attributes: Option<HashMap<String, MessageAttributeValue>>,

    #[serde(
        rename = "MessageDeduplicationId",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_deduplication_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SendMessageResponse {
    #[serde(rename = "MessageId")]
    #[schema(value_type = String, example = "5fea77560ea4451aa703a558b933e274")]
    pub message_id: MessageId,

    #[serde(rename = "MD5OfMessageBody")]
    #[schema(example = "b25c02566801a9c07a892c18fe3ac6b7")]
    pub md5_of_body: String,

    #[serde(
        rename = "MD5OfMessageAttributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub md5_of_message_attributes: Option<String>,

    #[serde(
        rename = "MD5OfMessageSystemAttributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub md5_of_message_system_attributes: Option<String>,
}

#[derive(Clone, Default, Serialize, Deserialize, ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct SendMessageBatchRequest {
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,

    #[serde(rename = "Entries")]
    pub entries: Vec<SendMessageBatchRequestEntry>,
}

#[derive(Clone, Default, Serialize, Deserialize, ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct SendMessageBatchRequestEntry {
    #[serde(rename = "Id")]
    pub id: String,

    #[serde(rename = "MessageBody")]
    pub message_body: String,

    #[serde(rename = "DelaySeconds", skip_serializing_if = "Option::is_none")]
    #[serde(default, deserialize_with = "crate::integer_member::optional")]
    pub delay_seconds: Option<i64>,

    #[serde(rename = "MessageAttributes", skip_serializing_if = "Option::is_none")]
    pub message_attributes: Option<HashMap<String, MessageAttributeValue>>,

    /// Only `AWSTraceHeader` (a `String`) exists.
    #[serde(
        rename = "MessageSystemAttributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_system_attributes: Option<HashMap<String, MessageAttributeValue>>,

    #[serde(
        rename = "MessageDeduplicationId",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_deduplication_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SendMessageBatchResponse {
    #[serde(rename = "Successful")]
    pub successful: Vec<SendMessageBatchResultEntry>,

    #[serde(rename = "Failed")]
    pub failed: Vec<BatchResultErrorEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SendMessageBatchResultEntry {
    #[serde(rename = "Id")]
    pub id: String,

    #[serde(rename = "MessageId")]
    #[schema(value_type = String)]
    pub message_id: MessageId,

    #[serde(rename = "MD5OfMessageBody")]
    pub md5_of_message_body: String,

    #[serde(
        rename = "MD5OfMessageAttributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub md5_of_message_attributes: Option<String>,

    #[serde(
        rename = "MD5OfMessageSystemAttributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub md5_of_message_system_attributes: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(from = "ReceiveMessageMembers")]
pub struct ReceiveMessageRequest {
    #[serde(rename = "QueueUrl", default)]
    #[schema(
        min_length = 1,
        example = "http://127.0.0.1:3000/queue/000000000000/MyQueue"
    )]
    pub queue_url: String,

    #[serde(
        rename = "MaxNumberOfMessages",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(minimum = 1, maximum = 10, example = 1)]
    // Keep in sync with MAX_RECEIVE_MESSAGES.
    #[serde(default, deserialize_with = "crate::integer_member::optional")]
    pub max_number_of_messages: Option<i64>,

    #[serde(rename = "VisibilityTimeout", skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 0, maximum = 43_200, example = 30)]
    // Keep in sync with MAX_VISIBILITY_TIMEOUT_SECONDS.
    #[serde(default, deserialize_with = "crate::integer_member::optional")]
    pub visibility_timeout: Option<i64>,

    #[serde(rename = "WaitTimeSeconds", skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 0, maximum = 20, example = 0)]
    // Keep in sync with MAX_WAIT_TIME_SECONDS.
    #[serde(default, deserialize_with = "crate::integer_member::optional")]
    pub wait_time_seconds: Option<i64>,

    #[serde(
        rename = "MessageSystemAttributeNames",
        skip_serializing_if = "Option::is_none"
    )]
    pub attribute_names: Option<Vec<String>>,

    #[serde(
        rename = "MessageAttributeNames",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_attribute_names: Option<Vec<String>>,
}

/// The decoded `ReceiveMessage` members. AWS accepts the deprecated
/// `AttributeNames` together with `MessageSystemAttributeNames` and selects
/// their union (probe 12); a single field with an alias rejects both.
#[derive(Deserialize)]
struct ReceiveMessageMembers {
    #[serde(rename = "QueueUrl", default)]
    queue_url: String,
    #[serde(rename = "MaxNumberOfMessages")]
    #[serde(default, deserialize_with = "crate::integer_member::optional")]
    max_number_of_messages: Option<i64>,
    #[serde(rename = "VisibilityTimeout")]
    #[serde(default, deserialize_with = "crate::integer_member::optional")]
    visibility_timeout: Option<i64>,
    #[serde(rename = "WaitTimeSeconds")]
    #[serde(default, deserialize_with = "crate::integer_member::optional")]
    wait_time_seconds: Option<i64>,
    #[serde(rename = "AttributeNames")]
    attribute_names: Option<Vec<String>>,
    #[serde(rename = "MessageSystemAttributeNames")]
    message_system_attribute_names: Option<Vec<String>>,
    #[serde(rename = "MessageAttributeNames")]
    message_attribute_names: Option<Vec<String>>,
}

impl From<ReceiveMessageMembers> for ReceiveMessageRequest {
    fn from(members: ReceiveMessageMembers) -> Self {
        let attribute_names = match (
            members.attribute_names,
            members.message_system_attribute_names,
        ) {
            (Some(mut names), Some(system)) => {
                for name in system {
                    if !names.contains(&name) {
                        names.push(name);
                    }
                }
                Some(names)
            }
            (names, system) => names.or(system),
        };
        Self {
            queue_url: members.queue_url,
            max_number_of_messages: members.max_number_of_messages,
            visibility_timeout: members.visibility_timeout,
            wait_time_seconds: members.wait_time_seconds,
            attribute_names,
            message_attribute_names: members.message_attribute_names,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct ReceiveMessageResponse {
    #[serde(rename = "Messages")]
    #[schema(max_items = 10)] // Keep in sync with MAX_RECEIVE_MESSAGES.
    pub messages: Vec<MessageResponse>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct DeleteMessageRequest {
    #[serde(rename = "QueueUrl", default)]
    #[schema(
        min_length = 1,
        example = "http://127.0.0.1:3000/queue/000000000000/MyQueue"
    )]
    pub queue_url: String,

    #[serde(rename = "ReceiptHandle")]
    #[schema(value_type = String, min_length = 36, max_length = 36)]
    pub receipt_handle: ReceiptHandle,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct DeleteMessageBatchRequest {
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,

    #[serde(rename = "Entries")]
    pub entries: Vec<DeleteMessageBatchRequestEntry>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct DeleteMessageBatchRequestEntry {
    #[serde(rename = "Id")]
    pub id: String,

    #[serde(rename = "ReceiptHandle")]
    #[schema(value_type = String)]
    pub receipt_handle: ReceiptHandle,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteMessageBatchResponse {
    #[serde(rename = "Successful")]
    pub successful: Vec<DeleteMessageBatchResultEntry>,

    #[serde(rename = "Failed")]
    pub failed: Vec<BatchResultErrorEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteMessageBatchResultEntry {
    #[serde(rename = "Id")]
    pub id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ChangeMessageVisibilityRequest {
    #[serde(rename = "QueueUrl", default)]
    #[schema(
        min_length = 1,
        example = "http://127.0.0.1:3000/queue/000000000000/MyQueue"
    )]
    pub queue_url: String,

    #[serde(rename = "ReceiptHandle")]
    #[schema(
        value_type = String,
        min_length = 1,
        example = "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a..."
    )]
    pub receipt_handle: ReceiptHandle,

    #[serde(rename = "VisibilityTimeout")]
    #[schema(minimum = 0, maximum = 43_200, example = 60)]
    // Keep in sync with MAX_VISIBILITY_TIMEOUT_SECONDS.
    #[serde(deserialize_with = "crate::integer_member::required")]
    pub visibility_timeout: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ChangeMessageVisibilityBatchRequest {
    #[serde(rename = "QueueUrl", default)]
    pub queue_url: String,

    #[serde(rename = "Entries")]
    pub entries: Vec<ChangeMessageVisibilityBatchRequestEntry>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ChangeMessageVisibilityBatchRequestEntry {
    #[serde(rename = "Id")]
    pub id: String,

    #[serde(rename = "ReceiptHandle")]
    #[schema(value_type = String)]
    pub receipt_handle: ReceiptHandle,

    #[serde(rename = "VisibilityTimeout")]
    #[serde(deserialize_with = "crate::integer_member::required")]
    pub visibility_timeout: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeMessageVisibilityBatchResponse {
    #[serde(rename = "Successful")]
    pub successful: Vec<ChangeMessageVisibilityBatchResultEntry>,

    #[serde(rename = "Failed")]
    pub failed: Vec<BatchResultErrorEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeMessageVisibilityBatchResultEntry {
    #[serde(rename = "Id")]
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BatchResultErrorEntry {
    #[serde(rename = "Id")]
    pub id: String,

    #[serde(rename = "SenderFault")]
    pub sender_fault: bool,

    #[serde(rename = "Code")]
    pub code: String,

    #[serde(rename = "Message")]
    pub message: String,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct UpdateMessageSnapshotRequest {
    #[schema(
        min_length = 1,
        example = "http://127.0.0.1:3000/queue/000000000000/MyQueue"
    )]
    pub queue_url: String,
    #[schema(value_type = String, example = "5fea7756-0ea4-451a-a703-a558b933e274")]
    pub receipt_handle: String,
    #[schema(example = "checkpoint_data_example")]
    pub checkpoint_data: String,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct Message {
    #[serde(rename = "MessageId")]
    #[schema(value_type = String, example = "5fea77560ea4451aa703a558b933e274")]
    pub message_id: MessageId,

    #[serde(rename = "ReceiptHandle")]
    #[schema(example = "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...")]
    pub receipt_handle: String,

    #[serde(rename = "Body")]
    #[schema(max_length = 1_048_576, example = "Hello, World!")]
    pub body: String,

    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, String>>,

    #[serde(rename = "MessageAttributes", skip_serializing_if = "Option::is_none")]
    pub message_attributes: Option<HashMap<String, MessageAttributeValue>>,
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct MessageAttributeValue {
    #[serde(rename = "StringValue", skip_serializing_if = "Option::is_none")]
    #[schema(example = "John Doe")]
    pub string_value: Option<String>,

    #[serde(rename = "BinaryValue", skip_serializing_if = "Option::is_none")]
    #[schema(example = "dGVzdA==")]
    pub binary_value: Option<String>,

    #[serde(rename = "DataType")]
    #[schema(
        min_length = 1,
        pattern = "^(String|Number|Binary)(\\..+)?$",
        example = "String"
    )]
    pub data_type: String,
}

#[derive(Clone, Serialize, Deserialize, Default)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct QueueMessage {
    /// Durable deliveries, incremented atomically with each visibility lease.
    #[serde(default)]
    pub receive_count: u32,
    pub message_id: MessageId,
    pub queue_url: String,
    pub body: String,
    /// Stored in name order, so that the bytes of one record are the same
    /// after each decode; ordered-key stores compare those bytes to detect a
    /// concurrent change.
    #[serde(serialize_with = "serialize_sorted_attributes")]
    pub message_attributes: Option<HashMap<String, MessageAttributeValue>>,
    pub receipt_handle: Option<ReceiptHandle>,
    pub created_at: TimestampMillis,
    pub visibility_timestamp: Option<TimestampMillis>,
    /// The `AWSTraceHeader` system attribute: immutable message content that
    /// a dead-letter move keeps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_header: Option<String>,
}

fn serialize_sorted_attributes<S: serde::Serializer>(
    attributes: &Option<HashMap<String, MessageAttributeValue>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    attributes
        .as_ref()
        .map(|attributes| {
            attributes
                .iter()
                .collect::<std::collections::BTreeMap<_, _>>()
        })
        .serialize(serializer)
}

/// A request integer after `validate_request`. Every validated range is
/// inside `u32`, so the fallback is never used.
#[must_use]
pub fn validated_u32(value: i64) -> u32 {
    u32::try_from(value).unwrap_or_default()
}

/// Build the provider message for one validated send, sharing the batch
/// timestamp. An omitted delay stays distinct from an explicit zero so the
/// queue's default delay can be applied inside the native send attempt.
#[must_use]
pub fn queue_message_for_send(
    request: SendMessageRequest,
    created_at: TimestampMillis,
) -> QueueMessage {
    let visibility_timestamp = request
        .delay_seconds
        .map(|seconds| created_at + storage_types::DurationSeconds::from(validated_u32(seconds)));
    QueueMessage {
        receive_count: 0,
        message_id: MessageId::default(),
        queue_url: request.queue_url,
        body: request.message_body,
        message_attributes: request.message_attributes.map(plain_exponent_numbers),
        receipt_handle: None,
        created_at,
        visibility_timestamp,
        trace_header: request
            .message_system_attributes
            .and_then(|mut attributes| attributes.remove(TRACE_HEADER_ATTRIBUTE))
            .filter(crate::request_validation::keeps_trace_header)
            .and_then(|value| value.string_value),
    }
}

/// Suite script: a Number written with an exponent is stored and read back in
/// plain notation; every other value stays as sent. The request validation has
/// already accepted each Number value.
fn plain_exponent_numbers(
    mut attributes: HashMap<String, MessageAttributeValue>,
) -> HashMap<String, MessageAttributeValue> {
    for value in attributes.values_mut() {
        if value.data_type.starts_with("Number")
            && let Some(text) = value.string_value.as_mut()
            && text.contains(['e', 'E'])
            && let Ok(plain) = storage_types::canonical_sqs_number(text)
        {
            *text = plain;
        }
    }
    attributes
}

pub struct Visible;
pub struct Invisible;
pub struct Deleted;

pub struct QueueMessageState<State> {
    inner: QueueMessage,
    _state: PhantomData<State>,
}

pub type VisibleQueueMessage = QueueMessageState<Visible>;
pub type InvisibleQueueMessage = QueueMessageState<Invisible>;
pub type DeletedQueueMessage = QueueMessageState<Deleted>;

impl VisibleQueueMessage {
    #[must_use]
    pub fn new(message: QueueMessage) -> Self {
        // Messages can reappear after a visibility timeout with a prior receipt
        // handle.
        Self {
            inner: message,
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn into_invisible(
        mut self,
        receipt_handle: ReceiptHandle,
        visibility_timestamp: TimestampMillis,
    ) -> InvisibleQueueMessage {
        self.inner.receipt_handle = Some(receipt_handle);
        self.inner.visibility_timestamp = Some(visibility_timestamp);
        InvisibleQueueMessage {
            inner: self.inner,
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn as_message(&self) -> &QueueMessage {
        &self.inner
    }
}

impl InvisibleQueueMessage {
    #[must_use]
    pub fn receipt_handle(&self) -> Option<&ReceiptHandle> {
        self.inner.receipt_handle.as_ref()
    }

    #[must_use]
    pub fn into_message_response(self) -> Option<MessageResponse> {
        let mut message = self.inner;
        let receipt_handle = message.receipt_handle.take()?;
        Some(MessageResponse::from_message(message, receipt_handle))
    }

    #[must_use]
    pub fn into_deleted(self) -> DeletedQueueMessage {
        DeletedQueueMessage {
            inner: self.inner,
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn as_message(&self) -> &QueueMessage {
        &self.inner
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct MessageResponse {
    #[serde(rename = "MessageId")]
    pub message_id: String,
    #[serde(rename = "ReceiptHandle")]
    pub receipt_handle: String,
    #[serde(rename = "MD5OfBody")]
    pub md5_of_body: String,
    #[serde(
        rename = "MD5OfMessageAttributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub md5_of_message_attributes: Option<String>,
    #[serde(rename = "Body")]
    pub body: String,
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, String>>,
    #[serde(rename = "MessageAttributes", skip_serializing_if = "Option::is_none")]
    pub message_attributes: Option<HashMap<String, MessageAttributeValue>>,
}

impl MessageResponse {
    #[must_use]
    pub fn from_message(msg: QueueMessage, receipt_handle: ReceiptHandle) -> Self {
        let md5_of_body = format!("{:x}", md5::compute(msg.body.as_bytes()));
        let md5_of_message_attributes = msg
            .message_attributes
            .as_ref()
            .and_then(md5_of_message_attributes);
        let mut attributes = HashMap::from([
            (
                "SentTimestamp".to_string(),
                msg.created_at.timestamp_millis().to_string(),
            ),
            (
                "ApproximateReceiveCount".to_string(),
                msg.receive_count.to_string(),
            ),
        ]);
        if let Some(trace_header) = msg.trace_header {
            attributes.insert(TRACE_HEADER_ATTRIBUTE.to_owned(), trace_header);
        }
        let attributes = Some(attributes);
        Self {
            message_id: msg.message_id.to_string(),
            receipt_handle: receipt_handle.0,
            md5_of_body,
            md5_of_message_attributes,
            body: msg.body,
            attributes,
            message_attributes: msg.message_attributes,
        }
    }
}

#[must_use]
pub fn md5_of_message_attributes(
    attributes: &HashMap<String, MessageAttributeValue>,
) -> Option<String> {
    let mut entries: Vec<_> = attributes.iter().collect();
    entries.sort_unstable_by_key(|(left, _)| *left);
    let mut context = md5::Context::new();

    for (name, value) in entries {
        encode_queue_attribute_string(&mut context, name);
        encode_queue_attribute_string(&mut context, &value.data_type);
        if value.data_type.starts_with("Binary") {
            context.consume([2]);
            let binary_value = value.binary_value.as_ref()?;
            let decoded = STANDARD.decode(binary_value).ok()?;
            encode_queue_attribute_binary(&mut context, &decoded);
        } else {
            context.consume([1]);
            let string_value = value.string_value.as_ref()?;
            encode_queue_attribute_string(&mut context, string_value);
        }
    }

    Some(format!("{:x}", context.finalize()))
}

/// `MD5OfMessageSystemAttributes` of a message whose only system attribute
/// is `AWSTraceHeader` (the message attribute MD5 algorithm).
#[must_use]
pub fn md5_of_trace_header(trace_header: &str) -> String {
    let mut context = md5::Context::new();
    encode_queue_attribute_string(&mut context, TRACE_HEADER_ATTRIBUTE);
    encode_queue_attribute_string(&mut context, "String");
    context.consume([1]);
    encode_queue_attribute_string(&mut context, trace_header);
    format!("{:x}", context.finalize())
}

fn encode_queue_attribute_string(output: &mut md5::Context, value: &str) {
    encode_queue_attribute_binary(output, value.as_bytes());
}

fn encode_queue_attribute_binary(output: &mut md5::Context, value: &[u8]) {
    let length = value.len().min(u32::MAX as usize) as u32;
    output.consume(length.to_be_bytes());
    output.consume(value);
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Queue {
    pub queue_name: String,
    pub queue_url: String,
    pub attributes: HashMap<String, String>,
    pub created_at: TimestampMillis,
    /// Set by create and by each attribute change (`LastModifiedTimestamp`).
    #[serde(default)]
    pub updated_at: TimestampMillis,
    /// Stored in the catalog row; `tag_rules` bounds the set.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct MessageCheckpoint {
    pub message_id: MessageId,
    pub checkpoint_data: String,
    pub updated_at: TimestampMillis,
}
