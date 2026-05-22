use std::{collections::HashMap, marker::PhantomData};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use storage_types::TimestampMillis;
use utoipa::ToSchema;

use crate::{ReceiptHandle, newtypes::MessageId};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateQueueResponse {
    #[serde(rename = "QueueUrl")]
    #[schema(example = "http://127.0.0.1:3000/queue/000000000000/MyQueue")]
    pub queue_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteQueueRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct DeleteQueueResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct ListQueuesRequest {
    #[serde(rename = "QueueNamePrefix", skip_serializing_if = "Option::is_none")]
    pub queue_name_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListQueuesResponse {
    #[serde(rename = "QueueUrls")]
    pub queue_urls: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetQueueUrlRequest {
    #[serde(rename = "QueueName")]
    pub queue_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetQueueUrlResponse {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetQueueAttributesRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,

    #[serde(rename = "AttributeNames", skip_serializing_if = "Option::is_none")]
    pub attribute_names: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetQueueAttributesResponse {
    #[serde(rename = "Attributes")]
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SetQueueAttributesRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,

    #[serde(rename = "Attributes")]
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct SetQueueAttributesResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PurgeQueueRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct PurgeQueueResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SendMessageRequest {
    #[serde(rename = "QueueUrl")]
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
    pub delay_seconds: Option<u32>,

    #[serde(rename = "MessageAttributes", skip_serializing_if = "Option::is_none")]
    pub message_attributes: Option<HashMap<String, MessageAttributeValue>>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SendMessageBatchRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,

    #[serde(rename = "Entries")]
    pub entries: Vec<SendMessageBatchRequestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SendMessageBatchRequestEntry {
    #[serde(rename = "Id")]
    pub id: String,

    #[serde(rename = "MessageBody")]
    pub message_body: String,

    #[serde(rename = "DelaySeconds", skip_serializing_if = "Option::is_none")]
    pub delay_seconds: Option<u32>,

    #[serde(rename = "MessageAttributes", skip_serializing_if = "Option::is_none")]
    pub message_attributes: Option<HashMap<String, MessageAttributeValue>>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReceiveMessageRequest {
    #[serde(rename = "QueueUrl")]
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
    pub max_number_of_messages: Option<u32>,

    #[serde(rename = "VisibilityTimeout", skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 0, maximum = 43_200, example = 30)]
    // Keep in sync with MAX_VISIBILITY_TIMEOUT_SECONDS.
    pub visibility_timeout: Option<u32>,

    #[serde(rename = "WaitTimeSeconds", skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 0, maximum = 20, example = 0)]
    // Keep in sync with MAX_WAIT_TIME_SECONDS.
    pub wait_time_seconds: Option<u32>,

    #[serde(rename = "AttributeNames", skip_serializing_if = "Option::is_none")]
    pub attribute_names: Option<Vec<String>>,

    #[serde(
        rename = "MessageAttributeNames",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_attribute_names: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReceiveMessageResponse {
    #[serde(rename = "Messages")]
    #[schema(max_items = 10)] // Keep in sync with MAX_RECEIVE_MESSAGES.
    pub messages: Vec<MessageResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteMessageRequest {
    #[serde(rename = "QueueUrl")]
    #[schema(
        min_length = 1,
        example = "http://127.0.0.1:3000/queue/000000000000/MyQueue"
    )]
    pub queue_url: String,

    #[serde(rename = "ReceiptHandle")]
    #[schema(value_type = String, min_length = 36, max_length = 36)]
    pub receipt_handle: ReceiptHandle,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteMessageBatchRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,

    #[serde(rename = "Entries")]
    pub entries: Vec<DeleteMessageBatchRequestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeMessageVisibilityRequest {
    #[serde(rename = "QueueUrl")]
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
    pub visibility_timeout: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeMessageVisibilityBatchRequest {
    #[serde(rename = "QueueUrl")]
    pub queue_url: String,

    #[serde(rename = "Entries")]
    pub entries: Vec<ChangeMessageVisibilityBatchRequestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeMessageVisibilityBatchRequestEntry {
    #[serde(rename = "Id")]
    pub id: String,

    #[serde(rename = "ReceiptHandle")]
    #[schema(value_type = String)]
    pub receipt_handle: ReceiptHandle,

    #[serde(rename = "VisibilityTimeout")]
    pub visibility_timeout: u32,
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueueMessage {
    pub message_id: MessageId,
    pub queue_url: String,
    pub body: String,
    pub message_attributes: Option<HashMap<String, MessageAttributeValue>>,
    pub receipt_handle: Option<ReceiptHandle>,
    pub created_at: TimestampMillis,
    pub visibility_timestamp: Option<TimestampMillis>,
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
        // Messages can reappear after a visibility timeout with a prior receipt handle.
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
        let receipt_handle = self.inner.receipt_handle.clone()?;
        Some(MessageResponse::from_message(self.inner, &receipt_handle))
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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
    pub fn from_message(msg: QueueMessage, receipt_handle: &ReceiptHandle) -> Self {
        let md5_of_body = format!("{:x}", md5::compute(msg.body.as_bytes()));
        let md5_of_message_attributes = msg
            .message_attributes
            .as_ref()
            .and_then(md5_of_message_attributes);
        let attributes = Some(HashMap::from([(
            "SentTimestamp".to_string(),
            msg.created_at.timestamp_millis().to_string(),
        )]));
        Self {
            message_id: msg.message_id.to_string(),
            receipt_handle: receipt_handle.to_string(),
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
    let mut encoded = Vec::new();
    let mut names: Vec<_> = attributes.keys().collect();
    names.sort();

    for name in names {
        let value = attributes.get(name)?;
        encode_queue_attribute_string(&mut encoded, name);
        encode_queue_attribute_string(&mut encoded, &value.data_type);
        if value.data_type.starts_with("Binary") {
            encoded.push(2);
            let binary_value = value.binary_value.as_ref()?;
            let decoded = STANDARD.decode(binary_value).ok()?;
            encode_queue_attribute_binary(&mut encoded, &decoded);
        } else {
            encoded.push(1);
            let string_value = value.string_value.as_ref()?;
            encode_queue_attribute_string(&mut encoded, string_value);
        }
    }

    Some(format!("{:x}", md5::compute(encoded)))
}

fn encode_queue_attribute_string(output: &mut Vec<u8>, value: &str) {
    encode_queue_attribute_binary(output, value.as_bytes());
}

fn encode_queue_attribute_binary(output: &mut Vec<u8>, value: &[u8]) {
    let length = value.len().min(u32::MAX as usize) as u32;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Queue {
    pub queue_name: String,
    pub queue_url: String,
    pub attributes: HashMap<String, String>,
    pub created_at: TimestampMillis,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueMessageCounts {
    pub visible: u64,
    pub not_visible: u64,
    pub delayed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageCheckpoint {
    pub message_id: MessageId,
    pub checkpoint_data: String,
    pub updated_at: TimestampMillis,
}
