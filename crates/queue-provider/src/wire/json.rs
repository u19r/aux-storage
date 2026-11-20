//! The SQS JSON protocol (`application/x-amz-json-1.0`).

use http_error::HttpApiError;
use serde::{Serialize, de::DeserializeOwned};

use super::{
    JSON_CONTENT_TYPE, JSON_TARGET_PREFIX, QueueAction, QueueRequest, QueueResponse,
    RequestMembers, WireResponse, invalid_action,
};
use crate::sqs_json_error_type;

pub(super) fn decode(
    target: Option<&str>,
    body: &[u8],
    queue_url: Option<&str>,
) -> Result<QueueRequest, HttpApiError> {
    let name = target.and_then(|target| target.strip_prefix(JSON_TARGET_PREFIX));
    let action = name
        .and_then(QueueAction::from_name)
        .ok_or_else(|| invalid_action(name.or(target)))?;
    let Some(queue_url) = queue_url else {
        return QueueRequest::decode(action, JsonBody(body));
    };
    // Only a JSON request posted to a queue URL reads its body twice.
    let mut members: serde_json::Value = JsonBody(body)
        .into_request()
        .map_err(|error| super::members_error(action, &error))?;
    // `PublishMessage` names its queue inside `Message`.
    let target = if action == QueueAction::PublishMessage {
        members.get_mut("Message")
    } else {
        Some(&mut members)
    };
    if let Some(target) = target.and_then(serde_json::Value::as_object_mut) {
        target.entry("QueueUrl").or_insert_with(|| queue_url.into());
    }
    QueueRequest::decode(action, members)
}

/// A JSON body decodes straight into the typed request; unknown members are
/// ignored (D3).
struct JsonBody<'a>(&'a [u8]);

impl RequestMembers for JsonBody<'_> {
    /// A `null` member fails the direct decode; the body is then read as a
    /// value without its `null` members (review L1).
    fn into_request<T: DeserializeOwned>(self) -> Result<T, super::MembersError> {
        let mut body = serde_json::Deserializer::from_slice(self.0);
        serde_path_to_error::deserialize(&mut body).or_else(|error| {
            if !error.inner().to_string().starts_with("invalid type: null") {
                return Err(error);
            }
            let mut body = serde_json::Deserializer::from_slice(self.0);
            let members: serde_json::Value = serde_path_to_error::deserialize(&mut body)?;
            members.into_request()
        })
    }
}

pub(super) fn encode_success(response: &QueueResponse) -> WireResponse {
    let body = match response {
        QueueResponse::CreateQueue(result) => to_body(result),
        QueueResponse::ListQueues(result) => to_body(result),
        QueueResponse::GetQueueUrl(result) => to_body(result),
        QueueResponse::GetQueueAttributes(result) => to_body(result),
        QueueResponse::SendMessage(result) | QueueResponse::PublishMessage(result) => {
            to_body(result)
        }
        QueueResponse::SendMessageBatch(result) => to_body(result),
        QueueResponse::ReceiveMessage(result) => to_body(result),
        QueueResponse::DeleteMessageBatch(result) => to_body(result),
        QueueResponse::ChangeMessageVisibilityBatch(result) => to_body(result),
        QueueResponse::ListDeadLetterSourceQueues(result) => to_body(result),
        QueueResponse::ListQueueTags(result) => to_body(result),
        QueueResponse::DeleteQueue
        | QueueResponse::SetQueueAttributes
        | QueueResponse::PurgeQueue
        | QueueResponse::DeleteMessage
        | QueueResponse::ChangeMessageVisibility
        | QueueResponse::TagQueue
        | QueueResponse::UntagQueue => b"{}".to_vec(),
    };
    WireResponse {
        status: 200,
        content_type: JSON_CONTENT_TYPE,
        headers: Vec::new(),
        body,
    }
}

pub(super) fn encode_error(error: &HttpApiError) -> WireResponse {
    let body = serde_json::json!({
        "__type": sqs_json_error_type(&error.error_type),
        "message": error.message,
    });
    WireResponse {
        status: error.status_code,
        content_type: JSON_CONTENT_TYPE,
        headers: Vec::new(),
        body: body.to_string().into_bytes(),
    }
}

/// Response types hold only strings, numbers, and maps, so serialization
/// cannot fail.
fn to_body(value: &impl Serialize) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_default()
}
