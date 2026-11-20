use http_error::HttpApiError;
use serde::de::DeserializeOwned;

use crate::{
    ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResponse,
    ChangeMessageVisibilityRequest, CreateQueueRequest, CreateQueueResponse,
    DeleteMessageBatchRequest, DeleteMessageBatchResponse, DeleteMessageRequest,
    DeleteQueueRequest, GetQueueAttributesRequest, GetQueueAttributesResponse, GetQueueUrlRequest,
    GetQueueUrlResponse, ListDeadLetterSourceQueuesRequest, ListDeadLetterSourceQueuesResponse,
    ListQueueTagsRequest, ListQueueTagsResponse, ListQueuesRequest, ListQueuesResponse,
    PublishMessageRequest, PurgeQueueRequest, QueueResult, ReceiveMessageRequest,
    ReceiveMessageResponse, SQS_INVALID_ACTION_ERROR_TYPE, SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
    SendMessageBatchRequest, SendMessageBatchResponse, SendMessageRequest, SendMessageResponse,
    SetQueueAttributesRequest, TagQueueRequest, UntagQueueRequest,
    request_validation::QueueRequestValidation,
};

pub(crate) const JSON_CONTENT_TYPE: &str = "application/x-amz-json-1.0";
pub(crate) const QUERY_CONTENT_TYPE: &str = "application/x-www-form-urlencoded";
pub(crate) const JSON_TARGET_PREFIX: &str = "AmazonSQS.";

/// The two SQS protocols. JSON is `application/x-amz-json-1.0` with an
/// `X-Amz-Target` header; Query is a form body with an `Action` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueProtocol {
    Json,
    Query,
}

impl QueueProtocol {
    /// The protocol of a request body. A body that is not a form is answered
    /// in JSON.
    #[must_use]
    pub fn of(content_type: Option<&str>) -> Self {
        if content_type.is_some_and(|value| value.starts_with(QUERY_CONTENT_TYPE)) {
            Self::Query
        } else {
            Self::Json
        }
    }
}

/// The HTTP parts of one SQS request that the wire owner reads.
#[derive(Clone, Copy)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct WireRequestParts<'a> {
    pub content_type: Option<&'a str>,
    pub target: Option<&'a str>,
    pub body: &'a [u8],
    /// The queue URL that the request was posted to. It is the `QueueUrl`
    /// member when the body has none; a body `QueueUrl` wins (not probed).
    pub queue_url: Option<&'a str>,
}

#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct QueueWireRequest {
    pub protocol: QueueProtocol,
    pub request: QueueRequest,
}

/// A request that failed before dispatch, with the protocol of its answer.
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct QueueWireRejection {
    pub protocol: QueueProtocol,
    pub error: HttpApiError,
}

/// One encoded SQS response. The host copies it to HTTP without change.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct WireResponse {
    pub status: u16,
    pub content_type: &'static str,
    /// Lower-case names: `x-amzn-requestid`, `x-amzn-query-error`,
    /// `retry-after`.
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
}

/// Decode and validate one request. The answer to a rejection uses the
/// protocol of the request body.
pub fn decode_wire_request(
    parts: WireRequestParts<'_>,
) -> Result<QueueWireRequest, QueueWireRejection> {
    let protocol = QueueProtocol::of(parts.content_type);
    let request = match protocol {
        QueueProtocol::Json if is_json_content_type(parts.content_type) => {
            super::json::decode(parts.target, parts.body, parts.queue_url)
        }
        QueueProtocol::Json => Err(HttpApiError::aws_query_error(
            SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
            format!(
                "The content type {} is not supported.",
                parts.content_type.unwrap_or_default()
            ),
            400,
        )),
        QueueProtocol::Query => super::query::decode(parts.body, parts.queue_url),
    };
    request
        .map(|request| QueueWireRequest { protocol, request })
        .map_err(|error| QueueWireRejection {
            protocol,
            error: sqs_decode_error(error),
        })
}

/// A decoder's generic validation error takes the SQS type of its text.
fn sqs_decode_error(error: HttpApiError) -> HttpApiError {
    if error.error_type != HttpApiError::validation_error("").error_type {
        return error;
    }
    let error_type = crate::sqs_validation_error_type(&error.message);
    HttpApiError::aws_query_error(error_type, error.message, 400)
}

#[must_use]
pub fn encode_success(
    protocol: QueueProtocol,
    request_id: &str,
    response: &QueueResponse,
) -> WireResponse {
    let mut encoded = match protocol {
        QueueProtocol::Json => super::json::encode_success(response),
        QueueProtocol::Query => super::query_xml::encode_success(request_id, response),
    };
    encoded
        .headers
        .push(("x-amzn-requestid", request_id.to_owned()));
    encoded
}

/// The error shape of both protocols. The `x-amzn-query-error` code is the
/// full stored error type; the fault is `Sender` for 4xx and `Receiver` for
/// 5xx.
#[must_use]
pub fn encode_error(
    protocol: QueueProtocol,
    request_id: &str,
    error: &HttpApiError,
) -> WireResponse {
    let fault = if error.status_code >= 500 {
        "Receiver"
    } else {
        "Sender"
    };
    let mut response = match protocol {
        QueueProtocol::Json => super::json::encode_error(error),
        QueueProtocol::Query => super::query_xml::encode_error(request_id, error, fault),
    };
    response
        .headers
        .push(("x-amzn-requestid", request_id.to_owned()));
    response.headers.push((
        "x-amzn-query-error",
        format!("{};{fault}", error.error_type),
    ));
    for (name, value) in &error.response_headers {
        if name.eq_ignore_ascii_case("retry-after") {
            response.headers.push(("retry-after", value.clone()));
        }
    }
    response
}

pub(crate) fn is_json_content_type(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|value| value.starts_with(JSON_CONTENT_TYPE))
}

/// A typed decode error. A missing required member is 400 `MissingParameter`
/// with the AWS text (AWS documentation); serde names the member only in
/// its message, so the member is read from it. A member of a batch entry
/// is named with its entry, such as `DeleteMessageBatchRequestEntry.2.Id`
/// (review L2). Other errors are 400 `InvalidParameterValue`.
pub(crate) fn members_error(action: QueueAction, error: &MembersError) -> HttpApiError {
    let text = error.inner().to_string();
    if let Some(member) = text
        .strip_prefix("missing field `")
        .and_then(|rest| rest.split('`').next())
    {
        let entry = error.path().iter().find_map(|segment| match segment {
            serde_path_to_error::Segment::Seq { index } => Some(index + 1),
            _ => None,
        });
        let member = match entry {
            Some(entry) => format!("{}RequestEntry.{entry}.{member}", action.name()),
            None => member.to_owned(),
        };
        return HttpApiError::aws_query_error(
            crate::SQS_MISSING_PARAMETER_ERROR_TYPE,
            format!("The request must contain the parameter {member}."),
            400,
        );
    }
    // The serde text names Rust types; the client gets only the class.
    HttpApiError::aws_query_error(
        SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
        "A parameter has a value of the wrong type or format.",
        400,
    )
}

/// 400 `InvalidAction` for a missing or unknown action name.
pub(crate) fn invalid_action(name: Option<&str>) -> HttpApiError {
    let message = name.map_or_else(
        || "The request must contain an action.".to_owned(),
        |name| format!("The action {name} is not valid for this endpoint."),
    );
    HttpApiError::aws_query_error(SQS_INVALID_ACTION_ERROR_TYPE, message, 400)
}

/// A request whose fields passed `QueueRequestValidation`.
#[derive(Debug)]
pub struct ValidatedQueueRequest<T>(T);

impl<T: QueueRequestValidation> ValidatedQueueRequest<T> {
    pub fn new(request: T) -> QueueResult<Self> {
        request.validate_request()?;
        Ok(Self(request))
    }
}

impl<T> ValidatedQueueRequest<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for ValidatedQueueRequest<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

macro_rules! queue_actions {
    ($($action:ident($request:ty)),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum QueueAction {
            $($action,)+
        }

        impl QueueAction {
            #[must_use]
            pub fn from_name(name: &str) -> Option<Self> {
                match name {
                    $(stringify!($action) => Some(Self::$action),)+
                    _ => None,
                }
            }

            #[must_use]
            pub const fn name(self) -> &'static str {
                match self {
                    $(Self::$action => stringify!($action),)+
                }
            }
        }

        #[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
        pub enum QueueRequest {
            $($action(ValidatedQueueRequest<$request>),)+
        }

        impl QueueRequest {
            #[must_use]
            pub const fn action(&self) -> QueueAction {
                match self {
                    $(Self::$action(_) => QueueAction::$action,)+
                }
            }

            /// The `QueueUrl` member of a request that names one queue.
            pub fn queue_url(&self) -> Option<&str> {
                queue_url_of(self)
            }

            /// Decode the members of `action` into its typed request and
            /// validate it once.
            pub(crate) fn decode(action: QueueAction, members: impl RequestMembers) -> Result<Self, HttpApiError> {
                match action {
                    $(QueueAction::$action => {
                        let request = members
                            .into_request::<$request>()
                            .map_err(|error| members_error(action, &error))?;
                        Ok(Self::$action(validated(request)?))
                    })+
                }
            }
        }
    };
}

queue_actions!(
    CreateQueue(CreateQueueRequest),
    DeleteQueue(DeleteQueueRequest),
    ListQueues(ListQueuesRequest),
    GetQueueUrl(GetQueueUrlRequest),
    GetQueueAttributes(GetQueueAttributesRequest),
    SetQueueAttributes(SetQueueAttributesRequest),
    PurgeQueue(PurgeQueueRequest),
    SendMessage(SendMessageRequest),
    PublishMessage(PublishMessageRequest),
    SendMessageBatch(SendMessageBatchRequest),
    ReceiveMessage(ReceiveMessageRequest),
    DeleteMessage(DeleteMessageRequest),
    DeleteMessageBatch(DeleteMessageBatchRequest),
    ChangeMessageVisibility(ChangeMessageVisibilityRequest),
    ChangeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest),
    ListDeadLetterSourceQueues(ListDeadLetterSourceQueuesRequest),
    TagQueue(TagQueueRequest),
    UntagQueue(UntagQueueRequest),
    ListQueueTags(ListQueueTagsRequest),
);

/// The members of one request in one protocol.
pub(crate) trait RequestMembers {
    fn into_request<T: DeserializeOwned>(self) -> Result<T, MembersError>;
}

/// A typed decode error and the member path where it happened.
pub(crate) type MembersError = serde_path_to_error::Error<serde_json::Error>;

fn validated<T: QueueRequestValidation>(
    request: T,
) -> Result<ValidatedQueueRequest<T>, HttpApiError> {
    ValidatedQueueRequest::new(request).map_err(HttpApiError::from)
}

fn queue_url_of(request: &QueueRequest) -> Option<&str> {
    match request {
        QueueRequest::CreateQueue(_)
        | QueueRequest::ListQueues(_)
        | QueueRequest::GetQueueUrl(_) => None,
        QueueRequest::DeleteQueue(request) => Some(&request.0.queue_url),
        QueueRequest::GetQueueAttributes(request) => Some(&request.0.queue_url),
        QueueRequest::SetQueueAttributes(request) => Some(&request.0.queue_url),
        QueueRequest::PurgeQueue(request) => Some(&request.0.queue_url),
        QueueRequest::SendMessage(request) => Some(&request.0.queue_url),
        QueueRequest::PublishMessage(request) => Some(&request.0.message.queue_url),
        QueueRequest::SendMessageBatch(request) => Some(&request.0.queue_url),
        QueueRequest::ReceiveMessage(request) => Some(&request.0.queue_url),
        QueueRequest::DeleteMessage(request) => Some(&request.0.queue_url),
        QueueRequest::DeleteMessageBatch(request) => Some(&request.0.queue_url),
        QueueRequest::ChangeMessageVisibility(request) => Some(&request.0.queue_url),
        QueueRequest::ChangeMessageVisibilityBatch(request) => Some(&request.0.queue_url),
        QueueRequest::ListDeadLetterSourceQueues(request) => Some(&request.0.queue_url),
        QueueRequest::TagQueue(request) => Some(&request.0.queue_url),
        QueueRequest::UntagQueue(request) => Some(&request.0.queue_url),
        QueueRequest::ListQueueTags(request) => Some(&request.0.queue_url),
    }
}

/// One success result per `QueueAction`. Actions with no result carry
/// nothing.
#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub enum QueueResponse {
    CreateQueue(CreateQueueResponse),
    DeleteQueue,
    ListQueues(ListQueuesResponse),
    GetQueueUrl(GetQueueUrlResponse),
    GetQueueAttributes(GetQueueAttributesResponse),
    SetQueueAttributes,
    PurgeQueue,
    SendMessage(SendMessageResponse),
    PublishMessage(SendMessageResponse),
    SendMessageBatch(SendMessageBatchResponse),
    ReceiveMessage(ReceiveMessageResponse),
    DeleteMessage,
    DeleteMessageBatch(DeleteMessageBatchResponse),
    ChangeMessageVisibility,
    ChangeMessageVisibilityBatch(ChangeMessageVisibilityBatchResponse),
    ListDeadLetterSourceQueues(ListDeadLetterSourceQueuesResponse),
    TagQueue,
    UntagQueue,
    ListQueueTags(ListQueueTagsResponse),
}

impl QueueResponse {
    #[must_use]
    pub const fn action(&self) -> QueueAction {
        match self {
            Self::CreateQueue(_) => QueueAction::CreateQueue,
            Self::DeleteQueue => QueueAction::DeleteQueue,
            Self::ListQueues(_) => QueueAction::ListQueues,
            Self::GetQueueUrl(_) => QueueAction::GetQueueUrl,
            Self::GetQueueAttributes(_) => QueueAction::GetQueueAttributes,
            Self::SetQueueAttributes => QueueAction::SetQueueAttributes,
            Self::PurgeQueue => QueueAction::PurgeQueue,
            Self::SendMessage(_) => QueueAction::SendMessage,
            Self::PublishMessage(_) => QueueAction::PublishMessage,
            Self::SendMessageBatch(_) => QueueAction::SendMessageBatch,
            Self::ReceiveMessage(_) => QueueAction::ReceiveMessage,
            Self::DeleteMessage => QueueAction::DeleteMessage,
            Self::DeleteMessageBatch(_) => QueueAction::DeleteMessageBatch,
            Self::ChangeMessageVisibility => QueueAction::ChangeMessageVisibility,
            Self::ChangeMessageVisibilityBatch(_) => QueueAction::ChangeMessageVisibilityBatch,
            Self::ListDeadLetterSourceQueues(_) => QueueAction::ListDeadLetterSourceQueues,
            Self::TagQueue => QueueAction::TagQueue,
            Self::UntagQueue => QueueAction::UntagQueue,
            Self::ListQueueTags(_) => QueueAction::ListQueueTags,
        }
    }
}
