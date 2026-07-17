use http_error::HttpApiError;
use serde::de::DeserializeOwned;

use crate::{
    ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityRequest, CreateQueueRequest,
    DeleteMessageBatchRequest, DeleteMessageRequest, DeleteQueueRequest, GetQueueAttributesRequest,
    GetQueueUrlRequest, ListQueuesRequest, PurgeQueueRequest, QueueRequestValidation, QueueResult,
    ReceiveMessageRequest, SendMessageBatchRequest, SendMessageRequest, SetQueueAttributesRequest,
};

#[derive(Debug)]
pub struct ValidatedQueueRequest<T>(T);

impl<T: QueueRequestValidation> ValidatedQueueRequest<T> {
    pub fn new(request: T) -> QueueResult<Self> {
        request.validate_request()?;
        Ok(Self(request))
    }
}

pub trait IntoValidatedQueueRequest<T> {
    fn into_validated(self) -> QueueResult<ValidatedQueueRequest<T>>;
}

impl<T: QueueRequestValidation> IntoValidatedQueueRequest<T> for T {
    fn into_validated(self) -> QueueResult<ValidatedQueueRequest<T>> {
        ValidatedQueueRequest::new(self)
    }
}

impl<T> IntoValidatedQueueRequest<T> for ValidatedQueueRequest<T> {
    fn into_validated(self) -> QueueResult<ValidatedQueueRequest<T>> {
        Ok(self)
    }
}

impl<T> ValidatedQueueRequest<T> {
    fn from_validated(request: T) -> Self {
        Self(request)
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueAction {
    CreateQueue,
    DeleteQueue,
    ListQueues,
    GetQueueUrl,
    GetQueueAttributes,
    SetQueueAttributes,
    PurgeQueue,
    SendMessage,
    SendMessageBatch,
    ReceiveMessage,
    DeleteMessage,
    DeleteMessageBatch,
    ChangeMessageVisibility,
    ChangeMessageVisibilityBatch,
}

impl QueueAction {
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "CreateQueue" => Some(Self::CreateQueue),
            "DeleteQueue" => Some(Self::DeleteQueue),
            "ListQueues" => Some(Self::ListQueues),
            "GetQueueUrl" => Some(Self::GetQueueUrl),
            "GetQueueAttributes" => Some(Self::GetQueueAttributes),
            "SetQueueAttributes" => Some(Self::SetQueueAttributes),
            "PurgeQueue" => Some(Self::PurgeQueue),
            "SendMessage" => Some(Self::SendMessage),
            "SendMessageBatch" => Some(Self::SendMessageBatch),
            "ReceiveMessage" => Some(Self::ReceiveMessage),
            "DeleteMessage" => Some(Self::DeleteMessage),
            "DeleteMessageBatch" => Some(Self::DeleteMessageBatch),
            "ChangeMessageVisibility" => Some(Self::ChangeMessageVisibility),
            "ChangeMessageVisibilityBatch" => Some(Self::ChangeMessageVisibilityBatch),
            _ => None,
        }
    }

    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::CreateQueue => "CreateQueue",
            Self::DeleteQueue => "DeleteQueue",
            Self::ListQueues => "ListQueues",
            Self::GetQueueUrl => "GetQueueUrl",
            Self::GetQueueAttributes => "GetQueueAttributes",
            Self::SetQueueAttributes => "SetQueueAttributes",
            Self::PurgeQueue => "PurgeQueue",
            Self::SendMessage => "SendMessage",
            Self::SendMessageBatch => "SendMessageBatch",
            Self::ReceiveMessage => "ReceiveMessage",
            Self::DeleteMessage => "DeleteMessage",
            Self::DeleteMessageBatch => "DeleteMessageBatch",
            Self::ChangeMessageVisibility => "ChangeMessageVisibility",
            Self::ChangeMessageVisibilityBatch => "ChangeMessageVisibilityBatch",
        }
    }
}

#[derive(Debug)]
pub enum QueueRequest {
    CreateQueue(ValidatedQueueRequest<CreateQueueRequest>),
    DeleteQueue(ValidatedQueueRequest<DeleteQueueRequest>),
    ListQueues(ValidatedQueueRequest<ListQueuesRequest>),
    GetQueueUrl(ValidatedQueueRequest<GetQueueUrlRequest>),
    GetQueueAttributes(ValidatedQueueRequest<GetQueueAttributesRequest>),
    SetQueueAttributes(ValidatedQueueRequest<SetQueueAttributesRequest>),
    PurgeQueue(ValidatedQueueRequest<PurgeQueueRequest>),
    SendMessage(ValidatedQueueRequest<SendMessageRequest>),
    SendMessageBatch(ValidatedQueueRequest<SendMessageBatchRequest>),
    ReceiveMessage(ValidatedQueueRequest<ReceiveMessageRequest>),
    DeleteMessage(ValidatedQueueRequest<DeleteMessageRequest>),
    DeleteMessageBatch(ValidatedQueueRequest<DeleteMessageBatchRequest>),
    ChangeMessageVisibility(ValidatedQueueRequest<ChangeMessageVisibilityRequest>),
    ChangeMessageVisibilityBatch(ValidatedQueueRequest<ChangeMessageVisibilityBatchRequest>),
}

impl QueueRequest {
    #[must_use]
    pub const fn action(&self) -> QueueAction {
        match self {
            Self::CreateQueue(_) => QueueAction::CreateQueue,
            Self::DeleteQueue(_) => QueueAction::DeleteQueue,
            Self::ListQueues(_) => QueueAction::ListQueues,
            Self::GetQueueUrl(_) => QueueAction::GetQueueUrl,
            Self::GetQueueAttributes(_) => QueueAction::GetQueueAttributes,
            Self::SetQueueAttributes(_) => QueueAction::SetQueueAttributes,
            Self::PurgeQueue(_) => QueueAction::PurgeQueue,
            Self::SendMessage(_) => QueueAction::SendMessage,
            Self::SendMessageBatch(_) => QueueAction::SendMessageBatch,
            Self::ReceiveMessage(_) => QueueAction::ReceiveMessage,
            Self::DeleteMessage(_) => QueueAction::DeleteMessage,
            Self::DeleteMessageBatch(_) => QueueAction::DeleteMessageBatch,
            Self::ChangeMessageVisibility(_) => QueueAction::ChangeMessageVisibility,
            Self::ChangeMessageVisibilityBatch(_) => QueueAction::ChangeMessageVisibilityBatch,
        }
    }
}

pub fn decode_json_request(action: QueueAction, body: &[u8]) -> Result<QueueRequest, HttpApiError> {
    macro_rules! decode {
        ($request:ty, $variant:ident) => {
            decode_json(body, <$request>::from_json).map(QueueRequest::$variant)
        };
    }

    match action {
        QueueAction::CreateQueue => decode!(CreateQueueRequest, CreateQueue),
        QueueAction::DeleteQueue => decode!(DeleteQueueRequest, DeleteQueue),
        QueueAction::ListQueues => decode!(ListQueuesRequest, ListQueues),
        QueueAction::GetQueueUrl => decode!(GetQueueUrlRequest, GetQueueUrl),
        QueueAction::GetQueueAttributes => {
            decode!(GetQueueAttributesRequest, GetQueueAttributes)
        }
        QueueAction::SetQueueAttributes => {
            decode!(SetQueueAttributesRequest, SetQueueAttributes)
        }
        QueueAction::PurgeQueue => decode!(PurgeQueueRequest, PurgeQueue),
        QueueAction::SendMessage => decode!(SendMessageRequest, SendMessage),
        QueueAction::SendMessageBatch => decode!(SendMessageBatchRequest, SendMessageBatch),
        QueueAction::ReceiveMessage => decode!(ReceiveMessageRequest, ReceiveMessage),
        QueueAction::DeleteMessage => decode!(DeleteMessageRequest, DeleteMessage),
        QueueAction::DeleteMessageBatch => {
            decode!(DeleteMessageBatchRequest, DeleteMessageBatch)
        }
        QueueAction::ChangeMessageVisibility => {
            decode!(ChangeMessageVisibilityRequest, ChangeMessageVisibility)
        }
        QueueAction::ChangeMessageVisibilityBatch => decode!(
            ChangeMessageVisibilityBatchRequest,
            ChangeMessageVisibilityBatch
        ),
    }
}

pub fn decode_value_request(
    action: QueueAction,
    value: serde_json::Value,
) -> Result<QueueRequest, HttpApiError> {
    macro_rules! decode {
        ($request:ty, $variant:ident) => {
            <$request>::from_json(value)
                .map(ValidatedQueueRequest::from_validated)
                .map(QueueRequest::$variant)
        };
    }

    match action {
        QueueAction::CreateQueue => decode!(CreateQueueRequest, CreateQueue),
        QueueAction::DeleteQueue => decode!(DeleteQueueRequest, DeleteQueue),
        QueueAction::ListQueues => decode!(ListQueuesRequest, ListQueues),
        QueueAction::GetQueueUrl => decode!(GetQueueUrlRequest, GetQueueUrl),
        QueueAction::GetQueueAttributes => decode!(GetQueueAttributesRequest, GetQueueAttributes),
        QueueAction::SetQueueAttributes => decode!(SetQueueAttributesRequest, SetQueueAttributes),
        QueueAction::PurgeQueue => decode!(PurgeQueueRequest, PurgeQueue),
        QueueAction::SendMessage => decode!(SendMessageRequest, SendMessage),
        QueueAction::SendMessageBatch => decode!(SendMessageBatchRequest, SendMessageBatch),
        QueueAction::ReceiveMessage => decode!(ReceiveMessageRequest, ReceiveMessage),
        QueueAction::DeleteMessage => decode!(DeleteMessageRequest, DeleteMessage),
        QueueAction::DeleteMessageBatch => decode!(DeleteMessageBatchRequest, DeleteMessageBatch),
        QueueAction::ChangeMessageVisibility => {
            decode!(ChangeMessageVisibilityRequest, ChangeMessageVisibility)
        }
        QueueAction::ChangeMessageVisibilityBatch => decode!(
            ChangeMessageVisibilityBatchRequest,
            ChangeMessageVisibilityBatch
        ),
    }
}

fn decode_json<T>(
    body: &[u8],
    legacy_decode: fn(serde_json::Value) -> Result<T, HttpApiError>,
) -> Result<ValidatedQueueRequest<T>, HttpApiError>
where
    T: DeserializeOwned + QueueRequestValidation,
{
    match serde_json::from_slice(body) {
        Ok(request) => ValidatedQueueRequest::new(request).map_err(HttpApiError::from),
        Err(error) if error.to_string().starts_with("duplicate field") => Err(
            HttpApiError::validation_error(format!("Invalid request format: {error}")),
        ),
        Err(_) => {
            let value = serde_json::from_slice(body)
                .map_err(|error| HttpApiError::validation_error(format!("invalid_json:{error}")))?;
            legacy_decode(value).map(ValidatedQueueRequest::from_validated)
        }
    }
}
