use http_error::{AwsQueryAnswer, AwsQueryInternal, HttpApiError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::{
    COMPAT_PRODUCT_NAME, SNS_INTERNAL_ERROR_TYPE, SNS_INVALID_PARAMETER_ERROR_TYPE,
    SNS_NOT_FOUND_ERROR_TYPE, SNS_RESOURCE_NOT_FOUND_ERROR_TYPE,
};

/// The `resource_type` of a missing tagging target.
const TAGGED_RESOURCE: &str = "resource";

/// The SNS 500 answer of a storage failure (wire text unchanged).
const SNS_STORAGE_INTERNAL: AwsQueryInternal = AwsQueryInternal {
    error_type: SNS_INTERNAL_ERROR_TYPE,
    message: "Internal server error: storage",
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PubsubValidationKind {
    InvalidTopicName,
    InvalidTopicArn,
    InvalidSubscriptionArn,
    InvalidEndpoint,
    UnsupportedProtocol,
    UnsupportedAttribute,
    EmptyMessage,
    InvalidToken,
    InvalidMessageAttribute,
    MessageTooLarge,
    InvalidSubject,
    InvalidSqsEndpoint,
    /// A supported attribute or parameter with an invalid value; the detail
    /// is the AWS reason text.
    InvalidAttributeValue,
    ProtocolNotImplemented,
    SubscriptionExists,
    TopicExists,
    /// A required batch member is absent; the detail is its request path.
    MissingBatchMember,
    TooManyEntriesInBatchRequest,
    /// The request names `PublishBatchRequestEntries` with an empty list
    /// (an absent parameter is `MissingBatchMember`).
    EmptyBatchRequest,
    BatchEntryIdsNotDistinct,
    /// The detail is the entry `Id`.
    BatchEntryIdTooLong,
    /// The detail is the entry `Id`.
    BatchEntryIdCharacter,
    BatchRequestTooLong,
    /// The detail is the attribute name.
    InvalidMessageAttributeType,
    /// A message attribute rule with its own text; the detail is the whole
    /// AWS message.
    MessageAttributeRule,
    /// The detail is the token.
    InvalidNextToken,
    /// A batch member field name that is not `<index>.<name>`; the detail
    /// is the field name.
    InvalidBatchMember,
}

impl PubsubValidationKind {
    #[must_use]
    pub const fn as_code(self) -> &'static str {
        match self {
            Self::InvalidTopicName => "invalid_topic_name",
            Self::InvalidTopicArn => "invalid_topic_arn",
            Self::InvalidSubscriptionArn => "invalid_subscription_arn",
            Self::InvalidEndpoint => "invalid_endpoint",
            Self::UnsupportedProtocol => "unsupported_protocol",
            Self::UnsupportedAttribute => "unsupported_attribute",
            Self::EmptyMessage => "empty_message",
            Self::InvalidToken => "invalid_token",
            Self::InvalidMessageAttribute => "invalid_message_attribute",
            Self::MessageTooLarge => "message_too_large",
            Self::InvalidSubject => "invalid_subject",
            Self::InvalidSqsEndpoint => "invalid_sqs_endpoint",
            Self::InvalidAttributeValue => "invalid_attribute_value",
            Self::ProtocolNotImplemented => "protocol_not_implemented",
            Self::SubscriptionExists => "subscription_exists",
            Self::TopicExists => "topic_exists",
            Self::MissingBatchMember => "missing_batch_member",
            Self::TooManyEntriesInBatchRequest => "too_many_entries_in_batch_request",
            Self::EmptyBatchRequest => "empty_batch_request",
            Self::BatchEntryIdsNotDistinct => "batch_entry_ids_not_distinct",
            Self::BatchEntryIdTooLong => "batch_entry_id_too_long",
            Self::BatchEntryIdCharacter => "batch_entry_id_character",
            Self::BatchRequestTooLong => "batch_request_too_long",
            Self::InvalidMessageAttributeType => "invalid_message_attribute_type",
            Self::MessageAttributeRule => "message_attribute_rule",
            Self::InvalidNextToken => "invalid_next_token",
            Self::InvalidBatchMember => "invalid_batch_member",
        }
    }

    #[must_use]
    pub const fn aws_query_error_type(self) -> &'static str {
        match self {
            Self::MissingBatchMember | Self::InvalidBatchMember => "ValidationError",
            Self::TooManyEntriesInBatchRequest => "TooManyEntriesInBatchRequest",
            Self::EmptyBatchRequest => "EmptyBatchRequest",
            Self::BatchEntryIdsNotDistinct => "BatchEntryIdsNotDistinct",
            Self::BatchEntryIdTooLong | Self::BatchEntryIdCharacter => "InvalidBatchEntryId",
            Self::BatchRequestTooLong => "BatchRequestTooLong",
            Self::InvalidMessageAttributeType | Self::MessageAttributeRule => {
                "ParameterValueInvalid"
            }
            _ => SNS_INVALID_PARAMETER_ERROR_TYPE,
        }
    }

    #[must_use]
    pub fn aws_query_message(self, detail: Option<&str>) -> String {
        match self {
            Self::InvalidTopicName => "Invalid parameter: Topic Name".to_string(),
            Self::InvalidTopicArn => "Invalid parameter: TopicArn Reason: An ARN must have at \
                                      least 6 elements, not 1"
                .to_string(),
            Self::InvalidSubscriptionArn => "Invalid parameter: SubscriptionArn Reason: An ARN \
                                             must have at least 6 elements, not 1"
                .to_string(),
            Self::InvalidEndpoint => "Invalid parameter: Endpoint".to_string(),
            Self::UnsupportedProtocol => format!(
                "Invalid parameter: {COMPAT_PRODUCT_NAME} does not support this protocol string: \
                 {}",
                detail.unwrap_or_default()
            ),
            Self::UnsupportedAttribute => "Invalid parameter: AttributeName".to_string(),
            Self::EmptyMessage => "Invalid parameter: Empty message".to_string(),
            Self::InvalidToken => "Invalid token".to_string(),
            Self::InvalidMessageAttribute => "Invalid parameter: MessageAttributes".to_string(),
            Self::MessageTooLarge => "Invalid parameter: Message too long".to_string(),
            Self::InvalidSubject => "Invalid parameter: Subject".to_string(),
            Self::InvalidSqsEndpoint => "Invalid parameter: SQS endpoint ARN".to_string(),
            Self::InvalidAttributeValue => {
                format!("Invalid parameter: {}", detail.unwrap_or_default())
            }
            Self::ProtocolNotImplemented => format!(
                "Invalid parameter: Protocol Reason: {} is not supported by this service",
                detail.unwrap_or_default()
            ),
            Self::SubscriptionExists => "Invalid parameter: Attributes Reason: Subscription \
                                         already exists with different attributes"
                .to_string(),
            Self::TopicExists => "Invalid parameter: Attributes Reason: Topic already exists with \
                                  different attributes"
                .to_string(),
            // Texts from AWS run 2, probe A7.
            Self::MissingBatchMember => format!(
                "1 validation error detected: Value null at '{}' failed to satisfy constraint: \
                 Member must not be null",
                detail.unwrap_or_default()
            ),
            Self::TooManyEntriesInBatchRequest => {
                "The batch request contains more entries than permissible.".to_string()
            }
            Self::EmptyBatchRequest => "The batch request doesn't contain any entries.".to_string(),
            Self::BatchEntryIdsNotDistinct => {
                "Two or more batch entries in the request have the same Id.".to_string()
            }
            Self::BatchEntryIdTooLong => format!(
                "The Id of a batch entry in the batch request is too long: {}",
                detail.unwrap_or_default()
            ),
            Self::BatchEntryIdCharacter => format!(
                "The Id of a batch entry in the batch request contains an impermissible \
                 character: {}",
                detail.unwrap_or_default()
            ),
            Self::BatchRequestTooLong => {
                "The length of all the messages put together is more than the limit.".to_string()
            }
            Self::InvalidMessageAttributeType => format!(
                "The message attribute '{}' has an invalid message attribute type, the set of \
                 supported type prefixes is Binary, Number, and String.",
                detail.unwrap_or_default()
            ),
            Self::MessageAttributeRule => detail.unwrap_or_default().to_string(),
            // Not probed: AWS tokens are opaque (design 8.4, probe A11).
            Self::InvalidNextToken => "Invalid parameter: NextToken".to_string(),
            // Not probed.
            Self::InvalidBatchMember => format!(
                "1 validation error detected: Value at '{}' failed to satisfy constraint: Member \
                 name is invalid",
                detail.unwrap_or_default()
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubsubInternalKind {
    LockPoisoned,
    Storage,
}

impl PubsubInternalKind {
    #[must_use]
    pub const fn as_code(self) -> &'static str {
        match self {
            Self::LockPoisoned => "lock_poisoned",
            Self::Storage => "storage",
        }
    }
}

#[derive(Debug, Error)]
pub enum PubsubError {
    #[error("Requested resource not found: {resource_type}: {resource_id} not found")]
    ResourceNotFound {
        resource_type: &'static str,
        resource_id: String,
    },

    #[error("Validation error: {}", kind.as_code())]
    Validation {
        kind: PubsubValidationKind,
        detail: Option<String>,
    },

    #[error("Delivery claim lost: {delivery_id}")]
    DeliveryClaimLost { delivery_id: String },

    #[error("Internal server error: {}", kind.as_code())]
    Internal {
        kind: PubsubInternalKind,
        detail: Option<String>,
    },

    #[error("storage: {0}")]
    Storage(#[from] storage_types::StorageError),

    #[error("Serialization error: {:?} at {}:{}", .0.classify(), .0.line(), .0.column())]
    Serialization(#[from] serde_json::Error),
}

impl PubsubError {
    #[must_use]
    pub fn validation(kind: PubsubValidationKind) -> Self {
        Self::Validation { kind, detail: None }
    }

    pub fn validation_with_detail(
        kind: PubsubValidationKind,
        detail: impl std::fmt::Display,
    ) -> Self {
        Self::Validation {
            kind,
            detail: Some(detail.to_string()),
        }
    }

    #[must_use]
    pub fn internal(kind: PubsubInternalKind) -> Self {
        Self::Internal { kind, detail: None }
    }

    pub fn storage(detail: impl std::fmt::Display) -> Self {
        Self::Internal {
            kind: PubsubInternalKind::Storage,
            detail: Some(detail.to_string()),
        }
    }

    pub fn topic_not_found(topic_arn: impl Into<String>) -> Self {
        Self::ResourceNotFound {
            resource_type: "topic",
            resource_id: topic_arn.into(),
        }
    }

    /// A tagging call on a topic that is gone (`ResourceNotFound`, suite
    /// script `list_tags_for_nonexistent_topic_fails`).
    pub fn tagged_resource_not_found(resource_arn: impl Into<String>) -> Self {
        Self::ResourceNotFound {
            resource_type: TAGGED_RESOURCE,
            resource_id: resource_arn.into(),
        }
    }

    pub fn subscription_not_found(subscription_arn: impl Into<String>) -> Self {
        Self::ResourceNotFound {
            resource_type: "subscription",
            resource_id: subscription_arn.into(),
        }
    }

    #[must_use]
    pub fn delivery_claim_lost(delivery_id: impl Into<String>) -> Self {
        Self::DeliveryClaimLost {
            delivery_id: delivery_id.into(),
        }
    }

    /// The storage failure this error carries.
    #[must_use]
    pub fn storage_error(&self) -> Option<&storage_types::StorageError> {
        match self {
            Self::Storage(error) => Some(error),
            Self::ResourceNotFound { .. }
            | Self::Validation { .. }
            | Self::DeliveryClaimLost { .. }
            | Self::Internal { .. }
            | Self::Serialization(_) => None,
        }
    }

    /// The SNS answer of a storage failure, chosen by its class (ARCH-2-f):
    /// the same rule as SQS.
    #[must_use]
    pub fn storage_answer(&self) -> Option<AwsQueryAnswer> {
        self.storage_error()
            .map(|error| AwsQueryAnswer::for_storage(error, SNS_STORAGE_INTERNAL))
    }

    #[must_use]
    pub fn aws_query_error_type(&self) -> &'static str {
        match self {
            Self::Storage(error) => {
                AwsQueryAnswer::for_storage(error, SNS_STORAGE_INTERNAL).error_type
            }
            Self::ResourceNotFound {
                resource_type: TAGGED_RESOURCE,
                ..
            } => SNS_RESOURCE_NOT_FOUND_ERROR_TYPE,
            Self::ResourceNotFound { .. } => SNS_NOT_FOUND_ERROR_TYPE,
            Self::Validation { kind, .. } => kind.aws_query_error_type(),
            Self::DeliveryClaimLost { .. } | Self::Internal { .. } | Self::Serialization(_) => {
                SNS_INTERNAL_ERROR_TYPE
            }
        }
    }

    #[must_use]
    pub fn aws_query_message(&self) -> String {
        match self {
            Self::Storage(error) => AwsQueryAnswer::for_storage(error, SNS_STORAGE_INTERNAL)
                .message
                .to_owned(),
            Self::ResourceNotFound { resource_type, .. } => match *resource_type {
                "topic" => "Topic does not exist".to_string(),
                "subscription" => "Subscription does not exist".to_string(),
                TAGGED_RESOURCE => "Resource does not exist".to_string(),
                _ => self.to_string(),
            },
            Self::Validation { kind, detail } => kind.aws_query_message(detail.as_deref()),
            Self::DeliveryClaimLost { .. } | Self::Internal { .. } => self.to_string(),
            Self::Serialization(_) => "Internal server error: serialization".to_owned(),
        }
    }

    #[must_use]
    pub fn aws_query_status_code(&self) -> u16 {
        match self {
            Self::Storage(error) => {
                AwsQueryAnswer::for_storage(error, SNS_STORAGE_INTERNAL).status_code
            }
            Self::DeliveryClaimLost { .. } | Self::Internal { .. } | Self::Serialization(_) => 500,
            // Probe run 1: AWS answers a missing topic with HTTP 404.
            Self::ResourceNotFound { .. } => 404,
            Self::Validation { .. } => 400,
        }
    }
}

pub type PubsubResult<T> = Result<T, PubsubError>;

impl From<PubsubError> for HttpApiError {
    fn from(error: PubsubError) -> Self {
        Self::from(&error)
    }
}

impl From<&PubsubError> for HttpApiError {
    fn from(error: &PubsubError) -> Self {
        if let Some(answer) = error.storage_answer() {
            return answer.into_http();
        }
        HttpApiError::aws_query_error(
            error.aws_query_error_type(),
            error.aws_query_message(),
            error.aws_query_status_code(),
        )
    }
}
