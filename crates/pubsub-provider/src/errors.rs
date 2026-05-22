use http_error::HttpApiError;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubsubValidationKind {
    InvalidTopicName,
    InvalidTopicArn,
    InvalidSubscriptionArn,
    InvalidEndpoint,
    UnsupportedProtocol,
    UnsupportedAttribute,
    EmptyMessage,
    InvalidToken,
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
        }
    }

    #[must_use]
    pub const fn aws_query_error_type(self) -> &'static str {
        SNS_INVALID_PARAMETER_ERROR_TYPE
    }

    #[must_use]
    pub fn aws_query_message(self, detail: Option<&str>) -> String {
        const COMPAT_PRODUCT_NAME: &str = concat!("Amazon ", "SN", "S");

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
        }
    }
}

pub const SNS_NOT_FOUND_ERROR_TYPE: &str = "NotFound";
pub const SNS_INVALID_PARAMETER_ERROR_TYPE: &str = "InvalidParameter";
pub const SNS_INTERNAL_ERROR_TYPE: &str = "InternalError";

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

    #[error("Internal server error: {}", kind.as_code())]
    Internal {
        kind: PubsubInternalKind,
        detail: Option<String>,
    },

    #[error("Serialization error: {0}")]
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

    pub fn subscription_not_found(subscription_arn: impl Into<String>) -> Self {
        Self::ResourceNotFound {
            resource_type: "subscription",
            resource_id: subscription_arn.into(),
        }
    }

    #[must_use]
    pub fn aws_query_error_type(&self) -> &'static str {
        match self {
            Self::ResourceNotFound { .. } => SNS_NOT_FOUND_ERROR_TYPE,
            Self::Validation { kind, .. } => kind.aws_query_error_type(),
            Self::Internal { .. } | Self::Serialization(_) => SNS_INTERNAL_ERROR_TYPE,
        }
    }

    #[must_use]
    pub fn aws_query_message(&self) -> String {
        match self {
            Self::ResourceNotFound { resource_type, .. } => match *resource_type {
                "topic" => "Topic does not exist".to_string(),
                "subscription" => "Subscription does not exist".to_string(),
                _ => self.to_string(),
            },
            Self::Validation { kind, detail } => kind.aws_query_message(detail.as_deref()),
            Self::Internal { .. } | Self::Serialization(_) => self.to_string(),
        }
    }

    #[must_use]
    pub const fn aws_query_status_code(&self) -> u16 {
        match self {
            Self::Internal { .. } | Self::Serialization(_) => 500,
            Self::ResourceNotFound { .. } | Self::Validation { .. } => 400,
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
        HttpApiError::aws_query_error(
            error.aws_query_error_type(),
            error.aws_query_message(),
            error.aws_query_status_code(),
        )
    }
}
