use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use storage_types::{
    AttributeMap, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, DYNAMODB_MISSING_AUTH_TOKEN_MESSAGE,
    DYNAMODB_REQUEST_LIMIT_EXCEEDED_MESSAGE, DYNAMODB_RESOURCE_IN_USE_MESSAGE,
    DYNAMODB_RESOURCE_NOT_FOUND_MESSAGE, DYNAMODB_THROTTLING_MESSAGE,
    DYNAMODB_TRANSACTION_CANCELED_MESSAGE, StorageEnum, StorageError, context::WrappedError as _,
};
use utoipa::ToSchema;

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    /// AWS-compatible JSON error field name. DynamoDB and SQS JSON protocol
    /// errors use `__type`. Machine-readable API error type.
    #[schema(example = "ValidationException")]
    #[serde(rename = "__type")]
    pub error_type: String,

    /// AWS-compatible JSON error field name. DynamoDB and SQS JSON protocol
    /// errors use `message`. Human-readable error message.
    #[schema(example = "One or more parameter values were invalid.")]
    #[serde(rename = "message", skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(
        rename = "CancellationReasons",
        skip_serializing_if = "Option::is_none"
    )]
    /// DynamoDB transaction cancellation reasons.
    #[schema(nullable = true)]
    pub cancellation_reasons: Option<Vec<DynamoDbCancellationReason>>,
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    /// DynamoDB item returned by ReturnValuesOnConditionCheckFailure.
    #[schema(nullable = true)]
    pub item: Option<AttributeMap>,
    #[serde(rename = "Message", skip_serializing_if = "Option::is_none")]
    /// DynamoDB transaction cancellation message field.
    #[schema(nullable = true)]
    pub transaction_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Request correlation identifier echoed in the response body.
    #[schema(nullable = true, example = "018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional documentation link for operator or developer remediation.
    #[schema(nullable = true, example = "/docs/explanations/error-code-catalog")]
    pub documentation_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional retry budget for rate-limited responses.
    #[schema(nullable = true, example = 2)]
    pub retry_after_seconds: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct DynamoDbCancellationReason {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<AttributeMap>,
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

pub trait IntoApiError {
    fn status_code(&self) -> u16;
    fn error_code(&self) -> Cow<'static, str>;
    fn error_message(&self) -> Cow<'static, str>;

    fn error_response(&self) -> ErrorResponse {
        ErrorResponse {
            error_type: self.error_code().into_owned(),
            message: self.error_message().into_owned(),
            transaction_message: None,
            cancellation_reasons: None,
            item: None,
            request_id: None,
            documentation_url: None,
            retry_after_seconds: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ValidationIssue {
    /// Machine-readable validation error code.
    #[schema(example = "invalid_request")]
    pub code: Cow<'static, str>,
    /// Field name that failed validation.
    #[schema(example = "slug")]
    pub field: Cow<'static, str>,
    /// JSON path for the invalid field.
    #[schema(example = "body.slug")]
    pub path: Cow<'static, str>,
    /// Human-readable validation message.
    #[schema(example = "slug must contain only lowercase letters, numbers, and hyphens")]
    pub message: Cow<'static, str>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ErrorEnvelope {
    /// Machine-readable error code.
    #[schema(example = "invalid_request")]
    pub code: Cow<'static, str>,
    /// Human-readable error message.
    #[schema(example = "Invalid request body")]
    pub message: Cow<'static, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional field associated with this error.
    #[schema(nullable = true, example = "slug")]
    pub field: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional JSON path associated with this error.
    #[schema(nullable = true, example = "body.slug")]
    pub path: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Request correlation identifier echoed in the response body.
    #[schema(nullable = true, example = "018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd")]
    pub request_id: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional documentation link for operator or developer remediation.
    #[schema(nullable = true, example = "/docs/explanations/error-code-catalog")]
    pub documentation_url: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional retry budget for rate-limited responses.
    #[schema(nullable = true, example = 2)]
    pub retry_after_seconds: Option<u64>,
}

impl ErrorEnvelope {
    #[must_use]
    pub fn new(code: impl Into<Cow<'static, str>>, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            field: None,
            path: None,
            request_id: None,
            documentation_url: None,
            retry_after_seconds: None,
        }
    }

    #[must_use]
    pub fn with_field_path(
        mut self,
        field: Option<impl Into<Cow<'static, str>>>,
        path: Option<impl Into<Cow<'static, str>>>,
    ) -> Self {
        self.field = field.map(Into::into);
        self.path = path.map(Into::into);
        self
    }
}

/// HTTP API error type that doesn't depend on Axum
#[derive(Debug, Clone)]
pub struct HttpApiError {
    pub error_type: String,
    pub message: String,
    pub status_code: u16,
    pub cancellation_reasons: Option<Vec<DynamoDbCancellationReason>>,
    pub item: Option<Box<AttributeMap>>,
    pub response_headers: Vec<(String, String)>,
}

impl HttpApiError {
    pub fn validation_error(message: impl Into<String>) -> Self {
        Self {
            error_type: "ValidationException".to_string(),
            message: message.into(),
            status_code: 400,
            cancellation_reasons: None,
            item: None,
            response_headers: Vec::new(),
        }
    }

    pub fn internal_server_error(message: impl Into<String>) -> Self {
        Self {
            error_type: "InternalServerError".to_string(),
            message: message.into(),
            status_code: 500,
            cancellation_reasons: None,
            item: None,
            response_headers: Vec::new(),
        }
    }

    pub fn resource_in_use_error(message: impl Into<String>) -> Self {
        Self {
            error_type: "ResourceInUseException".to_string(),
            message: message.into(),
            status_code: 400,
            cancellation_reasons: None,
            item: None,
            response_headers: Vec::new(),
        }
    }

    pub fn resource_not_found_error(message: impl Into<String>) -> Self {
        Self {
            error_type: "ResourceNotFoundException".to_string(),
            message: message.into(),
            status_code: 400,
            cancellation_reasons: None,
            item: None,
            response_headers: Vec::new(),
        }
    }

    pub fn throttled_error(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
            status_code: 429,
            cancellation_reasons: None,
            item: None,
            response_headers: Vec::new(),
        }
    }

    pub fn service_unavailable(retry_after_seconds: u64) -> Self {
        Self::dynamodb_protocol_error(
            "ServiceUnavailableException",
            "Storage is temporarily unavailable.",
            503,
        )
        .with_response_header("Retry-After", retry_after_seconds.max(1).to_string())
    }

    pub fn unauthorized_error(message: impl Into<String>) -> Self {
        Self {
            error_type: "UnrecognizedClientException".to_string(),
            message: message.into(),
            status_code: 401,
            cancellation_reasons: None,
            item: None,
            response_headers: Vec::new(),
        }
    }

    pub fn access_denied_error(message: impl Into<String>) -> Self {
        Self {
            error_type: "AccessDeniedException".to_string(),
            message: message.into(),
            status_code: 403,
            cancellation_reasons: None,
            item: None,
            response_headers: Vec::new(),
        }
    }

    pub fn dynamodb_error(
        error_type: impl Into<String>,
        message: impl Into<String>,
        status_code: u16,
    ) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
            status_code,
            cancellation_reasons: None,
            item: None,
            response_headers: Vec::new(),
        }
    }

    pub fn dynamodb_protocol_error(
        error_type: impl AsRef<str>,
        message: impl Into<String>,
        status_code: u16,
    ) -> Self {
        Self::dynamodb_error(
            dynamodb_full_error_type(error_type.as_ref()),
            message,
            status_code,
        )
    }

    pub fn aws_query_error(
        error_type: impl Into<String>,
        message: impl Into<String>,
        status_code: u16,
    ) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
            status_code,
            cancellation_reasons: None,
            item: None,
            response_headers: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_response_header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.response_headers.push((name.into(), value.into()));
        self
    }

    #[must_use]
    pub fn with_item(mut self, item: AttributeMap) -> Self {
        self.item = Some(Box::new(item));
        self
    }
}

impl IntoApiError for HttpApiError {
    fn status_code(&self) -> u16 {
        self.status_code
    }

    fn error_code(&self) -> Cow<'static, str> {
        Cow::Owned(self.error_type.clone())
    }

    fn error_message(&self) -> Cow<'static, str> {
        Cow::Owned(self.message.clone())
    }

    fn error_response(&self) -> ErrorResponse {
        ErrorResponse {
            error_type: self.error_code().into_owned(),
            message: self.error_message().into_owned(),
            transaction_message: None,
            cancellation_reasons: self.cancellation_reasons.clone(),
            item: self.item.as_ref().map(|item| (**item).clone()),
            request_id: None,
            documentation_url: None,
            retry_after_seconds: self
                .response_headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
                .and_then(|(_, value)| value.parse::<u64>().ok()),
        }
    }
}

impl From<StorageError> for HttpApiError {
    fn from(error: StorageError) -> Self {
        let variant: &StorageEnum = error.to_enum();
        match variant {
            StorageEnum::Database(err) => {
                Self::internal_server_error(format!("Database error: {err}"))
            }
            StorageEnum::AwsSerialization(err) => {
                Self::internal_server_error(format!("AWS serialization error: {err}"))
            }
            StorageEnum::Serialization(err) => {
                Self::internal_server_error(format!("Serialization error: {err}"))
            }

            StorageEnum::TableAlreadyExists { name } => Self::dynamodb_protocol_error(
                "ResourceInUseException",
                format!("Table already exists: {name}"),
                400,
            ),
            StorageEnum::TableNotFound { message, .. } => {
                Self::dynamodb_protocol_error("ResourceNotFoundException", message.clone(), 400)
            }
            StorageEnum::Validation { message } => Self::dynamodb_protocol_error(
                "ValidationException",
                dynamodb_validation_message(message),
                400,
            ),
            StorageEnum::RawValidation { message } => {
                Self::dynamodb_protocol_error("ValidationException", message.clone(), 400)
            }
            StorageEnum::DeletionProtectionEnabled { message, .. } => {
                Self::dynamodb_protocol_error("ValidationException", message.clone(), 400)
            }

            StorageEnum::InternalServerError { message } => {
                Self::dynamodb_protocol_error("InternalServerError", message.clone(), 500)
            }
            StorageEnum::ServiceUnavailable {
                retry_after_seconds,
                ..
            } => Self::service_unavailable(*retry_after_seconds),
            StorageEnum::GuardConflict { message } => Self::internal_server_error(message.clone()),
            StorageEnum::Unsupported { message } => Self::internal_server_error(message.clone()),
            StorageEnum::TransactionConflict { message } => {
                Self::dynamodb_protocol_error("TransactionConflictException", message.clone(), 400)
            }
            StorageEnum::TransactionInProgress { message } => Self::dynamodb_protocol_error(
                "TransactionInProgressException",
                message.clone(),
                400,
            ),
            StorageEnum::ConditionalCheckFailed => Self::dynamodb_protocol_error(
                "ConditionalCheckFailedException",
                DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE,
                400,
            ),
            StorageEnum::ConditionalCheckFailedWithItem { item } => Self::dynamodb_protocol_error(
                "ConditionalCheckFailedException",
                DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE,
                400,
            )
            .with_item(item.clone()),
            StorageEnum::IndexNotFound { .. } => Self::dynamodb_protocol_error(
                "ResourceNotFoundException",
                DYNAMODB_RESOURCE_NOT_FOUND_MESSAGE,
                400,
            ),
            StorageEnum::ResourceNotFound { .. } => Self::dynamodb_protocol_error(
                "ResourceNotFoundException",
                DYNAMODB_RESOURCE_NOT_FOUND_MESSAGE,
                400,
            ),
            StorageEnum::ResourceExists { .. } => Self::dynamodb_protocol_error(
                "ResourceInUseException",
                DYNAMODB_RESOURCE_IN_USE_MESSAGE,
                400,
            ),
            StorageEnum::KeyValidation(message) => {
                Self::dynamodb_protocol_error("ValidationException", message.to_string(), 400)
            }
            StorageEnum::TransactionCanceled { reasons } => {
                let cancellation_reasons = dynamodb_cancellation_reasons(reasons);
                let reason_codes = cancellation_reasons
                    .iter()
                    .map(|reason| reason.code.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let message = if reason_codes.is_empty() {
                    DYNAMODB_TRANSACTION_CANCELED_MESSAGE.to_string()
                } else {
                    format!(
                        "Transaction cancelled, please refer cancellation reasons for specific \
                         reasons [{reason_codes}]"
                    )
                };
                let mut error =
                    Self::dynamodb_protocol_error("TransactionCanceledException", message, 400);
                error.cancellation_reasons = Some(cancellation_reasons);
                error
            }
            StorageEnum::ProvisionedThroughputExceeded { message } => {
                Self::dynamodb_protocol_error(
                    "ProvisionedThroughputExceededException",
                    message.clone(),
                    400,
                )
            }
            StorageEnum::Throttled { .. } => {
                Self::throttled_error("ThrottlingException", DYNAMODB_THROTTLING_MESSAGE)
            }
            StorageEnum::LimitExceeded { message } => {
                Self::dynamodb_protocol_error("LimitExceededException", message.clone(), 400)
            }
            StorageEnum::RequestLimitExceeded => Self::dynamodb_protocol_error(
                "RequestLimitExceeded",
                DYNAMODB_REQUEST_LIMIT_EXCEEDED_MESSAGE,
                400,
            ),
            StorageEnum::MissingAuthenticationToken => Self::dynamodb_protocol_error(
                "MissingAuthenticationToken",
                DYNAMODB_MISSING_AUTH_TOKEN_MESSAGE,
                400,
            ),
            StorageEnum::Authentication { message } => Self::unauthorized_error(message.clone()),
            StorageEnum::AccessDenied { message } => Self::access_denied_error(message.clone()),
            StorageEnum::AwsService { code, message } => Self {
                error_type: code
                    .clone()
                    .unwrap_or_else(|| "AwsServiceException".to_string()),
                message: message.clone(),
                status_code: 500,
                cancellation_reasons: None,
                item: None,
                response_headers: Vec::new(),
            },
        }
    }
}

fn dynamodb_full_error_type(error_type: &str) -> String {
    let prefix = match error_type {
        "ValidationException" => "com.amazon.coral.validate#",
        "SerializationException"
        | "UnknownOperationException"
        | "MissingAuthenticationToken"
        | "IncompleteSignature"
        | "InvalidSignatureException" => "com.amazon.coral.service#",
        _ => "com.amazonaws.dynamodb.v20120810#",
    };
    format!("{prefix}{error_type}")
}

fn dynamodb_validation_message(message: &str) -> String {
    if message == "The parameter cannot be converted to a numeric value" {
        return "1 validation error detected: The parameter cannot be converted to a numeric \
                value: "
            .to_string();
    }
    if message.starts_with("1 validation error detected:")
        || message.starts_with("One or more parameter values were invalid:")
        || message == "The provided key element does not match the schema"
        || message == "Item size has exceeded the maximum allowed size"
    {
        return message.to_string();
    }

    if message.starts_with("Invalid UpdateExpression:")
        || message.starts_with("Invalid ConditionExpression:")
        || message == "Attempting to store more than 38 significant digits in a Number"
        || message
            == "Number underflow. Attempting to store a number with magnitude smaller than \
                supported range"
        || message.starts_with("Value provided in ExpressionAttributeValues unused in expressions:")
        || message.starts_with("Value provided in ExpressionAttributeNames unused in expressions:")
    {
        return format!("1 validation error detected: {message}");
    }

    message.to_string()
}

impl From<HttpApiError> for (axum::http::StatusCode, axum::response::Json<ErrorResponse>) {
    #[expect(clippy::match_same_arms)]
    fn from(error: HttpApiError) -> Self {
        let status = match error.status_code {
            400 => axum::http::StatusCode::BAD_REQUEST,
            401 => axum::http::StatusCode::UNAUTHORIZED,
            403 => axum::http::StatusCode::FORBIDDEN,
            404 => axum::http::StatusCode::NOT_FOUND,
            429 => axum::http::StatusCode::TOO_MANY_REQUESTS,
            503 => axum::http::StatusCode::SERVICE_UNAVAILABLE,
            500 => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        };

        let response = ErrorResponse {
            error_type: error.error_type,
            message: if error.cancellation_reasons.is_some() {
                String::new()
            } else {
                error.message.clone()
            },
            transaction_message: error.cancellation_reasons.as_ref().map(|_| error.message),
            cancellation_reasons: error.cancellation_reasons,
            item: error.item.map(|item| *item),
            retry_after_seconds: error
                .response_headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
                .and_then(|(_, value)| value.parse::<u64>().ok()),
            ..Default::default()
        };

        (status, axum::response::Json(response))
    }
}

fn dynamodb_cancellation_reasons(reasons: &[String]) -> Vec<DynamoDbCancellationReason> {
    reasons
        .iter()
        .map(|reason| {
            let mut parts = reason.splitn(3, '\t');
            let code = parts.next().unwrap_or_default();
            let message = parts.next().map(ToString::to_string);
            let item = parts
                .next()
                .and_then(|item| serde_json::from_str::<AttributeMap>(item).ok());
            DynamoDbCancellationReason {
                item,
                code: code.to_string(),
                message: message.or_else(|| default_cancellation_reason_message(code)),
            }
        })
        .collect()
}

fn default_cancellation_reason_message(code: &str) -> Option<String> {
    match code {
        "ConditionalCheckFailed" => Some(DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE.to_string()),
        "ValidationError" => Some(String::new()),
        "None" => None,
        _ => None,
    }
}
