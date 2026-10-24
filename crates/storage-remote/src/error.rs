use http::StatusCode;
use serde::Deserialize;
use storage_types::{
    AttributeMap, StorageEnum, StorageError, parse_dynamodb_table_not_found_message,
};

#[derive(Debug, Default, Deserialize)]
pub struct RemoteErrorResponse {
    #[serde(default, rename = "__type", alias = "type", alias = "errorType")]
    pub error_type: Option<String>,
    #[serde(default, rename = "code", alias = "Code", alias = "errorCode")]
    pub code: Option<String>,
    #[serde(default, rename = "message", alias = "Message")]
    pub message: Option<String>,
    #[serde(default, rename = "Item", alias = "item")]
    pub item: Option<serde_json::Value>,
    #[serde(
        default,
        rename = "CancellationReasons",
        alias = "cancellationReasons",
        alias = "cancellation_reasons"
    )]
    pub cancellation_reasons: Option<Vec<RemoteCancellationReason>>,
}

#[derive(Debug, Default, Deserialize)]
pub struct RemoteCancellationReason {
    #[serde(default, rename = "Code", alias = "code")]
    pub code: Option<String>,
    #[serde(default, rename = "Message", alias = "message")]
    pub message: Option<String>,
    #[serde(default, rename = "Item", alias = "item")]
    pub item: Option<serde_json::Value>,
}

pub fn classify_error_response(
    status: StatusCode,
    error: RemoteErrorResponse,
) -> (StorageError, bool, Option<String>) {
    let code = error
        .code
        .or(error.error_type)
        .map(|value| normalize_error_code(&value));
    let message = error
        .message
        .unwrap_or_else(|| "remote operation failed".to_string());
    let conditional_failure_item = error
        .item
        .and_then(|item| serde_json::from_value::<AttributeMap>(item).ok());
    let table_name = message
        .strip_prefix("Table already exists: ")
        .unwrap_or(&message)
        .to_string();

    let (storage_error, retryable) = match code.as_deref() {
        Some("ProvisionedThroughputExceededException") => (
            StorageError::from(StorageEnum::ProvisionedThroughputExceeded { message }),
            true,
        ),
        Some("ThrottlingException") => {
            (StorageError::from(StorageEnum::Throttled { message }), true)
        }
        Some("RequestLimitExceeded" | "RequestLimitExceededException") => {
            (StorageError::from(StorageEnum::RequestLimitExceeded), true)
        }
        Some("LimitExceededException") => (
            StorageError::from(StorageEnum::LimitExceeded { message }),
            true,
        ),
        Some("ConditionalCheckFailedException") => match conditional_failure_item {
            Some(item) => (
                StorageError::from(StorageEnum::ConditionalCheckFailedWithItem { item }),
                false,
            ),
            None => (
                StorageError::from(StorageEnum::ConditionalCheckFailed),
                false,
            ),
        },
        Some("TransactionConflictException") => (
            StorageError::from(StorageEnum::TransactionConflict { message }),
            true,
        ),
        Some("TransactionCanceledException") => {
            let reasons = error
                .cancellation_reasons
                .unwrap_or_default()
                .into_iter()
                .map(encode_cancellation_reason)
                .collect::<Vec<_>>();
            (
                StorageError::from(StorageEnum::TransactionCanceled { reasons }),
                false,
            )
        }
        Some("TransactionInProgressException") => (
            StorageError::from(StorageEnum::TransactionInProgress { message }),
            true,
        ),
        Some("IdempotentParameterMismatchException") => {
            (StorageError::idempotent_parameter_mismatch(), false)
        }
        Some("NotLeaderException") => (
            StorageError::from(StorageEnum::AwsService {
                code: Some("NotLeaderException".to_string()),
                message,
            }),
            true,
        ),
        Some("ResourceNotFoundException") => {
            if let Some(table_name) = parse_dynamodb_table_not_found_message(&message) {
                (StorageError::table_not_found(&table_name), false)
            } else {
                (
                    StorageError::from(StorageEnum::ResourceNotFound {
                        resource_type: "dynamodb",
                        resource_id: message.clone(),
                    }),
                    false,
                )
            }
        }
        Some("ResourceInUseException") => (
            StorageError::from(StorageEnum::TableAlreadyExists {
                name: table_name.clone(),
            }),
            false,
        ),
        Some("TableNotFoundException") => {
            if let Some(table_name) = parse_dynamodb_table_not_found_message(&message) {
                (StorageError::table_not_found(&table_name), false)
            } else {
                (
                    StorageError::from(StorageEnum::ResourceNotFound {
                        resource_type: "dynamodb",
                        resource_id: message.clone(),
                    }),
                    false,
                )
            }
        }
        Some("TableAlreadyExistsException") => (
            StorageError::from(StorageEnum::TableAlreadyExists { name: table_name }),
            false,
        ),
        Some("ValidationException") => (
            StorageError::from(StorageEnum::Validation {
                kind: storage_types::StorageValidationKind::Message,
                message,
            }),
            false,
        ),
        Some("AccessDeniedException" | "AccessDenied") => (
            StorageError::from(StorageEnum::AccessDenied { message }),
            false,
        ),
        Some("MissingAuthenticationTokenException") => (
            StorageError::from(StorageEnum::MissingAuthenticationToken),
            false,
        ),
        Some("UnrecognizedClientException") => (
            StorageError::from(StorageEnum::Authentication { message }),
            false,
        ),
        Some("ExpiredTokenException") => (
            StorageError::from(StorageEnum::AwsService {
                code: Some("ExpiredTokenException".to_string()),
                message,
            }),
            false,
        ),
        Some("InternalServerError") => (StorageError::internal(&message), true),
        Some("ServiceUnavailable" | "ServiceUnavailableException") => {
            (StorageError::service_unavailable(1), true)
        }
        Some("SerializationException") => (
            StorageError::from(StorageEnum::AwsSerialization(message)),
            false,
        ),
        Some(unknown) => (
            StorageError::from(StorageEnum::AwsService {
                code: Some(unknown.to_string()),
                message,
            }),
            status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS,
        ),
        None => (
            StorageError::from(StorageEnum::AwsService {
                code: None,
                message,
            }),
            status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS,
        ),
    };

    (storage_error, retryable, code)
}

fn normalize_error_code(raw: &str) -> String {
    raw.split('#').next_back().unwrap_or(raw).to_string()
}

fn encode_cancellation_reason(reason: RemoteCancellationReason) -> String {
    let code = reason.code.unwrap_or_else(|| "None".to_string());
    let Some(item) = reason.item else {
        return reason
            .message
            .map_or(code.clone(), |message| format!("{code}\t{message}"));
    };
    let item = item.to_string();
    format!("{code}\t{}\t{item}", reason.message.unwrap_or_default())
}
