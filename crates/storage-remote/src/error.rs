use http::StatusCode;
use serde::Deserialize;
use storage_types::{StorageEnum, StorageError, parse_dynamodb_table_not_found_message};

#[derive(Debug, Default, Deserialize)]
pub struct RemoteErrorResponse {
    #[serde(default, rename = "__type", alias = "type", alias = "errorType")]
    pub error_type: Option<String>,
    #[serde(default, rename = "code", alias = "Code", alias = "errorCode")]
    pub code: Option<String>,
    #[serde(default, rename = "message", alias = "Message")]
    pub message: Option<String>,
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

    let (storage_error, retryable) = match code.as_deref() {
        Some("ProvisionedThroughputExceededException") => (
            StorageError::Base(StorageEnum::ProvisionedThroughputExceeded { message }),
            true,
        ),
        Some("ThrottlingException") => {
            (StorageError::Base(StorageEnum::Throttled { message }), true)
        }
        Some("RequestLimitExceeded" | "RequestLimitExceededException") => {
            (StorageError::Base(StorageEnum::RequestLimitExceeded), true)
        }
        Some("LimitExceededException") => (
            StorageError::Base(StorageEnum::LimitExceeded { message }),
            true,
        ),
        Some("ConditionalCheckFailedException") => (
            StorageError::Base(StorageEnum::ConditionalCheckFailed),
            false,
        ),
        Some("TransactionConflictException") => (
            StorageError::Base(StorageEnum::TransactionConflict { message }),
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
                StorageError::Base(StorageEnum::TransactionCanceled { reasons }),
                false,
            )
        }
        Some("TransactionInProgressException") => (
            StorageError::Base(StorageEnum::TransactionInProgress { message }),
            true,
        ),
        Some("NotLeaderException") => (
            StorageError::Base(StorageEnum::AwsService {
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
                    StorageError::Base(StorageEnum::ResourceNotFound {
                        resource_type: "dynamodb",
                        resource_id: message.clone(),
                    }),
                    false,
                )
            }
        }
        Some("ResourceInUseException") => (
            StorageError::Base(StorageEnum::TableAlreadyExists {
                name: message.clone(),
            }),
            false,
        ),
        Some("TableNotFoundException") => {
            if let Some(table_name) = parse_dynamodb_table_not_found_message(&message) {
                (StorageError::table_not_found(&table_name), false)
            } else {
                (
                    StorageError::Base(StorageEnum::ResourceNotFound {
                        resource_type: "dynamodb",
                        resource_id: message.clone(),
                    }),
                    false,
                )
            }
        }
        Some("TableAlreadyExistsException") => (
            StorageError::Base(StorageEnum::TableAlreadyExists {
                name: message.clone(),
            }),
            false,
        ),
        Some("ValidationException") => (
            StorageError::Base(StorageEnum::Validation { message }),
            false,
        ),
        Some("AccessDeniedException" | "AccessDenied") => (
            StorageError::Base(StorageEnum::AccessDenied { message }),
            false,
        ),
        Some("MissingAuthenticationTokenException") => (
            StorageError::Base(StorageEnum::MissingAuthenticationToken),
            false,
        ),
        Some("UnrecognizedClientException") => (
            StorageError::Base(StorageEnum::Authentication { message }),
            false,
        ),
        Some("ExpiredTokenException") => (
            StorageError::Base(StorageEnum::AwsService {
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
            StorageError::Base(StorageEnum::AwsSerialization(message)),
            false,
        ),
        Some(unknown) => (
            StorageError::Base(StorageEnum::AwsService {
                code: Some(unknown.to_string()),
                message,
            }),
            status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS,
        ),
        None => (
            StorageError::Base(StorageEnum::AwsService {
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
