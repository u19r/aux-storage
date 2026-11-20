use http_error::{AwsQueryAnswer, AwsQueryInternal, HttpApiError};
use serde::{Deserialize, Serialize};
use storage_types::{StorageEnum, StorageError};
use thiserror::Error;

use crate::constants::{
    SQS_INTERNAL_ERROR_MESSAGE, SQS_INTERNAL_ERROR_TYPE, SQS_INVALID_ATTRIBUTE_NAME_ERROR_TYPE,
    SQS_INVALID_ATTRIBUTE_VALUE_ERROR_TYPE, SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
    SQS_NON_EXISTENT_QUEUE_ERROR_TYPE, SQS_QUEUE_ALREADY_EXISTS_ERROR_TYPE,
    SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE, SQS_RECEIPT_HANDLE_INVALID_MESSAGE,
};

/// The SQS 500 answer of a storage failure.
const SQS_STORAGE_INTERNAL: AwsQueryInternal = AwsQueryInternal {
    error_type: SQS_INTERNAL_ERROR_TYPE,
    message: SQS_INTERNAL_ERROR_MESSAGE,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueValidationKind {
    InvalidParameterValue,
    InvalidQueueUrlFormat,
    MessageNotFoundOrAlreadyProcessed,
    MessageNotFound,
    CannotOperateVisibleMessage,
    InvalidAttributeName,
    InvalidAttributeValue,
    /// A `CreateQueue` of an existing name with other settings.
    QueueAlreadyExists,
}

impl QueueValidationKind {
    #[must_use]
    pub const fn as_code(self) -> &'static str {
        match self {
            Self::InvalidParameterValue => "invalid_parameter_value",
            Self::InvalidQueueUrlFormat => "invalid_queue_url_format",
            Self::MessageNotFoundOrAlreadyProcessed => "message_not_found_or_already_processed",
            Self::MessageNotFound => "message_not_found",
            Self::CannotOperateVisibleMessage => "cannot_operate_visible_message",
            Self::InvalidAttributeName => "invalid_attribute_name",
            Self::InvalidAttributeValue => "invalid_attribute_value",
            Self::QueueAlreadyExists => "queue_already_exists",
        }
    }

    #[must_use]
    pub const fn aws_query_error_type(self) -> &'static str {
        match self {
            Self::MessageNotFound | Self::MessageNotFoundOrAlreadyProcessed => {
                SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE
            }
            Self::InvalidParameterValue
            | Self::InvalidQueueUrlFormat
            | Self::CannotOperateVisibleMessage => SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
            Self::InvalidAttributeName => SQS_INVALID_ATTRIBUTE_NAME_ERROR_TYPE,
            Self::InvalidAttributeValue => SQS_INVALID_ATTRIBUTE_VALUE_ERROR_TYPE,
            Self::QueueAlreadyExists => SQS_QUEUE_ALREADY_EXISTS_ERROR_TYPE,
        }
    }

    #[must_use]
    pub fn aws_query_message(self, detail: Option<String>) -> String {
        detail.unwrap_or_else(|| match self {
            Self::MessageNotFound | Self::MessageNotFoundOrAlreadyProcessed => {
                SQS_RECEIPT_HANDLE_INVALID_MESSAGE.to_string()
            }
            // Client texts, never internal codes (review L5).
            Self::InvalidParameterValue => "One or more parameters are invalid.".to_owned(),
            Self::InvalidQueueUrlFormat => {
                "The queue URL is not valid for this endpoint.".to_owned()
            }
            Self::CannotOperateVisibleMessage => "The message is not in flight.".to_owned(),
            Self::InvalidAttributeName => "An attribute name is not valid.".to_owned(),
            Self::InvalidAttributeValue => "An attribute value is not valid.".to_owned(),
            Self::QueueAlreadyExists => {
                "A queue already exists with the same name and different settings.".to_owned()
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueInternalKind {
    InvalidMessageVisibilityKeyFormat,
    MissingQueuePartitionState,
    NoWritableQueuePartition,
    PartitionedQueueBodyDecode,
    PartitionedQueueStateDecode,
    PartitionedQueueSendRetriesExhausted,
    SQLiteBackendDisabled,
    PostgresBackendDisabled,
    RocksDbBackendDisabled,
    TursoBackendDisabled,
    FoundationDbBackendDisabled,
    ReceiveCoalescerClosed,
    RemoteBackendNotImplemented,
    QueueEndpointNotConfigured,
}

impl QueueInternalKind {
    #[must_use]
    pub const fn as_code(self) -> &'static str {
        match self {
            Self::InvalidMessageVisibilityKeyFormat => "invalid_message_visibility_key_format",
            Self::MissingQueuePartitionState => "missing_queue_partition_state",
            Self::NoWritableQueuePartition => "no_writable_queue_partition",
            Self::PartitionedQueueBodyDecode => "partitioned_queue_body_decode",
            Self::PartitionedQueueStateDecode => "partitioned_queue_state_decode",
            Self::PartitionedQueueSendRetriesExhausted => {
                "partitioned_queue_send_retries_exhausted"
            }
            Self::SQLiteBackendDisabled => "sqlite_backend_disabled",
            Self::PostgresBackendDisabled => "postgres_backend_disabled",
            Self::RocksDbBackendDisabled => "rocksdb_backend_disabled",
            Self::TursoBackendDisabled => "turso_backend_disabled",
            Self::FoundationDbBackendDisabled => "foundationdb_backend_disabled",
            Self::ReceiveCoalescerClosed => "receive_coalescer_closed",
            Self::RemoteBackendNotImplemented => "remote_backend_not_implemented",
            Self::QueueEndpointNotConfigured => "queue_endpoint_not_configured",
        }
    }
}

/// The text of a malformed receipt handle. Probe 4: a handle of the base64
/// alphabet is echoed (`invalid`, a random base64 string). The suite scripts
/// (`sim2real-bogus`, `AQEB...-...`) show the generic text for a handle that
/// has characters outside that alphabet, such as a hyphen. The decode result
/// of the handle decides.
#[must_use]
pub fn receipt_handle_invalid_message(handle: &str) -> String {
    match crate::ReceiptHandle::from(handle).decode() {
        crate::newtypes::DecodedReceipt::NotBase64 => {
            "The input receipt handle is invalid.".to_owned()
        }
        _ => format!("The input receipt handle \"{handle}\" is not a valid receipt handle."),
    }
}

#[derive(Error, Debug)]
pub enum QueueError {
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Requested resource already exists: {resource_type}: {resource_id} already exists")]
    ResourceExists {
        resource_type: &'static str,
        resource_id: String,
    },

    #[error("Requested resource not found: {resource_type}: {resource_id} not found")]
    ResourceNotFound {
        resource_type: &'static str,
        resource_id: String,
    },

    #[error("Validation error: {}", kind.as_code())]
    Validation {
        kind: QueueValidationKind,
        detail: Option<String>,
    },

    #[error("Internal server error: {}", kind.as_code())]
    Internal {
        kind: QueueInternalKind,
        detail: Option<String>,
    },

    #[error(transparent)]
    StorageError(StorageError),

    #[error("Transact Write failed")]
    TransactWrite(StorageError),

    #[error("Batch entry failed: {code}: {message}")]
    BatchEntry {
        sender_fault: bool,
        code: String,
        message: String,
    },
}

impl QueueError {
    #[must_use]
    pub fn validation(kind: QueueValidationKind) -> Self {
        Self::Validation { kind, detail: None }
    }

    pub fn validation_with_detail(
        kind: QueueValidationKind,
        detail: impl std::fmt::Display,
    ) -> Self {
        Self::Validation {
            kind,
            detail: Some(detail.to_string()),
        }
    }

    #[must_use]
    pub fn internal(kind: QueueInternalKind) -> Self {
        Self::Internal { kind, detail: None }
    }

    pub fn internal_with_detail(kind: QueueInternalKind, detail: impl std::fmt::Display) -> Self {
        Self::Internal {
            kind,
            detail: Some(detail.to_string()),
        }
    }

    pub fn table_already_exists(name: impl Into<String>) -> Self {
        Self::ResourceExists {
            resource_type: "table",
            resource_id: name.into(),
        }
    }

    pub fn queue_already_exists(name: impl Into<String>) -> Self {
        Self::ResourceExists {
            resource_type: "queue",
            resource_id: name.into(),
        }
    }

    pub fn table_not_found(name: impl Into<String>) -> Self {
        Self::ResourceNotFound {
            resource_type: "table",
            resource_id: name.into(),
        }
    }

    pub fn batch_entry(sender_fault: bool, code: String, message: String) -> Self {
        Self::BatchEntry {
            sender_fault,
            code,
            message,
        }
    }

    /// The storage failure this error carries.
    #[must_use]
    pub fn storage_error(&self) -> Option<&StorageError> {
        match self {
            Self::StorageError(error) | Self::TransactWrite(error) => Some(error),
            Self::Serialization(_)
            | Self::ResourceExists { .. }
            | Self::ResourceNotFound { .. }
            | Self::Validation { .. }
            | Self::Internal { .. }
            | Self::BatchEntry { .. } => None,
        }
    }

    /// The SQS answer of a storage failure, chosen by its class (ARCH-2-f;
    /// replaces W2 D5's `is_native_rejection`, whose set it keeps and extends
    /// to every known-not-committed class).
    #[must_use]
    pub fn storage_answer(&self) -> Option<AwsQueryAnswer> {
        self.storage_error()
            .map(|error| AwsQueryAnswer::for_storage(error, SQS_STORAGE_INTERNAL))
    }

    #[must_use]
    pub fn aws_query_error_type(&self) -> &str {
        match self {
            Self::StorageError(error) | Self::TransactWrite(error) => {
                AwsQueryAnswer::for_storage(error, SQS_STORAGE_INTERNAL).error_type
            }
            Self::ResourceExists { .. } => SQS_QUEUE_ALREADY_EXISTS_ERROR_TYPE,
            Self::ResourceNotFound {
                resource_type: "receipt_handle",
                ..
            } => SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE,
            Self::ResourceNotFound { .. } => SQS_NON_EXISTENT_QUEUE_ERROR_TYPE,
            Self::Validation {
                kind: QueueValidationKind::InvalidParameterValue,
                detail: Some(detail),
            } => crate::sqs_validation_error_type(detail),
            Self::Validation { kind, .. } => kind.aws_query_error_type(),
            Self::BatchEntry { code, .. } => code,
            Self::Serialization(_) | Self::Internal { .. } => SQS_INTERNAL_ERROR_TYPE,
        }
    }

    #[must_use]
    pub fn aws_query_message(&self) -> String {
        match self {
            Self::StorageError(error) | Self::TransactWrite(error) => {
                AwsQueryAnswer::for_storage(error, SQS_STORAGE_INTERNAL)
                    .message
                    .to_owned()
            }
            Self::ResourceNotFound {
                resource_type: "receipt_handle",
                resource_id,
            } => receipt_handle_invalid_message(resource_id),
            Self::ResourceNotFound { .. } => "The specified queue does not exist.".to_string(),
            Self::Validation { kind, detail } => kind.aws_query_message(detail.clone()),
            Self::BatchEntry { message, .. } => message.clone(),
            Self::Serialization(_) => SQS_INTERNAL_ERROR_MESSAGE.to_owned(),
            Self::ResourceExists { .. } | Self::Internal { .. } => self.to_string(),
        }
    }

    #[must_use]
    pub fn aws_query_status_code(&self) -> u16 {
        match self {
            Self::StorageError(error) | Self::TransactWrite(error) => {
                AwsQueryAnswer::for_storage(error, SQS_STORAGE_INTERNAL).status_code
            }
            Self::Serialization(_) | Self::Internal { .. } => 500,
            Self::BatchEntry { sender_fault, .. } => {
                if *sender_fault {
                    400
                } else {
                    500
                }
            }
            Self::ResourceNotFound {
                resource_type: "receipt_handle",
                ..
            } => 404,
            Self::Validation {
                kind:
                    QueueValidationKind::MessageNotFound
                    | QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
                ..
            } => 404,
            Self::ResourceExists { .. }
            | Self::ResourceNotFound { .. }
            | Self::Validation { .. } => 400,
        }
    }

    #[must_use]
    pub fn is_sender_fault(&self) -> bool {
        matches!(
            self,
            Self::ResourceExists { .. } | Self::ResourceNotFound { .. } | Self::Validation { .. }
        ) || matches!(
            self,
            Self::BatchEntry {
                sender_fault: true,
                ..
            }
        )
    }
}

pub type QueueResult<T> = std::result::Result<T, QueueError>;

impl QueueError {
    /// Lower a queue error into the shared storage error for harmonised
    /// repository boundaries. Inner storage errors pass through untouched;
    /// queue-native failures keep their exact SQS-parity identity via the
    /// matching `StorageEnum` shape.
    #[must_use]
    pub fn into_storage_error(self) -> StorageError {
        match self {
            Self::StorageError(error) | Self::TransactWrite(error) => error,
            Self::ResourceNotFound {
                resource_type,
                resource_id,
            } => StorageError::from(StorageEnum::ResourceNotFound {
                resource_type,
                resource_id,
            }),
            Self::ResourceExists {
                resource_type,
                resource_id,
            } => StorageError::from(StorageEnum::ResourceExists {
                resource_type,
                resource_id,
            }),
            Self::Validation { .. } | Self::Internal { .. } | Self::BatchEntry { .. } => {
                StorageError::internal(&self.to_string())
            }
            Self::Serialization(error) => StorageError::internal(&format!(
                "queue serialization failed: {:?} at {}:{}",
                error.classify(),
                error.line(),
                error.column()
            )),
        }
    }
}

impl From<QueueError> for HttpApiError {
    fn from(error: QueueError) -> Self {
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

impl From<StorageError> for QueueError {
    fn from(error: StorageError) -> Self {
        if let StorageEnum::Validation { message, .. } = error.as_ref() {
            return Self::validation_with_detail(
                QueueValidationKind::InvalidParameterValue,
                message.clone(),
            );
        }
        Self::StorageError(error)
    }
}
