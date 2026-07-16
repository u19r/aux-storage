use http_error::HttpApiError;
use storage_types::StorageError;
use thiserror::Error;

use crate::constants::{
    SQS_INTERNAL_ERROR_TYPE, SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
    SQS_NON_EXISTENT_QUEUE_ERROR_TYPE, SQS_QUEUE_NAME_EXISTS_ERROR_TYPE,
    SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE, SQS_RECEIPT_HANDLE_INVALID_MESSAGE,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueValidationKind {
    InvalidParameterValue,
    InvalidQueueUrlFormat,
    MessageNotFoundOrAlreadyProcessed,
    MessageNotFound,
    CannotOperateVisibleMessage,
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
        }
    }

    #[must_use]
    pub fn aws_query_message(self, detail: Option<String>) -> String {
        detail.unwrap_or_else(|| match self {
            Self::MessageNotFound | Self::MessageNotFoundOrAlreadyProcessed => {
                SQS_RECEIPT_HANDLE_INVALID_MESSAGE.to_string()
            }
            Self::InvalidParameterValue
            | Self::InvalidQueueUrlFormat
            | Self::CannotOperateVisibleMessage => "validation_error".to_string(),
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
        }
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
    StorageError(#[from] StorageError),

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

    #[must_use]
    pub fn aws_query_error_type(&self) -> &str {
        match self {
            Self::ResourceExists { .. } => SQS_QUEUE_NAME_EXISTS_ERROR_TYPE,
            Self::ResourceNotFound {
                resource_type: "receipt_handle",
                ..
            } => SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE,
            Self::ResourceNotFound { .. } => SQS_NON_EXISTENT_QUEUE_ERROR_TYPE,
            Self::Validation { kind, .. } => kind.aws_query_error_type(),
            Self::BatchEntry { code, .. } => code,
            Self::Serialization(_)
            | Self::Internal { .. }
            | Self::StorageError(_)
            | Self::TransactWrite(_) => SQS_INTERNAL_ERROR_TYPE,
        }
    }

    #[must_use]
    pub fn aws_query_message(&self) -> String {
        match self {
            Self::ResourceNotFound {
                resource_type: "receipt_handle",
                resource_id,
            } => {
                format!("The input receipt handle \"{resource_id}\" is not a valid receipt handle.")
            }
            Self::ResourceNotFound { .. } => "The specified queue does not exist.".to_string(),
            Self::Validation { kind, detail } => kind.aws_query_message(detail.clone()),
            Self::BatchEntry { message, .. } => message.clone(),
            other => other.to_string(),
        }
    }

    #[must_use]
    pub fn aws_query_status_code(&self) -> u16 {
        match self {
            Self::Serialization(_)
            | Self::Internal { .. }
            | Self::StorageError(_)
            | Self::TransactWrite(_) => 500,
            Self::BatchEntry { sender_fault, .. } => {
                if *sender_fault { 400 } else { 500 }
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
            Self::ResourceExists { .. }
                | Self::ResourceNotFound { .. }
                | Self::Validation { .. }
        ) || matches!(self, Self::BatchEntry { sender_fault: true, .. })
    }
}

pub type QueueResult<T> = std::result::Result<T, QueueError>;

impl From<QueueError> for HttpApiError {
    fn from(error: QueueError) -> Self {
        HttpApiError::aws_query_error(
            error.aws_query_error_type(),
            error.aws_query_message(),
            error.aws_query_status_code(),
        )
    }
}
