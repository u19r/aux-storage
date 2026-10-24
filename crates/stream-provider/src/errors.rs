use std::fmt::Error;

use storage_types::{StorageEnum, StorageError, err_context};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamValidationKind {
    EmptyName,
    InvalidLimit,
    InvalidNameBoundary,
    InvalidNameCharacters,
    InvalidTtl,
    ItemDataEmpty,
    ItemDataTooLarge,
    MissingPartitionKey,
    NameTooLong,
    NoWritablePartition,
    TargetItemNotFound,
    SplitPartitionNotFound,
    Message,
}

impl StreamValidationKind {
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::EmptyName => "name cannot be empty",
            Self::InvalidLimit => "invalid limit",
            Self::InvalidNameBoundary => "name must start and end with alphanumeric characters",
            Self::InvalidNameCharacters => "name contains invalid characters",
            Self::InvalidTtl => "invalid ttl",
            Self::ItemDataEmpty => "item data cannot be empty",
            Self::ItemDataTooLarge => "item data too large",
            Self::MissingPartitionKey => "partition key is required for key-ordered streams",
            Self::NameTooLong => "name too long",
            Self::NoWritablePartition => "no writable partition found",
            Self::TargetItemNotFound => "target item not found in stream",
            Self::SplitPartitionNotFound => "split partition not found",
            Self::Message => "validation error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamInternalKind {
    CleanupTask,
    Deserialization,
    InvalidOrMissingKeyAttribute,
    ParseNewImage,
    ParseOldImage,
    ParseStreamPointer,
    Serialization,
    Message,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamContinuityKind {
    CursorMissing,
    CursorIdentityMismatch,
    BoundaryMismatch,
    ResumePositionMissing,
    CursorPositionRegressed,
    ProtectedRetentionLost,
    Unsupported,
}

impl StreamContinuityKind {
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::CursorMissing => "protected cursor is missing",
            Self::CursorIdentityMismatch => "protected cursor identity mismatch",
            Self::BoundaryMismatch => "protected cursor boundary mismatch",
            Self::ResumePositionMissing => "protected cursor resume position is missing",
            Self::CursorPositionRegressed => "protected cursor position regressed",
            Self::ProtectedRetentionLost => "protected stream retention was lost",
            Self::Unsupported => "protected cursor continuity is unsupported",
        }
    }
}

impl StreamInternalKind {
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::CleanupTask => "cleanup task failed",
            Self::Deserialization => "deserialization failed",
            Self::InvalidOrMissingKeyAttribute => "invalid or missing key attribute",
            Self::ParseNewImage => "parse new image failed",
            Self::ParseOldImage => "parse old image failed",
            Self::ParseStreamPointer => "parse stream pointer failed",
            Self::Serialization => "serialization failed",
            Self::Message => "internal server error",
        }
    }
}

pub enum StreamValidationInput {
    Kind(StreamValidationKind),
    Message(String),
}

pub enum StreamInternalInput {
    Kind(StreamInternalKind),
    Message(String),
}

impl From<StreamValidationKind> for StreamValidationInput {
    fn from(value: StreamValidationKind) -> Self {
        Self::Kind(value)
    }
}

impl From<String> for StreamValidationInput {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for StreamValidationInput {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}

impl From<StreamInternalKind> for StreamInternalInput {
    fn from(value: StreamInternalKind) -> Self {
        Self::Kind(value)
    }
}

impl From<String> for StreamInternalInput {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for StreamInternalInput {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}

#[derive(Error, Debug, Clone)]
pub enum StreamEnum {
    #[error("Database error: {0}")]
    Database(#[from] Error),

    #[error("Serialization error: {}", .0.as_ref())]
    Serialization(#[from] lru_ttl_cache::SharedCause<serde_json::Error>),

    #[error("IO error: {}", .0.as_ref())]
    Io(#[from] lru_ttl_cache::SharedCause<std::io::Error>),

    #[error("Requested resource not found: {resource_type}: {resource_id} not found")]
    ResourceNotFound {
        resource_type: &'static str,
        resource_id: String,
    },

    #[error("Requested resource already exists: {resource_type}: {resource_id} already exists")]
    ResourceExists {
        resource_type: &'static str,
        resource_id: String,
    },

    #[error("Validation error: {message}")]
    Validation {
        kind: StreamValidationKind,
        message: String,
    },

    #[error("Stream continuity error: {message}")]
    Continuity {
        kind: StreamContinuityKind,
        message: String,
    },

    #[error(transparent)]
    StorageError(#[from] StorageError),

    #[error("Internal server error: {message}")]
    Internal {
        kind: StreamInternalKind,
        message: String,
    },
}

err_context!(StreamError, StreamEnum);

impl StreamError {
    pub fn validation(input: impl Into<StreamValidationInput>) -> Self {
        let (kind, message) = match input.into() {
            StreamValidationInput::Kind(kind) => (kind, kind.message().to_string()),
            StreamValidationInput::Message(message) => (StreamValidationKind::Message, message),
        };
        Self::from(StreamEnum::Validation { kind, message })
    }

    pub fn validation_with_detail(
        kind: StreamValidationKind,
        detail: impl std::fmt::Display,
    ) -> Self {
        Self::from(StreamEnum::Validation {
            kind,
            message: detail.to_string(),
        })
    }

    pub fn internal(input: impl Into<StreamInternalInput>) -> Self {
        let (kind, message) = match input.into() {
            StreamInternalInput::Kind(kind) => (kind, kind.message().to_string()),
            StreamInternalInput::Message(message) => (StreamInternalKind::Message, message),
        };
        Self::from(StreamEnum::Internal { kind, message })
    }

    pub fn internal_with_detail(kind: StreamInternalKind, detail: impl std::fmt::Display) -> Self {
        Self::from(StreamEnum::Internal {
            kind,
            message: format!("{}: {detail}", kind.message()),
        })
    }

    pub fn continuity(kind: StreamContinuityKind, detail: impl std::fmt::Display) -> Self {
        Self::from(StreamEnum::Continuity {
            kind,
            message: format!("{}: {detail}", kind.message()),
        })
    }

    pub fn stream_already_exists(name: impl Into<String>) -> Self {
        Self::from(StreamEnum::ResourceExists {
            resource_type: "stream",
            resource_id: name.into(),
        })
    }

    pub fn stream_not_found(name: impl Into<String>) -> Self {
        Self::from(StreamEnum::ResourceNotFound {
            resource_type: "stream",
            resource_id: name.into(),
        })
    }

    pub fn cursor_not_found(name: impl Into<String>) -> Self {
        Self::from(StreamEnum::ResourceNotFound {
            resource_type: "cursor",
            resource_id: name.into(),
        })
    }

    pub fn cursor_already_exists(name: impl Into<String>) -> Self {
        Self::from(StreamEnum::ResourceExists {
            resource_type: "cursor",
            resource_id: name.into(),
        })
    }

    pub fn item_not_found(id: impl Into<String>) -> Self {
        Self::from(StreamEnum::ResourceNotFound {
            resource_type: "stream_item",
            resource_id: id.into(),
        })
    }

    pub fn cleanup_task(message: impl Into<String>) -> Self {
        Self::internal_with_detail(StreamInternalKind::CleanupTask, message.into())
    }

    pub fn serialization_error(context: impl Into<String>) -> Self {
        Self::internal_with_detail(StreamInternalKind::Serialization, context.into())
    }

    pub fn deserialization_error(context: impl Into<String>) -> Self {
        Self::internal_with_detail(StreamInternalKind::Deserialization, context.into())
    }
}

impl From<StreamEnum> for StorageEnum {
    fn from(value: StreamEnum) -> Self {
        match value {
            StreamEnum::StorageError(error) => error.into_enum(),
            StreamEnum::Validation { message, .. } => StorageEnum::Validation {
                kind: storage_types::StorageValidationKind::Message,
                message,
            },
            StreamEnum::Continuity { message, .. } => StorageEnum::InternalServerError { message },
            StreamEnum::Internal { message, .. } => StorageEnum::InternalServerError { message },
            StreamEnum::ResourceExists {
                resource_type,
                resource_id,
            } => StorageEnum::ResourceExists {
                resource_type,
                resource_id,
            },
            StreamEnum::ResourceNotFound {
                resource_type,
                resource_id,
            } => StorageEnum::ResourceNotFound {
                resource_type,
                resource_id,
            },
            StreamEnum::Serialization(err) => Into::<StorageEnum>::into(err),
            e => StorageEnum::InternalServerError {
                message: format!("Stream error: {e}"),
            },
        }
    }
}

impl StreamError {
    #[must_use]
    pub fn into_storage_enum(self) -> StorageEnum {
        StorageEnum::from(self.into_enum())
    }
}

pub type StreamResult<T> = std::result::Result<T, StreamError>;

impl StreamError {
    /// Preserve stream resource identity when a native attempt returns a
    /// storage error. DynamoDB's storage display deliberately hides it.
    pub fn from_storage_error(error: StorageError) -> Self {
        match error.as_ref() {
            StorageEnum::ResourceNotFound {
                resource_type,
                resource_id,
            } => Self::from(StreamEnum::ResourceNotFound {
                resource_type,
                resource_id: resource_id.clone(),
            }),
            StorageEnum::ResourceExists {
                resource_type,
                resource_id,
            } => Self::from(StreamEnum::ResourceExists {
                resource_type,
                resource_id: resource_id.clone(),
            }),
            _ => Self::from(error),
        }
    }

    /// Lower a stream error into the shared storage error for harmonised
    /// repository boundaries. Inner storage errors pass through untouched;
    /// stream-native failures keep their exact identity via the matching
    /// `StorageEnum` shape.
    #[must_use]
    pub fn into_storage_error(self) -> StorageError {
        match self.into_enum() {
            StreamEnum::StorageError(error) => error,
            StreamEnum::ResourceNotFound {
                resource_type,
                resource_id,
            } => StorageEnum::ResourceNotFound {
                resource_type,
                resource_id,
            }
            .into(),
            StreamEnum::ResourceExists {
                resource_type,
                resource_id,
            } => StorageEnum::ResourceExists {
                resource_type,
                resource_id,
            }
            .into(),
            StreamEnum::Validation { message, .. } => StorageError::validation(message),
            other => StorageError::internal(&other.to_string()),
        }
    }
}

impl From<serde_json::Error> for StreamEnum {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error.into())
    }
}

impl From<std::io::Error> for StreamEnum {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.into())
    }
}
