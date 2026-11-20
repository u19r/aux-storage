//! Native failure traces retain safe internal diagnostics without logging binds
//! or keys.

pub use storage_types::NativeEngine;
use storage_types::{StorageEnum, StorageError, context::WrappedError as _};

#[derive(Clone, Copy, Debug)]
pub enum NativeOperation {
    SqlQuery,
    SqlExecute,
    KvGet,
    KvGetBatch,
    KvScan,
    KvScanBatch,
    KvStage,
    KvStageVersionstamped,
    CatalogBindMetadata,
    CatalogReadTable,
}

impl NativeOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SqlQuery => "sql.query",
            Self::SqlExecute => "sql.execute",
            Self::KvGet => "kv.get",
            Self::KvGetBatch => "kv.get_batch",
            Self::KvScan => "kv.scan",
            Self::KvScanBatch => "kv.scan_batch",
            Self::KvStage => "kv.stage",
            Self::KvStageVersionstamped => "kv.stage_versionstamped",
            Self::CatalogBindMetadata => "catalog.bind_metadata",
            Self::CatalogReadTable => "catalog.read_table",
        }
    }
}

pub fn trace_native_failure(
    backend: NativeEngine,
    operation: NativeOperation,
    error: &StorageError,
) {
    let (error_code, diagnostic) = storage_error_diagnostic(error);
    if error.attempt_kind() == Some(storage_types::AttemptKind::Conflict) {
        tracing::debug!(
            backend = backend.as_str(),
            operation = operation.as_str(),
            error_code,
            diagnostic,
            cause = error.attempt_failure().map(tracing::field::display),
            "native storage operation failed"
        );
    } else {
        tracing::warn!(
            backend = backend.as_str(),
            operation = operation.as_str(),
            error_code,
            diagnostic,
            cause = error.attempt_failure().map(tracing::field::display),
            "native storage operation failed"
        );
    }
}

/// Return the precise error code and a diagnostic safe for structured logs.
pub fn storage_error_diagnostic(error: &StorageError) -> (&'static str, &str) {
    match error.to_enum() {
        StorageEnum::InternalServerError { message } => ("InternalServerError", message.as_str()),
        StorageEnum::Attempt(failure) => (attempt_error_code(failure.kind()), failure.reason()),
        StorageEnum::Database(_) => ("DatabaseError", "native database operation failed"),
        StorageEnum::AwsSerialization(_) => ("AwsSerializationError", "AWS serialization failed"),
        StorageEnum::Serialization(_) => ("SerializationError", "serialization failed"),
        StorageEnum::ResourceNotFound { .. }
        | StorageEnum::TableNotFound { .. }
        | StorageEnum::IndexNotFound { .. } => ("ResourceNotFoundException", "resource not found"),
        StorageEnum::ResourceExists { .. } | StorageEnum::TableAlreadyExists { .. } => {
            ("ResourceInUseException", "resource already exists")
        }
        StorageEnum::KeyValidation(_)
        | StorageEnum::Validation { .. }
        | StorageEnum::RawValidation { .. }
        | StorageEnum::DeletionProtectionEnabled { .. } => {
            ("ValidationException", "request validation failed")
        }
        StorageEnum::HistoryUnavailable => {
            ("HistoryUnavailable", "required history is unavailable")
        }
        StorageEnum::HistoryExpired => ("HistoryExpired", "the public history cursor has expired"),
        StorageEnum::TrimmedDataAccess => (
            "TrimmedDataAccessException",
            "the stream cursor has expired",
        ),
        StorageEnum::ConditionalCheckFailed
        | StorageEnum::ConditionalCheckFailedWithItem { .. } => {
            ("ConditionalCheckFailedException", "condition failed")
        }
        StorageEnum::GuardConflict { .. } => ("GuardConflict", "durable guard conflict"),
        StorageEnum::Unsupported { .. } => ("UnsupportedOperation", "unsupported operation"),
        StorageEnum::TransactionInProgress { .. } => (
            "TransactionInProgressException",
            "transaction is in progress",
        ),
        StorageEnum::IdempotentParameterMismatch => (
            "IdempotentParameterMismatchException",
            "idempotency parameters differ",
        ),
        StorageEnum::IdempotencyReplay { .. } => ("IdempotencyReplay", "native replay escaped"),
        StorageEnum::TransactionCanceled { .. } => {
            ("TransactionCanceledException", "transaction was cancelled")
        }
        StorageEnum::ProvisionedThroughputExceeded { .. } => (
            "ProvisionedThroughputExceededException",
            "provisioned throughput exceeded",
        ),
        StorageEnum::Throttled { .. } => ("ThrottlingException", "request throttled"),
        StorageEnum::ServiceUnavailable { .. } => ("ServiceUnavailable", "service unavailable"),
        StorageEnum::InfrastructureResult(_) => (
            "InternalServerError",
            "infrastructure storage operation failed",
        ),
        StorageEnum::LimitExceeded { .. } => ("LimitExceededException", "limit exceeded"),
        StorageEnum::Authentication { .. } => ("AuthenticationException", "authentication failed"),
        StorageEnum::AccessDenied { .. } => ("AccessDeniedException", "access denied"),
        StorageEnum::RequestLimitExceeded => ("RequestLimitExceeded", "request limit exceeded"),
        StorageEnum::MissingAuthenticationToken => (
            "MissingAuthenticationTokenException",
            "authentication token is missing",
        ),
        StorageEnum::AwsService { .. } => ("AwsServiceError", "upstream AWS service failed"),
    }
}

const fn attempt_error_code(kind: storage_types::AttemptKind) -> &'static str {
    match kind {
        storage_types::AttemptKind::Conflict => "TransactionConflictException",
        storage_types::AttemptKind::FreshValidation => "FreshValidationRequired",
        storage_types::AttemptKind::Capacity | storage_types::AttemptKind::TransactionBytes => {
            "NativeCapacityRejected"
        }
        storage_types::AttemptKind::Unavailable | storage_types::AttemptKind::Deadline => {
            "NativeUnavailable"
        }
        storage_types::AttemptKind::CommitUnknown => "CommitUnknown",
    }
}
