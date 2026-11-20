use std::fmt::Error;

use thiserror::Error;

use crate::{
    AttemptFailure, AttributeMap, CancellationCode, CancellationReason, ErrorClass,
    InfrastructureResultKind, ItemKeyError, context::WrappedError as _,
    dynamodb_table_not_found_message, err_context,
};

#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableNameParseError {
    #[error("TableName cannot be empty")]
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageValidationKind {
    InvalidConditionExpression,
    InvalidOrMissingKey,
    BeginsWithRequiresString,
    EmptyStringKey,
    EmptyBinaryKey,
    EmptyIndexer,
    InvalidIndexerType,
    InvalidNumber,
    NumberPrecision,
    NumberUnderflow,
    NumberOverflow,
    Message,
}

impl StorageValidationKind {
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::InvalidConditionExpression => "Invalid condition expression",
            Self::InvalidOrMissingKey => {
                "One or more parameter values were invalid: Invalid or missing key"
            }
            Self::BeginsWithRequiresString => "begins_with is only valid for string types",
            Self::EmptyStringKey => "key attribute cannot contain an empty string value",
            Self::EmptyBinaryKey => "key attribute cannot contain an empty binary value",
            Self::EmptyIndexer => "Indexers:empty_string",
            Self::InvalidIndexerType => "Indexers:attribute_must_be_string",
            Self::InvalidNumber => "The parameter cannot be converted to a numeric value",
            Self::NumberPrecision => {
                "Attempting to store more than 38 significant digits in a Number"
            }
            Self::NumberUnderflow => {
                "Number underflow. Attempting to store a number with magnitude smaller than \
                 supported range"
            }
            Self::NumberOverflow => {
                "Number overflow. Attempting to store a number with magnitude larger than \
                 supported range"
            }
            Self::Message => "Validation error",
        }
    }
}

pub enum StorageValidationInput {
    Kind(StorageValidationKind),
    Message(String),
}

impl From<StorageValidationKind> for StorageValidationInput {
    fn from(value: StorageValidationKind) -> Self {
        Self::Kind(value)
    }
}

impl From<String> for StorageValidationInput {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for StorageValidationInput {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}

#[derive(Error, Clone)]
pub enum StorageEnum {
    // Not a DynamoDB error type – internal helper. Keep message but mark
    // internal.
    #[error("Database error: {0}")]
    Database(#[from] Error),

    #[error("AWS serialization error: {0}")]
    AwsSerialization(String),

    // Not a DynamoDB error type – internal helper.
    #[error("Serialization error: {}", .0.as_ref())]
    Serialization(#[from] lru_ttl_cache::SharedCause<serde_json::Error>),

    #[error("{0}")]
    InfrastructureResult(#[from] InfrastructureResultKind),

    /// A failed native attempt. Display prints its cause; the wire text is
    /// chosen by the API mapping.
    #[error("{0}")]
    Attempt(#[from] AttemptFailure),

    // Maps to ResourceNotFoundException (DynamoDB). Dynamo message is generic;
    // we store context separately.
    #[error("Requested resource not found")]
    ResourceNotFound {
        resource_type: &'static str,
        resource_id: String,
    },

    // Used for internal resources (e.g. cursor) – no direct Dynamo analogue.
    // Use Dynamo wording when surfaced via DynamoDB-compatible APIs.
    #[error("The resource which you are attempting to change is in use.")]
    ResourceExists {
        resource_type: &'static str,
        resource_id: String,
    },

    // ResourceNotFoundException for an index the request names but the table
    // does not have (UpdateTable delete or update of an absent index).
    #[error("Requested resource not found: Index {index_name} for table {table_name}")]
    IndexNotFound {
        index_name: String,
        table_name: String,
    },

    // Dynamo returns ValidationException with specific message for key errors;
    // we forward key error message.
    #[error("{0}")]
    KeyValidation(#[from] ItemKeyError),

    // The cause; the wire text is DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE
    // (http-error).
    #[error("internal error: {message}")]
    InternalServerError { message: String },

    /// A protected logical history has missing or inconsistent durable
    /// evidence. Maintenance must retain its last valid position and block
    /// that consumer.
    #[error("Required logical history is unavailable")]
    HistoryUnavailable,

    /// A public canonical history cursor precedes its installed visibility
    /// floor. This says nothing about internal journal retirement.
    #[error("The public history cursor has expired")]
    HistoryExpired,

    #[error("The data you are trying to access has been trimmed.")]
    TrimmedDataAccess,

    // ConditionalCheckFailedException canonical message.
    #[error("The conditional request failed")]
    ConditionalCheckFailed,

    // ConditionalCheckFailedException with the item requested by
    // ReturnValuesOnConditionCheckFailure.
    #[error("The conditional request failed")]
    ConditionalCheckFailedWithItem { item: AttributeMap },

    // Internal cache-authority guard conflict. Manager paths must translate
    // this to durable fallback, not Dynamo condition failure.
    #[error("Durable guard conflict: {message}")]
    GuardConflict { message: String },

    // Internal provider capability boundary. Manager paths must fail closed
    // or use the existing durable path.
    #[error("Unsupported storage provider operation: {message}")]
    Unsupported { message: String },

    // ResourceInUseException when table exists: canonical DynamoDB phrasing.
    #[error("Table already exists: {name}")]
    TableAlreadyExists { name: String },

    // ResourceNotFoundException for table – canonical DynamoDB message.
    #[error("{message}")]
    TableNotFound { name: String, message: String },

    // ValidationException – DO NOT prefix with 'Validation error:' to keep
    // exact message parity.
    #[error("{message}")]
    Validation {
        kind: StorageValidationKind,
        message: String,
    },

    // ValidationException with a DynamoDB message that must not be normalized
    // by API adapters.
    #[error("{message}")]
    RawValidation { message: String },

    // ValidationException for DynamoDB table deletion protection.
    #[error("{message}")]
    DeletionProtectionEnabled { table_name: String, message: String },

    // TransactionInProgressException canonical message.
    #[error("The transaction with the given request token is already in progress.")]
    TransactionInProgress { message: String },

    // IdempotentParameterMismatchException for a reused token with different
    // request parameters.
    #[error(
        "Specified idempotent token was used with different request parameters within the \
         idempotency window"
    )]
    IdempotentParameterMismatch,

    // Internal control result: the native token fence found a live replay
    // while it still owned the attempt. The KV provider consumes the payload
    // before this error can cross a public storage boundary.
    #[error("idempotency replay is available from the native attempt")]
    IdempotencyReplay { response: Vec<u8> },

    // TransactionCanceledException canonical message (British spelling
    // 'cancelled' matches AWS response text).
    #[error("Transaction cancelled, please refer cancellation reasons for specific reasons.")]
    TransactionCanceled { reasons: Vec<CancellationReason> },

    #[error(
        "You exceeded your maximum allowed provisioned throughput for a table or for one or more \
         global secondary indexes. To view performance metrics for provisioned throughput vs. \
         consumed throughput, open the Amazon CloudWatch console."
    )]
    ProvisionedThroughputExceeded { message: String },

    #[error("Rate of requests exceeds the allowed throughput.")]
    Throttled { message: String },

    /// Local admission rejected work before a provider operation started.
    /// This is intentionally distinct from upstream throttling, which maps to
    /// DynamoDB's 429 response.
    #[error("Storage is temporarily unavailable.")]
    ServiceUnavailable {
        message: String,
        retry_after_seconds: u64,
    },

    #[error("Too many operations for a given subscriber.")]
    LimitExceeded { message: String },

    #[error("The Access Key ID or security token is invalid.")]
    Authentication { message: String },

    #[error("Access denied.")]
    AccessDenied { message: String },

    // RequestLimitExceeded (DynamoDB).
    #[error(
        "Throughput exceeds the current throughput limit for your account. To request a limit increase, contact AWS Support at https://aws.amazon.com/support."
    )]
    RequestLimitExceeded,

    // MissingAuthenticationTokenException (DynamoDB).
    #[error("Request must contain a valid (registered) AWS Access Key ID.")]
    MissingAuthenticationToken,

    #[error("AWS service error: {message}")]
    AwsService {
        code: Option<String>,
        message: String,
    },
}

/// Hand-written so that no variant prints item data: a condition failure with
/// an item prints its attribute count only, and cancellation reasons use their
/// own redacting `Debug`.
impl std::fmt::Debug for StorageEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => f.debug_tuple("Database").field(error).finish(),
            Self::AwsSerialization(message) => {
                f.debug_tuple("AwsSerialization").field(message).finish()
            }
            Self::Serialization(cause) => f.debug_tuple("Serialization").field(cause).finish(),
            Self::InfrastructureResult(kind) => {
                f.debug_tuple("InfrastructureResult").field(kind).finish()
            }
            Self::Attempt(failure) => f.debug_tuple("Attempt").field(failure).finish(),
            Self::ResourceNotFound {
                resource_type,
                resource_id,
            } => f
                .debug_struct("ResourceNotFound")
                .field("resource_type", resource_type)
                .field("resource_id", resource_id)
                .finish(),
            Self::ResourceExists {
                resource_type,
                resource_id,
            } => f
                .debug_struct("ResourceExists")
                .field("resource_type", resource_type)
                .field("resource_id", resource_id)
                .finish(),
            Self::IndexNotFound {
                index_name,
                table_name,
            } => f
                .debug_struct("IndexNotFound")
                .field("index_name", index_name)
                .field("table_name", table_name)
                .finish(),
            Self::KeyValidation(error) => f.debug_tuple("KeyValidation").field(error).finish(),
            Self::InternalServerError { message } => f
                .debug_struct("InternalServerError")
                .field("message", message)
                .finish(),
            Self::HistoryUnavailable => f.write_str("HistoryUnavailable"),
            Self::HistoryExpired => f.write_str("HistoryExpired"),
            Self::TrimmedDataAccess => f.write_str("TrimmedDataAccess"),
            Self::ConditionalCheckFailed => f.write_str("ConditionalCheckFailed"),
            Self::ConditionalCheckFailedWithItem { item } => f
                .debug_struct("ConditionalCheckFailedWithItem")
                .field("item", &format_args!("<{} attributes>", item.len()))
                .finish(),
            Self::GuardConflict { message } => f
                .debug_struct("GuardConflict")
                .field("message", message)
                .finish(),
            Self::Unsupported { message } => f
                .debug_struct("Unsupported")
                .field("message", message)
                .finish(),
            Self::TableAlreadyExists { name } => f
                .debug_struct("TableAlreadyExists")
                .field("name", name)
                .finish(),
            Self::TableNotFound { name, message } => f
                .debug_struct("TableNotFound")
                .field("name", name)
                .field("message", message)
                .finish(),
            Self::Validation { kind, message } => f
                .debug_struct("Validation")
                .field("kind", kind)
                .field("message", message)
                .finish(),
            Self::RawValidation { message } => f
                .debug_struct("RawValidation")
                .field("message", message)
                .finish(),
            Self::DeletionProtectionEnabled {
                table_name,
                message,
            } => f
                .debug_struct("DeletionProtectionEnabled")
                .field("table_name", table_name)
                .field("message", message)
                .finish(),
            Self::TransactionInProgress { message } => f
                .debug_struct("TransactionInProgress")
                .field("message", message)
                .finish(),
            Self::IdempotentParameterMismatch => f.write_str("IdempotentParameterMismatch"),
            Self::IdempotencyReplay { response } => f
                .debug_struct("IdempotencyReplay")
                .field("response", response)
                .finish(),
            Self::TransactionCanceled { reasons } => f
                .debug_struct("TransactionCanceled")
                .field("reasons", reasons)
                .finish(),
            Self::ProvisionedThroughputExceeded { message } => f
                .debug_struct("ProvisionedThroughputExceeded")
                .field("message", message)
                .finish(),
            Self::Throttled { message } => f
                .debug_struct("Throttled")
                .field("message", message)
                .finish(),
            Self::ServiceUnavailable {
                message,
                retry_after_seconds,
            } => f
                .debug_struct("ServiceUnavailable")
                .field("message", message)
                .field("retry_after_seconds", retry_after_seconds)
                .finish(),
            Self::LimitExceeded { message } => f
                .debug_struct("LimitExceeded")
                .field("message", message)
                .finish(),
            Self::Authentication { message } => f
                .debug_struct("Authentication")
                .field("message", message)
                .finish(),
            Self::AccessDenied { message } => f
                .debug_struct("AccessDenied")
                .field("message", message)
                .finish(),
            Self::RequestLimitExceeded => f.write_str("RequestLimitExceeded"),
            Self::MissingAuthenticationToken => f.write_str("MissingAuthenticationToken"),
            Self::AwsService { code, message } => f
                .debug_struct("AwsService")
                .field("code", code)
                .field("message", message)
                .finish(),
        }
    }
}

err_context!(StorageError, StorageEnum);

impl StorageError {
    #[must_use]
    pub fn is_commit_unknown(&self) -> bool {
        self.attempt_kind() == Some(crate::AttemptKind::CommitUnknown)
    }

    /// The member reasons of a `TransactionCanceled`; `None` for any other
    /// error.
    #[must_use]
    pub fn cancellation_reasons(&self) -> Option<&[CancellationReason]> {
        match self.to_enum() {
            StorageEnum::TransactionCanceled { reasons } => Some(reasons),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_canceled_condition_check_failed(&self, index: usize) -> bool {
        self.cancellation_reasons()
            .and_then(|reasons| reasons.get(index))
            .is_some_and(CancellationReason::is_conditional_check_failed)
    }

    #[must_use]
    pub fn is_conditional_write_contention(&self) -> bool {
        match self.to_enum() {
            StorageEnum::ConditionalCheckFailed
            | StorageEnum::ConditionalCheckFailedWithItem { .. } => true,
            StorageEnum::TransactionCanceled { reasons } => {
                reasons
                    .iter()
                    .any(CancellationReason::is_conditional_check_failed)
                    && reasons.iter().all(|reason| {
                        matches!(
                            reason.code,
                            CancellationCode::None | CancellationCode::ConditionalCheckFailed
                        )
                    })
            }
            _ => false,
        }
    }

    /// The class every retry owner and wire mapping starts from.
    #[must_use]
    pub fn class(&self) -> ErrorClass {
        if let Some(kind) = self.attempt_kind() {
            return match kind {
                crate::AttemptKind::CommitUnknown => ErrorClass::CommitUnknown,
                crate::AttemptKind::Capacity | crate::AttemptKind::TransactionBytes => {
                    ErrorClass::Capacity
                }
                crate::AttemptKind::Conflict
                | crate::AttemptKind::FreshValidation
                | crate::AttemptKind::Unavailable
                | crate::AttemptKind::Deadline => ErrorClass::Retryable,
            };
        }
        match self.to_enum() {
            StorageEnum::TransactionInProgress { .. }
            | StorageEnum::ProvisionedThroughputExceeded { .. }
            | StorageEnum::Throttled { .. }
            | StorageEnum::ServiceUnavailable { .. }
            | StorageEnum::LimitExceeded { .. }
            | StorageEnum::RequestLimitExceeded => ErrorClass::Retryable,
            StorageEnum::ResourceNotFound { .. }
            | StorageEnum::ResourceExists { .. }
            | StorageEnum::IndexNotFound { .. }
            | StorageEnum::KeyValidation(_)
            | StorageEnum::HistoryExpired
            | StorageEnum::TrimmedDataAccess
            | StorageEnum::ConditionalCheckFailed
            | StorageEnum::ConditionalCheckFailedWithItem { .. }
            | StorageEnum::TableAlreadyExists { .. }
            | StorageEnum::TableNotFound { .. }
            | StorageEnum::Validation { .. }
            | StorageEnum::RawValidation { .. }
            | StorageEnum::DeletionProtectionEnabled { .. }
            | StorageEnum::IdempotentParameterMismatch
            | StorageEnum::TransactionCanceled { .. }
            | StorageEnum::Authentication { .. }
            | StorageEnum::AccessDenied { .. }
            | StorageEnum::MissingAuthenticationToken => ErrorClass::Client,
            StorageEnum::Database(_)
            | StorageEnum::AwsSerialization(_)
            | StorageEnum::Serialization(_)
            | StorageEnum::InfrastructureResult(_)
            | StorageEnum::InternalServerError { .. }
            | StorageEnum::HistoryUnavailable
            | StorageEnum::GuardConflict { .. }
            | StorageEnum::Unsupported { .. }
            | StorageEnum::IdempotencyReplay { .. }
            | StorageEnum::AwsService { .. }
            // Covered by `attempt_kind` above.
            | StorageEnum::Attempt(_)
            => ErrorClass::Internal,
        }
    }

    /// Fixed-cardinality name of this error for logs and metric labels. Never
    /// a message: safe on paths that handle item data.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self.to_enum() {
            StorageEnum::Database(_) => "database",
            StorageEnum::AwsSerialization(_) => "aws_serialization",
            StorageEnum::Serialization(_) => "serialization",
            StorageEnum::ResourceNotFound { .. } => "resource_not_found",
            StorageEnum::ResourceExists { .. } => "resource_exists",
            StorageEnum::IndexNotFound { .. } => "index_not_found",
            StorageEnum::KeyValidation(_) => "key_validation",
            StorageEnum::InternalServerError { .. } | StorageEnum::InfrastructureResult(_) => {
                "internal_server_error"
            }
            StorageEnum::HistoryUnavailable => "history_unavailable",
            StorageEnum::HistoryExpired => "history_expired",
            StorageEnum::TrimmedDataAccess => "trimmed_data_access",
            StorageEnum::GuardConflict { .. } => "guard_conflict",
            StorageEnum::Unsupported { .. } => "unsupported",
            StorageEnum::ConditionalCheckFailed
            | StorageEnum::ConditionalCheckFailedWithItem { .. } => "conditional_check_failed",
            StorageEnum::TableAlreadyExists { .. } => "table_exists",
            StorageEnum::TableNotFound { .. } => "table_not_found",
            StorageEnum::Validation { .. }
            | StorageEnum::RawValidation { .. }
            | StorageEnum::DeletionProtectionEnabled { .. } => "validation",
            StorageEnum::Attempt(failure) => failure.kind().as_str(),
            StorageEnum::TransactionInProgress { .. } => "transaction_in_progress",
            StorageEnum::IdempotentParameterMismatch => "idempotent_parameter_mismatch",
            StorageEnum::IdempotencyReplay { .. } => "idempotency_replay",
            StorageEnum::TransactionCanceled { .. } => "transaction_canceled",
            StorageEnum::ProvisionedThroughputExceeded { .. } => "provisioned_throughput",
            StorageEnum::Throttled { .. } => "throttled",
            StorageEnum::LimitExceeded { .. } => "limit_exceeded",
            StorageEnum::RequestLimitExceeded => "request_limit_exceeded",
            StorageEnum::ServiceUnavailable { .. } => "service_unavailable",
            StorageEnum::Authentication { .. } => "authentication",
            StorageEnum::MissingAuthenticationToken => "missing_authentication_token",
            StorageEnum::AccessDenied { .. } => "access_denied",
            StorageEnum::AwsService { .. } => "aws_service",
        }
    }

    pub fn validation(input: impl Into<StorageValidationInput>) -> Self {
        match input.into() {
            StorageValidationInput::Kind(kind) => Self::validation_detail(kind, kind.message()),
            StorageValidationInput::Message(message) => {
                Self::validation_detail(StorageValidationKind::Message, message)
            }
        }
    }

    pub(crate) fn validation_detail(
        kind: StorageValidationKind,
        message: impl Into<String>,
    ) -> Self {
        Self::from(StorageEnum::Validation {
            kind,
            message: message.into(),
        })
    }

    /// The kind and value type of an empty key attribute error.
    fn empty_key_kind(&self) -> Option<(StorageValidationKind, &'static str)> {
        match self.to_enum() {
            StorageEnum::Validation {
                kind: kind @ StorageValidationKind::EmptyStringKey,
                ..
            } => Some((*kind, "string")),
            StorageEnum::Validation {
                kind: kind @ StorageValidationKind::EmptyBinaryKey,
                ..
            } => Some((*kind, "binary")),
            _ => None,
        }
    }

    /// An empty value for a secondary index key attribute written by a put
    /// names the index; other errors pass through.
    #[must_use]
    pub fn for_index_key_put(self, index_name: &str, attribute: &str) -> Self {
        let Some((kind, value_type)) = self.empty_key_kind() else {
            return self;
        };
        Self::validation_detail(
            kind,
            format!(
                "One or more parameter values are not valid. A value specified for a secondary \
                 index key is not supported. The AttributeValue for a key attribute cannot \
                 contain an empty {value_type} value. IndexName: {index_name}, IndexKey: \
                 {attribute}"
            ),
        )
    }

    /// An empty value for a secondary index key attribute set by an update
    /// expression; other errors pass through.
    #[must_use]
    pub fn for_index_key_update(self) -> Self {
        let Some((kind, value_type)) = self.empty_key_kind() else {
            return self;
        };
        Self::validation_detail(
            kind,
            format!(
                "One or more parameter values are not valid. The update expression attempted to \
                 update a secondary index key to a value that is not supported. The \
                 AttributeValue for a key attribute cannot contain an empty {value_type} value."
            ),
        )
    }

    #[must_use]
    pub(crate) fn validation_kind(&self) -> StorageValidationKind {
        match self.to_enum() {
            StorageEnum::Validation { kind, .. } => *kind,
            _ => StorageValidationKind::Message,
        }
    }

    pub(crate) fn is_transact_get_key_error_top_level(&self) -> bool {
        matches!(
            self.validation_kind(),
            StorageValidationKind::EmptyStringKey | StorageValidationKind::EmptyBinaryKey
        )
    }

    pub(crate) fn is_transact_write_key_error_top_level(&self) -> bool {
        matches!(
            self.validation_kind(),
            StorageValidationKind::EmptyStringKey
                | StorageValidationKind::EmptyBinaryKey
                | StorageValidationKind::EmptyIndexer
                | StorageValidationKind::InvalidIndexerType
                | StorageValidationKind::InvalidNumber
                | StorageValidationKind::NumberPrecision
                | StorageValidationKind::NumberUnderflow
        )
    }

    /// An empty key attribute value rejected while a transaction member is
    /// evaluated, such as an empty secondary index key, fails the whole
    /// request as a validation error rather than cancelling the member.
    pub fn into_transact_empty_key_validation(self) -> Result<Self, Self> {
        if self.empty_key_kind().is_some() {
            Ok(self.into_raw_validation())
        } else {
            Err(self)
        }
    }

    #[must_use]
    pub(crate) fn into_raw_validation(self) -> Self {
        if !matches!(self.to_enum(), StorageEnum::Validation { .. }) {
            return self;
        }
        match self.into_enum() {
            StorageEnum::Validation { message, .. } => Self::raw_validation(message),
            error => error.into(),
        }
    }

    pub fn raw_validation(message: impl Into<String>) -> Self {
        Self::from(StorageEnum::RawValidation {
            message: message.into(),
        })
    }

    pub fn deletion_protection_enabled(table_name: &(impl ToString + ?Sized)) -> Self {
        Self::from(StorageEnum::DeletionProtectionEnabled {
            table_name: table_name.to_string(),
            message: "Resource cannot be deleted as it is currently protected against deletion. \
                      Disable deletion protection first."
                .to_string(),
        })
    }

    #[must_use]
    pub fn invalid_or_missing_key() -> Self {
        Self::validation(StorageValidationKind::InvalidOrMissingKey)
    }

    pub fn internal(message: &(impl ToString + ?Sized)) -> Self {
        // Display prints the message: it must not contain item data, keys or
        // secrets (ARCH-1-c 1).
        Self::from(StorageEnum::InternalServerError {
            message: message.to_string(),
        })
    }

    pub fn service_unavailable(retry_after_seconds: u64) -> Self {
        Self::from(StorageEnum::ServiceUnavailable {
            message: "Storage is temporarily unavailable.".to_string(),
            retry_after_seconds: retry_after_seconds.max(1),
        })
    }

    pub fn idempotent_parameter_mismatch() -> Self {
        Self::from(StorageEnum::IdempotentParameterMismatch)
    }

    pub fn idempotency_replay(response: Vec<u8>) -> Self {
        Self::from(StorageEnum::IdempotencyReplay { response })
    }

    pub fn guard_conflict(message: &(impl ToString + ?Sized)) -> Self {
        Self::from(StorageEnum::GuardConflict {
            message: message.to_string(),
        })
    }

    pub fn unsupported(message: &(impl ToString + ?Sized)) -> Self {
        Self::from(StorageEnum::Unsupported {
            message: message.to_string(),
        })
    }

    pub fn unsupported_custom_stream_duration() -> Self {
        Self::unsupported("custom stream duration is not supported by this storage provider")
    }

    /// Whether this is the item-over-the-size-limit refusal, which PutItem
    /// answers only after the table lookup.
    pub fn is_item_too_large(&self) -> bool {
        matches!(
            self.to_enum(),
            StorageEnum::Validation { message, .. }
                if message == crate::logical_item_size::ITEM_TOO_LARGE_MESSAGE
        )
    }

    pub fn table_already_exists(name: &(impl ToString + ?Sized)) -> Self {
        Self::from(StorageEnum::TableAlreadyExists {
            name: name.to_string(),
        })
    }

    pub fn table_not_found(name: &(impl ToString + ?Sized)) -> Self {
        let name = name.to_string();
        Self::from(StorageEnum::TableNotFound {
            message: dynamodb_table_not_found_message(&name),
            name,
        })
    }

    /// The single owner of the data-plane wire text: DynamoDB answers a
    /// missing table to item operations (GetItem, PutItem, UpdateItem,
    /// DeleteItem, Query, Scan, BatchGetItem, BatchWriteItem, TransactGetItems)
    /// with the bare "Requested resource not found". Control-plane operations
    /// keep the `Table: <name> not found` text of `table_not_found`.
    pub fn into_data_plane_table_not_found(self) -> Self {
        if let StorageEnum::TableNotFound { name, .. } = self.to_enum() {
            return Self::from(StorageEnum::ResourceNotFound {
                resource_type: "table",
                resource_id: name.clone(),
            });
        }
        self
    }

    pub fn index_not_found(
        index_name: &(impl ToString + ?Sized),
        table_name: &(impl ToString + ?Sized),
    ) -> Self {
        Self::from(StorageEnum::IndexNotFound {
            index_name: index_name.to_string(),
            table_name: table_name.to_string(),
        })
    }

    pub fn cursor_not_found(name: &(impl ToString + ?Sized)) -> Self {
        Self::from(StorageEnum::ResourceNotFound {
            resource_type: "cursor",
            resource_id: name.to_string(),
        })
    }

    pub fn cursor_already_exists(name: &(impl ToString + ?Sized)) -> Self {
        Self::from(StorageEnum::ResourceExists {
            resource_type: "cursor",
            resource_id: name.to_string(),
        })
    }
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

impl From<serde_json::Error> for StorageEnum {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error.into())
    }
}
