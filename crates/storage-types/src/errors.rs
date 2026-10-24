use std::fmt::Error;

use thiserror::Error;

use crate::{
    AttributeMap, InfrastructureResultKind, ItemKeyError, context::WrappedError as _,
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

#[derive(Error, Debug, Clone)]
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

    // Dynamo would also surface this as ResourceNotFoundException; keep for
    // internal diagnostics.
    #[error("Requested resource not found")]
    IndexNotFound {
        index_name: &'static str,
        table_name: String,
    },

    // Dynamo returns ValidationException with specific message for key errors;
    // we forward key error message.
    #[error("{0}")]
    KeyValidation(#[from] ItemKeyError),

    // InternalServerError (DynamoDB) canonical message.
    #[error("DynamoDB could not process your request.")]
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

    // ConditionalCheckFailedException canonical message.
    #[error("The conditional request failed")]
    ConditionalCheckFailed,

    // ConditionalCheckFailedException with the item requested by
    // ReturnValuesOnConditionCheckFailure.
    #[error("The conditional request failed")]
    ConditionalCheckFailedWithItem { item: AttributeMap },

    // Internal cache-authority guard conflict. Manager paths must translate
    // this to durable fallback, not Dynamo condition failure.
    #[error("Durable guard conflict.")]
    GuardConflict { message: String },

    // Internal provider capability boundary. Manager paths must fail closed
    // or use the existing durable path.
    #[error("Unsupported storage provider operation.")]
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

    // TransactionConflictException canonical message.
    #[error("Operation was rejected because there is an ongoing transaction for the item.")]
    TransactionConflict { message: String },

    // TransactionInProgressException canonical message.
    #[error("The transaction with the given request token is already in progress.")]
    TransactionInProgress { message: String },

    // IdempotentParameterMismatchException for a reused token with different
    // request parameters.
    #[error(
        "The request was rejected because the same client request token was used with different \
         parameters."
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
    TransactionCanceled { reasons: Vec<String> },

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

    /// Public compatibility for consumers that classify this error; ordinary
    /// and regional storage never originate it.
    #[error("Storage sync node is not the current leader.")]
    SyncNotLeader { leader_hint: Option<String> },

    #[error("Sync proposal outcome is ambiguous: {message}")]
    SyncSubmissionAmbiguous {
        proposal_id: Option<String>,
        message: String,
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

err_context!(StorageError, StorageEnum);

impl StorageError {
    #[must_use]
    pub fn is_commit_unknown(&self) -> bool {
        matches!(self.to_enum(), StorageEnum::SyncSubmissionAmbiguous { .. })
    }

    #[must_use]
    pub fn is_canceled_condition_check_failed(&self, index: usize) -> bool {
        let StorageEnum::TransactionCanceled { reasons } = self.to_enum() else {
            return false;
        };
        reasons.get(index).is_some_and(|reason| {
            reason == "ConditionalCheckFailed" || reason.starts_with("ConditionalCheckFailed\t")
        })
    }

    #[must_use]
    pub fn is_conditional_write_contention(&self) -> bool {
        match self.to_enum() {
            StorageEnum::ConditionalCheckFailed
            | StorageEnum::ConditionalCheckFailedWithItem { .. } => true,
            StorageEnum::TransactionCanceled { reasons } => {
                let mut has_conditional_failure = false;
                let only_conditional_reasons = reasons.iter().all(|reason| {
                    if reason == "None" {
                        return true;
                    }
                    if reason == "ConditionalCheckFailed"
                        || reason.starts_with("ConditionalCheckFailed\t")
                    {
                        has_conditional_failure = true;
                        return true;
                    }
                    false
                });
                only_conditional_reasons && has_conditional_failure
            }
            _ => false,
        }
    }

    #[must_use]
    pub fn is_retryable_write(&self) -> bool {
        matches!(
            self.to_enum(),
            StorageEnum::TransactionConflict { .. }
                | StorageEnum::TransactionInProgress { .. }
                | StorageEnum::ProvisionedThroughputExceeded { .. }
                | StorageEnum::Throttled { .. }
                | StorageEnum::RequestLimitExceeded
                | StorageEnum::LimitExceeded { .. }
        )
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
        // Message stored for diagnostics but not exposed in Display (AWS
        // parity).
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

    pub fn sync_not_leader(leader_hint: Option<String>) -> Self {
        Self::from(StorageEnum::SyncNotLeader { leader_hint })
    }

    pub fn sync_submission_ambiguous(
        proposal_id: Option<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::from(StorageEnum::SyncSubmissionAmbiguous {
            proposal_id,
            message: message.into(),
        })
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
