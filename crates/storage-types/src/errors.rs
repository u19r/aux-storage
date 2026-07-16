use std::fmt::Error;

use thiserror::Error;

use crate::{
    AttributeMap, ItemKeyError, context::WrappedError as _, dynamodb_table_not_found_message,
    err_context,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageValidationKind {
    InvalidConditionExpression,
    InvalidOrMissingKey,
    BeginsWithRequiresString,
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

#[derive(Error, Debug)]
pub enum StorageEnum {
    // Not a DynamoDB error type – internal helper. Keep message but mark internal.
    #[error("Database error: {0}")]
    Database(#[from] Error),

    #[error("AWS serialization error: {0}")]
    AwsSerialization(String),

    // Not a DynamoDB error type – internal helper.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    // Maps to ResourceNotFoundException (DynamoDB). Dynamo message is generic; we store context
    // separately.
    #[error("Requested resource not found")]
    ResourceNotFound {
        resource_type: &'static str,
        resource_id: String,
    },

    // Used for internal resources (e.g. cursor) – no direct Dynamo analogue. Use Dynamo wording
    // when surfaced via DynamoDB-compatible APIs.
    #[error("The resource which you are attempting to change is in use.")]
    ResourceExists {
        resource_type: &'static str,
        resource_id: String,
    },

    // Dynamo would also surface this as ResourceNotFoundException; keep for internal diagnostics.
    #[error("Requested resource not found")]
    IndexNotFound {
        index_name: &'static str,
        table_name: String,
    },

    // Dynamo returns ValidationException with specific message for key errors; we forward key
    // error message.
    #[error("{0}")]
    KeyValidation(#[from] ItemKeyError),

    // InternalServerError (DynamoDB) canonical message.
    #[error("DynamoDB could not process your request.")]
    InternalServerError { message: String },

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

    // ValidationException – DO NOT prefix with 'Validation error:' to keep exact message parity.
    #[error("{message}")]
    Validation { message: String },

    // ValidationException with a DynamoDB message that must not be normalized by API adapters.
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

    // TransactionCanceledException canonical message (British spelling 'cancelled' matches AWS
    // response text).
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
    pub fn is_retryable_write(&self) -> bool {
        matches!(
            self.to_enum(),
            StorageEnum::TransactionConflict { .. }
                | StorageEnum::TransactionInProgress { .. }
                | StorageEnum::TransactionCanceled { .. }
                | StorageEnum::ProvisionedThroughputExceeded { .. }
                | StorageEnum::Throttled { .. }
                | StorageEnum::RequestLimitExceeded
                | StorageEnum::LimitExceeded { .. }
        )
    }

    pub fn validation(input: impl Into<StorageValidationInput>) -> Self {
        let message = match input.into() {
            StorageValidationInput::Kind(kind) => kind.message().to_string(),
            StorageValidationInput::Message(message) => message,
        };
        Self::Base(StorageEnum::Validation { message })
    }

    pub fn raw_validation(message: impl Into<String>) -> Self {
        Self::Base(StorageEnum::RawValidation {
            message: message.into(),
        })
    }

    pub fn deletion_protection_enabled(table_name: &(impl ToString + ?Sized)) -> Self {
        Self::Base(StorageEnum::DeletionProtectionEnabled {
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
        // Message stored for diagnostics but not exposed in Display (AWS parity).
        Self::Base(StorageEnum::InternalServerError {
            message: message.to_string(),
        })
    }

    pub fn guard_conflict(message: &(impl ToString + ?Sized)) -> Self {
        Self::Base(StorageEnum::GuardConflict {
            message: message.to_string(),
        })
    }

    pub fn unsupported(message: &(impl ToString + ?Sized)) -> Self {
        Self::Base(StorageEnum::Unsupported {
            message: message.to_string(),
        })
    }

    pub fn unsupported_custom_stream_duration() -> Self {
        Self::unsupported("custom stream duration is not supported by this storage provider")
    }

    pub fn table_already_exists(name: &(impl ToString + ?Sized)) -> Self {
        Self::Base(StorageEnum::TableAlreadyExists {
            name: name.to_string(),
        })
    }

    pub fn table_not_found(name: &(impl ToString + ?Sized)) -> Self {
        let name = name.to_string();
        Self::Base(StorageEnum::TableNotFound {
            message: dynamodb_table_not_found_message(&name),
            name,
        })
    }

    pub fn cursor_not_found(name: &(impl ToString + ?Sized)) -> Self {
        Self::Base(StorageEnum::ResourceNotFound {
            resource_type: "cursor",
            resource_id: name.to_string(),
        })
    }

    pub fn cursor_already_exists(name: &(impl ToString + ?Sized)) -> Self {
        Self::Base(StorageEnum::ResourceExists {
            resource_type: "cursor",
            resource_id: name.to_string(),
        })
    }
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;
