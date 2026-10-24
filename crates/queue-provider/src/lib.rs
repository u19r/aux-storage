//! Queue provider traits, types, and helpers.
//!
//! This is the stable provider surface for queue consumers.

mod config;
#[cfg(test)]
mod config_tests;
mod constants;
#[cfg(test)]
mod constants_tests;
mod errors;
mod execution_inputs;
#[cfg(test)]
mod execution_inputs_tests;
mod newtypes;
mod operations_provider;
mod provider;
mod query_protocol;
#[cfg(test)]
mod query_protocol_tests;
mod queue_catalog;
mod queue_cleanup;
mod queue_cleanup_selection;
mod queue_creation;
mod queue_message;
mod queue_operation;
mod queue_operations;
mod queue_port;
mod queue_publication;
mod queue_receipt;
mod queue_receive;
mod queue_send;
mod request_fields;
mod request_validation;
mod serde_types;
mod types;
mod wire_request;

pub use config::*;
pub use constants::{
    DEFAULT_RECEIVE_MAX_MESSAGES, DEFAULT_RECEIVE_VISIBILITY_TIMEOUT_SECS,
    DEFAULT_RECEIVE_WAIT_TIME_SECONDS, MAX_DELAY_SECONDS, MAX_MESSAGE_RETENTION_SECONDS,
    MAX_VISIBILITY_TIMEOUT_SECONDS, MAX_WAIT_TIME_SECONDS, MESSAGE_ID_VERSIONSTAMP_LEN,
    SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE, SQS_INVALID_ACTION_ERROR_TYPE,
    SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE, SQS_MISSING_PARAMETER_ERROR_TYPE,
    SQS_NON_EXISTENT_QUEUE_ERROR_TYPE, sqs_json_error_type,
};
pub use errors::{QueueError, QueueInternalKind, QueueResult, QueueValidationKind};
pub use execution_inputs::{ChangeMessageVisibilityInput, ReceiveMessagesInput};
pub use newtypes::*;
pub use provider::QueueProvider;
pub use query_protocol::query_fields_to_json;
pub use queue_catalog::{
    QueueCatalogPage, QueueCatalogRecord, QueueCatalogRequest, QueueIncarnation, read_queue_by_url,
};
pub use queue_cleanup::QueueMessageCleanupHandler;
pub use queue_cleanup_selection::{QueueCleanupInput, QueueCleanupSelection};
pub use queue_creation::{QueueCreation, create_queue_in_attempt};
pub use queue_message::{
    QueueCleanupCursor, QueueCleanupPage, QueueCleanupRequest, QueueDueRequest,
    QueueMessageCleanupRepository, QueueMessageRecord, cleanup_queue_messages_page,
};
pub use queue_operation::QueueOperation;
pub use queue_operations::QueueOperations;
pub use queue_port::{QueueAttempt, QueuePort};
pub use queue_publication::PublishMessageRequest;
pub use queue_receipt::QueueReceiptAction;
pub use queue_receive::PreparedReceiveMessages;
pub use types::*;
pub use wire_request::{
    IntoValidatedQueueRequest, QueueAction, QueueRequest, decode_json_request, decode_value_request,
};

#[cfg(test)]
mod errors_tests;
#[cfg(test)]
mod newtypes_perf_tests;
#[cfg(test)]
mod newtypes_tests;
#[cfg(test)]
mod types_tests;
#[cfg(test)]
mod wire_request_tests;
