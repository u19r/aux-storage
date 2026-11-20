//! Queue provider traits, types, and helpers.
//!
//! This is the stable provider surface for queue consumers.

mod constants;
#[cfg(test)]
mod constants_tests;
mod errors;
mod execution_inputs;
#[cfg(test)]
mod execution_inputs_tests;
mod integer_member;
mod newtypes;
mod page_token;
mod queue_arn;
mod queue_catalog;
mod queue_catalog_hint;
#[cfg(test)]
mod queue_catalog_hint_tests;
mod queue_cleanup;
mod queue_cleanup_selection;
mod queue_creation;
mod queue_message;
#[cfg(test)]
mod queue_message_tests;
mod queue_operation;
mod queue_operations;
mod queue_port;
mod queue_publication;
mod queue_receipt;
mod queue_receive;
#[cfg(test)]
mod queue_receive_tests;
mod queue_redrive;
mod queue_send;
mod queue_wake;
mod request_validation;
mod tag_rules;
mod types;
pub mod wire;

pub use constants::{
    DEFAULT_MAX_CLOCK_SKEW_MILLIS, DEFAULT_RECEIVE_MAX_MESSAGES,
    DEFAULT_RECEIVE_VISIBILITY_TIMEOUT_SECS, DEFAULT_RECEIVE_WAIT_TIME_SECONDS,
    INVALID_NEXT_TOKEN_MESSAGE, MAX_BATCH_PAYLOAD_BYTES, MAX_DEAD_LETTER_SOURCE_RESULTS,
    MAX_DELAY_SECONDS, MAX_LIST_QUEUES_RESULTS, MAX_MESSAGE_RETENTION_SECONDS,
    MAX_VISIBILITY_TIMEOUT_SECONDS, MAX_WAIT_TIME_SECONDS, MESSAGE_ID_LEN, MESSAGE_ID_TEXT_LEN,
    QUEUE_ATTRIBUTE_DEFAULTS, QUEUE_CLAIM_WINDOW, QUEUE_COUNT_SCAN_ENTRIES,
    QUEUE_REDRIVE_POLICY_ATTRIBUTE, QUEUE_WAKE_SLOTS, SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE,
    SQS_BATCH_REQUEST_TOO_LONG_ERROR_TYPE, SQS_INVALID_ACTION_ERROR_TYPE,
    SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE, SQS_MISSING_PARAMETER_ERROR_TYPE,
    SQS_NON_EXISTENT_QUEUE_ERROR_TYPE, SQS_SERVICE_UNAVAILABLE_ERROR_TYPE, TRACE_HEADER_ATTRIBUTE,
    sqs_json_error_type,
};
pub use errors::{
    QueueError, QueueInternalKind, QueueResult, QueueValidationKind, receipt_handle_invalid_message,
};
pub use execution_inputs::{ChangeMessageVisibilityInput, ReceiveMessagesInput};
pub use newtypes::*;
pub use page_token::{decode_page_token, encode_page_token};
pub use queue_arn::QueueArn;
pub use queue_catalog::{
    QueueCatalogPage, QueueCatalogRecord, QueueCatalogRequest, QueueCountFacts, QueueId,
    QueueIncarnation, QueueMessageCounts, get_queue_with_bounded_counts, queue_name_of,
    read_queue_by_url,
};
pub use queue_catalog_hint::{QUEUE_CATALOG_HINT_MILLIS, QueueCatalogHints};
pub use queue_cleanup::QueueMessageCleanupHandler;
pub use queue_cleanup_selection::{QueueCleanupInput, QueueCleanupSelection};
pub use queue_creation::{QueueCreation, create_queue_in_attempt};
pub use queue_message::{
    QueueCleanupCursor, QueueCleanupPage, QueueCleanupRequest, QueueDueBatch, QueueDueMark,
    QueueDueRequest, QueueDueSelection, QueueMessageBody, QueueMessageCleanupRepository,
    QueueMessageRecord, QueueMessageState, cleanup_queue_messages_page,
};
pub use queue_operation::QueueOperation;
pub use queue_operations::QueueOperations;
pub use queue_port::{QueueAttempt, QueuePort};
pub use queue_publication::PublishMessageRequest;
pub use queue_receipt::QueueReceiptAction;
pub use queue_receive::{ReceiveProbe, ReceivedMessages};
pub use queue_redrive::{RedrivePolicy, check_redrive_identity};
pub use queue_wake::{
    QueueWakeBoard, QueueWakeInterest, QueueWakeMark, QueueWakeSlot, QueueWakeSlots, QueueWakeTasks,
};
pub use request_validation::sqs_validation_error_type;
pub use tag_rules::{TagRuleViolation, apply_tag_change};
pub use types::*;
pub use wire::{QueueAction, QueueRequest, QueueResponse, ValidatedQueueRequest};

#[cfg(test)]
mod errors_tests;
#[cfg(test)]
mod newtypes_perf_tests;
#[cfg(test)]
mod newtypes_tests;
#[cfg(test)]
mod types_tests;

#[cfg(test)]
mod queue_wake_tests;
