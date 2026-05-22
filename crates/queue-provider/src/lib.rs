//! Queue provider traits, types, and helpers.
//!
//! This is the stable provider surface for queue consumers.

mod config;
mod constants;
mod errors;
mod newtypes;
mod provider;
mod request_fields;
mod request_validation;
mod serde_types;
mod types;

pub use config::*;
pub use constants::{
    SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE, SQS_INTERNAL_ERROR_TYPE,
    SQS_INVALID_ACTION_ERROR_TYPE, SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
    SQS_MISSING_PARAMETER_ERROR_TYPE, SQS_NON_EXISTENT_QUEUE_ERROR_TYPE,
    SQS_QUEUE_NAME_EXISTS_ERROR_TYPE, SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE, sqs_json_error_type,
};
pub use errors::{QueueError, QueueInternalKind, QueueResult, QueueValidationKind};
pub use newtypes::*;
pub use provider::QueueProvider;
pub use types::*;

#[cfg(test)]
mod errors_tests;
#[cfg(test)]
mod newtypes_perf_tests;
#[cfg(test)]
mod newtypes_tests;
#[cfg(test)]
mod types_tests;
