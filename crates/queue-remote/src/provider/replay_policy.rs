//! What a transport failure of one remote queue request means for the caller.
//!
//! The client sends each request once. A write that may have reached the
//! server may have applied: it answers an unknown outcome, never a known
//! failure. A request known not sent, or one without effect, answers a
//! retryable known abort.

use http_request::HttpRequestDispatchStatus;
use queue_provider::QueueError;
use storage_types::{AttemptFailure, NativeUnavailable, StorageError};

/// Fixed reason of the unknown outcome that a write answers.
pub(crate) const REMOTE_QUEUE_WRITE_OUTCOME_UNKNOWN: &str = "remote queue write may have applied";

/// Targets without effect on the server: a repeat answers what the first send
/// would have answered. Every other target, and any unlisted one, may change
/// state (`ReceiveMessage` hides the messages it returns).
pub(crate) fn is_read_only(target: &str) -> bool {
    matches!(
        target,
        "AmazonSQS.GetQueueAttributes"
            | "AmazonSQS.GetQueueUrl"
            | "AmazonSQS.ListDeadLetterSourceQueues"
            | "AmazonSQS.ListQueueTags"
            | "AmazonSQS.ListQueues"
    )
}

/// The answer to a transport failure of one send of `target`.
pub(crate) fn transport_failure(target: &str, dispatch: HttpRequestDispatchStatus) -> QueueError {
    let error =
        if dispatch == HttpRequestDispatchStatus::PossiblyDispatched && !is_read_only(target) {
            StorageError::commit_unknown(REMOTE_QUEUE_WRITE_OUTCOME_UNKNOWN)
        } else {
            AttemptFailure::Unavailable(NativeUnavailable::ConnectionLost).into()
        };
    QueueError::StorageError(error)
}
