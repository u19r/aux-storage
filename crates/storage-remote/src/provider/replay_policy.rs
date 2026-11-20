//! The one replay table of the remote client: may a request be sent again
//! after a failed attempt?
//!
//! An attempt that may have reached the server may have applied its request.
//! A `SendOnce` request then answers `CommitUnknown` and is never sent again;
//! an attempt known not to have applied may be retried.

use http_request::HttpRequestDispatchStatus;
use storage_types::{
    AttemptFailure, STORAGE_NOT_LEADER_ERROR_TYPE, StorageEnum, context::WrappedError as _,
};

use super::implementation::RemoteInvocationError;

/// Fixed reason of the unknown outcome that a `SendOnce` request answers.
pub(super) const REMOTE_WRITE_OUTCOME_UNKNOWN: &str = "remote write may have applied";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReplayPolicy {
    /// Has no effect, or sets the complete state the request names: a repeat
    /// answers what the first send would have answered.
    Repeatable,
    /// May have applied once it may have reached the server. Sent again only
    /// after an attempt known not to have applied.
    SendOnce,
}

/// What the invocation loop does after one failed attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AfterFailure {
    Retry,
    Return,
    CommitUnknown,
}

impl ReplayPolicy {
    /// The table. Writes (`PutItem`, `UpdateItem`, `DeleteItem`,
    /// `BatchWriteItem`, `TransactWriteItems` with or without a token,
    /// `CreateTable`, `DeleteTable`, `UpdateTable`, `UpdateTimeToLive`) and any
    /// unlisted target are `SendOnce`.
    pub(super) fn for_target(target: &str) -> Self {
        match target {
            "DynamoDB_20120810.BatchGetItem"
            | "DynamoDB_20120810.DescribeTable"
            | "DynamoDB_20120810.DescribeTimeToLive"
            | "DynamoDB_20120810.GetItem"
            | "DynamoDB_20120810.GetStreamRecords"
            | "DynamoDB_20120810.ListTables"
            | "DynamoDB_20120810.Query"
            | "DynamoDB_20120810.QuerySourcePage"
            | "DynamoDB_20120810.ReadSequence"
            | "DynamoDB_20120810.Scan"
            | "DynamoDB_20120810.TransactGetItems"
            | "DynamoDB_20120810.UpdateContinuousBackups"
            | "DynamoDBStreams_20120810.DescribeStream"
            | "DynamoDBStreams_20120810.GetRecords"
            | "DynamoDBStreams_20120810.GetShardIterator"
            | "DynamoDBStreams_20120810.ListStreams" => Self::Repeatable,
            _ => Self::SendOnce,
        }
    }

    /// Classifies one failed attempt. `attempt` carries that attempt's own
    /// dispatch status, not the status of earlier attempts.
    pub(super) fn classify_failure(
        self,
        attempt: &RemoteInvocationError,
        retryable: bool,
    ) -> AfterFailure {
        if self == Self::SendOnce && attempt.is_ambiguous() {
            return AfterFailure::CommitUnknown;
        }
        if retryable {
            AfterFailure::Retry
        } else {
            AfterFailure::Return
        }
    }
}

impl RemoteInvocationError {
    /// The request may have reached the server and may have applied.
    pub(super) fn is_ambiguous(&self) -> bool {
        self.dispatch_status == HttpRequestDispatchStatus::PossiblyDispatched
            && !self.is_known_not_applied()
    }

    /// A typed answer the server gives only before it applies anything.
    fn is_known_not_applied(&self) -> bool {
        match self.error.to_enum() {
            StorageEnum::AwsService {
                code: Some(code), ..
            } => {
                code.as_str() == STORAGE_NOT_LEADER_ERROR_TYPE
                    || code.as_str() == "ExpiredTokenException"
                    || code.as_str() == "TransactionCanceledException"
            }
            StorageEnum::KeyValidation(_)
            | StorageEnum::ResourceNotFound { .. }
            | StorageEnum::IndexNotFound { .. }
            | StorageEnum::Validation { .. }
            | StorageEnum::RawValidation { .. }
            | StorageEnum::Unsupported { .. }
            | StorageEnum::TableNotFound { .. }
            | StorageEnum::TableAlreadyExists { .. }
            | StorageEnum::AccessDenied { .. }
            | StorageEnum::Authentication { .. }
            | StorageEnum::MissingAuthenticationToken
            | StorageEnum::TransactionCanceled { .. }
            | StorageEnum::Attempt(AttemptFailure::Conflict(_))
            | StorageEnum::ConditionalCheckFailed
            | StorageEnum::ConditionalCheckFailedWithItem { .. }
            | StorageEnum::IdempotentParameterMismatch
            | StorageEnum::AwsSerialization(_)
            | StorageEnum::Database(_)
            // Typed refusals: answered before anything is applied (ARCH-2-f:
            // a 503 says nothing committed). A client deadline is
            // `Attempt(Unavailable(Deadline))`, never `ServiceUnavailable`.
            | StorageEnum::ServiceUnavailable { .. }
            | StorageEnum::Throttled { .. }
            | StorageEnum::ProvisionedThroughputExceeded { .. }
            | StorageEnum::RequestLimitExceeded
            | StorageEnum::LimitExceeded { .. }
            | StorageEnum::TransactionInProgress { .. } => true,
            _ => false,
        }
    }
}
