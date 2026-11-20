use std::{num::NonZeroU32, time::Duration};

use storage_types::{
    AttemptDurability, AttemptMode, AttemptPlan, DeadlineClass, NativeWork, WriteRetryPolicy,
};

pub(crate) const CATALOG_PAGE_ITEMS: NonZeroU32 = NonZeroU32::MIN.saturating_add(100 - 1);
pub(crate) const QUEUE_WRITE_RETRY: WriteRetryPolicy =
    WriteRetryPolicy::new(5, Duration::from_millis(2));

/// Catalog change (create, tags, delete, purge, attributes): protected read and
/// write.
pub(crate) const CATALOG_WRITE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::SchemaChange)
        .retrying(QUEUE_WRITE_RETRY);
/// One catalog row.
pub(crate) const CATALOG_POINT_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).retrying(QUEUE_WRITE_RETRY);
/// Catalog pages, dead-letter sources, and the counts of GetQueueAttributes:
/// range reads.
pub(crate) const CATALOG_RANGE_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::RangeRead)
        .retrying(QUEUE_WRITE_RETRY)
        .work(NativeWork::RangeRead);
/// One page of scheduled message cleanup.
pub(crate) const CLEANUP_PAGE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::BackgroundPage)
        .retrying(QUEUE_WRITE_RETRY);
/// Queue defaults and due head for a waiting receive; a hint, so Relaxed (rule
/// DUR-F6).
pub(crate) const RECEIVE_PROBE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::LongPollProbe)
        .retrying(QUEUE_WRITE_RETRY)
        .with_durability(AttemptDurability::Relaxed);
/// A claim of due messages (decision D11: Relaxed).
pub(crate) const MESSAGE_CLAIM: AttemptPlan = message_request(1, AttemptDurability::Relaxed);
/// A publication with a stable identity.
pub(crate) const MESSAGE_PUBLISH: AttemptPlan = message_request(1, AttemptDurability::Durable);

/// A message operation (decisions D1, D2, D11): an item attempt whose writes
/// all target rows it protects first. `Relaxed` only for operations that a
/// standard queue may repeat after a crash. A batch takes the transaction
/// budget.
pub(crate) const fn message_request(actions: usize, durability: AttemptDurability) -> AttemptPlan {
    let class = if actions > 1 {
        DeadlineClass::Transaction
    } else {
        DeadlineClass::Write
    };
    AttemptPlan::new(AttemptMode::ReadWriteItems, class)
        .retrying(QUEUE_WRITE_RETRY)
        .with_durability(durability)
}
/// Due-index entries that one ordered-key claim reads with a snapshot read
/// (about 50 KB, one range read).
pub const QUEUE_CLAIM_WINDOW: NonZeroU32 = NonZeroU32::MIN.saturating_add(1024 - 1);
/// Entries after the claim time that a claim or probe reads to report the
/// next due time.
pub(crate) const QUEUE_DUE_HEAD: NonZeroU32 = NonZeroU32::MIN.saturating_add(8 - 1);
/// Stored payload bytes of dead-letter copies that one claim may insert (W7-0,
/// W7-A). FoundationDB rejects a transaction above 10,000,000 bytes.
pub(crate) const QUEUE_CLAIM_WRITE_BYTES: usize = 7 * 1024 * 1024;
/// Largest checkpoint data of one message (W7). The state record that holds
/// it is never chunked on KV backends (FoundationDB inline value: 100,000
/// bytes).
pub(crate) const MAX_CHECKPOINT_DATA_BYTES: usize = 64 * 1024;
/// Largest escaped JSON form of a checkpoint (W7): the state is stored as JSON,
/// so quotes, backslashes and control characters grow it.
pub(crate) const MAX_CHECKPOINT_JSON_BYTES: usize = 96 * 1024;
/// Each of the two count regions stops after this many entries, so
/// `ApproximateNumberOf*` values stop at this bound.
pub const QUEUE_COUNT_SCAN_ENTRIES: NonZeroU32 = NonZeroU32::MIN.saturating_add(100_000 - 1);
/// Default largest clock difference between nodes (decision D14).
pub const DEFAULT_MAX_CLOCK_SKEW_MILLIS: i64 = 500;
/// SQS limit on the sum of body and attribute bytes of one batch request.
pub const MAX_BATCH_PAYLOAD_BYTES: usize = 1_048_576;
/// Every name that `GetQueueAttributes` accepts; another name is
/// `InvalidAttributeName` (probe 7).
pub const QUEUE_ATTRIBUTE_NAMES: &[&str] = &[
    "All",
    "QueueArn",
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible",
    "ApproximateNumberOfMessagesDelayed",
    "CreatedTimestamp",
    "LastModifiedTimestamp",
    "VisibilityTimeout",
    "MaximumMessageSize",
    "MessageRetentionPeriod",
    "DelaySeconds",
    "ReceiveMessageWaitTimeSeconds",
    "Policy",
    "RedrivePolicy",
    "RedriveAllowPolicy",
    "KmsMasterKeyId",
    "KmsDataKeyReusePeriodSeconds",
    "SqsManagedSseEnabled",
    "FifoQueue",
    "ContentBasedDeduplication",
    "DeduplicationScope",
    "FifoThroughputLimit",
];
/// Queue attribute that holds the dead-letter policy in its normal form.
pub const QUEUE_REDRIVE_POLICY_ATTRIBUTE: &str = "RedrivePolicy";
/// SQS bound of `maxReceiveCount` in a redrive policy.
pub(crate) const MAX_REDRIVE_RECEIVE_COUNT: u32 = 1_000;
/// The one message system attribute that a send may set (AWS documentation).
pub const TRACE_HEADER_ATTRIBUTE: &str = "AWSTraceHeader";
/// Message system attribute that names the queue a dead-letter move came from.
pub(crate) const DEAD_LETTER_SOURCE_ARN_ATTRIBUTE: &str = "DeadLetterQueueSourceArn";
/// `ListQueues` bound of `MaxResults`, and its page size when `MaxResults` is
/// 0 or absent.
pub const MAX_LIST_QUEUES_RESULTS: NonZeroU32 = NonZeroU32::MIN.saturating_add(1_000 - 1);
/// Probe 7: the text of a token that does not continue the listing.
pub const INVALID_NEXT_TOKEN_MESSAGE: &str = "Invalid or expired next token.";
/// `ListDeadLetterSourceQueues` bound of `MaxResults`.
pub const MAX_DEAD_LETTER_SOURCE_RESULTS: NonZeroU32 = NonZeroU32::MIN.saturating_add(1_000 - 1);
/// Catalog rows that one `ListDeadLetterSourceQueues` call reads at most; a
/// call that stops at this bound returns a `NextToken`.
pub(crate) const DEAD_LETTER_SOURCE_SCAN_ROWS: NonZeroU32 =
    NonZeroU32::MIN.saturating_add(1_000 - 1);

/// A message id is the 16 bytes of a UUID. Ordinary sends use a UUIDv7, so ids
/// sort by creation time; stable publications choose their own UUID.
pub const MESSAGE_ID_LEN: usize = 16;
/// Characters of the hyphenated UUID text that the wire carries.
pub const MESSAGE_ID_TEXT_LEN: usize = 36;

pub const MAX_DELAY_SECONDS: u32 = 900;
pub const MIN_MAXIMUM_MESSAGE_SIZE_BYTES: u32 = 1_024;
pub const MAX_MAXIMUM_MESSAGE_SIZE_BYTES: u32 = 1_048_576;
pub const MIN_MESSAGE_RETENTION_SECONDS: u32 = 60;
pub const MAX_MESSAGE_RETENTION_SECONDS: u32 = 1_209_600;
pub const MAX_RECEIVE_MESSAGES: u32 = 10;
/// Aux bounds, not probed: values that a catalog row or message returns.
pub(crate) const MAX_KMS_MASTER_KEY_ID_BYTES: usize = 2_048;
pub(crate) const MAX_TRACE_HEADER_BYTES: usize = 2_048;
/// Serialized attributes and tags of one catalog row: a FoundationDB value
/// is at most 100 KB, and listings read whole rows.
pub(crate) const MAX_QUEUE_ROW_SETTINGS_BYTES: usize = 90 * 1_024;
/// `KmsDataKeyReusePeriodSeconds` bounds (AWS documentation).
pub const MIN_KMS_DATA_KEY_REUSE_SECONDS: u32 = 60;
pub const MAX_KMS_DATA_KEY_REUSE_SECONDS: u32 = 86_400;
/// SQS receive default: one message when `MaxNumberOfMessages` is omitted.
pub const DEFAULT_RECEIVE_MAX_MESSAGES: u32 = 1;
/// SQS default visibility timeout (seconds) when `VisibilityTimeout` is
/// omitted.
pub const DEFAULT_RECEIVE_VISIBILITY_TIMEOUT_SECS: u32 = 30;
/// SQS default wait time (seconds) when `WaitTimeSeconds` is omitted.
pub const DEFAULT_RECEIVE_WAIT_TIME_SECONDS: u32 = 0;
/// The value of each settable queue attribute that a queue without a stored
/// value reads (probe 7 for `SqsManagedSseEnabled`). `GetQueueAttributes`
/// reports them, and `CreateQueue` compares attributes with them applied.
pub const QUEUE_ATTRIBUTE_DEFAULTS: [(&str, &str); 6] = [
    ("DelaySeconds", "0"),
    ("VisibilityTimeout", "30"),
    ("MaximumMessageSize", "1048576"),
    ("MessageRetentionPeriod", "345600"),
    ("ReceiveMessageWaitTimeSeconds", "0"),
    ("SqsManagedSseEnabled", "true"),
];
/// Queue attribute that supplies an omitted receive `VisibilityTimeout`.
pub(crate) const QUEUE_VISIBILITY_TIMEOUT_ATTRIBUTE: &str = "VisibilityTimeout";
/// Queue attribute that supplies an omitted receive `WaitTimeSeconds`.
pub(crate) const QUEUE_RECEIVE_WAIT_TIME_ATTRIBUTE: &str = "ReceiveMessageWaitTimeSeconds";
pub const MAX_MESSAGE_ATTRIBUTES: usize = 10;
pub const MAX_WAIT_TIME_SECONDS: u32 = 20;
pub const MAX_VISIBILITY_TIMEOUT_SECONDS: u32 = 43_200;

/// Query code of a `CreateQueue` name clash; the JSON type is
/// `QueueNameExists`.
pub const SQS_QUEUE_ALREADY_EXISTS_ERROR_TYPE: &str = "QueueAlreadyExists";
pub const SQS_NON_EXISTENT_QUEUE_ERROR_TYPE: &str = "AWS.SimpleQueueService.NonExistentQueue";
pub const SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE: &str = "ReceiptHandleIsInvalid";
pub const SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE: &str = "InvalidParameterValue";
pub const SQS_INVALID_ACTION_ERROR_TYPE: &str = "InvalidAction";
pub const SQS_MISSING_PARAMETER_ERROR_TYPE: &str = "MissingParameter";
pub const SQS_INVALID_ATTRIBUTE_NAME_ERROR_TYPE: &str = "InvalidAttributeName";
pub const SQS_INVALID_ATTRIBUTE_VALUE_ERROR_TYPE: &str = "InvalidAttributeValue";
pub const SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE: &str =
    "AWS.SimpleQueueService.BatchEntryIdsNotDistinct";
pub const SQS_INVALID_BATCH_ENTRY_ID_ERROR_TYPE: &str =
    "AWS.SimpleQueueService.InvalidBatchEntryId";
pub const SQS_INVALID_MESSAGE_CONTENTS_ERROR_TYPE: &str = "InvalidMessageContents";
/// The `Failed` code of a batch send entry with an empty body.
pub(crate) const SQS_EMPTY_VALUE_ERROR_TYPE: &str = "EmptyValue";
pub const SQS_INTERNAL_ERROR_TYPE: &str = "InternalError";
pub const SQS_INTERNAL_ERROR_MESSAGE: &str = "An internal error occurred.";
pub const SQS_BATCH_REQUEST_TOO_LONG_ERROR_TYPE: &str =
    "AWS.SimpleQueueService.BatchRequestTooLong";
pub const SQS_SERVICE_UNAVAILABLE_ERROR_TYPE: &str = "ServiceUnavailable";
pub const SQS_VISIBILITY_MESSAGE_NOT_AVAILABLE_MESSAGE: &str =
    "Message does not exist or is not available for visibility timeout change";
pub const SQS_RECEIPT_HANDLE_INVALID_MESSAGE: &str =
    "The input receipt handle is not a valid receipt handle.";

#[must_use]
pub fn sqs_json_error_type(query_error_type: &str) -> &'static str {
    match query_error_type {
        SQS_NON_EXISTENT_QUEUE_ERROR_TYPE => "com.amazonaws.sqs#QueueDoesNotExist",
        SQS_QUEUE_ALREADY_EXISTS_ERROR_TYPE => "com.amazonaws.sqs#QueueNameExists",
        SQS_INVALID_ATTRIBUTE_NAME_ERROR_TYPE => "com.amazonaws.sqs#InvalidAttributeName",
        SQS_INVALID_ATTRIBUTE_VALUE_ERROR_TYPE => "com.amazonaws.sqs#InvalidAttributeValue",
        SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE => "com.amazonaws.sqs#ReceiptHandleIsInvalid",
        SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE => "com.amazonaws.sqs#BatchEntryIdsNotDistinct",
        SQS_INVALID_BATCH_ENTRY_ID_ERROR_TYPE => "com.amazonaws.sqs#InvalidBatchEntryId",
        SQS_INVALID_MESSAGE_CONTENTS_ERROR_TYPE => "com.amazonaws.sqs#InvalidMessageContents",
        SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE => {
            "com.amazon.coral.service#InvalidParameterValueException"
        }
        SQS_MISSING_PARAMETER_ERROR_TYPE => {
            "com.amazon.coral.service#MissingRequiredParameterException"
        }
        SQS_INVALID_ACTION_ERROR_TYPE => "com.amazonaws.sqs#InvalidAction",
        SQS_BATCH_REQUEST_TOO_LONG_ERROR_TYPE => "com.amazonaws.sqs#BatchRequestTooLong",
        "AWS.SimpleQueueService.EmptyBatchRequest" => "com.amazonaws.sqs#EmptyBatchRequest",
        "AWS.SimpleQueueService.TooManyEntriesInBatchRequest" => {
            "com.amazonaws.sqs#TooManyEntriesInBatchRequest"
        }
        SQS_SERVICE_UNAVAILABLE_ERROR_TYPE => {
            "com.amazon.coral.service#ServiceUnavailableException"
        }
        _ => "com.amazonaws.sqs#InternalError",
    }
}

/// Wake slots shared by every node of one store. All nodes must use the same
/// value: FoundationDB slot keys and Postgres payloads carry slot numbers.
pub const QUEUE_WAKE_SLOTS: u16 = 1024;
