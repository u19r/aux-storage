/// Queue message IDs use the first 12 bytes of a UUIDv7-compatible
/// versionstamp so they sort by creation time while staying compact in storage.
pub const MESSAGE_ID_VERSIONSTAMP_LEN: usize = 12;

pub const MAX_DELAY_SECONDS: u32 = 900;
pub const MIN_MAXIMUM_MESSAGE_SIZE_BYTES: u32 = 1_024;
pub const MAX_MAXIMUM_MESSAGE_SIZE_BYTES: u32 = 1_048_576;
pub const MIN_MESSAGE_RETENTION_SECONDS: u32 = 60;
pub const MAX_MESSAGE_RETENTION_SECONDS: u32 = 1_209_600;
pub const MAX_RECEIVE_MESSAGES: u32 = 10;
pub const MAX_MESSAGE_ATTRIBUTES: usize = 10;
pub const MAX_WAIT_TIME_SECONDS: u32 = 20;
pub const MAX_VISIBILITY_TIMEOUT_SECONDS: u32 = 43_200;

pub const SQS_QUEUE_NAME_EXISTS_ERROR_TYPE: &str = "AWS.SimpleQueueService.QueueNameExists";
pub const SQS_NON_EXISTENT_QUEUE_ERROR_TYPE: &str = "AWS.SimpleQueueService.NonExistentQueue";
pub const SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE: &str = "ReceiptHandleIsInvalid";
pub const SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE: &str = "InvalidParameterValue";
pub const SQS_INVALID_ACTION_ERROR_TYPE: &str = "InvalidAction";
pub const SQS_MISSING_PARAMETER_ERROR_TYPE: &str = "MissingParameter";
pub const SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE: &str =
    "AWS.SimpleQueueService.BatchEntryIdsNotDistinct";
pub const SQS_INTERNAL_ERROR_TYPE: &str = "InternalError";
pub const SQS_RECEIPT_HANDLE_INVALID_MESSAGE: &str =
    "The input receipt handle is not a valid receipt handle.";

#[must_use]
pub fn sqs_json_error_type(query_error_type: &str) -> &'static str {
    match query_error_type {
        SQS_NON_EXISTENT_QUEUE_ERROR_TYPE => "com.amazonaws.sqs#QueueDoesNotExist",
        SQS_QUEUE_NAME_EXISTS_ERROR_TYPE | "QueueAlreadyExists" => {
            "com.amazonaws.sqs#QueueNameExists"
        }
        SQS_RECEIPT_HANDLE_INVALID_ERROR_TYPE => "com.amazonaws.sqs#ReceiptHandleIsInvalid",
        SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE => "com.amazonaws.sqs#BatchEntryIdsNotDistinct",
        SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE => {
            "com.amazon.coral.service#InvalidParameterValueException"
        }
        SQS_MISSING_PARAMETER_ERROR_TYPE => {
            "com.amazon.coral.service#MissingRequiredParameterException"
        }
        SQS_INVALID_ACTION_ERROR_TYPE => "com.amazonaws.sqs#InvalidAction",
        _ => "com.amazonaws.sqs#InternalError",
    }
}
