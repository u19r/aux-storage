pub const DYNAMODB_TABLE_NOT_FOUND_PREFIX: &str = "Requested resource not found: Table: ";
pub const DYNAMODB_TABLE_NOT_FOUND_SUFFIX: &str = " not found";
pub const DYNAMODB_ACCESS_DENIED_MESSAGE: &str = "Access denied.";
pub const DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE: &str = "The conditional request failed";
pub const DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE: &str = "DynamoDB could not process your request.";
pub const DYNAMODB_LIMIT_EXCEEDED_MESSAGE: &str = "Too many operations for a given subscriber.";
pub const DYNAMODB_MISSING_AUTH_TOKEN_MESSAGE: &str =
    "Request must contain a valid (registered) AWS Access Key ID.";
pub const DYNAMODB_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE: &str =
    "You exceeded your maximum allowed provisioned throughput for a table or for one or more \
     global secondary indexes. To view performance metrics for provisioned throughput vs. \
     consumed throughput, open the Amazon CloudWatch console.";
pub const DYNAMODB_REQUEST_LIMIT_EXCEEDED_MESSAGE: &str = "Throughput exceeds the current throughput limit for your account. To request a limit increase, contact AWS Support at https://aws.amazon.com/support.";
pub const DYNAMODB_RESOURCE_IN_USE_MESSAGE: &str =
    "The resource which you are attempting to change is in use.";
pub const DYNAMODB_RESOURCE_NOT_FOUND_MESSAGE: &str = "Requested resource not found";
pub const DYNAMODB_THROTTLING_MESSAGE: &str = "Rate of requests exceeds the allowed throughput.";
pub const DYNAMODB_TRANSACTION_CANCELED_MESSAGE: &str =
    "Transaction cancelled, please refer cancellation reasons for specific reasons.";
pub const DYNAMODB_TRANSACTION_CONFLICT_MESSAGE: &str =
    "Operation was rejected because there is an ongoing transaction for the item.";
pub const DYNAMODB_TRANSACTION_IN_PROGRESS_MESSAGE: &str =
    "The transaction with the given request token is already in progress.";
pub const DYNAMODB_UNRECOGNIZED_CLIENT_MESSAGE: &str =
    "The Access Key ID or security token is invalid.";
pub const DYNAMODB_STREAM_RECORDS_LIMIT_MIN: u32 = 1;
pub const DYNAMODB_STREAM_RECORDS_LIMIT_MAX: u32 = 1000;
pub const DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE: &str = "Limit must be between 1 and 1000";
pub const SYSTEM_STREAM_RECORDS_LIMIT_MAX: u32 = 8_192;
pub const STREAM_RECORDS_MAX_ENCODED_BYTES: u32 = 4 * 1024 * 1024;
pub const STORAGE_SERDE_RAW_JSON_LIMIT_BYTES: usize = 4 * 1024;
pub const STORAGE_SERDE_MIN_COMPRESSION_SAVINGS_BYTES: usize = 64;
pub const STORAGE_SERDE_MIN_COMPRESSION_SAVINGS_DIVISOR: usize = 8;

#[must_use]
pub fn dynamodb_table_not_found_message(table_name: &str) -> String {
    format!("{DYNAMODB_TABLE_NOT_FOUND_PREFIX}{table_name}{DYNAMODB_TABLE_NOT_FOUND_SUFFIX}")
}

#[must_use]
pub fn parse_dynamodb_table_not_found_message(message: &str) -> Option<String> {
    let trimmed = message.trim();
    let remainder = trimmed.strip_prefix(DYNAMODB_TABLE_NOT_FOUND_PREFIX)?;
    let name = remainder.strip_suffix(DYNAMODB_TABLE_NOT_FOUND_SUFFIX)?;
    let name = name.trim();
    let name = name
        .strip_prefix('`')
        .and_then(|value| value.strip_suffix('`'))
        .unwrap_or(name);
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}
