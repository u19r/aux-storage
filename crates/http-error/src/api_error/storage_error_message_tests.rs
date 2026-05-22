//! Tests ensuring StorageError -> HttpApiError mapping keeps canonical DynamoDB
//! messages.
use storage_types::{DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, StorageEnum, StorageError};

use super::HttpApiError;

#[test]
fn conditional_check_failed_http_message() {
    let storage_err: StorageError = StorageEnum::ConditionalCheckFailed.into();
    let handler: HttpApiError = storage_err.into();
    assert_eq!(
        handler.error_type,
        "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
    );
    assert_eq!(handler.message, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE);
}
