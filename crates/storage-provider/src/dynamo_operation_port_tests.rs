use storage_types::{StorageEnum, StorageError, context::WrappedError as _};

use super::dynamo_operation_port::split_byte_rejected_group;

fn bytes_rejected() -> StorageError {
    storage_types::AttemptFailure::TRANSACTION_BYTES.into()
}

#[test]
fn given_byte_rejected_batch_group_when_split_then_it_halves_until_one_member_returns_validation() {
    assert_eq!(split_byte_rejected_group(bytes_rejected(), 25).unwrap(), 12);
    assert_eq!(split_byte_rejected_group(bytes_rejected(), 2).unwrap(), 1);
    let error = split_byte_rejected_group(bytes_rejected(), 1).unwrap_err();
    assert!(
        matches!(error.to_enum(), StorageEnum::Validation { message, .. } if message == "Transaction size exceeds the provider limit"),
        "a one-member group answers as TransactWriteItems does"
    );
}

#[test]
fn given_other_batch_group_failure_when_split_then_it_is_returned_unchanged() {
    let conflict = split_byte_rejected_group(StorageError::internal("other"), 4).unwrap_err();
    assert!(matches!(
        conflict.to_enum(),
        StorageEnum::InternalServerError { .. }
    ));
}
