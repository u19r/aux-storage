use storage_types::{StorageEnum, StorageError, context::WrappedError as _};

pub(crate) fn normalize_conditional_transaction_error(error: StorageError) -> StorageError {
    if let StorageEnum::TransactionCanceled { reasons } = error.to_enum()
        && is_conditional_only_transaction_cancel(reasons)
    {
        return StorageEnum::ConditionalCheckFailed.into();
    }
    error
}

fn is_conditional_only_transaction_cancel(reasons: &[String]) -> bool {
    let mut saw_conditional_check_failed = false;

    for reason in reasons {
        if reason == "ConditionalCheckFailed" {
            saw_conditional_check_failed = true;
            continue;
        }
        if reason == "None" {
            continue;
        }
        return false;
    }

    saw_conditional_check_failed
}
