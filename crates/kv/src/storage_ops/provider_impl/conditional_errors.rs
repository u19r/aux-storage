use storage_types::{AttributeMap, StorageEnum, StorageError, context::WrappedError as _};

pub(crate) fn normalize_conditional_transaction_error(error: StorageError) -> StorageError {
    if let StorageEnum::TransactionCanceled { reasons } = error.to_enum()
        && is_conditional_only_transaction_cancel(reasons)
    {
        if let Some(item) = reasons
            .iter()
            .find_map(|reason| conditional_failure_item(reason))
        {
            return StorageEnum::ConditionalCheckFailedWithItem { item }.into();
        }
        return StorageEnum::ConditionalCheckFailed.into();
    }
    error
}

fn conditional_failure_item(reason: &str) -> Option<AttributeMap> {
    let mut parts = reason.splitn(3, '\t');
    if parts.next()? != "ConditionalCheckFailed" {
        return None;
    }
    let _message = parts.next()?;
    serde_json::from_str(parts.next()?).ok()
}

fn is_conditional_only_transaction_cancel(reasons: &[String]) -> bool {
    let mut saw_conditional_check_failed = false;

    for reason in reasons {
        if reason == "ConditionalCheckFailed" || reason.starts_with("ConditionalCheckFailed\t") {
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
