use storage_types::StorageEnum;

use crate::storage_provider::normalize_conditional_transaction_error;

#[test]
fn conditional_only_transaction_cancel_maps_to_conditional_failed() {
    let err: storage_types::StorageError = StorageEnum::TransactionCanceled {
        reasons: vec!["ConditionalCheckFailed".to_string()],
    }
    .into();

    let normalized = normalize_conditional_transaction_error(err);
    assert!(matches!(
        normalized.as_ref(),
        StorageEnum::ConditionalCheckFailed
    ));
}

#[test]
fn mixed_transaction_cancel_reasons_stay_transaction_canceled() {
    let err: storage_types::StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![
            "ConditionalCheckFailed".to_string(),
            "TransactionConflict".to_string(),
        ],
    }
    .into();

    let normalized = normalize_conditional_transaction_error(err);
    assert!(matches!(
        normalized.as_ref(),
        StorageEnum::TransactionCanceled { .. }
    ));
}
