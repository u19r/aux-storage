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
fn conditional_transaction_preimage_maps_to_direct_failure_item() {
    let err: storage_types::StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![
            "ConditionalCheckFailed\tThe conditional request failed\t{\"status\":{\"S\":\"open\"}}"
                .to_string(),
        ],
    }
    .into();

    let normalized = normalize_conditional_transaction_error(err);
    let StorageEnum::ConditionalCheckFailedWithItem { item } = normalized.as_ref() else {
        panic!("expected conditional failure item, got {normalized:?}");
    };
    assert_eq!(
        item.get("status"),
        Some(&storage_types::AttributeValue::S("open".to_string()))
    );
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
