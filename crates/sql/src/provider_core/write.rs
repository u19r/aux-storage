use std::collections::HashMap;

use storage_condition::{Condition, evaluate_condition};
use storage_provider::{BoundUpdateOperation, apply_bound_update_operations};
use storage_types::{AttributeValue, KeyAttributes, StorageEnum, StorageError, StorageResult};

pub(crate) fn validate_update_key(key: &KeyAttributes) -> StorageResult<()> {
    if key.is_empty() {
        return Err(StorageError::validation(
            "Update request must specify a key",
        ));
    }
    Ok(())
}

pub(crate) fn apply_update_to_existing_or_key(
    existing_item: Option<HashMap<String, AttributeValue>>,
    key: &KeyAttributes,
    operations: &[BoundUpdateOperation<'_>],
) -> StorageResult<(
    HashMap<String, AttributeValue>,
    HashMap<String, AttributeValue>,
)> {
    validate_update_key(key)?;
    let item_to_update = existing_item.unwrap_or_else(|| {
        key.iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect()
    });
    let updated_item = apply_bound_update_operations(item_to_update.clone(), operations)?;
    Ok((item_to_update, updated_item))
}

pub(crate) fn plan_update_from_existing_item(
    existing_item: Option<HashMap<String, AttributeValue>>,
    key: &KeyAttributes,
    operations: &[BoundUpdateOperation<'_>],
    condition: Option<&Condition>,
) -> StorageResult<(
    HashMap<String, AttributeValue>,
    HashMap<String, AttributeValue>,
)> {
    let item_for_condition = existing_item.clone().unwrap_or_default();
    if let Some(condition) = condition
        && !evaluate_condition(&item_for_condition, condition)
    {
        return Err(StorageEnum::ConditionalCheckFailed.into());
    }

    apply_update_to_existing_or_key(existing_item, key, operations)
}
