use std::{collections::HashMap, sync::LazyLock};

use storage_condition::{Condition, evaluate_condition};
use storage_provider::{BoundUpdateOperation, apply_bound_update_operations};
use storage_types::{AttributeValue, KeyAttributes, StorageEnum, StorageError, StorageResult};

type AttributeMap = HashMap<String, AttributeValue>;
type UpdatePlan = (AttributeMap, AttributeMap);

pub(crate) fn validate_update_key(key: &KeyAttributes) -> StorageResult<()> {
    if key.is_empty() {
        return Err(StorageError::validation(
            "Update request must specify a key",
        ));
    }
    Ok(())
}

pub(crate) fn apply_update_to_existing_or_key(
    existing_item: Option<AttributeMap>,
    key: &KeyAttributes,
    operations: &[BoundUpdateOperation<'_>],
) -> StorageResult<UpdatePlan> {
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
    existing_item: Option<AttributeMap>,
    key: &KeyAttributes,
    operations: &[BoundUpdateOperation<'_>],
    condition: Option<&Condition>,
    return_old_on_condition_failure: bool,
) -> StorageResult<UpdatePlan> {
    if let Some(condition) = condition
        && !evaluate_condition(condition_item_ref(existing_item.as_ref()), condition)
    {
        return Err(conditional_failure(
            existing_item.as_ref(),
            return_old_on_condition_failure,
        ));
    }

    apply_update_to_existing_or_key(existing_item, key, operations)
}

pub(crate) fn conditional_failure(
    old_item: Option<&AttributeMap>,
    return_old: bool,
) -> StorageError {
    if return_old
        && let Some(item) = old_item
    {
        return StorageEnum::ConditionalCheckFailedWithItem {
            item: item.clone().into(),
        }
        .into();
    }
    StorageEnum::ConditionalCheckFailed.into()
}

fn condition_item_ref(
    old_item: Option<&HashMap<String, AttributeValue>>,
) -> &HashMap<String, AttributeValue> {
    static EMPTY_ITEM: LazyLock<HashMap<String, AttributeValue>> = LazyLock::new(HashMap::new);
    old_item.unwrap_or(&EMPTY_ITEM)
}
