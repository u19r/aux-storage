use http_error::HttpApiError;
use storage_types::{
    AttributeMap, AttributeValue, KeyAttributes, KeySchemaElement, StorageEnum, StorageError,
    context::WrappedError as _, return_values_on_condition_check_failure_all_old,
};

pub(super) fn should_return_old_item_on_condition_failure(
    condition_expression: Option<&str>,
    return_values_on_condition_check_failure: Option<&String>,
) -> bool {
    condition_expression.is_some()
        && return_values_on_condition_check_failure_all_old(
            return_values_on_condition_check_failure,
        )
}

pub(super) fn conditional_failure_with_old_item(
    error: StorageError,
    old_item: Option<AttributeMap>,
) -> HttpApiError {
    if matches!(error.to_enum(), StorageEnum::ConditionalCheckFailed)
        && let Some(item) = old_item
    {
        return HttpApiError::from(StorageError::from(
            StorageEnum::ConditionalCheckFailedWithItem { item },
        ));
    }
    error.into()
}

pub(super) fn key_from_item(
    key_schema: &[KeySchemaElement],
    item: &std::collections::HashMap<String, AttributeValue>,
) -> Option<KeyAttributes> {
    let mut key = KeyAttributes::with_capacity(key_schema.len());
    for key_element in key_schema {
        let value = item.get(&key_element.attribute_name)?;
        key.insert(key_element.attribute_name.clone(), value.clone());
    }
    Some(key)
}
