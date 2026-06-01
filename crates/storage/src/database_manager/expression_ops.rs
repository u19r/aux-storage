use std::collections::HashMap;

use storage_types::{
    AttributeValue, StorageResult, TransactEncodeItem, TransactWriteItem,
    validate_expression_attribute_usage,
};

pub(crate) fn validate_transact_write_item_expression_usage(
    item: &TransactWriteItem,
) -> StorageResult<()> {
    if let Some(put) = &item.put {
        validate_expression_attribute_usage(
            put.expression_attribute_names.as_ref(),
            put.expression_attribute_values.as_ref(),
            put.condition_expression.as_deref().into_iter(),
        )?;
    }
    if let Some(update) = &item.update {
        validate_update_expression_usage(
            Some(&update.update_expression),
            update.condition_expression.as_deref(),
            update.expression_attribute_names.as_ref(),
            update.expression_attribute_values.as_ref(),
        )?;
    }
    if let Some(delete) = &item.delete {
        validate_expression_attribute_usage(
            delete.expression_attribute_names.as_ref(),
            delete.expression_attribute_values.as_ref(),
            delete.condition_expression.as_deref().into_iter(),
        )?;
    }
    if let Some(check) = &item.condition_check {
        validate_expression_attribute_usage(
            check.expression_attribute_names.as_ref(),
            check.expression_attribute_values.as_ref(),
            std::iter::once(check.condition_expression.as_str()),
        )?;
    }
    Ok(())
}

pub(crate) fn validate_transact_encode_item_expression_usage(
    item: &TransactEncodeItem,
) -> StorageResult<()> {
    if let Some(put) = &item.put {
        validate_expression_attribute_usage(
            put.expression_attribute_names.as_ref(),
            put.expression_attribute_values.as_ref(),
            put.condition_expression.as_deref().into_iter(),
        )?;
    }
    if let Some(update) = &item.update {
        validate_update_expression_usage(
            Some(&update.update_expression),
            update.condition_expression.as_deref(),
            update.expression_attribute_names.as_ref(),
            update.expression_attribute_values.as_ref(),
        )?;
    }
    if let Some(delete) = &item.delete {
        validate_expression_attribute_usage(
            delete.expression_attribute_names.as_ref(),
            delete.expression_attribute_values.as_ref(),
            delete.condition_expression.as_deref().into_iter(),
        )?;
    }
    if let Some(check) = &item.condition_check {
        validate_expression_attribute_usage(
            check.expression_attribute_names.as_ref(),
            check.expression_attribute_values.as_ref(),
            std::iter::once(check.condition_expression.as_str()),
        )?;
    }
    Ok(())
}

pub(crate) fn validate_update_expression_usage(
    update_expression: Option<&str>,
    condition_expression: Option<&str>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<()> {
    validate_expression_attribute_usage(
        expression_attribute_names,
        expression_attribute_values,
        update_expression.into_iter().chain(condition_expression),
    )
}
