use std::collections::HashMap;

use storage_condition::{evaluate_condition, parse_condition_expression};
use storage_types::{AttributeValue, StorageEnum, StorageResult};

pub(super) fn evaluate_optional_condition(
    old_item: Option<&HashMap<String, AttributeValue>>,
    condition_expression: Option<&str>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<()> {
    let Some(condition_expression) = condition_expression else {
        return Ok(());
    };
    let condition = parse_condition_expression(
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
    )
    .map_err(|_| StorageEnum::ConditionalCheckFailed)?;
    if evaluate_condition(&old_item.cloned().unwrap_or_default(), &condition) {
        Ok(())
    } else {
        Err(StorageEnum::ConditionalCheckFailed.into())
    }
}
