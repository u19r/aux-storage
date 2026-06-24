use std::{collections::HashMap, sync::Arc};

use storage_condition::{Condition, parse_condition_expression};
use storage_provider::{UpdateOperation, parse_update_expression};
use storage_types::{AttributeValue, StorageError, StorageResult};

pub(crate) struct TransactUpdateBindingCacheEntry {
    update_expression: String,
    condition_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    operations: Arc<[UpdateOperation]>,
    condition: Option<Condition>,
}

impl TransactUpdateBindingCacheEntry {
    fn matches(
        &self,
        update_expression: &str,
        condition_expression: Option<&str>,
        expression_attribute_names: Option<&HashMap<String, String>>,
        expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
    ) -> bool {
        self.update_expression == update_expression
            && self.condition_expression.as_deref() == condition_expression
            && self.expression_attribute_names.as_ref() == expression_attribute_names
            && self.expression_attribute_values.as_ref() == expression_attribute_values
    }
}

pub(crate) struct TransactConditionBindingCacheEntry {
    condition_expression: String,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    condition: Condition,
}

impl TransactConditionBindingCacheEntry {
    fn matches(
        &self,
        condition_expression: &str,
        expression_attribute_names: Option<&HashMap<String, String>>,
        expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
    ) -> bool {
        self.condition_expression == condition_expression
            && self.expression_attribute_names.as_ref() == expression_attribute_names
            && self.expression_attribute_values.as_ref() == expression_attribute_values
    }
}

pub(crate) fn cached_transact_condition_binding(
    cache: &mut Vec<TransactConditionBindingCacheEntry>,
    condition_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
) -> StorageResult<Option<Condition>> {
    let Some(condition_expression) = condition_expression else {
        return Ok(None);
    };

    if let Some(entry) = cache.iter().find(|entry| {
        entry.matches(
            condition_expression.as_str(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )
    }) {
        return Ok(Some(entry.condition.clone()));
    }

    let condition = parse_condition_expression(
        condition_expression.as_str(),
        expression_attribute_names.as_ref(),
        expression_attribute_values.as_ref(),
    )
    .map_err(StorageError::validation)?;
    cache.push(TransactConditionBindingCacheEntry {
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
        condition: condition.clone(),
    });
    Ok(Some(condition))
}

pub(crate) fn cached_transact_update_binding(
    cache: &mut Vec<TransactUpdateBindingCacheEntry>,
    update_expression: String,
    condition_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
) -> StorageResult<(Arc<[UpdateOperation]>, Option<Condition>)> {
    if let Some(entry) = cache.iter().find(|entry| {
        entry.matches(
            update_expression.as_str(),
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )
    }) {
        return Ok((Arc::clone(&entry.operations), entry.condition.clone()));
    }

    let operations = parse_update_expression(
        update_expression.as_str(),
        expression_attribute_names.as_ref(),
        expression_attribute_values.as_ref(),
    )?;
    let condition = if let Some(condition_expression) = condition_expression.as_deref() {
        Some(
            parse_condition_expression(
                condition_expression,
                expression_attribute_names.as_ref(),
                expression_attribute_values.as_ref(),
            )
            .map_err(StorageError::validation)?,
        )
    } else {
        None
    };
    let operations = Arc::<[UpdateOperation]>::from(operations);
    cache.push(TransactUpdateBindingCacheEntry {
        update_expression,
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
        operations: Arc::clone(&operations),
        condition: condition.clone(),
    });
    Ok((operations, condition))
}
