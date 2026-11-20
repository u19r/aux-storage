use super::{
    Condition, evaluate_condition, evaluate_wire_condition, parse_condition_expression_opt,
    parse_condition_expression_typed,
};
use crate::{AttributeMap, AttributeValueLookup, StorageError, StorageResult, VecMap, WireItem};

/// A condition expression parsed once by the request owner. Every attempt and
/// every item of the request evaluates this value; nothing parses it again.
#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct CompiledCondition {
    condition: Condition,
}

impl CompiledCondition {
    /// `ConditionExpression` of PutItem, UpdateItem, DeleteItem and a
    /// transaction `ConditionCheck`. A parse error is the validation error
    /// "Invalid condition string" (`parse_condition_expression_opt`).
    pub fn compile_write(
        expression: Option<String>,
        names: Option<VecMap<String, String>>,
        values: Option<AttributeMap>,
    ) -> StorageResult<Option<Self>> {
        parse_condition_expression_opt(expression, names, values)
            .map(|condition| condition.map(|condition| Self { condition }))
    }

    /// `FilterExpression` of Query and Scan.
    pub fn compile_filter(
        expression: &str,
        names: Option<&VecMap<String, String>>,
        values: Option<&AttributeMap>,
    ) -> StorageResult<Self> {
        parse_condition_expression_typed(expression, names, values)
            .map(|condition| Self { condition })
            .map_err(|message| {
                StorageError::validation(message.replace("ConditionExpression", "FilterExpression"))
            })
    }

    /// `KeyConditionExpression` of Query. The key-schema rules
    /// (`validate_key_condition`) need the bound table and stay with it.
    pub fn compile_key_condition(
        expression: &str,
        names: Option<&VecMap<String, String>>,
        values: Option<&AttributeMap>,
    ) -> StorageResult<Self> {
        parse_condition_expression_typed(expression, names, values)
            .map(|condition| Self { condition })
            .map_err(|error| {
                StorageError::validation(format!("Invalid key condition expression: {error}"))
            })
    }

    #[must_use]
    pub fn condition(&self) -> &Condition {
        &self.condition
    }

    /// Evaluate against the protected old image of a write (`None`: no item).
    /// Borrowing keeps the successful path from cloning the image; callers
    /// clone only after a failed condition when the public error needs the old
    /// item.
    pub fn check_borrowed(&self, old: Option<&WireItem>) -> StorageResult<bool> {
        evaluate_wire_condition(old, &self.condition)
    }

    /// Evaluate a filter against one decoded item.
    #[must_use]
    pub fn matches(&self, item: &impl AttributeValueLookup) -> bool {
        evaluate_condition(item, &self.condition)
    }
}
