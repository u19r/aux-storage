use std::collections::HashMap;

use crate::{
    AttributeMap, AttributeValue, AttributeValueLookup, KeyAttributeType, KeySchemaElement,
    KeyType, StorageError, StorageResult, VecMap,
    condition::{
        AttributePath, Condition,
        parser_impl::{ExpressionNameLookup, Lexer, Parser, visit_key_condition_operands},
    },
};

pub fn parse_condition_expression_opt(
    condition_expression: Option<String>,
    expression_attribute_names: Option<VecMap<String, String>>,
    expression_attribute_values: Option<AttributeMap>,
) -> StorageResult<Option<Condition>> {
    let Some(condition_expression) = condition_expression else {
        return Ok(None);
    };
    let attribute_names = expression_attribute_names.unwrap_or_default();
    let attribute_values = expression_attribute_values.unwrap_or_default();

    let lexer = Lexer::new(&condition_expression);
    let mut parser = Parser::new(lexer).map_err(|error| invalid_condition_parse_error(&error))?;

    parser
        .parse(&attribute_names, &attribute_values)
        .map(Some)
        .map_err(|error| invalid_condition_parse_error(&error))
}

/// Returns an error if the condition expression cannot be parsed.
pub fn parse_condition_expression(
    condition_expression: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> Result<Condition, String> {
    parse_condition_expression_with_lookups(
        condition_expression,
        expression_attribute_names.map(|names| names as &dyn ExpressionNameLookup),
        expression_attribute_values.map(|values| values as &dyn AttributeValueLookup),
    )
}

pub fn parse_condition_expression_typed(
    condition_expression: &str,
    expression_attribute_names: Option<&VecMap<String, String>>,
    expression_attribute_values: Option<&AttributeMap>,
) -> Result<Condition, String> {
    parse_condition_expression_with_lookups(
        condition_expression,
        expression_attribute_names.map(|names| names as &dyn ExpressionNameLookup),
        expression_attribute_values.map(|values| values as &dyn AttributeValueLookup),
    )
}

fn parse_condition_expression_with_lookups(
    condition_expression: &str,
    expression_attribute_names: Option<&dyn ExpressionNameLookup>,
    expression_attribute_values: Option<&dyn AttributeValueLookup>,
) -> Result<Condition, String> {
    let empty_names = VecMap::new();
    let empty_values = AttributeMap::new();
    let attribute_names = expression_attribute_names.unwrap_or(&empty_names);
    let attribute_values = expression_attribute_values.unwrap_or(&empty_values);
    let lexer = Lexer::new(condition_expression);
    let mut parser = Parser::new(lexer)?;
    parser.parse(attribute_names, attribute_values)
}

/// Rejects a key-condition operand whose type differs from the type of the
/// key attribute it is compared to. `key_type` returns the declared type of a
/// key attribute and `None` for other attributes.
pub fn validate_key_condition_operand_types(
    key_condition_expression: &str,
    expression_attribute_names: Option<&VecMap<String, String>>,
    expression_attribute_values: Option<&AttributeMap>,
    key_type: impl Fn(&str) -> Option<KeyAttributeType>,
) -> StorageResult<()> {
    let empty_names = VecMap::new();
    let empty_values = AttributeMap::new();
    visit_key_condition_operands(
        key_condition_expression,
        expression_attribute_names.unwrap_or(&empty_names),
        expression_attribute_values.unwrap_or(&empty_values),
        |attribute, value| {
            let matches = match key_type(attribute) {
                None => true,
                Some(KeyAttributeType::S) => matches!(value, AttributeValue::S(_)),
                Some(KeyAttributeType::N) => matches!(value, AttributeValue::N(_)),
                Some(KeyAttributeType::B) => matches!(value, AttributeValue::B(_)),
            };
            if matches {
                Ok(())
            } else {
                Err(KEY_CONDITION_TYPE_MISMATCH.to_owned())
            }
        },
    )
    .map_err(StorageError::validation)
}

/// Shared by every check of a key-condition operand against the key schema.
pub const KEY_CONDITION_TYPE_MISMATCH: &str = "One or more parameter values were invalid: \
                                               Condition parameter type does not match schema type";

/// Validates the subset of condition expressions accepted as a DynamoDB
/// `KeyConditionExpression`.
///
/// The parser intentionally accepts the broader condition grammar because the
/// same AST is also used for filters and conditional writes.  Key conditions
/// add the partition-key equality and sort-key operator rules at this boundary.
pub fn validate_key_condition(
    condition: &Condition,
    key_schema: &[KeySchemaElement],
) -> Result<(), String> {
    reject_nested_key_paths(condition)?;
    let mut validator = KeyConditionValidator::new(key_schema, leaf_count(condition))?;
    validator.visit(condition)?;
    validator.finish()
}

fn reject_nested_key_paths(condition: &Condition) -> Result<(), String> {
    match condition {
        Condition::And { conditions } => conditions.iter().try_for_each(reject_nested_key_paths),
        Condition::Equal { field, .. }
        | Condition::LessThan { field, .. }
        | Condition::LessThanEqual { field, .. }
        | Condition::GreaterThan { field, .. }
        | Condition::GreaterThanEqual { field, .. }
        | Condition::Between { field, .. }
        | Condition::BeginsWith { field, .. }
            if field.top_level().is_none() =>
        {
            Err("KeyConditionExpressions cannot have conditions on nested attributes".to_string())
        }
        _ => Ok(()),
    }
}

fn leaf_count(condition: &Condition) -> usize {
    match condition {
        Condition::And { conditions } => conditions.iter().map(leaf_count).sum(),
        _ => 1,
    }
}

struct KeyConditionValidator<'a> {
    condition_count: usize,
    hash_key: &'a str,
    range_key: Option<&'a str>,
    hash_equality_seen: bool,
    range_condition_seen: bool,
    non_key_condition_seen: bool,
}

const INVALID_OPERATOR: &str =
    "Invalid KeyConditionExpression: Invalid operator used in KeyConditionExpression";
const NESTED_OPERATIONS: &str =
    "Invalid KeyConditionExpression: KeyConditionExpressions cannot contain nested operations";
const ONE_CONDITION_PER_KEY: &str = "Invalid KeyConditionExpression: KeyConditionExpressions must \
                                     only contain one condition per key";

impl<'a> KeyConditionValidator<'a> {
    fn new(key_schema: &'a [KeySchemaElement], condition_count: usize) -> Result<Self, String> {
        let hash_key = key_schema
            .iter()
            .find(|key| key.key_type == KeyType::Hash)
            .map(|key| key.attribute_name.as_str())
            .ok_or_else(|| "table hash key schema missing".to_string())?;
        let range_key = key_schema
            .iter()
            .find(|key| key.key_type == KeyType::Range)
            .map(|key| key.attribute_name.as_str());
        Ok(Self {
            condition_count,
            hash_key,
            range_key,
            hash_equality_seen: false,
            range_condition_seen: false,
            non_key_condition_seen: false,
        })
    }

    fn visit(&mut self, condition: &Condition) -> Result<(), String> {
        match condition {
            Condition::And { conditions } => self.visit_and(conditions),
            Condition::Equal { field, .. } if field == self.hash_key => {
                if self.hash_equality_seen {
                    return Err(ONE_CONDITION_PER_KEY.to_string());
                }
                self.hash_equality_seen = true;
                Ok(())
            }
            Condition::Equal { field, .. }
            | Condition::LessThan { field, .. }
            | Condition::LessThanEqual { field, .. }
            | Condition::GreaterThan { field, .. }
            | Condition::GreaterThanEqual { field, .. }
            | Condition::Between { field, .. }
            | Condition::BeginsWith { field, .. } => self.visit_range(field),
            Condition::Size { .. } | Condition::SizeCompare { .. } => {
                Err(NESTED_OPERATIONS.to_string())
            }
            _ => Err(INVALID_OPERATOR.to_string()),
        }
    }

    fn visit_and(&mut self, conditions: &[Condition]) -> Result<(), String> {
        if conditions.is_empty() {
            return Err("Invalid key condition expression: empty AND condition".to_string());
        }
        for condition in conditions {
            self.visit(condition)?;
        }
        Ok(())
    }

    fn visit_range(&mut self, field: &AttributePath) -> Result<(), String> {
        if self.range_key.is_none() && field.top_level() != Some(self.hash_key) {
            return Err(format!(
                "The number of query conditions ({}) exceeds the number of key attributes defined \
                 in the schema (1)",
                self.condition_count
            ));
        }
        match (field.top_level(), self.range_key) {
            (Some(field), Some(range)) if field == range => {
                if self.range_condition_seen {
                    return Err(ONE_CONDITION_PER_KEY.to_string());
                }
                self.range_condition_seen = true;
            }
            (Some(field), Some(_)) if field != self.hash_key => self.non_key_condition_seen = true,
            _ => return Err("Query key condition not supported".to_string()),
        }
        Ok(())
    }

    fn finish(self) -> Result<(), String> {
        if !self.hash_equality_seen {
            return Err(format!(
                "Query condition missed key schema element: {}",
                self.hash_key
            ));
        }
        match self.range_key {
            Some(range) if self.non_key_condition_seen => {
                if self.range_condition_seen && self.condition_count > 2 {
                    Err(format!(
                        "The number of query conditions ({}) exceeds the number of key attributes \
                         defined in the schema (2)",
                        self.condition_count
                    ))
                } else if self.range_condition_seen {
                    Err("Query key condition not supported".to_string())
                } else {
                    Err(format!(
                        "Query condition missed key schema element: {range}"
                    ))
                }
            }
            _ => Ok(()),
        }
    }
}

#[cold]
#[inline(never)]
fn invalid_condition_parse_error(_: &str) -> StorageError {
    StorageError::validation("Invalid condition string")
}
