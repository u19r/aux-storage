use std::collections::HashMap;

use storage_types::{
    AttributeMap, AttributeValue, AttributeValueLookup, KeySchemaElement, KeyType, StorageError,
    StorageResult, VecMap,
};

use crate::{
    Condition,
    parser_impl::{ExpressionNameLookup, Lexer, Parser},
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
    let mut validator = KeyConditionValidator::new(key_schema)?;
    validator.visit(condition)?;
    validator.finish()
}

struct KeyConditionValidator<'a> {
    hash_key: &'a str,
    range_key: Option<&'a str>,
    hash_equality_seen: bool,
    range_condition_seen: bool,
}

impl<'a> KeyConditionValidator<'a> {
    fn new(key_schema: &'a [KeySchemaElement]) -> Result<Self, String> {
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
            hash_key,
            range_key,
            hash_equality_seen: false,
            range_condition_seen: false,
        })
    }

    fn visit(&mut self, condition: &Condition) -> Result<(), String> {
        match condition {
            Condition::And { conditions } => self.visit_and(conditions),
            Condition::Equal { field, .. } if field == self.hash_key => {
                if self.hash_equality_seen {
                    return Err("Query key condition not supported".to_string());
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
            _ => Err("Invalid operator used in KeyConditionExpression".to_string()),
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

    fn visit_range(&mut self, field: &str) -> Result<(), String> {
        if Some(field) != self.range_key || self.range_condition_seen {
            return Err("Query key condition not supported".to_string());
        }
        self.range_condition_seen = true;
        Ok(())
    }

    fn finish(self) -> Result<(), String> {
        if self.hash_equality_seen {
            Ok(())
        } else {
            Err(format!(
                "Query condition missed key schema element: {}",
                self.hash_key
            ))
        }
    }
}

#[cold]
#[inline(never)]
fn invalid_condition_parse_error(_: &str) -> StorageError {
    StorageError::validation("Invalid condition string")
}
