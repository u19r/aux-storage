use std::collections::HashMap;

use storage_types::{AttributeValue, StorageError, StorageResult};
use tracing::warn;

use crate::{
    Condition,
    parser_impl::{Lexer, Parser},
};

#[expect(clippy::implicit_hasher)]
pub fn parse_condition_expression_opt(
    condition_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
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
    let empty_names = HashMap::new();
    let empty_values = HashMap::new();
    let attribute_names = expression_attribute_names.unwrap_or(&empty_names);
    let attribute_values = expression_attribute_values.unwrap_or(&empty_values);
    let lexer = Lexer::new(condition_expression);
    let mut parser = Parser::new(lexer)?;

    parser.parse(attribute_names, attribute_values)
}

#[cold]
#[inline(never)]
fn invalid_condition_parse_error(error: &str) -> StorageError {
    warn!(error = %error);
    StorageError::validation("Invalid condition string")
}
