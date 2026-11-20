//! DynamoDB condition-expression parser and evaluator.

mod compiled;
mod evaluate_condition;
mod helpers;
mod parser;
mod parser_impl;
mod types;

pub use compiled::CompiledCondition;
pub use evaluate_condition::*;
pub(crate) use helpers::{between_upper_is_less_than_lower, dynamodb_attribute_value_display};
pub use parser::*;
pub use types::*;

#[cfg(test)]
mod compiled_tests;
#[cfg(test)]
mod evaluate_condition_tests;
#[cfg(test)]
mod parser_perf_tests;
#[cfg(test)]
mod parser_tests;
