//! Internal DynamoDB condition-expression parser and evaluator.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

mod helpers;
mod parser_impl;

mod evaluate_condition;
mod parser;
mod types;
pub use evaluate_condition::*;
pub use parser::*;
pub use types::*;

#[cfg(test)]
mod evaluate_condition_tests;
#[cfg(test)]
mod parser_perf_tests;
#[cfg(test)]
mod parser_tests;
