//! Exact DynamoDB number parsing and key encoding shared by storage backends.

mod mantissa;
mod number_codec;
mod parse;

pub use number_codec::*;

#[cfg(test)]
mod dynamodb_number_tests;
