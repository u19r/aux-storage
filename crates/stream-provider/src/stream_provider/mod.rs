//! Stream provider for DynamoDB-compatible change streams.

mod provider_trait;

pub use provider_trait::{StreamProvider, validate_limit};

#[cfg(test)]
mod stream_provider_tests;
