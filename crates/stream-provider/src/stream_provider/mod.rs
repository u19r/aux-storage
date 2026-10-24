//! Stream provider for DynamoDB-compatible change streams.

mod pointer_read;
mod provider_trait;

pub use provider_trait::{StreamProvider, validate_limit};

#[cfg(test)]
mod stream_provider_tests;
