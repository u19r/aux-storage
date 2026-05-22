//! Internal remote DynamoDB-compatible storage provider implementation.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

mod constants;
mod error;
mod provider;

#[cfg(test)]
mod error_tests;

pub use constants::{MAX_ENDPOINT_RETRIES, MAX_REMOTE_RETRIES};
pub use provider::RemoteStorageProvider;
