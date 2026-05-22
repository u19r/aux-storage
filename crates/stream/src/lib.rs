//! Internal stream service implementation and provider factory.
//!
//! This crate is not a supported downstream API. Consumers should prefer
//! `stream-provider` for stable traits and types.
#![doc(hidden)]

mod manager;
pub use manager::*;
pub use stream_provider::*;

#[cfg(test)]
mod stream_tests;

mod constants;
mod factory;
pub use factory::create_stream_provider;

mod validation;
