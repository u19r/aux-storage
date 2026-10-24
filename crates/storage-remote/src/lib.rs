//! Internal remote DynamoDB-compatible storage provider implementation.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

mod constants;
mod error;
mod provider;

#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod remote_body_allocation_tests;
#[cfg(test)]
mod remote_conformance_corrections_tests;
#[cfg(test)]
mod remote_provider_tests;
#[cfg(test)]
mod scheduled_job_store_tests;

pub use provider::{
    RemoteStorageProvider, build_scheduled_job_store, connect_scheduled_job_store,
    validate_scheduled_job_http_endpoints,
};
