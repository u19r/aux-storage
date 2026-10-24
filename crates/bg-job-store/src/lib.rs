//! Internal storage-backed job store implementations.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

#[doc(hidden)]
pub mod backend;
mod config;
mod constants;
mod error;
mod provider_job_store;

pub use crate::{
    backend::ScheduledJobStoreSource, config::ScheduledJobStoreConfig,
    error::ScheduledJobStoreError, provider_job_store::ScheduledJobStore,
};

#[cfg(test)]
mod provider_job_store_tests;
