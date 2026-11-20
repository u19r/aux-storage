//! Public storage facade and embedded runtime entry point.
//!
//! Consumers should depend on this crate for storage manager APIs, request and
//! entity types, derive macros, provider contracts, and selected
//! backend-neutral helpers. Implementation crates remain available inside the
//! workspace, but this crate is the supported downstream surface.

extern crate self as storage;

/// The supported facade for the exact provider-backed aux-common scheduled-job
/// runtime store contract. Consumers should use this name rather than
/// depending on the implementation crate.
///
/// Store commits are atomic: a failed check applies no mutation and returns
/// `Conflict`; `Unknown` is reserved for an ambiguous post-dispatch outcome
/// that must be reconciled by the caller. The supported backend levels are
/// SQLite `Local`, Turso `Local`, RocksDB `Local`, remote DynamoDB-compatible
/// storage `Distributed`, Postgres `Distributed`, and FoundationDB
/// `Distributed`. Remote scheduled-job access is library-only over a
/// private-network HTTP endpoint using the existing DynamoDB-compatible
/// actions. Its service contract requires strong reads, atomic conditional
/// transactions, and durable operation receipts, so independent clients share
/// one distributed scheduled-job namespace without exposing backend technology
/// in this facade.
pub use bg_job_store::{ScheduledJobStore, ScheduledJobStoreError};
#[cfg(feature = "foundationdb")]
pub use kv_backend::foundationdb::shutdown_foundationdb_network;

pub mod common;

pub mod derive;

/// Supported provider configuration and stable provider contracts for embedded
/// consumers.
///
/// This is an explicit facade rather than a wildcard export: each name is
/// either part of [`DatabaseManager`]'s public signatures or is used by an
/// existing storage consumer. The re-exports retain the provider crate's type
/// identity while keeping backend implementations behind this facade.
pub mod provider;

pub mod types;

pub mod admission;
mod maintenance_runtime;
pub mod point_read;
mod provider_bundle;
mod regional;
#[cfg(feature = "remote")]
pub use provider_bundle::{RemoteScheduledJobStoreConfig, connect_remote_scheduled_job_store};
#[cfg(feature = "remote")]
pub use storage_remote::RemoteStorageProvider;
mod constants;
#[cfg(all(feature = "test-support", feature = "foundationdb"))]
mod foundationdb_test_scope;
#[cfg(all(feature = "test-support", feature = "foundationdb"))]
pub use foundationdb_test_scope::FoundationDbTestScope;
mod connection;
mod database_manager;
#[cfg(all(test, feature = "test-support", feature = "foundationdb"))]
mod foundationdb_test_scope_tests;
pub mod messaging;
pub mod startup;
mod updated_at_apply;
#[cfg(test)]
mod updated_at_apply_tests;
pub use admission::{
    AdmissionClass, AdmissionConfig, AdmissionOutcome, AdmissionPermit, ControlPermit,
};
pub use database_manager::{
    CappedEntityCreateOutcome, CappedEntityCreateRejection, CappedStorageError,
    CreateCappedEntityInput, DatabaseManager, DatabaseManagerRuntimeOptions, DeleteItemInput,
    DomainEffectAuthorization, PreparedCappedEntityCreate, PutItemEntityEncodeInput, PutItemInput,
    QueryIndexInput, QueryTableInput, UpdateItemInput,
};
pub use storage_types::*;

mod tables;
pub use tables::Tables;

#[cfg(all(test, feature = "sqlite"))]
mod contract_tests;
#[cfg(test)]
mod database_manager_tests;
#[cfg(test)]
mod storage_tests;

#[cfg(test)]
mod manager_cutover_tests;
#[cfg(test)]
mod startup_tests;
#[cfg(test)]
mod storage_test_support_tests;

pub use provider_bundle::InitializedStore;
