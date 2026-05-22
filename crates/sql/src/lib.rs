//! Internal SQL backend implementations for aux-storage.
//!
//! This crate is not a supported downstream API. Consumers should use
//! `storage` or `storage-provider` instead.
#![doc(hidden)]

mod backends;
mod billing_metrics;
mod constants;
pub mod dialect;
pub mod driver;
mod parse_conditions;
pub mod provider;
mod provider_core;
pub mod sql_types;
#[cfg(feature = "sqlite-backend")]
mod sqlite_cache_config;
#[cfg(all(test, feature = "sqlite-backend"))]
mod sqlite_cache_config_tests;
mod write_plan;
#[cfg(feature = "postgres-backend")]
pub use backends::postgres::PostgresStorageProvider;
#[cfg(feature = "sqlite-backend")]
pub use backends::sqlite::SQLiteStorageProvider;
#[cfg(all(feature = "sqlite-backend", test, not(feature = "turso-backend")))]
pub(crate) use backends::sqlite::delete_item_impl;
#[cfg(feature = "sqlite-backend")]
pub(crate) use backends::sqlite::sql_statements;
#[cfg(feature = "sqlite-backend")]
pub use backends::sqlite::sync_raft_log_store::SqliteSyncRaftLogStore;
#[cfg(feature = "sqlite-backend")]
pub(crate) use backends::sqlite::{
    batch_write, process_gsi_updates, storage_provider, stream_trim, stream_writer,
    transaction_manager, ttl_sweep,
};
#[cfg(feature = "turso-backend")]
pub use backends::turso::TursoStorageProvider;

mod error_handler;
mod errors;
mod helpers;
mod names;
pub use names::{AttributeName, GsiPhysicalName, PhysicalTableName};
mod gsi_lifecycle;
mod key_attribute_handler;
mod naming;
mod read_path;
mod sql_builder;
mod utils;

#[cfg(test)]
mod gsi_lifecycle_alloc_tests;
#[cfg(test)]
mod gsi_lifecycle_tests;
#[cfg(all(test, any(feature = "postgres-backend", feature = "turso-backend")))]
mod gsi_profile_support_tests;
#[cfg(all(test, feature = "turso-backend"))]
mod put_item_perf_tests;
#[cfg(test)]
mod shared_module_guard_tests;
#[cfg(test)]
mod utils_tests;

#[cfg(test)]
mod billing_metrics_tests;
