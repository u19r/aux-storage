mod provider;

pub use provider::SQLiteStorageProvider;

#[cfg(test)]
mod batch_get_plan_perf_tests;
pub(crate) mod batch_write;
pub(crate) mod delete_item_impl;
pub(crate) mod durable_revision;
pub(crate) mod get_item_impl;
pub(crate) mod get_table_info_impl;
pub(crate) mod logical_backfill;
pub(crate) mod logical_backfill_gsi;
pub(crate) mod logical_backfill_import;
pub(crate) mod logical_backfill_stream;
pub(crate) mod logical_backfill_sync_store;
#[cfg(test)]
mod logical_backfill_tests;
pub(crate) mod ops_item;
pub(crate) mod ops_table;
pub(crate) mod process_gsi_updates;
#[cfg(test)]
mod process_gsi_updates_alloc_tests;
pub(crate) mod provider_read;
pub(crate) mod provider_table_lifecycle;
pub(crate) mod pubsub_provider;
pub(crate) mod put_item_impl;
pub(crate) mod queue_provider;
pub(crate) mod sql_statements;
pub(crate) mod storage_provider;
pub(crate) mod stream_duration;
#[cfg(test)]
mod stream_duration_tests;
pub(crate) mod stream_provider;
pub(crate) mod stream_trim;
pub(crate) mod stream_writer;
pub mod sync_raft_log_store;
pub(crate) mod transact_write_impl;
pub(crate) mod transaction_manager;
#[cfg(test)]
mod transaction_manager_tests;
pub(crate) mod ttl;
pub(crate) mod ttl_sweep;
pub(crate) mod update_item_impl;

#[cfg(test)]
mod defensive_checks_tests;
#[cfg(all(test, not(feature = "turso-backend")))]
mod delete_item_alloc_tests;
#[cfg(test)]
mod delete_item_tests;
#[cfg(test)]
mod error_tests;
#[cfg(all(test, not(feature = "turso-backend")))]
mod get_item_alloc_tests;
#[cfg(test)]
mod mod_tests;
#[cfg(test)]
mod pubsub_provider_tests;
#[cfg(all(test, not(feature = "turso-backend")))]
mod read_path_alloc_tests;
#[cfg(test)]
mod read_path_projection_tests;
#[cfg(test)]
mod sql_statements_tests;
#[cfg(test)]
mod sqlite_tests;
#[cfg(test)]
mod storage_provider_tests;
#[cfg(test)]
mod stream_provider_tests;
#[cfg(test)]
mod sync_raft_log_store_tests;
#[cfg(test)]
mod transact_write_impl_tests;
#[cfg(test)]
mod update_item_tests;
#[cfg(all(test, not(feature = "turso-backend")))]
mod write_path_alloc_tests;

#[cfg(test)]
mod ttl_sweep_tests;
