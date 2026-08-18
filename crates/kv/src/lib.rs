//! Internal key-value backend implementations for aux-storage.
//!
//! This crate is not a supported downstream API. Consumers should use
//! `storage`, `storage-provider`, `queue-provider`, or `stream-provider`
//! instead.
#![doc(hidden)]

pub mod backends;
pub mod backfill;
pub mod batch_operation_helpers;
mod billing_metrics;
pub mod constants;
pub mod helpers;
pub mod key_template;
pub mod keyspace;
pub mod newtypes;
pub mod partition_family;
pub mod partition_reconcile;
pub mod partition_runtime_load;
pub mod pubsub;
pub mod query_helpers;
pub mod queue;
pub mod queue_provider;
pub mod sorted_kv;
pub mod sorted_kv_store;
mod storage_ops;
pub mod stream;
pub mod ttl;

pub mod storage_provider {
    pub(crate) use crate::storage_ops::*;
}

#[cfg(feature = "foundationdb-backend")]
pub use backends::fdb::{
    FoundationDbConfig, FoundationDbKvStore, foundationdb_operation_metrics_reset,
    foundationdb_operation_metrics_snapshot,
};
#[cfg(feature = "rocksdb-backend")]
pub use backends::rocksdb::RocksDbKvStore;
pub use sorted_kv::SortedKvDbStorageProvider;

#[cfg(test)]
mod helpers_tests;
#[cfg(test)]
mod key_template_tests;
#[cfg(all(
    test,
    feature = "rocksdb-backend",
    not(feature = "foundationdb-backend")
))]
mod kv_key_shape_tests;
#[cfg(test)]
mod kv_perf_tests;
#[cfg(test)]
mod newtypes_tests;
#[cfg(test)]
mod partition_reconcile_tests;
#[cfg(test)]
mod queue_provider_visibility_tests;
#[cfg(test)]
mod sorted_kv_store_tests;
#[cfg(test)]
mod sorted_kv_tests;
#[cfg(all(test, feature = "foundationdb-backend"))]
mod storage_api_perf_tests;

#[cfg(test)]
mod read_path_alloc_tests;
#[cfg(test)]
mod write_path_alloc_tests;

#[cfg(test)]
pub mod kv_support_tests;
