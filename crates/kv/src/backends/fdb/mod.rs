#[cfg(feature = "foundationdb-backend")]
mod conflicts;
#[cfg(feature = "foundationdb-backend")]
mod constants;
#[cfg(feature = "foundationdb-backend")]
mod error;
#[cfg(feature = "foundationdb-backend")]
mod keyspace;
#[cfg(feature = "foundationdb-backend")]
mod mapped_range;
#[cfg(feature = "foundationdb-backend")]
mod metrics;
#[cfg(feature = "foundationdb-backend")]
mod network;
#[cfg(feature = "foundationdb-backend")]
mod range_read;
#[cfg(feature = "foundationdb-backend")]
mod read_context;
#[cfg(feature = "foundationdb-backend")]
mod store;

#[cfg(all(test, feature = "foundationdb-backend"))]
pub(crate) use metrics::foundationdb_operation_metrics_test_guard;
#[cfg(feature = "foundationdb-backend")]
pub use metrics::{foundationdb_operation_metrics_reset, foundationdb_operation_metrics_snapshot};
#[cfg(feature = "foundationdb-backend")]
pub use store::{FoundationDbConfig, FoundationDbKvStore};

#[cfg(all(test, feature = "foundationdb-backend"))]
pub(crate) mod fdb_support_tests;

#[cfg(all(test, feature = "foundationdb-backend"))]
mod grv_cache_tests;

#[cfg(all(test, feature = "foundationdb-backend"))]
mod keyspace_tests;

#[cfg(all(test, feature = "foundationdb-backend"))]
mod mapped_range_tests;

#[cfg(all(test, feature = "foundationdb-backend"))]
mod queue_provider_tests;

#[cfg(all(test, feature = "foundationdb-backend"))]
mod read_context_tests;

#[cfg(all(test, feature = "foundationdb-backend"))]
mod operation_shape_tests;

#[cfg(all(test, feature = "foundationdb-backend"))]
mod stream_provider_tests;
