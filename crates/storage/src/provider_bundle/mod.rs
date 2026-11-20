mod construction;
mod models;
#[cfg(test)]
mod models_test_support;
mod native_store;
#[cfg(feature = "remote")]
pub use construction::connect_remote_scheduled_job_store;
pub(crate) use construction::open_store;
pub use models::InitializedStore;
#[cfg(feature = "remote")]
pub use models::RemoteScheduledJobStoreConfig;
pub(crate) use models::{MaintenanceRepositories, StoreKind};
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
pub(crate) use models_test_support::CallerMaintenance;
pub(crate) use native_store::NativeStore;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod construction_tests;

#[cfg(all(test, feature = "rocksdb"))]
mod maintenance_tests;
#[cfg(all(test, feature = "rocksdb"))]
mod partition_tests;
