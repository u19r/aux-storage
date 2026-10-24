#[cfg(any(
    feature = "sqlite",
    feature = "turso",
    feature = "postgres",
    feature = "rocksdb",
    feature = "foundationdb"
))]
mod assembly;
mod construction;
mod delivery;
mod maintenance;
mod models;
pub(crate) use delivery::{DeliveryProviderSource, DeliveryProviders};
pub(crate) use maintenance::MaintenanceProviderSource;
#[cfg(feature = "regional")]
mod regional;

#[cfg(feature = "remote")]
pub use construction::connect_remote_scheduled_job_store;
pub(crate) use construction::create_storage_provider_bundle;
pub(crate) use models::MaintenanceRepositories;
#[cfg(feature = "remote")]
pub use models::RemoteScheduledJobStoreConfig;
pub use models::StorageProviderBundle;

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
mod partition_tests;
